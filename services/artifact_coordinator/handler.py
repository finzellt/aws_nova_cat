"""artifact_coordinator — Sweep coordinator Lambda (DESIGN-003 §4.2).

Invoked by an EventBridge scheduled rule (6-hour cron) or manually via
the AWS console / CLI.  The coordinator is a **planning and dispatch**
step — it never generates artifacts itself.

Execution steps (§4.2):
  1. Query the work queue (``PK=WORKQUEUE``, paginated).
  2. Check for stale or in-progress batch plans.
     - ``PENDING`` plan → abandon and rebuild (§4.2 step 2).
     - ``IN_PROGRESS`` plan → exit immediately (§4.6).
  3. Build per-nova manifests via the dependency matrix (§3.4).
  4. Emit structured warnings for stale WorkItems (>7 days).
  5. Persist a ``RegenBatchPlan`` item with status ``PENDING``.
  6. Start the ``regenerate_artifacts`` Step Functions workflow.

Exit paths:
  - Empty queue → no-op, no plan created.
  - In-progress plan found → exit with log, no plan created.
  - Normal → plan created and workflow launched.

Environment variables (injected by CDK):
    NOVA_CAT_TABLE_NAME                     — DynamoDB table name
    REGENERATE_ARTIFACTS_STATE_MACHINE_ARN  — Step Functions workflow ARN
    WORKITEM_STALE_THRESHOLD_DAYS           — stale WorkItem warning threshold
                                              (default 7)
    LOG_LEVEL                               — logging level (default INFO)
    POWERTOOLS_SERVICE_NAME                 — AWS Lambda Powertools service name
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Key
from nova_common.logging import logger
from nova_common.tracing import tracer

from contracts.models.regeneration import (
    NovaManifest,
    PlanStatus,
    artifacts_for_dirty_types,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_STATE_MACHINE_ARN = os.environ["REGENERATE_ARTIFACTS_STATE_MACHINE_ARN"]
_STALE_THRESHOLD_DAYS = int(os.environ.get("WORKITEM_STALE_THRESHOLD_DAYS", "7"))

_WORKQUEUE_PK = "WORKQUEUE"
_REGEN_PLAN_PK = "REGEN_PLAN"
_PLAN_TTL_DAYS = 7
_SECONDS_PER_DAY = 86_400

# ---------------------------------------------------------------------------
# AWS clients — module-level for connection reuse across invocations
# ---------------------------------------------------------------------------

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)
_sfn = boto3.client("stepfunctions")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    """Lambda entry point.  EventBridge invokes with an empty event."""
    logger.append_keys(workflow_name="artifact_coordinator")
    logger.info("Coordinator invoked")

    # Step 1 — Query the work queue
    work_items = _query_work_queue()
    if not work_items:
        logger.info("Work queue is empty — exiting with no action")
        return {"action": "no_op", "reason": "empty_queue"}

    logger.info("WorkItems found", extra={"workitem_count": len(work_items)})

    # Step 2 — Check for existing batch plans
    existing_plan = _find_latest_active_plan()
    if existing_plan is not None:
        plan_status = existing_plan["status"]

        if plan_status == PlanStatus.in_progress.value:
            logger.info(
                "In-progress plan found — exiting to avoid concurrent sweeps",
                extra={
                    "existing_plan_id": existing_plan.get("plan_id"),
                    "execution_arn": existing_plan.get("execution_arn"),
                },
            )
            return {
                "action": "skipped",
                "reason": "in_progress_plan",
                "existing_plan_id": existing_plan.get("plan_id"),
            }

        if plan_status == PlanStatus.pending.value:
            _abandon_plan(existing_plan)

    # Step 3 — Build per-nova manifests
    nova_manifests = _build_nova_manifests(work_items)

    # Step 4 — Emit stale WorkItem warnings
    _warn_stale_work_items(work_items)

    # Step 5 — Persist the batch plan
    workitem_sks = [item["SK"] for item in work_items]
    plan_id, plan_sk = _persist_batch_plan(nova_manifests, workitem_sks)

    logger.info(
        "Batch plan persisted",
        extra={
            "plan_id": plan_id,
            "nova_count": len(nova_manifests),
            "workitem_count": len(workitem_sks),
        },
    )

    # Step 6 — Launch the Step Functions workflow
    execution_arn = _start_workflow(plan_id)
    _update_plan_execution_arn(plan_sk, execution_arn)

    logger.info(
        "Workflow launched",
        extra={"plan_id": plan_id, "execution_arn": execution_arn},
    )

    return {
        "action": "launched",
        "plan_id": plan_id,
        "nova_count": len(nova_manifests),
        "workitem_count": len(workitem_sks),
        "execution_arn": execution_arn,
    }


# ---------------------------------------------------------------------------
# Step 1 — Query WORKQUEUE (paginated)
# ---------------------------------------------------------------------------


def _query_work_queue() -> list[dict[str, Any]]:
    """Return all WorkItems from the ``WORKQUEUE`` partition."""
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": Key("PK").eq(_WORKQUEUE_PK),
    }
    while True:
        response = _table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


# ---------------------------------------------------------------------------
# Step 2 — Find latest non-terminal plan
# ---------------------------------------------------------------------------


def _find_latest_active_plan() -> dict[str, Any] | None:
    """Return the most recent PENDING or IN_PROGRESS plan, or None.

    Plans are stored with ``PK=REGEN_PLAN`` and ``SK=<created_at>#<plan_id>``.
    Scanning in reverse order returns the newest plan first.  We check the
    first few items for a non-terminal status and return it.
    """
    response = _table.query(
        KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
        ScanIndexForward=False,
        Limit=5,
    )
    non_terminal = {PlanStatus.pending.value, PlanStatus.in_progress.value}
    for item in response.get("Items", []):
        if item.get("status") in non_terminal:
            return dict(item)
    return None


# ---------------------------------------------------------------------------
# Step 2b — Abandon a stale PENDING plan
# ---------------------------------------------------------------------------


def _abandon_plan(plan_item: dict[str, Any]) -> None:
    """Set a PENDING plan's status to ABANDONED (§4.2 step 2)."""
    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    _table.update_item(
        Key={"PK": _REGEN_PLAN_PK, "SK": plan_item["SK"]},
        UpdateExpression="SET #s = :abandoned, completed_at = :now",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":abandoned": PlanStatus.abandoned.value,
            ":now": now,
        },
    )
    logger.info(
        "Abandoned stale PENDING plan",
        extra={"abandoned_plan_id": plan_item.get("plan_id")},
    )


# ---------------------------------------------------------------------------
# Step 3 — Build per-nova manifests
# ---------------------------------------------------------------------------


def _build_nova_manifests(
    work_items: list[dict[str, Any]],
) -> dict[str, NovaManifest]:
    """Group WorkItems by ``nova_id`` and apply the dependency matrix (§3.4).

    Returns a dict of ``nova_id`` → ``NovaManifest``.
    """
    # Group dirty_types by nova
    nova_dirty_types: dict[str, set[str]] = {}
    for item in work_items:
        nova_id = item["nova_id"]
        dirty_type = item["dirty_type"]
        nova_dirty_types.setdefault(nova_id, set()).add(dirty_type)

    manifests: dict[str, NovaManifest] = {}
    for nova_id, dirty_types in nova_dirty_types.items():
        artifacts = artifacts_for_dirty_types(dirty_types)
        manifests[nova_id] = NovaManifest(
            dirty_types=sorted(dirty_types),
            artifacts=sorted(artifacts, key=lambda a: a.value),
        )

    return manifests


# ---------------------------------------------------------------------------
# Step 4 — Stale WorkItem warnings
# ---------------------------------------------------------------------------


def _warn_stale_work_items(work_items: list[dict[str, Any]]) -> None:
    """Log structured warnings for WorkItems older than the threshold (§4.2 step 4)."""
    threshold = datetime.now(UTC) - timedelta(days=_STALE_THRESHOLD_DAYS)
    stale_items: list[dict[str, str]] = []

    for item in work_items:
        created_at_str = item.get("created_at", "")
        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if created_at < threshold:
            stale_items.append(
                {
                    "nova_id": item.get("nova_id", ""),
                    "dirty_type": item.get("dirty_type", ""),
                    "created_at": created_at_str,
                    "job_run_id": item.get("job_run_id", ""),
                }
            )

    if stale_items:
        logger.warning(
            "Stale WorkItems detected — items have been in the queue longer than "
            f"{_STALE_THRESHOLD_DAYS} days",
            extra={"stale_workitems": stale_items, "stale_count": len(stale_items)},
        )


# ---------------------------------------------------------------------------
# Step 5 — Persist RegenBatchPlan
# ---------------------------------------------------------------------------


def _persist_batch_plan(
    nova_manifests: dict[str, NovaManifest],
    workitem_sks: list[str],
) -> tuple[str, str]:
    """Write a RegenBatchPlan item and return ``(plan_id, SK)``."""
    now = datetime.now(UTC)
    created_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    plan_id = str(uuid4())
    sk = f"{created_at}#{plan_id}"
    ttl = int(now.timestamp()) + (_PLAN_TTL_DAYS * _SECONDS_PER_DAY)

    # Serialise manifests to plain dicts for DDB storage
    manifests_ddb: dict[str, dict[str, Any]] = {}
    for nova_id, manifest in nova_manifests.items():
        manifests_ddb[nova_id] = {
            "dirty_types": manifest.dirty_types,
            "artifacts": [a.value for a in manifest.artifacts],
        }

    _table.put_item(
        Item={
            "PK": _REGEN_PLAN_PK,
            "SK": sk,
            "entity_type": "RegenBatchPlan",
            "schema_version": "1.0.0",
            "plan_id": plan_id,
            "status": PlanStatus.pending.value,
            "nova_manifests": manifests_ddb,
            "nova_count": len(nova_manifests),
            "workitem_sks": workitem_sks,
            "created_at": created_at,
            "completed_at": None,
            "execution_arn": None,
            "ttl": ttl,
        }
    )
    return plan_id, sk


# ---------------------------------------------------------------------------
# Step 6 — Launch Step Functions workflow
# ---------------------------------------------------------------------------


def _start_workflow(plan_id: str) -> str:
    """Start the ``regenerate_artifacts`` workflow and return the execution ARN."""
    execution_name = f"sweep-{plan_id}"
    response = _sfn.start_execution(
        stateMachineArn=_STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps({"plan_id": plan_id}),
    )
    arn: str = response["executionArn"]
    return arn


def _update_plan_execution_arn(plan_sk: str, execution_arn: str) -> None:
    """Record the execution ARN on the batch plan (§4.3)."""
    _table.update_item(
        Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
        UpdateExpression="SET execution_arn = :arn",
        ExpressionAttributeValues={":arn": execution_arn},
    )
