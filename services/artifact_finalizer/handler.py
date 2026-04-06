"""artifact_finalizer — Finalize and FailHandler Lambda (DESIGN-003 §4.5).

Called by the ``regenerate_artifacts`` Step Functions workflow in two
contexts:

**Finalize** (state 3 — happy path):
  Reads the Fargate task's per-nova result payload from the
  ``RegenBatchPlan`` item.  For each nova that **succeeded**:
  - Deletes consumed WorkItems (using the ``workitem_sks`` snapshot,
    filtered to the successful nova's items).
  - Writes ``spectra_count``, ``photometry_count``, ``references_count``,
    and ``has_sparkline`` to the Nova DDB item (``PK=<nova_id>``,
    ``SK=NOVA``).
  For each nova that **failed**: leaves WorkItems in queue, no counts
  updated.  Updates the plan status to ``COMPLETED`` (all succeeded) or
  ``FAILED`` (at least one failed).

**FailHandler** (state 4 — Fargate crash):
  Updates the ``RegenBatchPlan`` status to ``FAILED``.  All WorkItems
  are retained — nothing is lost, and the next sweep will rebuild.

Task dispatch table:
  Finalize    — commit succeeded novae, update plan status
  FailHandler — mark plan as FAILED after Fargate crash

Environment variables (injected by CDK):
    NOVA_CAT_TABLE_NAME           — DynamoDB table name
    LOG_LEVEL                     — logging level (default INFO)
    POWERTOOLS_SERVICE_NAME       — AWS Lambda Powertools service name
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key
from nova_common.logging import configure_logging, logger
from nova_common.timing import log_duration
from nova_common.tracing import tracer

from contracts.models.regeneration import PlanStatus

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_WORKQUEUE_PK = "WORKQUEUE"
_REGEN_PLAN_PK = "REGEN_PLAN"
_BATCH_DELETE_LIMIT = 25  # DDB batch_write_item max per call

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    """Lambda entry point — dispatches on ``task_name``."""
    configure_logging(event)
    task_name = event.get("task_name")
    handler_fn = _TASK_HANDLERS.get(task_name)  # type: ignore[arg-type]
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}")
    logger.info("Task started", extra={"task_name": task_name})
    with log_duration(f"task:{task_name}"):
        result = handler_fn(event, context)
    return result


# ---------------------------------------------------------------------------
# Task: Finalize (§4.5 state 3)
# ---------------------------------------------------------------------------


@tracer.capture_method
def _finalize(event: dict[str, Any], context: object) -> dict[str, Any]:
    """Commit succeeded novae and update the batch plan status."""
    plan_id = event["plan_id"]
    logger.append_keys(workflow_name="artifact_finalizer", plan_id=plan_id)
    logger.info("Finalize started")

    plan = _load_batch_plan(plan_id)
    nova_results: list[dict[str, Any]] = plan.get("nova_results", [])
    workitem_sks: list[str] = plan.get("workitem_sks", [])

    succeeded_nova_ids: list[str] = []
    failed_nova_ids: list[str] = []

    for result in nova_results:
        nova_id = result["nova_id"]
        if result.get("success"):
            succeeded_nova_ids.append(nova_id)
            _write_observation_counts(nova_id, result)
            _delete_work_items_for_nova(nova_id, workitem_sks)
        else:
            failed_nova_ids.append(nova_id)

    # Determine terminal status
    terminal_status = PlanStatus.failed if failed_nova_ids else PlanStatus.completed

    _update_plan_status(plan["SK"], terminal_status)

    logger.info(
        "Finalize completed",
        extra={
            "novae_succeeded": len(succeeded_nova_ids),
            "novae_failed": len(failed_nova_ids),
            "plan_status": terminal_status.value,
            "workitems_deleted": _count_work_items_for_novae(
                succeeded_nova_ids,
                workitem_sks,
            ),
        },
    )

    return {
        "plan_id": plan_id,
        "status": terminal_status.value,
        "novae_succeeded": len(succeeded_nova_ids),
        "novae_failed": len(failed_nova_ids),
    }


# ---------------------------------------------------------------------------
# Task: FailHandler (§4.5 state 4)
# ---------------------------------------------------------------------------


@tracer.capture_method
def _fail_handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    """Mark the batch plan as FAILED after a Fargate crash."""
    plan_id = event["plan_id"]
    logger.append_keys(workflow_name="artifact_finalizer", plan_id=plan_id)
    logger.info("FailHandler invoked — Fargate task crashed or timed out")

    plan = _load_batch_plan(plan_id)
    _update_plan_status(plan["SK"], PlanStatus.failed)

    logger.info(
        "Plan marked as FAILED — all WorkItems retained for next sweep",
    )

    return {
        "plan_id": plan_id,
        "status": PlanStatus.failed.value,
    }


# ---------------------------------------------------------------------------
# Task: UpdatePlanInProgress (§4.5 state 1)
# ---------------------------------------------------------------------------


@tracer.capture_method
def _update_plan_in_progress(event: dict[str, Any], context: object) -> dict[str, Any]:
    """Set plan status to IN_PROGRESS and record the execution ARN."""
    plan_id = event["plan_id"]
    execution_arn = event.get("execution_arn", "")
    logger.append_keys(workflow_name="artifact_finalizer", plan_id=plan_id)
    logger.info("Updating plan to IN_PROGRESS", extra={"execution_arn": execution_arn})

    plan = _load_batch_plan(plan_id)
    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    _table.update_item(
        Key={"PK": _REGEN_PLAN_PK, "SK": plan["SK"]},
        UpdateExpression="SET #s = :status, execution_arn = :arn, updated_at = :now",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":status": PlanStatus.in_progress.value,
            ":arn": execution_arn,
            ":now": now,
        },
    )

    return {
        "plan_id": plan_id,
        "status": PlanStatus.in_progress.value,
    }


# ---------------------------------------------------------------------------
# Plan loading
# ---------------------------------------------------------------------------


def _load_batch_plan(plan_id: str) -> dict[str, Any]:
    """Load a RegenBatchPlan by ``plan_id``."""
    response = _table.query(
        KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
        FilterExpression=Attr("plan_id").eq(plan_id),
    )
    items = response.get("Items", [])
    if not items:
        raise ValueError(f"Batch plan not found: {plan_id}")
    return dict(items[0])


# ---------------------------------------------------------------------------
# WorkItem deletion
# ---------------------------------------------------------------------------


def _filter_sks_for_nova(nova_id: str, workitem_sks: list[str]) -> list[str]:
    """Return WorkItem SKs belonging to a specific nova.

    WorkItem SK format: ``<nova_id>#<dirty_type>#<created_at>``.
    """
    prefix = f"{nova_id}#"
    return [sk for sk in workitem_sks if sk.startswith(prefix)]


def _delete_work_items_for_nova(
    nova_id: str,
    workitem_sks: list[str],
) -> None:
    """Delete consumed WorkItems for a succeeded nova (§4.5).

    Uses ``batch_write_item`` with batches of 25 (DDB limit).
    Only deletes items from the ``workitem_sks`` snapshot — not any
    WorkItems that arrived during execution.
    """
    sks_to_delete = _filter_sks_for_nova(nova_id, workitem_sks)
    if not sks_to_delete:
        return

    for i in range(0, len(sks_to_delete), _BATCH_DELETE_LIMIT):
        batch = sks_to_delete[i : i + _BATCH_DELETE_LIMIT]
        _dynamodb.batch_write_item(
            RequestItems={
                _TABLE_NAME: [
                    {
                        "DeleteRequest": {
                            "Key": {"PK": _WORKQUEUE_PK, "SK": sk},
                        }
                    }
                    for sk in batch
                ]
            }
        )


def _count_work_items_for_novae(
    nova_ids: list[str],
    workitem_sks: list[str],
) -> int:
    """Count how many WorkItem SKs belong to the given nova IDs."""
    total = 0
    for nova_id in nova_ids:
        total += len(_filter_sks_for_nova(nova_id, workitem_sks))
    return total


# ---------------------------------------------------------------------------
# Observation count writeback
# ---------------------------------------------------------------------------


def _write_observation_counts(
    nova_id: str,
    result: dict[str, Any],
) -> None:
    """Write observation counts to the Nova DDB item (§4.5, §11.10).

    Fields written: ``spectra_count``, ``photometry_count``,
    ``references_count``, ``has_sparkline``.
    """
    _table.update_item(
        Key={"PK": nova_id, "SK": "NOVA"},
        UpdateExpression=(
            "SET spectra_count = :sc, "
            "photometry_count = :pc, "
            "references_count = :rc, "
            "has_sparkline = :hs"
        ),
        ExpressionAttributeValues={
            ":sc": result.get("spectra_count", 0),
            ":pc": result.get("photometry_count", 0),
            ":rc": result.get("references_count", 0),
            ":hs": result.get("has_sparkline", False),
        },
    )


# ---------------------------------------------------------------------------
# Plan status update
# ---------------------------------------------------------------------------


def _update_plan_status(plan_sk: str, status: PlanStatus) -> None:
    """Update the RegenBatchPlan status and completed_at timestamp."""
    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    _table.update_item(
        Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
        UpdateExpression="SET #s = :status, completed_at = :now",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":status": status.value,
            ":now": now,
        },
    )


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "UpdatePlanInProgress": _update_plan_in_progress,
    "Finalize": _finalize,
    "FailHandler": _fail_handler,
}
