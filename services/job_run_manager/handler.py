"""
job_run_manager Lambda handler

Manages JobRun and Attempt records for all Nova Cat workflows.

Task dispatch table:
  BeginJobRun               — emit JobRun STARTED, generate correlation_id if missing
  FinalizeJobRunSuccess     — emit JobRun SUCCEEDED with outcome
  FinalizeJobRunFailed      — emit JobRun FAILED with error classification
  FinalizeJobRunQuarantined — emit JobRun QUARANTINED

DynamoDB item model (see dynamodb-item-model.md):
  JobRun:  PK = "WORKFLOW#<correlation_id>" (pre-nova) or "<nova_id>" (post-creation)
           SK = "JOBRUN#<workflow_name>#<started_at>#<job_run_id>"

Note on PK before nova_id is known:
  initialize_nova calls BeginJobRun before a nova_id exists. JobRuns are
  partitioned under "WORKFLOW#<correlation_id>" until a nova_id is assigned.
  For workflows that never produce a nova_id (NOT_FOUND, NOT_A_CLASSICAL_NOVA)
  this partition is permanent.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr
from nova_common.errors import RetryableError  # noqa: F401 — imported for consistency
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_SCHEMA_VERSION = "1"

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    handler_fn = _TASK_HANDLERS.get(task_name)  # type: ignore[arg-type]
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}")
    return handler_fn(event, context)


# ---------------------------------------------------------------------------
# Task implementations
# ---------------------------------------------------------------------------


@tracer.capture_method
def _begin_job_run(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Emit JobRun STARTED and generate a correlation_id if missing.

    Returns:
        job_run_id     — new UUID for this execution
        correlation_id — caller-supplied or freshly generated
        started_at     — ISO-8601 UTC timestamp
        pk, sk         — DynamoDB key for subsequent FinalizeJobRun* updates
    """
    workflow_name: str = event["workflow_name"]
    candidate_name: str = event["candidate_name"]
    correlation_id: str = event.get("correlation_id") or str(uuid.uuid4())
    job_run_id: str = str(uuid.uuid4())
    started_at: str = _now()

    pk = f"WORKFLOW#{correlation_id}"
    sk = f"JOBRUN#{workflow_name}#{started_at}#{job_run_id}"

    item = {
        "PK": pk,
        "SK": sk,
        "entity_type": "JobRun",
        "schema_version": _SCHEMA_VERSION,
        "job_run_id": job_run_id,
        "workflow_name": workflow_name,
        "correlation_id": correlation_id,
        "candidate_name": candidate_name,
        "status": "RUNNING",
        "started_at": started_at,
        "created_at": started_at,
        "updated_at": started_at,
    }

    _table.put_item(
        Item=item,
        ConditionExpression=Attr("PK").not_exists(),
    )

    logger.info(
        "JobRun STARTED",
        extra={
            "job_run_id": job_run_id,
            "candidate_name": candidate_name,
        },
    )

    return {
        "job_run_id": job_run_id,
        "correlation_id": correlation_id,
        "started_at": started_at,
        "pk": pk,
        "sk": sk,
    }


@tracer.capture_method
def _finalize_job_run_success(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Emit JobRun SUCCEEDED with the terminal outcome.

    Expected outcomes:
        CREATED_AND_LAUNCHED | EXISTS_AND_LAUNCHED |
        NOT_FOUND | NOT_A_CLASSICAL_NOVA
    """
    job_run: dict[str, Any] = event["job_run"]
    outcome: str = event["outcome"]
    ended_at: str = _now()

    _table.update_item(
        Key={"PK": job_run["pk"], "SK": job_run["sk"]},
        UpdateExpression=(
            "SET #status = :status, outcome = :outcome, "
            "ended_at = :ended_at, updated_at = :updated_at"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "SUCCEEDED",
            ":outcome": outcome,
            ":ended_at": ended_at,
            ":updated_at": ended_at,
        },
    )

    logger.info("JobRun SUCCEEDED", extra={"outcome": outcome})
    return {"outcome": outcome, "ended_at": ended_at}


@tracer.capture_method
def _finalize_job_run_failed(event: dict[str, Any], context: object) -> dict[str, Any]:
    """Emit JobRun FAILED with error classification."""
    job_run: dict[str, Any] = event["job_run"]
    error: dict[str, Any] = event.get("error", {})
    ended_at: str = _now()

    _table.update_item(
        Key={"PK": job_run["pk"], "SK": job_run["sk"]},
        UpdateExpression=(
            "SET #status = :status, error_type = :error_type, "
            "error_message = :error_message, ended_at = :ended_at, "
            "updated_at = :updated_at"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "FAILED",
            ":error_type": error.get("Error", "UnknownError"),
            ":error_message": error.get("Cause", "")[:500],
            ":ended_at": ended_at,
            ":updated_at": ended_at,
        },
    )

    logger.error("JobRun FAILED", extra={"error": error})
    return {"status": "FAILED", "ended_at": ended_at}


@tracer.capture_method
def _finalize_job_run_quarantined(event: dict[str, Any], context: object) -> dict[str, Any]:
    """Emit JobRun QUARANTINED."""
    job_run: dict[str, Any] = event["job_run"]
    ended_at: str = _now()

    _table.update_item(
        Key={"PK": job_run["pk"], "SK": job_run["sk"]},
        UpdateExpression=("SET #status = :status, ended_at = :ended_at, updated_at = :updated_at"),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "QUARANTINED",
            ":ended_at": ended_at,
            ":updated_at": ended_at,
        },
    )

    logger.warning("JobRun QUARANTINED")
    return {"status": "QUARANTINED", "ended_at": ended_at}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "BeginJobRun": _begin_job_run,
    "FinalizeJobRunSuccess": _finalize_job_run_success,
    "FinalizeJobRunFailed": _finalize_job_run_failed,
    "FinalizeJobRunQuarantined": _finalize_job_run_quarantined,
}
