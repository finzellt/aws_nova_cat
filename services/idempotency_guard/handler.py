"""
idempotency_guard Lambda handler

Acquires workflow-level idempotency locks via conditional DynamoDB writes.
Prevents duplicate workflow executions for the same logical operation within
a time bucket.

Task dispatch table:
  AcquireIdempotencyLock — attempt to acquire lock; raise RetryableError if
                           already held by a concurrent execution

DynamoDB item model:
  PK = "IDEMPOTENCY#<idempotency_key>"
  SK = "LOCK"

  Fields:
    idempotency_key          — the full computed key (internal only)
    job_run_id               — the execution that holds the lock
    workflow_name            — for human debugging
    acquired_at              — ISO-8601 UTC
    ttl                      — Unix epoch; DynamoDB TTL for automatic cleanup

Idempotency key format (initialize_nova):
  InitializeNova:{normalized_candidate_name}:{schema_version}:{time_bucket}
  where time_bucket = YYYY-MM-DDTHH (1-hour granularity)

Lock semantics:
  - Conditional put with attribute_not_exists(PK) — first writer wins
  - If lock already exists: raise RetryableError (Step Functions will retry
    with backoff; the concurrent execution should complete within the window)
  - TTL set to 24 hours to prevent stale locks from blocking future runs

Manual override:
  To release a stale lock during debugging, delete the DynamoDB item:
    PK = "IDEMPOTENCY#<idempotency_key>"
    SK = "LOCK"
  Via CLI:
    aws dynamodb delete-item --table-name NovaCat \\
      --key '{"PK":{"S":"IDEMPOTENCY#<key>"},"SK":{"S":"LOCK"}}'
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from nova_common.errors import RetryableError
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_SCHEMA_VERSION = "1"
_LOCK_TTL_HOURS = 24
_TIME_BUCKET_FORMAT = "%Y-%m-%dT%H"  # 1-hour granularity

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
def _acquire_idempotency_lock(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Attempt to acquire the workflow-level idempotency lock.

    Raises RetryableError if the lock is already held by a concurrent
    execution. Step Functions will retry with backoff per the ASL policy.

    Returns:
        idempotency_key — the computed key (internal; for logging only)
        acquired_at     — ISO-8601 UTC timestamp
    """
    workflow_name: str = event["workflow_name"]
    normalized_candidate_name: str = event["normalized_candidate_name"]
    job_run_id: str = event["job_run_id"]

    idempotency_key = _compute_key(workflow_name, normalized_candidate_name)
    acquired_at = _now()
    ttl = _ttl_epoch()

    pk = f"IDEMPOTENCY#{idempotency_key}"

    try:
        _table.put_item(
            Item={
                "PK": pk,
                "SK": "LOCK",
                "entity_type": "IdempotencyLock",
                "schema_version": _SCHEMA_VERSION,
                "idempotency_key": idempotency_key,
                "job_run_id": job_run_id,
                "workflow_name": workflow_name,
                "normalized_candidate_name": normalized_candidate_name,
                "acquired_at": acquired_at,
                "ttl": ttl,
            },
            ConditionExpression=Attr("PK").not_exists(),
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(
                "Idempotency lock already held — signalling retry",
                extra={"idempotency_key": idempotency_key},
            )
            raise RetryableError(f"Idempotency lock already held for key: {idempotency_key}") from e
        raise

    logger.info(
        "Idempotency lock acquired",
        extra={"idempotency_key": idempotency_key},
    )

    return {
        "idempotency_key": idempotency_key,
        "acquired_at": acquired_at,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_key(workflow_name: str, normalized_candidate_name: str) -> str:
    """
    Compute the workflow-level idempotency key.

    Format: {workflow_name}:{normalized_candidate_name}:{schema_version}:{time_bucket}
    Time bucket granularity: 1 hour (YYYY-MM-DDTHH)

    The time bucket ensures that re-runs on different hours are treated as
    distinct executions, while rapid re-triggers within the same hour are
    deduplicated. To force a re-run within the same hour, delete the lock
    item from DynamoDB (see module docstring for CLI command).
    """
    time_bucket = datetime.now(UTC).strftime(_TIME_BUCKET_FORMAT)
    return f"{workflow_name}:{normalized_candidate_name}:{_SCHEMA_VERSION}:{time_bucket}"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _ttl_epoch() -> int:
    """TTL for the lock item — 24 hours from now, as Unix epoch seconds."""
    expiry = datetime.now(UTC) + timedelta(hours=_LOCK_TTL_HOURS)
    return int(expiry.timestamp())


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "AcquireIdempotencyLock": _acquire_idempotency_lock,
}
