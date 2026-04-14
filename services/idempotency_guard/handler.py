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
    entity_type              — always "IdempotencyLock"
    schema_version           — internal item evolution
    idempotency_key          — the full computed key (internal only)
    job_run_id               — the execution that holds the lock
    workflow_name            — for human debugging
    primary_id               — the workflow's primary identifier
                               (e.g. normalized_candidate_name for initialize_nova,
                               nova_id for ingest_new_nova and all downstream workflows)
    acquired_at              — ISO-8601 UTC
    ttl                      — Unix epoch; DynamoDB TTL for automatic cleanup

Idempotency key format:
  {workflow_name}:{primary_id}:{schema_version}:{time_bucket}
  where time_bucket = YYYY-MM-DDTHH (1-hour granularity)

  Examples:
    initialize_nova:v1324 sco:1:2026-03-01T14
    ingest_new_nova:4e9b0e88-...:1:2026-03-01T14

Lock semantics:
  - Conditional put with attribute_not_exists(PK) — first writer wins
  - If lock already exists: raise RetryableError (Step Functions will retry
    with backoff; the concurrent execution should complete within the window)
  - TTL set to 15 minutes to prevent stale locks from blocking future runs

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
from nova_common.timing import log_duration
from nova_common.tracing import tracer

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_SCHEMA_VERSION = "1"
_LOCK_TTL_MINUTES = 15
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
    logger.info("Task started", extra={"task_name": task_name})
    with log_duration(f"task:{task_name}"):
        result = handler_fn(event, context)
    return result


# ---------------------------------------------------------------------------
# Task implementations
# ---------------------------------------------------------------------------


@tracer.capture_method
def _acquire_idempotency_lock(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Attempt to acquire the workflow-level idempotency lock.

    Raises RetryableError if the lock is already held by a concurrent
    execution. Step Functions will retry with backoff per the ASL policy.

    The caller supplies `primary_id` — the workflow's natural primary
    identifier for the idempotency key:
      - initialize_nova        → normalized_candidate_name
      - ingest_new_nova        → nova_id
      - all downstream workflows → nova_id (or data_product_id where applicable)

    Returns:
        idempotency_key — the computed key (internal; for logging only)
        acquired_at     — ISO-8601 UTC timestamp
    """
    workflow_name: str = event["workflow_name"]
    primary_id: str = event["primary_id"]
    job_run_id: str = event["job_run_id"]

    idempotency_key = _compute_key(workflow_name, primary_id)
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
                "primary_id": primary_id,
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


def _compute_key(workflow_name: str, primary_id: str) -> str:
    """
    Compute the workflow-level idempotency key.

    Format: {workflow_name}:{primary_id}:{schema_version}:{time_bucket}
    Time bucket granularity: 1 hour (YYYY-MM-DDTHH)

    The time bucket ensures that re-runs on different hours are treated as
    distinct executions, while rapid re-triggers within the same hour are
    deduplicated. To force a re-run within the same hour, delete the lock
    item from DynamoDB (see module docstring for CLI command).
    """
    time_bucket = datetime.now(UTC).strftime(_TIME_BUCKET_FORMAT)
    return f"{workflow_name}:{primary_id}:{_SCHEMA_VERSION}:{time_bucket}"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _ttl_epoch() -> int:
    """TTL for the lock item — 15 minutes from now, as Unix epoch seconds."""
    expiry = datetime.now(UTC) + timedelta(minutes=_LOCK_TTL_MINUTES)
    return int(expiry.timestamp())


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "AcquireIdempotencyLock": _acquire_idempotency_lock,
}
