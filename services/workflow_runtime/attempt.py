"""
Attempt persistence primitives.

This module records execution attempts for individual workflow tasks.

Attempt records:

- Are stored in the per-nova partition (PK = <nova_id>)
- Use SK format:
  ATTEMPT#<job_run_id>#<task_name>#<attempt_no>#<timestamp>
- Include entity_type = "Attempt"
- Include schema_version = "1"

Each attempt captures:

- Start time
- End time
- Status
- Duration
- Error classification + fingerprint (if applicable)

Attempts provide fine-grained observability into retry behavior and
task-level failures.

This module does not implement retry logic itself; it only records metadata.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .ddb import TableRef, dynamodb_client, table_ref, to_ddb_item, to_ddb_value

SCHEMA_VERSION = "1"


def _iso_utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _attempt_sk(job_run_id: str, task_name: str, attempt_no: int, timestamp: str) -> str:
    return f"ATTEMPT#{job_run_id}#{task_name}#{attempt_no}#{timestamp}"


def record_attempt_started(
    *,
    nova_id: str,
    job_run_id: str,
    task_name: str,
    attempt_no: int,
    started_at: str | None = None,
    identifiers: Mapping[str, str] | None = None,
    ddb: Any | None = None,
    table: TableRef | None = None,
) -> str:
    """Insert an Attempt record and return its SK."""

    ddb = ddb or dynamodb_client()
    table = table or table_ref()

    now = started_at or _iso_utc_now()
    sk = _attempt_sk(job_run_id, task_name, attempt_no, now)

    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": sk,
        "entity_type": "Attempt",
        "schema_version": SCHEMA_VERSION,
        "job_run_id": job_run_id,
        "task_name": task_name,
        "attempt_no": attempt_no,
        "status": "STARTED",
        "started_at": now,
        "created_at": now,
        "updated_at": now,
    }

    if identifiers:
        item.update({k: v for k, v in identifiers.items() if v is not None})

    ddb.put_item(
        TableName=table.name,
        Item=to_ddb_item(item),
        ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)",
    )

    return sk


def record_attempt_finished(
    *,
    nova_id: str,
    attempt_sk: str,
    status: str,
    duration_ms: int,
    ended_at: str | None = None,
    error_fields: Mapping[str, Any] | None = None,
    ddb: Any | None = None,
    table: TableRef | None = None,
) -> None:
    """Update an Attempt record at its known SK."""

    ddb = ddb or dynamodb_client()
    table = table or table_ref()

    now = ended_at or _iso_utc_now()

    expr_parts = [
        "#st = :st",
        "finished_at = :finished_at",
        "duration_ms = :duration_ms",
        "updated_at = :updated_at",
    ]
    attr_names = {"#st": "status"}
    attr_values: dict[str, Any] = {
        ":st": {"S": status},
        ":finished_at": {"S": now},
        ":duration_ms": {"N": str(duration_ms)},
        ":updated_at": {"S": now},
    }

    if error_fields:
        cleaned = {k: v for k, v in error_fields.items() if v is not None}
        if cleaned:
            expr_parts.append("error = :error")
            attr_values[":error"] = to_ddb_value(cleaned)

    update_expr = "SET " + ", ".join(expr_parts)

    ddb.update_item(
        TableName=table.name,
        Key={"PK": {"S": nova_id}, "SK": {"S": attempt_sk}},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=attr_values,
        ConditionExpression="attribute_exists(PK) AND attribute_exists(SK)",
    )
