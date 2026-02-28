"""
JobRun persistence primitives.

This module implements creation and finalization of JobRun records
according to the Nova Cat DynamoDB item model.

JobRun records:

- Are stored in the per-nova partition (PK = <nova_id>)
- Use SK format:
  JOBRUN#<workflow_name>#<started_at>#<job_run_id>
- Include entity_type = "JobRun"
- Include schema_version = "1"

JobRun represents the lifecycle of a single workflow execution.

This module enforces:

- Deterministic key construction
- Single-finalization semantics (conditional update)
- Explicit status/outcome tracking

It does not manage domain entities or workflow logic.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .ddb import TableRef, dynamodb_client, table_ref, to_ddb_item, to_ddb_value

SCHEMA_VERSION = "1"


def _iso_utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _jobrun_sk(workflow_name: str, started_at: str, job_run_id: str) -> str:
    return f"JOBRUN#{workflow_name}#{started_at}#{job_run_id}"


def begin_job_run(
    *,
    nova_id: str,
    workflow_name: str,
    execution_arn: str,
    correlation_id: str,
    idempotency_key: str,
    identifiers: Mapping[str, str] | None = None,
    started_at: str | None = None,
    # Test hook: allow deterministic job_run_id.
    job_run_id: str | None = None,
    ddb: Any | None = None,
    table: TableRef | None = None,
) -> str:
    """Create a JobRun item and return job_run_id."""

    ddb = ddb or dynamodb_client()
    table = table or table_ref()

    jr_id = job_run_id or str(uuid.uuid4())
    now = started_at or _iso_utc_now()
    sk = _jobrun_sk(workflow_name, now, jr_id)

    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": sk,
        "entity_type": "JobRun",
        "schema_version": SCHEMA_VERSION,
        "job_run_id": jr_id,
        "workflow_name": workflow_name,
        "execution_arn": execution_arn,
        "status": "RUNNING",
        "started_at": now,
        "correlation_id": correlation_id,
        "idempotency_key": idempotency_key,
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

    return jr_id


def finalize_job_run(
    *,
    nova_id: str,
    workflow_name: str,
    started_at: str,
    job_run_id: str,
    status: str,
    ended_at: str | None = None,
    outcome: str | None = None,
    summary_fields: Mapping[str, Any] | None = None,
    ddb: Any | None = None,
    table: TableRef | None = None,
) -> None:
    """Finalize a JobRun.

    Uses a conditional update to prevent double-finalize.
    """

    ddb = ddb or dynamodb_client()
    table = table or table_ref()

    now = ended_at or _iso_utc_now()
    sk = _jobrun_sk(workflow_name, started_at, job_run_id)

    expr_parts = ["#st = :st", "ended_at = :ended_at", "updated_at = :updated_at"]
    attr_names = {"#st": "status"}
    attr_values: dict[str, Any] = {
        ":st": {"S": status},
        ":ended_at": {"S": now},
        ":updated_at": {"S": now},
    }

    if outcome is not None:
        expr_parts.append("outcome = :outcome")
        attr_values[":outcome"] = {"S": outcome}

    if summary_fields:
        expr_parts.append("summary = :summary")
        attr_values[":summary"] = to_ddb_value(
            {k: v for k, v in summary_fields.items() if v is not None}
        )

    update_expr = "SET " + ", ".join(expr_parts)

    ddb.update_item(
        TableName=table.name,
        Key={"PK": {"S": nova_id}, "SK": {"S": sk}},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=attr_values,
        ConditionExpression="attribute_not_exists(ended_at)",
    )
