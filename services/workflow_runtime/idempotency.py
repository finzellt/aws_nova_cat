"""
Idempotency lock primitives.

This module implements DynamoDB-based idempotency locks to prevent
duplicate processing of workflow steps.

Locks:

- Are stored in the operational DynamoDB table.
- Use conditional writes to ensure "first writer wins".
- Include TTL attributes to allow automatic expiration.

These locks support execution governance policies around step-level
idempotency without embedding idempotency keys in external event schemas.

This module intentionally avoids higher-level retry orchestration logic
and focuses strictly on lock acquisition and release.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import ClientError

from .ddb import TableRef, dynamodb_client, table_ref, to_ddb_item


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _lock_pk(key: str) -> str:
    return f"LOCK#{key}"


def acquire_lock(
    key: str,
    ttl_seconds: int,
    *,
    now_epoch: int | None = None,
    ddb: Any | None = None,
    table: TableRef | None = None,
) -> bool:
    """Acquire an idempotency lock.

    Returns True if acquired, False if lock already exists.

    Lock item uses a TTL attribute `expires_at` (epoch seconds) so it can be
    auto-cleaned by DynamoDB TTL.
    """

    import time as _time

    ddb = ddb or dynamodb_client()
    table = table or table_ref()

    now_epoch = int(now_epoch if now_epoch is not None else _time.time())
    expires_at = now_epoch + int(ttl_seconds)

    item: dict[str, Any] = {
        "PK": _lock_pk(key),
        "SK": "LOCK",
        "entity_type": "LOCK",
        "lock_key": key,
        "created_at": _utc_now_iso(),
        "expires_at": expires_at,
    }

    try:
        ddb.put_item(
            TableName=table.name,
            Item=to_ddb_item(item),
            ConditionExpression="attribute_not_exists(PK)",
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def release_lock(
    key: str,
    *,
    ddb: Any | None = None,
    table: TableRef | None = None,
) -> None:
    ddb = ddb or dynamodb_client()
    table = table or table_ref()

    ddb.delete_item(
        TableName=table.name,
        Key={"PK": {"S": _lock_pk(key)}, "SK": {"S": "LOCK"}},
    )
