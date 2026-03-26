"""
ddb_writer.py — DDB and S3 side-effects for the ticket ingestor photometry branch.

Responsibilities:
  - Write PhotometryRow items to the dedicated photometry DynamoDB table using
    conditional PutItem to suppress duplicate row_ids (row-level idempotency).
  - Persist row-level failure diagnostics to S3.
  - Upsert the PRODUCT#PHOTOMETRY_TABLE envelope item in the main NovaCat table,
    using an "ensure exists" pattern (ADR-020 §Decision 6).

This module performs no CSV parsing and no band resolution.  All boto3
resource/client objects are injected by the caller so that tests can pass
moto-patched objects directly without any import-time side effects.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError

from ticket_ingestor.photometry_reader import ResolvedRow, RowFailure

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ENVELOPE_SK = "PRODUCT#PHOTOMETRY_TABLE"
_ENTITY_TYPE_ROW = "PhotometryRow"
_ENTITY_TYPE_PRODUCT = "DataProduct"
# Internal schema version for the DDB envelope item itself (not PhotometryRow).
_ENVELOPE_SCHEMA_VERSION = "1"
# Schema version of the PhotometryRow payload stored on row items.
_PHOTOMETRY_SCHEMA_VERSION = "1"
_INGESTION_SOURCE = "ticket_ingestion"
_PRODUCT_TYPE = "PHOTOMETRY_TABLE"
_CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WriteResult:
    """Counts returned by write_photometry_rows.

    Attributes:
        rows_written:            Number of rows successfully written to DDB.
        rows_skipped_duplicate:  Number of rows suppressed because their
                                 row_id already existed in DDB.
    """

    rows_written: int
    rows_skipped_duplicate: int


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def write_photometry_rows(
    rows: list[ResolvedRow],
    nova_id: uuid.UUID,
    table_name: str,
    table: Any,  # boto3 DynamoDB Table resource
) -> WriteResult:
    """Write ResolvedRow items to the dedicated photometry DynamoDB table.

    Each write uses a conditional PutItem that suppresses the write when an
    item with the same SK already exists, providing row-level idempotency.
    Re-running the same ticket against the same table produces no duplicates.

    Args:
        rows:       Rows to write, as produced by photometry_reader.
        nova_id:    Resolved nova UUID; becomes the partition key.
        table_name: Table name string, used for logging only.
        table:      Injected boto3 DynamoDB Table resource.

    Returns:
        WriteResult with rows_written and rows_skipped_duplicate counts.

    Raises:
        ClientError: For any DynamoDB error other than
            ConditionalCheckFailedException.
    """
    rows_written = 0
    rows_skipped_duplicate = 0

    for resolved in rows:
        item = _photometry_row_to_item(nova_id=nova_id, resolved_row=resolved)
        try:
            table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(SK)",
            )
            rows_written += 1
        except ClientError as exc:
            if exc.response["Error"]["Code"] == _CONDITIONAL_CHECK_FAILED:
                rows_skipped_duplicate += 1
            else:
                raise

    return WriteResult(
        rows_written=rows_written,
        rows_skipped_duplicate=rows_skipped_duplicate,
    )


def persist_row_failures(
    failures: list[RowFailure],
    nova_id: uuid.UUID,
    filename: str,
    bucket: str,
    s3: Any,  # boto3 S3 client
) -> None:
    """Serialise RowFailure records to an S3 diagnostics key.

    No-ops immediately if *failures* is empty, producing no S3 write.

    The S3 key stem is the SHA-256 hex digest of *filename* so that
    repeated ingestion of the same source file overwrites the same
    diagnostics object rather than accumulating stale entries.

    Key pattern:
        diagnostics/photometry/<nova_id>/row_failures/<sha256_of_filename>.json

    Args:
        failures:  RowFailure records to persist.  Empty list → no-op.
        nova_id:   Resolved nova UUID, used in the S3 key path.
        filename:  Ticket data_filename; its SHA-256 digest becomes the key stem.
        bucket:    S3 bucket name (DIAGNOSTICS_BUCKET).
        s3:        Injected boto3 S3 client.
    """
    if not failures:
        return

    filename_sha256 = hashlib.sha256(filename.encode()).hexdigest()
    s3_key = f"diagnostics/photometry/{nova_id}/row_failures/{filename_sha256}.json"

    payload = json.dumps(
        [
            {
                "row_number": f.row_number,
                "reason": f.reason,
                "raw_row": f.raw_row,
            }
            for f in failures
        ],
        ensure_ascii=False,
        indent=2,
    ).encode()

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=payload,
        ContentType="application/json",
    )


def upsert_envelope_item(
    nova_id: uuid.UUID,
    rows_written: int,
    table: Any,  # boto3 DynamoDB Table resource
) -> None:
    """Update the PRODUCT#PHOTOMETRY_TABLE envelope item for *nova_id*.

    Implements the "ensure exists" pattern from ADR-020 §Decision 6:

    Step 1 — conditional PutItem (create path):
        Attempts to create the envelope item with row_count = rows_written
        and ingestion_count = 1.  The condition ``attribute_not_exists(SK)``
        ensures this only succeeds when the item is absent.

    Step 2 — UpdateItem (update path):
        If Step 1 raises ConditionalCheckFailedException the item already
        exists (created by initialize_nova or a prior ingestion).  An
        unconditional UpdateItem increments row_count and ingestion_count and
        refreshes the last_ingestion_* fields.

    The two-step design means a concurrent creation race (two Lambda
    invocations both seeing the item as absent) is safe: the loser of Step 1
    falls through to Step 2 and performs a net-zero increment on an item that
    was just created with row_count = 0 by the winner.

    Args:
        nova_id:      Resolved nova UUID; becomes the partition key.
        rows_written: Number of new rows written in this invocation.
        table:        Injected boto3 DynamoDB Table resource.

    Raises:
        ClientError: For any DynamoDB error other than
            ConditionalCheckFailedException on the PutItem.
    """
    now_iso = _now_iso()
    pk = str(nova_id)

    # Derive a stable data_product_id for the envelope item so downstream
    # consumers always see the same UUID regardless of which invocation
    # created the item.
    data_product_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{nova_id}:{_ENVELOPE_SK}"))

    # ------------------------------------------------------------------
    # Step 1: create path
    # ------------------------------------------------------------------
    create_item: dict[str, Any] = {
        "PK": pk,
        "SK": _ENVELOPE_SK,
        "entity_type": _ENTITY_TYPE_PRODUCT,
        "schema_version": _ENVELOPE_SCHEMA_VERSION,
        "data_product_id": data_product_id,
        "product_type": _PRODUCT_TYPE,
        "photometry_schema_version": _PHOTOMETRY_SCHEMA_VERSION,
        "row_count": Decimal(rows_written),
        "ingestion_count": Decimal(1),
        "last_ingestion_at": now_iso,
        "last_ingestion_source": _INGESTION_SOURCE,
        "created_at": now_iso,
        "updated_at": now_iso,
    }

    try:
        table.put_item(
            Item=create_item,
            ConditionExpression="attribute_not_exists(SK)",
        )
        return  # Created successfully — done.
    except ClientError as exc:
        if exc.response["Error"]["Code"] != _CONDITIONAL_CHECK_FAILED:
            raise

    # ------------------------------------------------------------------
    # Step 2: update path (item already exists)
    # ------------------------------------------------------------------
    table.update_item(
        Key={"PK": pk, "SK": _ENVELOPE_SK},
        UpdateExpression=(
            "SET last_ingestion_at = :ts, "
            "    last_ingestion_source = :src, "
            "    updated_at = :ts "
            "ADD row_count :delta, "
            "    ingestion_count :one"
        ),
        ExpressionAttributeValues={
            ":ts": now_iso,
            ":src": _INGESTION_SOURCE,
            ":delta": Decimal(rows_written),
            ":one": Decimal(1),
        },
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with 'Z' suffix."""
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _photometry_row_to_item(
    nova_id: uuid.UUID,
    resolved_row: ResolvedRow,
) -> dict[str, Any]:
    """Serialise a ResolvedRow into a DynamoDB item dict.

    Conversion rules applied to each PhotometryRow field:
      - ``float``  → ``Decimal(str(v))``   (DDB rejects Python floats)
      - ``UUID``   → ``str(v)``
      - enum       → ``v.value``
      - ``None``   → field omitted entirely (DDB rejects explicit None)
      - ``bool``   → kept as-is (DDB supports bool natively via the boto3 resource)
      - ``str``    → kept as-is
    """
    row = resolved_row.row

    # Use model_dump to get a plain dict, then apply DDB-safe type coercions.
    # mode="python" returns Python-native types (UUIDs, enums, etc.) rather
    # than JSON-serialised strings, which is what we want here so we can apply
    # our own coercions uniformly.
    raw: dict[str, Any] = row.model_dump(mode="python", exclude_none=True)

    item: dict[str, Any] = {
        "PK": str(nova_id),
        "SK": f"PHOT#{resolved_row.row_id}",
        "entity_type": _ENTITY_TYPE_ROW,
        "schema_version": _PHOTOMETRY_SCHEMA_VERSION,
        "ingested_at": _now_iso(),
        "ingestion_source": _INGESTION_SOURCE,
    }

    for field_name, value in raw.items():
        item[field_name] = _coerce_for_ddb(value)

    return item


def _coerce_for_ddb(value: Any) -> Any:
    """Recursively coerce a value to a DynamoDB-safe type.

    DynamoDB via the boto3 resource layer does not accept Python floats or
    uuid.UUID objects.  Enums are stored as their string value so that items
    remain human-readable without requiring the enum definition to decode.

    Nested structures (lists, dicts) are coerced recursively.
    """
    import enum  # local import to keep module-level imports minimal

    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _coerce_for_ddb(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_coerce_for_ddb(v) for v in value]
    return value
