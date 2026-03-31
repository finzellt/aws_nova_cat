"""
nova_resolver Lambda handler

Resolves candidate names and coordinates to stable nova identities.
All database-side identity operations for initialize_nova live here.

Task dispatch table:
  NormalizeCandidateName          — normalize raw candidate name for dedup/lookup
  CheckExistingNovaByName         — query NameMapping for normalized name
  CheckExistingNovaByCoordinates  — compute angular separation against all novae
  CreateNovaId                    — generate stable nova_id, write Nova stub
  UpsertMinimalNovaMetadata       — persist coordinates, aliases, and NameMapping
  UpsertAliasForExistingNova      — add candidate name as alias to existing nova

Angular separation:
  Computed via the haversine formula on ICRS RA/Dec — no astropy dependency.
  Thresholds per initialize-nova.md:
    < 2"   → DUPLICATE  (same nova)
    2"-10" → AMBIGUOUS  (human review required)
    > 10"  → NONE       (distinct object)
"""

from __future__ import annotations

import math
import os
import re
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import boto3
from boto3.dynamodb.conditions import Attr, Key
from nova_common.errors import TerminalError
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_SCHEMA_VERSION = "1"

# Angular separation thresholds in arcseconds
_SEP_DUPLICATE_ARCSEC = 2.0
_SEP_AMBIGUOUS_ARCSEC = 10.0

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
def _normalize_candidate_name(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Normalize a raw candidate name to canonical form for dedup and lookup.

    Normalization rules:
      - Lowercase
      - Collapse internal whitespace to single space
      - Strip leading/trailing whitespace

    Returns:
        normalized_candidate_name — canonical form for DynamoDB lookups
    """
    candidate_name: str = event["candidate_name"]

    if not candidate_name or not candidate_name.strip():
        raise TerminalError(f"candidate_name is empty or whitespace-only: {candidate_name!r}")

    normalized = re.sub(r"\s+", " ", candidate_name.strip().lower())

    logger.info(
        "Candidate name normalized",
        extra={"normalized_candidate_name": normalized},
    )

    return {"normalized_candidate_name": normalized}


@tracer.capture_method
def _check_existing_nova_by_name(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Query NameMapping items for the normalized candidate name.

    Checks both primary names and aliases. Returns the first match found.

    Returns:
        exists  — bool
        nova_id — UUID string if exists=true, else absent
    """
    normalized_candidate_name: str = event["normalized_candidate_name"]
    pk = f"NAME#{normalized_candidate_name}"

    response = _table.query(
        KeyConditionExpression=Key("PK").eq(pk),
        Limit=1,
    )

    items = response.get("Items", [])
    if items:
        nova_id = cast(str, items[0]["nova_id"])
        logger.info(
            "Nova found by name",
            extra={"nova_id": nova_id},
        )
        return {"exists": True, "nova_id": nova_id}

    logger.info("No nova found by name")
    return {"exists": False}


@tracer.capture_method
def _check_existing_nova_by_coordinates(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Compute angular separation between resolved coordinates and all persisted novae.

    Scans all Nova items (PK = <nova_id>, SK = "NOVA") that have coordinates,
    computes angular separation using the haversine formula, and classifies
    the minimum separation.

    Inputs (from resolution output):
        resolved_ra    — ICRS right ascension in degrees
        resolved_dec   — ICRS declination in degrees
        resolved_epoch — coordinate epoch string (e.g. "J2000"); informational only

    Returns:
        match_outcome      — "DUPLICATE" | "AMBIGUOUS" | "NONE"
        min_sep_arcsec     — minimum angular separation found
        matched_nova_id    — nova_id of closest match (present when DUPLICATE)
    """
    resolved_ra: float = float(event["resolved_ra"])
    resolved_dec: float = float(event["resolved_dec"])

    # Scan all Nova items with coordinates
    # At MVP scale (<1000 novae) a full scan is acceptable.
    # A future GSI on coord fields would be needed at larger scale.
    response = _table.scan(
        FilterExpression=Attr("SK").eq("NOVA"),
        ProjectionExpression="nova_id, ra_deg, dec_deg",
    )

    items = response.get("Items", [])
    min_sep = float("inf")
    matched_nova_id: str | None = None

    for item in items:
        if "ra_deg" not in item or "dec_deg" not in item:
            continue
        ra = float(cast(str, item["ra_deg"]))
        dec = float(cast(str, item["dec_deg"]))
        sep = _angular_separation_arcsec(resolved_ra, resolved_dec, ra, dec)
        if sep < min_sep:
            min_sep = sep
            matched_nova_id = cast(str, item["nova_id"])

    if min_sep == float("inf"):
        logger.info("No novae with coordinates in database — no coordinate match")
        return {
            "match_outcome": "NONE",
            "min_sep_arcsec": None,
        }

    if min_sep < _SEP_DUPLICATE_ARCSEC:
        outcome = "DUPLICATE"
    elif min_sep < _SEP_AMBIGUOUS_ARCSEC:
        outcome = "AMBIGUOUS"
    else:
        outcome = "NONE"

    logger.info(
        "Coordinate match classification",
        extra={
            "match_outcome": outcome,
            "min_sep_arcsec": round(min_sep, 4),
            "matched_nova_id": matched_nova_id,
        },
    )

    result: dict[str, Any] = {
        "match_outcome": outcome,
        "min_sep_arcsec": round(min_sep, 4),
    }
    if outcome == "DUPLICATE" and matched_nova_id:
        result["matched_nova_id"] = matched_nova_id

    return result


@tracer.capture_method
def _create_nova_id(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Generate a stable nova_id and write the initial Nova stub to DynamoDB.

    The Nova item is written with status=PENDING. UpsertMinimalNovaMetadata
    will populate coordinates and other fields immediately after.

    Returns:
        nova_id — the newly assigned UUID
    """
    candidate_name: str = event["candidate_name"]
    normalized_candidate_name: str = event["normalized_candidate_name"]
    job_run_id: str = event["job_run_id"]
    now = _now()
    nova_id = str(uuid.uuid4())

    _table.put_item(
        Item={
            "PK": nova_id,
            "SK": "NOVA",
            "entity_type": "Nova",
            "schema_version": _SCHEMA_VERSION,
            "nova_id": nova_id,
            "primary_name": candidate_name,
            "primary_name_normalized": normalized_candidate_name,
            "status": "ACTIVE",
            "nova_type": None,
            "created_by_job_run_id": job_run_id,
            "created_at": now,
        }
    )

    logger.info("Nova stub created", extra={"nova_id": nova_id})
    return {"nova_id": nova_id}


@tracer.capture_method
def _upsert_minimal_nova_metadata(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Persist resolved coordinates, aliases, and NameMapping for a newly created nova.

    Writes:
      1. Updates the Nova item with coordinates, ACTIVE status, and aliases list
      2. Writes a PRIMARY NameMapping item
      3. Writes an ALIAS NameMapping item for each SIMBAD alias, if provided

    The `aliases` field on the Nova item is a denormalized list of raw alias
    strings (e.g. ["NOVA Sco 2012", "Gaia DR3 4043499439062100096"]).
    It exists so that refresh_references can retrieve all known names for a
    nova in a single get_item call without querying NameMapping partitions.

    SIMBAD aliases are supplied via the optional `aliases` field in the event
    (list of raw strings from archive_resolver). Each alias is:
      - Stored raw in the Nova.aliases list and in name_raw on NameMapping
      - Normalized (lowercase, collapse whitespace) for the NameMapping PK
        so that CheckExistingNovaByName can find it
      - Skipped if its normalized form matches normalized_candidate_name

    Returns:
        nova_id — echoed from input
    """
    nova_id: str = event["nova_id"]
    candidate_name: str = event["candidate_name"]
    normalized_candidate_name: str = event["normalized_candidate_name"]
    resolved_ra: float = float(event["resolved_ra"])
    resolved_dec: float = float(event["resolved_dec"])
    resolved_epoch: str = event.get("resolved_epoch") or "J2000"
    resolver_source: str = event.get("resolver_source") or "UNKNOWN"
    aliases: list[str] = event.get("aliases") or []
    now = _now()

    # Update Nova item with coordinates, ACTIVE status, and aliases list
    _table.update_item(
        Key={"PK": nova_id, "SK": "NOVA"},
        UpdateExpression=(
            "SET ra_deg = :ra, dec_deg = :dec, coord_epoch = :epoch, "
            "coord_frame = :frame, resolver_source = :source, "
            "#status = :status, aliases = :aliases, updated_at = :now"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":ra": Decimal(str(resolved_ra)),
            ":dec": Decimal(str(resolved_dec)),
            ":epoch": resolved_epoch,
            ":frame": "ICRS",
            ":source": resolver_source,
            ":status": "ACTIVE",
            ":aliases": aliases,
            ":now": now,
        },
    )

    # Write PRIMARY NameMapping
    _table.put_item(
        Item={
            "PK": f"NAME#{normalized_candidate_name}",
            "SK": f"NOVA#{nova_id}",
            "entity_type": "NameMapping",
            "schema_version": _SCHEMA_VERSION,
            "name_raw": candidate_name,
            "name_normalized": normalized_candidate_name,
            "name_kind": "PRIMARY",
            "nova_id": nova_id,
            "source": "INGESTION",
            "created_at": now,
            "updated_at": now,
        }
    )

    # Write ALIAS NameMapping items for each SIMBAD alias
    alias_count = 0
    for alias_raw in aliases:
        normalized_alias = re.sub(r"\s+", " ", alias_raw.strip().lower())
        if not normalized_alias:
            continue
        if normalized_alias == normalized_candidate_name:
            continue
        _table.put_item(
            Item={
                "PK": f"NAME#{normalized_alias}",
                "SK": f"NOVA#{nova_id}",
                "entity_type": "NameMapping",
                "schema_version": _SCHEMA_VERSION,
                "name_raw": alias_raw,
                "name_normalized": normalized_alias,
                "name_kind": "ALIAS",
                "nova_id": nova_id,
                "source": "SIMBAD",
                "created_at": now,
                "updated_at": now,
            }
        )
        alias_count += 1

    logger.info(
        "Minimal nova metadata upserted",
        extra={"nova_id": nova_id, "alias_count": alias_count},
    )
    return {"nova_id": nova_id}


@tracer.capture_method
def _upsert_alias_for_existing_nova(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Add the candidate name as an ALIAS NameMapping for an existing nova.

    Called when coordinate match is DUPLICATE (< 2"). Does not create a
    new nova_id — the existing matched_nova_id is used.

    Returns:
        nova_id — the existing nova_id the alias was added to
    """
    candidate_name: str = event["candidate_name"]
    normalized_candidate_name: str = event["normalized_candidate_name"]
    nova_id: str = event["nova_id"]
    now = _now()

    _table.put_item(
        Item={
            "PK": f"NAME#{normalized_candidate_name}",
            "SK": f"NOVA#{nova_id}",
            "entity_type": "NameMapping",
            "schema_version": _SCHEMA_VERSION,
            "name_raw": candidate_name,
            "name_normalized": normalized_candidate_name,
            "name_kind": "ALIAS",
            "nova_id": nova_id,
            "source": "INGESTION",
            "created_at": now,
            "updated_at": now,
        }
    )

    logger.info(
        "Alias upserted for existing nova",
        extra={"nova_id": nova_id},
    )
    return {"nova_id": nova_id}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _angular_separation_arcsec(
    ra1_deg: float,
    dec1_deg: float,
    ra2_deg: float,
    dec2_deg: float,
) -> float:
    """
    Compute angular separation between two ICRS coordinates in arcseconds.

    Uses the haversine formula for numerical stability at small separations.
    Inputs are in degrees; output is in arcseconds.
    """
    ra1 = math.radians(ra1_deg)
    dec1 = math.radians(dec1_deg)
    ra2 = math.radians(ra2_deg)
    dec2 = math.radians(dec2_deg)

    delta_ra = ra2 - ra1
    delta_dec = dec2 - dec1

    a = math.sin(delta_dec / 2) ** 2 + math.cos(dec1) * math.cos(dec2) * math.sin(delta_ra / 2) ** 2
    sep_rad = 2 * math.asin(math.sqrt(a))
    return math.degrees(sep_rad) * 3600.0


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "NormalizeCandidateName": _normalize_candidate_name,
    "CheckExistingNovaByName": _check_existing_nova_by_name,
    "CheckExistingNovaByCoordinates": _check_existing_nova_by_coordinates,
    "CreateNovaId": _create_nova_id,
    "UpsertMinimalNovaMetadata": _upsert_minimal_nova_metadata,
    "UpsertAliasForExistingNova": _upsert_alias_for_existing_nova,
}
