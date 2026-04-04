"""references.json artifact generator (DESIGN-003 §6).

Generates the per-nova references artifact consumed by the references
table on the nova detail page.  Fetched independently of ``nova.json``
to allow the metadata region to render before the references table is
populated.

Input sources (§6.2):
    Main table — NovaReference link items (``NOVAREF#<bibcode>``)
    Main table — Reference global items (``REFERENCE#<bibcode>``)

Output:
    ADR-014 ``references.json`` schema (``schema_version "1.0"``).

Side effects on *nova_context*:
    ``references_count``  — int, number of references in the artifact.
    ``references_output`` — list[dict], full output records for the
                            bundle generator (§10).
"""

from __future__ import annotations

import logging
from typing import Any

from boto3.dynamodb.conditions import Key

from generators.shared import generated_at_timestamp

_logger = logging.getLogger("artifact_generator")

_ADS_BASE_URL = "https://ui.adsabs.harvard.edu/abs/"
_BATCH_GET_LIMIT = 100  # DynamoDB BatchGetItem ceiling per request
_MISSING_YEAR_SORT_KEY = 9999  # §6.5: missing year sorts to end
_SCHEMA_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_references_json(
    nova_id: str,
    table: Any,
    dynamodb_resource: Any,
    nova_context: dict[str, Any],
) -> dict[str, Any]:
    """Generate the ``references.json`` artifact for a nova.

    Parameters
    ----------
    nova_id
        Nova UUID string.
    table
        boto3 DynamoDB Table resource for the main NovaCat table.
    dynamodb_resource
        boto3 DynamoDB service resource (for ``BatchGetItem``).
    nova_context
        Mutable dict accumulating per-nova state across generators.

    Returns
    -------
    dict[str, Any]
        Complete ``references.json`` artifact conforming to ADR-014.
    """
    # Step 1 — Query NovaReference link items.
    nova_refs = _query_nova_references(nova_id, table)

    if not nova_refs:
        _logger.info(
            "No references found for nova",
            extra={"nova_id": nova_id, "phase": "generate_references"},
        )
        return _finalize(nova_id, [], nova_context)

    # Step 2 — Batch-fetch the corresponding Reference entities.
    bibcodes = [ref["bibcode"] for ref in nova_refs]
    ref_items = _batch_get_references(
        bibcodes,
        table.table_name,
        dynamodb_resource,
        nova_id,
    )

    # Step 3 — Build output records, skipping orphaned links.
    records: list[dict[str, Any]] = []
    for bibcode in bibcodes:
        ref_item = ref_items.get(bibcode)
        if ref_item is None:
            _logger.warning(
                "Orphaned NovaReference — no Reference entity found",
                extra={"nova_id": nova_id, "bibcode": bibcode},
            )
            continue
        records.append(_build_record(ref_item))

    # Step 4 — Sort: year ascending, bibcode tiebreaker (§6.3).
    records.sort(
        key=lambda r: (
            r["year"] if r["year"] is not None else _MISSING_YEAR_SORT_KEY,
            r["bibcode"],
        )
    )

    return _finalize(nova_id, records, nova_context)


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------


def _query_nova_references(
    nova_id: str,
    table: Any,
) -> list[dict[str, Any]]:
    """Query all ``NOVAREF#`` items for *nova_id*, handling pagination."""
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": (Key("PK").eq(nova_id) & Key("SK").begins_with("NOVAREF#")),
    }
    while True:
        response: dict[str, Any] = table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _batch_get_references(
    bibcodes: list[str],
    table_name: str,
    dynamodb_resource: Any,
    nova_id: str,
) -> dict[str, dict[str, Any]]:
    """Batch-fetch Reference items, returning a dict keyed by bibcode.

    Pages through ``BatchGetItem`` in chunks of 100 and retries any
    unprocessed keys (DynamoDB throttling).
    """
    result: dict[str, dict[str, Any]] = {}

    for i in range(0, len(bibcodes), _BATCH_GET_LIMIT):
        chunk = bibcodes[i : i + _BATCH_GET_LIMIT]
        keys: list[dict[str, str]] = [{"PK": f"REFERENCE#{bc}", "SK": "METADATA"} for bc in chunk]
        request_items: dict[str, Any] = {table_name: {"Keys": keys}}

        while request_items:
            response: dict[str, Any] = dynamodb_resource.batch_get_item(
                RequestItems=request_items,
            )
            for item in response.get("Responses", {}).get(table_name, []):
                bc: str | None = item.get("bibcode")
                if bc is not None:
                    result[bc] = dict(item)

            # Retry any keys that were throttled.
            unprocessed = response.get("UnprocessedKeys", {})
            request_items = unprocessed if unprocessed else {}

    fetched = len(result)
    requested = len(bibcodes)
    if fetched < requested:
        _logger.info(
            "Some references not found in batch fetch",
            extra={
                "nova_id": nova_id,
                "requested": requested,
                "fetched": fetched,
            },
        )

    return result


# ---------------------------------------------------------------------------
# Record construction
# ---------------------------------------------------------------------------


def _build_record(ref_item: dict[str, Any]) -> dict[str, Any]:
    """Map a Reference DDB item to an ADR-014 reference record."""
    bibcode: str = ref_item["bibcode"]
    year: int | None = ref_item.get("year")

    if year is None:
        _logger.warning(
            "Reference missing year field — will sort to end",
            extra={"bibcode": bibcode},
        )

    return {
        "bibcode": bibcode,
        "title": ref_item.get("title"),
        "authors": ref_item.get("authors", []),
        "year": year,
        "doi": ref_item.get("doi"),
        "arxiv_id": ref_item.get("arxiv_id"),
        "ads_url": f"{_ADS_BASE_URL}{bibcode}",
    }


# ---------------------------------------------------------------------------
# Finalization
# ---------------------------------------------------------------------------


def _finalize(
    nova_id: str,
    records: list[dict[str, Any]],
    nova_context: dict[str, Any],
) -> dict[str, Any]:
    """Build the artifact dict and update *nova_context*."""
    nova_context["references_count"] = len(records)
    nova_context["references_output"] = records

    return {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at_timestamp(),
        "nova_id": nova_id,
        "references": records,
    }
