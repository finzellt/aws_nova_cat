"""catalog.json artifact generator (DESIGN-003 §11).

Generates the global catalog artifact consumed by the homepage stats bar,
the catalog table, and the search page.  Unlike per-nova artifacts, this
is scoped to the *entire* catalog — it carries a summary record for every
ACTIVE nova and aggregate statistics.

Generated once per sweep, after all per-nova artifacts are processed
(§4.4 step 4).  Because it must include every ACTIVE nova — not just
those in the current sweep batch — it reads from both in-process state
and DynamoDB.

Input sources (§11.2):
    In-memory sweep results — ``NovaResult`` list from the Fargate task.
    Main table — DDB Scan of all Nova items with ``status == "ACTIVE"``.

Output:
    ADR-014 ``catalog.json`` schema (``schema_version "1.1"`` per §11.9).

Schema amendments (§11.9):
    1. ``discovery_year`` replaced by ``discovery_date`` (``str | None``).
    2. ``schema_version`` bumped from ``"1.0"`` to ``"1.1"``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from boto3.dynamodb.conditions import Attr  # type: ignore[import-untyped]

from contracts.models.regeneration import NovaResult
from generators.shared import format_coordinates, generated_at_timestamp

_logger = logging.getLogger("artifact_generator")

_SCHEMA_VERSION = "1.1"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_catalog_json(
    nova_results: list[NovaResult],
    table: Any,
) -> dict[str, Any]:
    """Generate the ``catalog.json`` artifact.

    Parameters
    ----------
    nova_results
        Per-nova result list accumulated by the Fargate task during the
        sweep.  Only results with ``success=True`` are used as overlays;
        failed novae fall back to DDB values.
    table
        boto3 DynamoDB Table resource for the main NovaCat table.

    Returns
    -------
    dict[str, Any]
        Complete ``catalog.json`` artifact conforming to ADR-014 §11.5
        (schema version ``"1.1"``).
    """
    # Step 1 — Build in-memory overlay from succeeded sweep results.
    sweep_overlay = _build_sweep_overlay(nova_results)

    # Step 2 — Scan all ACTIVE Nova items from DDB.
    active_novae = _scan_active_novae(table)

    # Step 3 — Merge: DDB as base, in-memory as overlay (§11.3).
    records = _merge_records(active_novae, sweep_overlay)

    # Step 4 — Sort: spectra_count desc, primary_name asc (§11.6).
    records.sort(key=lambda r: (-r["spectra_count"], r["primary_name"]))

    # Step 5 — Compute stats block (§11.4).
    stats = _compute_stats(records)

    return {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at_timestamp(),
        "stats": stats,
        "novae": records,
    }


# ---------------------------------------------------------------------------
# Sweep overlay
# ---------------------------------------------------------------------------


def _build_sweep_overlay(
    nova_results: list[NovaResult],
) -> dict[str, _SweepCounts]:
    """Build a lookup of in-memory counts for succeeded novae.

    Only novae with ``success=True`` are included.  Failed novae are
    excluded — their catalog entries use DDB values (§11.3).
    """
    overlay: dict[str, _SweepCounts] = {}
    for result in nova_results:
        if not result.success:
            continue
        overlay[result.nova_id] = _SweepCounts(
            spectra_count=result.spectra_count or 0,
            photometry_count=result.photometry_count or 0,
            references_count=result.references_count or 0,
            has_sparkline=result.has_sparkline or False,
        )
    return overlay


class _SweepCounts:
    """Lightweight container for in-memory observation counts."""

    __slots__ = (
        "has_sparkline",
        "photometry_count",
        "references_count",
        "spectra_count",
    )

    def __init__(
        self,
        *,
        spectra_count: int,
        photometry_count: int,
        references_count: int,
        has_sparkline: bool,
    ) -> None:
        self.spectra_count = spectra_count
        self.photometry_count = photometry_count
        self.references_count = references_count
        self.has_sparkline = has_sparkline


# ---------------------------------------------------------------------------
# DDB Scan
# ---------------------------------------------------------------------------


def _scan_active_novae(table: Any) -> list[dict[str, Any]]:
    """Paginated Scan of all ACTIVE Nova items (§11.2).

    Returns the raw DDB items.  Pagination is handled for correctness
    even though a single page suffices at MVP scale (<1,000 novae).
    """
    items: list[dict[str, Any]] = []
    scan_kwargs: dict[str, Any] = {
        "FilterExpression": Attr("status").eq("ACTIVE") & Attr("SK").eq("NOVA"),
    }

    while True:
        response: dict[str, Any] = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))

        last_key = response.get("LastEvaluatedKey")
        if last_key is None:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    _logger.info(
        "DDB Scan complete",
        extra={"active_novae": len(items), "phase": "catalog_scan"},
    )
    return items


# ---------------------------------------------------------------------------
# Record assembly (§11.3)
# ---------------------------------------------------------------------------


def _merge_records(
    active_novae: list[dict[str, Any]],
    sweep_overlay: dict[str, _SweepCounts],
) -> list[dict[str, Any]]:
    """Merge DDB items with in-memory sweep results.

    For each ACTIVE nova:
    - If it was swept successfully: overlay in-memory counts onto the
      DDB item's metadata.
    - Otherwise: use DDB item values as-is (§11.3).

    Novae missing required coordinates are excluded with an error log
    (§11.7).
    """
    records: list[dict[str, Any]] = []

    for item in active_novae:
        nova_id: str = item.get("nova_id", item.get("PK", ""))

        # Validate required coordinates (P-1, §5.8, §11.7).
        ra_deg = item.get("ra_deg")
        dec_deg = item.get("dec_deg")
        if ra_deg is None or dec_deg is None:
            _logger.error(
                "ACTIVE nova missing coordinates — excluded from catalog",
                extra={"nova_id": nova_id, "phase": "catalog_merge"},
            )
            continue

        # Resolve counts: in-memory overlay for succeeded swept novae,
        # DDB values for everything else (§11.3).
        overlay = sweep_overlay.get(nova_id)
        if overlay is not None:
            spectra_count = overlay.spectra_count
            photometry_count = overlay.photometry_count
            references_count = overlay.references_count
            has_sparkline = overlay.has_sparkline
        else:
            spectra_count = _to_int(item.get("spectra_count", 0))
            photometry_count = _to_int(item.get("photometry_count", 0))
            references_count = _to_int(item.get("references_count", 0))
            has_sparkline = bool(item.get("has_sparkline", False))

        # Coordinate formatting (§5.3).
        ra_str, dec_str = format_coordinates(
            float(_to_decimal(ra_deg)),
            float(_to_decimal(dec_deg)),
        )

        # Output mapping (§11.5).
        records.append(
            {
                "nova_id": nova_id,
                "primary_name": item.get("primary_name", ""),
                "aliases": list(item.get("aliases", [])),
                "ra": ra_str,
                "dec": dec_str,
                "discovery_date": item.get("discovery_date"),
                "spectra_count": spectra_count,
                "photometry_count": photometry_count,
                "references_count": references_count,
                "has_sparkline": has_sparkline,
            }
        )

    return records


# ---------------------------------------------------------------------------
# Stats block (§11.4)
# ---------------------------------------------------------------------------


def _compute_stats(records: list[dict[str, Any]]) -> dict[str, int]:
    """Compute aggregate statistics across all catalog records.

    All values are sums across the final merged record list — not
    independent queries.  ``references_count`` is intentionally excluded
    from the stats block (§11.4).
    """
    nova_count = len(records)
    spectra_total = sum(r["spectra_count"] for r in records)
    photometry_total = sum(r["photometry_count"] for r in records)

    return {
        "nova_count": nova_count,
        "spectra_count": spectra_total,
        "photometry_count": photometry_total,
    }


# ---------------------------------------------------------------------------
# Type coercion helpers
# ---------------------------------------------------------------------------


def _to_int(value: object) -> int:
    """Coerce a DynamoDB value (possibly ``Decimal``) to ``int``."""
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, int | float):
        return int(value)
    return 0


def _to_decimal(value: object) -> Decimal:
    """Coerce a numeric value to ``Decimal`` for coordinate conversion."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
