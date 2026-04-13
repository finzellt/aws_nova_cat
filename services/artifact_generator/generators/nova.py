"""nova.json artifact generator (DESIGN-003 §5).

Generates the per-nova metadata artifact powering the header region of
the nova detail page.  Carries core object properties and observation
counts.  References are intentionally excluded (delivered separately
in ``references.json``).

Input sources (§5.2):
    Per-nova context — ``nova_item`` (loaded by the Fargate per-nova
    loop), plus ``spectra_count`` and ``photometry_count`` from the
    upstream spectra and photometry generators.

Output:
    ADR-014 ``nova.json`` schema (``schema_version "1.0"``).

This is the simplest generator: no DDB queries, no S3 reads, no
complex computation.  All inputs come from ``nova_context``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from generators.shared import discovery_date_to_mjd, format_coordinates, generated_at_timestamp

_logger = logging.getLogger("artifact_generator")

_SCHEMA_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_nova_json(
    nova_id: str,
    nova_context: dict[str, Any],
) -> dict[str, Any]:
    """Generate the ``nova.json`` artifact for a nova.

    Parameters
    ----------
    nova_id
        Nova UUID string.
    nova_context
        Must contain ``nova_item`` (the Nova DDB item dict) and
        observation counts from upstream generators (``spectra_count``,
        ``photometry_count``).

    Returns
    -------
    dict[str, Any]
        Complete ``nova.json`` artifact conforming to ADR-014.

    Raises
    ------
    KeyError
        If ``nova_item`` is missing from *nova_context* or the Nova
        item lacks required ``ra_deg`` / ``dec_deg`` fields.
    """
    nova_item: dict[str, Any] = nova_context["nova_item"]

    # Coordinate formatting (§5.3) — required on ACTIVE Nova items (P-1).
    ra_deg = float(_to_float(nova_item["ra_deg"]))
    dec_deg = float(_to_float(nova_item["dec_deg"]))
    ra_str, dec_str = format_coordinates(ra_deg, dec_deg)

    # Discovery date pass-through (§5.3) — YYYY-MM-DD or null.
    discovery_date: str | None = nova_item.get("discovery_date")

    # Nova type (§5.3) — null until post-MVP enrichment.
    nova_type: str | None = nova_item.get("nova_type")

    # Discovery date as MJD (null when discovery_date is absent).
    discovery_date_mjd: float | None = None
    if discovery_date is not None:
        discovery_date_mjd = round(discovery_date_to_mjd(discovery_date), 1)

    # Observation counts from upstream generators (§5.4).
    spectra_count: int = nova_context.get("spectra_count", 0)
    photometry_count: int = nova_context.get("photometry_count", 0)

    # Spectral visits (distinct observing nights with spectra).
    spectral_visits: int = nova_context.get("spectral_visits", 0)

    return {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at_timestamp(),
        "nova_id": nova_id,
        "primary_name": nova_item.get("primary_name", ""),
        "aliases": nova_item.get("aliases", []),
        "ra": ra_str,
        "dec": dec_str,
        "discovery_date": discovery_date,
        "nova_type": nova_type,
        "discovery_date_mjd": discovery_date_mjd,
        "spectra_count": spectra_count,
        "photometry_count": photometry_count,
        "spectral_visits": spectral_visits,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_float(value: Any) -> float:
    """Convert a DynamoDB Decimal (or other numeric) to float."""
    if isinstance(value, Decimal):
        return float(value)
    return float(value)
