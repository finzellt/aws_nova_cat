#!/usr/bin/env python3
"""
Seed script for band_registry.json (ADR-017, amended 2026-03-25).

Queries the SVO Filter Profile Service via ``astroquery.svo_fps`` for each
canonical band defined in this script and emits a fully-populated
``band_registry.json`` conforming to the ADR-017 entry schema.

Usage::

    python scripts/exploratory/seed_band_registry.py
    python scripts/exploratory/seed_band_registry.py --output /path/to/band_registry.json
    python scripts/exploratory/seed_band_registry.py --dry-run   # prints summary, no file
    python scripts/exploratory/seed_band_registry.py --specs band_specs.json

The output file is intended for OPERATOR REVIEW before being committed to::

    services/photometry_ingestor/band_registry/band_registry.json

Design notes
------------
* SVO is the definitional authority (ADR-017 §3, Decision 1, SVO-first principle).
  This script never hardcodes spectral field values; all lambda_eff, fwhm, zero-point
  etc. values are drawn from the live API response.

* ``get_filter_list(facility, instrument)`` returns one row per calibration system
  (Vega / AB / ST) for each filter in that facility+instrument combination.  Calls are
  cached by (facility, instrument) pair so a second band in the same system (e.g.
  Bessell B after Bessell V) issues no additional HTTP request.

* If an SVO lookup fails for all candidate filter IDs, a sparse entry is emitted and
  a WARNING is logged.  Sparse entries have ``null`` for all SVO-derived fields.
  Review them before committing.

* The two sparse entries that are *intentionally* sparse (``Generic_K``, ``Open``)
  do not trigger the failure log.

* ``photometric_system`` is abolished per ADR-019 Decision 1 and does not appear
  in ``BandSpec`` or the output registry entries.

* ``band_id`` follows the two-track naming convention from the ADR-017 amendment:
  instrument-specific entries use ``{Facility}_{Instrument}_{BandLabel}`` (with
  redundancy collapsing), Generic fallbacks use ``Generic_{BandLabel}``.

Requirements
------------
    pip install astroquery astropy numpy

Operator tooling — no CI requirements (mypy / ruff not enforced).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Lazy import so the script at least imports cleanly if astroquery is absent.
# ---------------------------------------------------------------------------
try:
    from astropy.table import Table as AstropyTable
    from astroquery.svo_fps import SvoFps
except ImportError as _err:  # pragma: no cover
    print(f"ERROR: {_err}\n  pip install astroquery astropy", file=sys.stderr)
    sys.exit(1)


log = logging.getLogger("seed_band_registry")

# Speed of light in Å s⁻¹ (used for Jy → erg cm⁻² s⁻¹ Å⁻¹ conversion)
_C_AA_S: float = 2.99792458e18

# ── ADR-017 schema version ──────────────────────────────────────────────────
REGISTRY_SCHEMA_VERSION = "1.1.0"

# ---------------------------------------------------------------------------
# Band specification table
# ---------------------------------------------------------------------------


@dataclass
class BandSpec:
    """
    Static, human-curated metadata for one canonical NovaCat band.

    ``svo_candidates`` is a list of SVO filter IDs (``"Facility/Instrument.Band"``)
    to try in order; the first one that returns a non-empty table wins.  An empty
    list means "no SVO entry exists" and a sparse entry will be generated.

    ``photometric_system`` is abolished per ADR-019 Decision 1 and does not appear
    here.
    """

    band_id: str
    """NovaCat canonical band ID (ADR-017 Decision 2, amended)."""

    aliases: list[str]
    """Alias list; band_id must be the first element (ADR-017 Decision 3)."""

    band_name: str | None
    regime: str | None
    """Provisional; must be reconciled with ADR-019 vocabulary."""

    observatory_facility: str | None
    instrument: str | None

    svo_candidates: list[dict[str, str | None]] = field(default_factory=list)
    """
    SVO lookup candidates to try in priority order.  Each entry is a dict with:

    * ``filter_id``  — the SVO filterID string (e.g. ``"SLOAN/SDSS.u"``)
    * ``facility``   — passed to ``SvoFps.get_filter_list()`` as the first argument
    * ``instrument`` — passed as the ``instrument=`` keyword (may be ``None``)

    ``facility`` and ``instrument`` are kept explicit and separate from
    ``filter_id`` because the filterID path component after ``/`` is not always
    the instrument name (e.g. ``SLOAN/SDSS.u`` has facility=SLOAN, instrument=None).
    """

    # Intentionally-sparse entries (Generic_*, Open) bypass SVO lookup
    sparse: bool = False

    excluded: bool = False
    exclusion_reason: str | None = None

    # Optional wavelength hint for sparse entries (Å)
    lambda_eff_hint: float | None = None


# ---------------------------------------------------------------------------
# Band definitions — 22 instrument-specific + 8 Generic fallbacks + 1 excluded
#
# ADR-017 amendment (2026-03-25): band_id follows two-track convention.
# Instrument-specific: {Facility}_{Instrument}_{BandLabel} (redundancy-collapsed).
# Generic fallback: Generic_{BandLabel}.
#
# photometric_system is abolished (ADR-019 Decision 1).
#
# svo_candidates format: {"filter_id": str, "facility": str, "instrument": str|None}
# facility/instrument are passed *directly* to SvoFps.get_filter_list(); they
# are NOT derived from filter_id to avoid assumption errors.
# ---------------------------------------------------------------------------
BAND_SPECS: list[BandSpec] = [
    # ── Johnson-Cousins (Bessell reference profiles) ───────────────────────
    # Generic/Johnson and Generic/Cousins do not exist in the SVO database.
    # OAF/Bessell and HCT/HFOSC are used instead: Bessell (1990) defines the
    # canonical UBVRI transmission curves that operationally ARE the
    # Johnson-Cousins system in modern CCD photometry.
    # observatory_facility and instrument now reflect the actual SVO profile
    # source, not a generic label.
    BandSpec(
        band_id="HCT_HFOSC_Bessell_U",
        aliases=["HCT_HFOSC_Bessell_U", "Johnson_U", "U"],
        band_name="U",
        regime="optical",
        observatory_facility="HCT",
        instrument="HFOSC",
        svo_candidates=[
            {"filter_id": "HCT/HFOSC.Bessell_U", "facility": "HCT", "instrument": "HFOSC"},
            {"filter_id": "HCT/HFOSC.Bessell_U", "facility": "HCT", "instrument": "HFOSC"},
        ],
    ),
    BandSpec(
        band_id="HCT_HFOSC_Bessell_B",
        aliases=["HCT_HFOSC_Bessell_B", "Johnson_B", "B"],
        band_name="B",
        regime="optical",
        observatory_facility="HCT",
        instrument="HFOSC",
        svo_candidates=[
            {"filter_id": "HCT/HFOSC.Bessell_B", "facility": "HCT", "instrument": "HFOSC"},
            {"filter_id": "OAF/Bessell.B", "facility": "OAF", "instrument": "Bessell"},
        ],
    ),
    BandSpec(
        band_id="HCT_HFOSC_Bessell_V",
        aliases=["HCT_HFOSC_Bessell_V", "Johnson_V", "V", "Johnson V", "Vmag"],
        band_name="V",
        regime="optical",
        observatory_facility="HCT",
        instrument="HFOSC",
        svo_candidates=[
            {"filter_id": "HCT/HFOSC.Bessell_V", "facility": "HCT", "instrument": "HFOSC"},
            {"filter_id": "OAF/Bessell.V", "facility": "OAF", "instrument": "Bessell"},
        ],
    ),
    BandSpec(
        band_id="HCT_HFOSC_Bessell_R",
        aliases=["HCT_HFOSC_Bessell_R", "Cousins_R", "R"],
        band_name="R",
        regime="optical",
        observatory_facility="HCT",
        instrument="HFOSC",
        svo_candidates=[
            {"filter_id": "HCT/HFOSC.Bessell_R", "facility": "HCT", "instrument": "HFOSC"},
            {"filter_id": "OAF/Bessell.R", "facility": "OAF", "instrument": "Bessell"},
        ],
    ),
    BandSpec(
        band_id="HCT_HFOSC_Bessell_I",
        aliases=["HCT_HFOSC_Bessell_I", "Cousins_I", "I"],
        band_name="I",
        regime="optical",
        observatory_facility="HCT",
        instrument="HFOSC",
        svo_candidates=[
            {"filter_id": "HCT/HFOSC.Bessell_I", "facility": "HCT", "instrument": "HFOSC"},
            {"filter_id": "OAF/Bessell.I", "facility": "OAF", "instrument": "Bessell"},
        ],
    ),
    # ── Sloan unprimed ─────────────────────────────────────────────────────
    # facility=SLOAN, instrument=None  (NOT "SDSS" — the path component in the
    # filterID is not the API instrument parameter for these entries).
    BandSpec(
        band_id="SLOAN_SDSS_u",
        aliases=["SLOAN_SDSS_u", "Sloan_u", "u"],
        band_name="u",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.u", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="SLOAN_SDSS_g",
        aliases=["SLOAN_SDSS_g", "Sloan_g", "g"],
        band_name="g",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.g", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="SLOAN_SDSS_r",
        aliases=["SLOAN_SDSS_r", "Sloan_r", "r"],
        band_name="r",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.r", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="SLOAN_SDSS_i",
        aliases=["SLOAN_SDSS_i", "Sloan_i", "i"],
        band_name="i",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.i", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="SLOAN_SDSS_z",
        aliases=["SLOAN_SDSS_z", "Sloan_z", "z_s"],
        band_name="z",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.z", "facility": "SLOAN", "instrument": None},
        ],
    ),
    # ── Sloan primed ───────────────────────────────────────────────────────
    # SVO naming for primed SDSS bands is uncertain; multiple candidates listed.
    # Operator must verify the matched filterID in the output.
    BandSpec(
        band_id="SLOAN_SDSS_up",
        aliases=["SLOAN_SDSS_up", "Sloan_up", "up"],
        band_name="u'",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.up", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="SLOAN_SDSS_gp",
        aliases=["SLOAN_SDSS_gp", "Sloan_gp", "gp"],
        band_name="g'",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.gp", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="SLOAN_SDSS_rp",
        aliases=["SLOAN_SDSS_rp", "Sloan_rp", "rp"],
        band_name="r'",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.rp", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="SLOAN_SDSS_ip",
        aliases=["SLOAN_SDSS_ip", "Sloan_ip", "ip"],
        band_name="i'",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.ip", "facility": "SLOAN", "instrument": None},
        ],
    ),
    # ── 2MASS ──────────────────────────────────────────────────────────────
    # Redundancy collapse: 2MASS/2MASS.J → 2MASS_J (facility = instrument)
    BandSpec(
        band_id="2MASS_J",
        aliases=["2MASS_J", "J"],
        band_name="J",
        regime="nir",
        observatory_facility="2MASS",
        instrument=None,
        svo_candidates=[
            {"filter_id": "2MASS/2MASS.J", "facility": "2MASS", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="2MASS_H",
        aliases=["2MASS_H", "H"],
        band_name="H",
        regime="nir",
        observatory_facility="2MASS",
        instrument=None,
        svo_candidates=[
            {"filter_id": "2MASS/2MASS.H", "facility": "2MASS", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="2MASS_Ks",
        aliases=["2MASS_Ks", "Ks"],
        band_name="Ks",
        regime="nir",
        observatory_facility="2MASS",
        instrument=None,
        svo_candidates=[
            {"filter_id": "2MASS/2MASS.Ks", "facility": "2MASS", "instrument": None},
        ],
    ),
    # ── Swift UVOT ─────────────────────────────────────────────────────────
    BandSpec(
        band_id="Swift_UVOT_UVW1",
        aliases=["Swift_UVOT_UVW1", "UVOT_UVW1", "uvw1"],
        band_name="UVW1",
        regime="uv",
        observatory_facility="Swift",
        instrument="UVOT",
        svo_candidates=[
            {"filter_id": "Swift/UVOT.UVW1", "facility": "Swift", "instrument": "UVOT"},
        ],
    ),
    BandSpec(
        band_id="Swift_UVOT_UVW2",
        aliases=["Swift_UVOT_UVW2", "UVOT_UVW2", "uvw2"],
        band_name="UVW2",
        regime="uv",
        observatory_facility="Swift",
        instrument="UVOT",
        svo_candidates=[
            {"filter_id": "Swift/UVOT.UVW2", "facility": "Swift", "instrument": "UVOT"},
        ],
    ),
    BandSpec(
        band_id="Swift_UVOT_UVM2",
        aliases=["Swift_UVOT_UVM2", "UVOT_UVM2", "uvm2", "UVM2"],
        band_name="UVM2",
        regime="uv",
        observatory_facility="Swift",
        instrument="UVOT",
        svo_candidates=[
            {"filter_id": "Swift/UVOT.UVM2", "facility": "Swift", "instrument": "UVOT"},
        ],
    ),
    # ── HST ────────────────────────────────────────────────────────────────
    # Prefer WFC3_UVIS2; fall back to WFPC2 if not found.
    BandSpec(
        band_id="HST_WFC3_UVIS2_F555W",
        aliases=["HST_WFC3_UVIS2_F555W", "HST_F555W", "F555W"],
        band_name="F555W",
        regime="optical",
        observatory_facility="HST",
        instrument="WFC3_UVIS2",
        svo_candidates=[
            {"filter_id": "HST/WFC3_UVIS2.F555W", "facility": "HST", "instrument": "WFC3_UVIS2"},
            {"filter_id": "HST/WFPC2.F555W", "facility": "HST", "instrument": "WFPC2"},
        ],
    ),
    # ── Spitzer IRAC ───────────────────────────────────────────────────────
    BandSpec(
        band_id="Spitzer_IRAC_I1",
        aliases=["Spitzer_IRAC_I1", "Spitzer_IRAC1", "[3.6]", "3.6"],
        band_name="[3.6]",
        regime="mir",
        observatory_facility="Spitzer",
        instrument="IRAC",
        svo_candidates=[
            {"filter_id": "Spitzer/IRAC.I1", "facility": "Spitzer", "instrument": "IRAC"},
        ],
    ),
    BandSpec(
        band_id="Spitzer_IRAC_I2",
        aliases=["Spitzer_IRAC_I2", "Spitzer_IRAC2", "[4.5]", "4.5"],
        band_name="[4.5]",
        regime="mir",
        observatory_facility="Spitzer",
        instrument="IRAC",
        svo_candidates=[
            {"filter_id": "Spitzer/IRAC.I2", "facility": "Spitzer", "instrument": "IRAC"},
        ],
    ),
    # ── Generic fallbacks (ADR-018 disambiguation targets) ─────────────────
    # Strictly low-confidence fallbacks. Sparse by definition.
    BandSpec(
        band_id="Generic_U",
        aliases=["Generic_U"],
        band_name="U",
        regime="optical",
        observatory_facility=None,
        instrument=None,
        sparse=True,
    ),
    BandSpec(
        band_id="Generic_B",
        aliases=["Generic_B"],
        band_name="B",
        regime="optical",
        observatory_facility=None,
        instrument=None,
        sparse=True,
    ),
    BandSpec(
        band_id="Generic_V",
        aliases=["Generic_V"],
        band_name="V",
        regime="optical",
        observatory_facility=None,
        instrument=None,
        sparse=True,
    ),
    BandSpec(
        band_id="Generic_R",
        aliases=["Generic_R"],
        band_name="R",
        regime="optical",
        observatory_facility=None,
        instrument=None,
        sparse=True,
    ),
    BandSpec(
        band_id="Generic_I",
        aliases=["Generic_I"],
        band_name="I",
        regime="optical",
        observatory_facility=None,
        instrument=None,
        sparse=True,
    ),
    BandSpec(
        band_id="Generic_J",
        aliases=["Generic_J"],
        band_name="J",
        regime="nir",
        observatory_facility=None,
        instrument=None,
        sparse=True,
    ),
    BandSpec(
        band_id="Generic_H",
        aliases=["Generic_H"],
        band_name="H",
        regime="nir",
        observatory_facility=None,
        instrument=None,
        sparse=True,
    ),
    # λ_eff hint ≈ 2190 nm = 21900 Å (community-standard K-band centre).
    # Operator must annotate this entry with a source justification before commit.
    BandSpec(
        band_id="Generic_K",
        aliases=["Generic_K", "K"],
        band_name="K",
        regime="nir",
        observatory_facility=None,
        instrument=None,
        sparse=True,
        lambda_eff_hint=21900.0,
    ),
    # ── Excluded ───────────────────────────────────────────────────────────
    BandSpec(
        band_id="Open",
        aliases=["Open"],
        band_name=None,
        regime=None,
        observatory_facility=None,
        instrument=None,
        sparse=True,
        excluded=True,
        exclusion_reason="unfiltered/open observation",
    ),
]

# ---------------------------------------------------------------------------
# External spec file loader
# ---------------------------------------------------------------------------


def load_band_specs_from_file(path: str) -> list[BandSpec]:
    """
    Load ``BandSpec`` objects from an external JSON file.

    Expected file format::

        {
          "bands": [
            {
              "band_id": "HCT_HFOSC_Bessell_V",
              "aliases": ["HCT_HFOSC_Bessell_V", "Johnson_V", "V"],
              "band_name": "V",
              "regime": "optical",
              "observatory_facility": "HCT",
              "instrument": "HFOSC",
              "svo_candidates": [
                {"filter_id": "OAF/Bessell.V", "facility": "OAF", "instrument": "Bessell"}
              ],
              "sparse": false,
              "excluded": false,
              "exclusion_reason": null,
              "lambda_eff_hint": null
            }
          ]
        }

    All fields are required; use ``null`` for inapplicable values.  The
    ``svo_candidates`` list may be empty for intentionally-sparse entries
    (set ``"sparse": true`` in that case).

    Raises ``ValueError`` with a descriptive message on any schema violation.
    """
    import json as _json
    from pathlib import Path

    raw = _json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "bands" not in raw:
        raise ValueError(f"{path}: expected top-level object with 'bands' key")

    specs: list[BandSpec] = []
    for item in raw["bands"]:
        if not isinstance(item, dict):
            raise ValueError(f"{path}: each band must be a dict")
        missing = {"band_id", "aliases", "band_name", "regime"} - set(item.keys())
        if missing:
            raise ValueError(f"{path}: band missing required keys: {missing}")
        specs.append(
            BandSpec(
                band_id=item["band_id"],
                aliases=item["aliases"],
                band_name=item.get("band_name"),
                regime=item.get("regime"),
                observatory_facility=item.get("observatory_facility"),
                instrument=item.get("instrument"),
                svo_candidates=item.get("svo_candidates", []),
                sparse=bool(item.get("sparse", False)),
                excluded=bool(item.get("excluded", False)),
                exclusion_reason=item.get("exclusion_reason"),
                lambda_eff_hint=item.get("lambda_eff_hint"),
            )
        )

    if not specs:
        raise ValueError(f"{path}: 'bands' list is empty")

    # Uniqueness guard
    seen_ids: set[str] = set()
    for spec in specs:
        if spec.band_id in seen_ids:
            raise ValueError(f"{path}: duplicate band_id '{spec.band_id}'")
        seen_ids.add(spec.band_id)

    log.info("Loaded %d band specs from %s", len(specs), path)
    return specs


# ---------------------------------------------------------------------------
# SVO query helpers
# ---------------------------------------------------------------------------


# Cache keyed by (facility, instrument) → Table or None (None = fetch failed)
_svo_cache: dict[tuple[str, str | None], AstropyTable | None] = {}


def _fetch_facility_table(facility: str, instrument: str | None) -> AstropyTable | None:
    """
    Return the SVO filter table for (facility, instrument), using cache.

    Returns ``None`` if the network call fails or the table is empty.
    """
    key = (facility, instrument)
    if key in _svo_cache:
        return _svo_cache[key]

    log.debug("SVO: fetching facility=%s instrument=%s", facility, instrument)
    try:
        table: AstropyTable = SvoFps.get_filter_list(facility, instrument=instrument)
        if table is None or len(table) == 0:
            log.warning(
                "SVO returned empty table for facility=%s instrument=%s",
                facility,
                instrument,
            )
            _svo_cache[key] = None
        else:
            log.debug(
                "SVO: received %d rows for facility=%s instrument=%s",
                len(table),
                facility,
                instrument,
            )
            _svo_cache[key] = table
    except Exception as exc:
        log.warning(
            "SVO request failed for facility=%s instrument=%s: %s",
            facility,
            instrument,
            exc,
        )
        _svo_cache[key] = None

    return _svo_cache[key]


def _filter_table_for_band(
    table: AstropyTable,
    filter_id: str,
) -> AstropyTable | None:
    """
    Return the rows of ``table`` whose ``filterID`` column matches ``filter_id``.

    SVO returns one row per calibration system (Vega / AB / ST), so the result
    may have 1–3 rows.  Returns ``None`` if no rows match.
    """
    if "filterID" not in table.colnames:
        log.warning("SVO table missing 'filterID' column; columns: %s", table.colnames)
        return None

    mask = table["filterID"] == filter_id
    subset = table[mask]
    if len(subset) == 0:
        return None
    return subset


def _query_svo_for_filter(
    filter_id: str,
    facility: str,
    instrument: str | None,
) -> AstropyTable | None:
    """
    Retrieve SVO rows for a specific filter using explicit lookup parameters.

    ``facility`` and ``instrument`` are passed directly to
    ``SvoFps.get_filter_list()``; they must not be inferred from ``filter_id``
    because the filterID path components do not reliably encode the API
    parameters (e.g. ``SLOAN/SDSS.u`` → facility=SLOAN, instrument=None).
    """
    full_table = _fetch_facility_table(facility, instrument)
    if full_table is None:
        return None

    rows = _filter_table_for_band(full_table, filter_id)
    if rows is None or len(rows) == 0:
        log.debug("SVO: no rows matched filterID=%s in facility table", filter_id)
        return None

    return rows


# ---------------------------------------------------------------------------
# Value extraction helpers
# ---------------------------------------------------------------------------


def _col(row: Any, *names: str) -> Any:
    """
    Return the value of the first column name found in ``row``, or ``None``.

    Tries each name in turn to accommodate minor SVO schema variations.
    """
    for name in names:
        try:
            val = row[name]
            return val
        except (KeyError, IndexError):
            continue
    return None


def _to_float(val: Any) -> float | None:
    """Convert a possibly-masked / NaN SVO value to float, or None."""
    if val is None:
        return None
    # astropy masked values
    if hasattr(val, "mask") and val.mask:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if np.isnan(f) or np.isinf(f):
        return None
    return f


def _to_str(val: Any) -> str | None:
    """Convert a possibly-masked SVO value to str, or None."""
    if val is None:
        return None
    if hasattr(val, "mask") and val.mask:
        return None
    s = str(val).strip()
    sentinel = {"", "nan", "none", "--", "n/a", "null"}
    return s if s.lower() not in sentinel else None


def _jy_to_flam(zero_point_jy: float, lambda_pivot_aa: float) -> float | None:
    """
    Convert zero-point flux density from Jy to erg cm⁻² s⁻¹ Å⁻¹.

    Formula:  f_λ = f_ν * c / λ²  (with unit conversions)
              f_λ [erg cm⁻² s⁻¹ Å⁻¹] = f_ν [Jy] * 1e-23 * c [Å/s] / λ² [Å²]
    """
    if lambda_pivot_aa is None or lambda_pivot_aa <= 0:
        return None
    return zero_point_jy * 1e-23 * _C_AA_S / (lambda_pivot_aa**2)


def _round_sig(val: float | None, sig: int = 6) -> float | None:
    """Round to ``sig`` significant figures, or return None."""
    if val is None:
        return None
    if val == 0:
        return 0.0
    from math import floor, log10

    magnitude = floor(log10(abs(val)))
    factor = 10 ** (sig - 1 - magnitude)
    return round(val * factor) / factor


def _build_calibration(rows: AstropyTable) -> dict[str, Any]:
    """
    Build the calibration block from SVO rows.

    SVO returns one row per calibration system.  We aggregate into
    {vega, ab, st} sub-blocks.
    """
    cal: dict[str, Any] = {"vega": None, "ab": None, "st": None}

    for row in rows:
        photcal_id = _to_str(_col(row, "PhotCalID"))
        zp_type = _to_str(_col(row, "MagSys"))
        zp_jy = _to_float(_col(row, "ZeroPoint"))
        lambda_pivot = _to_float(_col(row, "WavelengthPivot"))

        if photcal_id is None:
            continue

        # Determine which system this row represents
        pid_lower = photcal_id.lower()
        if "vega" in pid_lower:
            key = "vega"
        elif "/ab" in pid_lower:
            key = "ab"
        elif "/st" in pid_lower:
            key = "st"
        else:
            log.debug("Unknown calibration system in PhotCalID=%s, skipping", photcal_id)
            continue

        zp_flam = None
        if zp_jy is not None and lambda_pivot is not None:
            zp_flam = _round_sig(_jy_to_flam(zp_jy, lambda_pivot))

        cal[key] = {
            "zero_point_flux_lambda": zp_flam,
            "zero_point_flux_nu": _round_sig(zp_jy),
            "zeropoint_type": zp_type,
            "photcal_id": photcal_id,
        }

    return cal


def _detector_type_str(raw: Any) -> str | None:
    """
    Convert SVO DetectorType integer to a human-readable string.

    SVO convention: 0 = energy counter (bolometer), 1 = photon counter (CCD/etc.)
    """
    s = _to_str(raw)
    if s == "0":
        return "energy"
    if s == "1":
        return "photon"
    # Pass through non-numeric strings as-is (future-proof)
    return s


def _build_entry_from_svo(spec: BandSpec, rows: AstropyTable) -> dict[str, Any]:
    """Build a fully-populated ADR-017 entry from live SVO data."""
    # Spectral fields are identical across all calibration rows; use row 0.
    row0 = rows[0]

    lambda_eff = _round_sig(_to_float(_col(row0, "WavelengthEff", "WavelengthMean")))
    lambda_pivot = _round_sig(_to_float(_col(row0, "WavelengthPivot")))
    lambda_min = _round_sig(_to_float(_col(row0, "WavelengthMin")))
    lambda_max = _round_sig(_to_float(_col(row0, "WavelengthMax")))
    fwhm = _round_sig(_to_float(_col(row0, "FWHM")))
    effective_width = _round_sig(_to_float(_col(row0, "WidthEff")))
    detector_type = _detector_type_str(_col(row0, "DetectorType", "Detector"))

    # The filterID in the response confirms which SVO entry was actually matched.
    svo_filter_id = _to_str(_col(row0, "filterID", "FilterID"))

    calibration = _build_calibration(rows)

    return {
        "band_id": spec.band_id,
        "svo_filter_id": svo_filter_id,
        "band_name": spec.band_name,
        "regime": spec.regime,
        "detector_type": detector_type,
        "observatory_facility": spec.observatory_facility,
        "instrument": spec.instrument,
        "aliases": spec.aliases,
        "excluded": spec.excluded,
        "exclusion_reason": spec.exclusion_reason,
        "lambda_eff": lambda_eff,
        "lambda_pivot": lambda_pivot,
        "lambda_min": lambda_min,
        "lambda_max": lambda_max,
        "fwhm": fwhm,
        "effective_width": effective_width,
        "calibration": calibration,
        "disambiguation_hints": {},
    }


def _build_sparse_entry(spec: BandSpec) -> dict[str, Any]:
    """
    Build a sparse ADR-017 entry with null SVO-derived fields.

    Used for intentionally-sparse entries (``Generic_*``, ``Open``) and as a
    fallback when all SVO lookup candidates fail.
    """
    return {
        "band_id": spec.band_id,
        "svo_filter_id": None,
        "band_name": spec.band_name,
        "regime": spec.regime,
        "detector_type": None,
        "observatory_facility": spec.observatory_facility,
        "instrument": spec.instrument,
        "aliases": spec.aliases,
        "excluded": spec.excluded,
        "exclusion_reason": spec.exclusion_reason,
        "lambda_eff": spec.lambda_eff_hint,
        "lambda_pivot": None,
        "lambda_min": None,
        "lambda_max": None,
        "fwhm": None,
        "effective_width": None,
        "calibration": {"vega": None, "ab": None, "st": None},
        "disambiguation_hints": {},
    }


# ---------------------------------------------------------------------------
# Per-band fetch logic
# ---------------------------------------------------------------------------


def _fetch_band(spec: BandSpec) -> tuple[dict[str, Any], bool]:
    """
    Fetch SVO data for ``spec`` and return ``(entry, svo_ok)``.

    ``svo_ok`` is ``True`` if SVO data was obtained (or the entry is
    intentionally sparse).  ``False`` means all candidate lookups failed
    and the entry is a fallback sparse entry — flag for operator review.
    """
    # Intentionally-sparse entries bypass SVO entirely
    if spec.sparse or not spec.svo_candidates:
        entry = _build_sparse_entry(spec)
        return entry, True

    for candidate in spec.svo_candidates:
        filter_id: str = candidate["filter_id"]
        facility: str = candidate["facility"]
        instrument: str | None = candidate.get("instrument")
        log.info(
            "%-30s  trying SVO: %s (facility=%s instrument=%s)",
            spec.band_id,
            filter_id,
            facility,
            instrument,
        )
        rows = _query_svo_for_filter(filter_id, facility, instrument)
        if rows is not None:
            entry = _build_entry_from_svo(spec, rows)
            matched_id = entry.get("svo_filter_id") or filter_id
            log.info(
                "%-30s  ✓ matched %s  (%d cal row%s)",
                spec.band_id,
                matched_id,
                len(rows),
                "s" if len(rows) != 1 else "",
            )
            return entry, True

    # All candidates exhausted
    filter_ids = [c["filter_id"] for c in spec.svo_candidates]
    log.warning(
        "%-30s  ✗ SVO lookup failed (tried %s) — sparse entry emitted",
        spec.band_id,
        filter_ids,
    )
    entry = _build_sparse_entry(spec)
    return entry, False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _print_summary(
    entries: list[dict[str, Any]],
    failures: list[str],
    output_path: str,
    specs: list[BandSpec],
) -> None:
    """Print a human-readable post-run summary to stderr."""
    n_instrument = sum(
        1 for e in entries if not e["band_id"].startswith("Generic_") and not e.get("excluded")
    )
    n_generic = sum(1 for e in entries if e["band_id"].startswith("Generic_"))
    n_excluded = sum(1 for e in entries if e.get("excluded"))
    n_sparse = sum(1 for e in entries if e.get("lambda_eff") is None and not e.get("excluded"))

    print("\n" + "=" * 60, file=sys.stderr)
    print(f"Band Registry Seed — {len(entries)} entries", file=sys.stderr)
    print(f"  Instrument-specific: {n_instrument}", file=sys.stderr)
    print(f"  Generic fallbacks:   {n_generic}", file=sys.stderr)
    print(f"  Excluded:            {n_excluded}", file=sys.stderr)
    print(f"  Sparse (no SVO):     {n_sparse}", file=sys.stderr)
    if failures:
        print(f"\n  ⚠ SVO lookup failures ({len(failures)}):", file=sys.stderr)
        for band_id in failures:
            print(f"    - {band_id}", file=sys.stderr)
    print(f"\n  Output: {output_path}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Seed band_registry.json from SVO Filter Profile Service"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="band_registry.json",
        help="Output file path (default: band_registry.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary without writing file",
    )
    parser.add_argument(
        "--specs",
        default=None,
        help="Path to external band specs JSON file (overrides built-in BAND_SPECS)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-8s %(message)s",
        stream=sys.stderr,
    )

    specs = BAND_SPECS
    if args.specs:
        specs = load_band_specs_from_file(args.specs)

    entries: list[dict[str, Any]] = []
    failures: list[str] = []

    for spec in specs:
        entry, svo_ok = _fetch_band(spec)
        entries.append(entry)
        if not svo_ok:
            failures.append(spec.band_id)

    registry = {
        "_schema_version": REGISTRY_SCHEMA_VERSION,
        "_generated_at": datetime.now(UTC).isoformat(),
        "_note": (
            "AUTO-GENERATED by scripts/exploratory/seed_band_registry.py — "
            "operator review required before commit to band_registry/. "
            "band_id convention: ADR-017 amendment 2026-03-25."
        ),
        "bands": entries,
    }

    _print_summary(entries, failures, args.output, specs)

    if args.dry_run:
        log.info("Dry run — no file written")
        return 0

    output_json = json.dumps(registry, indent=2, ensure_ascii=False)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(output_json + "\n")

    log.info("Wrote %s (%d bytes)", args.output, len(output_json))
    return 0


if __name__ == "__main__":
    sys.exit(main())
