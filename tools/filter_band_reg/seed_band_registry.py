#!/usr/bin/env python3
"""
Seed script for band_registry.json (ADR-017).

Queries the SVO Filter Profile Service via ``astroquery.svo_fps`` for each
canonical band defined in this script and emits a fully-populated
``band_registry.json`` conforming to the ADR-017 entry schema.

Usage::

    python seed_band_registry.py
    python seed_band_registry.py --output /path/to/band_registry.json
    python seed_band_registry.py --dry-run   # prints summary, no file

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
  Johnson B after Johnson V) issues no additional HTTP request.

* If an SVO lookup fails for all candidate filter IDs, a sparse entry is emitted and
  a WARNING is logged.  Sparse entries have ``null`` for all SVO-derived fields.
  Review them before committing.

* The two sparse entries that are *intentionally* sparse (``Generic_K``, ``Open``)
  do not trigger the failure log.

* Regime vocabulary used here (optical / uv / nir / mir) is provisional and must be
  reconciled with ADR-019 before the registry is committed.

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
REGISTRY_SCHEMA_VERSION = "1.0.0"

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
    """

    band_id: str
    """NovaCat canonical band ID (ADR-017 Decision 2)."""

    aliases: list[str]
    """Alias list; band_id must be the first element (ADR-017 Decision 3)."""

    band_name: str | None
    regime: str | None

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

    # Intentionally-sparse entries (Generic_K, Open) bypass SVO lookup
    sparse: bool = False

    excluded: bool = False
    exclusion_reason: str | None = None

    # Optional wavelength hint for sparse entries (Å)
    lambda_eff_hint: float | None = None


# ---------------------------------------------------------------------------
# Band definitions — 24 physical bands + 1 excluded entry
#
# svo_candidates format: {"filter_id": str, "facility": str, "instrument": str|None}
# facility/instrument are passed *directly* to SvoFps.get_filter_list(); they
# are NOT derived from filter_id to avoid assumption errors (e.g. SLOAN/SDSS.u
# has facility=SLOAN, instrument=None — the "SDSS" in the path is not the API
# instrument parameter).
# ---------------------------------------------------------------------------
BAND_SPECS: list[BandSpec] = [
    # ── Johnson-Cousins ────────────────────────────────────────────────────
    # Generic/Johnson and Generic/Cousins do not exist in the SVO database.
    # OAF/Bessell and HCT/HFOSC are used instead: Bessell (1990) defines the
    # canonical UBVRI transmission curves that operationally ARE the
    # Johnson-Cousins system in modern CCD photometry.  The photometric_system
    # field on the registry entry remains "Johnson"/"Cousins" to reflect the
    # scientific system identity; svo_filter_id records the SVO profile used.
    BandSpec(
        band_id="Johnson_U",
        aliases=["Johnson_U", "U"],
        band_name="U",
        regime="optical",
        observatory_facility="Generic",
        instrument=None,
        svo_candidates=[
            {"filter_id": "OAF/Bessell.U", "facility": "OAF", "instrument": "Bessell"},
            {"filter_id": "HCT/HFOSC.Bessell_U", "facility": "HCT", "instrument": "HFOSC"},
        ],
    ),
    BandSpec(
        band_id="Johnson_B",
        aliases=["Johnson_B", "B"],
        band_name="B",
        regime="optical",
        observatory_facility="Generic",
        instrument=None,
        svo_candidates=[
            {"filter_id": "OAF/Bessell.B", "facility": "OAF", "instrument": "Bessell"},
            {"filter_id": "HCT/HFOSC.Bessell_B", "facility": "HCT", "instrument": "HFOSC"},
        ],
    ),
    BandSpec(
        band_id="Johnson_V",
        aliases=["Johnson_V", "V"],
        band_name="V",
        regime="optical",
        observatory_facility="Generic",
        instrument=None,
        svo_candidates=[
            {"filter_id": "OAF/Bessell.V", "facility": "OAF", "instrument": "Bessell"},
            {"filter_id": "HCT/HFOSC.Bessell_V", "facility": "HCT", "instrument": "HFOSC"},
        ],
    ),
    BandSpec(
        band_id="Cousins_R",
        aliases=["Cousins_R", "R"],
        band_name="R",
        regime="optical",
        observatory_facility="Generic",
        instrument=None,
        svo_candidates=[
            {"filter_id": "OAF/Bessell.R", "facility": "OAF", "instrument": "Bessell"},
            {"filter_id": "HCT/HFOSC.Bessell_R", "facility": "HCT", "instrument": "HFOSC"},
        ],
    ),
    BandSpec(
        band_id="Cousins_I",
        aliases=["Cousins_I", "I"],
        band_name="I",
        regime="optical",
        observatory_facility="Generic",
        instrument=None,
        svo_candidates=[
            {"filter_id": "OAF/Bessell.I", "facility": "OAF", "instrument": "Bessell"},
            {"filter_id": "HCT/HFOSC.Bessell_I", "facility": "HCT", "instrument": "HFOSC"},
        ],
    ),
    # ── Sloan unprimed ─────────────────────────────────────────────────────
    # facility=SLOAN, instrument=None  (NOT "SDSS" — the path component in the
    # filterID is not the API instrument parameter for these entries).
    BandSpec(
        band_id="Sloan_u",
        aliases=["Sloan_u", "u"],
        band_name="u",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.u", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="Sloan_g",
        aliases=["Sloan_g", "g"],
        band_name="g",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.g", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="Sloan_r",
        aliases=["Sloan_r", "r"],
        band_name="r",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.r", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="Sloan_i",
        aliases=["Sloan_i", "i"],
        band_name="i",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.i", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="Sloan_z",
        aliases=["Sloan_z", "z_s"],
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
        band_id="Sloan_up",
        aliases=["Sloan_up", "up"],
        band_name="u'",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.up", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="Sloan_gp",
        aliases=["Sloan_gp", "gp"],
        band_name="g'",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.gp", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="Sloan_rp",
        aliases=["Sloan_rp", "rp"],
        band_name="r'",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.rp", "facility": "SLOAN", "instrument": None},
        ],
    ),
    BandSpec(
        band_id="Sloan_ip",
        aliases=["Sloan_ip", "ip"],
        band_name="i'",
        regime="optical",
        observatory_facility="SLOAN",
        instrument=None,
        svo_candidates=[
            {"filter_id": "SLOAN/SDSS.ip", "facility": "SLOAN", "instrument": None},
        ],
    ),
    # ── 2MASS ──────────────────────────────────────────────────────────────
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
    # ── Generic K — intentionally sparse (no SVO entry for generic K) ──────
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
    # ── Swift UVOT ─────────────────────────────────────────────────────────
    BandSpec(
        band_id="UVOT_UVW1",
        aliases=["UVOT_UVW1", "uvw1"],
        band_name="UVW1",
        regime="uv",
        observatory_facility="Swift",
        instrument="UVOT",
        svo_candidates=[
            {"filter_id": "Swift/UVOT.UVW1", "facility": "Swift", "instrument": "UVOT"},
        ],
    ),
    BandSpec(
        band_id="UVOT_UVW2",
        aliases=["UVOT_UVW2", "uvw2"],
        band_name="UVW2",
        regime="uv",
        observatory_facility="Swift",
        instrument="UVOT",
        svo_candidates=[
            {"filter_id": "Swift/UVOT.UVW2", "facility": "Swift", "instrument": "UVOT"},
        ],
    ),
    BandSpec(
        band_id="UVOT_UVM2",
        aliases=["UVOT_UVM2", "uvm2", "UVM2"],
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
        band_id="HST_F555W",
        aliases=["HST_F555W", "F555W"],
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
        band_id="Spitzer_IRAC1",
        aliases=["Spitzer_IRAC1", "[3.6]", "3.6"],
        band_name="[3.6]",
        regime="mir",
        observatory_facility="Spitzer",
        instrument="IRAC",
        svo_candidates=[
            {"filter_id": "Spitzer/IRAC.I1", "facility": "Spitzer", "instrument": "IRAC"},
        ],
    ),
    BandSpec(
        band_id="Spitzer_IRAC2",
        aliases=["Spitzer_IRAC2", "[4.5]", "4.5"],
        band_name="[4.5]",
        regime="mir",
        observatory_facility="Spitzer",
        instrument="IRAC",
        svo_candidates=[
            {"filter_id": "Spitzer/IRAC.I2", "facility": "Spitzer", "instrument": "IRAC"},
        ],
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
              "band_id": "Johnson_V",
              "aliases": ["Johnson_V", "V"],
              "photometric_system": "Johnson",
              "band_name": "V",
              "regime": "optical",
              "observatory_facility": "Generic",
              "instrument": null,
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
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)

    if not isinstance(raw, dict) or "bands" not in raw:
        raise ValueError(f"{path}: top-level object must have a 'bands' key")

    specs: list[BandSpec] = []
    for i, item in enumerate(raw["bands"]):
        ctx = f"{path}:bands[{i}]"

        def _require(key: str, _item: dict[str, Any] = item, _ctx: str = ctx) -> Any:
            if key not in _item:
                raise ValueError(f"{_ctx}: missing required field '{key}'")
            return _item[key]

        band_id: str = _require("band_id")
        aliases: list[str] = _require("aliases")
        if not isinstance(aliases, list) or not aliases:
            raise ValueError(f"{ctx} ({band_id}): 'aliases' must be a non-empty list")
        if aliases[0] != band_id:
            raise ValueError(
                f"{ctx} ({band_id}): first alias must equal band_id, got {aliases[0]!r}"
            )

        raw_candidates = _require("svo_candidates")
        if not isinstance(raw_candidates, list):
            raise ValueError(f"{ctx} ({band_id}): 'svo_candidates' must be a list")
        candidates: list[dict[str, str | None]] = []
        for j, c in enumerate(raw_candidates):
            if not isinstance(c, dict):
                raise ValueError(f"{ctx} ({band_id}): svo_candidates[{j}] must be an object")
            for req_key in ("filter_id", "facility"):
                if req_key not in c:
                    raise ValueError(f"{ctx} ({band_id}): svo_candidates[{j}] missing '{req_key}'")
            candidates.append(
                {
                    "filter_id": c["filter_id"],
                    "facility": c["facility"],
                    "instrument": c.get("instrument"),
                }
            )

        specs.append(
            BandSpec(
                band_id=band_id,
                aliases=aliases,
                photometric_system=item.get("photometric_system"),
                band_name=item.get("band_name"),
                regime=item.get("regime"),
                observatory_facility=item.get("observatory_facility"),
                instrument=item.get("instrument"),
                svo_candidates=candidates,
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

    F_λ  =  F_ν  ×  c  /  λ²

    where:
        F_ν  is in erg cm⁻² s⁻¹ Hz⁻¹  (1 Jy = 1e-23 erg cm⁻² s⁻¹ Hz⁻¹)
        c    is in Å s⁻¹               (_C_AA_S)
        λ    is in Å                   (lambda_pivot_aa)
    """
    if lambda_pivot_aa <= 0:
        return None
    return (zero_point_jy * 1e-23 * _C_AA_S) / (lambda_pivot_aa**2)


def _round_sig(val: float | None, digits: int = 6) -> float | None:
    """Round to ``digits`` significant figures, or return None."""
    if val is None:
        return None
    if val == 0.0:
        return 0.0
    from math import floor, log10

    magnitude = floor(log10(abs(val)))
    factor = 10 ** (digits - 1 - magnitude)
    return round(val * factor) / factor


# ---------------------------------------------------------------------------
# Entry builders
# ---------------------------------------------------------------------------


def _build_calibration(rows: AstropyTable) -> dict[str, Any]:
    """
    Build the ``calibration`` block from an SVO filter table (multi-row).

    SVO returns one row per photometric calibration system.  We recognise
    'Vega', 'AB', and 'ST' (case-insensitive) and map each to its sub-block.
    Unknown systems are logged and skipped.
    """
    calib: dict[str, Any] = {"vega": None, "ab": None, "st": None}

    for row in rows:
        mag_sys_raw = _to_str(_col(row, "MagSys", "MagSystem"))
        if mag_sys_raw is None:
            continue

        key = mag_sys_raw.lower()
        if key not in calib:
            log.debug("SVO: unknown MagSys '%s' — skipped", mag_sys_raw)
            continue

        zero_point_jy = _to_float(_col(row, "ZeroPoint", "ZP"))
        zp_type = _to_str(_col(row, "ZeroPointType"))
        photcal_id = _to_str(_col(row, "PhotCalID"))
        lambda_pivot_aa = _to_float(_col(row, "WavelengthPivot", "WavelengthMean", "WavelengthCen"))

        if zero_point_jy is not None and lambda_pivot_aa is not None:
            zp_flam = _round_sig(_jy_to_flam(zero_point_jy, lambda_pivot_aa))
        else:
            zp_flam = None

        calib[key] = {
            "zero_point_flux_lambda": zp_flam,
            "zero_point_flux_nu": _round_sig(zero_point_jy),
            "zeropoint_type": zp_type,
            "photcal_id": photcal_id,
        }

    return calib


def _detector_type_str(raw: Any) -> str | None:
    """
    Map SVO DetectorType code to a human-readable string.

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

    Used for intentionally-sparse entries (``Generic_K``, ``Open``) and as a
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
            "%-20s  trying SVO: %s (facility=%s instrument=%s)",
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
                "%-20s  ✓ matched %s  (%d cal row%s)",
                spec.band_id,
                matched_id,
                len(rows),
                "s" if len(rows) != 1 else "",
            )
            return entry, True

    # All candidates exhausted
    filter_ids = [c["filter_id"] for c in spec.svo_candidates]
    log.warning(
        "%-20s  ✗ SVO lookup failed (tried %s) — sparse entry emitted",
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
    total = len(entries)
    svo_ok = total - len(failures)
    sparse_by_design = sum(1 for s in specs if s.sparse or not s.svo_candidates)

    print("", file=sys.stderr)
    print("─" * 60, file=sys.stderr)
    print("  band_registry.json seed summary", file=sys.stderr)
    print("─" * 60, file=sys.stderr)
    print(f"  Total entries   : {total}", file=sys.stderr)
    print(f"  SVO-populated   : {svo_ok - sparse_by_design}", file=sys.stderr)
    print(f"  Sparse by design: {sparse_by_design}", file=sys.stderr)
    if failures:
        print(f"  SVO failures    : {len(failures)}  ← REVIEW REQUIRED", file=sys.stderr)
        for fid in failures:
            print(f"      • {fid}", file=sys.stderr)
    else:
        print("  SVO failures    : 0", file=sys.stderr)
    print("─" * 60, file=sys.stderr)
    print(f"  Output          : {output_path}", file=sys.stderr)
    print("", file=sys.stderr)
    if failures:
        print(
            "  ⚠  Sparse fallback entries were emitted for the bands above.\n"
            "     Review each entry and populate manually or fix SVO candidates\n"
            "     before committing to band_registry/.",
            file=sys.stderr,
        )
        print("", file=sys.stderr)
    print(
        "  ⚠  Regime vocabulary (optical / uv / nir / mir) is provisional.\n"
        "     Reconcile with ADR-019 before committing.",
        file=sys.stderr,
    )
    print("", file=sys.stderr)


def main(output_path: str, dry_run: bool, specs_file: str | None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-8s  %(message)s",
        stream=sys.stderr,
    )

    specs = load_band_specs_from_file(specs_file) if specs_file is not None else BAND_SPECS

    entries: list[dict[str, Any]] = []
    failures: list[str] = []

    for spec in specs:
        entry, svo_ok = _fetch_band(spec)
        entries.append(entry)
        if not svo_ok:
            failures.append(spec.band_id)

    registry: dict[str, Any] = {
        "_schema_version": REGISTRY_SCHEMA_VERSION,
        "_generated_at": datetime.now(UTC).isoformat(),
        "_note": (
            "AUTO-GENERATED by scripts/exploratory/seed_band_registry.py — "
            "operator review required before commit to band_registry/. "
            "Regime vocabulary is provisional (reconcile with ADR-019)."
        ),
        "bands": entries,
    }

    _print_summary(entries, failures, output_path, specs)

    if dry_run:
        log.info("Dry-run mode: no file written.")
        print(json.dumps(registry, indent=2, ensure_ascii=False))
        return 0

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(registry, fh, indent=2, ensure_ascii=False)
        fh.write("\n")  # POSIX-friendly trailing newline

    log.info("Wrote %d entries to %s", len(entries), output_path)
    return 1 if failures else 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output",
        default="band_registry.json",
        metavar="PATH",
        help="Output file path (default: band_registry.json in cwd)",
    )
    parser.add_argument(
        "--specs-file",
        default=None,
        metavar="PATH",
        help=(
            "JSON file of BandSpec definitions to use instead of the built-in "
            "BAND_SPECS table.  See load_band_specs_from_file() for the expected format."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print JSON to stdout instead of writing a file",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG logging (shows per-column SVO details)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    sys.exit(main(args.output, args.dry_run, args.specs_file))
