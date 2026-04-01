#!/usr/bin/env python3
"""
Seed script for band_registry.json (ADR-017, amended 2026-03-25).

Queries the SVO Filter Profile Service via ``astroquery.svo_fps`` for each
canonical band defined in ``band_specs.json`` and emits a fully-populated
``band_registry.json`` conforming to the ADR-017 entry schema.

Usage::

    python tools/filter_band_reg/seed_band_registry.py
    python tools/filter_band_reg/seed_band_registry.py --output /path/to/band_registry.json
    python tools/filter_band_reg/seed_band_registry.py --dry-run
    python tools/filter_band_reg/seed_band_registry.py --specs /alt/band_specs.json

By default the script loads band definitions from ``band_specs.json`` in the
same directory as this script.  Use ``--specs`` to override.

The output file is intended for OPERATOR REVIEW before being committed to::

    services/photometry_ingestor/band_registry/band_registry.json

Design notes
------------
* Band definitions live in ``band_specs.json``, NOT in this script.  Data and
  code are separated so that alias ownership, SVO candidates, and entry
  metadata can be reviewed and diffed independently of the script logic.
  See ``band_specs.json`` header comments for the alias ownership rule.

* SVO is the definitional authority (ADR-017 §3, Decision 1, SVO-first
  principle).  This script never hardcodes spectral field values; all
  lambda_eff, fwhm, zero-point etc. values are drawn from the live API
  response.

* ``get_filter_list(facility, instrument)`` returns one row per calibration
  system (Vega / AB / ST) for each filter in that facility+instrument
  combination.  Calls are cached by (facility, instrument) pair so a second
  band in the same system (e.g. Bessell B after Bessell V) issues no
  additional HTTP request.

* If an SVO lookup fails for all candidate filter IDs, a sparse entry is
  emitted and a WARNING is logged.  Sparse entries have ``null`` for all
  SVO-derived fields.  Review them before committing.

* Intentionally-sparse entries (those with ``"sparse": true`` in
  band_specs.json, e.g. ``Generic_K``, ``Open``) bypass SVO lookup entirely
  and do not trigger the failure log.

* ``photometric_system`` is abolished per ADR-019 Decision 1 and does not
  appear in the output registry entries.

* ``band_id`` follows the two-track naming convention from the ADR-017
  amendment: instrument-specific entries use
  ``{Facility}_{Instrument}_{BandLabel}`` (with redundancy collapsing),
  Generic fallbacks use ``Generic_{BandLabel}``.

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
from pathlib import Path
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

# ── Default band_specs.json path (co-located with this script) ──────────────
_DEFAULT_SPECS_PATH = Path(__file__).parent / "band_specs.json"


# ---------------------------------------------------------------------------
# Band specification dataclass
# ---------------------------------------------------------------------------


@dataclass
class BandSpec:
    """
    Static, human-curated metadata for one canonical NovaCat band.

    ``svo_candidates`` is a list of SVO filter IDs
    (``"Facility/Instrument.Band"``) to try in order; the first one that
    returns a non-empty table wins.  An empty list means "no SVO entry exists"
    and a sparse entry will be generated.

    ``photometric_system`` is abolished per ADR-019 Decision 1 and does not
    appear here.
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
    * ``facility``   — passed to ``SvoFps.get_filter_list()`` as the first
                        argument
    * ``instrument`` — passed as the ``instrument=`` keyword (may be ``None``)

    ``facility`` and ``instrument`` are kept explicit and separate from
    ``filter_id`` because the filterID path component after ``/`` is not
    always the instrument name (e.g. ``SLOAN/SDSS.u`` has facility=SLOAN,
    instrument=None).
    """

    # Intentionally-sparse entries (Generic_*, Open) bypass SVO lookup
    sparse: bool = False

    excluded: bool = False
    exclusion_reason: str | None = None

    # Optional wavelength hint for sparse entries (Å)
    lambda_eff_hint: float | None = None


# ---------------------------------------------------------------------------
# Band specs loader
# ---------------------------------------------------------------------------


def load_band_specs(path: Path) -> list[BandSpec]:
    """
    Load ``BandSpec`` objects from a ``band_specs.json`` file.

    Expected file format::

        {
          "bands": [
            {
              "band_id": "Generic_V",
              "aliases": ["Generic_V", "Johnson_V", "V", "Johnson V", "Vmag"],
              "band_name": "V",
              "regime": "optical",
              "observatory_facility": null,
              "instrument": null,
              "svo_candidates": [
                {"filter_id": "HCT/HFOSC.Bessell_V", "facility": "HCT",
                 "instrument": "HFOSC"}
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
    raw = json.loads(path.read_text(encoding="utf-8"))
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
    seen_aliases: dict[str, str] = {}
    for spec in specs:
        if spec.band_id in seen_ids:
            raise ValueError(f"{path}: duplicate band_id '{spec.band_id}'")
        seen_ids.add(spec.band_id)

        if spec.aliases[0] != spec.band_id:
            raise ValueError(
                f"{path}: band_id '{spec.band_id}' must be first element of "
                f"aliases, got '{spec.aliases[0]}'"
            )

        for alias in spec.aliases:
            if alias in seen_aliases:
                raise ValueError(
                    f"{path}: duplicate alias '{alias}' on '{spec.band_id}' "
                    f"(already on '{seen_aliases[alias]}')"
                )
            seen_aliases[alias] = spec.band_id

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
    Return the rows of ``table`` whose ``filterID`` column matches
    ``filter_id``.

    SVO returns one row per calibration system (Vega / AB / ST), so the
    result may have 1–3 rows.  Returns ``None`` if no rows match.
    """
    if "filterID" not in table.colnames:
        log.warning(
            "SVO table missing 'filterID' column; columns: %s",
            table.colnames,
        )
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
    ``SvoFps.get_filter_list()``; they must not be inferred from
    ``filter_id`` because the filterID path components do not reliably
    encode the API parameters (e.g. ``SLOAN/SDSS.u`` → facility=SLOAN,
    instrument=None).
    """
    full_table = _fetch_facility_table(facility, instrument)
    if full_table is None:
        return None

    rows = _filter_table_for_band(full_table, filter_id)
    if rows is None or len(rows) == 0:
        log.debug(
            "SVO: no rows matched filterID=%s in facility table",
            filter_id,
        )
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


# ---------------------------------------------------------------------------
# Entry builders
# ---------------------------------------------------------------------------


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
            log.debug(
                "Unknown calibration system in PhotCalID=%s, skipping",
                photcal_id,
            )
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

    SVO convention: 0 = energy counter (bolometer), 1 = photon counter
    (CCD/etc.)
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

    lambda_eff = _round_sig(
        _to_float(_col(row0, "WavelengthEff", "WavelengthMean"))
    )
    lambda_pivot = _round_sig(_to_float(_col(row0, "WavelengthPivot")))
    lambda_min = _round_sig(_to_float(_col(row0, "WavelengthMin")))
    lambda_max = _round_sig(_to_float(_col(row0, "WavelengthMax")))
    fwhm = _round_sig(_to_float(_col(row0, "FWHM")))
    effective_width = _round_sig(_to_float(_col(row0, "WidthEff")))
    detector_type = _detector_type_str(_col(row0, "DetectorType", "Detector"))

    # The filterID in the response confirms which SVO entry was matched.
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

    Used for intentionally-sparse entries (e.g. ``Generic_K``, ``Open``)
    and as a fallback when all SVO lookup candidates fail.
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
# Summary and main
# ---------------------------------------------------------------------------


def _print_summary(
    entries: list[dict[str, Any]],
    failures: list[str],
    output_path: str,
    specs_path: str,
) -> None:
    """Print a human-readable post-run summary to stderr."""
    n_instrument = sum(
        1
        for e in entries
        if not e["band_id"].startswith("Generic_") and not e.get("excluded")
    )
    n_generic = sum(
        1 for e in entries if e["band_id"].startswith("Generic_")
    )
    n_excluded = sum(1 for e in entries if e.get("excluded"))
    n_sparse = sum(
        1
        for e in entries
        if e.get("lambda_eff") is None and not e.get("excluded")
    )

    print("\n" + "=" * 60, file=sys.stderr)
    print(f"Band Registry Seed — {len(entries)} entries", file=sys.stderr)
    print(f"  Specs loaded from:   {specs_path}", file=sys.stderr)
    print(f"  Instrument-specific: {n_instrument}", file=sys.stderr)
    print(f"  Generic fallbacks:   {n_generic}", file=sys.stderr)
    print(f"  Excluded:            {n_excluded}", file=sys.stderr)
    print(f"  Sparse (no SVO):     {n_sparse}", file=sys.stderr)
    if failures:
        print(
            f"\n  ⚠ SVO lookup failures ({len(failures)}):",
            file=sys.stderr,
        )
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
        help=(
            "Path to band_specs.json file "
            f"(default: {_DEFAULT_SPECS_PATH})"
        ),
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

    # ── Load band specs ─────────────────────────────────────────────────
    specs_path = Path(args.specs) if args.specs else _DEFAULT_SPECS_PATH
    if not specs_path.exists():
        log.error("Band specs file not found: %s", specs_path)
        return 1

    try:
        specs = load_band_specs(specs_path)
    except ValueError as exc:
        log.error("Invalid band specs file: %s", exc)
        return 1

    # ── Fetch SVO data and build entries ────────────────────────────────
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
            "AUTO-GENERATED by seed_band_registry.py from "
            f"{specs_path.name} — operator review required before "
            "commit to band_registry/. "
            "band_id convention: ADR-017 amendment 2026-03-25. "
            "Alias ownership: ADR-017 amendment correction 2026-04-01."
        ),
        "bands": entries,
    }

    _print_summary(entries, failures, args.output, str(specs_path))

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
