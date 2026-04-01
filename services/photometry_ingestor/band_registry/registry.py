# services/photometry_ingestor/band_registry/registry.py

"""Band registry module for NovaCat (ADR-017 Decision 8).

Loads ``band_registry.json`` once at module import time and exposes a
minimal read-only API for resolving filter strings to canonical band
identities.

Public API
----------
  lookup_band_id(alias)    — exact case-sensitive alias → band_id, or None
  get_entry(band_id)       — full BandRegistryEntry for a band_id, or None
  is_excluded(band_id)     — True if the band exists and is excluded
  list_all_entries()       — shallow copy of all entries

``BandRegistryEntry`` is the only public type exported by this module.
It is a frozen Pydantic model mirroring the JSON schema from ADR-017
Decision 3, extended with two derived fields that consumers need at
ingestion time:

  spectral_coord_value_unit — derived from ``regime`` at load time
  bandpass_width            — mapped from ``fwhm`` in the JSON

No other module should parse ``band_registry.json`` directly.  This
module is the single read path.

Versioning (ADR-017 Decision 9)
--------------------------------
The JSON carries a top-level ``_schema_version`` (semver).  On a major
version mismatch this module raises ``RuntimeError`` at import time
rather than silently operating against an incompatible schema.  Minor
and patch mismatches are accepted without error.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from contracts.models.entities import SpectralCoordUnit

# ---------------------------------------------------------------------------
# Schema version guard
# ---------------------------------------------------------------------------

# The major version this module was written against.  Bump when a
# breaking change to the JSON entry schema requires coordinated code
# updates (ADR-017 Decision 9).
_SUPPORTED_MAJOR_VERSION: int = 1

# ---------------------------------------------------------------------------
# BandRegistryEntry — public Pydantic model
# ---------------------------------------------------------------------------

# Calibration sub-models


class _CalibrationSystem(BaseModel):
    """Zero-point calibration data for one photometric system."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    zero_point_flux_lambda: float | None = None
    zero_point_flux_nu: float | None = None
    zeropoint_type: str | None = None
    photcal_id: str | None = None


class _Calibration(BaseModel):
    """Vega / AB / ST calibration block."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    vega: _CalibrationSystem | None = None
    ab: _CalibrationSystem | None = None
    st: _CalibrationSystem | None = None


class BandRegistryEntry(BaseModel):
    """A single entry in the NovaCat band registry (ADR-017 Decision 3).

    Frozen at construction time; treat as immutable.

    All fields mirror the JSON schema exactly, with two additions:

    ``spectral_coord_unit``
        Derived from ``regime`` at load time.  Provides the correct
        ``SpectralCoordUnit`` for ``PhotometryRow.spectral_coord_unit``
        without requiring each caller to re-derive it.  ``None`` for
        regimes that have no well-defined spectral unit at MVP scale
        (currently none — all supported regimes map to a unit).

    ``bandpass_width``
        Mapped from ``fwhm`` in the JSON.  ``None`` when ``fwhm`` is
        null (sparse entries and excluded entries).
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    # ── Core identity ────────────────────────────────────────────────────
    band_id: str = Field(..., min_length=1)
    svo_filter_id: str | None = None
    band_name: str | None = None
    regime: str | None = None

    # ── Instrument provenance ────────────────────────────────────────────
    detector_type: str | None = None
    observatory_facility: str | None = None
    instrument: str | None = None

    # ── Aliases ──────────────────────────────────────────────────────────
    aliases: list[str] = Field(default_factory=list)

    # ── Exclusion ────────────────────────────────────────────────────────
    excluded: bool = False
    exclusion_reason: str | None = None

    # ── Spectral data (SVO-sourced, Angstrom) ────────────────────────────
    lambda_eff: float | None = None
    lambda_pivot: float | None = None
    lambda_min: float | None = None
    lambda_max: float | None = None
    fwhm: float | None = None
    effective_width: float | None = None

    # ── Calibration ──────────────────────────────────────────────────────
    calibration: _Calibration = Field(default_factory=_Calibration)

    # ── Disambiguation hints (reserved for ADR-018) ──────────────────────
    disambiguation_hints: dict[str, Any] = Field(default_factory=dict)

    # ── Derived fields (populated by _build_entry, not from JSON) ────────
    spectral_coord_unit: SpectralCoordUnit | None = None
    bandpass_width: float | None = None


# ---------------------------------------------------------------------------
# Regime → SpectralCoordUnit derivation
# ---------------------------------------------------------------------------

_REGIME_TO_UNIT: dict[str, SpectralCoordUnit] = {
    "optical": SpectralCoordUnit.angstrom,
    "uv": SpectralCoordUnit.angstrom,
    "nir": SpectralCoordUnit.angstrom,
    "mir": SpectralCoordUnit.angstrom,
    "fir": SpectralCoordUnit.angstrom,
    "radio": SpectralCoordUnit.ghz,
    "xray": SpectralCoordUnit.kev,
    "gamma": SpectralCoordUnit.mev,
}


def _derive_spectral_coord_unit(regime: str | None) -> SpectralCoordUnit | None:
    if regime is None:
        return None
    return _REGIME_TO_UNIT.get(regime.lower())


# ---------------------------------------------------------------------------
# JSON loading and alias index construction
# ---------------------------------------------------------------------------

_REGISTRY_PATH = Path(__file__).parent / "band_registry.json"


def _load_registry(
    path: Path,
) -> tuple[dict[str, BandRegistryEntry], dict[str, str]]:
    """Load ``band_registry.json`` and return (entry_index, alias_index).

    entry_index: band_id → BandRegistryEntry
    alias_index: alias   → band_id  (case-sensitive)

    Raises:
        RuntimeError: On major schema version mismatch or structural
            violations (missing 'bands' key, duplicate band_id, duplicate
            alias, band_id not first alias).
        FileNotFoundError: If the JSON file is absent from the package.
    """
    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

    # Version guard
    schema_version: str = raw.get("_schema_version", "0.0.0")
    try:
        major = int(schema_version.split(".")[0])
    except (ValueError, IndexError) as exc:
        raise RuntimeError(
            f"band_registry.json: cannot parse _schema_version {schema_version!r}"
        ) from exc
    if major != _SUPPORTED_MAJOR_VERSION:
        raise RuntimeError(
            f"band_registry.json major version {major} is not supported by this "
            f"module (expected {_SUPPORTED_MAJOR_VERSION}).  Update registry.py "
            "to handle the new schema."
        )

    bands_raw: list[dict[str, Any]] = raw.get("bands", [])
    if not isinstance(bands_raw, list):
        raise RuntimeError("band_registry.json: 'bands' must be a list")

    entry_index: dict[str, BandRegistryEntry] = {}
    alias_index: dict[str, str] = {}

    for i, raw_entry in enumerate(bands_raw):
        band_id: str = raw_entry.get("band_id", "")
        if not band_id:
            raise RuntimeError(f"band_registry.json: entry at index {i} has no band_id")

        if band_id in entry_index:
            raise RuntimeError(f"band_registry.json: duplicate band_id {band_id!r} at index {i}")

        aliases: list[str] = raw_entry.get("aliases", [])
        if not aliases:
            raise RuntimeError(f"band_registry.json: entry {band_id!r} has an empty aliases list")
        if aliases[0] != band_id:
            raise RuntimeError(
                f"band_registry.json: entry {band_id!r}: first alias must equal "
                f"band_id, got {aliases[0]!r}"
            )

        for alias in aliases:
            if alias in alias_index:
                raise RuntimeError(
                    f"band_registry.json: duplicate alias {alias!r} found on "
                    f"entry {band_id!r} (already owned by {alias_index[alias]!r})"
                )
            alias_index[alias] = band_id

        # Derive fields not present in the JSON
        regime: str | None = raw_entry.get("regime")
        spectral_coord_unit = _derive_spectral_coord_unit(regime)
        bandpass_width: float | None = raw_entry.get("fwhm")

        entry = BandRegistryEntry.model_validate(
            {
                **raw_entry,
                "spectral_coord_unit": spectral_coord_unit,
                "bandpass_width": bandpass_width,
            }
        )
        entry_index[band_id] = entry

    return entry_index, alias_index


# Module-level singletons — loaded once at import time.
_ENTRY_INDEX: dict[str, BandRegistryEntry]
_ALIAS_INDEX: dict[str, str]
_ENTRY_INDEX, _ALIAS_INDEX = _load_registry(_REGISTRY_PATH)

# ---------------------------------------------------------------------------
# Public API (ADR-017 Decision 8)
# ---------------------------------------------------------------------------


def lookup_band_id(alias: str) -> str | None:
    """Return the band_id for an exact case-sensitive alias match, or None."""
    return _ALIAS_INDEX.get(alias)


def get_entry(band_id: str) -> BandRegistryEntry | None:
    """Return the full registry entry for a band_id, or None if not found."""
    return _ENTRY_INDEX.get(band_id)


def is_excluded(band_id: str) -> bool:
    """Return True if band_id exists in the registry and is marked excluded."""
    entry = _ENTRY_INDEX.get(band_id)
    return entry is not None and entry.excluded


def list_all_entries() -> list[BandRegistryEntry]:
    """Return a shallow copy of all registry entries in load order."""
    return list(_ENTRY_INDEX.values())
