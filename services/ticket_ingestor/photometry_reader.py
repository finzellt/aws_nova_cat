"""Pure-transform photometry reader for the ticket-driven ingestion path.

Reads a headerless photometry CSV described by a PhotometryTicket, resolves
each row's filter string against an injected band registry, and constructs
PhotometryRow objects.  Returns a PhotometryReadResult containing the
successful rows and any per-row failures.

**No boto3, no DDB imports.**  All I/O (CSV reading, band registry access)
is either performed directly on the filesystem path passed in or injected as
a protocol-typed argument.  This makes the module trivially unit-testable
without any AWS mocking.

Band resolution (DESIGN-004 §6.5) uses a three-step sequence:

  1. Alias lookup: ``registry.lookup_band_id(filter_string)``
     - If matched and not excluded → canonical resolution (high confidence).
     - If matched and excluded → row failure (excluded filter).
  2. Radio frequency fuzzy match: ``registry.resolve_radio_frequency(filter_string)``
     - If the string looks like a radio frequency (e.g. "36.5 GHz"),
       finds the nearest registered radio band within ±20% tolerance.
     - If matched → synonym resolution (medium confidence).
  3. Generic fallback: ``registry.get_entry(f"Generic_{filter_string}")``
     - If present → generic_fallback resolution (low confidence).
     - If absent → row failure (unresolvable filter string).

Row ID derivation (DESIGN-004 §8.2):

  row_id = UUID(SHA-256(nova_id|epoch_raw|band_id|flux_value_raw|filename)[:16 bytes])

All five participating fields are string representations so the derivation
is deterministic regardless of float representation choices at the call site.
"""

from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from contracts.models.entities import (
    BandResolutionConfidence,
    BandResolutionType,
    DataOrigin,
    DataRights,
    FluxDensityUnit,
    PhotometryRow,
    QualityFlag,
    SpectralCoordType,
    SpectralCoordUnit,
    TimeOrigSys,
)
from contracts.models.tickets import PhotometryTicket

# ---------------------------------------------------------------------------
# Band registry protocol
# ---------------------------------------------------------------------------


class RegistryEntryLike(Protocol):
    """Structural protocol for a band registry entry.

    Mirrors the fields consumed by the photometry reader from a
    BandRegistryEntry.  The real BandRegistryEntry (contracts layer,
    forthcoming) satisfies this protocol structurally.  Tests provide a
    plain dataclass.
    """

    @property
    def band_id(self) -> str: ...

    @property
    def band_name(self) -> str | None: ...

    @property
    def regime(self) -> str: ...

    @property
    def svo_filter_id(self) -> str | None: ...

    @property
    def lambda_eff(self) -> float | None: ...

    @property
    def spectral_coord_unit(self) -> SpectralCoordUnit | None: ...

    @property
    def bandpass_width(self) -> float | None: ...


class BandRegistryProtocol(Protocol):
    """Structural protocol for the band registry module interface (ADR-017 §8)."""

    def lookup_band_id(self, alias: str) -> str | None: ...

    def get_entry(self, band_id: str) -> Any: ...

    def is_excluded(self, band_id: str) -> bool: ...

    def resolve_radio_frequency(self, filter_string: str) -> str | None: ...


# ---------------------------------------------------------------------------
# Public result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RowFailure:
    """A row that could not be transformed into a PhotometryRow.

    Attributes:
        row_number: 1-based line number in the CSV.
        reason:     Human-readable description of the failure.
        raw_row:    The original CSV fields as strings (for diagnostics).
    """

    row_number: int
    reason: str
    raw_row: list[str]


@dataclass(frozen=True)
class ResolvedRow:
    """A successfully transformed row paired with its deterministic identity key.

    Keeping row_id alongside the PhotometryRow avoids recomputing the hash
    in ddb_writer (Chunk 3b) where it becomes the DynamoDB SK.
    """

    row_id: UUID
    row: PhotometryRow


@dataclass(frozen=True)
class PhotometryReadResult:
    """Result of reading and transforming a photometry CSV.

    Attributes:
        rows:     Successfully constructed and keyed rows.
        failures: Rows that could not be transformed.
    """

    rows: list[ResolvedRow]
    failures: list[RowFailure]


# ---------------------------------------------------------------------------
# Internal types
# ---------------------------------------------------------------------------


@dataclass
class _RawFields:
    """Per-row extracted values before type conversion."""

    epoch_raw: str
    flux_value_raw: str
    flux_error_raw: str | None
    filter_string: str
    upper_limit: bool
    telescope: str | None
    observer: str | None


@dataclass(frozen=True)
class _BandResolution:
    """Result of resolving a filter string against the registry."""

    band_id: str
    resolution_type: BandResolutionType
    confidence: BandResolutionConfidence
    entry: Any  # RegistryEntryLike; typed as Any to avoid import coupling


class _RowError(Exception):
    """Internal exception for expected per-row failures.

    Distinct from ValueError / Pydantic ValidationError so that all
    anticipated failure modes can be caught uniformly at the batch level
    without masking programming errors.
    """


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def read_photometry_csv(
    csv_path: str | Path,
    ticket: PhotometryTicket,
    nova_id: UUID,
    primary_name: str,
    ra_deg: float,
    dec_deg: float,
    registry: BandRegistryProtocol,
) -> PhotometryReadResult:
    """Read a headerless photometry CSV and transform it into PhotometryRow objects.

    Args:
        csv_path:     Path to the headerless CSV data file.
        ticket:       Parsed and validated PhotometryTicket.
        nova_id:      Resolved nova UUID (from ResolveNova).
        primary_name: Resolved primary nova name.
        ra_deg:       Right ascension in decimal degrees.
        dec_deg:      Declination in decimal degrees.
        registry:     Band registry implementing BandRegistryProtocol.

    Returns:
        PhotometryReadResult with successful rows and per-row failures.
    """
    rows: list[ResolvedRow] = []
    failures: list[RowFailure] = []

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        for row_number, raw_row in enumerate(reader, start=1):
            # Skip blank / whitespace-only rows
            if not any(cell.strip() for cell in raw_row):
                continue
            try:
                row = _process_row(
                    raw_row=raw_row,
                    row_number=row_number,
                    ticket=ticket,
                    nova_id=nova_id,
                    primary_name=primary_name,
                    ra_deg=ra_deg,
                    dec_deg=dec_deg,
                    registry=registry,
                )
                rows.append(row)
            except _RowError as exc:
                failures.append(
                    RowFailure(
                        row_number=row_number,
                        reason=str(exc),
                        raw_row=list(raw_row),
                    )
                )
            except Exception as exc:  # noqa: BLE001 — broad catch is intentional
                failures.append(
                    RowFailure(
                        row_number=row_number,
                        reason=f"Unexpected error: {type(exc).__name__}: {exc}",
                        raw_row=list(raw_row),
                    )
                )

    return PhotometryReadResult(rows=rows, failures=failures)


# ---------------------------------------------------------------------------
# Per-row pipeline
# ---------------------------------------------------------------------------


def _process_row(
    raw_row: list[str],
    row_number: int,
    ticket: PhotometryTicket,
    nova_id: UUID,
    primary_name: str,
    ra_deg: float,
    dec_deg: float,
    registry: BandRegistryProtocol,
) -> ResolvedRow:
    """Transform one CSV row into a PhotometryRow.

    Raises:
        _RowError: For any expected per-row failure (excluded filter,
            unresolvable filter, type coercion error).
    """
    # 1. Extract raw fields using the ticket's column index / default pattern.
    raw = _extract_fields(raw_row, ticket, row_number)

    # 2. Convert time value to MJD.
    try:
        epoch_float = float(raw.epoch_raw)
    except ValueError as exc:
        raise _RowError(f"Cannot convert epoch to float: {raw.epoch_raw!r}") from exc
    time_mjd, time_bary_corr, time_orig_sys = _convert_time(epoch_float, ticket.time_system)

    # 3. Parse flux value and optional error.
    try:
        flux_value = float(raw.flux_value_raw)
    except ValueError as exc:
        raise _RowError(f"Cannot convert flux value to float: {raw.flux_value_raw!r}") from exc

    flux_error: float | None = None
    if raw.flux_error_raw is not None:
        try:
            flux_error = float(raw.flux_error_raw)
        except ValueError as exc:
            raise _RowError(f"Cannot convert flux error to float: {raw.flux_error_raw!r}") from exc

    # 4. Resolve filter string → band registry entry.
    band_res = _resolve_band(raw.filter_string, registry)

    # 5. Derive deterministic row_id.
    row_id = _derive_row_id(
        nova_id=nova_id,
        epoch_raw=raw.epoch_raw,
        band_id=band_res.band_id,
        flux_value_raw=raw.flux_value_raw,
        filename=ticket.data_filename,
    )

    # 6. Extract spectral coord fields from registry entry.
    entry: RegistryEntryLike = band_res.entry
    spectral_coord_value: float | None = entry.lambda_eff
    spectral_coord_unit: SpectralCoordUnit = (
        entry.spectral_coord_unit
        if entry.spectral_coord_unit is not None
        else SpectralCoordUnit.angstrom  # safe default for optical/UV/NIR corpus
    )

    # 7. Branch on wavelength regime to populate measurement fields.
    regime = ticket.wavelength_regime

    magnitude: float | None = None
    mag_err: float | None = None
    flux_density: float | None = None
    flux_density_err: float | None = None
    flux_density_unit: FluxDensityUnit | None = None

    if regime in ("optical", "uv", "nir", "mir"):
        # Magnitude regimes — current behavior.
        if not raw.upper_limit:
            magnitude = flux_value
            mag_err = flux_error
    elif regime == "radio":
        # Radio regime — flux density semantics.
        if not raw.upper_limit:
            flux_density = flux_value
            flux_density_err = flux_error
        flux_density_unit = _resolve_flux_density_unit(ticket.flux_units)
    elif regime in ("xray", "gamma"):
        raise _RowError(f"X-ray/gamma regime not yet supported: {regime!r}")
    else:
        raise _RowError(f"Unrecognised wavelength regime: {regime!r}")

    # 8. Construct and validate PhotometryRow.
    limiting_value: float | None = flux_value if raw.upper_limit else None

    return ResolvedRow(
        row_id=row_id,
        row=PhotometryRow(
            # Section 1 — Identity
            nova_id=nova_id,
            primary_name=primary_name,
            ra_deg=ra_deg,
            dec_deg=dec_deg,
            # Section 2 — Temporal
            time_mjd=time_mjd,
            time_bary_corr=time_bary_corr,
            time_orig=epoch_float,
            time_orig_sys=time_orig_sys,
            # Section 3 — Spectral / Bandpass
            band_id=band_res.band_id,
            band_name=entry.band_name if entry.band_name else band_res.band_id,
            regime=entry.regime,
            svo_filter_id=entry.svo_filter_id,
            spectral_coord_type=SpectralCoordType.wavelength,
            spectral_coord_value=spectral_coord_value,
            spectral_coord_unit=spectral_coord_unit,
            bandpass_width=entry.bandpass_width,
            # Section 4 — Photometric Measurement
            magnitude=magnitude,
            mag_err=mag_err,
            flux_density=flux_density,
            flux_density_err=flux_density_err,
            flux_density_unit=flux_density_unit,
            is_upper_limit=raw.upper_limit,
            limiting_value=limiting_value,
            quality_flag=QualityFlag.good,
            # Section 5 — Provenance
            bibcode=ticket.bibcode,
            telescope=raw.telescope,
            observer=raw.observer,
            data_rights=DataRights.public,
            band_resolution_type=band_res.resolution_type,
            band_resolution_confidence=band_res.confidence,
            sidecar_contributed=False,
            data_origin=DataOrigin.literature,
            donor_attribution=None,
        ),
    )


# ---------------------------------------------------------------------------
# Field extraction (DESIGN-004 §6.3)
# ---------------------------------------------------------------------------


def _extract_fields(
    raw_row: list[str],
    ticket: PhotometryTicket,
    row_number: int,
) -> _RawFields:
    """Extract per-row fields applying the ticket column index / default pattern.

    If a column index is present on the ticket, the CSV value at that index is
    used.  If the index is None, the ticket-level default applies.

    Raises:
        _RowError: If a required column index is out of range.
    """

    def _cell(col: int, label: str) -> str:
        if col >= len(raw_row):
            raise _RowError(
                f"Column index {col} ({label}) out of range for row with {len(raw_row)} fields"
            )
        return raw_row[col].strip()

    # Required per-row columns
    epoch_raw = _cell(ticket.time_col, "time_col")
    flux_value_raw = _cell(ticket.flux_col, "flux_col")

    # Optional error column
    flux_error_raw: str | None = None
    if ticket.flux_error_col is not None:
        val = _cell(ticket.flux_error_col, "flux_error_col").strip()
        flux_error_raw = val if val else None

    # Filter string: per-row column or ticket-level default.
    filter_string: str
    if ticket.filter_col is not None:
        filter_string = _cell(ticket.filter_col, "filter_col")
    else:
        # No per-row filter column — there must be a ticket-level filter default.
        # If neither is present, the row cannot be resolved.
        if ticket.filter_system is None:
            raise _RowError(
                "No filter column index and no ticket-level filter_system default; "
                "cannot determine filter string for this row."
            )
        filter_string = ticket.filter_system

    # Upper limit flag: "1" → True, "0" / absent → False.
    upper_limit = False
    if ticket.upper_limit_flag_col is not None:
        flag_val = _cell(ticket.upper_limit_flag_col, "upper_limit_flag_col").strip()
        if flag_val == "1":
            upper_limit = True
        elif flag_val not in ("0", ""):
            raise _RowError(f"Unrecognised upper_limit_flag value: {flag_val!r}")

    # Telescope: per-row column overrides ticket default.
    telescope: str | None = ticket.telescope
    if ticket.telescope_col is not None:
        val = _cell(ticket.telescope_col, "telescope_col").strip()
        telescope = val if val else ticket.telescope

    # Observer: per-row column overrides ticket default.
    observer: str | None = ticket.observer
    if ticket.observer_col is not None:
        val = _cell(ticket.observer_col, "observer_col").strip()
        observer = val if val else ticket.observer

    return _RawFields(
        epoch_raw=epoch_raw,
        flux_value_raw=flux_value_raw,
        flux_error_raw=flux_error_raw,
        filter_string=filter_string,
        upper_limit=upper_limit,
        telescope=telescope,
        observer=observer,
    )


# ---------------------------------------------------------------------------
# Time conversion (DESIGN-004 §6.4)
# ---------------------------------------------------------------------------

# JD epoch offset: JD 0.0 = MJD −2400000.5
_JD_TO_MJD_OFFSET: float = 2_400_000.5


def _convert_time(
    value: float,
    time_system: str,
) -> tuple[float, bool, TimeOrigSys]:
    """Convert a raw time value to MJD with provenance metadata.

    Args:
        value:       Raw time value from the CSV.
        time_system: Ticket time system string: "JD", "MJD", "HJD", or "BJD".

    Returns:
        (time_mjd, time_bary_corr, time_orig_sys)

    Raises:
        _RowError: For unrecognised time system values.
    """
    if time_system == "MJD":
        return value, False, TimeOrigSys.mjd_utc
    if time_system == "JD":
        return value - _JD_TO_MJD_OFFSET, False, TimeOrigSys.jd_utc
    if time_system == "HJD":
        # Heliocentric — not barycentric.  time_bary_corr is False.
        return value - _JD_TO_MJD_OFFSET, False, TimeOrigSys.hjd_utc
    if time_system == "BJD":
        # Barycentric Julian Date.  TimeOrigSys has no bjd_* variant yet;
        # "other" is used until the enum is extended (non-blocking for MVP).
        return value - _JD_TO_MJD_OFFSET, True, TimeOrigSys.other
    raise _RowError(f"Unrecognised time_system: {time_system!r}")


# ---------------------------------------------------------------------------
# Band resolution (DESIGN-004 §6.5)
# ---------------------------------------------------------------------------


def _resolve_band(
    filter_string: str,
    registry: BandRegistryProtocol,
) -> _BandResolution:
    """Resolve a filter string to a canonical band registry entry.

    Three-step sequence:
      1. Alias lookup — exact case-sensitive match in the alias index.
      2. Radio frequency fuzzy match — if the string looks like a radio
         frequency (e.g. ``"36.5 GHz"``), find the nearest registered
         radio band within ±20% tolerance.
      3. Generic fallback — look up ``Generic_{filter_string}`` as a band_id.

    Raises:
        _RowError: If the filter is excluded or cannot be resolved.
    """
    # Step 1 — alias lookup
    band_id = registry.lookup_band_id(filter_string)
    if band_id is not None:
        if registry.is_excluded(band_id):
            raise _RowError(f"Filter string {filter_string!r} maps to excluded band {band_id!r}")
        entry = registry.get_entry(band_id)
        if entry is None:
            # Alias index points to a band_id with no entry — registry integrity error.
            raise _RowError(
                f"Registry alias index maps {filter_string!r} → {band_id!r} "
                "but get_entry returned None; registry may be corrupt."
            )
        return _BandResolution(
            band_id=band_id,
            resolution_type=BandResolutionType.canonical,
            confidence=BandResolutionConfidence.high,
            entry=entry,
        )

    # Step 2 — radio frequency fuzzy match
    radio_band_id = registry.resolve_radio_frequency(filter_string)
    if radio_band_id is not None:
        if registry.is_excluded(radio_band_id):
            raise _RowError(
                f"Filter string {filter_string!r} fuzzy-matched to excluded band {radio_band_id!r}"
            )
        radio_entry = registry.get_entry(radio_band_id)
        if radio_entry is None:
            raise _RowError(
                f"Radio fuzzy resolver returned {radio_band_id!r} "
                "but get_entry returned None; registry may be corrupt."
            )
        return _BandResolution(
            band_id=radio_band_id,
            resolution_type=BandResolutionType.synonym,
            confidence=BandResolutionConfidence.medium,
            entry=radio_entry,
        )

    # Step 3 — Generic fallback
    generic_band_id = f"Generic_{filter_string}"
    generic_entry = registry.get_entry(generic_band_id)
    if generic_entry is not None:
        if registry.is_excluded(generic_band_id):
            raise _RowError(
                f"Filter string {filter_string!r} resolved to Generic entry "
                f"{generic_band_id!r} which is excluded."
            )
        return _BandResolution(
            band_id=generic_band_id,
            resolution_type=BandResolutionType.generic_fallback,
            confidence=BandResolutionConfidence.low,
            entry=generic_entry,
        )

    # Neither alias nor Generic entry found.
    raise _RowError(
        f"Unresolvable filter string: {filter_string!r} has no alias match "
        f"and no Generic_{filter_string} entry in the band registry."
    )


# ---------------------------------------------------------------------------
# Flux density unit resolution
# ---------------------------------------------------------------------------

_FLUX_UNIT_MAP: dict[str, FluxDensityUnit] = {
    "mJy": FluxDensityUnit.mjy,
    "Jy": FluxDensityUnit.jy,
    "uJy": FluxDensityUnit.ujy,
    "μJy": FluxDensityUnit.ujy,
}


def _resolve_flux_density_unit(flux_units: str | None) -> FluxDensityUnit:
    """Map a ticket's flux_units string to a FluxDensityUnit enum value.

    Falls back to mJy when the value is None or unrecognised.
    """
    if flux_units is None:
        return FluxDensityUnit.mjy
    return _FLUX_UNIT_MAP.get(flux_units, FluxDensityUnit.mjy)


# ---------------------------------------------------------------------------
# Row ID derivation (DESIGN-004 §8.2)
# ---------------------------------------------------------------------------


def _derive_row_id(
    nova_id: UUID,
    epoch_raw: str,
    band_id: str,
    flux_value_raw: str,
    filename: str,
) -> UUID:
    """Derive a deterministic row_id UUID from the row's natural identity.

    Participating fields:
      nova_id        — isolates rows across novae
      epoch_raw      — raw CSV string (pre-conversion) for reproducibility
      band_id        — resolved canonical band identifier
      flux_value_raw — raw CSV string (pre-conversion) for reproducibility
      filename       — ticket data_filename for source traceability

    The hash input uses '|' as a field separator.  Fields are taken as their
    string representations so the derivation is stable across Python sessions
    and independent of float formatting choices.

    Hash: SHA-256, first 16 bytes → UUID (version-unset raw bytes variant).
    """
    payload = f"{nova_id}|{epoch_raw}|{band_id}|{flux_value_raw}|{filename}"
    digest = hashlib.sha256(payload.encode("utf-8")).digest()
    return UUID(bytes=digest[:16])
