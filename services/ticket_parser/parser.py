"""Ticket parser for ticket-driven ingestion (DESIGN-004).

Two-stage process:

  Stage 1 — ``parse_ticket_file(path) -> dict[str, str]``
    Format-aware, schema-ignorant.  Reads the .txt file and returns raw
    key-value pairs.  NA values are preserved as the literal string "NA".
    Raises TicketParseError on formatting violations (no ':' delimiter,
    duplicate key).

  Stage 2 — ``validate_ticket(raw_dict, *, path) -> PhotometryTicket | SpectraTicket``
    Schema-aware.  Discriminates ticket type, maps raw keys to Pydantic
    field names, coerces values to their target types, applies string
    normalizations, and constructs the validated model.  Raises
    TicketParseError on schema or validation failures.

The two stages are separated so that Stage 1 can be unit-tested against
malformed files independently of schema validation.  The Lambda handler
calls both in sequence; see services/ticket_parser/handler.py.

TicketParseError is the single error surface for all parse failures.
A bad ticket is an operator authoring error — no quarantine, no retry.
The operator fixes the ticket and reruns the workflow.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from contracts.models.tickets import PhotometryTicket, SpectraTicket

# ---------------------------------------------------------------------------
# Error model
# ---------------------------------------------------------------------------


@dataclass
class TicketParseError(Exception):
    """Raised for any ticket parsing or validation failure.

    Attributes:
        path:        Path to the .txt ticket file (or "<unknown>").
        reason:      Human-readable description of the failure.
        line_number: 1-based source line where the error occurred, or
                     None for schema-level failures without a single
                     source line.
    """

    path: str
    reason: str
    line_number: int | None = None

    def __str__(self) -> str:
        loc = f" (line {self.line_number})" if self.line_number is not None else ""
        return f"{self.path}{loc}: {self.reason}"


# ---------------------------------------------------------------------------
# Stage 1: Raw parse
# ---------------------------------------------------------------------------


def parse_ticket_file(path: str | Path) -> dict[str, str]:
    """Stage 1: format-aware, schema-ignorant parse.

    Reads the .txt ticket file and returns raw key-value pairs.

    Rules:
    - Each line is split on the *first* ':' only.
    - Leading/trailing whitespace is stripped from both key and value.
    - Empty lines and whitespace-only lines are skipped.
    - "NA" values are preserved as the literal string "NA" — conversion
      to None happens in Stage 2.
    - Lines with no ':' delimiter raise TicketParseError with the
      1-based line number.
    - Duplicate keys raise TicketParseError with the line number of the
      second occurrence.

    Args:
        path: Filesystem path to the .txt ticket file.

    Returns:
        dict mapping raw ticket key strings to raw value strings.
        Example: {"OBJECT NAME": "V4739_Sgr", "FLUX UNITS": "mags", ...}

    Raises:
        TicketParseError: On any formatting violation.
        OSError: If the file cannot be opened (propagated to caller).
    """
    path_str = str(path)
    raw: dict[str, str] = {}

    with open(path, encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            stripped = line.rstrip("\n")

            # Skip empty and whitespace-only lines
            if not stripped.strip():
                continue

            # Split on first ':' only
            if ":" not in stripped:
                raise TicketParseError(
                    path=path_str,
                    reason=f"No ':' delimiter found: {stripped!r}",
                    line_number=line_num,
                )

            key, _, value = stripped.partition(":")
            key = key.strip()
            value = value.strip()

            if key in raw:
                raise TicketParseError(
                    path=path_str,
                    reason=f"Duplicate key {key!r}",
                    line_number=line_num,
                )

            raw[key] = value

    return raw


# ---------------------------------------------------------------------------
# Stage 2: Key mapping tables (single source of truth)
# ---------------------------------------------------------------------------

# Keys shared by both ticket types (defined on _TicketCommon).
_COMMON_KEY_MAP: dict[str, str] = {
    "OBJECT NAME": "object_name",
    "WAVELENGTH REGIME": "wavelength_regime",
    "TIME SYSTEM": "time_system",
    "ASSUMED DATE OF OUTBURST": "assumed_outburst_date",
    "REFERENCE": "reference",
    "BIBCODE": "bibcode",
    "TICKET STATUS": "ticket_status",
}

# Keys exclusive to photometry tickets (header-level defaults + column indices).
_PHOTOMETRY_ONLY_KEY_MAP: dict[str, str] = {
    # Header-level defaults
    "TIME UNITS": "time_units",
    "FLUX UNITS": "flux_units",
    "FLUX ERROR UNITS": "flux_error_units",
    "FILTER SYSTEM": "filter_system",
    "MAGNITUDE SYSTEM": "magnitude_system",
    "TELESCOPE": "telescope",
    "OBSERVER": "observer",
    "DATA FILENAME": "data_filename",
    # Column indices into the headerless data CSV
    "TIME COLUMN NUMBER": "time_col",
    "FLUX COLUMN NUMBER": "flux_col",
    "FLUX ERROR COLUMN NUMBER": "flux_error_col",
    "FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER": "filter_col",
    "UPPER LIMIT FLAG COLUMN NUMBER": "upper_limit_flag_col",
    "TELESCOPE COLUMN NUMBER": "telescope_col",
    "OBSERVER COLUMN NUMBER": "observer_col",
    "FILTER SYSTEM COLUMN NUMBER": "filter_system_col",
}

# Keys exclusive to spectra tickets (header-level fields + column indices
# into the metadata CSV).
_SPECTRA_ONLY_KEY_MAP: dict[str, str] = {
    # Header-level fields
    "FLUX UNITS": "flux_units",
    "FLUX ERROR UNITS": "flux_error_units",
    "DEREDDENED FLAG": "dereddened",
    "METADATA FILENAME": "metadata_filename",
    # Column indices into the metadata CSV
    "FILENAME COLUMN": "filename_col",
    "WAVELENGTH COLUMN": "wavelength_col",
    "FLUX COLUMN": "flux_col",
    "FLUX ERROR COLUMN": "flux_error_col",
    "FLUX UNITS COLUMN": "flux_units_col",
    "DATE COLUMN": "date_col",
    "TELESCOPE COLUMN": "telescope_col",
    "INSTRUMENT COLUMN": "instrument_col",
    "OBSERVER COLUMN": "observer_col",
    "SNR COLUMN": "snr_col",
    "DISPERSION COLUMN": "dispersion_col",
    "RESOLUTION COLUMN": "resolution_col",
    "WAVELENGTH RANGE COLUMN": "wavelength_range_cols",
}

# Combined maps used during validation.  Built once at module load time.
_PHOTOMETRY_KEY_MAP: dict[str, str] = {**_COMMON_KEY_MAP, **_PHOTOMETRY_ONLY_KEY_MAP}
_SPECTRA_KEY_MAP: dict[str, str] = {**_COMMON_KEY_MAP, **_SPECTRA_ONLY_KEY_MAP}

# ---------------------------------------------------------------------------
# Stage 2: Coercion helpers
# ---------------------------------------------------------------------------

# Fields whose non-NA value must be coerced to int.
# Covers both required (int) and optional (int | None) column index fields.
# NA → None is handled universally before this check; Pydantic enforces
# required-vs-optional after construction.
_INT_FIELDS: frozenset[str] = frozenset(
    {
        # Photometry column indices
        "time_col",
        "flux_col",
        "flux_error_col",
        "filter_col",
        "upper_limit_flag_col",
        "telescope_col",
        "observer_col",
        "filter_system_col",
        # Spectra column indices (flux_col, telescope_col, observer_col shared above)
        "filename_col",
        "wavelength_col",
        "flux_units_col",
        "date_col",
        "instrument_col",
        "snr_col",
        "dispersion_col",
        "resolution_col",
    }
)

# Fields whose non-NA value must be coerced to bool.
_BOOL_FIELDS: frozenset[str] = frozenset({"dereddened"})

# Fields whose non-NA value must be coerced to float.
_FLOAT_FIELDS: frozenset[str] = frozenset({"assumed_outburst_date"})

# Fields whose non-NA value must be coerced to tuple[int, int].
# The raw ticket value is a comma-separated pair, e.g. "10,11".
_INT_PAIR_FIELDS: frozenset[str] = frozenset({"wavelength_range_cols"})

# Fields that are lowercased after coercion (normalizations).
_LOWERCASE_FIELDS: frozenset[str] = frozenset({"wavelength_regime", "ticket_status"})


def _coerce_value(field_name: str, raw_value: str) -> Any:
    """Coerce a raw string value to its target Python type.

    Applied after key mapping, before Pydantic construction.

    Coercion order:
      1. "NA" (case-insensitive) → None  (universal rule)
      2. Bool fields: "true"/"false" (case-insensitive) → bool
      3. Float fields: float-parseable string → float
      4. Int-pair fields: "N,M" → tuple[int, int]
      5. Int fields: digit string → int
      6. Default: str, preserved as-is (already stripped in Stage 1)

    Args:
        field_name: The Pydantic field name (after key mapping).
        raw_value:  The raw string value from the ticket file.

    Returns:
        The coerced value.

    Raises:
        ValueError: If the raw value cannot be coerced to the expected type.
    """
    # 1. Universal NA → None
    if raw_value.upper() == "NA":
        return None

    # 2. Bool fields
    if field_name in _BOOL_FIELDS:
        normalized = raw_value.lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
        raise ValueError(f"Expected 'True' or 'False' for field {field_name!r}, got {raw_value!r}")

    # 3. Float fields
    if field_name in _FLOAT_FIELDS:
        try:
            return float(raw_value)
        except ValueError:
            raise ValueError(
                f"Expected a numeric value for field {field_name!r}, got {raw_value!r}"
            ) from None

    # 4. Int-pair fields  (e.g. wavelength_range_cols: "10,11")
    if field_name in _INT_PAIR_FIELDS:
        parts = [p.strip() for p in raw_value.split(",")]
        if len(parts) != 2:
            raise ValueError(
                f"Expected 'N,M' (two comma-separated integers) for field "
                f"{field_name!r}, got {raw_value!r}"
            )
        try:
            return (int(parts[0]), int(parts[1]))
        except ValueError:
            raise ValueError(
                f"Expected integer pair for field {field_name!r}, got {raw_value!r}"
            ) from None

    # 5. Int fields
    if field_name in _INT_FIELDS:
        try:
            return int(raw_value)
        except ValueError:
            raise ValueError(
                f"Expected an integer for field {field_name!r}, got {raw_value!r}"
            ) from None

    # 6. Default: string preserved as-is
    return raw_value


# ---------------------------------------------------------------------------
# Stage 2: validate_ticket
# ---------------------------------------------------------------------------


def validate_ticket(
    raw_dict: dict[str, str],
    *,
    path: str = "<unknown>",
) -> PhotometryTicket | SpectraTicket:
    """Stage 2: schema-aware discrimination, key mapping, coercion, validation.

    Raises TicketParseError for:
    - Ambiguous type discriminator (both DATA FILENAME and METADATA FILENAME present)
    - Missing type discriminator (neither key present)
    - Any raw key that is not in the selected key map for the ticket type
    - Type coercion failures (e.g. non-integer column index)
    - Pydantic ValidationError (missing required fields, constraint violations)

    Normalizations applied unconditionally (after coercion):
    - ``wavelength_regime`` is lowercased.
    - ``ticket_status`` is lowercased.

    Args:
        raw_dict: Output of ``parse_ticket_file`` — raw key-value pairs.
        path:     Path string used in TicketParseError messages.
                  Defaults to "<unknown>"; callers should pass the actual path.

    Returns:
        A fully validated ``PhotometryTicket`` or ``SpectraTicket``.

    Raises:
        TicketParseError: On any schema or validation failure.
    """
    # -- Discrimination ------------------------------------------------------
    has_data_filename = "DATA FILENAME" in raw_dict
    has_metadata_filename = "METADATA FILENAME" in raw_dict

    if has_data_filename and has_metadata_filename:
        raise TicketParseError(
            path=path,
            reason=(
                "Ambiguous ticket type: both DATA FILENAME and METADATA FILENAME "
                "are present — exactly one must be set"
            ),
        )
    if not has_data_filename and not has_metadata_filename:
        raise TicketParseError(
            path=path,
            reason=(
                "Cannot determine ticket type: neither DATA FILENAME nor "
                "METADATA FILENAME is present — exactly one must be set"
            ),
        )

    is_photometry = has_data_filename
    ticket_type_label = "photometry" if is_photometry else "spectra"
    key_map = _PHOTOMETRY_KEY_MAP if is_photometry else _SPECTRA_KEY_MAP

    # -- Key mapping and unknown-key rejection --------------------------------
    mapped: dict[str, Any] = {}
    for raw_key, raw_value in raw_dict.items():
        if raw_key not in key_map:
            raise TicketParseError(
                path=path,
                reason=(
                    f"Unknown key {raw_key!r} for {ticket_type_label} ticket. "
                    f"Valid keys: {sorted(key_map)}"
                ),
            )
        field_name = key_map[raw_key]
        try:
            mapped[field_name] = _coerce_value(field_name, raw_value)
        except ValueError as exc:
            raise TicketParseError(path=path, reason=str(exc)) from exc

    # -- Normalizations -------------------------------------------------------
    for field_name in _LOWERCASE_FIELDS:
        if isinstance(mapped.get(field_name), str):
            mapped[field_name] = mapped[field_name].lower()

    # -- Pydantic construction ------------------------------------------------
    model_cls: type[PhotometryTicket] | type[SpectraTicket] = (
        PhotometryTicket if is_photometry else SpectraTicket
    )
    try:
        return model_cls.model_validate(mapped)
    except ValidationError as exc:
        # Produce a compact, operator-readable summary of every failing field.
        problems = "; ".join(
            f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        raise TicketParseError(
            path=path,
            reason=f"Pydantic validation failed — {problems}",
        ) from exc
