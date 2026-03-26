"""Ticket models for ticket-driven ingestion (DESIGN-004).

A Ticket is the parsed, validated representation of a hand-curated .txt
metadata file that completely describes a data file's structure.  Two
ticket types exist:

- PhotometryTicket: points directly at a headerless CSV of photometric
  observations.  Column indices map into the data file.
- SpectraTicket: points at a metadata CSV whose rows each describe one
  spectrum data file.  Column indices map into the metadata CSV (two-hop
  indirection: ticket → metadata CSV → spectrum data files).

The discriminated union ``Ticket`` selects the correct type based on the
``ticket_type`` literal field, which is derived during parsing from the
presence of ``DATA FILENAME`` (photometry) vs ``METADATA FILENAME``
(spectra) in the raw ticket.

All models use ``extra = "forbid"`` to reject unknown fields.  The raw
.txt → dict[str, str] parsing step is handled by the ticket parser
service, not by these models.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

# ── Shared field set (mixin, not a base class) ──────────────────────────


class _TicketCommon(BaseModel):
    """Fields shared by both ticket types.

    Not exported; used only as a mixin base for the two concrete types.
    """

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(
        ...,
        min_length=1,
        description="Nova name from OBJECT NAME.  Input to initialize_nova.",
    )
    wavelength_regime: str = Field(
        ...,
        description=(
            "Wavelength regime, lowercased.  Controlled vocabulary: "
            "optical, uv, nir, mir, radio, xray, gamma."
        ),
    )
    time_system: str = Field(
        ...,
        description="Time system of temporal values.  JD, MJD, HJD, or BJD.",
    )
    assumed_outburst_date: float | None = Field(
        default=None,
        description="Assumed outburst date in the ticket's time_system.  None if NA.",
    )
    reference: str = Field(
        ...,
        min_length=1,
        description="Human-readable citation string.",
    )
    bibcode: str = Field(
        ...,
        min_length=19,
        max_length=19,
        description="19-character ADS bibcode.",
    )
    ticket_status: str = Field(
        ...,
        description="Ticket curation status.  Only 'completed' tickets are processed.",
    )


# ── Photometry ticket ───────────────────────────────────────────────────


class PhotometryTicket(_TicketCommon):
    """Parsed photometry ticket.

    Column index fields are 0-based indices into the headerless data CSV.
    When a column index is None, the corresponding ticket-level default
    is used for every row.
    """

    ticket_type: Literal["photometry"] = "photometry"

    # ── Header-level defaults ───────────────────────────────────────────
    time_units: str = Field(..., description="Unit of the time column (e.g. 'days').")
    flux_units: str = Field(..., description="Unit of the flux column (e.g. 'mags').")
    flux_error_units: str = Field(..., description="Unit of the flux error column (e.g. 'mags').")
    filter_system: str | None = Field(
        default=None,
        description="Default photometric system (e.g. 'Johnson-Cousins').  Overridden per-row if filter_system_col is set.",
    )
    magnitude_system: str | None = Field(
        default=None,
        description="Magnitude system (e.g. 'Vega', 'AB').  Applies to all rows.",
    )
    telescope: str | None = Field(
        default=None,
        description="Default telescope.  Overridden per-row if telescope_col is set.",
    )
    observer: str | None = Field(
        default=None,
        description="Default observer.  Overridden per-row if observer_col is set.",
    )
    data_filename: str = Field(
        ...,
        min_length=1,
        description="Filename of the headerless CSV data file.",
    )

    # ── Column index mappings (0-based) ─────────────────────────────────
    time_col: int = Field(..., ge=0)
    flux_col: int = Field(..., ge=0)
    flux_error_col: int | None = Field(default=None, ge=0)
    filter_col: int | None = Field(default=None, ge=0)
    upper_limit_flag_col: int | None = Field(default=None, ge=0)
    telescope_col: int | None = Field(default=None, ge=0)
    observer_col: int | None = Field(default=None, ge=0)
    filter_system_col: int | None = Field(default=None, ge=0)


# ── Spectra ticket ──────────────────────────────────────────────────────


class SpectraTicket(_TicketCommon):
    """Parsed spectra ticket.

    Column index fields are 0-based indices into the *metadata CSV*, not
    the individual spectrum data files.  Each row of the metadata CSV
    describes one spectrum file (two-hop indirection).
    """

    ticket_type: Literal["spectra"] = "spectra"

    # ── Header-level fields ─────────────────────────────────────────────
    flux_units: str | None = Field(
        default=None,
        description="Default flux units.  Overridden per-spectrum if flux_units_col is set.",
    )
    flux_error_units: str | None = Field(
        default=None,
        description="Default flux error units.",
    )
    dereddened: bool = Field(
        ...,
        description="True if the spectra have been dereddened by the source authors.",
    )
    metadata_filename: str = Field(
        ...,
        min_length=1,
        description="Filename of the metadata CSV (has headers).",
    )

    # ── Column indices into the metadata CSV (0-based) ──────────────────
    filename_col: int = Field(..., ge=0)
    wavelength_col: int = Field(..., ge=0)
    flux_col: int = Field(..., ge=0)
    flux_error_col: int | None = Field(default=None, ge=0)
    flux_units_col: int | None = Field(default=None, ge=0)
    date_col: int = Field(..., ge=0)
    telescope_col: int | None = Field(default=None, ge=0)
    instrument_col: int | None = Field(default=None, ge=0)
    observer_col: int | None = Field(default=None, ge=0)
    snr_col: int | None = Field(default=None, ge=0)
    dispersion_col: int | None = Field(default=None, ge=0)
    resolution_col: int | None = Field(default=None, ge=0)
    wavelength_range_cols: tuple[int, int] | None = Field(
        default=None,
        description=(
            "Pair of 0-based column indices for wavelength range start and end "
            "in the metadata CSV.  Parsed from the comma-separated "
            "'WAVELENGTH RANGE COLUMN' ticket field."
        ),
    )


# ── Discriminated union ─────────────────────────────────────────────────

Ticket = Annotated[
    PhotometryTicket | SpectraTicket,
    Field(discriminator="ticket_type"),
]
