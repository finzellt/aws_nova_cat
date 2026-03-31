"""spectra_reader — metadata CSV parsing and per-spectrum FITS construction.

Reads a spectra metadata CSV (with headers), iterates rows, reads each
spectrum data CSV, builds a FITS file, and returns a structured result.
No S3, no DDB, no boto3.

Two-hop indirection
-------------------
The SpectraTicket's column index fields point into the *metadata CSV*, not
the individual spectrum data files::

    Ticket field          Metadata CSV col      Spectrum CSV col
    ─────────────         ────────────────      ────────────────
    filename_col      →   spectrum filename
    wavelength_col    →   WAVELENGTH COL NUM →  wavelength column
    flux_col          →   FLUX COL NUM       →  flux column
    flux_error_col    →   FLUX ERR COL NUM   →  flux-error column (opt)
    date_col          →   DATE (JD value)
    telescope_col     →   TELESCOPE string
    ...

Public API
----------
read_spectra(metadata_csv_path, data_dir, ticket, nova_id) -> SpectraReadResult
"""

from __future__ import annotations

import csv
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from contracts.models.tickets import SpectraTicket
from ticket_ingestor.fits_builder import FloatArray, build_fits

# ---------------------------------------------------------------------------
# Public result types
# ---------------------------------------------------------------------------

_NA_SENTINELS = frozenset({"NA", "N/A", "na", "n/a", ""})


@dataclass(frozen=True)
class SpectrumFailure:
    """Describes a per-spectrum processing failure."""

    spectrum_filename: str
    reason: str


@dataclass(frozen=True)
class SpectrumResult:
    """Successful per-spectrum processing result.

    The enrichment fields (instrument, telescope, observation_date_mjd,
    flux_unit) are extracted from the metadata CSV / ticket during read and
    surfaced here so that spectra_writer can persist them on the DataProduct
    DDB item (ADR-031 Decisions 2, 3, 5).
    """

    spectrum_filename: str
    data_product_id: uuid.UUID
    locator_identity: str
    s3_key: str
    fits_bytes: bytes

    # --- ADR-031 enrichment fields (Decisions 2, 3, 5) ---
    instrument: str | None = None
    telescope: str | None = None
    observation_date_mjd: float | None = None
    flux_unit: str | None = None


@dataclass(frozen=True)
class SpectraReadResult:
    """Aggregate result returned by read_spectra."""

    results: list[SpectrumResult]
    failures: list[SpectrumFailure]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# JD epoch offset: JD 0.0 = MJD −2400000.5
# Consistent with photometry_reader._JD_TO_MJD_OFFSET.
_JD_TO_MJD_OFFSET: float = 2_400_000.5


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _jd_to_iso(jd: float) -> str:
    """Convert a Julian Date to an ISO 8601 date string (YYYY-MM-DD).

    Uses astropy.time so the conversion is numerically correct for the
    full range of JD values encountered in nova light-curve data.

    Import is deferred to function scope so that the module is importable
    in environments where astropy is available but not at top-level
    (e.g. test-time mocking), and to avoid a hard module-load dependency
    for callers that only use other helpers.
    """
    from astropy.time import Time

    return str(Time(jd, format="jd").to_value("iso", subfmt="date"))


def _to_mjd(raw_value: float, time_system: str) -> float:
    """Convert a raw temporal value to Modified Julian Date.

    Mirrors the conversion logic in photometry_reader._convert_time but
    returns only the MJD float — the spectra path does not need provenance
    metadata (time_bary_corr, time_orig_sys).

    Supported time systems: JD, MJD, HJD, BJD.
    HJD and BJD are treated identically to JD for MJD conversion (the
    heliocentric/barycentric correction is sub-minute and does not affect
    waterfall plot ordering).

    Raises ValueError for unrecognised time systems.
    """
    if time_system == "MJD":
        return raw_value
    if time_system in {"JD", "HJD", "BJD"}:
        return raw_value - _JD_TO_MJD_OFFSET
    raise ValueError(f"Unrecognised time_system: {time_system!r}")


def _derive_data_product_id(
    bibcode: str,
    spectrum_filename: str,
    nova_id: uuid.UUID,
) -> uuid.UUID:
    """Derive a deterministic data_product_id via UUID5(NAMESPACE_OID, ...).

    The identity string is ``{bibcode}:{spectrum_filename}:{nova_id}`` —
    stable across re-runs for the same source observation.
    """
    identity = f"{bibcode}:{spectrum_filename}:{nova_id}"
    return uuid.uuid5(uuid.NAMESPACE_OID, identity)


def _col_value(row: list[str], col: int | None) -> str | None:
    """Return the stripped cell value, or None if col is None or value is NA."""
    if col is None:
        return None
    raw = row[col].strip()
    return None if raw in _NA_SENTINELS else raw


def _inner_col(row: list[str], meta_col: int) -> int | None:
    """Read an inner column index from a metadata CSV cell.

    Returns None when the cell is NA (i.e. the column is absent in the
    spectrum data file).
    """
    raw = row[meta_col].strip()
    if raw in _NA_SENTINELS:
        return None
    return int(raw)


def _build_keyword_dict(
    ticket: SpectraTicket,
    row: list[str],
    nova_id: uuid.UUID,
    wav_start: float,
) -> dict[str, Any]:
    """Build the FITS primary header keyword dict for one spectrum.

    All optional fields are included only when the ticket carries the
    relevant column index and the metadata CSV cell is not NA.

    BUNIT resolution order
    ~~~~~~~~~~~~~~~~~~~~~~
    1. Per-spectrum flux_units_col value from the metadata CSV row.
    2. Ticket-level flux_units default.
    3. Neither available → empty string (valid FITS; "unspecified units").

    Parameters
    ----------
    ticket:
        Validated SpectraTicket for this batch.
    row:
        One row from the metadata CSV (already split by csv.reader).
    nova_id:
        Resolved nova UUID — written as NOVA_ID for downstream traceability.
    wav_start:
        First value of the wavelength array (Å); becomes CRVAL1.
    """
    keywords: dict[str, Any] = {}

    # --- Required / always-present keywords --------------------------------
    keywords["OBJECT"] = ticket.object_name
    keywords["BIBCODE"] = ticket.bibcode
    keywords["DEREDDEN"] = ticket.dereddened

    # --- DATE-OBS (JD → ISO 8601 date) ------------------------------------
    date_raw = row[ticket.date_col].strip()
    if date_raw not in _NA_SENTINELS:
        keywords["DATE-OBS"] = _jd_to_iso(float(date_raw))

    # --- WCS axis (wavelength) --------------------------------------------
    keywords["CRVAL1"] = wav_start
    keywords["CRPIX1"] = 1.0
    keywords["CTYPE1"] = "WAVE"
    keywords["CUNIT1"] = "Angstrom"

    # CDELT1 from dispersion column (Å/pixel)
    dispersion_val = _col_value(row, ticket.dispersion_col)
    if dispersion_val is not None:
        keywords["CDELT1"] = float(dispersion_val)

    # --- BUNIT ------------------------------------------------------------
    per_spectrum_units = _col_value(row, ticket.flux_units_col)
    if per_spectrum_units is not None:
        keywords["BUNIT"] = per_spectrum_units
    elif ticket.flux_units is not None:
        keywords["BUNIT"] = ticket.flux_units
    else:
        keywords["BUNIT"] = ""

    # --- Optional provenance keywords ------------------------------------
    telescope_val = _col_value(row, ticket.telescope_col)
    if telescope_val is not None:
        keywords["TELESCOP"] = telescope_val

    instrument_val = _col_value(row, ticket.instrument_col)
    if instrument_val is not None:
        keywords["INSTRUME"] = instrument_val

    observer_val = _col_value(row, ticket.observer_col)
    if observer_val is not None:
        keywords["OBSERVER"] = observer_val

    snr_val = _col_value(row, ticket.snr_col)
    if snr_val is not None:
        keywords["SNR"] = float(snr_val)

    if ticket.wavelength_range_cols is not None:
        wav_min_val = _col_value(row, ticket.wavelength_range_cols[0])
        wav_max_val = _col_value(row, ticket.wavelength_range_cols[1])
        if wav_min_val is not None:
            keywords["WAV_MIN"] = float(wav_min_val)
        if wav_max_val is not None:
            keywords["WAV_MAX"] = float(wav_max_val)

    # --- Traceability -----------------------------------------------------
    keywords["NOVA_ID"] = str(nova_id)

    return keywords


def _extract_enrichment_fields(
    keywords: dict[str, Any],
    row: list[str],
    ticket: SpectraTicket,
) -> dict[str, Any]:
    """Extract ADR-031 enrichment fields from the built keywords and raw row.

    Reads instrument, telescope, and flux_unit from the already-resolved
    keywords dict to avoid duplicating resolution logic.  observation_date_mjd
    is computed from the raw date column value and the ticket's time_system.

    Returns a dict suitable for splatting into the SpectrumResult constructor.
    All values are None when the source data is absent.
    """
    # instrument and telescope: read from keywords (already resolved by
    # _build_keyword_dict from the metadata CSV via _col_value).
    instrument: str | None = keywords.get("INSTRUME")
    telescope: str | None = keywords.get("TELESCOP")

    # flux_unit: read from BUNIT keyword, normalize empty string to None
    # (ADR-031 contract: null means absent).
    raw_bunit: str = keywords.get("BUNIT", "")
    flux_unit: str | None = raw_bunit if raw_bunit else None

    # observation_date_mjd: convert the raw date value using the ticket's
    # time_system.  The date column is required on SpectraTicket, but the
    # cell value may be NA for a given row.
    observation_date_mjd: float | None = None
    date_raw = row[ticket.date_col].strip()
    if date_raw not in _NA_SENTINELS:
        observation_date_mjd = _to_mjd(float(date_raw), ticket.time_system)

    return {
        "instrument": instrument,
        "telescope": telescope,
        "observation_date_mjd": observation_date_mjd,
        "flux_unit": flux_unit,
    }


def _read_spectrum_arrays(
    spectrum_path: Path,
    wav_col: int,
    flux_col: int,
    ferr_col: int | None,
) -> tuple[FloatArray, FloatArray, FloatArray | None]:
    """Read a headerless spectrum CSV and return (wavelength, flux, flux_err).

    Parameters
    ----------
    spectrum_path:
        Absolute path to the headerless spectrum CSV.
    wav_col:
        0-based column index for wavelength in the spectrum CSV.
    flux_col:
        0-based column index for flux in the spectrum CSV.
    ferr_col:
        0-based column index for flux error, or None if absent.

    Returns
    -------
    tuple of (wavelength, flux, flux_err)
        flux_err is None when ferr_col is None.
    """
    wavelengths: list[float] = []
    fluxes: list[float] = []
    flux_errs: list[float] = []

    with spectrum_path.open(newline="") as fh:
        reader = csv.reader(fh)
        for line_num, row in enumerate(reader, start=1):
            try:
                wavelengths.append(float(row[wav_col]))
                fluxes.append(float(row[flux_col]))
                if ferr_col is not None:
                    flux_errs.append(float(row[ferr_col]))
            except (IndexError, ValueError) as exc:
                raise ValueError(
                    f"Malformed spectrum row {line_num} in {spectrum_path.name}: {exc}"
                ) from exc

    wav_arr: FloatArray = np.array(wavelengths, dtype=np.float64)
    flux_arr: FloatArray = np.array(fluxes, dtype=np.float64)
    ferr_arr: FloatArray | None = (
        np.array(flux_errs, dtype=np.float64) if ferr_col is not None else None
    )
    return wav_arr, flux_arr, ferr_arr


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def read_spectra(
    metadata_csv_path: Path,
    data_dir: Path,
    ticket: SpectraTicket,
    nova_id: uuid.UUID,
) -> SpectraReadResult:
    """Read a spectra metadata CSV and produce FITS bytes for each spectrum.

    Iterates rows in the metadata CSV (skipping the header row), reads each
    referenced spectrum data CSV, constructs FITS bytes via fits_builder, and
    collects results.  Per-spectrum failures are caught and recorded without
    aborting the batch.

    Parameters
    ----------
    metadata_csv_path:
        Absolute path to the metadata CSV file (has headers).
    data_dir:
        Directory containing the individual spectrum data CSV files referenced
        by the metadata CSV.
    ticket:
        Validated SpectraTicket describing column structure.
    nova_id:
        Resolved UUID for the nova; embedded in the S3 key and FITS header.

    Returns
    -------
    SpectraReadResult
        `.results` — list of SpectrumResult for each successfully processed
        spectrum.
        `.failures` — list of SpectrumFailure for each spectrum that could
        not be processed; does not include successfully processed spectra.
    """
    results: list[SpectrumResult] = []
    failures: list[SpectrumFailure] = []

    with metadata_csv_path.open(newline="") as fh:
        reader = csv.reader(fh)
        # Skip header row — metadata CSVs always have a header.
        next(reader)

        for row in reader:
            spectrum_filename = row[ticket.filename_col].strip()

            try:
                # ── Resolve inner column indices ─────────────────────────
                wav_col = _inner_col(row, ticket.wavelength_col)
                if wav_col is None:
                    raise ValueError("Wavelength column index is NA in metadata CSV.")

                flux_col_inner = _inner_col(row, ticket.flux_col)
                if flux_col_inner is None:
                    raise ValueError("Flux column index is NA in metadata CSV.")

                ferr_col: int | None = None
                if ticket.flux_error_col is not None:
                    ferr_col = _inner_col(row, ticket.flux_error_col)
                    # None here is valid — means no error column for this spectrum.

                # ── Read spectrum data CSV ───────────────────────────────
                spectrum_path = data_dir / spectrum_filename
                wav_arr, flux_arr, ferr_arr = _read_spectrum_arrays(
                    spectrum_path, wav_col, flux_col_inner, ferr_col
                )

                # ── Build FITS keyword dict ──────────────────────────────
                wav_start = float(wav_arr[0]) if len(wav_arr) > 0 else 0.0
                keywords = _build_keyword_dict(ticket, row, nova_id, wav_start)

                # ── Extract ADR-031 enrichment fields ────────────────────
                enrichment = _extract_enrichment_fields(keywords, row, ticket)

                # ── Construct FITS bytes ─────────────────────────────────
                fits_bytes = build_fits(wav_arr, flux_arr, ferr_arr, keywords)

                # ── Derive stable identity ───────────────────────────────
                data_product_id = _derive_data_product_id(
                    ticket.bibcode, spectrum_filename, nova_id
                )
                locator_identity = f"ticket_ingestion:{ticket.bibcode}:{spectrum_filename}"
                s3_key = f"raw/{nova_id}/ticket_ingestion/{data_product_id}.fits"

                results.append(
                    SpectrumResult(
                        spectrum_filename=spectrum_filename,
                        data_product_id=data_product_id,
                        locator_identity=locator_identity,
                        s3_key=s3_key,
                        fits_bytes=fits_bytes,
                        instrument=enrichment["instrument"],
                        telescope=enrichment["telescope"],
                        observation_date_mjd=enrichment["observation_date_mjd"],
                        flux_unit=enrichment["flux_unit"],
                    )
                )

            except Exception as exc:  # noqa: BLE001
                failures.append(
                    SpectrumFailure(
                        spectrum_filename=spectrum_filename,
                        reason=str(exc),
                    )
                )

    return SpectraReadResult(results=results, failures=failures)
