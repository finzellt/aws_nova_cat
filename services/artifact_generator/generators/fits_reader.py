"""FITS spectrum reader for the compositing pipeline.

Downloads a raw FITS file from S3 and extracts the wavelength and
flux arrays needed for compositing.  Wavelengths are returned in nm
regardless of the unit stored in the FITS header (handled via astropy
WCS and unit conversion).

This is the only module in the compositing pipeline that touches S3.
All downstream processing (cleaning, resampling, combination) operates
on the numpy arrays returned here.

See ADR-033 for design rationale.
"""

from __future__ import annotations

import io
import logging
from typing import Any

import astropy.units as u
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
from numpy.typing import NDArray

_logger = logging.getLogger(__name__)


def read_fits_spectrum(
    s3_client: Any,
    bucket: str,
    raw_s3_key: str,
    data_product_id: str,
) -> tuple[NDArray[np.float64], NDArray[np.float64]] | None:
    """Download a FITS file from S3 and extract wavelength + flux arrays.

    Wavelengths are converted to nanometres using the WCS unit metadata
    in the FITS header (``CUNIT1``).  If no unit is specified, Ångströms
    are assumed (the ESO default).

    Parameters
    ----------
    s3_client:
        Boto3 S3 client (not resource).
    bucket:
        Private data bucket name.
    raw_s3_key:
        S3 key for the raw FITS file.
    data_product_id:
        For logging context.

    Returns
    -------
    tuple[NDArray, NDArray] | None:
        ``(wavelengths_nm, fluxes)`` as float64 arrays, or ``None`` if
        the FITS file could not be read or contains no usable data.
    """
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=raw_s3_key)
        fits_bytes = resp["Body"].read()
    except Exception:
        _logger.warning(
            "Failed to download FITS from S3",
            extra={
                "data_product_id": data_product_id,
                "bucket": bucket,
                "raw_s3_key": raw_s3_key,
            },
            exc_info=True,
        )
        return None

    return extract_spectrum_from_fits(fits_bytes, data_product_id)


def extract_spectrum_from_fits(
    fits_bytes: bytes,
    data_product_id: str,
) -> tuple[NDArray[np.float64], NDArray[np.float64]] | None:
    """Extract wavelength and flux arrays from in-memory FITS bytes.

    Tries three strategies in order:
    1. Primary HDU (hdul[0]) image data + WCS wavelength keywords.
    2. First extension (hdul[1]) image data + WCS (common for ESO
       UVES/X-Shooter where the primary HDU is header-only).
    3. Binary table extension with named wavelength/flux columns.

    Separated from ``read_fits_spectrum`` so it can be tested with
    synthetic FITS data without mocking S3.

    Parameters
    ----------
    fits_bytes:
        Raw FITS file contents.
    data_product_id:
        For logging context.

    Returns
    -------
    tuple[NDArray, NDArray] | None:
        ``(wavelengths_nm, fluxes)`` or ``None`` on failure.
    """
    try:
        with fits.open(io.BytesIO(fits_bytes), memmap=False) as hdul:
            # --- Strategy 1: Primary HDU image data + WCS ---
            primary_data = hdul[0].data  # type: ignore[index]
            if primary_data is not None and primary_data.ndim > 0:
                return _extract_from_image_hdu(hdul, 0, data_product_id)

            # --- Strategy 2: First extension image data + WCS ---
            # Many ESO products (UVES, X-Shooter) store the spectrum
            # in hdul[1] with the same image+WCS layout.  Only attempt
            # this on ImageHDUs — BinTableHDUs fall through to Strategy 3.
            if len(hdul) > 1 and isinstance(hdul[1], fits.ImageHDU | fits.CompImageHDU):
                ext1_data = hdul[1].data
                if ext1_data is not None and ext1_data.ndim > 0:
                    result = _extract_from_image_hdu(hdul, 1, data_product_id)
                    if result is not None:
                        return result

            # --- Strategy 3: Binary table extension with named columns ---
            if len(hdul) > 1:
                return _extract_from_table_hdu(hdul, data_product_id)

            _logger.warning(
                "FITS has no image data and no table extensions",
                extra={"data_product_id": data_product_id},
            )
            return None

    except Exception:
        _logger.warning(
            "Failed to parse FITS file",
            extra={"data_product_id": data_product_id},
            exc_info=True,
        )
        return None


#: Column name candidates for wavelength and flux in binary table HDUs.
#: Tried in order; first match wins.
_WAVELENGTH_COLUMNS = ("WAVE", "WAVELENGTH", "LAMBDA", "wave", "wavelength", "lambda")
_FLUX_COLUMNS = ("FLUX", "FLUX_REDUCED", "DATA", "FLUX_OPTIMAL", "flux", "data")


def _extract_from_image_hdu(
    hdul: Any,
    ext_idx: int,
    data_product_id: str,
) -> tuple[NDArray[np.float64], NDArray[np.float64]] | None:
    """Extract spectrum from an image HDU with data + WCS."""
    header = hdul[ext_idx].header  # type: ignore[index]
    flux_data = hdul[ext_idx].data  # type: ignore[index]

    flux = flux_data.squeeze()
    if flux.ndim != 1:
        _logger.warning(
            "FITS flux array is not 1-D after squeeze",
            extra={
                "data_product_id": data_product_id,
                "shape": str(flux_data.shape),
            },
        )
        return None

    wavelengths_nm = _wavelengths_from_wcs(header, len(flux), data_product_id)
    if wavelengths_nm is None:
        return None

    return wavelengths_nm, np.asarray(flux, dtype=np.float64)


def _extract_from_table_hdu(
    hdul: Any,
    data_product_id: str,
) -> tuple[NDArray[np.float64], NDArray[np.float64]] | None:
    """Extract spectrum from a binary table extension with named columns."""
    # Search extensions for a table with recognisable wavelength + flux columns.
    for ext_idx in range(1, len(hdul)):
        ext = hdul[ext_idx]
        if not hasattr(ext, "columns"):
            continue

        col_names = [c.name for c in ext.columns]

        wl_col = _find_column(col_names, _WAVELENGTH_COLUMNS)
        fx_col = _find_column(col_names, _FLUX_COLUMNS)

        if wl_col is None or fx_col is None:
            continue

        wl_raw = np.asarray(ext.data[wl_col], dtype=np.float64).squeeze()
        fx_raw = np.asarray(ext.data[fx_col], dtype=np.float64).squeeze()

        if wl_raw.ndim != 1 or fx_raw.ndim != 1:
            _logger.warning(
                "Binary table columns are not 1-D after squeeze",
                extra={
                    "data_product_id": data_product_id,
                    "ext_index": ext_idx,
                    "wl_shape": str(wl_raw.shape),
                    "fx_shape": str(fx_raw.shape),
                },
            )
            continue

        if len(wl_raw) != len(fx_raw):
            _logger.warning(
                "Wavelength and flux columns have different lengths",
                extra={
                    "data_product_id": data_product_id,
                    "wl_len": len(wl_raw),
                    "fx_len": len(fx_raw),
                },
            )
            continue

        # Convert wavelength to nm.  Binary table wavelengths are
        # typically in Angstrom for ESO products.  Check the header
        # for a unit hint.
        wl_nm = _convert_table_wavelengths_to_nm(wl_raw, hdul[ext_idx].header, data_product_id)

        _logger.debug(
            "Extracted spectrum from binary table extension",
            extra={
                "data_product_id": data_product_id,
                "ext_index": ext_idx,
                "wl_col": wl_col,
                "fx_col": fx_col,
                "n_points": len(wl_nm),
            },
        )
        return wl_nm, fx_raw

    _logger.warning(
        "No binary table extension with recognisable wavelength/flux columns",
        extra={"data_product_id": data_product_id},
    )
    return None


def _find_column(
    available: list[str],
    candidates: tuple[str, ...],
) -> str | None:
    """Return the first candidate name present in *available*, or None."""
    for name in candidates:
        if name in available:
            return name
    return None


def _convert_table_wavelengths_to_nm(
    wavelengths: NDArray[np.float64],
    header: Any,
    data_product_id: str,
) -> NDArray[np.float64]:
    """Convert binary table wavelengths to nm.

    Checks ``TUNIT`` keywords and common header hints.  Falls back
    to assuming Angstrom if no unit metadata is found.
    """
    # Check TUNITn keywords for the wavelength column.
    for key in header:
        if key.startswith("TUNIT"):
            val = str(header[key]).strip().lower()
            if val in ("angstrom", "angstroms", "ang", "a"):
                return wavelengths / 10.0
            if val in ("nm", "nanometer", "nanometers", "nanometre"):
                return wavelengths
            if val in ("m", "meter", "metre"):
                return wavelengths * 1e9

    # Heuristic: if median wavelength is > 100, it's probably Angstrom.
    median_wl = float(np.median(wavelengths))
    if median_wl > 100.0:
        _logger.debug(
            "No wavelength unit in table header; assuming Angstrom (median=%.1f)",
            median_wl,
            extra={"data_product_id": data_product_id},
        )
        return wavelengths / 10.0

    # Already in nm (or something exotic — best guess is nm).
    return wavelengths


def _wavelengths_from_wcs(
    header: Any,
    n_pixels: int,
    data_product_id: str,
) -> NDArray[np.float64] | None:
    """Reconstruct the wavelength array from FITS WCS header keywords.

    Uses astropy's WCS to handle the pixel → world coordinate
    transformation, including unit conversion to nm.  Falls back to
    assuming Ångströms if ``CUNIT1`` is absent.

    Parameters
    ----------
    header:
        FITS primary HDU header.
    n_pixels:
        Length of the flux array (``NAXIS1``).
    data_product_id:
        For logging context.

    Returns
    -------
    NDArray[np.float64] | None:
        Wavelength array in nm, or ``None`` if WCS reconstruction fails.
    """
    try:
        wcs = WCS(header, naxis=1)
        pixel_indices = np.arange(n_pixels)
        wavelengths_raw = wcs.pixel_to_world(pixel_indices)

        # pixel_to_world returns an astropy Quantity if the WCS has a
        # recognised spectral axis, or a plain ndarray if CUNIT1 is
        # missing.
        if hasattr(wavelengths_raw, "unit"):
            wavelengths_nm = wavelengths_raw.to(u.nm).value
        else:
            # No unit metadata — assume Ångströms (ESO default).
            _logger.debug(
                "No CUNIT1 in FITS header; assuming Angstrom",
                extra={"data_product_id": data_product_id},
            )
            wavelengths_nm = np.asarray(wavelengths_raw, dtype=np.float64) / 10.0

        return np.asarray(wavelengths_nm, dtype=np.float64)

    except Exception:
        _logger.warning(
            "Failed to reconstruct wavelengths from WCS",
            extra={"data_product_id": data_product_id},
            exc_info=True,
        )
        return None
