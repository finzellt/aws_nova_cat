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
            header = hdul[0].header  # type: ignore[index]
            flux_data = hdul[0].data  # type: ignore[index]

            if flux_data is None or flux_data.ndim == 0:
                _logger.warning(
                    "FITS primary HDU has no data array",
                    extra={"data_product_id": data_product_id},
                )
                return None

            # Flatten if the data has extra degenerate dimensions
            # (some ESO products have shape (1, 1, N)).
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

            # Build wavelength array from WCS.
            wavelengths_nm = _wavelengths_from_wcs(header, len(flux), data_product_id)
            if wavelengths_nm is None:
                return None

            flux_arr = np.asarray(flux, dtype=np.float64)
            return wavelengths_nm, flux_arr

    except Exception:
        _logger.warning(
            "Failed to parse FITS file",
            extra={"data_product_id": data_product_id},
            exc_info=True,
        )
        return None


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
