"""fits_builder — pure in-memory FITS file construction.

Accepts pre-extracted numpy arrays and a caller-supplied keyword dict and
returns raw FITS bytes.  No CSV reading, no S3 interaction, no DDB
interaction.  All decisions about keyword values (including BUNIT
empty-string convention) are made by the caller.

Public API
----------
build_fits(wavelength, flux, flux_err, keywords) -> bytes
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np
import numpy.typing as npt
from astropy.io import fits

# ---------------------------------------------------------------------------
# Public
# ---------------------------------------------------------------------------

FloatArray = npt.NDArray[np.float64]


def build_fits(
    wavelength: FloatArray,
    flux: FloatArray,
    flux_err: FloatArray | None,
    keywords: dict[str, Any],
) -> bytes:
    """Construct a single- or dual-extension FITS file and return raw bytes.

    Layout
    ------
    - Primary HDU:  flux as the data array.  All entries in *keywords* are
      written into the primary header via ``hdr.set()``.  No special-casing
      of any keyword — BUNIT='' is written as-is, which is valid FITS and
      signals "unspecified units" without breaking loaders.
    - Optional IMAGE extension ``FLUX_ERR``:  appended when *flux_err* is
      not None.  Carries no additional header keywords beyond EXTNAME.

    WCS
    ---
    The wavelength axis is encoded purely through WCS keywords in *keywords*
    (CRVAL1, CDELT1, CRPIX1, CTYPE1, CUNIT1).  The caller is responsible for
    populating these; this function does not touch the wavelength array beyond
    confirming it has the same length as flux for the optional FLUX_ERR
    extension check.

    Parameters
    ----------
    wavelength:
        1-D wavelength array in Å.  Used only for length validation when
        *flux_err* is provided; WCS keywords in *keywords* encode the axis.
    flux:
        1-D flux array.  Written as the primary HDU data.
    flux_err:
        Optional 1-D flux error array.  When provided, must be the same
        length as *flux*.  Written as a second IMAGE extension.
    keywords:
        Flat ``dict[str, Any]`` of FITS header keywords to write into the
        primary HDU.  The caller owns all values; this function writes them
        unconditionally.

    Returns
    -------
    bytes
        Raw FITS file bytes suitable for direct upload to S3 or writing to
        disk.

    Raises
    ------
    ValueError
        If *flux_err* is provided but its length differs from *flux*.
    """
    if flux_err is not None and len(flux_err) != len(flux):
        raise ValueError(
            f"flux_err length ({len(flux_err)}) does not match flux length ({len(flux)})."
        )

    # --- Primary HDU (flux) ------------------------------------------------
    primary_hdu = fits.PrimaryHDU(data=flux.astype(np.float64))
    hdr = primary_hdu.header

    for key, value in keywords.items():
        hdr.set(key, value)

    # --- Optional flux-error extension ------------------------------------
    hdu_list: list[fits.HDU] = [primary_hdu]  # type: ignore[type-arg]
    if flux_err is not None:
        err_hdu = fits.ImageHDU(data=flux_err.astype(np.float64))
        err_hdu.header.set("EXTNAME", "FLUX_ERR")
        hdu_list.append(err_hdu)

    # --- Serialise to bytes -----------------------------------------------
    hdus = fits.HDUList(hdu_list)
    buf = io.BytesIO()
    hdus.writeto(buf, output_verify="fix")
    return buf.getvalue()
