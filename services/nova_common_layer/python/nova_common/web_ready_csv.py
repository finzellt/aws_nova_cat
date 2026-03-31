"""web_ready_csv — generate and upload downsampled, nm-wavelength CSV files.

Implements ADR-031 Decision 4 (DESIGN-003 §7.9, P-4): after a spectrum is
validated, write a two-column CSV (wavelength_nm, flux) to S3 at
``derived/spectra/<nova_id>/<data_product_id>/web_ready.csv``.

The CSV is consumed by the ``spectra.json`` artifact generator (DESIGN-003
§7.2), which reads pre-processed files rather than raw FITS — avoiding an
astropy dependency in the Fargate container.

This module requires numpy and astropy.units, which are available only in
Docker-based Lambda services (spectra_validator, ticket_ingestor). It must
NOT be imported by zip-bundled Lambdas.

Public API
----------
build_web_ready_csv(wavelength, flux, spectral_units) -> str
    Pure function: unit-convert, downsample, return CSV string.

write_web_ready_csv_to_s3(csv_content, nova_id, data_product_id, s3, bucket) -> str
    Upload CSV bytes to S3, return the key written.

derive_web_ready_s3_key(nova_id, data_product_id) -> str
    Return the canonical S3 key for a web-ready CSV.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np
from astropy import units as u

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_POINTS: int = 2000
_CSV_HEADER: str = "wavelength_nm,flux"
_S3_KEY_TEMPLATE: str = "derived/spectra/{nova_id}/{data_product_id}/web_ready.csv"
_CONTENT_TYPE: str = "text/csv"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def derive_web_ready_s3_key(nova_id: str, data_product_id: str) -> str:
    """Return the canonical S3 key for a web-ready CSV."""
    return _S3_KEY_TEMPLATE.format(nova_id=nova_id, data_product_id=data_product_id)


def build_web_ready_csv(
    wavelength: np.ndarray,
    flux: np.ndarray,
    spectral_units: str,
) -> str:
    """Convert wavelengths to nm, downsample to ≤2000 points, return CSV string.

    Parameters
    ----------
    wavelength:
        1-D array of spectral axis values in the unit given by *spectral_units*.
    flux:
        1-D array of flux values, parallel to *wavelength*.
    spectral_units:
        Unit string parseable by ``astropy.units.Unit`` — e.g. ``"Angstrom"``,
        ``"angstrom"``, ``"nm"``, ``"um"``, ``"micron"``.

    Returns
    -------
    str
        Two-column CSV with header ``wavelength_nm,flux`` and up to 2000 data
        rows.  Wavelengths are in nanometres.
    """
    if len(wavelength) != len(flux):
        raise ValueError(
            f"wavelength and flux arrays must have the same length, "
            f"got {len(wavelength)} and {len(flux)}"
        )
    if len(wavelength) == 0:
        raise ValueError("Cannot build web-ready CSV from empty arrays")

    # --- Unit conversion to nm ---
    source_unit = u.Unit(spectral_units)
    wavelength_nm: np.ndarray = (wavelength * source_unit).to(u.nm).value

    # --- Downsample if needed ---
    # Uses np.linspace to select exactly _MAX_POINTS evenly-spaced indices.
    # For arrays slightly over the limit (e.g. 2030), this removes a small
    # number of points distributed across the array rather than halving via
    # naive stride.  For very large arrays (e.g. 200 000), it produces a
    # clean uniform subsample.
    if len(wavelength_nm) > _MAX_POINTS:
        indices = np.round(np.linspace(0, len(wavelength_nm) - 1, _MAX_POINTS)).astype(int)
        wavelength_nm = wavelength_nm[indices]
        flux = flux[indices]

    # --- Serialize to CSV ---
    buf = io.StringIO()
    buf.write(_CSV_HEADER)
    buf.write("\n")
    for w, f in zip(wavelength_nm, flux):
        buf.write(f"{w},{f}\n")

    return buf.getvalue()


def write_web_ready_csv_to_s3(
    csv_content: str,
    nova_id: str,
    data_product_id: str,
    s3: Any,
    bucket: str,
) -> str:
    """Upload a web-ready CSV string to S3.

    Parameters
    ----------
    csv_content:
        CSV string produced by :func:`build_web_ready_csv`.
    nova_id:
        Nova UUID string.
    data_product_id:
        DataProduct UUID string.
    s3:
        Injected boto3 S3 client.
    bucket:
        Name of the private S3 bucket.

    Returns
    -------
    str
        The S3 key that was written.
    """
    key = derive_web_ready_s3_key(nova_id, data_product_id)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_content.encode("utf-8"),
        ContentType=_CONTENT_TYPE,
    )
    return key
