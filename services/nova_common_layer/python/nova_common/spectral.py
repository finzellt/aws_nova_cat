"""nova_common.spectral — spectral analysis utilities.

Provides signal-to-noise estimation for spectra that lack a native SNR
value from the archive or instrument pipeline.

Public surface:
    der_snr(flux)  — Stoehr et al. (2008) second-difference SNR estimator
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from numpy.typing import ArrayLike

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Converts MAD (median absolute deviation) to standard deviation for a
# Gaussian distribution.  MAD = σ × Φ⁻¹(3/4) ≈ σ × 0.6745, so
# σ ≈ MAD / 0.6745 ≈ MAD × 1.482602.
_MAD_TO_SIGMA = 1.482602

# The second-difference operator 2f[i] − f[i−2] − f[i+2] applied to
# independent Gaussian noise with variance σ² has variance 6σ².
# Therefore noise_estimate = MAD_TO_SIGMA × median(|diff|) / √6.
_NOISE_DENOMINATOR = np.sqrt(6.0)

# Minimum number of flux values required for a meaningful estimate.
# The second-difference operator needs indices [2 .. n-3], so fewer
# than 5 points yields an empty differenced array.
_MIN_POINTS = 5


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def der_snr(flux: ArrayLike) -> float:
    """Estimate per-pixel SNR using the DER_SNR method.

    Implements the algorithm from Stoehr et al. (2008, *ST-ECF Newsletter*
    42, 4) — the same method used by ESO's Quality Control pipeline.

    The method exploits the fact that for a smoothly varying signal with
    additive noise, the second derivative is dominated by the noise term.
    A median-based estimator makes it robust to the sharp spectral features
    (emission/absorption lines) that are common in nova spectra.

    Parameters
    ----------
    flux : array-like
        1-D flux array.  Need not be continuum-subtracted or normalised.
        NaN and non-finite values are removed before computation.

    Returns
    -------
    float
        Estimated median signal-to-noise ratio per pixel.
        Returns 0.0 if the array has fewer than ``_MIN_POINTS`` finite
        values, or if the estimated noise is zero (constant spectrum).

    Notes
    -----
    **Limitations for nova spectra:** Very line-dominated spectra (late
    nebular phase with almost no continuum) can yield unreliable estimates
    because the "smooth signal" assumption breaks down for the majority of
    pixels.  For most early/mid-phase nova spectra with visible continua,
    the method produces reliable results.

    The method is insensitive to the wavelength grid — only the flux values
    matter.  Wavelength units, spacing, and calibration are irrelevant.
    """
    arr = np.asarray(flux, dtype=np.float64).ravel()

    # Strip non-finite values (NaN, inf).
    arr = arr[np.isfinite(arr)]

    if len(arr) < _MIN_POINTS:
        return 0.0

    # Second-difference with stride 2: 2·f[i] − f[i−2] − f[i+2]
    # Array indices: i runs from 2 to n−3 inclusive.
    n = len(arr)
    diff = np.abs(2.0 * arr[2 : n - 2] - arr[0 : n - 4] - arr[4:n])

    median_diff = float(np.median(diff))
    noise = _MAD_TO_SIGMA * median_diff / _NOISE_DENOMINATOR

    if noise <= 0.0:
        return 0.0

    # Signal estimate: median of the same interior region.
    signal = float(np.median(arr[2 : n - 2]))

    if signal <= 0.0:
        # Negative or zero median flux — SNR is not meaningful.
        return 0.0

    return float(signal / noise)
