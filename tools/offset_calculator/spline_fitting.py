"""Spline fitting for the band offset algorithm.

Each photometric band's time series is approximated as a smooth curve
before offset computation.  This module handles the fitting step and
returns objects satisfying the :class:`FittedSpline` protocol defined
in ``types.py``.

Routing logic (ADR-032 Decision 1):

* **≥ 4 unique epochs** → cubic smoothing spline
  (``scipy.interpolate.UnivariateSpline``, GCV-based smoothing).
* **2–3 unique epochs** → piecewise linear interpolant
  (``numpy.interp``).
* **< 2 unique epochs** → not fittable; raises ``ValueError``.
  The pipeline orchestrator (Chunk 6) is responsible for catching
  this and assigning zero offset.

A residual guard ensures the cubic spline does not deviate from the
raw data by more than ``ε / 4`` (where ε is the separation threshold).
If the guard trips, the fitter falls back to an interpolating spline
(``scipy.interpolate.InterpolatedUnivariateSpline``, s = 0) which
passes through every data point exactly.

References
----------
- ADR-032 Decision 1: Piecewise Smooth Approximation
- ADR-032 Decision 3: Separation Threshold (ε = 0.5 mag)
"""

from __future__ import annotations

import logging

import numpy as np
from scipy.interpolate import (  # type: ignore[import-untyped]
    InterpolatedUnivariateSpline,
    UnivariateSpline,
)

from .types import (
    DEFAULT_SEPARATION_THRESHOLD,
    MAX_RESIDUAL_FRACTION,
    MIN_CUBIC_POINTS,
    MIN_INTERPOLATION_POINTS,
    BandObservations,
    FloatArray,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Concrete FittedSpline implementations
# ---------------------------------------------------------------------------


class CubicSplineWrapper:
    """Wrapper around a scipy univariate spline.

    Satisfies the :class:`~.types.FittedSpline` protocol.  Used for both
    ``UnivariateSpline`` (smoothing) and ``InterpolatedUnivariateSpline``
    (exact interpolation) since they share a calling convention.
    """

    __slots__ = ("_spline", "_domain")

    def __init__(
        self,
        spline: UnivariateSpline | InterpolatedUnivariateSpline,
        t_min: float,
        t_max: float,
    ) -> None:
        self._spline = spline
        self._domain = (t_min, t_max)

    @property
    def domain(self) -> tuple[float, float]:
        """The ``(t_min, t_max)`` interval over which the spline is defined."""
        return self._domain

    def __call__(self, t: FloatArray) -> FloatArray:
        """Evaluate the spline at the given MJD values."""
        result: FloatArray = np.asarray(self._spline(t), dtype=np.float64)
        return result


class LinearSplineWrapper:
    """Piecewise linear interpolant for sparse bands (2–3 points).

    Satisfies the :class:`~.types.FittedSpline` protocol.  Uses
    ``numpy.interp`` under the hood — values outside the data range
    are clamped to the boundary values.
    """

    __slots__ = ("_mjd", "_mag", "_domain")

    def __init__(self, mjd: FloatArray, mag: FloatArray) -> None:
        self._mjd = mjd
        self._mag = mag
        self._domain = (float(mjd[0]), float(mjd[-1]))

    @property
    def domain(self) -> tuple[float, float]:
        """The ``(t_min, t_max)`` interval over which the interpolant is defined."""
        return self._domain

    def __call__(self, t: FloatArray) -> FloatArray:
        """Evaluate the piecewise linear interpolant at the given MJD values."""
        result: FloatArray = np.asarray(np.interp(t, self._mjd, self._mag), dtype=np.float64)
        return result


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _deduplicate_and_sort(mjd: FloatArray, mag: FloatArray) -> tuple[FloatArray, FloatArray]:
    """Sort by MJD and average magnitudes at duplicate epochs.

    ``scipy.interpolate.UnivariateSpline`` requires strictly increasing
    *x* values.  This function ensures that constraint by sorting and
    collapsing duplicate epochs to their mean magnitude.

    Returns
    -------
    tuple[FloatArray, FloatArray]:
        ``(unique_mjd, averaged_mag)`` both sorted ascending.
    """
    order = np.argsort(mjd)
    mjd_sorted: FloatArray = np.asarray(mjd[order], dtype=np.float64)
    mag_sorted: FloatArray = np.asarray(mag[order], dtype=np.float64)

    unique_mjd, inverse = np.unique(mjd_sorted, return_inverse=True)
    if len(unique_mjd) == len(mjd_sorted):
        return mjd_sorted, mag_sorted

    # Average magnitudes at duplicate epochs.
    unique_mag = np.zeros_like(unique_mjd)
    counts = np.zeros_like(unique_mjd)
    np.add.at(unique_mag, inverse, mag_sorted)
    np.add.at(counts, inverse, 1.0)
    unique_mag = unique_mag / counts

    n_dupes = len(mjd_sorted) - len(unique_mjd)
    logger.debug(
        "Collapsed %d duplicate MJD values (%d → %d unique epochs)",
        n_dupes,
        len(mjd_sorted),
        len(unique_mjd),
    )

    return (
        np.asarray(unique_mjd, dtype=np.float64),
        np.asarray(unique_mag, dtype=np.float64),
    )


def _fit_cubic(
    mjd: FloatArray,
    mag: FloatArray,
    max_residual: float,
) -> CubicSplineWrapper:
    """Fit a cubic smoothing spline with a residual guard.

    Strategy:

    1. Fit with ``UnivariateSpline`` using the default GCV-based
       smoothing parameter.
    2. Evaluate the spline at the original data points and check the
       maximum absolute residual.
    3. If the residual exceeds *max_residual*, fall back to
       ``InterpolatedUnivariateSpline`` (s = 0, exact interpolation).

    .. note::

       ADR-032 Decision 1 states *"the smoothing should be relaxed
       (higher s)"* when the residual guard trips.  Higher *s* in
       scipy's convention means *more* smoothing, which would
       *increase* residuals — the opposite of the stated goal.  This
       implementation follows the **intent** (reduce the spline's
       deviation from the data) by falling back to exact interpolation.
    """
    spline = UnivariateSpline(mjd, mag, k=3)

    fitted: FloatArray = np.asarray(spline(mjd), dtype=np.float64)
    residual = float(np.max(np.abs(mag - fitted)))

    if residual <= max_residual:
        logger.debug(
            "Cubic spline max residual %.4f within threshold %.4f",
            residual,
            max_residual,
        )
        return CubicSplineWrapper(spline, float(mjd[0]), float(mjd[-1]))

    # Residual too large — fall back to interpolating spline.
    logger.info(
        "Cubic spline max residual (%.4f) exceeds threshold (%.4f); "
        "falling back to interpolating spline (s=0)",
        residual,
        max_residual,
    )
    interp_spline = InterpolatedUnivariateSpline(mjd, mag, k=3)
    return CubicSplineWrapper(interp_spline, float(mjd[0]), float(mjd[-1]))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fit_band_spline(
    obs: BandObservations,
    epsilon: float = DEFAULT_SEPARATION_THRESHOLD,
) -> CubicSplineWrapper | LinearSplineWrapper:
    """Fit a spline to a photometric band's observations.

    The returned object satisfies the :class:`~.types.FittedSpline`
    protocol and can be used directly in pairwise gap analysis
    (Chunk 3).

    Parameters
    ----------
    obs:
        Subsampled observations for a single band.
    epsilon:
        Separation threshold (magnitudes).  Used to compute the
        residual guard: ``max_residual = epsilon * MAX_RESIDUAL_FRACTION``.

    Returns
    -------
    CubicSplineWrapper | LinearSplineWrapper:
        A fitted spline satisfying the ``FittedSpline`` protocol.

    Raises
    ------
    ValueError
        If the band has fewer than ``MIN_INTERPOLATION_POINTS`` (2)
        unique epochs after deduplication.
    """
    n_raw = len(obs.mjd)

    if n_raw < MIN_INTERPOLATION_POINTS:
        raise ValueError(
            f"Band {obs.band_id!r} has {n_raw} observations, fewer than "
            f"the minimum {MIN_INTERPOLATION_POINTS} required for interpolation"
        )

    mjd, mag = _deduplicate_and_sort(obs.mjd, obs.mag)
    n_unique = len(mjd)

    if n_unique < MIN_INTERPOLATION_POINTS:
        raise ValueError(
            f"Band {obs.band_id!r} has {n_unique} unique epochs after "
            f"deduplication, fewer than the minimum {MIN_INTERPOLATION_POINTS}"
        )

    if n_unique < MIN_CUBIC_POINTS:
        logger.debug(
            "Band %r has %d unique epochs; using piecewise linear interpolation",
            obs.band_id,
            n_unique,
        )
        return LinearSplineWrapper(mjd, mag)

    max_residual = epsilon * MAX_RESIDUAL_FRACTION
    logger.debug(
        "Fitting cubic spline for band %r (%d unique epochs, max_residual=%.4f)",
        obs.band_id,
        n_unique,
        max_residual,
    )
    return _fit_cubic(mjd, mag, max_residual)
