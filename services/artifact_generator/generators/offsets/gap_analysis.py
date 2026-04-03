"""Pairwise gap analysis for the band offset algorithm.

For each pair of fitted band splines, this module computes the minimum
gap, maximum gap, crossing count, and overlap fraction of their difference
function ``g(t) = f_a(t) − f_b(t)`` over the shared time domain.

These statistics are collected into a :pydata:`GapTable` that drives the
ordering search (Chunk 4) and the crowding-aware trigger (clustering and
stack depth checks).

The implementation uses a numerical bracketing strategy: evaluate the
difference on a dense grid to locate sign changes and approximate extrema,
then refine with ``scipy.optimize.brentq`` (roots) and
``scipy.optimize.minimize_scalar`` (extrema).  This produces
machine-precision results while depending only on the ``FittedSpline``
protocol — no access to raw spline coefficients is required.

References
----------
- ADR-032 Decision 4: Crossing-Aware Constraint Evaluation
- ADR-032 Decision 1: Piecewise Smooth Approximation
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import numpy as np
from scipy.optimize import brentq, minimize_scalar  # type: ignore[import-untyped]

from .types import (
    DEFAULT_SEPARATION_THRESHOLD,
    FittedSpline,
    FloatArray,
    GapTable,
    PairwiseGap,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_GRID_SIZE: int = 1000
"""Number of evenly spaced evaluation points for bracketing.

With typical time domains of 100–5000 days and spline knot spacings of
10–50 days, 1000 points provides ~1–5 day resolution — well below the
spline feature scale, ensuring no roots or extrema are missed during
the bracketing pass.
"""

_ROOT_XTOL: float = 1e-10
"""Absolute tolerance for brentq root refinement (days).

Machine-precision convergence; the tolerance is far below any
physically meaningful timescale.
"""


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _shared_domain(
    domain_a: tuple[float, float],
    domain_b: tuple[float, float],
) -> tuple[float, float] | None:
    """Return the intersection of two time-domain intervals.

    Returns ``None`` if the intervals are disjoint or degenerate
    (zero-width overlap).
    """
    t_lo = max(domain_a[0], domain_b[0])
    t_hi = min(domain_a[1], domain_b[1])
    if t_lo >= t_hi:
        return None
    return (t_lo, t_hi)


def _make_diff_scalar(
    spline_a: FittedSpline,
    spline_b: FittedSpline,
) -> Callable[[float], float]:
    """Create a scalar function ``g(t) = spline_a(t) − spline_b(t)``.

    The returned callable accepts and returns plain floats, which is the
    signature expected by ``scipy.optimize.brentq`` and
    ``minimize_scalar``.
    """

    def g(t: float) -> float:
        arr = np.array([t], dtype=np.float64)
        return float(spline_a(arr)[0] - spline_b(arr)[0])

    return g


def _find_roots(
    t_grid: FloatArray,
    g_grid: FloatArray,
    g_scalar: Callable[[float], float],
) -> list[float]:
    """Find zero crossings of *g* via sign-change bracketing + refinement.

    Only strict sign changes (``g[i] * g[i+1] < 0``) are counted.
    Tangent touches — where the function reaches zero but does not change
    sign — are not crossings in the physical sense and are excluded.

    Parameters
    ----------
    t_grid:
        Dense grid of evaluation times.
    g_grid:
        Difference values ``g(t)`` at each grid point.
    g_scalar:
        Scalar callable for refinement with ``brentq``.

    Returns
    -------
    list[float]:
        Refined root locations, sorted ascending.
    """
    roots: list[float] = []
    n = len(g_grid)

    for i in range(n - 1):
        if g_grid[i] * g_grid[i + 1] < 0:
            try:
                root: float = brentq(
                    g_scalar,
                    float(t_grid[i]),
                    float(t_grid[i + 1]),
                    xtol=_ROOT_XTOL,
                )
                roots.append(root)
            except ValueError:
                # brentq can fail if the bracket is degenerate; skip.
                logger.debug(
                    "brentq failed on bracket [%f, %f]; skipping",
                    t_grid[i],
                    t_grid[i + 1],
                )

    return sorted(roots)


def _refine_extrema(
    t_grid: FloatArray,
    g_grid: FloatArray,
    t_lo: float,
    t_hi: float,
    g_scalar: Callable[[float], float],
) -> tuple[float, float]:
    """Find the global minimum and maximum of *g* over ``[t_lo, t_hi]``.

    Strategy: start from the grid-level approximate extrema, bracket each
    in a ±1-cell neighbourhood, and refine with ``minimize_scalar``
    (bounded method).  Domain endpoints are always included as candidates.

    Parameters
    ----------
    t_grid:
        Dense grid of evaluation times.
    g_grid:
        Difference values at each grid point.
    t_lo, t_hi:
        Domain boundaries.
    g_scalar:
        Scalar callable for refinement.

    Returns
    -------
    tuple[float, float]:
        ``(global_min, global_max)`` of *g* over the shared domain.
    """
    n = len(t_grid)

    # Endpoint values are always extrema candidates.
    g_lo = g_scalar(t_lo)
    g_hi = g_scalar(t_hi)
    min_val = min(g_lo, g_hi)
    max_val = max(g_lo, g_hi)

    # --- Refine grid-level minimum ---
    grid_min_idx = int(np.argmin(g_grid))
    bracket_lo = float(t_grid[max(0, grid_min_idx - 1)])
    bracket_hi = float(t_grid[min(n - 1, grid_min_idx + 1)])
    if bracket_lo < bracket_hi:
        result = minimize_scalar(g_scalar, bounds=(bracket_lo, bracket_hi), method="bounded")
        min_val = min(min_val, float(result.fun))
    else:
        min_val = min(min_val, float(g_grid[grid_min_idx]))

    # --- Refine grid-level maximum (minimise −g) ---
    grid_max_idx = int(np.argmax(g_grid))
    bracket_lo = float(t_grid[max(0, grid_max_idx - 1)])
    bracket_hi = float(t_grid[min(n - 1, grid_max_idx + 1)])
    if bracket_lo < bracket_hi:

        def neg_g(t: float) -> float:
            return -g_scalar(t)

        result = minimize_scalar(neg_g, bounds=(bracket_lo, bracket_hi), method="bounded")
        max_val = max(max_val, -float(result.fun))
    else:
        max_val = max(max_val, float(g_grid[grid_max_idx]))

    return (min_val, max_val)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_pairwise_gap(
    spline_a: FittedSpline,
    spline_b: FittedSpline,
    band_a: str,
    band_b: str,
    *,
    epsilon: float = DEFAULT_SEPARATION_THRESHOLD,
    grid_size: int = _DEFAULT_GRID_SIZE,
) -> PairwiseGap:
    """Compute gap statistics for one ordered pair of bands.

    The difference function is ``g(t) = spline_a(t) − spline_b(t)``,
    evaluated over the intersection of the two splines' time domains.

    Parameters
    ----------
    spline_a:
        Fitted spline for band *a* (the minuend).
    spline_b:
        Fitted spline for band *b* (the subtrahend).
    band_a:
        Canonical band identifier for band *a*.
    band_b:
        Canonical band identifier for band *b*.
    epsilon:
        Separation threshold in magnitudes.  Used to compute the
        overlap fraction (the proportion of the time domain where
        ``|g(t)| < ε``).
    grid_size:
        Number of evenly spaced evaluation points for the bracketing
        grid.  Higher values increase reliability at marginal cost
        (the grid evaluation is vectorised).

    Returns
    -------
    PairwiseGap:
        Gap statistics for the ``(band_a, band_b)`` pair.

    Raises
    ------
    ValueError
        If the two splines have non-overlapping time domains.
    """
    domain = _shared_domain(spline_a.domain, spline_b.domain)
    if domain is None:
        raise ValueError(
            f"Splines for bands {band_a!r} and {band_b!r} have "
            f"non-overlapping domains: {spline_a.domain} vs {spline_b.domain}"
        )

    t_lo, t_hi = domain
    t_grid: FloatArray = np.linspace(t_lo, t_hi, grid_size, dtype=np.float64)
    g_grid: FloatArray = spline_a(t_grid) - spline_b(t_grid)

    g_scalar = _make_diff_scalar(spline_a, spline_b)

    # --- Roots (crossings) ---
    roots = _find_roots(t_grid, g_grid, g_scalar)

    # --- Extrema ---
    min_gap, max_gap = _refine_extrema(t_grid, g_grid, t_lo, t_hi, g_scalar)

    # --- Overlap fraction ---
    # Fraction of the grid where the two bands are within ε of each other.
    overlap_count = int(np.count_nonzero(np.abs(g_grid) < epsilon))
    overlap_fraction = overlap_count / len(g_grid)

    return PairwiseGap(
        band_a=band_a,
        band_b=band_b,
        min_gap=min_gap,
        max_gap=max_gap,
        crossing_count=len(roots),
        overlap_fraction=overlap_fraction,
    )


def build_gap_table(
    splines: dict[str, FittedSpline],
    *,
    epsilon: float = DEFAULT_SEPARATION_THRESHOLD,
    grid_size: int = _DEFAULT_GRID_SIZE,
) -> GapTable:
    """Build the full pairwise gap table for all bands.

    Only pairs with overlapping time domains are included.  Pairs are
    stored with their band identifiers in lexicographic order
    ``(band_a < band_b)`` as required by the :pydata:`GapTable` contract.

    Pairs with non-overlapping domains are silently skipped — they
    impose no separation constraint (bands that never coexist in time
    cannot visually overlap).

    Parameters
    ----------
    splines:
        Mapping from band identifier to fitted spline, for all bands
        participating in offset computation.
    epsilon:
        Separation threshold in magnitudes.  Passed through to
        :func:`compute_pairwise_gap` for overlap fraction computation.
    grid_size:
        Passed through to :func:`compute_pairwise_gap`.

    Returns
    -------
    GapTable:
        Precomputed gap records keyed by ``(band_a, band_b)`` tuples.
    """
    band_ids = sorted(splines.keys())
    table: GapTable = {}

    for i, band_a in enumerate(band_ids):
        for band_b in band_ids[i + 1 :]:
            domain = _shared_domain(splines[band_a].domain, splines[band_b].domain)
            if domain is None:
                logger.debug(
                    "Bands %r and %r have non-overlapping domains; skipping gap computation",
                    band_a,
                    band_b,
                )
                continue

            gap = compute_pairwise_gap(
                splines[band_a],
                splines[band_b],
                band_a,
                band_b,
                epsilon=epsilon,
                grid_size=grid_size,
            )
            table[(band_a, band_b)] = gap

    logger.info(
        "Gap table built: %d pairs computed, %d pairs skipped (non-overlapping)",
        len(table),
        len(band_ids) * (len(band_ids) - 1) // 2 - len(table),
    )
    return table


def peak_stack_depth(
    splines: dict[str, FittedSpline],
    epsilon: float = DEFAULT_SEPARATION_THRESHOLD,
    *,
    grid_size: int = _DEFAULT_GRID_SIZE,
) -> int:
    """Compute the worst-case band stacking depth across all epochs.

    At each epoch on a dense time grid, this function counts the maximum
    number of bands whose magnitudes fall within a sliding window of
    width ε.  The return value is the global maximum of that count.

    A peak stack depth of 1 means no two bands are ever within ε of
    each other at the same epoch.  A depth of 2 means at most two bands
    overlap at any point.  The pipeline uses this to decide whether
    offsets are needed at all: if the depth is below
    ``DEFAULT_MIN_STACK_DEPTH_TRIGGER``, the plot is considered legible
    without offsets.

    Parameters
    ----------
    splines:
        Mapping from band identifier to fitted spline.
    epsilon:
        Width of the sliding magnitude window.
    grid_size:
        Number of evenly spaced evaluation points across the global
        time domain.

    Returns
    -------
    int:
        The maximum number of bands that simultaneously fall within an
        ε-wide magnitude window at any epoch.  Returns 0 if *splines*
        is empty.
    """
    if not splines:
        return 0

    # --- Build a global time grid spanning all spline domains ---
    t_min = min(s.domain[0] for s in splines.values())
    t_max = max(s.domain[1] for s in splines.values())
    t_grid: FloatArray = np.linspace(t_min, t_max, grid_size, dtype=np.float64)

    # --- Evaluate each spline at grid points within its domain ---
    # For each grid point, collect the magnitudes of all bands active there.
    band_list = list(splines.items())
    n_bands = len(band_list)

    # Pre-evaluate: for each band, compute a mask of valid grid points
    # and the magnitude values at those points.
    masks: list[FloatArray] = []
    values: list[FloatArray] = []
    for _band_id, spline in band_list:
        lo, hi = spline.domain
        mask: FloatArray = np.asarray((t_grid >= lo) & (t_grid <= hi), dtype=np.float64)
        # Evaluate at ALL grid points (extrapolated values are ignored via mask).
        # This avoids per-band fancy indexing and keeps the loop simple.
        vals: FloatArray = spline(t_grid)
        masks.append(mask)
        values.append(vals)

    # --- Sliding-window stack depth at each epoch ---
    global_max_depth = 0
    if n_bands == 1:
        return 1

    for gi in range(grid_size):
        # Collect magnitudes of bands active at this grid point.
        mags: list[float] = []
        for bi in range(n_bands):
            if masks[bi][gi] > 0.5:  # mask is 0.0 or 1.0
                mags.append(float(values[bi][gi]))

        n_active = len(mags)
        if n_active <= 1:
            global_max_depth = max(global_max_depth, n_active)
            continue

        # Sort and slide a window of width epsilon.
        mags.sort()
        max_depth_here = 1
        left = 0
        for right in range(1, n_active):
            while mags[right] - mags[left] >= epsilon:
                left += 1
            max_depth_here = max(max_depth_here, right - left + 1)

        global_max_depth = max(global_max_depth, max_depth_here)

    logger.debug(
        "Peak stack depth: %d (across %d bands, %d grid points)",
        global_max_depth,
        n_bands,
        grid_size,
    )
    return global_max_depth
