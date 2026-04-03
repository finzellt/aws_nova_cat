"""Ordering search and offset propagation for the band offset algorithm.

This module contains the combinatorial core of the offset algorithm:
given a precomputed :pydata:`GapTable`, find the vertical ordering of
bands that minimises total offset cost, then compute the raw offsets
for that ordering.

The search is exhaustive over all *n*! permutations (ADR-032 Decision 2).
This is tractable for optical-regime band counts (*n* ≤ ~10).

**Offset convention (internal).** The pseudocode anchors σ(1) — the
faintest band (bottom of the inverted-magnitude plot) — at δ = 0 and
propagates upward.  Raw offsets are therefore non-negative and
monotonically increasing along the chain.  The normalisation to the
output contract (brightest band at 0, offsets in the fainter direction)
is performed by the rounding module (Chunk 5).

References
----------
- ADR-032 Decision 2: Global Ordering with Exhaustive Search
- ADR-032 Decision 5: Zero-Offset Fast Path
"""

from __future__ import annotations

import itertools
import logging
import math

from .types import (
    DEFAULT_MAX_OVERLAP_FRACTION,
    DEFAULT_SEPARATION_THRESHOLD,
    GapTable,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WARN_BAND_COUNT: int = 8
"""Log a warning when more than this many bands enter the permutation search.

At n = 8 the search evaluates 40,320 permutations — still fast, but
approaching the boundary where wall-clock time becomes noticeable.
"""

_MAX_BAND_COUNT: int = 12
"""Hard cap on band count for the exhaustive search.

12! = 479,001,600 permutations.  Beyond this the factorial growth makes
exhaustive search impractical and a heuristic should be used instead
(ADR-032 Decision 2, scalability note).
"""


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def fast_path_check(
    gap_table: GapTable,
    epsilon: float,
    *,
    max_overlap_fraction: float = DEFAULT_MAX_OVERLAP_FRACTION,
) -> bool:
    """Check whether all band pairs are effectively separated.

    A pair is considered effectively separated if **either**:

    1. **Naturally separated.** The gap function maintains a consistent
       sign with absolute value > ε throughout the shared time domain
       (one band is *always* at least ε fainter than the other).
    2. **Transiently overlapping.** The pair spends less than
       *max_overlap_fraction* of the shared time domain within ε of
       each other.  Brief crossings or near-collisions that don't
       meaningfully reduce readability are tolerated.

    Pairs not present in the gap table (non-overlapping time domains)
    are trivially separated.

    Parameters
    ----------
    gap_table:
        Precomputed pairwise gap statistics.
    epsilon:
        Separation threshold in magnitudes.
    max_overlap_fraction:
        Maximum fraction of the shared time domain a pair may spend
        within ε before it is considered "overlapping".  Pairs below
        this threshold pass the fast-path check even if their absolute
        gap dips below ε momentarily.

    Returns
    -------
    bool:
        ``True`` if every pair is effectively separated and no offsets
        are needed.

    Notes
    -----
    The ADR-032 pseudocode uses ``min(|min_gap|, |max_gap|) > ε`` for
    this check.  That formula is incorrect for crossing bands (where
    ``min_gap < 0 < max_gap``): it yields a positive value even though
    ``min_t |g(t)| = 0`` at the crossing.  This implementation uses the
    corrected check: ``(min_gap > ε) or (max_gap < −ε)``, extended
    with an overlap fraction tolerance.
    """
    for gap in gap_table.values():
        naturally_separated = gap.min_gap > epsilon or gap.max_gap < -epsilon
        transiently_overlapping = gap.overlap_fraction < max_overlap_fraction

        if not (naturally_separated or transiently_overlapping):
            return False
    return True


def min_gap_for_ordering(
    gap_table: GapTable,
    band_a: str,
    band_b: str,
) -> float:
    """Look up ``min_t[f_a(t) − f_b(t)]`` from the precomputed gap table.

    The gap table stores entries only for lexicographically ordered pairs
    ``(a < b)``.  For the reversed pair the identity
    ``min(f_a − f_b) = −max(f_b − f_a)`` is applied.

    Parameters
    ----------
    gap_table:
        Precomputed pairwise gap statistics.
    band_a:
        Band identifier for the minuend spline.
    band_b:
        Band identifier for the subtrahend spline.

    Returns
    -------
    float:
        The minimum value of the directed gap ``f_a(t) − f_b(t)`` over
        the shared time domain.  Returns ``math.inf`` for pairs not in
        the gap table (non-overlapping domains), signalling that no
        separation constraint exists between them.
    """
    if band_a == band_b:
        return 0.0

    if band_a < band_b:
        key = (band_a, band_b)
        if key in gap_table:
            return gap_table[key].min_gap
        return math.inf

    # band_a > band_b: reverse lookup.
    key = (band_b, band_a)
    if key in gap_table:
        return -gap_table[key].max_gap
    return math.inf


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _evaluate_permutation(
    perm: tuple[str, ...],
    gap_table: GapTable,
    epsilon: float,
) -> tuple[dict[str, float], float]:
    """Compute raw offsets and cost for a single candidate ordering.

    The permutation is ordered bottom-to-top in the inverted-magnitude
    convention: ``perm[0]`` is the faintest band (anchored at δ = 0),
    ``perm[-1]`` is the brightest.

    Parameters
    ----------
    perm:
        Candidate ordering of band identifiers.
    gap_table:
        Precomputed pairwise gap statistics.
    epsilon:
        Separation threshold in magnitudes.

    Returns
    -------
    tuple[dict[str, float], float]:
        ``(offsets, cost)`` where *offsets* maps each band to its raw
        offset (non-negative, σ(1) = 0) and *cost* is ``Σ δ_i``.
    """
    offsets: dict[str, float] = {perm[0]: 0.0}

    for k in range(len(perm) - 1):
        lower = perm[k]  # fainter (below on inverted-mag plot)
        upper = perm[k + 1]  # brighter (above)

        # The gap between the lower and upper band in this ordering.
        # Positive means lower is already fainter than upper (good).
        gap = min_gap_for_ordering(gap_table, lower, upper)
        constraint = epsilon - gap
        offsets[upper] = offsets[lower] + max(0.0, constraint)

    # All offsets are non-negative; cost is their sum.
    cost = sum(offsets.values())
    return offsets, cost


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def find_optimal_offsets(
    gap_table: GapTable,
    band_ids: list[str],
    epsilon: float = DEFAULT_SEPARATION_THRESHOLD,
) -> dict[str, float]:
    """Find the band ordering that minimises total offset cost.

    Evaluates all *n*! permutations of *band_ids* and selects the one
    with the smallest ``Σ δ_i`` (ADR-032 Decision 2).

    **Offset convention (internal).** The returned offsets anchor the
    faintest band in the optimal ordering at 0.0 and accumulate
    upward.  All values are non-negative.  Normalisation to the output
    contract (brightest band at 0, offsets in the fainter direction) is
    the responsibility of the rounding module (Chunk 5).

    Parameters
    ----------
    gap_table:
        Precomputed pairwise gap statistics.
    band_ids:
        List of band identifiers to order.  Must contain at least one
        band.
    epsilon:
        Separation threshold in magnitudes.

    Returns
    -------
    dict[str, float]:
        Mapping from band identifier to raw offset (non-negative).

    Raises
    ------
    ValueError
        If *band_ids* is empty or contains more than
        ``_MAX_BAND_COUNT`` (12) bands.
    """
    n = len(band_ids)

    if n == 0:
        raise ValueError("band_ids must be non-empty")

    if n == 1:
        return {band_ids[0]: 0.0}

    if n > _MAX_BAND_COUNT:
        raise ValueError(
            f"Exhaustive ordering search is limited to {_MAX_BAND_COUNT} "
            f"bands ({_MAX_BAND_COUNT}! = "
            f"{math.factorial(_MAX_BAND_COUNT):,} permutations); "
            f"got {n} bands. A heuristic search is needed at this scale "
            f"(see ADR-032 Decision 2, scalability note)."
        )

    if n > _WARN_BAND_COUNT:
        n_perms = math.factorial(n)
        logger.warning(
            "Permutation search over %d bands (%s permutations); this may be slow",
            n,
            f"{n_perms:,}",
        )

    best_cost = math.inf
    best_offsets: dict[str, float] = {}
    best_perm: tuple[str, ...] = ()

    for perm in itertools.permutations(band_ids):
        offsets, cost = _evaluate_permutation(perm, gap_table, epsilon)
        if cost < best_cost:
            best_cost = cost
            best_offsets = offsets
            best_perm = perm

    logger.info(
        "Optimal ordering found: cost=%.4f, ordering=%s (bottom → top)",
        best_cost,
        list(best_perm),
    )

    return best_offsets
