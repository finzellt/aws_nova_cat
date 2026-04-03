"""Pipeline orchestrator for the per-band photometric offset algorithm.

This module provides the single public entry point for the offset
computation: :func:`compute_band_offsets`.  It wires together all
preceding pipeline stages — spline fitting, pairwise gap analysis,
stack depth evaluation, cluster partitioning, ordering search, and
rounding — into a coherent end-to-end flow.

The enhanced pipeline incorporates three crowding-aware features beyond
the base ADR-032 algorithm:

1. **Overlap fraction tolerance.** Transient near-collisions that
   affect less than ``max_overlap_fraction`` of the time domain are
   tolerated without triggering offsets.
2. **Stack depth gating.** Offsets are only computed when the peak
   number of bands piled within ε at any single epoch meets or exceeds
   ``min_stack_depth_trigger``.
3. **Cluster partitioning.** Bands are grouped into independent overlap
   clusters.  Only bands that are transitively connected by significant
   overlap enter the permutation search.  Isolated bands receive zero
   offset automatically.

References
----------
- ADR-032: Per-Band Photometric Offset Algorithm
- DESIGN-003 §8.7: Band Offset Computation and Caching (interface contract)
"""

from __future__ import annotations

import logging

from .clustering import partition_into_clusters
from .gap_analysis import build_gap_table, peak_stack_depth
from .ordering import fast_path_check, find_optimal_offsets
from .rounding import round_and_assemble
from .spline_fitting import fit_band_spline
from .types import (
    DEFAULT_MAX_OVERLAP_FRACTION,
    DEFAULT_MIN_STACK_DEPTH_TRIGGER,
    DEFAULT_SEPARATION_THRESHOLD,
    MIN_INTERPOLATION_POINTS,
    BandObservations,
    BandOffsetResult,
    FittedSpline,
    GapTable,
    OffsetDirection,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _zero_result(band_id: str) -> BandOffsetResult:
    """Construct a zero-offset result for a band that needs no displacement."""
    return BandOffsetResult(
        band_id=band_id,
        offset_mag=0.0,
        offset_direction=OffsetDirection.none,
    )


def _cluster_gap_table(
    gap_table: GapTable,
    cluster_bands: set[str],
) -> GapTable:
    """Extract the subset of a gap table relevant to a single cluster."""
    return {
        key: gap
        for key, gap in gap_table.items()
        if key[0] in cluster_bands and key[1] in cluster_bands
    }


def _solve_cluster(
    cluster_band_ids: list[str],
    gap_table: GapTable,
    epsilon: float,
    max_overlap_fraction: float,
) -> list[BandOffsetResult]:
    """Run the offset algorithm for a single overlap cluster.

    Returns a list of :class:`BandOffsetResult` for the bands in the
    cluster.  If the fast-path check passes, all bands receive zero
    offset.
    """
    sub_table = _cluster_gap_table(gap_table, set(cluster_band_ids))

    if fast_path_check(sub_table, epsilon, max_overlap_fraction=max_overlap_fraction):
        logger.debug(
            "Cluster %s passes fast-path check; all zero offsets",
            cluster_band_ids,
        )
        return [_zero_result(bid) for bid in cluster_band_ids]

    raw_offsets = find_optimal_offsets(sub_table, cluster_band_ids, epsilon)
    return round_and_assemble(raw_offsets)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_band_offsets(
    bands: list[BandObservations],
    *,
    epsilon: float = DEFAULT_SEPARATION_THRESHOLD,
    max_overlap_fraction: float = DEFAULT_MAX_OVERLAP_FRACTION,
    min_stack_depth_trigger: int = DEFAULT_MIN_STACK_DEPTH_TRIGGER,
) -> list[BandOffsetResult]:
    """Compute per-band magnitude offsets for a set of photometric bands.

    This is the top-level entry point for the offset pipeline.  It
    accepts subsampled, filtered observation arrays (the output of
    DESIGN-003 §8.6) and returns one :class:`BandOffsetResult` per
    input band.

    Parameters
    ----------
    bands:
        Subsampled observations for each band in a single wavelength
        regime.
    epsilon:
        Minimum required separation between consecutive bands in
        magnitudes (ADR-032 Decision 3).
    max_overlap_fraction:
        Maximum fraction of the shared time domain two bands may spend
        within ε before the pair is considered "overlapping."  Pairs
        below this threshold are tolerated without triggering offsets.
    min_stack_depth_trigger:
        Minimum peak stack depth (number of bands within ε at any
        single epoch) required to trigger offset computation.  If the
        peak depth is below this, all bands receive zero offset.

    Returns
    -------
    list[BandOffsetResult]:
        One result per input band, sorted by ``band_id``.  Bands
        excluded from computation (< 2 observations, spline fitting
        failure, below stack depth trigger, or in a singleton cluster)
        receive ``offset_mag = 0.0``.
    """
    n_input = len(bands)

    # --- Trivial cases ---
    if n_input == 0:
        return []

    if n_input == 1:
        return [_zero_result(bands[0].band_id)]

    # --- Partition into fittable vs excluded ---
    fittable: list[BandObservations] = []
    excluded_ids: list[str] = []

    for obs in bands:
        if len(obs.mjd) < MIN_INTERPOLATION_POINTS:
            excluded_ids.append(obs.band_id)
        else:
            fittable.append(obs)

    if excluded_ids:
        logger.info(
            "%d band(s) excluded (< %d observations): %s",
            len(excluded_ids),
            MIN_INTERPOLATION_POINTS,
            excluded_ids,
        )

    # --- Fit splines ---
    splines: dict[str, FittedSpline] = {}
    for obs in fittable:
        try:
            splines[obs.band_id] = fit_band_spline(obs, epsilon)
        except ValueError:
            logger.warning(
                "Band %r failed spline fitting; assigning zero offset",
                obs.band_id,
            )
            excluded_ids.append(obs.band_id)

    if len(splines) <= 1:
        logger.info(
            "≤1 fittable band after spline fitting; all zero offsets",
        )
        return sorted(
            [_zero_result(bid) for bid in excluded_ids] + [_zero_result(bid) for bid in splines],
            key=lambda r: r.band_id,
        )

    # --- Build gap table with overlap fractions ---
    gap_table = build_gap_table(splines, epsilon=epsilon)

    # --- Stack depth gating ---
    depth = peak_stack_depth(splines, epsilon)
    if depth < min_stack_depth_trigger:
        logger.info(
            "Peak stack depth %d is below trigger %d; all zero offsets",
            depth,
            min_stack_depth_trigger,
        )
        return sorted(
            [_zero_result(bid) for bid in excluded_ids] + [_zero_result(bid) for bid in splines],
            key=lambda r: r.band_id,
        )

    # --- Cluster partitioning ---
    fittable_ids = list(splines.keys())
    clusters = partition_into_clusters(
        gap_table,
        fittable_ids,
        max_overlap_fraction=max_overlap_fraction,
    )

    # --- Per-cluster solving ---
    results: list[BandOffsetResult] = []

    for cluster in clusters:
        if len(cluster) == 1:
            results.append(_zero_result(cluster[0]))
        else:
            cluster_results = _solve_cluster(cluster, gap_table, epsilon, max_overlap_fraction)
            results.extend(cluster_results)

    # --- Merge excluded bands ---
    results.extend(_zero_result(bid) for bid in excluded_ids)

    # --- Sort for deterministic output ---
    results.sort(key=lambda r: r.band_id)

    logger.info(
        "Offset computation complete: %d bands, %d with non-zero offset",
        len(results),
        sum(1 for r in results if r.offset_mag > 0.0),
    )

    return results
