"""Domain types for the per-band photometric offset algorithm.

This module defines the data structures that flow through the offset
computation pipeline specified in ADR-032.  All types are immutable
value objects (frozen dataclasses) or structural protocols; none carry
behaviour beyond validation.

The pipeline flow is::

    BandObservations
        → (spline fitting) → FittedSpline instances
        → (pairwise analysis) → PairwiseGap records collected into a GapTable
        → (stack depth check) → bail early if peak crowding is low
        → (cluster partitioning) → independent overlap groups
        → (per-cluster ordering search + rounding) → BandOffsetResult list

References
----------
- ADR-032: Per-Band Photometric Offset Algorithm
- DESIGN-003 §8.7: Band Offset Computation and Caching
- ADR-013: Visualization Design (half-integer rounding, legend format)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol, TypeAlias

import numpy as np
import numpy.typing as npt

# ---------------------------------------------------------------------------
# Array alias (matches fits_builder.py convention)
# ---------------------------------------------------------------------------

FloatArray: TypeAlias = npt.NDArray[np.float64]

# ---------------------------------------------------------------------------
# Constants (ADR-032 Decision 3)
# ---------------------------------------------------------------------------

DEFAULT_SEPARATION_THRESHOLD: float = 0.5
"""Minimum required separation (ε) between consecutive bands in magnitudes.

This is the smallest half-integer increment permitted by the ADR-013
rounding convention.  It provides clear visual separation in the Plotly.js
renderer at typical zoom levels.
"""

MAX_RESIDUAL_FRACTION: float = 0.25
"""Maximum spline residual as a fraction of ε (ADR-032 Decision 1).

If the maximum absolute residual of a cubic smoothing spline exceeds
``ε * MAX_RESIDUAL_FRACTION`` for any band, the smoothing should be
relaxed for that band.
"""

MIN_CUBIC_POINTS: int = 4
"""Minimum observation count for cubic spline fitting (ADR-032 Decision 1).

Bands with fewer points fall back to piecewise linear interpolation.
"""

MIN_INTERPOLATION_POINTS: int = 2
"""Minimum observation count for any interpolation (ADR-032 Decision 1).

Bands with fewer points are excluded from offset computation and receive
zero offset.
"""

# ---------------------------------------------------------------------------
# Crowding-aware trigger constants
# ---------------------------------------------------------------------------

DEFAULT_MAX_OVERLAP_FRACTION: float = 0.15
"""Maximum fraction of the shared time domain two bands may spend within ε
of each other before the pair is considered "overlapping".

A pair whose overlap fraction is below this threshold is treated as
effectively separated, even if the absolute gap dips below ε momentarily
(e.g., during a brief crossing).  This prevents the offset algorithm from
firing on transient near-collisions that don't meaningfully reduce
readability.
"""

DEFAULT_MIN_STACK_DEPTH_TRIGGER: int = 3
"""Minimum peak stack depth required to trigger offset computation.

Stack depth is the maximum number of bands that fall within an ε-wide
magnitude window at any single epoch.  When the peak stack depth is below
this threshold, the plot is considered legible without offsets — even if
some pairwise overlaps exist — and all bands receive zero offset.

The default of 3 reflects the observation that two overlapping bands are
still individually trackable, while three or more become difficult to
distinguish.
"""


# ---------------------------------------------------------------------------
# Input type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BandObservations:
    """Subsampled observations for a single photometric band.

    This is the input to the offset pipeline.  Arrays are produced by the
    density-preserving log subsampling step (DESIGN-003 §8.6) and must
    already be sorted by ``mjd`` ascending.

    Attributes
    ----------
    band_id:
        Canonical band identifier (ADR-017).
    mjd:
        Observation epochs in Modified Julian Date, sorted ascending.
    mag:
        Apparent magnitudes corresponding to each epoch.
    """

    band_id: str
    mjd: FloatArray
    mag: FloatArray

    def __post_init__(self) -> None:
        if len(self.mjd) != len(self.mag):
            raise ValueError(
                f"mjd and mag arrays must have equal length; "
                f"got {len(self.mjd)} and {len(self.mag)}"
            )


# ---------------------------------------------------------------------------
# Spline protocol
# ---------------------------------------------------------------------------


class FittedSpline(Protocol):
    """Structural protocol for a fitted spline representation.

    Abstracts over cubic smoothing splines (≥4 points) and piecewise
    linear interpolants (2–3 points).  Any callable that evaluates
    magnitude at arbitrary MJD values and exposes its valid time domain
    satisfies this protocol.
    """

    @property
    def domain(self) -> tuple[float, float]:
        """The (t_min, t_max) interval over which the spline is defined."""
        ...

    def __call__(self, t: FloatArray) -> FloatArray:
        """Evaluate the spline at the given MJD values.

        Parameters
        ----------
        t:
            1-D array of MJD values.  All values must lie within
            ``self.domain``.

        Returns
        -------
        FloatArray:
            Magnitude values at the requested epochs.
        """
        ...


# ---------------------------------------------------------------------------
# Pairwise gap analysis types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PairwiseGap:
    """Precomputed gap statistics for one ordered pair of bands.

    All gap values are signed:  ``gap(t) = f_i(t) − f_j(t)`` where *i*
    and *j* are identified by ``band_a`` and ``band_b`` respectively.

    In the inverted magnitude convention (brighter = numerically smaller),
    a positive gap means band *a* is brighter (higher on the plot) than
    band *b* at that epoch.

    Attributes
    ----------
    band_a:
        Band identifier for the first (minuend) spline.
    band_b:
        Band identifier for the second (subtrahend) spline.
    min_gap:
        The analytic minimum of ``f_a(t) − f_b(t)`` over the shared
        time domain.  A negative value indicates that band *b* is
        brighter than band *a* for at least part of the time range.
    max_gap:
        The analytic maximum of ``f_a(t) − f_b(t)`` over the shared
        time domain.
    crossing_count:
        Number of times the two spline trajectories cross (roots of
        ``f_a(t) − f_b(t) = 0``).  Diagnostic only — not used in offset
        computation (ADR-032 Decision 4).
    overlap_fraction:
        Fraction of the shared time domain where the two bands are
        within ε of each other (``|g(t)| < ε``).  Ranges from 0.0
        (never within ε) to 1.0 (always within ε).  Used by the
        crowding-aware trigger to decide whether this pair constitutes
        a "real" overlap worth correcting.
    """

    band_a: str
    band_b: str
    min_gap: float
    max_gap: float
    crossing_count: int
    overlap_fraction: float

    def __post_init__(self) -> None:
        if self.min_gap > self.max_gap:
            raise ValueError(f"min_gap ({self.min_gap}) must be ≤ max_gap ({self.max_gap})")
        if self.crossing_count < 0:
            raise ValueError(f"crossing_count must be non-negative; got {self.crossing_count}")
        if not 0.0 <= self.overlap_fraction <= 1.0:
            raise ValueError(f"overlap_fraction must be in [0.0, 1.0]; got {self.overlap_fraction}")


# ---------------------------------------------------------------------------
# Gap table (precomputed lookup consumed by the ordering search)
# ---------------------------------------------------------------------------

GapTable: TypeAlias = dict[tuple[str, str], PairwiseGap]
"""Mapping from ``(band_a, band_b)`` pairs to their precomputed gap record.

Only pairs where ``band_a < band_b`` (lexicographic) are stored.  The
ordering search retrieves the asymmetric view via
``min_gap_for_ordering()`` (ADR-032 §3).
"""


# ---------------------------------------------------------------------------
# Output types (ADR-032 Decision 6)
# ---------------------------------------------------------------------------


class OffsetDirection(str, Enum):
    """Direction of the applied magnitude offset.

    Values match the ``offset_direction`` field in the ``photometry.json``
    ``band_metadata`` array.
    """

    fainter = "fainter"
    none = "none"


@dataclass(frozen=True)
class BandOffsetResult:
    """Computed offset for a single photometric band.

    This is the per-band output of the offset pipeline, matching the
    contract in ADR-032 Decision 6.  The ``photometry.json`` generator
    maps these directly into the ``band_metadata`` array's
    ``vertical_offset`` field.

    Attributes
    ----------
    band_id:
        Canonical band identifier (ADR-017).
    offset_mag:
        Applied offset in magnitudes.  Always a non-negative half-integer
        multiple: 0.0, 0.5, 1.0, 1.5, etc.
    offset_direction:
        ``"fainter"`` when ``offset_mag > 0``; ``"none"`` when
        ``offset_mag == 0.0``.
    """

    band_id: str
    offset_mag: float
    offset_direction: OffsetDirection

    def __post_init__(self) -> None:
        if self.offset_mag < 0.0:
            raise ValueError(f"offset_mag must be non-negative; got {self.offset_mag}")
        # Verify half-integer constraint: offset_mag * 2 must be an integer.
        if self.offset_mag != 0.0 and (self.offset_mag * 2) % 1.0 != 0.0:
            raise ValueError(f"offset_mag must be a half-integer multiple; got {self.offset_mag}")
        expected_dir = OffsetDirection.fainter if self.offset_mag > 0.0 else OffsetDirection.none
        if self.offset_direction != expected_dir:
            raise ValueError(
                f"offset_direction must be {expected_dir.value!r} when "
                f"offset_mag={self.offset_mag}; got {self.offset_direction.value!r}"
            )
