"""Unit tests for band_offset.gap_analysis.

All tests use stub splines — simple callables with known mathematical
behaviour — so expected values can be computed on paper.  No scipy
fitting is involved; this isolates the gap analysis logic from the
spline fitting layer.

Note: if the band_offset package moves (e.g., into tools/), update the
import paths below accordingly.
"""

from __future__ import annotations

import numpy as np
import pytest
from artifact_generator.band_offset.gap_analysis import (
    build_gap_table,
    compute_pairwise_gap,
    peak_stack_depth,
)
from artifact_generator.band_offset.types import FloatArray

# ---------------------------------------------------------------------------
# Stub splines
# ---------------------------------------------------------------------------


class _ConstantSpline:
    """A spline that returns a fixed magnitude at all epochs."""

    def __init__(self, value: float, t_min: float, t_max: float) -> None:
        self._value = value
        self._domain = (t_min, t_max)

    @property
    def domain(self) -> tuple[float, float]:
        return self._domain

    def __call__(self, t: FloatArray) -> FloatArray:
        return np.full_like(t, self._value, dtype=np.float64)


class _LinearSpline:
    """A spline that returns ``slope * t + intercept``."""

    def __init__(self, slope: float, intercept: float, t_min: float, t_max: float) -> None:
        self._slope = slope
        self._intercept = intercept
        self._domain = (t_min, t_max)

    @property
    def domain(self) -> tuple[float, float]:
        return self._domain

    def __call__(self, t: FloatArray) -> FloatArray:
        return np.asarray(self._slope * t + self._intercept, dtype=np.float64)


# ---------------------------------------------------------------------------
# compute_pairwise_gap
# ---------------------------------------------------------------------------


class TestComputePairwiseGap:
    """Tests for compute_pairwise_gap."""

    def test_two_constants_well_separated(self) -> None:
        """Two constant bands 2 mag apart → no overlap, no crossings."""
        a = _ConstantSpline(10.0, 0.0, 100.0)
        b = _ConstantSpline(12.0, 0.0, 100.0)

        gap = compute_pairwise_gap(a, b, "A", "B", epsilon=0.5)

        # g(t) = 10.0 - 12.0 = -2.0 everywhere
        assert gap.band_a == "A"
        assert gap.band_b == "B"
        assert gap.min_gap == pytest.approx(-2.0, abs=1e-6)
        assert gap.max_gap == pytest.approx(-2.0, abs=1e-6)
        assert gap.crossing_count == 0
        assert gap.overlap_fraction == pytest.approx(0.0)

    def test_two_constants_within_epsilon(self) -> None:
        """Two constant bands 0.3 mag apart → full overlap, no crossings."""
        a = _ConstantSpline(10.0, 0.0, 100.0)
        b = _ConstantSpline(10.3, 0.0, 100.0)

        gap = compute_pairwise_gap(a, b, "A", "B", epsilon=0.5)

        # g(t) = 10.0 - 10.3 = -0.3 everywhere; |g| = 0.3 < 0.5
        assert gap.min_gap == pytest.approx(-0.3, abs=1e-6)
        assert gap.max_gap == pytest.approx(-0.3, abs=1e-6)
        assert gap.crossing_count == 0
        assert gap.overlap_fraction == pytest.approx(1.0)

    def test_two_constants_exactly_at_epsilon(self) -> None:
        """Two constant bands exactly ε apart → no overlap (strict <)."""
        a = _ConstantSpline(10.0, 0.0, 100.0)
        b = _ConstantSpline(10.5, 0.0, 100.0)

        gap = compute_pairwise_gap(a, b, "A", "B", epsilon=0.5)

        # |g| = 0.5, and overlap uses strict < ε, so fraction = 0.0
        assert gap.overlap_fraction == pytest.approx(0.0)
        assert gap.crossing_count == 0

    def test_linear_bands_single_crossing(self) -> None:
        """Two linear bands that cross once at the midpoint.

        A(t) = 0.01t + 10.0   → A(0) = 10.0,  A(100) = 11.0
        B(t) = -0.01t + 11.0  → B(0) = 11.0,  B(100) = 10.0

        g(t) = 0.02t - 1.0
        Crossing at t=50 where g=0.
        min_gap = g(0) = -1.0, max_gap = g(100) = 1.0

        |g(t)| < 0.5 when 25 < t < 75 → overlap ≈ 50% of domain.
        """
        a = _LinearSpline(0.01, 10.0, 0.0, 100.0)
        b = _LinearSpline(-0.01, 11.0, 0.0, 100.0)

        gap = compute_pairwise_gap(a, b, "A", "B", epsilon=0.5)

        assert gap.min_gap == pytest.approx(-1.0, abs=0.01)
        assert gap.max_gap == pytest.approx(1.0, abs=0.01)
        assert gap.crossing_count == 1
        assert gap.overlap_fraction == pytest.approx(0.5, abs=0.02)

    def test_linear_bands_no_crossing(self) -> None:
        """Two parallel linear bands that never cross.

        A(t) = 0.01t + 10.0
        B(t) = 0.01t + 12.0
        g(t) = -2.0 everywhere.
        """
        a = _LinearSpline(0.01, 10.0, 0.0, 100.0)
        b = _LinearSpline(0.01, 12.0, 0.0, 100.0)

        gap = compute_pairwise_gap(a, b, "A", "B", epsilon=0.5)

        assert gap.min_gap == pytest.approx(-2.0, abs=0.01)
        assert gap.max_gap == pytest.approx(-2.0, abs=0.01)
        assert gap.crossing_count == 0
        assert gap.overlap_fraction == pytest.approx(0.0)

    def test_converging_bands_partial_overlap(self) -> None:
        """Two bands that start far apart and converge.

        A(t) = 10.0       (constant)
        B(t) = -0.02t + 12.0  → B(0) = 12.0, B(100) = 10.0

        g(t) = 10.0 - (-0.02t + 12.0) = 0.02t - 2.0
        min_gap = g(0) = -2.0,  max_gap = g(100) = 0.0

        |g(t)| < 0.5 when -0.5 < 0.02t - 2.0 < 0.5
        → 75 < t < 125, clamped to [75, 100] → 25% of domain.

        No crossing (g reaches 0 at t=100 but never goes positive).
        """
        a = _ConstantSpline(10.0, 0.0, 100.0)
        b = _LinearSpline(-0.02, 12.0, 0.0, 100.0)

        gap = compute_pairwise_gap(a, b, "A", "B", epsilon=0.5)

        assert gap.min_gap == pytest.approx(-2.0, abs=0.01)
        assert gap.max_gap == pytest.approx(0.0, abs=0.01)
        assert gap.crossing_count == 0
        assert gap.overlap_fraction == pytest.approx(0.25, abs=0.02)

    def test_non_overlapping_domains_raises(self) -> None:
        """Splines with disjoint time domains → ValueError."""
        a = _ConstantSpline(10.0, 0.0, 50.0)
        b = _ConstantSpline(10.0, 60.0, 100.0)

        with pytest.raises(ValueError, match="non-overlapping"):
            compute_pairwise_gap(a, b, "A", "B")

    def test_partially_overlapping_domains(self) -> None:
        """Gap is computed only over the shared domain [40, 60].

        A(t) = 10.0 on [0, 60],  B(t) = 10.2 on [40, 100].
        Shared domain: [40, 60].
        g(t) = -0.2 everywhere in shared domain.
        """
        a = _ConstantSpline(10.0, 0.0, 60.0)
        b = _ConstantSpline(10.2, 40.0, 100.0)

        gap = compute_pairwise_gap(a, b, "A", "B", epsilon=0.5)

        assert gap.min_gap == pytest.approx(-0.2, abs=1e-6)
        assert gap.max_gap == pytest.approx(-0.2, abs=1e-6)
        assert gap.overlap_fraction == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# build_gap_table
# ---------------------------------------------------------------------------


class TestBuildGapTable:
    """Tests for build_gap_table."""

    def test_two_overlapping_bands(self) -> None:
        """Two bands with shared domain → one entry in the table."""
        splines = {
            "B": _ConstantSpline(10.0, 0.0, 100.0),
            "V": _ConstantSpline(11.0, 0.0, 100.0),
        }

        table = build_gap_table(splines, epsilon=0.5)

        assert len(table) == 1
        assert ("B", "V") in table
        assert table[("B", "V")].min_gap == pytest.approx(-1.0, abs=1e-6)

    def test_lexicographic_key_ordering(self) -> None:
        """Keys are always (smaller, larger) lexicographically."""
        splines = {
            "Z": _ConstantSpline(10.0, 0.0, 100.0),
            "A": _ConstantSpline(11.0, 0.0, 100.0),
        }

        table = build_gap_table(splines, epsilon=0.5)

        assert ("A", "Z") in table
        assert ("Z", "A") not in table

    def test_non_overlapping_pair_skipped(self) -> None:
        """Bands with disjoint time domains are not in the table."""
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 50.0),
            "B": _ConstantSpline(10.0, 60.0, 100.0),
        }

        table = build_gap_table(splines, epsilon=0.5)

        assert len(table) == 0

    def test_three_bands_mixed_overlap(self) -> None:
        """Three bands: A-B overlap, B-C overlap, A-C disjoint.

        A: [0, 50], B: [30, 80], C: [70, 100]
        Shared domains: A-B = [30,50], B-C = [70,80], A-C = none.
        """
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 50.0),
            "B": _ConstantSpline(10.0, 30.0, 80.0),
            "C": _ConstantSpline(10.0, 70.0, 100.0),
        }

        table = build_gap_table(splines, epsilon=0.5)

        assert len(table) == 2
        assert ("A", "B") in table
        assert ("B", "C") in table
        assert ("A", "C") not in table

    def test_single_band_empty_table(self) -> None:
        """A single band produces no pairs."""
        splines = {"V": _ConstantSpline(10.0, 0.0, 100.0)}

        table = build_gap_table(splines, epsilon=0.5)

        assert len(table) == 0

    def test_empty_splines_empty_table(self) -> None:
        """No bands → no pairs."""
        table = build_gap_table({}, epsilon=0.5)
        assert len(table) == 0

    def test_epsilon_affects_overlap_fraction(self) -> None:
        """Larger ε → more of the domain classified as overlapping."""
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 100.0),
            "B": _ConstantSpline(10.8, 0.0, 100.0),
        }

        table_narrow = build_gap_table(splines, epsilon=0.5)
        table_wide = build_gap_table(splines, epsilon=1.0)

        # Gap is 0.8.  With ε=0.5, |gap| > ε → overlap = 0.
        # With ε=1.0, |gap| < ε → overlap = 1.0.
        assert table_narrow[("A", "B")].overlap_fraction == pytest.approx(0.0)
        assert table_wide[("A", "B")].overlap_fraction == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# peak_stack_depth
# ---------------------------------------------------------------------------


class TestPeakStackDepth:
    """Tests for peak_stack_depth."""

    def test_empty_splines(self) -> None:
        assert peak_stack_depth({}, epsilon=0.5) == 0

    def test_single_band(self) -> None:
        splines = {"V": _ConstantSpline(10.0, 0.0, 100.0)}
        assert peak_stack_depth(splines, epsilon=0.5) == 1

    def test_two_bands_well_separated(self) -> None:
        """Bands 2 mag apart → never within ε → depth 1."""
        splines = {
            "B": _ConstantSpline(10.0, 0.0, 100.0),
            "R": _ConstantSpline(12.0, 0.0, 100.0),
        }
        assert peak_stack_depth(splines, epsilon=0.5) == 1

    def test_two_bands_close(self) -> None:
        """Bands 0.3 mag apart → always within ε → depth 2."""
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 100.0),
            "B": _ConstantSpline(10.3, 0.0, 100.0),
        }
        assert peak_stack_depth(splines, epsilon=0.5) == 2

    def test_three_bands_piled_up(self) -> None:
        """Three bands within ε of each other → depth 3."""
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 100.0),
            "B": _ConstantSpline(10.1, 0.0, 100.0),
            "C": _ConstantSpline(10.3, 0.0, 100.0),
        }
        assert peak_stack_depth(splines, epsilon=0.5) == 3

    def test_three_bands_two_close_one_far(self) -> None:
        """Two bands piled up, third far away → depth 2."""
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 100.0),
            "B": _ConstantSpline(10.2, 0.0, 100.0),
            "C": _ConstantSpline(14.0, 0.0, 100.0),
        }
        assert peak_stack_depth(splines, epsilon=0.5) == 2

    def test_bands_converge_late(self) -> None:
        """Two bands that only pile up at the end of the domain.

        A(t) = 10.0      (constant)
        B(t) = -0.02t + 12.0  → B(100) = 10.0

        They're within 0.5 mag for t > 75.  Peak depth should be 2.
        """
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 100.0),
            "B": _LinearSpline(-0.02, 12.0, 0.0, 100.0),
        }
        assert peak_stack_depth(splines, epsilon=0.5) == 2

    def test_non_overlapping_domains_depth_one(self) -> None:
        """Bands that don't coexist in time → max depth 1."""
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 50.0),
            "B": _ConstantSpline(10.0, 60.0, 100.0),
        }
        assert peak_stack_depth(splines, epsilon=0.5) == 1

    def test_four_bands_sliding_window(self) -> None:
        """Four bands spread across 1.2 mag.  ε = 0.5.

        Mags: 10.0, 10.3, 10.6, 11.2
        Window [10.0, 10.5): 10.0, 10.3 → 2
        Window [10.3, 10.8): 10.3, 10.6 → 2
        Window [10.6, 11.1): 10.6 → 1
        Peak depth = 2.
        """
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 100.0),
            "B": _ConstantSpline(10.3, 0.0, 100.0),
            "C": _ConstantSpline(10.6, 0.0, 100.0),
            "D": _ConstantSpline(11.2, 0.0, 100.0),
        }
        assert peak_stack_depth(splines, epsilon=0.5) == 2

    def test_larger_epsilon_increases_depth(self) -> None:
        """Wider window captures more bands."""
        splines = {
            "A": _ConstantSpline(10.0, 0.0, 100.0),
            "B": _ConstantSpline(10.3, 0.0, 100.0),
            "C": _ConstantSpline(10.6, 0.0, 100.0),
        }

        # ε = 0.5: window captures at most 2 (10.0+10.3 or 10.3+10.6)
        assert peak_stack_depth(splines, epsilon=0.5) == 2

        # ε = 1.0: window captures all 3 (10.0 to 10.6 < 1.0)
        assert peak_stack_depth(splines, epsilon=1.0) == 3
