"""Tests for generators.compositing — pure computation utilities.

Covers night clustering, compositing group identification, common grid
determination, composite fingerprinting, and deterministic composite ID
generation.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import numpy as np
import pytest
from generators.compositing import (
    CleanedSpectrum,
    cluster_by_night,
    compute_composite_fingerprint,
    compute_composite_id,
    determine_common_grid,
    identify_compositing_groups,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _product(
    mjd: float | Decimal,
    instrument: str = "UVES",
    dp_id: str | None = None,
) -> dict[str, Any]:
    """Build a minimal DataProduct-like dict for testing."""
    return {
        "observation_date_mjd": mjd,
        "instrument": instrument,
        "data_product_id": dp_id or f"dp-{float(mjd):.4f}",
    }


def _cleaned(
    dp_id: str,
    wl_start: float,
    wl_end: float,
    n_points: int,
) -> CleanedSpectrum:
    """Build a CleanedSpectrum with uniform wavelength spacing."""
    return CleanedSpectrum(
        data_product_id=dp_id,
        wavelengths=np.linspace(wl_start, wl_end, n_points),
        fluxes=np.ones(n_points),
    )


# ===================================================================
# cluster_by_night
# ===================================================================


class TestClusterByNight:
    """Night clustering via sequential gap detection."""

    def test_single_product(self) -> None:
        """A single product forms one group of one."""
        groups = cluster_by_night([_product(60000.5)])
        assert len(groups) == 1
        assert len(groups[0]) == 1

    def test_two_products_same_night(self) -> None:
        """Products within the gap threshold land in the same group."""
        groups = cluster_by_night(
            [
                _product(60000.3),
                _product(60000.5),
            ]
        )
        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_two_products_different_nights(self) -> None:
        """Products separated by more than the threshold split."""
        groups = cluster_by_night(
            [
                _product(60000.3),
                _product(60001.0),
            ]
        )
        assert len(groups) == 2
        assert len(groups[0]) == 1
        assert len(groups[1]) == 1

    def test_three_nights(self) -> None:
        """Multiple nights are correctly separated."""
        groups = cluster_by_night(
            [
                _product(60000.3),
                _product(60000.4),
                _product(60001.3),
                _product(60002.3),
                _product(60002.4),
                _product(60002.45),
            ]
        )
        assert len(groups) == 3
        assert len(groups[0]) == 2
        assert len(groups[1]) == 1
        assert len(groups[2]) == 3

    def test_chronological_ordering(self) -> None:
        """Groups and their members are sorted by MJD ascending."""
        groups = cluster_by_night(
            [
                _product(60002.0),
                _product(60000.1),
                _product(60000.3),
            ]
        )
        assert len(groups) == 2
        # First group is the earliest night.
        assert float(groups[0][0]["observation_date_mjd"]) == pytest.approx(60000.1)
        assert float(groups[0][1]["observation_date_mjd"]) == pytest.approx(60000.3)
        assert float(groups[1][0]["observation_date_mjd"]) == pytest.approx(60002.0)

    def test_decimal_mjd_values(self) -> None:
        """DynamoDB returns Decimal; clustering handles it transparently."""
        groups = cluster_by_night(
            [
                _product(Decimal("60000.300")),
                _product(Decimal("60000.400")),
                _product(Decimal("60001.500")),
            ]
        )
        assert len(groups) == 2
        assert len(groups[0]) == 2
        assert len(groups[1]) == 1

    def test_gap_exactly_at_threshold(self) -> None:
        """A gap exactly equal to the threshold does NOT split."""
        groups = cluster_by_night(
            [
                _product(60000.0),
                _product(60000.5),
            ]
        )
        assert len(groups) == 1

    def test_gap_just_over_threshold(self) -> None:
        """A gap just above the threshold does split."""
        groups = cluster_by_night(
            [
                _product(60000.0),
                _product(60000.5001),
            ]
        )
        assert len(groups) == 2

    def test_custom_gap_threshold(self) -> None:
        """A tighter threshold splits observations that would otherwise group."""
        groups = cluster_by_night(
            [_product(60000.0), _product(60000.1)],
            gap_threshold=0.05,
        )
        assert len(groups) == 2

    def test_empty_raises(self) -> None:
        """Empty input raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            cluster_by_night([])


# ===================================================================
# identify_compositing_groups
# ===================================================================


class TestIdentifyCompositingGroups:
    """Instrument grouping + night clustering → compositing groups."""

    def test_two_same_instrument_same_night(self) -> None:
        """The simplest compositable case: 2 spectra, same instrument, same night."""
        groups = identify_compositing_groups(
            [
                _product(60000.3, instrument="UVES"),
                _product(60000.4, instrument="UVES"),
            ]
        )
        assert len(groups) == 1
        assert groups[0]["instrument"] == "UVES"
        assert len(groups[0]["products"]) == 2

    def test_different_instruments_not_grouped(self) -> None:
        """Same-night spectra from different instruments are separate."""
        groups = identify_compositing_groups(
            [
                _product(60000.3, instrument="UVES"),
                _product(60000.4, instrument="XSHOOTER"),
            ]
        )
        assert len(groups) == 0

    def test_singletons_excluded(self) -> None:
        """A night with only one spectrum per instrument produces no group."""
        groups = identify_compositing_groups(
            [
                _product(60000.3, instrument="UVES"),
                _product(60001.3, instrument="UVES"),
            ]
        )
        assert len(groups) == 0

    def test_mixed_instruments_and_nights(self) -> None:
        """Multiple instruments across multiple nights."""
        groups = identify_compositing_groups(
            [
                _product(60000.3, instrument="UVES"),
                _product(60000.4, instrument="UVES"),
                _product(60000.35, instrument="XSHOOTER"),
                _product(60001.3, instrument="UVES"),
                _product(60001.4, instrument="UVES"),
                _product(60001.45, instrument="UVES"),
            ]
        )
        # UVES night 1 (2 spectra), UVES night 2 (3 spectra).
        # XSHOOTER singleton excluded.
        assert len(groups) == 2
        uves_groups = [g for g in groups if g["instrument"] == "UVES"]
        assert len(uves_groups) == 2
        sizes = sorted(len(g["products"]) for g in uves_groups)
        assert sizes == [2, 3]

    def test_single_instrument_single_product(self) -> None:
        """One product total → no groups."""
        groups = identify_compositing_groups(
            [
                _product(60000.3, instrument="UVES"),
            ]
        )
        assert len(groups) == 0

    def test_empty_input(self) -> None:
        """No products → no groups (not an error)."""
        groups = identify_compositing_groups([])
        assert groups == []


# ===================================================================
# determine_common_grid
# ===================================================================


class TestDetermineCommonGrid:
    """Common wavelength grid from coarsest input."""

    def test_two_identical_spectra(self) -> None:
        """Two spectra with the same resolution produce a grid matching that resolution."""
        s1 = _cleaned("a", 400.0, 500.0, 1001)
        s2 = _cleaned("b", 400.0, 500.0, 1001)
        grid = determine_common_grid([s1, s2])
        step = float(grid[1] - grid[0])
        expected_step = 100.0 / 1000  # 0.1 nm
        assert step == pytest.approx(expected_step, rel=0.01)
        assert float(grid[0]) == pytest.approx(400.0)
        assert float(grid[-1]) == pytest.approx(500.0)

    def test_coarsest_sets_step(self) -> None:
        """The coarser spectrum's step size determines the grid."""
        fine = _cleaned("fine", 400.0, 500.0, 10001)  # ~0.01 nm step
        coarse = _cleaned("coarse", 400.0, 500.0, 101)  # ~1.0 nm step
        grid = determine_common_grid([fine, coarse])
        step = float(grid[1] - grid[0])
        # Should be close to the coarse step (~1.0 nm), not the fine one.
        assert step == pytest.approx(1.0, rel=0.05)

    def test_union_wavelength_range(self) -> None:
        """Grid spans the union range even for non-overlapping spectra."""
        blue = _cleaned("blue", 300.0, 500.0, 2001)
        red = _cleaned("red", 600.0, 900.0, 3001)
        grid = determine_common_grid([blue, red])
        assert float(grid[0]) == pytest.approx(300.0)
        assert float(grid[-1]) == pytest.approx(900.0)

    def test_partial_overlap(self) -> None:
        """Partially overlapping spectra produce a grid spanning the union."""
        s1 = _cleaned("a", 400.0, 600.0, 2001)
        s2 = _cleaned("b", 500.0, 700.0, 2001)
        grid = determine_common_grid([s1, s2])
        assert float(grid[0]) == pytest.approx(400.0)
        assert float(grid[-1]) == pytest.approx(700.0)

    def test_uniform_spacing(self) -> None:
        """The grid is uniformly spaced (constant step)."""
        s1 = _cleaned("a", 400.0, 500.0, 501)
        s2 = _cleaned("b", 400.0, 500.0, 201)
        grid = determine_common_grid([s1, s2])
        steps = np.diff(grid)
        assert np.allclose(steps, steps[0], rtol=1e-10)

    def test_fewer_than_two_raises(self) -> None:
        """A single spectrum raises ValueError."""
        with pytest.raises(ValueError, match="Need ≥ 2"):
            determine_common_grid([_cleaned("a", 400.0, 500.0, 100)])

    def test_spectrum_with_one_point_raises(self) -> None:
        """A spectrum with < 2 points after cleaning raises."""
        s1 = _cleaned("a", 400.0, 500.0, 1001)
        s2 = CleanedSpectrum(
            data_product_id="tiny",
            wavelengths=np.array([450.0]),
            fluxes=np.array([1.0]),
        )
        with pytest.raises(ValueError, match="< 2 points"):
            determine_common_grid([s1, s2])


# ===================================================================
# compute_composite_fingerprint
# ===================================================================


class TestCompositeFingerprint:
    """Deterministic fingerprint from constituent IDs + sha256 hashes."""

    def test_deterministic(self) -> None:
        """Same inputs always produce the same fingerprint."""
        ids = ["dp-aaa", "dp-bbb"]
        shas = {"dp-aaa": "sha_aaa", "dp-bbb": "sha_bbb"}
        fp1 = compute_composite_fingerprint(ids, shas)
        fp2 = compute_composite_fingerprint(ids, shas)
        assert fp1 == fp2

    def test_order_independent(self) -> None:
        """Input order does not affect the fingerprint."""
        shas = {"dp-aaa": "sha_aaa", "dp-bbb": "sha_bbb"}
        fp_forward = compute_composite_fingerprint(["dp-aaa", "dp-bbb"], shas)
        fp_reverse = compute_composite_fingerprint(["dp-bbb", "dp-aaa"], shas)
        assert fp_forward == fp_reverse

    def test_different_shas_different_fingerprint(self) -> None:
        """Changing a sha256 changes the fingerprint."""
        ids = ["dp-aaa", "dp-bbb"]
        fp1 = compute_composite_fingerprint(ids, {"dp-aaa": "sha_1", "dp-bbb": "sha_2"})
        fp2 = compute_composite_fingerprint(ids, {"dp-aaa": "sha_1", "dp-bbb": "sha_CHANGED"})
        assert fp1 != fp2

    def test_different_ids_different_fingerprint(self) -> None:
        """A different set of constituent IDs produces a different fingerprint."""
        shas = {"dp-aaa": "sha_aaa", "dp-bbb": "sha_bbb", "dp-ccc": "sha_ccc"}
        fp_ab = compute_composite_fingerprint(["dp-aaa", "dp-bbb"], shas)
        fp_ac = compute_composite_fingerprint(["dp-aaa", "dp-ccc"], shas)
        assert fp_ab != fp_ac

    def test_hex_digest_length(self) -> None:
        """Result is a 64-character SHA-256 hex digest."""
        fp = compute_composite_fingerprint(["dp-x"], {"dp-x": "sha_x"})
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)

    def test_missing_sha_raises(self) -> None:
        """A constituent ID not in the sha256 map raises KeyError."""
        with pytest.raises(KeyError):
            compute_composite_fingerprint(
                ["dp-aaa", "dp-missing"],
                {"dp-aaa": "sha_aaa"},
            )

    def test_empty_ids_raises(self) -> None:
        """Empty constituent list raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            compute_composite_fingerprint([], {})


# ===================================================================
# compute_composite_id
# ===================================================================


class TestCompositeId:
    """Deterministic UUID v5 from constituent IDs."""

    def test_deterministic(self) -> None:
        """Same inputs always produce the same UUID."""
        id1 = compute_composite_id(["dp-aaa", "dp-bbb"])
        id2 = compute_composite_id(["dp-aaa", "dp-bbb"])
        assert id1 == id2

    def test_order_independent(self) -> None:
        """Input order does not affect the UUID."""
        id_forward = compute_composite_id(["dp-aaa", "dp-bbb"])
        id_reverse = compute_composite_id(["dp-bbb", "dp-aaa"])
        assert id_forward == id_reverse

    def test_valid_uuid_format(self) -> None:
        """Result is a valid hyphenated UUID string."""
        import uuid as uuid_mod

        cid = compute_composite_id(["dp-aaa", "dp-bbb"])
        parsed = uuid_mod.UUID(cid)
        assert str(parsed) == cid
        assert parsed.version == 5

    def test_different_constituents_different_id(self) -> None:
        """Different sets of constituents produce different IDs."""
        id_ab = compute_composite_id(["dp-aaa", "dp-bbb"])
        id_ac = compute_composite_id(["dp-aaa", "dp-ccc"])
        assert id_ab != id_ac

    def test_empty_raises(self) -> None:
        """Empty constituent list raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            compute_composite_id([])
