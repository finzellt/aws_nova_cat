"""Tests for compositing A2 functions — cleaning, resampling, combination, CSV.

clean_spectrum tests mock the shared.py cleaning functions to isolate
the wrapper logic.  All other functions are tested directly with
synthetic data.
"""

from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pytest
from generators.compositing import (
    CleanedSpectrum,
    clean_spectrum,
    combine_spectra,
    composite_to_csv,
    resample_to_grid,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cleaned(
    dp_id: str,
    wl_start: float,
    wl_end: float,
    n_points: int,
    flux_value: float = 1.0,
) -> CleanedSpectrum:
    """Build a CleanedSpectrum with uniform spacing and constant flux."""
    return CleanedSpectrum(
        data_product_id=dp_id,
        wavelengths=np.linspace(wl_start, wl_end, n_points),
        fluxes=np.full(n_points, flux_value),
    )


# ===================================================================
# clean_spectrum
# ===================================================================


class TestCleanSpectrum:
    """Wrapper around the three shared cleaning functions."""

    _SHARED = "generators.compositing.generators.shared"

    def _passthrough(
        self, wl: list[float], fx: list[float], dp_id: str, **kwargs: object
    ) -> tuple[list[float], list[float]]:
        """Cleaning function that returns input unchanged."""
        return wl, fx

    def _empty(
        self, wl: list[float], fx: list[float], dp_id: str, **kwargs: object
    ) -> tuple[list[float], list[float]]:
        """Cleaning function that eliminates all points."""
        return [], []

    def test_all_cleaning_passes(self) -> None:
        """When all three cleaners pass through, the result is a CleanedSpectrum."""
        wl = np.linspace(400.0, 500.0, 100)
        fx = np.ones(100)
        with (
            patch("generators.shared.trim_dead_edges", side_effect=self._passthrough),
            patch("generators.shared.remove_interior_dead_runs", side_effect=self._passthrough),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=self._passthrough),
        ):
            result = clean_spectrum("dp-001", wl, fx)

        assert result is not None
        assert result["data_product_id"] == "dp-001"
        assert len(result["wavelengths"]) == 100
        assert len(result["fluxes"]) == 100

    def test_trim_dead_edges_empties(self) -> None:
        """Returns None when edge trimming eliminates all points."""
        wl = np.linspace(400.0, 500.0, 100)
        fx = np.ones(100)
        with (
            patch("generators.shared.trim_dead_edges", side_effect=self._empty),
            patch("generators.shared.remove_interior_dead_runs", side_effect=self._passthrough),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=self._passthrough),
        ):
            result = clean_spectrum("dp-001", wl, fx)

        assert result is None

    def test_interior_dead_runs_empties(self) -> None:
        """Returns None when interior dead run removal eliminates all points."""
        wl = np.linspace(400.0, 500.0, 100)
        fx = np.ones(100)
        with (
            patch("generators.shared.trim_dead_edges", side_effect=self._passthrough),
            patch("generators.shared.remove_interior_dead_runs", side_effect=self._empty),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=self._passthrough),
        ):
            result = clean_spectrum("dp-001", wl, fx)

        assert result is None

    def test_chip_gap_rejection_empties(self) -> None:
        """Returns None when chip gap rejection eliminates all points."""
        wl = np.linspace(400.0, 500.0, 100)
        fx = np.ones(100)
        with (
            patch("generators.shared.trim_dead_edges", side_effect=self._passthrough),
            patch("generators.shared.remove_interior_dead_runs", side_effect=self._passthrough),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=self._empty),
        ):
            result = clean_spectrum("dp-001", wl, fx)

        assert result is None

    def test_converts_to_numpy(self) -> None:
        """Output arrays are numpy float64, even though shared functions return lists."""
        wl = np.array([400.0, 450.0, 500.0])
        fx = np.array([1.0, 2.0, 3.0])
        with (
            patch("generators.shared.trim_dead_edges", side_effect=self._passthrough),
            patch("generators.shared.remove_interior_dead_runs", side_effect=self._passthrough),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=self._passthrough),
        ):
            result = clean_spectrum("dp-001", wl, fx)

        assert result is not None
        assert isinstance(result["wavelengths"], np.ndarray)
        assert result["wavelengths"].dtype == np.float64

    def test_calls_functions_in_order(self) -> None:
        """Cleaning functions are called in the correct order."""
        call_order: list[str] = []

        def _track_trim(
            wl: list[float], fx: list[float], dp_id: str
        ) -> tuple[list[float], list[float]]:
            call_order.append("trim")
            return wl, fx

        def _track_interior(
            wl: list[float], fx: list[float], dp_id: str, **kw: object
        ) -> tuple[list[float], list[float]]:
            call_order.append("interior")
            return wl, fx

        def _track_chip(
            wl: list[float], fx: list[float], dp_id: str
        ) -> tuple[list[float], list[float]]:
            call_order.append("chip")
            return wl, fx

        wl = np.linspace(400.0, 500.0, 50)
        fx = np.ones(50)
        with (
            patch("generators.shared.trim_dead_edges", side_effect=_track_trim),
            patch("generators.shared.remove_interior_dead_runs", side_effect=_track_interior),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=_track_chip),
        ):
            clean_spectrum("dp-001", wl, fx)

        assert call_order == ["trim", "interior", "chip"]


# ===================================================================
# resample_to_grid
# ===================================================================


class TestResampleToGrid:
    """Linear interpolation onto a common wavelength grid."""

    def test_exact_coverage(self) -> None:
        """Spectrum covering the full grid has no NaN values."""
        spec = _cleaned("a", 400.0, 500.0, 101, flux_value=2.0)
        grid = np.linspace(400.0, 500.0, 51)
        result = resample_to_grid(spec, grid)
        assert not np.any(np.isnan(result))
        np.testing.assert_allclose(result, 2.0)

    def test_partial_coverage_nan_outside(self) -> None:
        """Grid points outside the spectrum's range are NaN."""
        spec = _cleaned("a", 450.0, 550.0, 101)
        grid = np.linspace(400.0, 600.0, 201)
        result = resample_to_grid(spec, grid)

        # Points inside 450–550 should be valid.
        inside = (grid >= 450.0) & (grid <= 550.0)
        assert not np.any(np.isnan(result[inside]))

        # Points outside should be NaN.
        outside = (grid < 450.0) | (grid > 550.0)
        assert np.all(np.isnan(result[outside]))

    def test_interpolation_accuracy(self) -> None:
        """Linear interpolation produces correct intermediate values."""
        # Spectrum with a linear ramp: flux = wavelength.
        wl = np.array([400.0, 500.0])
        fx = np.array([400.0, 500.0])
        spec = CleanedSpectrum(data_product_id="ramp", wavelengths=wl, fluxes=fx)
        grid = np.array([400.0, 425.0, 450.0, 475.0, 500.0])
        result = resample_to_grid(spec, grid)
        np.testing.assert_allclose(result, grid)

    def test_grid_wider_than_spectrum(self) -> None:
        """NaN regions don't contaminate the valid interior."""
        spec = _cleaned("a", 500.0, 600.0, 101, flux_value=5.0)
        grid = np.linspace(400.0, 700.0, 301)
        result = resample_to_grid(spec, grid)

        valid_count = np.count_nonzero(~np.isnan(result))
        # Approximately 100 of 301 grid points should be valid (500–600 region).
        assert 95 <= valid_count <= 105


# ===================================================================
# combine_spectra
# ===================================================================


class TestCombineSpectra:
    """Subset-aware median combination of resampled flux arrays."""

    def test_full_overlap_median(self) -> None:
        """Two fully overlapping spectra produce the median at each point."""
        fx1 = np.array([1.0, 3.0, 5.0])
        fx2 = np.array([2.0, 4.0, 6.0])
        result = combine_spectra([fx1, fx2])
        # Median of [1,2]=1.5, [3,4]=3.5, [5,6]=5.5
        np.testing.assert_allclose(result, [1.5, 3.5, 5.5])

    def test_three_spectra_median(self) -> None:
        """Three spectra — median picks the middle value."""
        fx1 = np.array([1.0, 10.0])
        fx2 = np.array([2.0, 20.0])
        fx3 = np.array([3.0, 30.0])
        result = combine_spectra([fx1, fx2, fx3])
        np.testing.assert_allclose(result, [2.0, 20.0])

    def test_outlier_robustness(self) -> None:
        """Median resists a single outlier spectrum."""
        fx_normal1 = np.array([1.0, 1.0, 1.0])
        fx_normal2 = np.array([1.0, 1.0, 1.0])
        fx_outlier = np.array([1.0, 1000.0, 1.0])
        result = combine_spectra([fx_normal1, fx_normal2, fx_outlier])
        np.testing.assert_allclose(result, [1.0, 1.0, 1.0])

    def test_partial_overlap(self) -> None:
        """Non-overlapping regions use the single contributing spectrum."""
        # Spectrum A covers full range, B covers only the middle.
        fx_a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        fx_b = np.array([np.nan, 20.0, 30.0, 40.0, np.nan])
        result = combine_spectra([fx_a, fx_b])
        # Endpoints: only A contributes → A's value.
        assert result[0] == pytest.approx(1.0)
        assert result[4] == pytest.approx(5.0)
        # Overlap region: median of [2,20]=11, [3,30]=16.5, [4,40]=22
        assert result[1] == pytest.approx(11.0)
        assert result[2] == pytest.approx(16.5)
        assert result[3] == pytest.approx(22.0)

    def test_disjoint_spectra(self) -> None:
        """Completely disjoint spectra each contribute in their own region."""
        fx_a = np.array([1.0, 2.0, np.nan, np.nan])
        fx_b = np.array([np.nan, np.nan, 3.0, 4.0])
        result = combine_spectra([fx_a, fx_b])
        np.testing.assert_allclose(result, [1.0, 2.0, 3.0, 4.0])

    def test_all_nan_produces_nan(self) -> None:
        """Grid points with no coverage from any spectrum remain NaN."""
        fx_a = np.array([1.0, np.nan, 3.0])
        fx_b = np.array([2.0, np.nan, 4.0])
        result = combine_spectra([fx_a, fx_b])
        assert np.isnan(result[1])
        assert result[0] == pytest.approx(1.5)
        assert result[2] == pytest.approx(3.5)

    def test_fewer_than_two_raises(self) -> None:
        """Single flux array raises ValueError."""
        with pytest.raises(ValueError, match="Need ≥ 2"):
            combine_spectra([np.array([1.0, 2.0])])


# ===================================================================
# composite_to_csv
# ===================================================================


class TestCompositeToCsv:
    """CSV serialization of composite spectra."""

    def test_basic_output(self) -> None:
        """Produces a valid two-column CSV with header."""
        wl = np.array([400.0, 450.0, 500.0])
        fx = np.array([1.5, 2.5, 3.5])
        csv_str = composite_to_csv(wl, fx)
        lines = csv_str.strip().split("\n")
        assert lines[0] == "wavelength_nm,flux"
        assert len(lines) == 4  # header + 3 data rows

    def test_nan_excluded(self) -> None:
        """NaN flux values are excluded from the output."""
        wl = np.array([400.0, 450.0, 500.0, 550.0])
        fx = np.array([1.0, np.nan, 3.0, np.nan])
        csv_str = composite_to_csv(wl, fx)
        lines = csv_str.strip().split("\n")
        assert len(lines) == 3  # header + 2 data rows (NaNs excluded)

    def test_wavelength_precision(self) -> None:
        """Wavelengths are formatted to 6 decimal places."""
        wl = np.array([400.123456789])
        fx = np.array([1.0])
        csv_str = composite_to_csv(wl, fx)
        data_line = csv_str.strip().split("\n")[1]
        wl_str = data_line.split(",")[0]
        assert wl_str == "400.123457"  # rounded to 6 dp

    def test_trailing_newline(self) -> None:
        """Output ends with a newline."""
        wl = np.array([400.0])
        fx = np.array([1.0])
        csv_str = composite_to_csv(wl, fx)
        assert csv_str.endswith("\n")

    def test_empty_after_nan_removal(self) -> None:
        """All-NaN input produces a header-only CSV."""
        wl = np.array([400.0, 500.0])
        fx = np.array([np.nan, np.nan])
        csv_str = composite_to_csv(wl, fx)
        lines = csv_str.strip().split("\n")
        assert len(lines) == 1
        assert lines[0] == "wavelength_nm,flux"

    def test_roundtrip_values(self) -> None:
        """CSV values can be parsed back to the original floats."""
        wl = np.array([400.0, 500.0])
        fx = np.array([1.23e-15, 4.56e-15])
        csv_str = composite_to_csv(wl, fx)
        lines = csv_str.strip().split("\n")[1:]
        for i, line in enumerate(lines):
            parts = line.split(",")
            assert float(parts[0]) == pytest.approx(wl[i], rel=1e-5)
            assert float(parts[1]) == pytest.approx(fx[i], rel=1e-10)
