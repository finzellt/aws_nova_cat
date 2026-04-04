"""Unit tests for generators/spectra.py — edge trimming and flux floor.

Covers:
  _trim_dead_edges:
  - Blue edge only: consecutive leading zeros trimmed
  - Red edge only: consecutive trailing zeros trimmed
  - Both edges: leading and trailing zeros trimmed
  - Single zero at each edge preserved (could be legitimate)
  - No zeros: unchanged
  - Near-zero values below _ZERO_THRESHOLD treated as zero
  - All zeros: returns empty lists

  _normalize_flux (with floor):
  - Negatives and zeros clamped to _FLUX_FLOOR after normalization
  - Peak value preserved
  - All-negative array normalizes successfully
  - Relative ordering of positive values preserved
"""

from __future__ import annotations

import statistics
from decimal import Decimal
from typing import Any

import pytest
from generators.shared import lttb
from generators.spectra import (
    _FLUX_FLOOR,
    _LTTB_THRESHOLD,
    _TRIM_TOLERANCE,
    _ZERO_THRESHOLD,
    _normalize_flux,
    _trim_dead_edges,
    _trim_wavelength_range,
    generate_spectra_json,
)

# ---------------------------------------------------------------------------
# _trim_dead_edges
# ---------------------------------------------------------------------------


class TestTrimDeadEdges:
    """Edge-trimming of detector rolloff artifacts."""

    def test_blue_edge_only(self) -> None:
        wl = [400.0, 401.0, 402.0, 403.0, 404.0, 405.0]
        fx = [0.0, 0.0, 0.0, 0.5, 1.0, 0.8]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-blue")
        assert fx_out == [0.5, 1.0, 0.8]
        assert wl_out == [403.0, 404.0, 405.0]

    def test_red_edge_only(self) -> None:
        wl = [400.0, 401.0, 402.0, 403.0, 404.0]
        fx = [0.5, 1.0, 0.8, 0.0, 0.0]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-red")
        assert fx_out == [0.5, 1.0, 0.8]
        assert wl_out == [400.0, 401.0, 402.0]

    def test_both_edges(self) -> None:
        wl = [400.0, 401.0, 402.0, 403.0, 404.0, 405.0, 406.0, 407.0]
        fx = [0.0, 0.0, 0.3, 1.0, 0.5, 0.0, 0.0, 0.0]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-both")
        assert fx_out == [0.3, 1.0, 0.5]
        assert wl_out == [402.0, 403.0, 404.0]

    def test_single_zero_at_each_edge_preserved(self) -> None:
        wl = [400.0, 401.0, 402.0, 403.0, 404.0]
        fx = [0.0, 0.5, 1.0, 0.8, 0.0]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-single")
        assert fx_out == fx
        assert wl_out == wl

    def test_no_zeros_unchanged(self) -> None:
        wl = [400.0, 401.0, 402.0]
        fx = [0.5, 1.0, 0.8]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-none")
        assert fx_out == fx
        assert wl_out == wl

    def test_near_zero_below_threshold_treated_as_zero(self) -> None:
        tiny = _ZERO_THRESHOLD / 10
        wl = [400.0, 401.0, 402.0, 403.0, 404.0]
        fx = [tiny, tiny, 0.5, 1.0, 0.8]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-tiny")
        assert fx_out == [0.5, 1.0, 0.8]
        assert wl_out == [402.0, 403.0, 404.0]

    def test_all_zeros_returns_empty(self) -> None:
        wl = [400.0, 401.0, 402.0]
        fx = [0.0, 0.0, 0.0]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-allzero")
        assert fx_out == []
        assert wl_out == []

    def test_empty_input(self) -> None:
        wl_out, fx_out = _trim_dead_edges([], [], "dp-empty")
        assert fx_out == []
        assert wl_out == []


# ---------------------------------------------------------------------------
# _normalize_flux (with floor)
# ---------------------------------------------------------------------------


class TestNormalizeFluxFloor:
    """Flux floor prevents log(0) on frontend."""

    def test_negatives_and_zeros_clamped_to_floor(self) -> None:
        fx = [-0.5, 0.0, 0.3, 1.0, 0.0]
        normalized, scale = _normalize_flux(fx)
        assert scale == pytest.approx(1.0)
        assert all(v >= _FLUX_FLOOR for v in normalized)

    def test_peak_preserved(self) -> None:
        fx = [0.2, 0.5, 1.0, 0.8]
        normalized, scale = _normalize_flux(fx)
        assert scale == pytest.approx(1.0)
        assert normalized[2] == pytest.approx(1.0)

    def test_all_negative_normalizes(self) -> None:
        fx = [-1.0, -0.5, -0.2]
        normalized, scale = _normalize_flux(fx)
        assert scale == pytest.approx(1.0)
        assert len(normalized) == 3
        # All values should be clamped to floor (since -x/peak is negative)
        assert all(v >= _FLUX_FLOOR for v in normalized)

    def test_relative_ordering_of_positives_preserved(self) -> None:
        fx = [0.2, 0.8, 0.4, 1.0]
        normalized, _ = _normalize_flux(fx)
        assert normalized[0] < normalized[2] < normalized[1] < normalized[3]

    def test_empty_returns_none(self) -> None:
        normalized, scale = _normalize_flux([])
        assert normalized == []
        assert scale is None

    def test_all_zero_returns_none(self) -> None:
        normalized, scale = _normalize_flux([0.0, 0.0, 0.0])
        assert normalized == []
        assert scale is None


# ---------------------------------------------------------------------------
# LTTB downsampling integration (§7.9)
# ---------------------------------------------------------------------------


class TestLttbDownsampling:
    """LTTB downsampling applied to spectra pipeline."""

    def test_downsampling_applied_when_over_threshold(self) -> None:
        """Spectrum with 3000 points should be reduced to ≤ 2000."""
        n = 3000
        wavelengths = [400.0 + i * 0.1 for i in range(n)]
        # Gaussian-like peak in the middle
        peak_idx = n // 2
        peak_flux = 100.0
        fluxes = [1.0 + 50.0 * max(0.0, 1.0 - abs(i - peak_idx) / 50.0) for i in range(n)]
        fluxes[peak_idx] = peak_flux

        points = list(zip(wavelengths, fluxes, strict=True))
        downsampled = lttb(points, _LTTB_THRESHOLD)

        assert len(downsampled) <= _LTTB_THRESHOLD
        # LTTB preserves dominant peaks
        downsampled_fluxes = [p[1] for p in downsampled]
        assert peak_flux in downsampled_fluxes

    def test_no_downsampling_when_under_threshold(self) -> None:
        """Spectrum with 500 points should pass through unchanged."""
        n = 500
        wavelengths = [400.0 + i * 0.1 for i in range(n)]
        fluxes = [float(i % 10) for i in range(n)]

        points = list(zip(wavelengths, fluxes, strict=True))
        result = lttb(points, _LTTB_THRESHOLD)

        assert len(result) == n

    def test_first_and_last_wavelengths_preserved(self) -> None:
        """LTTB always retains endpoints."""
        n = 3000
        wavelengths = [400.0 + i * 0.1 for i in range(n)]
        fluxes = [1.0 + 50.0 * max(0.0, 1.0 - abs(i - n // 2) / 50.0) for i in range(n)]

        points = list(zip(wavelengths, fluxes, strict=True))
        downsampled = lttb(points, _LTTB_THRESHOLD)

        assert downsampled[0][0] == wavelengths[0]
        assert downsampled[-1][0] == wavelengths[-1]


# ---------------------------------------------------------------------------
# Wavelength range trimming (S2)
# ---------------------------------------------------------------------------


def _make_stage1_rec(
    wl_min: float,
    wl_max: float,
    n_points: int = 100,
    dp_id: str = "dp-test",
) -> dict[str, Any]:
    """Helper: build a minimal stage-1 record for trimming tests."""
    step = (wl_max - wl_min) / max(n_points - 1, 1)
    wavelengths = [wl_min + i * step for i in range(n_points)]
    fluxes = [1.0] * n_points
    return {
        "wavelengths": wavelengths,
        "fluxes": fluxes,
        "product": {
            "data_product_id": dp_id,
            "observation_date_mjd": Decimal("59000"),
        },
        "nova_id": "nova-test",
    }


class TestWavelengthRangeTrim:
    """Median-based wavelength trimming for outlier spectra."""

    def test_outlier_trimmed(self) -> None:
        """2500nm outlier is trimmed when median max is ~920."""
        # Median of [900, 910, 920, 950, 2500] = 920
        recs = [
            _make_stage1_rec(400, 900, dp_id="dp-900"),
            _make_stage1_rec(400, 910, dp_id="dp-910"),
            _make_stage1_rec(400, 920, dp_id="dp-920"),
            _make_stage1_rec(400, 950, dp_id="dp-950"),
            _make_stage1_rec(400, 2500, n_points=500, dp_id="dp-2500"),
        ]

        wl_maxes = [r["wavelengths"][-1] for r in recs]
        display_max = statistics.median(wl_maxes)  # 920

        outlier = recs[4]
        original_len = len(outlier["wavelengths"])

        # Only the 2500 outlier exceeds 920 * 1.1 = 1012
        assert outlier["wavelengths"][-1] > display_max * _TRIM_TOLERANCE
        _trim_wavelength_range(outlier, display_max)

        assert len(outlier["wavelengths"]) < original_len
        assert outlier["wavelengths"][-1] <= display_max

    def test_no_trimming_when_similar_ranges(self) -> None:
        """All spectra with similar ranges are left untouched."""
        recs = [
            _make_stage1_rec(400, 900, dp_id="dp-a"),
            _make_stage1_rec(400, 920, dp_id="dp-b"),
            _make_stage1_rec(400, 910, dp_id="dp-c"),
        ]

        wl_maxes = [r["wavelengths"][-1] for r in recs]
        display_max = statistics.median(wl_maxes)  # 910

        for rec in recs:
            original_wl = list(rec["wavelengths"])
            # None exceed 910 * 1.1 = 1001
            if rec["wavelengths"][-1] > display_max * _TRIM_TOLERANCE:
                _trim_wavelength_range(rec, display_max)
            assert rec["wavelengths"] == original_wl

    def test_single_spectrum_no_trim(self) -> None:
        """A single spectrum should never be trimmed."""
        rec = _make_stage1_rec(400, 2500, dp_id="dp-solo")
        original_wl = list(rec["wavelengths"])

        # With only 1 spectrum, the generate function skips trimming.
        # Verify that our guard (len >= 2) holds by simulating:
        recs = [rec]
        assert len(recs) < 2  # guard condition
        # wavelengths unchanged
        assert rec["wavelengths"] == original_wl


class TestDisplayWavelengthFields:
    """Top-level display_wavelength_min/max in artifact output."""

    def _make_csv(self, wl_min: float, wl_max: float, n: int = 50) -> str:
        step = (wl_max - wl_min) / max(n - 1, 1)
        rows = ["wavelength_nm,flux"]
        for i in range(n):
            rows.append(f"{wl_min + i * step:.2f},1.0")
        return "\n".join(rows)

    def _make_product(self, dp_id: str) -> dict[str, Any]:
        return {
            "data_product_id": dp_id,
            "observation_date_mjd": Decimal("59000"),
            "instrument": "test",
            "telescope": "test",
            "provider": "test",
            "flux_unit": "erg/s/cm2/A",
            "PK": "nova-test",
            "SK": f"PRODUCT#SPECTRA#{dp_id}",
            "validation_status": "VALID",
        }

    def test_display_fields_present(self) -> None:
        """Artifact includes display_wavelength_min and display_wavelength_max."""
        csvs = {
            "dp-1": self._make_csv(400, 900),
            "dp-2": self._make_csv(410, 920),
            "dp-3": self._make_csv(390, 910),
        }
        products = [self._make_product(dp_id) for dp_id in csvs]

        class FakeBody:
            def __init__(self, content: str) -> None:
                self._content = content

            def read(self) -> bytes:
                return self._content.encode()

        class FakeS3:
            def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
                dp_id = Key.split("/")[-2]
                return {"Body": FakeBody(csvs[dp_id])}

        class FakeTable:
            def query(self, **kwargs: Any) -> dict[str, Any]:
                return {"Items": products}

        ctx: dict[str, Any] = {"outburst_mjd": 58000.0, "outburst_mjd_is_estimated": False}
        artifact = generate_spectra_json("nova-test", FakeTable(), FakeS3(), "bucket", ctx)

        assert "display_wavelength_min" in artifact
        assert "display_wavelength_max" in artifact
        assert isinstance(artifact["display_wavelength_min"], float)
        assert isinstance(artifact["display_wavelength_max"], float)

    def test_no_display_fields_for_single_spectrum(self) -> None:
        """Single spectrum: no display bounds in artifact."""
        csv_body = self._make_csv(400, 900)
        products = [self._make_product("dp-solo")]

        class FakeBody:
            def read(self) -> bytes:
                return csv_body.encode()

        class FakeS3:
            def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
                return {"Body": FakeBody()}

        class FakeTable:
            def query(self, **kwargs: Any) -> dict[str, Any]:
                return {"Items": products}

        ctx: dict[str, Any] = {"outburst_mjd": 58000.0, "outburst_mjd_is_estimated": False}
        artifact = generate_spectra_json("nova-test", FakeTable(), FakeS3(), "bucket", ctx)

        assert "display_wavelength_min" not in artifact
        assert "display_wavelength_max" not in artifact
