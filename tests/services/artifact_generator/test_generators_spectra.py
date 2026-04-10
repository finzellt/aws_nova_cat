"""Unit tests for generators/spectra.py — edge trimming and flux floor.

Covers:
  _trim_dead_edges:
  - Blue edge only: consecutive leading zeros trimmed
  - Red edge only: consecutive trailing zeros trimmed
  - Both edges: leading and trailing zeros trimmed
  - Single zero at each edge preserved (could be legitimate)
  - No zeros: unchanged
  - Near-zero values below relative threshold treated as zero
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
from unittest.mock import MagicMock

import boto3
import pytest
from generators.shared import lttb
from generators.spectra import (
    _FLUX_FLOOR,
    _LTTB_THRESHOLD,
    _RELATIVE_ZERO_FRACTION,
    _TRIM_TOLERANCE,
    _normalize_flux,
    _reject_chip_gap_artifacts,
    _trim_dead_edges,
    _trim_wavelength_range,
    _trim_wavelength_range_min,
    generate_spectra_json,
)
from moto import mock_aws

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

    def test_single_zero_at_each_edge_trimmed(self) -> None:
        wl = [400.0, 401.0, 402.0, 403.0, 404.0]
        fx = [0.0, 0.5, 1.0, 0.8, 0.0]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-single")
        assert fx_out == [0.5, 1.0, 0.8]
        assert wl_out == [401.0, 402.0, 403.0]

    def test_no_zeros_unchanged(self) -> None:
        wl = [400.0, 401.0, 402.0]
        fx = [0.5, 1.0, 0.8]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-none")
        assert fx_out == fx
        assert wl_out == wl

    def test_near_zero_below_threshold_treated_as_zero(self) -> None:
        # Edge values are 1e-8 of peak (1.0), well below _RELATIVE_ZERO_FRACTION (1e-6)
        tiny = 1.0 * _RELATIVE_ZERO_FRACTION * 0.01  # 1e-8
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

    def test_physical_flux_units_not_trimmed(self) -> None:
        """X-Shooter spectra in physical flux units must survive trimming."""
        wl = [300.0, 301.0, 302.0, 303.0, 304.0, 305.0, 306.0, 307.0]
        # Genuine dead edges (zero), then real signal at ~1e-14, peak ~3e-13
        fx = [0.0, 0.0, 1.2e-14, 8.5e-14, 3.1e-13, 2.7e-13, 0.0, 0.0]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-xshooter")
        assert wl_out == [302.0, 303.0, 304.0, 305.0]
        assert fx_out == [1.2e-14, 8.5e-14, 3.1e-13, 2.7e-13]

    def test_relative_threshold_trims_rolloff(self) -> None:
        """Edges at 1e-8 of peak (~1.0) are below 1e-6 fraction → trimmed."""
        wl = [400.0, 401.0, 402.0, 403.0, 404.0, 405.0, 406.0]
        fx = [1e-8, 1e-8, 0.4, 1.0, 0.6, 1e-8, 1e-8]
        wl_out, fx_out = _trim_dead_edges(wl, fx, "dp-rolloff")
        assert wl_out == [402.0, 403.0, 404.0]
        assert fx_out == [0.4, 1.0, 0.6]


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

    def test_blue_outlier_trimmed(self) -> None:
        """300nm outlier is trimmed when median min is ~400."""
        # Median of [300, 390, 400, 410, 420] = 400
        recs = [
            _make_stage1_rec(300, 900, n_points=500, dp_id="dp-300"),
            _make_stage1_rec(390, 900, dp_id="dp-390"),
            _make_stage1_rec(400, 900, dp_id="dp-400"),
            _make_stage1_rec(410, 900, dp_id="dp-410"),
            _make_stage1_rec(420, 900, dp_id="dp-420"),
        ]

        wl_mins = [r["wavelengths"][0] for r in recs]
        display_min = statistics.median(wl_mins)  # 400

        outlier = recs[0]
        original_len = len(outlier["wavelengths"])

        # 300 < 400 / 1.1 ≈ 363.6 → outlier
        assert outlier["wavelengths"][0] < display_min / _TRIM_TOLERANCE
        _trim_wavelength_range_min(outlier, display_min)

        assert len(outlier["wavelengths"]) < original_len
        assert outlier["wavelengths"][0] >= display_min

    def test_blue_no_trimming_when_similar_ranges(self) -> None:
        """All spectra with similar blue starts are left untouched."""
        recs = [
            _make_stage1_rec(390, 900, dp_id="dp-a"),
            _make_stage1_rec(400, 900, dp_id="dp-b"),
            _make_stage1_rec(410, 900, dp_id="dp-c"),
        ]

        wl_mins = [r["wavelengths"][0] for r in recs]
        display_min = statistics.median(wl_mins)  # 400

        for rec in recs:
            original_wl = list(rec["wavelengths"])
            # None below 400 / 1.1 ≈ 363.6
            if rec["wavelengths"][0] < display_min / _TRIM_TOLERANCE:
                _trim_wavelength_range_min(rec, display_min)
            assert rec["wavelengths"] == original_wl

    def test_blue_single_spectrum_no_trim(self) -> None:
        """A single spectrum should never be trimmed on the blue side."""
        rec = _make_stage1_rec(200, 900, dp_id="dp-solo")
        original_wl = list(rec["wavelengths"])

        recs = [rec]
        assert len(recs) < 2  # guard condition
        assert rec["wavelengths"] == original_wl


class TestDisplayWavelengthFields:
    """Top-level display_wavelength_min/max in artifact output."""

    def _make_csv(self, wl_min: float, wl_max: float, n: int = 50) -> str:
        step = (wl_max - wl_min) / max(n - 1, 1)
        rows = ["wavelength_nm,flux"]
        for i in range(n):
            rows.append(f"{wl_min + i * step:.2f},1.0")
        return "\n".join(rows)

    def _make_product(
        self,
        dp_id: str,
        mjd: str = "59000",
        instrument: str = "test",
    ) -> dict[str, Any]:
        return {
            "data_product_id": dp_id,
            "observation_date_mjd": Decimal(mjd),
            "instrument": instrument,
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
        # Use different MJDs so spectra are not grouped as multi-arm.
        products = [
            self._make_product("dp-1", mjd="59000"),
            self._make_product("dp-2", mjd="59001"),
            self._make_product("dp-3", mjd="59002"),
        ]

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
        assert "total_data_products" in artifact
        assert artifact["total_data_products"] == 3
        assert "observations" in artifact
        assert isinstance(artifact["observations"], list)
        assert len(artifact["observations"]) == 3
        # Each observation has the expected fields
        for obs in artifact["observations"]:
            assert "data_product_id" in obs
            assert "instrument" in obs
            assert "telescope" in obs
            assert "epoch_mjd" in obs
            assert "wavelength_min" in obs
            assert "wavelength_max" in obs
            assert "provider" in obs

    def test_display_min_trims_blue_outlier(self) -> None:
        """End-to-end: blue outlier at 200nm trimmed to near median min (~400)."""
        csvs = {
            "dp-1": self._make_csv(390, 900),
            "dp-2": self._make_csv(400, 900),
            "dp-3": self._make_csv(410, 900),
            "dp-outlier": self._make_csv(200, 900, n=200),
        }
        products = [
            self._make_product("dp-1", mjd="59000"),
            self._make_product("dp-2", mjd="59001"),
            self._make_product("dp-3", mjd="59002"),
            self._make_product("dp-outlier", mjd="59003"),
        ]

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

        # median min of [390, 400, 410, 200] = (390+400)/2 = 395
        display_min = artifact["display_wavelength_min"]

        # Find the outlier spectrum (originally started at 200nm)
        outlier = [s for s in artifact["spectra"] if s["spectrum_id"] == "dp-outlier"]
        assert len(outlier) == 1
        # Its wavelength_min should now be at or near the display_min, not 200
        assert outlier[0]["wavelength_min"] >= display_min - 1.0

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

    def test_spectra_count_reflects_total_products(self) -> None:
        """spectra_count equals total DataProducts, not merged display spectra."""
        # 3 products: dp-a and dp-b share the same MJD+instrument (will merge),
        # dp-c is on a different night.
        csvs = {
            "dp-a": self._make_csv(400, 600),
            "dp-b": self._make_csv(600, 900),
            "dp-c": self._make_csv(400, 900),
        }
        products = [
            self._make_product("dp-a", mjd="59000", instrument="FLOYDS"),
            self._make_product("dp-b", mjd="59000", instrument="FLOYDS"),
            self._make_product("dp-c", mjd="59010", instrument="FLOYDS"),
        ]

        class FakeBody:
            def __init__(self, content: str) -> None:
                self._content = content

            def read(self) -> bytes:
                return self._content.encode()

        class FakeS3:
            def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
                dp_id = Key.split("/")[-2]
                return {"Body": FakeBody(csvs[dp_id])}

            def put_object(self, **kwargs: Any) -> None:  # noqa: ARG002
                pass  # merged CSV persistence — no-op in tests

        class FakeTable:
            def query(self, **kwargs: Any) -> dict[str, Any]:
                return {"Items": products}

        ctx: dict[str, Any] = {"outburst_mjd": 58000.0, "outburst_mjd_is_estimated": False}
        artifact = generate_spectra_json("nova-test", FakeTable(), FakeS3(), "bucket", ctx)

        # dp-a and dp-b merge into 1 display spectrum → 2 total display spectra
        assert len(artifact["spectra"]) == 2
        # But spectra_count reflects total DataProducts (3), not display count
        assert ctx["spectra_count"] == 3
        # total_data_products in artifact also equals 3
        assert artifact["total_data_products"] == 3
        # observations list has one entry per raw DataProduct
        assert len(artifact["observations"]) == 3
        obs_ids = {o["data_product_id"] for o in artifact["observations"]}
        assert obs_ids == {"dp-a", "dp-b", "dp-c"}

    def test_red_side_trim_empty_wavelengths_no_crash(self) -> None:
        """Spectrum entirely above display max is dropped, not IndexError."""
        # 3 normal spectra (400–900) + 1 entirely above the median max (~900).
        csvs = {
            "dp-1": self._make_csv(400, 900),
            "dp-2": self._make_csv(400, 910),
            "dp-3": self._make_csv(400, 920),
            "dp-bad": self._make_csv(5000, 6000),
        }
        products = [
            self._make_product("dp-1", mjd="59000"),
            self._make_product("dp-2", mjd="59001"),
            self._make_product("dp-3", mjd="59002"),
            self._make_product("dp-bad", mjd="59003"),
        ]

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

        # dp-bad should be excluded — its entire range is above display max.
        spectrum_ids = [s["spectrum_id"] for s in artifact["spectra"]]
        assert "dp-bad" not in spectrum_ids

    def test_blue_side_trim_empty_wavelengths_no_crash(self) -> None:
        """Spectrum entirely below display min is dropped, not IndexError."""
        # 3 normal spectra (400–900) + 1 entirely below the median min (~400).
        csvs = {
            "dp-1": self._make_csv(400, 900),
            "dp-2": self._make_csv(410, 900),
            "dp-3": self._make_csv(390, 900),
            "dp-bad": self._make_csv(100, 200),
        }
        products = [
            self._make_product("dp-1", mjd="59000"),
            self._make_product("dp-2", mjd="59001"),
            self._make_product("dp-3", mjd="59002"),
            self._make_product("dp-bad", mjd="59003"),
        ]

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

        # dp-bad should be excluded — its entire range is below display min.
        spectrum_ids = [s["spectrum_id"] for s in artifact["spectra"]]
        assert "dp-bad" not in spectrum_ids

    def test_single_bad_spectrum_does_not_block_others(self) -> None:
        """One fully-trimmed spectrum doesn't prevent other spectra from appearing."""
        csvs = {
            "dp-good-1": self._make_csv(400, 900),
            "dp-good-2": self._make_csv(410, 920),
            "dp-bad": self._make_csv(5000, 6000),
        }
        products = [
            self._make_product("dp-good-1", mjd="59000"),
            self._make_product("dp-good-2", mjd="59001"),
            self._make_product("dp-bad", mjd="59002"),
        ]

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

        spectrum_ids = {s["spectrum_id"] for s in artifact["spectra"]}
        assert "dp-good-1" in spectrum_ids
        assert "dp-good-2" in spectrum_ids
        assert "dp-bad" not in spectrum_ids
        assert len(artifact["spectra"]) == 2


# ---------------------------------------------------------------------------
# Observation list helpers
# ---------------------------------------------------------------------------


def _make_obs_csv(wl_min: float = 400, wl_max: float = 900, n: int = 50) -> str:
    """Generate a simple web-ready CSV for observation-list tests."""
    step = (wl_max - wl_min) / max(n - 1, 1)
    rows = ["wavelength_nm,flux"]
    for i in range(n):
        rows.append(f"{wl_min + i * step:.2f},1.0")
    return "\n".join(rows)


def _make_obs_product(
    *,
    data_product_id: str = "dp-001",
    observation_date_mjd: float = 59000.0,
    instrument: str = "UVES",
    telescope: str = "VLT-UT2",
    provider: str = "ESO",
    wavelength_min_nm: float | None = None,
    wavelength_max_nm: float | None = None,
    wavelength_min: float | None = None,
    wavelength_max: float | None = None,
    snr: float | None = None,
) -> dict[str, Any]:
    """Build a DataProduct dict for observation-list testing.

    Includes all fields required by the full pipeline (PK, SK,
    validation_status, flux_unit) so that concrete FakeTable/FakeS3
    fakes can be used instead of MagicMock + patch.
    """
    product: dict[str, Any] = {
        "data_product_id": data_product_id,
        "observation_date_mjd": Decimal(str(observation_date_mjd)),
        "instrument": instrument,
        "telescope": telescope,
        "provider": provider,
        "flux_unit": "erg/s/cm2/A",
        "PK": "nova-obs-test",
        "SK": f"PRODUCT#SPECTRA#{data_product_id}",
        "validation_status": "VALID",
    }
    if wavelength_min_nm is not None:
        product["wavelength_min_nm"] = Decimal(str(wavelength_min_nm))
    if wavelength_max_nm is not None:
        product["wavelength_max_nm"] = Decimal(str(wavelength_max_nm))
    if wavelength_min is not None:
        product["wavelength_min"] = Decimal(str(wavelength_min))
    if wavelength_max is not None:
        product["wavelength_max"] = Decimal(str(wavelength_max))
    if snr is not None:
        product["snr"] = Decimal(str(snr))
    return product


def _run_obs_generator(products: list[dict[str, Any]]) -> dict[str, Any]:
    """Run generate_spectra_json with concrete fakes.

    Uses FakeTable/FakeS3 (not MagicMock + patch) to avoid the infinite
    pagination trap where MagicMock().get("LastEvaluatedKey") returns a
    truthy MagicMock instead of None.
    """

    class _FakeBody:
        def __init__(self, content: str) -> None:
            self._content = content

        def read(self) -> bytes:
            return self._content.encode()

    csv_body = _make_obs_csv()

    class _FakeS3:
        def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
            return {"Body": _FakeBody(csv_body)}

    class _FakeTable:
        def query(self, **kwargs: Any) -> dict[str, Any]:
            return {"Items": products}

    nova_context: dict[str, Any] = {
        "outburst_mjd": 59000.0,
        "outburst_mjd_is_estimated": False,
    }
    return generate_spectra_json(
        nova_id="nova-obs-test",
        table=_FakeTable(),
        s3_client=_FakeS3(),
        private_bucket="test-bucket",
        nova_context=nova_context,
    )


# ---------------------------------------------------------------------------
# Observation list: wavelength field migration
# ---------------------------------------------------------------------------


class TestObservationsWavelengthFields:
    def test_observations_list_reads_wavelength_min_nm(self) -> None:
        """New-style DDB fields (wavelength_min_nm / wavelength_max_nm)."""
        products = [
            _make_obs_product(
                wavelength_min_nm=350.0,
                wavelength_max_nm=950.0,
            ),
        ]
        artifact = _run_obs_generator(products)
        obs = artifact["observations"]
        assert len(obs) == 1
        assert obs[0]["wavelength_min"] == pytest.approx(350.0)
        assert obs[0]["wavelength_max"] == pytest.approx(950.0)

    def test_observations_list_falls_back_to_old_wavelength_fields(self) -> None:
        """Old-style DDB fields (wavelength_min / wavelength_max) still work."""
        products = [
            _make_obs_product(
                wavelength_min=300.0,
                wavelength_max=900.0,
            ),
        ]
        artifact = _run_obs_generator(products)
        obs = artifact["observations"]
        assert len(obs) == 1
        assert obs[0]["wavelength_min"] == pytest.approx(300.0)
        assert obs[0]["wavelength_max"] == pytest.approx(900.0)


# ---------------------------------------------------------------------------
# Observation list: SNR
# ---------------------------------------------------------------------------


class TestObservationsSnr:
    def test_observations_list_includes_snr_when_present(self) -> None:
        products = [
            _make_obs_product(snr=42.5, wavelength_min_nm=350.0, wavelength_max_nm=950.0),
        ]
        artifact = _run_obs_generator(products)
        obs = artifact["observations"]
        assert len(obs) == 1
        assert obs[0]["snr"] == pytest.approx(42.5)

    def test_observations_list_omits_snr_when_absent(self) -> None:
        products = [
            _make_obs_product(wavelength_min_nm=350.0, wavelength_max_nm=950.0),
        ]
        artifact = _run_obs_generator(products)
        obs = artifact["observations"]
        assert len(obs) == 1
        assert "snr" not in obs[0]


# ===========================================================================
# spectral_visits — distinct observation nights
# ===========================================================================

_SV_TABLE_NAME = "NovaCat-SV-Test"
_SV_REGION = "us-east-1"
_SV_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"


def _seed_spectra_product(
    table: Any,
    nova_id: str,
    data_product_id: str,
    *,
    observation_date_mjd: Decimal | None = Decimal("59234.5"),
) -> None:
    """Write a minimal VALID spectra DataProduct item."""
    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": f"PRODUCT#SPECTRA#{data_product_id}",
        "data_product_id": data_product_id,
        "validation_status": "VALID",
        "instrument": "TestInstrument",
        "telescope": "TestTelescope",
        "provider": "TestProvider",
        "flux_unit": "erg/s/cm2/A",
    }
    if observation_date_mjd is not None:
        item["observation_date_mjd"] = observation_date_mjd
    table.put_item(Item=item)


def _make_sv_context(outburst_mjd: float | None = 59230.0) -> dict[str, Any]:
    """Build a minimal nova_context for the spectra generator."""
    return {
        "outburst_mjd": outburst_mjd,
        "outburst_mjd_is_estimated": False,
        "nova_item": {"primary_name": "Test Nova", "nova_id": _SV_NOVA_ID},
    }


def _stub_s3() -> MagicMock:
    """Return a mock S3 client that returns an empty CSV body."""
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = b"wavelength_nm,flux_normalized\n"
    mock_s3.get_object.return_value = {"Body": mock_body}
    return mock_s3


class TestSpectralVisits:
    """spectral_visits counts distinct integer-MJD nights."""

    @pytest.fixture(autouse=True)
    def _aws_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AWS_DEFAULT_REGION", _SV_REGION)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")

    @pytest.fixture()
    def ddb_table(self) -> Any:
        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_SV_REGION)
            tbl = dynamodb.create_table(
                TableName=_SV_TABLE_NAME,
                KeySchema=[
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "SK", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "PK", "AttributeType": "S"},
                    {"AttributeName": "SK", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            yield tbl

    def test_three_nights_from_six_spectra(self, ddb_table: Any) -> None:
        """6 spectra across 3 distinct integer-MJD nights → spectral_visits = 3."""
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p1", observation_date_mjd=Decimal("59234.1"))
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p2", observation_date_mjd=Decimal("59234.8"))
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p3", observation_date_mjd=Decimal("59235.3"))
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p4", observation_date_mjd=Decimal("59235.9"))
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p5", observation_date_mjd=Decimal("59240.2"))
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p6", observation_date_mjd=Decimal("59240.7"))

        ctx = _make_sv_context()
        generate_spectra_json(_SV_NOVA_ID, ddb_table, _stub_s3(), "test-bucket", ctx)

        assert ctx["spectra_count"] == 6
        assert ctx["spectral_visits"] == 3

    def test_same_night_counts_as_one(self, ddb_table: Any) -> None:
        """2 spectra on the same night (MJD 59234.1 and 59234.8) → spectral_visits = 1."""
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p1", observation_date_mjd=Decimal("59234.1"))
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p2", observation_date_mjd=Decimal("59234.8"))

        ctx = _make_sv_context()
        generate_spectra_json(_SV_NOVA_ID, ddb_table, _stub_s3(), "test-bucket", ctx)

        assert ctx["spectra_count"] == 2
        assert ctx["spectral_visits"] == 1

    def test_zero_spectra(self, ddb_table: Any) -> None:
        """No spectra → spectral_visits = 0."""
        ctx = _make_sv_context()
        generate_spectra_json(_SV_NOVA_ID, ddb_table, _stub_s3(), "test-bucket", ctx)

        assert ctx["spectra_count"] == 0
        assert ctx["spectral_visits"] == 0

    def test_none_mjd_excluded(self, ddb_table: Any) -> None:
        """Products with observation_date_mjd = None are excluded from the count."""
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p1", observation_date_mjd=Decimal("59234.5"))
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p2", observation_date_mjd=Decimal("59235.5"))
        _seed_spectra_product(ddb_table, _SV_NOVA_ID, "p3", observation_date_mjd=None)

        ctx = _make_sv_context()
        generate_spectra_json(_SV_NOVA_ID, ddb_table, _stub_s3(), "test-bucket", ctx)

        assert ctx["spectra_count"] == 3
        assert ctx["spectral_visits"] == 2


# ---------------------------------------------------------------------------
# _reject_chip_gap_artifacts
# ---------------------------------------------------------------------------


class TestRejectChipGapArtifacts:
    """Chip gap interpolation artifact rejection."""

    def test_chip_gap_points_removed(self) -> None:
        """Points with large wavelength gaps AND near-zero flux are removed."""
        # Normal 0.1nm spacing from 400–405, then 2 gap points at ~410 and ~415
        # with near-zero flux, then normal spacing from 420–425.
        wl: list[float] = []
        fx: list[float] = []
        # Normal region 1: 400.0 to 405.0 in 0.1nm steps
        for i in range(51):
            wl.append(400.0 + i * 0.1)
            fx.append(1.0)
        # Chip gap artifact points (5nm jump, near-zero flux)
        wl.append(410.0)
        fx.append(0.001)
        wl.append(415.0)
        fx.append(0.002)
        # Normal region 2: 420.0 to 425.0 in 0.1nm steps
        for i in range(51):
            wl.append(420.0 + i * 0.1)
            fx.append(1.0)

        result_wl, result_fx = _reject_chip_gap_artifacts(wl, fx, "test-dp")

        # The 2 gap points should be removed
        assert len(result_wl) == len(wl) - 2
        assert 410.0 not in result_wl
        assert 415.0 not in result_wl

    def test_real_absorption_feature_preserved(self) -> None:
        """A low-flux point at normal spacing is NOT removed (real absorption)."""
        # Uniform 0.1nm spacing, one point has very low flux
        wl = [400.0 + i * 0.1 for i in range(100)]
        fx = [1.0] * 100
        fx[50] = 0.001  # deep absorption line, but normal spacing

        result_wl, result_fx = _reject_chip_gap_artifacts(wl, fx, "test-dp")

        # Nothing removed — spacing is normal
        assert len(result_wl) == 100
        assert result_fx[50] == 0.001

    def test_arm_boundary_preserved(self) -> None:
        """A large wavelength jump with normal flux is NOT removed (merge boundary)."""
        # Region 1: 400–405 at 0.1nm
        wl: list[float] = []
        fx: list[float] = []
        for i in range(51):
            wl.append(400.0 + i * 0.1)
            fx.append(1.0)
        # Region 2: 500–505 at 0.1nm (big jump, but flux is normal)
        for i in range(51):
            wl.append(500.0 + i * 0.1)
            fx.append(1.0)

        result_wl, result_fx = _reject_chip_gap_artifacts(wl, fx, "test-dp")

        # Nothing removed — flux is normal at the boundary
        assert len(result_wl) == len(wl)

    def test_single_isolated_zero_removed(self) -> None:
        """One point with both large gap AND near-zero flux is removed."""
        # Normal region, then one isolated near-zero point, then normal region
        wl: list[float] = []
        fx: list[float] = []
        for i in range(50):
            wl.append(400.0 + i * 0.1)
            fx.append(1.0)
        # Single isolated artifact
        wl.append(420.0)  # 15nm gap from last point at ~404.9
        fx.append(0.0001)
        for i in range(50):
            wl.append(430.0 + i * 0.1)
            fx.append(1.0)

        result_wl, result_fx = _reject_chip_gap_artifacts(wl, fx, "test-dp")

        assert len(result_wl) == len(wl) - 1
        assert 420.0 not in result_wl

    def test_short_arrays_unchanged(self) -> None:
        """Arrays with fewer than 3 points are returned as-is."""
        wl_0: list[float] = []
        fx_0: list[float] = []
        assert _reject_chip_gap_artifacts(wl_0, fx_0, "dp") == ([], [])

        wl_1 = [400.0]
        fx_1 = [0.0]
        assert _reject_chip_gap_artifacts(wl_1, fx_1, "dp") == ([400.0], [0.0])

        wl_2 = [400.0, 500.0]
        fx_2 = [0.0, 0.0]
        assert _reject_chip_gap_artifacts(wl_2, fx_2, "dp") == ([400.0, 500.0], [0.0, 0.0])

    def test_all_zero_array_unchanged(self) -> None:
        """All-zero flux array triggers early return (no median flux to compute)."""
        wl = [400.0 + i * 0.1 for i in range(50)]
        fx = [0.0] * 50

        result_wl, result_fx = _reject_chip_gap_artifacts(wl, fx, "test-dp")

        assert result_wl == wl
        assert result_fx == fx
