"""Unit tests for multi-arm spectra merge logic in generators/spectra.py (S4).

Groups:
  1 — Arm group detection (_merge_multi_arm_spectra grouping)
  2 — Merge validation (overlap rejection in _merge_arm_group)
  3 — Overlap blending (_blend_overlap)
  4 — Gap handling (no NaN sentinels — simple concatenation)
  5 — LTTB downsampling (segment_aware_lttb)
  6 — Composite spectrum identity (deterministic, order-independent)
  7 — Merged CSV round-trip
  8 — End-to-end merge in generate_spectra_json
"""

from __future__ import annotations

import csv
import hashlib
import io
import math
import uuid
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from generators.shared import LTTB_THRESHOLD, segment_aware_lttb
from generators.spectra import (
    _blend_overlap,
    _merge_arm_group,
    _merge_multi_arm_spectra,
    _parse_web_ready_csv,
    generate_spectra_json,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stage1(
    *,
    dp_id: str = "dp-1",
    instrument: str = "XSHOOTER",
    mjd: float | Decimal = 56368.971,
    wl_min: float = 400.0,
    wl_max: float = 900.0,
    n_points: int = 50,
    flux_val: float = 1.0,
) -> dict[str, Any]:
    """Build a minimal stage-1 record for merge tests."""
    step = (wl_max - wl_min) / max(n_points - 1, 1)
    wavelengths = [wl_min + i * step for i in range(n_points)]
    fluxes = [flux_val] * n_points
    return {
        "wavelengths": wavelengths,
        "fluxes": fluxes,
        "product": {
            "data_product_id": dp_id,
            "instrument": instrument,
            "observation_date_mjd": mjd,
            "telescope": "VLT",
            "provider": "ESO",
            "flux_unit": "erg/s/cm2/A",
        },
        "nova_id": "nova-test",
    }


def _make_csv_body(wl_min: float, wl_max: float, n: int = 50) -> str:
    """Generate a web-ready CSV string."""
    step = (wl_max - wl_min) / max(n - 1, 1)
    rows = ["wavelength_nm,flux"]
    for i in range(n):
        wl = wl_min + i * step
        rows.append(f"{wl:.4f},1.0")
    return "\n".join(rows)


class FakeBody:
    def __init__(self, content: str) -> None:
        self._content = content

    def read(self) -> bytes:
        return self._content.encode()


class FakeS3:
    """S3 mock that returns pre-loaded CSV bodies by data_product_id."""

    def __init__(self, csvs: dict[str, str] | None = None) -> None:
        self._csvs = csvs or {}
        self.put_calls: list[dict[str, Any]] = []

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        dp_id = Key.split("/")[-2]
        return {"Body": FakeBody(self._csvs[dp_id])}

    def put_object(self, **kwargs: Any) -> None:  # noqa: ANN401
        self.put_calls.append(kwargs)


class FakeTable:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self._items = items

    def query(self, **kwargs: Any) -> dict[str, Any]:
        return {"Items": self._items}


# ---------------------------------------------------------------------------
# Group 1 — Arm group detection
# ---------------------------------------------------------------------------


class TestArmGroupDetection:
    """Grouping of stage-1 records into merge candidates."""

    def test_same_instrument_mjd_within_tolerance_grouped(self) -> None:
        """Two XSHOOTER records with MJDs 0.001d apart → one group of size 2."""
        recs = [
            _make_stage1(dp_id="a", mjd=56368.971, wl_min=300, wl_max=550),
            _make_stage1(dp_id="b", mjd=56368.972, wl_min=550, wl_max=1020),
        ]
        result = _merge_multi_arm_spectra(recs, "nova-t", MagicMock(), "bucket")
        # Merged into 1 record (from 2 arms).
        assert len(result) == 1

    def test_same_instrument_mjd_outside_tolerance_separate(self) -> None:
        """Two XSHOOTER records with MJDs 1.0d apart → two separate groups."""
        recs = [
            _make_stage1(dp_id="a", mjd=56368.0, wl_min=400, wl_max=600),
            _make_stage1(dp_id="b", mjd=56369.0, wl_min=400, wl_max=600),
        ]
        result = _merge_multi_arm_spectra(recs, "nova-t", MagicMock(), "bucket")
        assert len(result) == 2

    def test_different_instruments_same_mjd_separate(self) -> None:
        """XSHOOTER and UVES at the same MJD → two separate groups."""
        recs = [
            _make_stage1(dp_id="a", instrument="XSHOOTER", mjd=56368.971),
            _make_stage1(dp_id="b", instrument="UVES", mjd=56368.971),
        ]
        result = _merge_multi_arm_spectra(recs, "nova-t", MagicMock(), "bucket")
        assert len(result) == 2

    def test_three_arms_same_observation_one_group(self) -> None:
        """Three XSHOOTER arms within 0.01d → one merged group of size 3."""
        recs = [
            _make_stage1(dp_id="uvb", mjd=56368.970, wl_min=300, wl_max=550),
            _make_stage1(dp_id="vis", mjd=56368.975, wl_min=550, wl_max=1020),
            _make_stage1(dp_id="nir", mjd=56368.978, wl_min=1020, wl_max=2480),
        ]
        result = _merge_multi_arm_spectra(recs, "nova-t", MagicMock(), "bucket")
        assert len(result) == 1

    def test_mixed_groupable_and_not(self) -> None:
        """5 records → 3 groups: (3 XS arms), (1 UVES), (1 XS different night)."""
        recs = [
            _make_stage1(
                dp_id="xs-uvb", instrument="XSHOOTER", mjd=56368.970, wl_min=300, wl_max=550
            ),
            _make_stage1(
                dp_id="xs-vis", instrument="XSHOOTER", mjd=56368.975, wl_min=550, wl_max=1020
            ),
            _make_stage1(
                dp_id="xs-nir", instrument="XSHOOTER", mjd=56368.978, wl_min=1020, wl_max=2480
            ),
            _make_stage1(dp_id="uves", instrument="UVES", mjd=56368.971, wl_min=400, wl_max=700),
            _make_stage1(
                dp_id="xs-late", instrument="XSHOOTER", mjd=56400.0, wl_min=300, wl_max=550
            ),
        ]
        result = _merge_multi_arm_spectra(recs, "nova-t", MagicMock(), "bucket")
        assert len(result) == 3

    def test_observation_date_mjd_as_decimal(self) -> None:
        """DDB returns Decimals — grouping must handle them."""
        recs = [
            _make_stage1(dp_id="a", mjd=Decimal("56368.971"), wl_min=300, wl_max=550),
            _make_stage1(dp_id="b", mjd=Decimal("56368.972"), wl_min=550, wl_max=1020),
        ]
        result = _merge_multi_arm_spectra(recs, "nova-t", MagicMock(), "bucket")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Group 2 — Merge validation
# ---------------------------------------------------------------------------


class TestMergeValidation:
    """Overlap validation accepts or rejects arm groups."""

    def test_non_overlapping_arms_accepted(self) -> None:
        """UVES blue (350–470nm) and red (520–850nm) with gap → merge proceeds."""
        group = [
            _make_stage1(dp_id="blue", wl_min=350, wl_max=470),
            _make_stage1(dp_id="red", wl_min=520, wl_max=850),
        ]
        merged = _merge_arm_group(group, "nova-t", MagicMock(), "bucket")
        assert merged is not None
        assert merged["wavelengths"][0] == pytest.approx(350.0, abs=1)
        assert merged["wavelengths"][-1] == pytest.approx(850.0, abs=1)

    def test_moderate_overlap_accepted(self) -> None:
        """X-Shooter VIS/NIR with 26nm overlap → merge proceeds."""
        group = [
            _make_stage1(dp_id="vis", wl_min=550, wl_max=1020, n_points=100),
            _make_stage1(dp_id="nir", wl_min=994, wl_max=2480, n_points=100),
        ]
        merged = _merge_arm_group(group, "nova-t", MagicMock(), "bucket")
        assert merged is not None

    def test_excessive_overlap_drops_worse_arm(self) -> None:
        """200nm overlap (>100nm limit) → worse arm dropped, survivor returned."""
        group = [
            _make_stage1(dp_id="a", wl_min=400, wl_max=700),
            _make_stage1(dp_id="b", wl_min=500, wl_max=900),
        ]
        merged = _merge_arm_group(group, "nova-t", MagicMock(), "bucket")
        # One arm survives — returned as a single record (no merge possible).
        assert merged is not None
        # Arm "b" has broader range (400nm vs 300nm) → kept.
        assert merged["product"]["data_product_id"] == "b"

    def test_single_arm_group_skipped(self) -> None:
        """Single-arm input to _merge_multi_arm_spectra → passed through unchanged."""
        rec = _make_stage1(dp_id="solo")
        result = _merge_multi_arm_spectra([rec], "nova-t", MagicMock(), "bucket")
        assert len(result) == 1
        assert result[0] is rec


# ---------------------------------------------------------------------------
# Group 3 — Overlap blending
# ---------------------------------------------------------------------------


class TestOverlapBlending:
    """Flux averaging in overlap regions."""

    def test_simple_two_arm_overlap(self) -> None:
        """Overlap at [4, 5] → averaged flux; non-overlap regions unchanged."""
        wl_a = [1.0, 2.0, 3.0, 4.0, 5.0]
        fx_a = [10.0, 20.0, 30.0, 40.0, 50.0]
        wl_b = [4.0, 5.0, 6.0, 7.0, 8.0]
        fx_b = [40.0, 50.0, 60.0, 70.0, 80.0]

        merged_wl, merged_fx = _blend_overlap(wl_a, fx_a, wl_b, fx_b)

        assert merged_wl == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        # Pre-overlap: unchanged.
        assert merged_fx[0] == pytest.approx(10.0)
        assert merged_fx[1] == pytest.approx(20.0)
        assert merged_fx[2] == pytest.approx(30.0)
        # Overlap: averaged (both arms have identical flux at these points).
        assert merged_fx[3] == pytest.approx(40.0)
        assert merged_fx[4] == pytest.approx(50.0)
        # Post-overlap: unchanged.
        assert merged_fx[5] == pytest.approx(60.0)
        assert merged_fx[6] == pytest.approx(70.0)
        assert merged_fx[7] == pytest.approx(80.0)

    def test_overlap_with_different_point_densities(self) -> None:
        """Arm A has 10 points in overlap, arm B has 5 → uses denser grid."""
        # Arm A: 0-20nm, with dense coverage in overlap zone 15-20nm.
        wl_a = [float(i) for i in range(21)]  # 0..20, 21 points
        fx_a = [10.0] * 21
        # Arm B: 15-30nm, with 5-point steps = fewer points in overlap.
        wl_b = [15.0, 18.0, 20.0, 25.0, 30.0]
        fx_b = [20.0, 20.0, 20.0, 20.0, 20.0]

        merged_wl, merged_fx = _blend_overlap(wl_a, fx_a, wl_b, fx_b)

        # Result should use the denser grid (from arm A) in the overlap zone.
        # Overlap is [15, 20]. Arm A has 6 points there (15..20), arm B has 3.
        # The denser grid is arm A's.
        overlap_points = [w for w in merged_wl if 15.0 <= w <= 20.0]
        assert len(overlap_points) == 6  # arm A's density
        # Averaged flux in overlap: (10 + 20) / 2 = 15.
        for w, f in zip(merged_wl, merged_fx, strict=True):
            if 15.0 <= w <= 20.0:
                assert f == pytest.approx(15.0, abs=0.5)

    def test_zero_width_overlap_arms_exactly_touch(self) -> None:
        """Arm A ends at 550.0, arm B starts at 550.0 → single shared point."""
        wl_a = [540.0, 545.0, 550.0]
        fx_a = [1.0, 2.0, 3.0]
        wl_b = [550.0, 555.0, 560.0]
        fx_b = [3.0, 4.0, 5.0]

        merged_wl, merged_fx = _blend_overlap(wl_a, fx_a, wl_b, fx_b)

        # The shared point at 550.0 should appear once.
        assert merged_wl.count(550.0) == 1
        # Flux at 550.0 should be averaged: (3.0 + 3.0) / 2 = 3.0.
        idx_550 = merged_wl.index(550.0)
        assert merged_fx[idx_550] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# Group 4 — Gap handling
# ---------------------------------------------------------------------------


class TestGapHandling:
    """Gap regions between arms produce no NaN sentinels — simple concatenation."""

    def test_two_arms_with_gap_no_nan(self) -> None:
        """Arm A ends at 470nm, arm B starts at 520nm → no NaN, just a wavelength jump."""
        arm_a = _make_stage1(dp_id="blue", wl_min=350, wl_max=470, n_points=100)
        arm_b = _make_stage1(dp_id="red", wl_min=520, wl_max=850, n_points=100)
        group = [arm_a, arm_b]

        merged = _merge_arm_group(group, "nova-t", MagicMock(), "bucket")
        assert merged is not None

        nan_count = sum(1 for f in merged["fluxes"] if math.isnan(f))
        assert nan_count == 0
        # All wavelengths are real floats.
        assert all(isinstance(w, float) for w in merged["wavelengths"])
        # Wavelengths span both arms.
        assert merged["wavelengths"][0] == pytest.approx(350.0, abs=1)
        assert merged["wavelengths"][-1] == pytest.approx(850.0, abs=1)
        # Total points = sum of both arms (no sentinel inserted).
        assert len(merged["wavelengths"]) == 200

    def test_three_arms_with_two_gaps_no_nan(self) -> None:
        """UVB/VIS/NIR with gaps → zero NaN sentinels, just concatenated."""
        arms = [
            _make_stage1(dp_id="uvb", wl_min=300, wl_max=470, n_points=100),
            _make_stage1(dp_id="vis", wl_min=520, wl_max=900, n_points=100),
            _make_stage1(dp_id="nir", wl_min=1020, wl_max=2480, n_points=100),
        ]
        merged = _merge_arm_group(arms, "nova-t", MagicMock(), "bucket")
        assert merged is not None

        nan_count = sum(1 for f in merged["fluxes"] if math.isnan(f))
        assert nan_count == 0
        assert len(merged["wavelengths"]) == 300

    def test_no_gap_contiguous_arms(self) -> None:
        """Arms that overlap → blended, no NaN."""
        arms = [
            _make_stage1(dp_id="vis", wl_min=550, wl_max=1020, n_points=100),
            _make_stage1(dp_id="nir", wl_min=994, wl_max=2480, n_points=100),
        ]
        merged = _merge_arm_group(arms, "nova-t", MagicMock(), "bucket")
        assert merged is not None

        nan_count = sum(1 for f in merged["fluxes"] if math.isnan(f))
        assert nan_count == 0


# ---------------------------------------------------------------------------
# Group 5 — Segment-aware LTTB budget allocation
# ---------------------------------------------------------------------------


class TestSegmentAwareLttb:
    """Single-pass LTTB downsampling (no NaN segmentation)."""

    def test_downsampled_when_over_threshold(self) -> None:
        """3000-point spectrum reduced to ≤ LTTB_THRESHOLD."""
        n = 3000
        wl = [400.0 + i * 0.1 for i in range(n)]
        fx = [1.0] * n

        out_wl, out_fx = segment_aware_lttb(wl, fx)

        assert len(out_wl) <= LTTB_THRESHOLD
        assert len(out_wl) == len(out_fx)
        assert not any(math.isnan(f) for f in out_fx)

    def test_passthrough_when_under_threshold(self) -> None:
        """500-point spectrum passes through unchanged."""
        n = 500
        wl = [400.0 + i * 0.1 for i in range(n)]
        fx = [1.0] * n

        out_wl, out_fx = segment_aware_lttb(wl, fx)

        assert len(out_wl) == n
        assert out_wl == wl
        assert out_fx == fx

    def test_endpoints_preserved(self) -> None:
        """LTTB always retains first and last points."""
        n = 3000
        wl = [400.0 + i * 0.1 for i in range(n)]
        fx = [1.0 + 50.0 * max(0.0, 1.0 - abs(i - n // 2) / 50.0) for i in range(n)]

        out_wl, out_fx = segment_aware_lttb(wl, fx)

        assert out_wl[0] == pytest.approx(wl[0])
        assert out_wl[-1] == pytest.approx(wl[-1])


# ---------------------------------------------------------------------------
# Group 6 — Composite spectrum identity
# ---------------------------------------------------------------------------


class TestCompositeIdentity:
    """Composite ID properties: deterministic, order-independent, distinct."""

    @staticmethod
    def _composite_id(ids: list[str]) -> str:
        """Replicate the composite ID logic from _merge_arm_group."""
        sorted_ids = sorted(ids)
        return str(uuid.UUID(hashlib.md5("|".join(sorted_ids).encode()).hexdigest()))  # noqa: S324

    def test_deterministic(self) -> None:
        """Same input IDs always produce the same composite ID."""
        ids = ["aaa", "bbb", "ccc"]
        assert self._composite_id(ids) == self._composite_id(ids)

    def test_order_independent(self) -> None:
        """["aaa", "bbb"] and ["bbb", "aaa"] produce the same ID."""
        assert self._composite_id(["aaa", "bbb"]) == self._composite_id(["bbb", "aaa"])

    def test_distinct_from_constituents(self) -> None:
        """Composite ID differs from any individual input ID."""
        ids = ["aaa", "bbb"]
        composite = self._composite_id(ids)
        for individual in ids:
            assert composite != individual


# ---------------------------------------------------------------------------
# Group 7 — Merged CSV round-trip
# ---------------------------------------------------------------------------


class TestMergedCsvRoundTrip:
    """CSV serialization/deserialization for merged spectra."""

    @staticmethod
    def _serialize_csv(wavelengths: list[float], fluxes: list[float]) -> str:
        """Serialize wavelength/flux arrays to CSV (same as _persist_merged_csv)."""
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["wavelength_nm", "flux"])
        for wl, fx in zip(wavelengths, fluxes, strict=True):
            writer.writerow([wl, fx])
        return buf.getvalue()

    def test_round_trip(self) -> None:
        """Merged spectrum round-trips identically through CSV."""
        wavelengths = [400.0, 500.0, 600.0, 700.0, 800.0]
        fluxes = [1.0, 2.5, 3.0, 2.0, 1.5]

        csv_body = self._serialize_csv(wavelengths, fluxes)
        parsed_wl, parsed_fx = _parse_web_ready_csv(csv_body)

        assert len(parsed_wl) == len(wavelengths)
        for orig, parsed in zip(fluxes, parsed_fx, strict=True):
            assert parsed == pytest.approx(orig)


# ---------------------------------------------------------------------------
# Group 8 — End-to-end merge in generate_spectra_json
# ---------------------------------------------------------------------------


class TestEndToEndMerge:
    """Integration: full pipeline with mocked DDB and S3."""

    def test_xshooter_three_arm_merge(self) -> None:
        """3 X-Shooter arms → 1 merged spectrum in output artifact."""
        mjd = Decimal("59200.500")
        products = [
            {
                "data_product_id": "dp-uvb",
                "instrument": "XSHOOTER",
                "observation_date_mjd": mjd,
                "telescope": "VLT",
                "provider": "ESO",
                "flux_unit": "erg/s/cm2/A",
                "PK": "nova-e2e",
                "SK": "PRODUCT#SPECTRA#dp-uvb",
                "validation_status": "VALID",
            },
            {
                "data_product_id": "dp-vis",
                "instrument": "XSHOOTER",
                "observation_date_mjd": mjd,
                "telescope": "VLT",
                "provider": "ESO",
                "flux_unit": "erg/s/cm2/A",
                "PK": "nova-e2e",
                "SK": "PRODUCT#SPECTRA#dp-vis",
                "validation_status": "VALID",
            },
            {
                "data_product_id": "dp-nir",
                "instrument": "XSHOOTER",
                "observation_date_mjd": mjd,
                "telescope": "VLT",
                "provider": "ESO",
                "flux_unit": "erg/s/cm2/A",
                "PK": "nova-e2e",
                "SK": "PRODUCT#SPECTRA#dp-nir",
                "validation_status": "VALID",
            },
        ]

        # UVB: 300–550nm, VIS: 550–1020nm, NIR: 994–2480nm.
        csvs = {
            "dp-uvb": _make_csv_body(300, 550, n=500),
            "dp-vis": _make_csv_body(550, 1020, n=1000),
            "dp-nir": _make_csv_body(994, 2480, n=1000),
        }

        table = FakeTable(products)
        s3 = FakeS3(csvs)
        ctx: dict[str, Any] = {"outburst_mjd": 59190.0, "outburst_mjd_is_estimated": False}

        artifact = generate_spectra_json("nova-e2e", table, s3, "bucket", ctx)

        spectra = artifact["spectra"]
        # Should be 1 merged spectrum, not 3 separate ones.
        assert len(spectra) == 1

        spec = spectra[0]
        # Wavelength range spans all 3 arms.
        assert spec["wavelength_min"] < 310
        assert spec["wavelength_max"] > 2400

        # Composite ID — not any of the input IDs.
        assert spec["spectrum_id"] != "dp-uvb"
        assert spec["spectrum_id"] != "dp-vis"
        assert spec["spectrum_id"] != "dp-nir"

        # Point budget respected.
        assert len(spec["wavelengths"]) <= LTTB_THRESHOLD + 10  # margin for rounding

        # No NaN or None values in the output arrays.
        assert all(isinstance(f, float) for f in spec["flux_normalized"])
        assert all(isinstance(w, float) for w in spec["wavelengths"])
        assert not any(math.isnan(f) for f in spec["flux_normalized"])
        assert len(spec["flux_normalized"]) == len(spec["wavelengths"])
