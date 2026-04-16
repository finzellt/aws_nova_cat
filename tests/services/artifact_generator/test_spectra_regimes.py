"""Unit and integration tests for ADR-035 spectra regime features.

Groups:
  1 — Regime assignment (_assign_spectra_regime)
  2 — Cross-boundary splitting (_split_cross_boundary_spectrum)
  3 — Assign-and-split orchestration (_assign_and_split_regimes)
  4 — Per-regime trimming (_trim_per_regime)
  5 — End-to-end: mixed UV + optical (the original bug, now a regression test)
  6 — End-to-end: wide-coverage splitting through generate_spectra_json
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import numpy as np
import pytest
from generators.spectra import (
    _assign_and_split_regimes,
    _assign_spectra_regime,
    _split_cross_boundary_spectrum,
    _trim_per_regime,
    generate_spectra_json,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stage1_rec(
    wl_min: float,
    wl_max: float,
    *,
    dp_id: str = "dp-1",
    n_points: int = 50,
    flux_val: float = 1.0,
    instrument: str = "TestInstrument",
    nova_id: str = "nova-test",
) -> dict[str, Any]:
    """Build a minimal stage-1 parsed record for unit tests."""
    step = (wl_max - wl_min) / max(n_points - 1, 1)
    wavelengths = [wl_min + i * step for i in range(n_points)]
    fluxes = [flux_val] * n_points
    return {
        "wavelengths": wavelengths,
        "fluxes": fluxes,
        "product": {
            "data_product_id": dp_id,
            "instrument": instrument,
            "observation_date_mjd": Decimal("59000"),
            "telescope": "TestTelescope",
            "provider": "TestProvider",
            "flux_unit": "erg/s/cm2/A",
            "PK": nova_id,
            "SK": f"PRODUCT#SPECTRA#{dp_id}",
            "validation_status": "VALID",
        },
        "nova_id": nova_id,
    }


def _make_csv(wl_min: float, wl_max: float, n: int = 50) -> str:
    """Generate a web-ready CSV string with realistic flux (DER_SNR >> 5)."""
    rng = np.random.default_rng(42)
    step = (wl_max - wl_min) / max(n - 1, 1)
    rows = ["wavelength_nm,flux"]
    for i in range(n):
        rows.append(f"{wl_min + i * step:.4f},{1000.0 + rng.normal(0, 5):.6f}")
    return "\n".join(rows)


def _make_product(
    dp_id: str,
    *,
    mjd: str = "59000",
    instrument: str = "TestInstrument",
    telescope: str = "TestTelescope",
    provider: str = "TestProvider",
) -> dict[str, Any]:
    """Build a DataProduct dict for end-to-end tests."""
    return {
        "data_product_id": dp_id,
        "observation_date_mjd": Decimal(mjd),
        "instrument": instrument,
        "telescope": telescope,
        "provider": provider,
        "flux_unit": "erg/s/cm2/A",
        "PK": "nova-test",
        "SK": f"PRODUCT#SPECTRA#{dp_id}",
        "validation_status": "VALID",
        "snr": Decimal("10.0"),
    }


class _FakeBody:
    def __init__(self, content: str) -> None:
        self._content = content

    def read(self) -> bytes:
        return self._content.encode()


class _FakeS3ByDpId:
    """S3 mock that returns pre-loaded CSV bodies keyed by data_product_id."""

    def __init__(self, csvs: dict[str, str]) -> None:
        self._csvs = csvs

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        dp_id = Key.split("/")[-2]
        return {"Body": _FakeBody(self._csvs[dp_id])}


class _FakeTable:
    """DynamoDB Table mock returning a fixed list of items."""

    def __init__(self, items: list[dict[str, Any]]) -> None:
        self._items = items

    def query(self, **kwargs: Any) -> dict[str, Any]:
        return {"Items": self._items}


def _run_generator(
    products: list[dict[str, Any]],
    csvs: dict[str, str],
    *,
    outburst_mjd: float = 58000.0,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run generate_spectra_json and return (artifact, nova_context)."""
    ctx: dict[str, Any] = {
        "outburst_mjd": outburst_mjd,
        "outburst_mjd_is_estimated": False,
    }
    artifact = generate_spectra_json(
        "nova-test",
        _FakeTable(products),
        _FakeS3ByDpId(csvs),
        "test-bucket",
        ctx,
    )
    return artifact, ctx


# ---------------------------------------------------------------------------
# Group 1 — Regime assignment (_assign_spectra_regime)
# ---------------------------------------------------------------------------


class TestAssignSpectraRegime:
    """Midpoint-based regime assignment (ADR-034 Decision 2, ADR-035 Decision 1)."""

    def test_xray_regime(self) -> None:
        """Spectrum entirely below Lyman limit → xray."""
        assert _assign_spectra_regime(0.5, 15.0) == "xray"

    def test_uv_regime(self) -> None:
        """Spectrum in UV window → uv."""
        assert _assign_spectra_regime(115.0, 170.0) == "uv"

    def test_optical_regime(self) -> None:
        """Typical ground-based optical spectrum → optical."""
        assert _assign_spectra_regime(400.0, 900.0) == "optical"

    def test_nir_regime(self) -> None:
        """J/H/K band spectrum → nir."""
        assert _assign_spectra_regime(1000.0, 2500.0) == "nir"

    def test_mir_regime(self) -> None:
        """Mid-IR spectrum → mir."""
        assert _assign_spectra_regime(5000.0, 15000.0) == "mir"

    def test_uv_optical_boundary_midpoint_optical(self) -> None:
        """STIS G430L (290–570 nm): midpoint 430 nm → optical, not uv."""
        assert _assign_spectra_regime(290.0, 570.0) == "optical"

    def test_lyman_limit_boundary(self) -> None:
        """Midpoint exactly at 91 nm → uv (not xray, since 91 is the lower bound of uv)."""
        # Spectrum from 82–100 nm: midpoint = 91 nm → uv
        assert _assign_spectra_regime(82.0, 100.0) == "uv"

    def test_optical_nir_boundary_midpoint(self) -> None:
        """Spectrum straddling 1000 nm but midpoint above → nir."""
        assert _assign_spectra_regime(800.0, 1400.0) == "nir"


# ---------------------------------------------------------------------------
# Group 2 — Cross-boundary splitting (_split_cross_boundary_spectrum)
# ---------------------------------------------------------------------------


class TestSplitCrossBoundarySpectrum:
    """ADR-035 Decision 2: splitting wide-coverage spectra at regime boundaries."""

    def test_no_split_entirely_within_optical(self) -> None:
        """Spectrum fully within optical range → no split, one record returned."""
        rec = _make_stage1_rec(400.0, 900.0)
        result = _split_cross_boundary_spectrum(rec)
        assert len(result) == 1
        assert "_split_suffix" not in result[0]

    def test_xshooter_splits_at_1000nm(self) -> None:
        """X-Shooter 350–2500 nm: split at 1000 nm into optical + NIR."""
        rec = _make_stage1_rec(350.0, 2500.0, n_points=200)
        result = _split_cross_boundary_spectrum(rec)

        assert len(result) == 2

        optical_frag = [f for f in result if f["_regime"] == "optical"]
        nir_frag = [f for f in result if f["_regime"] == "nir"]
        assert len(optical_frag) == 1
        assert len(nir_frag) == 1

        # Optical fragment: 350 to <1000
        assert optical_frag[0]["wavelengths"][0] == pytest.approx(350.0, abs=1.0)
        assert optical_frag[0]["wavelengths"][-1] < 1000.0

        # NIR fragment: >=1000 to 2500
        assert nir_frag[0]["wavelengths"][0] >= 1000.0
        assert nir_frag[0]["wavelengths"][-1] == pytest.approx(2500.0, abs=1.0)

    def test_stis_g430l_no_split_below_threshold(self) -> None:
        """STIS G430L (290–570 nm): 30 nm UV portion < 45 nm min → no split."""
        rec = _make_stage1_rec(290.0, 570.0, n_points=100)
        result = _split_cross_boundary_spectrum(rec)
        assert len(result) == 1

    def test_boundary_point_goes_to_redder_regime(self) -> None:
        """A data point at exactly 1000.0 nm goes to NIR (the redder regime)."""
        # Create spectrum with a point exactly at the boundary.
        wavelengths = [float(w) for w in range(350, 2501, 10)]
        fluxes = [1.0] * len(wavelengths)
        rec: dict[str, Any] = {
            "wavelengths": wavelengths,
            "fluxes": fluxes,
            "product": {
                "data_product_id": "dp-boundary",
                "instrument": "XSHOOTER",
                "observation_date_mjd": Decimal("59000"),
                "telescope": "VLT",
                "provider": "ESO",
                "flux_unit": "erg/s/cm2/A",
                "PK": "nova-test",
                "SK": "PRODUCT#SPECTRA#dp-boundary",
                "validation_status": "VALID",
            },
            "nova_id": "nova-test",
        }

        result = _split_cross_boundary_spectrum(rec)
        assert len(result) == 2

        optical_frag = [f for f in result if f["_regime"] == "optical"][0]
        nir_frag = [f for f in result if f["_regime"] == "nir"][0]

        # 1000.0 must NOT be in the optical fragment
        assert 1000.0 not in optical_frag["wavelengths"]
        # 1000.0 must BE in the NIR fragment
        assert 1000.0 in nir_frag["wavelengths"]

    def test_fractional_threshold_prevents_split(self) -> None:
        """Minor side is ≥45 nm absolute but <15% fractional → no split."""
        # Spectrum 280–2500 nm. UV side = 320 - 280 = 40 nm... wait, that's <45.
        # Let's use: 250–2500 nm. UV side = 320 - 250 = 70 nm. Total = 2250.
        # Fraction = 70/2250 = 3.1%. Below 15% → no split at 320,
        # even though 70 nm > 45 nm.
        # But it WILL split at 1000 nm: minor = 250 nm optical / 1500 nm NIR.
        # Actually minor side for 1000 boundary: left = 1000-250=750, right=2500-1000=1500
        # minor = 750, fraction = 750/2250 = 33% → split at 1000.
        # So this tests that we DON'T split at 320 but DO split at 1000.
        rec = _make_stage1_rec(250.0, 2500.0, n_points=200)
        result = _split_cross_boundary_spectrum(rec)

        regimes = [f["_regime"] for f in result]
        # Should NOT have a UV fragment (70 nm > 45 but 3.1% < 15%)
        assert "uv" not in regimes
        # Should have optical and NIR from the 1000 nm split
        assert "optical" in regimes
        assert "nir" in regimes

    def test_split_preserves_product_metadata(self) -> None:
        """All fragments inherit the parent's product dict."""
        rec = _make_stage1_rec(350.0, 2500.0, dp_id="dp-meta", n_points=200)
        result = _split_cross_boundary_spectrum(rec)

        for frag in result:
            assert frag["product"]["data_product_id"] == "dp-meta"
            assert frag["product"]["instrument"] == "TestInstrument"
            assert frag["nova_id"] == "nova-test"

    def test_empty_wavelengths_returns_single_record(self) -> None:
        """Edge case: empty wavelengths → returns original record."""
        rec = _make_stage1_rec(400.0, 900.0)
        rec["wavelengths"] = []
        rec["fluxes"] = []
        result = _split_cross_boundary_spectrum(rec)
        assert len(result) == 1

    def test_uv_optical_split_eligible(self) -> None:
        """Spectrum from 200–600 nm: UV portion = 120 nm (30%) → split at 320."""
        rec = _make_stage1_rec(200.0, 600.0, n_points=200)
        result = _split_cross_boundary_spectrum(rec)

        assert len(result) == 2
        regimes = {f["_regime"] for f in result}
        assert regimes == {"uv", "optical"}


# ---------------------------------------------------------------------------
# Group 3 — Assign-and-split orchestration (_assign_and_split_regimes)
# ---------------------------------------------------------------------------


class TestAssignAndSplitRegimes:
    """ADR-035 Decision 4 steps 5–6 orchestration."""

    def test_all_optical_no_splits(self) -> None:
        """Homogeneous optical population → all assigned optical, no splits."""
        recs = [
            _make_stage1_rec(400.0, 900.0, dp_id="dp-1"),
            _make_stage1_rec(380.0, 850.0, dp_id="dp-2"),
            _make_stage1_rec(420.0, 920.0, dp_id="dp-3"),
        ]
        result = _assign_and_split_regimes(recs)

        assert len(result) == 3
        for rec in result:
            assert rec["_regime"] == "optical"
            assert "_split_suffix" not in rec

    def test_mixed_uv_optical_classified_separately(self) -> None:
        """UV and optical spectra get different regime labels."""
        recs = [
            _make_stage1_rec(115.0, 170.0, dp_id="dp-uv"),
            _make_stage1_rec(400.0, 900.0, dp_id="dp-opt"),
        ]
        result = _assign_and_split_regimes(recs)

        assert len(result) == 2
        regimes = {r["product"]["data_product_id"]: r["_regime"] for r in result}
        assert regimes["dp-uv"] == "uv"
        assert regimes["dp-opt"] == "optical"

    def test_wide_spectrum_split_increases_count(self) -> None:
        """One wide spectrum + one narrow → split produces 3 total records."""
        recs = [
            _make_stage1_rec(350.0, 2500.0, dp_id="dp-wide", n_points=200),
            _make_stage1_rec(400.0, 900.0, dp_id="dp-narrow"),
        ]
        result = _assign_and_split_regimes(recs)

        # dp-wide splits into optical + NIR = 2; dp-narrow stays = 1; total = 3
        assert len(result) == 3
        dp_ids = [r["product"]["data_product_id"] for r in result]
        assert dp_ids.count("dp-wide") == 2
        assert dp_ids.count("dp-narrow") == 1

    def test_empty_wavelengths_skipped(self) -> None:
        """Records with empty wavelengths are dropped."""
        recs = [
            _make_stage1_rec(400.0, 900.0, dp_id="dp-good"),
            _make_stage1_rec(400.0, 900.0, dp_id="dp-empty"),
        ]
        recs[1]["wavelengths"] = []
        recs[1]["fluxes"] = []

        result = _assign_and_split_regimes(recs)
        assert len(result) == 1
        assert result[0]["product"]["data_product_id"] == "dp-good"


# ---------------------------------------------------------------------------
# Group 4 — Per-regime trimming (_trim_per_regime)
# ---------------------------------------------------------------------------


class TestTrimPerRegime:
    """ADR-035 Decision 3: per-regime median display range computation."""

    def test_uv_not_trimmed_by_optical_median(self) -> None:
        """Core regression test: UV spectra survive when mixed with optical."""
        uv_recs = [
            _make_stage1_rec(115.0, 170.0, dp_id="dp-uv-1"),
            _make_stage1_rec(115.0, 310.0, dp_id="dp-uv-2"),
            _make_stage1_rec(160.0, 320.0, dp_id="dp-uv-3"),
        ]
        opt_recs = [
            _make_stage1_rec(350.0, 900.0, dp_id="dp-opt-1"),
            _make_stage1_rec(360.0, 910.0, dp_id="dp-opt-2"),
            _make_stage1_rec(340.0, 890.0, dp_id="dp-opt-3"),
        ]

        for rec in uv_recs:
            rec["_regime"] = "uv"
        for rec in opt_recs:
            rec["_regime"] = "optical"

        all_recs = uv_recs + opt_recs
        result = _trim_per_regime(all_recs, "nova-test")

        # All 6 spectra should survive — UV spectra are NOT trimmed by
        # the optical median.
        assert len(result) == 6

        # UV spectra should retain their full wavelength coverage.
        uv_results = [r for r in result if r["_regime"] == "uv"]
        for rec in uv_results:
            assert rec["wavelengths"][0] < 170.0, "UV spectrum blue edge was incorrectly trimmed"

    def test_single_spectrum_regime_no_trim(self) -> None:
        """Regime with only one spectrum → no trimming applied."""
        rec = _make_stage1_rec(115.0, 170.0, dp_id="dp-solo-uv")
        rec["_regime"] = "uv"

        result = _trim_per_regime([rec], "nova-test")
        assert len(result) == 1
        # Wavelengths unchanged
        assert result[0]["wavelengths"][0] == pytest.approx(115.0, abs=0.1)
        assert result[0]["wavelengths"][-1] == pytest.approx(170.0, abs=0.1)

    def test_intra_regime_outlier_still_trimmed(self) -> None:
        """An outlier WITHIN a regime is still trimmed by the regime's median."""
        recs = [
            _make_stage1_rec(400.0, 900.0, dp_id="dp-1"),
            _make_stage1_rec(410.0, 910.0, dp_id="dp-2"),
            _make_stage1_rec(390.0, 890.0, dp_id="dp-3"),
            _make_stage1_rec(200.0, 900.0, dp_id="dp-blue-outlier", n_points=200),
        ]
        for rec in recs:
            rec["_regime"] = "optical"

        result = _trim_per_regime(recs, "nova-test")

        # The blue outlier should have its blue side trimmed to near the
        # median min (~400 nm), not kept at 200 nm.
        outlier = [r for r in result if r["product"]["data_product_id"] == "dp-blue-outlier"]
        assert len(outlier) == 1
        assert outlier[0]["wavelengths"][0] >= 350.0

    def test_independent_regime_medians(self) -> None:
        """UV and optical groups compute independent medians."""
        uv_recs = [
            _make_stage1_rec(115.0, 170.0, dp_id="dp-uv-1"),
            _make_stage1_rec(120.0, 175.0, dp_id="dp-uv-2"),
            _make_stage1_rec(130.0, 310.0, dp_id="dp-uv-outlier", n_points=100),
        ]
        opt_recs = [
            _make_stage1_rec(400.0, 900.0, dp_id="dp-opt-1"),
            _make_stage1_rec(410.0, 910.0, dp_id="dp-opt-2"),
        ]

        for rec in uv_recs:
            rec["_regime"] = "uv"
        for rec in opt_recs:
            rec["_regime"] = "optical"

        result = _trim_per_regime(uv_recs + opt_recs, "nova-test")

        # UV outlier (130–310) should be trimmed to the UV median red edge
        # (~175), not the optical median red edge (~905).
        uv_outlier = [r for r in result if r["product"]["data_product_id"] == "dp-uv-outlier"]
        assert len(uv_outlier) == 1
        assert uv_outlier[0]["wavelengths"][-1] < 250.0


# ---------------------------------------------------------------------------
# Group 5 — End-to-end: mixed UV + optical (regression test for the bug)
# ---------------------------------------------------------------------------


class TestMixedUvOpticalEndToEnd:
    """End-to-end tests through generate_spectra_json with mixed regimes."""

    def test_uv_spectra_survive_with_optical_majority(self) -> None:
        """THE REGRESSION TEST: UV spectra are not destroyed by optical median.

        Before ADR-035, UV spectra covering 115–310 nm were trimmed to
        just 305–307 nm by the global median. After ADR-035, per-regime
        trimming preserves the full UV wavelength range.
        """
        csvs = {
            "dp-uv-1": _make_csv(115.0, 170.0),
            "dp-uv-2": _make_csv(160.0, 310.0),
            "dp-opt-1": _make_csv(350.0, 900.0),
            "dp-opt-2": _make_csv(360.0, 910.0),
            "dp-opt-3": _make_csv(340.0, 890.0),
            "dp-opt-4": _make_csv(370.0, 920.0),
            "dp-opt-5": _make_csv(380.0, 900.0),
        }
        products = [
            _make_product(
                "dp-uv-1", mjd="59000", instrument="STIS", telescope="HST", provider="MAST"
            ),
            _make_product(
                "dp-uv-2", mjd="59001", instrument="STIS", telescope="HST", provider="MAST"
            ),
            _make_product("dp-opt-1", mjd="59002"),
            _make_product("dp-opt-2", mjd="59003"),
            _make_product("dp-opt-3", mjd="59004"),
            _make_product("dp-opt-4", mjd="59005"),
            _make_product("dp-opt-5", mjd="59006"),
        ]

        artifact, _ctx = _run_generator(products, csvs)

        # Both regimes should be present.
        regime_ids = {r["id"] for r in artifact["regimes"]}
        assert "uv" in regime_ids
        assert "optical" in regime_ids

        # UV spectra should exist and have meaningful wavelength coverage.
        uv_spectra = [s for s in artifact["spectra"] if s["regime"] == "uv"]
        assert len(uv_spectra) >= 1

        for sp in uv_spectra:
            wl_range = sp["wavelength_max"] - sp["wavelength_min"]
            assert wl_range > 10.0, (
                f"UV spectrum {sp['spectrum_id']} has only {wl_range:.1f} nm "
                f"coverage — likely trimmed by optical median"
            )

        # Optical spectra should also be present and unaffected.
        opt_spectra = [s for s in artifact["spectra"] if s["regime"] == "optical"]
        assert len(opt_spectra) >= 1

    def test_single_regime_no_tab_bar_data(self) -> None:
        """All-optical nova → single regime entry, no UV/NIR."""
        csvs = {
            "dp-1": _make_csv(400.0, 900.0),
            "dp-2": _make_csv(410.0, 920.0),
        }
        products = [
            _make_product("dp-1", mjd="59000"),
            _make_product("dp-2", mjd="59001"),
        ]

        artifact, _ctx = _run_generator(products, csvs)

        assert len(artifact["regimes"]) == 1
        assert artifact["regimes"][0]["id"] == "optical"

    def test_schema_version_is_1_4(self) -> None:
        """Artifact carries schema version 1.4 per ADR-035."""
        csvs = {"dp-1": _make_csv(400.0, 900.0)}
        products = [_make_product("dp-1")]

        artifact, _ctx = _run_generator(products, csvs)
        assert artifact["schema_version"] == "1.4"


# ---------------------------------------------------------------------------
# Group 6 — End-to-end: wide-coverage splitting
# ---------------------------------------------------------------------------


class TestWideCoverageSplittingEndToEnd:
    """End-to-end tests for cross-boundary spectrum splitting."""

    def test_xshooter_split_produces_two_regimes(self) -> None:
        """X-Shooter-like spectrum (350–2500 nm) splits into optical + NIR."""
        csvs = {
            "dp-xsh": _make_csv(350.0, 2500.0, n=200),
            "dp-opt": _make_csv(400.0, 900.0),
        }
        products = [
            _make_product("dp-xsh", mjd="59000", instrument="XSHOOTER"),
            _make_product("dp-opt", mjd="59001"),
        ]

        artifact, _ctx = _run_generator(products, csvs)

        regime_ids = {r["id"] for r in artifact["regimes"]}
        assert "optical" in regime_ids
        assert "nir" in regime_ids

        # dp-xsh should appear as two spectra with :: suffixes.
        xsh_spectra = [s for s in artifact["spectra"] if s["spectrum_id"].startswith("dp-xsh")]
        assert len(xsh_spectra) == 2

        xsh_ids = {s["spectrum_id"] for s in xsh_spectra}
        assert "dp-xsh::optical" in xsh_ids
        assert "dp-xsh::nir" in xsh_ids

    def test_split_spectrum_id_format(self) -> None:
        """Split fragments use {dp_id}::{regime} as spectrum_id."""
        csvs = {"dp-wide": _make_csv(350.0, 2500.0, n=200)}
        products = [_make_product("dp-wide", mjd="59000")]

        artifact, _ctx = _run_generator(products, csvs)

        for sp in artifact["spectra"]:
            assert "::" in sp["spectrum_id"]
            parts = sp["spectrum_id"].split("::")
            assert parts[0] == "dp-wide"
            assert parts[1] in ("optical", "nir")

    def test_non_split_spectrum_no_suffix(self) -> None:
        """Normal optical spectrum has plain data_product_id as spectrum_id."""
        csvs = {"dp-plain": _make_csv(400.0, 900.0)}
        products = [_make_product("dp-plain")]

        artifact, _ctx = _run_generator(products, csvs)

        assert len(artifact["spectra"]) == 1
        assert artifact["spectra"][0]["spectrum_id"] == "dp-plain"
        assert "::" not in artifact["spectra"][0]["spectrum_id"]

    def test_stis_g430l_not_split(self) -> None:
        """STIS G430L (290–570 nm): minor UV side too small → no split."""
        csvs = {"dp-g430l": _make_csv(290.0, 570.0, n=100)}
        products = [_make_product("dp-g430l", mjd="59000", instrument="STIS")]

        artifact, _ctx = _run_generator(products, csvs)

        # Should be a single optical spectrum, not split.
        assert len(artifact["spectra"]) == 1
        assert artifact["spectra"][0]["regime"] == "optical"
        assert "::" not in artifact["spectra"][0]["spectrum_id"]

    def test_observations_count_unchanged_by_split(self) -> None:
        """Splitting doesn't inflate the observations list or spectra_count."""
        csvs = {
            "dp-xsh": _make_csv(350.0, 2500.0, n=200),
            "dp-opt": _make_csv(400.0, 900.0),
        }
        products = [
            _make_product("dp-xsh", mjd="59000"),
            _make_product("dp-opt", mjd="59001"),
        ]

        artifact, ctx = _run_generator(products, csvs)

        # spectra_count reflects raw DataProducts, not display spectra.
        assert ctx["spectra_count"] == 2
        # observations list has one entry per raw DataProduct.
        assert len(artifact["observations"]) == 2
        # But display spectra may be >2 due to splitting.
        assert len(artifact["spectra"]) >= 2
