"""Unit tests for generators/photometry.py.

Uses moto to mock DynamoDB for the dedicated photometry table.

Covers:
  Empty data:
  - No rows → empty arrays, count = 0, raw_items stashed

  Regime handling:
  - UV/NIR/MIR mapped to optical
  - Unrecognised regime excluded
  - Multiple regimes produce separate regime records

  Band resolution:
  - Registry hit → display label from band_name
  - Registry miss → raw band_id as fallback

  Upper limit suppression:
  - Non-constraining limit (brighter than brightest detection) dropped
  - Constraining limit (fainter than brightest detection) kept
  - Band with no detections → all limits kept

  Subsampling:
  - Under cap → all rows retained
  - Over cap → subsampled to cap

  Value routing:
  - Optical → magnitude fields populated, others null
  - Radio → flux_density populated
  - X-ray → count_rate populated
  - Gamma → photon_flux populated

  DPO computation:
  - Correct days_since_outburst
  - Null when outburst_mjd is None

  Sort and schema:
  - Observations sorted by regime order then epoch ascending
  - schema_version "1.1", outburst fields, generated_at

  Context:
  - photometry_count, photometry_raw_items, observations, bands
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

import boto3
import pytest
from generators.photometry import generate_photometry_json
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Photometry-Test"
_MAIN_TABLE_NAME = "NovaCat-Main-Test"
_REGION = "us-east-1"
_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"

# Minimal band registry for tests.
_REGISTRY: dict[str, Any] = {
    "Generic_V": {"band_name": "V", "lambda_eff": 5510.0},
    "Generic_B": {"band_name": "B", "lambda_eff": 4450.0},
    "Generic_R": {"band_name": "R", "lambda_eff": 6580.0},
    "Generic_I": {"band_name": "I", "lambda_eff": 8060.0},
    "Generic_U": {"band_name": "U", "lambda_eff": 3600.0},
    "2MASS_J": {"band_name": "J", "lambda_eff": 12350.0},
    "2MASS_H": {"band_name": "H", "lambda_eff": 16620.0},
    "2MASS_K": {"band_name": "K", "lambda_eff": 21590.0},
    "Swift_UVOT_UVW2": {"band_name": "UVW2", "lambda_eff": 2030.0},
    "VLA_5GHz": {"band_name": "5 GHz", "lambda_eff": None},
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture()
def _mock_tables(_aws_env: None) -> Any:
    """Create mocked DDB tables inside a shared mock_aws context."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        phot = dynamodb.create_table(
            TableName=_TABLE_NAME,
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
        main = dynamodb.create_table(
            TableName=_MAIN_TABLE_NAME,
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
        yield phot, main


@pytest.fixture()
def phot_table(_mock_tables: Any) -> Any:
    """Photometry DDB table from shared mock context."""
    return _mock_tables[0]


@pytest.fixture()
def main_table(_mock_tables: Any) -> Any:
    """Main NovaCat DDB table from shared mock context."""
    return _mock_tables[1]


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_phot_row(
    table: Any,
    nova_id: str,
    row_id: str,
    *,
    time_mjd: float = 51544.0,
    band_id: str = "Generic_V",
    regime: str = "optical",
    band_name: str | None = None,
    magnitude: float | None = 12.0,
    mag_err: float | None = 0.02,
    flux_density: float | None = None,
    flux_density_err: float | None = None,
    is_upper_limit: bool = False,
    telescope: str | None = "CTIO 1.3m",
    instrument: str | None = "ANDICAM",
    observer: str | None = None,
    orig_catalog: str | None = "SMARTS",
    bibcode: str | None = None,
) -> None:
    """Write a PhotometryRow item to the dedicated table."""
    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": f"PHOT#{row_id}",
        "row_id": row_id,
        "time_mjd": Decimal(str(time_mjd)),
        "band_id": band_id,
        "regime": regime,
        "is_upper_limit": is_upper_limit,
    }
    if magnitude is not None:
        item["magnitude"] = Decimal(str(magnitude))
    if mag_err is not None:
        item["mag_err"] = Decimal(str(mag_err))
    if flux_density is not None:
        item["flux_density"] = Decimal(str(flux_density))
    if flux_density_err is not None:
        item["flux_density_err"] = Decimal(str(flux_density_err))
    if telescope is not None:
        item["telescope"] = telescope
    if instrument is not None:
        item["instrument"] = instrument
    if observer is not None:
        item["observer"] = observer
    if orig_catalog is not None:
        item["orig_catalog"] = orig_catalog
    if bibcode is not None:
        item["bibcode"] = bibcode
    if band_name is not None:
        item["band_name"] = band_name
    table.put_item(Item=item)


def _base_context(
    *,
    outburst_mjd: float | None = 51540.0,
    is_estimated: bool = False,
) -> dict[str, Any]:
    return {
        "outburst_mjd": outburst_mjd,
        "outburst_mjd_is_estimated": is_estimated,
    }


# ---------------------------------------------------------------------------
# Empty data
# ---------------------------------------------------------------------------


class TestEmptyData:
    def test_no_rows_returns_empty_artifact(self, phot_table: Any, main_table: Any) -> None:
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["observations"] == []
        assert artifact["bands"] == []
        assert artifact["regimes"] == []
        assert ctx["photometry_count"] == 0

    def test_raw_items_stashed_even_when_empty(self, phot_table: Any, main_table: Any) -> None:
        ctx = _base_context()
        generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert ctx["photometry_raw_items"] == []


# ---------------------------------------------------------------------------
# Regime handling
# ---------------------------------------------------------------------------


class TestRegimeMapping:
    def test_uv_mapped_to_optical(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="uv",
            band_id="Swift_UVOT_UVW2",
            magnitude=15.0,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["observations"][0]["regime"] == "optical"

    def test_unrecognised_regime_excluded(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="submillimeter",
            magnitude=5.0,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["observations"] == []
        assert ctx["photometry_count"] == 0

    def test_multiple_regimes_produce_separate_regime_records(
        self, phot_table: Any, main_table: Any
    ) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="optical",
            magnitude=12.0,
        )
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r2",
            regime="radio",
            band_id="VLA_5GHz",
            magnitude=None,
            flux_density=1.5,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        regime_ids = [r["id"] for r in artifact["regimes"]]
        assert "optical" in regime_ids
        assert "radio" in regime_ids


# ---------------------------------------------------------------------------
# Band resolution
# ---------------------------------------------------------------------------


class TestBandResolution:
    def test_registry_hit_uses_band_name(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            band_id="Generic_V",
            band_name="V",
            magnitude=12.0,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["observations"][0]["band"] == "V"

    def test_registry_miss_uses_raw_band_id(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            band_id="UNKNOWN_FILTER_X",
            magnitude=12.0,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["observations"][0]["band"] == "UNKNOWN_FILTER_X"

    def test_wavelength_eff_nm_from_registry(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            band_id="Generic_V",
            magnitude=12.0,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        v_band = [b for b in artifact["bands"] if b["band"] == "V"]
        assert len(v_band) == 1
        assert v_band[0]["wavelength_eff_nm"] == pytest.approx(551.0)


# ---------------------------------------------------------------------------
# Upper limit suppression
# ---------------------------------------------------------------------------


class TestUpperLimitSuppression:
    def test_non_constraining_limit_dropped(self, phot_table: Any, main_table: Any) -> None:
        """Upper limit brighter than brightest detection is dropped."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "det1",
            magnitude=12.0,
            is_upper_limit=False,
        )
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "ul1",
            magnitude=10.0,
            is_upper_limit=True,  # brighter → drop
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert len(artifact["observations"]) == 1
        assert artifact["observations"][0]["is_upper_limit"] is False

    def test_constraining_limit_kept(self, phot_table: Any, main_table: Any) -> None:
        """Upper limit fainter than brightest detection is kept."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "det1",
            magnitude=12.0,
            is_upper_limit=False,
        )
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "ul1",
            magnitude=15.0,
            is_upper_limit=True,  # fainter → keep
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert len(artifact["observations"]) == 2

    def test_no_detections_keeps_all_limits(self, phot_table: Any, main_table: Any) -> None:
        """Band with only upper limits → all kept."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "ul1",
            magnitude=15.0,
            is_upper_limit=True,
        )
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "ul2",
            magnitude=14.0,
            is_upper_limit=True,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert len(artifact["observations"]) == 2


# ---------------------------------------------------------------------------
# Subsampling
# ---------------------------------------------------------------------------


class TestSubsampling:
    def test_under_cap_retains_all(self, phot_table: Any, main_table: Any) -> None:
        """Fewer than 500 rows → no subsampling."""
        for i in range(10):
            _seed_phot_row(
                phot_table,
                _NOVA_ID,
                f"r{i}",
                time_mjd=51544.0 + i,
                magnitude=12.0 + i * 0.1,
            )
        ctx = _base_context()
        generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert ctx["photometry_count"] == 10

    def test_over_cap_subsampled(self, phot_table: Any, main_table: Any) -> None:
        """More than 500 rows → subsampled to cap."""
        for i in range(600):
            _seed_phot_row(
                phot_table,
                _NOVA_ID,
                f"r{i}",
                time_mjd=51544.0 + i * 0.1,
                magnitude=12.0 + (i % 20) * 0.1,
            )
        ctx = _base_context()
        generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert ctx["photometry_count"] <= 500


# ---------------------------------------------------------------------------
# Value routing
# ---------------------------------------------------------------------------


class TestValueRouting:
    def test_optical_routes_to_magnitude(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="optical",
            magnitude=12.34,
            mag_err=0.02,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        obs = artifact["observations"][0]
        assert obs["magnitude"] == pytest.approx(12.34)
        assert obs["magnitude_error"] == pytest.approx(0.02)
        assert obs["flux_density"] is None
        assert obs["count_rate"] is None
        assert obs["photon_flux"] is None

    def test_radio_routes_to_flux_density(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="radio",
            band_id="VLA_5GHz",
            magnitude=None,
            flux_density=1.5,
            flux_density_err=0.1,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        obs = artifact["observations"][0]
        assert obs["flux_density"] == pytest.approx(1.5)
        assert obs["magnitude"] is None

    def test_xray_routes_to_count_rate(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="xray",
            band_id="Swift_XRT",
            magnitude=None,
            flux_density=0.5,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        obs = artifact["observations"][0]
        assert obs["count_rate"] == pytest.approx(0.5)
        assert obs["magnitude"] is None

    def test_gamma_routes_to_photon_flux(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="gamma",
            band_id="Fermi_LAT",
            magnitude=None,
            flux_density=1.2e-7,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        obs = artifact["observations"][0]
        assert obs["photon_flux"] == pytest.approx(1.2e-7)


# ---------------------------------------------------------------------------
# DPO computation
# ---------------------------------------------------------------------------


class TestDPO:
    def test_days_since_outburst_computed(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            time_mjd=51544.0,
            magnitude=12.0,
        )
        ctx = _base_context(outburst_mjd=51540.0)
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["observations"][0]["days_since_outburst"] == pytest.approx(4.0)

    def test_null_outburst_gives_null_dpo(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            time_mjd=51544.0,
            magnitude=12.0,
        )
        ctx = _base_context(outburst_mjd=None)
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["observations"][0]["days_since_outburst"] is None


# ---------------------------------------------------------------------------
# Sort order
# ---------------------------------------------------------------------------


class TestSortOrder:
    def test_regime_order_then_epoch(self, phot_table: Any, main_table: Any) -> None:
        """Observations sorted: optical < xray < radio, then epoch asc."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "radio1",
            regime="radio",
            band_id="VLA_5GHz",
            magnitude=None,
            flux_density=1.0,
            time_mjd=51545.0,
        )
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "opt1",
            regime="optical",
            magnitude=12.0,
            time_mjd=51546.0,
        )
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "opt2",
            regime="optical",
            magnitude=12.5,
            time_mjd=51544.0,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        regimes = [o["regime"] for o in artifact["observations"]]
        # Optical first (both), then radio.
        assert regimes == ["optical", "optical", "radio"]
        # Within optical, epoch ascending.
        optical_epochs = [
            o["epoch_mjd"] for o in artifact["observations"] if o["regime"] == "optical"
        ]
        assert optical_epochs == sorted(optical_epochs)


# ---------------------------------------------------------------------------
# Schema and context
# ---------------------------------------------------------------------------


class TestSchemaAndContext:
    def test_schema_version(self, phot_table: Any, main_table: Any) -> None:
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["schema_version"] == "1.1"

    def test_outburst_fields(self, phot_table: Any, main_table: Any) -> None:
        ctx = _base_context(outburst_mjd=51540.0, is_estimated=True)
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert artifact["outburst_mjd"] == pytest.approx(51540.0)
        assert artifact["outburst_mjd_is_estimated"] is True

    def test_generated_at_format(self, phot_table: Any, main_table: Any) -> None:
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        assert re.match(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
            artifact["generated_at"],
        )

    def test_photometry_count_in_context(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(phot_table, _NOVA_ID, "r1", magnitude=12.0)
        _seed_phot_row(phot_table, _NOVA_ID, "r2", magnitude=13.0)
        ctx = _base_context()
        generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert ctx["photometry_count"] == 2

    def test_raw_items_stashed_for_bundle(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(phot_table, _NOVA_ID, "r1", magnitude=12.0)
        ctx = _base_context()
        generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert len(ctx["photometry_raw_items"]) == 1

    def test_observations_and_bands_in_context(self, phot_table: Any, main_table: Any) -> None:
        _seed_phot_row(phot_table, _NOVA_ID, "r1", magnitude=12.0)
        ctx = _base_context()
        generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert len(ctx["photometry_observations"]) == 1
        assert len(ctx["photometry_bands"]) == 1

    def test_band_vertical_offset_zero_placeholder(self, phot_table: Any, main_table: Any) -> None:
        """Placeholder offsets are all 0.0."""
        _seed_phot_row(phot_table, _NOVA_ID, "r1", band_id="Generic_V", magnitude=12.0)
        _seed_phot_row(phot_table, _NOVA_ID, "r2", band_id="Generic_B", magnitude=13.0)
        ctx = _base_context()
        artifact = generate_photometry_json(
            _NOVA_ID,
            phot_table,
            main_table,
            _REGISTRY,
            ctx,
        )
        for band in artifact["bands"]:
            assert band["vertical_offset"] == 0.0


# ---------------------------------------------------------------------------
# Auto-flag large photometry errors as upper limits (P2)
# ---------------------------------------------------------------------------


class TestAutoFlagLargeErrors:
    """Optical points with mag_err > 1.0 are auto-flagged as upper limits."""

    def test_large_error_auto_flagged(self, phot_table: Any, main_table: Any) -> None:
        """mag_err=1.5, is_upper_limit=False → treated as upper limit in output."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="optical",
            magnitude=16.0,
            mag_err=1.5,
            is_upper_limit=False,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert len(artifact["observations"]) == 1
        assert artifact["observations"][0]["is_upper_limit"] is True

    def test_below_threshold_not_flagged(self, phot_table: Any, main_table: Any) -> None:
        """mag_err=0.8 → remains a detection."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="optical",
            magnitude=12.0,
            mag_err=0.8,
            is_upper_limit=False,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert len(artifact["observations"]) == 1
        assert artifact["observations"][0]["is_upper_limit"] is False

    def test_already_upper_limit_not_double_flagged(self, phot_table: Any, main_table: Any) -> None:
        """mag_err=2.0, is_upper_limit=True → unchanged (no double-flagging)."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="optical",
            magnitude=18.0,
            mag_err=2.0,
            is_upper_limit=True,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert len(artifact["observations"]) == 1
        assert artifact["observations"][0]["is_upper_limit"] is True

    def test_non_optical_not_flagged(self, phot_table: Any, main_table: Any) -> None:
        """Radio point with large flux_density_err → not auto-flagged."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="radio",
            band_id="VLA_5GHz",
            magnitude=None,
            mag_err=None,
            flux_density=1.5,
            flux_density_err=5.0,
            is_upper_limit=False,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert len(artifact["observations"]) == 1
        assert artifact["observations"][0]["is_upper_limit"] is False

    def test_boundary_exactly_threshold_not_flagged(self, phot_table: Any, main_table: Any) -> None:
        """mag_err=1.0 exactly → NOT flagged (strictly greater than)."""
        _seed_phot_row(
            phot_table,
            _NOVA_ID,
            "r1",
            regime="optical",
            magnitude=14.0,
            mag_err=1.0,
            is_upper_limit=False,
        )
        ctx = _base_context()
        artifact = generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        assert len(artifact["observations"]) == 1
        assert artifact["observations"][0]["is_upper_limit"] is False


# ---------------------------------------------------------------------------
# Regime band wavelength ordering (P5)
# ---------------------------------------------------------------------------


class TestRegimeBandWavelengthOrder:
    """Bands within each regime record are sorted by wavelength, not alphabetically."""

    def test_optical_bands_sorted_by_wavelength(self, phot_table: Any, main_table: Any) -> None:
        """B (4450Å), V (5510Å), R (6580Å), I (8060Å) — wavelength order, not alpha."""
        for row_id, band_id in [
            ("r1", "Generic_I"),
            ("r2", "Generic_B"),
            ("r3", "Generic_R"),
            ("r4", "Generic_V"),
        ]:
            _seed_phot_row(
                phot_table,
                _NOVA_ID,
                row_id,
                band_id=band_id,
                regime="optical",
                magnitude=12.0,
            )
        ctx = _base_context()
        artifact = generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        optical_regime = [r for r in artifact["regimes"] if r["id"] == "optical"]
        assert len(optical_regime) == 1
        assert optical_regime[0]["bands"] == ["B", "V", "R", "I"]

    def test_unknown_band_sorts_to_end(self, phot_table: Any, main_table: Any) -> None:
        """A band_id not in the registry appears after all known bands."""
        for row_id, band_id in [
            ("r1", "Generic_V"),
            ("r2", "Generic_B"),
            ("r3", "UNKNOWN_FILTER_X"),
        ]:
            _seed_phot_row(
                phot_table,
                _NOVA_ID,
                row_id,
                band_id=band_id,
                regime="optical",
                magnitude=12.0,
            )
        ctx = _base_context()
        artifact = generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        optical_regime = [r for r in artifact["regimes"] if r["id"] == "optical"]
        assert len(optical_regime) == 1
        bands = optical_regime[0]["bands"]
        # B and V first (wavelength order), unknown last
        assert bands[:2] == ["B", "V"]
        assert bands[-1] == "UNKNOWN_FILTER_X"

    def test_mixed_regimes_sorted_independently(self, phot_table: Any, main_table: Any) -> None:
        """Optical and NIR bands sort independently within the optical regime tab."""
        for row_id, band_id, regime in [
            ("r1", "Generic_R", "optical"),
            ("r2", "Generic_B", "optical"),
            ("r3", "2MASS_K", "nir"),
            ("r4", "2MASS_J", "nir"),
            ("r5", "2MASS_H", "nir"),
        ]:
            _seed_phot_row(
                phot_table,
                _NOVA_ID,
                row_id,
                band_id=band_id,
                regime=regime,
                magnitude=12.0,
            )
        ctx = _base_context()
        artifact = generate_photometry_json(_NOVA_ID, phot_table, main_table, _REGISTRY, ctx)
        # NIR maps to optical, so all are in one regime tab
        optical_regime = [r for r in artifact["regimes"] if r["id"] == "optical"]
        assert len(optical_regime) == 1
        # Should be: B (4450), R (6580), J (12350), H (16620), K (21590)
        assert optical_regime[0]["bands"] == ["B", "R", "J", "H", "K"]
