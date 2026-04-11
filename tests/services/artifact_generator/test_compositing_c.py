"""Tests for compositing C1+C2 — DDB queries, DDB writes, S3 persistence.

Uses moto to mock DynamoDB and S3.  LTTB downsampling is tested via
persist_composite_csvs with arrays exceeding the 2000-point threshold.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import patch

import boto3
import numpy as np
import pytest
from generators.compositing import (
    _lttb_downsample,
    find_compositable_products,
    find_existing_composites,
    persist_composite_csvs,
    write_composite_data_product,
)
from moto import mock_aws

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_TABLE_NAME = "TestNovaCat"
_BUCKET_NAME = "test-private-bucket"
_NOVA_ID = "aaaa-bbbb-cccc-dddd"


def _create_table(dynamodb: Any) -> Any:
    """Create a minimal DDB table matching the NovaCat key schema."""
    table = dynamodb.create_table(
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
    table.wait_until_exists()
    return table


def _create_bucket(s3_client: Any) -> None:
    """Create a test S3 bucket."""
    s3_client.create_bucket(Bucket=_BUCKET_NAME)


def _put_individual_spectrum(
    table: Any,
    nova_id: str,
    dp_id: str,
    provider: str = "ESO",
    instrument: str = "UVES",
    validation_status: str = "VALID",
    observation_date_mjd: float = 60000.3,
    sha256: str = "abc123",
) -> None:
    """Write a minimal individual spectra DataProduct item."""
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": f"PRODUCT#SPECTRA#{provider}#{dp_id}",
            "entity_type": "DataProduct",
            "data_product_id": dp_id,
            "product_type": "SPECTRA",
            "provider": provider,
            "instrument": instrument,
            "validation_status": validation_status,
            "observation_date_mjd": Decimal(str(observation_date_mjd)),
            "sha256": sha256,
        }
    )


def _put_composite_spectrum(
    table: Any,
    nova_id: str,
    composite_id: str,
    provider: str = "ESO",
    instrument: str = "UVES",
    constituent_ids: list[str] | None = None,
    fingerprint: str = "fp-abc",
) -> None:
    """Write a minimal composite DataProduct item."""
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": f"PRODUCT#SPECTRA#{provider}#COMPOSITE#{composite_id}",
            "entity_type": "DataProduct",
            "data_product_id": composite_id,
            "product_type": "SPECTRA",
            "provider": provider,
            "instrument": instrument,
            "validation_status": "VALID",
            "constituent_data_product_ids": constituent_ids or [],
            "rejected_data_product_ids": [],
            "composite_fingerprint": fingerprint,
        }
    )


# ===================================================================
# find_compositable_products
# ===================================================================


class TestFindCompositableProducts:
    """Query for VALID individual spectra, excluding composites."""

    @mock_aws
    def test_returns_valid_individuals(self) -> None:
        """Returns VALID individual spectra DataProducts."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        _put_individual_spectrum(table, _NOVA_ID, "dp-001")
        _put_individual_spectrum(table, _NOVA_ID, "dp-002")

        result = find_compositable_products(table, _NOVA_ID)
        ids = {item["data_product_id"] for item in result}
        assert ids == {"dp-001", "dp-002"}

    @mock_aws
    def test_excludes_non_valid(self) -> None:
        """Non-VALID spectra are filtered out."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        _put_individual_spectrum(table, _NOVA_ID, "dp-valid", validation_status="VALID")
        _put_individual_spectrum(table, _NOVA_ID, "dp-failed", validation_status="FAILED")
        _put_individual_spectrum(table, _NOVA_ID, "dp-unval", validation_status="UNVALIDATED")

        result = find_compositable_products(table, _NOVA_ID)
        ids = {item["data_product_id"] for item in result}
        assert ids == {"dp-valid"}

    @mock_aws
    def test_excludes_composites(self) -> None:
        """Composite DataProducts are excluded from the result."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        _put_individual_spectrum(table, _NOVA_ID, "dp-001")
        _put_composite_spectrum(table, _NOVA_ID, "comp-001", constituent_ids=["dp-001"])

        result = find_compositable_products(table, _NOVA_ID)
        ids = {item["data_product_id"] for item in result}
        assert ids == {"dp-001"}

    @mock_aws
    def test_empty_nova(self) -> None:
        """Nova with no spectra returns empty list."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        result = find_compositable_products(table, _NOVA_ID)
        assert result == []

    @mock_aws
    def test_ignores_other_novas(self) -> None:
        """Only returns products for the requested nova."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        _put_individual_spectrum(table, _NOVA_ID, "dp-mine")
        _put_individual_spectrum(table, "other-nova-id", "dp-theirs")

        result = find_compositable_products(table, _NOVA_ID)
        ids = {item["data_product_id"] for item in result}
        assert ids == {"dp-mine"}


# ===================================================================
# find_existing_composites
# ===================================================================


class TestFindExistingComposites:
    """Query for existing composite DataProduct items."""

    @mock_aws
    def test_returns_composites(self) -> None:
        """Returns composite items identified by COMPOSITE in SK."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        _put_composite_spectrum(table, _NOVA_ID, "comp-001")
        _put_composite_spectrum(table, _NOVA_ID, "comp-002")

        result = find_existing_composites(table, _NOVA_ID)
        ids = {item["data_product_id"] for item in result}
        assert ids == {"comp-001", "comp-002"}

    @mock_aws
    def test_excludes_individuals(self) -> None:
        """Individual spectra are excluded from the result."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        _put_individual_spectrum(table, _NOVA_ID, "dp-001")
        _put_composite_spectrum(table, _NOVA_ID, "comp-001")

        result = find_existing_composites(table, _NOVA_ID)
        ids = {item["data_product_id"] for item in result}
        assert ids == {"comp-001"}

    @mock_aws
    def test_empty_nova(self) -> None:
        """Nova with no composites returns empty list."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        _put_individual_spectrum(table, _NOVA_ID, "dp-001")

        result = find_existing_composites(table, _NOVA_ID)
        assert result == []


# ===================================================================
# write_composite_data_product
# ===================================================================


class TestWriteCompositeDataProduct:
    """PutItem for composite (and degenerate) DataProducts."""

    @mock_aws
    def test_writes_real_composite(self) -> None:
        """A real composite item has all expected fields."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        write_composite_data_product(
            table=table,
            nova_id=_NOVA_ID,
            composite_id="comp-001",
            provider="ESO",
            instrument="UVES",
            telescope="VLT-UT2",
            observation_date_mjd=60000.35,
            constituent_data_product_ids=["dp-001", "dp-002"],
            rejected_data_product_ids=["dp-003"],
            composite_fingerprint="fp-xyz",
            composite_s3_key="derived/spectra/nova/comp-001/composite_full.csv",
            web_ready_s3_key="derived/spectra/nova/comp-001/web_ready.csv",
        )

        resp = table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": "PRODUCT#SPECTRA#ESO#COMPOSITE#comp-001",
            }
        )
        item = resp["Item"]

        assert item["entity_type"] == "DataProduct"
        assert item["data_product_id"] == "comp-001"
        assert item["product_type"] == "SPECTRA"
        assert item["provider"] == "ESO"
        assert item["instrument"] == "UVES"
        assert item["telescope"] == "VLT-UT2"
        assert item["validation_status"] == "VALID"
        assert item["eligibility"] == "NONE"
        assert item["constituent_data_product_ids"] == ["dp-001", "dp-002"]
        assert item["rejected_data_product_ids"] == ["dp-003"]
        assert item["composite_fingerprint"] == "fp-xyz"
        assert item["composite_s3_key"] == "derived/spectra/nova/comp-001/composite_full.csv"
        assert item["web_ready_s3_key"] == "derived/spectra/nova/comp-001/web_ready.csv"
        assert "created_at" in item
        assert "updated_at" in item

    @mock_aws
    def test_writes_degenerate_composite(self) -> None:
        """A degenerate composite has web_ready_s3_key but no composite_s3_key."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        write_composite_data_product(
            table=table,
            nova_id=_NOVA_ID,
            composite_id="comp-degen",
            provider="ESO",
            instrument="UVES",
            telescope=None,
            observation_date_mjd=60000.4,
            constituent_data_product_ids=["dp-survivor"],
            rejected_data_product_ids=["dp-rejected-1", "dp-rejected-2"],
            composite_fingerprint="fp-degen",
            composite_s3_key=None,
            web_ready_s3_key="derived/spectra/nova/dp-survivor/web_ready.csv",
        )

        resp = table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": "PRODUCT#SPECTRA#ESO#COMPOSITE#comp-degen",
            }
        )
        item = resp["Item"]

        assert item["constituent_data_product_ids"] == ["dp-survivor"]
        assert item["rejected_data_product_ids"] == ["dp-rejected-1", "dp-rejected-2"]
        assert "composite_s3_key" not in item
        assert item["web_ready_s3_key"] == "derived/spectra/nova/dp-survivor/web_ready.csv"
        assert "telescope" not in item

    @mock_aws
    def test_overwrites_existing(self) -> None:
        """A second PutItem with the same key replaces the item."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        for fp in ("fp-old", "fp-new"):
            write_composite_data_product(
                table=table,
                nova_id=_NOVA_ID,
                composite_id="comp-001",
                provider="ESO",
                instrument="UVES",
                telescope=None,
                observation_date_mjd=60000.3,
                constituent_data_product_ids=["dp-001", "dp-002"],
                rejected_data_product_ids=[],
                composite_fingerprint=fp,
                composite_s3_key="derived/s.csv",
                web_ready_s3_key="derived/w.csv",
            )

        resp = table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": "PRODUCT#SPECTRA#ESO#COMPOSITE#comp-001",
            }
        )
        assert resp["Item"]["composite_fingerprint"] == "fp-new"

    @mock_aws
    def test_observation_date_stored_as_decimal(self) -> None:
        """MJD is stored as a Decimal (DynamoDB number type)."""
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = _create_table(dynamodb)

        write_composite_data_product(
            table=table,
            nova_id=_NOVA_ID,
            composite_id="comp-001",
            provider="ESO",
            instrument="UVES",
            telescope=None,
            observation_date_mjd=60000.3456,
            constituent_data_product_ids=["dp-001", "dp-002"],
            rejected_data_product_ids=[],
            composite_fingerprint="fp-test",
            composite_s3_key="s.csv",
            web_ready_s3_key="w.csv",
        )

        resp = table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": "PRODUCT#SPECTRA#ESO#COMPOSITE#comp-001",
            }
        )
        mjd = resp["Item"]["observation_date_mjd"]
        assert isinstance(mjd, Decimal)
        assert float(mjd) == pytest.approx(60000.3456)


# ===================================================================
# persist_composite_csvs
# ===================================================================


class TestPersistCompositeCsvs:
    """S3 upload of full-resolution and web-ready composite CSVs."""

    @mock_aws
    def test_writes_both_csvs(self) -> None:
        """Both composite_full.csv and web_ready.csv are written to S3."""
        s3 = boto3.client("s3", region_name="us-east-1")
        _create_bucket(s3)

        wl = np.linspace(400.0, 500.0, 100)
        fx = np.ones(100)

        full_key, wr_key = persist_composite_csvs(
            s3,
            _BUCKET_NAME,
            _NOVA_ID,
            "comp-001",
            wl,
            fx,
        )

        assert full_key == f"derived/spectra/{_NOVA_ID}/comp-001/composite_full.csv"
        assert wr_key == f"derived/spectra/{_NOVA_ID}/comp-001/web_ready.csv"

        # Verify both objects exist in S3.
        full_obj = s3.get_object(Bucket=_BUCKET_NAME, Key=full_key)
        wr_obj = s3.get_object(Bucket=_BUCKET_NAME, Key=wr_key)

        full_body = full_obj["Body"].read().decode("utf-8")
        wr_body = wr_obj["Body"].read().decode("utf-8")

        assert full_body.startswith("wavelength_nm,flux")
        assert wr_body.startswith("wavelength_nm,flux")

    @mock_aws
    def test_full_csv_has_all_points(self) -> None:
        """The full-resolution CSV contains all non-NaN grid points."""
        s3 = boto3.client("s3", region_name="us-east-1")
        _create_bucket(s3)

        wl = np.linspace(400.0, 500.0, 500)
        fx = np.ones(500)

        full_key, _ = persist_composite_csvs(
            s3,
            _BUCKET_NAME,
            _NOVA_ID,
            "comp-001",
            wl,
            fx,
        )

        body = s3.get_object(Bucket=_BUCKET_NAME, Key=full_key)["Body"].read().decode()
        lines = body.strip().split("\n")
        # Header + 500 data rows.
        assert len(lines) == 501

    @mock_aws
    def test_web_ready_downsampled(self) -> None:
        """Web-ready CSV is downsampled to ≤ 2000 points for large composites."""
        s3 = boto3.client("s3", region_name="us-east-1")
        _create_bucket(s3)

        # 10000 points — well above the 2000 LTTB threshold.
        wl = np.linspace(400.0, 800.0, 10000)
        fx = np.sin(np.linspace(0, 20 * np.pi, 10000))

        _, wr_key = persist_composite_csvs(
            s3,
            _BUCKET_NAME,
            _NOVA_ID,
            "comp-001",
            wl,
            fx,
        )

        body = s3.get_object(Bucket=_BUCKET_NAME, Key=wr_key)["Body"].read().decode()
        lines = body.strip().split("\n")
        data_rows = len(lines) - 1  # subtract header
        assert data_rows <= 2000

    @mock_aws
    def test_small_composite_not_downsampled(self) -> None:
        """Composites with ≤ 2000 points are not downsampled."""
        s3 = boto3.client("s3", region_name="us-east-1")
        _create_bucket(s3)

        wl = np.linspace(400.0, 500.0, 500)
        fx = np.ones(500)

        full_key, wr_key = persist_composite_csvs(
            s3,
            _BUCKET_NAME,
            _NOVA_ID,
            "comp-001",
            wl,
            fx,
        )

        full_body = s3.get_object(Bucket=_BUCKET_NAME, Key=full_key)["Body"].read().decode()
        wr_body = s3.get_object(Bucket=_BUCKET_NAME, Key=wr_key)["Body"].read().decode()

        full_rows = len(full_body.strip().split("\n")) - 1
        wr_rows = len(wr_body.strip().split("\n")) - 1
        assert full_rows == wr_rows == 500

    @mock_aws
    def test_nan_excluded_from_both_csvs(self) -> None:
        """NaN flux values are excluded from both CSVs."""
        s3 = boto3.client("s3", region_name="us-east-1")
        _create_bucket(s3)

        wl = np.linspace(400.0, 500.0, 10)
        fx = np.array([1.0, np.nan, 3.0, np.nan, 5.0, 6.0, np.nan, 8.0, 9.0, 10.0])

        full_key, wr_key = persist_composite_csvs(
            s3,
            _BUCKET_NAME,
            _NOVA_ID,
            "comp-001",
            wl,
            fx,
        )

        full_body = s3.get_object(Bucket=_BUCKET_NAME, Key=full_key)["Body"].read().decode()
        full_rows = len(full_body.strip().split("\n")) - 1
        assert full_rows == 7  # 10 - 3 NaN


# ===================================================================
# _lttb_downsample
# ===================================================================


class TestLttbDownsample:
    """LTTB downsampling wrapper with NaN stripping."""

    def test_strips_nan_before_lttb(self) -> None:
        """NaN values are removed before LTTB processes the data."""
        wl = np.array([1.0, 2.0, np.nan, 4.0, 5.0])
        fx = np.array([10.0, 20.0, np.nan, 40.0, 50.0])

        # Patch segment_aware_lttb to verify it receives clean arrays.
        def _capture_lttb(w: list[float], f: list[float]) -> tuple[list[float], list[float]]:
            assert not any(np.isnan(v) for v in w)
            assert not any(np.isnan(v) for v in f)
            return w, f

        with patch("generators.shared.segment_aware_lttb", side_effect=_capture_lttb):
            result_wl, result_fx = _lttb_downsample(wl, fx)

        assert len(result_wl) == 4  # 5 - 1 NaN in wavelengths

    def test_below_threshold_passthrough(self) -> None:
        """Arrays with ≤ 2000 points pass through unchanged."""
        wl = np.linspace(400.0, 500.0, 100)
        fx = np.ones(100)
        result_wl, result_fx = _lttb_downsample(wl, fx)
        assert len(result_wl) == 100
        assert len(result_fx) == 100
