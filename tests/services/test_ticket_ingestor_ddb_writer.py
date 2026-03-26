"""Unit tests for ticket_ingestor.ddb_writer.

Scope: DDB and S3 write layer only.  Transform logic (photometry_reader)
is not re-tested here.  All AWS calls are intercepted by moto.

Fixture summary
---------------
aws_credentials (autouse)
    Sets AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and
    the three application env vars consumed by handler.py at import time.
    Declared autouse so that the moto mock_aws context always starts with
    a valid credential environment.

photometry_table
    Moto-backed DynamoDB table for PhotometryRow items (PK + SK only).
    Yields the boto3 Table resource.

nova_cat_table
    Moto-backed DynamoDB table for envelope items (PK + SK only).
    Yields the boto3 Table resource.

s3_resources
    Moto-backed S3 bucket; yields (bucket_name, s3_client).

Each fixture manages its own mock_aws() context so that the mock is
active for exactly the duration of the test that uses it.  The three
fixtures test independent functions that do not interact, so separate
contexts are correct here.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Generator
from decimal import Decimal
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from ticket_ingestor.ddb_writer import (
    WriteResult,
    persist_row_failures,
    upsert_envelope_item,
    write_photometry_rows,
)
from ticket_ingestor.photometry_reader import ResolvedRow, RowFailure

from contracts.models.entities import (
    BandResolutionConfidence,
    BandResolutionType,
    DataOrigin,
    DataRights,
    PhotometryRow,
    QualityFlag,
    SpectralCoordType,
    SpectralCoordUnit,
    TimeOrigSys,
)

# ---------------------------------------------------------------------------
# Test-wide constants
# ---------------------------------------------------------------------------

_REGION = "us-east-1"
_PHOTOMETRY_TABLE_NAME = "NovaCat-Photometry-Test"
_NOVA_CAT_TABLE_NAME = "NovaCat-Test"
_BUCKET_NAME = "nova-cat-diagnostics-test"

# A valid 19-character ADS bibcode used by all PhotometryRow fixtures.
_BIBCODE = "2001MNRAS.326L..13L"

_ENVELOPE_SK = "PRODUCT#PHOTOMETRY_TABLE"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set mandatory AWS credential env vars and application env vars."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    # Application env vars consumed by handler.py at import time.
    monkeypatch.setenv("PHOTOMETRY_TABLE_NAME", _PHOTOMETRY_TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _NOVA_CAT_TABLE_NAME)
    monkeypatch.setenv("DIAGNOSTICS_BUCKET", _BUCKET_NAME)


@pytest.fixture
def photometry_table(aws_credentials: None) -> Generator[Any, None, None]:
    """Moto-backed DynamoDB table for PhotometryRow items."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        tbl = dynamodb.create_table(
            TableName=_PHOTOMETRY_TABLE_NAME,
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


@pytest.fixture
def nova_cat_table(aws_credentials: None) -> Generator[Any, None, None]:
    """Moto-backed DynamoDB table for envelope items."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        tbl = dynamodb.create_table(
            TableName=_NOVA_CAT_TABLE_NAME,
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


@pytest.fixture
def s3_resources(aws_credentials: None) -> Generator[tuple[str, Any], None, None]:
    """Moto-backed S3 diagnostics bucket; yields (bucket_name, s3_client)."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=_REGION)
        # us-east-1 does not accept a CreateBucketConfiguration.
        s3.create_bucket(Bucket=_BUCKET_NAME)
        yield _BUCKET_NAME, s3


# ---------------------------------------------------------------------------
# Test helper
# ---------------------------------------------------------------------------


def _make_resolved_row(
    nova_id: uuid.UUID,
    row_id: uuid.UUID | None = None,
) -> ResolvedRow:
    """Build a minimal but fully valid ResolvedRow for write-layer tests.

    The PhotometryRow satisfies all cross-field invariants:
      - is_upper_limit=False  →  magnitude is set (non-None measurement)
      - time_orig and time_orig_sys are both set (co-presence rule)
      - no flux_density  →  flux_density_unit is not required
    """
    effective_row_id = row_id if row_id is not None else uuid.uuid4()
    row = PhotometryRow(
        # Section 1 — Identity
        nova_id=nova_id,
        primary_name="V4739 Sgr",
        ra_deg=270.123,
        dec_deg=-28.456,
        # Section 2 — Temporal
        time_mjd=52148.339,
        time_bary_corr=False,
        time_orig=2452148.839,
        time_orig_sys=TimeOrigSys.jd_utc,
        # Section 3 — Spectral / Bandpass
        band_id="Generic_V",
        regime="optical",
        svo_filter_id=None,
        spectral_coord_type=SpectralCoordType.wavelength,
        spectral_coord_value=5500.0,
        spectral_coord_unit=SpectralCoordUnit.angstrom,
        bandpass_width=890.0,
        # Section 4 — Photometric Measurement
        magnitude=7.46,
        mag_err=0.009,
        is_upper_limit=False,
        quality_flag=QualityFlag.good,
        # Section 5 — Provenance
        bibcode=_BIBCODE,
        telescope="Mt John Observatory 0.6 m Cassegrain",
        observer="Gilmore, A. C.",
        data_rights=DataRights.public,
        band_resolution_type=BandResolutionType.generic_fallback,
        band_resolution_confidence=BandResolutionConfidence.low,
        sidecar_contributed=False,
        data_origin=DataOrigin.literature,
    )
    return ResolvedRow(row_id=effective_row_id, row=row)


# ---------------------------------------------------------------------------
# write_photometry_rows
# ---------------------------------------------------------------------------


def test_write_photometry_rows_duplicate_suppression(photometry_table: Any) -> None:
    """Passing the same ResolvedRow twice yields rows_written=1, rows_skipped_duplicate=1.

    The conditional PutItem on attribute_not_exists(SK) must suppress the
    second write without raising an exception, and the counter must reflect
    the suppression.
    """
    nova_id = uuid.uuid4()
    row = _make_resolved_row(nova_id)

    result = write_photometry_rows(
        rows=[row, row],
        nova_id=nova_id,
        table_name=_PHOTOMETRY_TABLE_NAME,
        table=photometry_table,
    )

    assert result == WriteResult(rows_written=1, rows_skipped_duplicate=1)


def test_write_photometry_rows_correct_counts(photometry_table: Any) -> None:
    """Two distinct ResolvedRows (different row_ids) both write successfully."""
    nova_id = uuid.uuid4()
    row_a = _make_resolved_row(nova_id, row_id=uuid.uuid4())
    row_b = _make_resolved_row(nova_id, row_id=uuid.uuid4())

    result = write_photometry_rows(
        rows=[row_a, row_b],
        nova_id=nova_id,
        table_name=_PHOTOMETRY_TABLE_NAME,
        table=photometry_table,
    )

    assert result == WriteResult(rows_written=2, rows_skipped_duplicate=0)


# ---------------------------------------------------------------------------
# persist_row_failures
# ---------------------------------------------------------------------------


def test_persist_row_failures_writes_correct_key_and_json(
    s3_resources: tuple[str, Any],
) -> None:
    """Non-empty failure list is written to the expected S3 key as valid JSON."""
    bucket_name, s3 = s3_resources
    nova_id = uuid.uuid4()
    filename = "V4739_Sgr_Livingston_optical_Photometry_Trimmed.csv"

    failures = [
        RowFailure(
            row_number=3,
            reason="Cannot convert epoch to float: 'N/A'",
            raw_row=["N/A", "7.5", "0.01", "V"],
        ),
        RowFailure(
            row_number=7,
            reason="Unresolvable filter string: 'clear'",
            raw_row=["59000.0", "8.1", "0.02", "clear"],
        ),
    ]

    persist_row_failures(
        failures=failures,
        nova_id=nova_id,
        filename=filename,
        bucket=bucket_name,
        s3=s3,
    )

    expected_sha = hashlib.sha256(filename.encode()).hexdigest()
    expected_key = f"diagnostics/photometry/{nova_id}/row_failures/{expected_sha}.json"

    obj = s3.get_object(Bucket=bucket_name, Key=expected_key)
    payload = json.loads(obj["Body"].read())

    assert isinstance(payload, list)
    assert len(payload) == 2
    assert payload[0]["row_number"] == 3
    assert payload[0]["reason"] == "Cannot convert epoch to float: 'N/A'"
    assert payload[0]["raw_row"] == ["N/A", "7.5", "0.01", "V"]
    assert payload[1]["row_number"] == 7


def test_persist_row_failures_noop_on_empty(
    s3_resources: tuple[str, Any],
) -> None:
    """Empty failure list must not write any S3 object."""
    bucket_name, s3 = s3_resources
    nova_id = uuid.uuid4()

    persist_row_failures(
        failures=[],
        nova_id=nova_id,
        filename="some_file.csv",
        bucket=bucket_name,
        s3=s3,
    )

    response = s3.list_objects_v2(Bucket=bucket_name)
    assert response["KeyCount"] == 0


# ---------------------------------------------------------------------------
# upsert_envelope_item
# ---------------------------------------------------------------------------


def test_upsert_envelope_item_creates_when_absent(nova_cat_table: Any) -> None:
    """When the envelope item does not exist it is created with correct fields."""
    nova_id = uuid.uuid4()

    upsert_envelope_item(nova_id=nova_id, rows_written=4, table=nova_cat_table)

    item = nova_cat_table.get_item(Key={"PK": str(nova_id), "SK": _ENVELOPE_SK})["Item"]

    assert item["entity_type"] == "DataProduct"
    assert item["product_type"] == "PHOTOMETRY_TABLE"
    assert item["last_ingestion_source"] == "ticket_ingestion"
    assert item["row_count"] == Decimal("4")
    assert item["ingestion_count"] == Decimal("1")
    assert "last_ingestion_at" in item
    assert "created_at" in item
    assert "data_product_id" in item
    # data_product_id must be a valid UUID string
    uuid.UUID(str(item["data_product_id"]))


def test_upsert_envelope_item_increments_when_present(nova_cat_table: Any) -> None:
    """Calling upsert twice increments both row_count and ingestion_count."""
    nova_id = uuid.uuid4()

    upsert_envelope_item(nova_id=nova_id, rows_written=3, table=nova_cat_table)
    upsert_envelope_item(nova_id=nova_id, rows_written=2, table=nova_cat_table)

    item = nova_cat_table.get_item(Key={"PK": str(nova_id), "SK": _ENVELOPE_SK})["Item"]

    assert item["row_count"] == Decimal("5")
    assert item["ingestion_count"] == Decimal("2")


def test_upsert_envelope_item_concurrent_creation_fallback(
    nova_cat_table: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When put_item raises ConditionalCheckFailedException the update path runs.

    Simulates a race where a concurrent invocation already created the
    envelope item by the time our create attempt fires.  The loser of the
    race must fall through to update_item and still record the ingestion.
    """
    nova_id = uuid.uuid4()

    # Pre-seed the item as if initialize_nova (or a concurrent ingest) already
    # created it, bypassing the monkeypatch that hasn't been applied yet.
    nova_cat_table.put_item(
        Item={
            "PK": str(nova_id),
            "SK": _ENVELOPE_SK,
            "entity_type": "DataProduct",
            "row_count": Decimal("0"),
            "ingestion_count": Decimal("0"),
        }
    )

    # Force every subsequent put_item call on this table to raise
    # ConditionalCheckFailedException, simulating the race-loss scenario.
    def _always_raise_conditional(*args: Any, **kwargs: Any) -> Any:
        raise ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "mocked"}},
            "PutItem",
        )

    monkeypatch.setattr(nova_cat_table, "put_item", _always_raise_conditional)

    # Must not raise — the update path handles the conditional check failure.
    upsert_envelope_item(nova_id=nova_id, rows_written=3, table=nova_cat_table)

    # update_item ran: row_count and ingestion_count were incremented from zero.
    item = nova_cat_table.get_item(Key={"PK": str(nova_id), "SK": _ENVELOPE_SK})["Item"]
    assert item["row_count"] == Decimal("3")
    assert item["ingestion_count"] == Decimal("1")
