"""
Integration tests for the acquire_and_validate_spectra workflow.

Simulates the full Step Functions execution by calling each Lambda handler
directly in ASL order, sharing a single mocked DynamoDB + S3 instance.
No real AWS or provider archive calls are made — HTTP downloads are patched
via requests.Session.get, and the real UVES profile validation pipeline runs
against synthetic FITS bytes built in memory with astropy.

Workflow order (per acquire_and_validate_spectra.asl.json):
  EnsureCorrelationId → BeginJobRun → AcquireIdempotencyLock →
  CheckOperationalStatus → CheckOperationalStatusOutcome →
    AlreadyValidated | QuarantineBlocked | CooldownActive | AcquireArtifact
  → ValidateBytes → DuplicateByFingerprint →
    RecordValidationResult | RecordDuplicateLinkage
  → FinalizeJobRunSuccess | TerminalFailHandler → FinalizeJobRunFailed

Paths covered:
  1. Happy path — VALID: download succeeds, UVES profile matches, product
     persisted with validation_status=VALID, GSI1 keys removed, sha256 stored
  2. AlreadyValidated skip: DataProduct already has validation_status=VALID →
     CheckOperationalStatusOutcome routes to AlreadyValidated → clean skip
  3. QuarantineBlocked skip: DataProduct is QUARANTINED without operator
     clearance → skip
  4. CooldownActive skip: next_eligible_attempt_at in the future → skip
  5. QUARANTINED outcome: synthetic FITS fails monotonicity sanity check →
     product persisted with validation_status=QUARANTINED, GSI1 keys removed
  6. Duplicate detection: sha256 collision with existing VALID product →
     RecordDuplicateLinkage, acquisition_status=SKIPPED_DUPLICATE, not VALID
  7. HTTP 5xx → RetryableError raised from AcquireArtifact
  8. HTTP 404 → ValueError raised from AcquireArtifact (terminal)
  9. attempt_count incremented before download completes
"""

from __future__ import annotations

import hashlib
import importlib
import io
import sys
import types
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock, patch

import boto3
import numpy as np
import pytest
from astropy.io import fits
from boto3.dynamodb.conditions import Key
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_BUCKET_NAME = "nova-cat-private-test"
_REGION = "us-east-1"
_ACCOUNT = "123456789012"

_NOVA_ID = "cccccccc-0000-0000-0000-000000000001"
_CORRELATION_ID = "integ-acquire-corr-001"
_PROVIDER = "ESO"
_DATA_PRODUCT_ID = "dddddddd-1111-1111-1111-000000000001"
_EXISTING_DP_ID = "dddddddd-2222-2222-2222-000000000002"  # pre-existing VALID product for dup tests
_ACCESS_URL = "https://archive.eso.org/datalink/links?ID=eso:test-001"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", f"arn:aws:sns:{_REGION}:{_ACCOUNT}:quarantine"
    )
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", _BUCKET_NAME)
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-public-test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def aws_resources(aws_env: None) -> Generator[tuple[Any, Any], None, None]:
    """DynamoDB table + S3 bucket, pre-seeded with a Nova item."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        tbl = dynamodb.create_table(
            TableName=_TABLE_NAME,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        tbl.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": "NOVA",
                "entity_type": "Nova",
                "nova_id": _NOVA_ID,
                "status": "ACTIVE",
                "ra_deg": Decimal("271.5755"),
                "dec_deg": Decimal("-30.6558"),
            }
        )
        s3 = boto3.client("s3", region_name=_REGION)
        s3.create_bucket(Bucket=_BUCKET_NAME)
        yield tbl, s3


# ---------------------------------------------------------------------------
# FITS byte builders
# ---------------------------------------------------------------------------


def _make_uves_fits_bytes(*, n: int = 1000) -> bytes:
    """Build a minimal synthetic UVES FITS file that passes all profile sanity checks."""
    primary = fits.PrimaryHDU()
    hdr = primary.header
    hdr["INSTRUME"] = "UVES"
    hdr["TELESCOP"] = "ESO-VLT-U2"
    hdr["MJD-OBS"] = 56082.05
    hdr["DATE-OBS"] = "2012-06-04"
    hdr["EXPTIME"] = 7200.0
    hdr["SPEC_RES"] = 42000.0
    hdr["FLUXCAL"] = "ABSOLUTE"

    wave = np.linspace(3200.0, 10000.0, n).astype(np.float32)
    flux = np.ones(n, dtype=np.float32) * 1.5e-16
    err = np.ones(n, dtype=np.float32) * 1.0e-17
    qual = np.zeros(n, dtype=np.int32)

    cols = fits.ColDefs(
        [
            fits.Column(name="WAVE", format=f"{n}E", array=wave.reshape(1, n)),
            fits.Column(name="FLUX", format=f"{n}E", array=flux.reshape(1, n)),
            fits.Column(name="ERR", format=f"{n}E", array=err.reshape(1, n)),
            fits.Column(name="QUAL", format=f"{n}J", array=qual.reshape(1, n)),
        ]
    )
    spectrum_hdu = fits.BinTableHDU.from_columns(cols, name="SPECTRUM")

    buf = io.BytesIO()
    fits.HDUList([primary, spectrum_hdu]).writeto(buf)
    return buf.getvalue()


def _make_quarantine_fits_bytes() -> bytes:
    """
    Build a synthetic UVES FITS file that fails the strict-monotonicity
    sanity check, producing a QUARANTINED outcome.
    """
    primary = fits.PrimaryHDU()
    hdr = primary.header
    hdr["INSTRUME"] = "UVES"
    hdr["TELESCOP"] = "ESO-VLT-U2"
    hdr["MJD-OBS"] = 56082.05
    hdr["DATE-OBS"] = "2012-06-04"
    hdr["EXPTIME"] = 7200.0
    hdr["SPEC_RES"] = 42000.0
    hdr["FLUXCAL"] = "ABSOLUTE"

    n = 1000
    # Non-monotonic wavelength array — deliberately reversed halfway through
    wave = np.concatenate(
        [np.linspace(3200.0, 6500.0, n // 2), np.linspace(6500.0, 3200.0, n - n // 2)]
    ).astype(np.float32)
    flux = np.ones(n, dtype=np.float32) * 1.5e-16
    err = np.ones(n, dtype=np.float32) * 1.0e-17
    qual = np.zeros(n, dtype=np.int32)

    cols = fits.ColDefs(
        [
            fits.Column(name="WAVE", format=f"{n}E", array=wave.reshape(1, n)),
            fits.Column(name="FLUX", format=f"{n}E", array=flux.reshape(1, n)),
            fits.Column(name="ERR", format=f"{n}E", array=err.reshape(1, n)),
            fits.Column(name="QUAL", format=f"{n}J", array=qual.reshape(1, n)),
        ]
    )
    spectrum_hdu = fits.BinTableHDU.from_columns(cols, name="SPECTRUM")

    buf = io.BytesIO()
    fits.HDUList([primary, spectrum_hdu]).writeto(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# HTTP mock helper
# ---------------------------------------------------------------------------


def _make_http_response(content: bytes, status_code: int = 200) -> MagicMock:
    import requests as req

    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.headers = {"Content-Length": str(len(content))}
    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = req.HTTPError(response=mock_resp)
    else:
        mock_resp.raise_for_status.return_value = None
        mock_resp.iter_content.return_value = iter([content])
    return mock_resp


# ---------------------------------------------------------------------------
# Handler loader
# ---------------------------------------------------------------------------


def _load_handlers() -> dict[str, types.ModuleType]:
    """
    Load all workflow handlers fresh inside the current moto context.
    Clears boto3-initializing modules so clients bind to the active mock.
    spectra_validator.profiles is intentionally cleared too — _bootstrap_astropy()
    runs at module level and must see the correct /tmp env var.
    """
    for key in list(sys.modules):
        if key.startswith(
            (
                "spectra_validator",
                "spectra_acquirer",
                "job_run_manager",
                "idempotency_guard",
            )
        ):
            del sys.modules[key]

    return {
        "job_run_manager": importlib.import_module("job_run_manager.handler"),
        "idempotency_guard": importlib.import_module("idempotency_guard.handler"),
        "spectra_acquirer": importlib.import_module("spectra_acquirer.handler"),
        "spectra_validator": importlib.import_module("spectra_validator.handler"),
    }


# ---------------------------------------------------------------------------
# DynamoDB seed / fetch helpers
# ---------------------------------------------------------------------------


def _seed_data_product(
    table: Any,
    *,
    data_product_id: str = _DATA_PRODUCT_ID,
    validation_status: str = "UNVALIDATED",
    acquisition_status: str = "STUB",
    eligibility: str = "ACQUIRE",
    attempt_count: int = 0,
    manual_review_status: str | None = None,
    next_eligible_attempt_at: str | None = None,
    sha256: str | None = None,
) -> None:
    item: dict[str, Any] = {
        "PK": _NOVA_ID,
        "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{data_product_id}",
        "entity_type": "DataProduct",
        "nova_id": _NOVA_ID,
        "data_product_id": data_product_id,
        "provider": _PROVIDER,
        "product_type": "SPECTRA",
        "validation_status": validation_status,
        "acquisition_status": acquisition_status,
        "eligibility": eligibility,
        "attempt_count": attempt_count,
        "locators": [{"role": "PRIMARY", "kind": "URL", "value": _ACCESS_URL}],
        "GSI1PK": _NOVA_ID,
        "GSI1SK": f"ELIG#ACQUIRE#SPECTRA#{_PROVIDER}#{data_product_id}",
    }
    if manual_review_status is not None:
        item["manual_review_status"] = manual_review_status
    if next_eligible_attempt_at is not None:
        item["next_eligible_attempt_at"] = next_eligible_attempt_at
    if sha256 is not None:
        item["sha256"] = sha256
    table.put_item(Item=item)


def _get_data_product(table: Any, data_product_id: str = _DATA_PRODUCT_ID) -> dict[str, Any] | None:
    return cast(
        dict[str, Any] | None,
        table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{data_product_id}",
            }
        ).get("Item"),
    )


def _get_job_run(table: Any, state: dict[str, Any]) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        table.get_item(Key={"PK": state["job_run"]["pk"], "SK": state["job_run"]["sk"]})["Item"],
    )


# ---------------------------------------------------------------------------
# Workflow runner helpers — mirror ASL task order exactly
# ---------------------------------------------------------------------------


def _run_prefix(h: dict[str, types.ModuleType]) -> dict[str, Any]:
    """BeginJobRun → AcquireIdempotencyLock. Returns accumulated state dict."""
    job_run = cast(
        dict[str, Any],
        h["job_run_manager"].handle(
            {
                "task_name": "BeginJobRun",
                "workflow_name": "acquire_and_validate_spectra",
                "nova_id": _NOVA_ID,
                "correlation_id": _CORRELATION_ID,
            },
            None,
        ),
    )
    h["idempotency_guard"].handle(
        {
            "task_name": "AcquireIdempotencyLock",
            "workflow_name": "acquire_and_validate_spectra",
            "primary_id": _DATA_PRODUCT_ID,
            "correlation_id": job_run["correlation_id"],
            "job_run_id": job_run["job_run_id"],
        },
        None,
    )
    return {"nova_id": _NOVA_ID, "job_run": job_run}


def _run_check_status(h: dict[str, types.ModuleType], state: dict[str, Any]) -> dict[str, Any]:
    """CheckOperationalStatus → returns status dict with decision flags + data_product."""
    return cast(
        dict[str, Any],
        h["spectra_validator"].handle(
            {
                "task_name": "CheckOperationalStatus",
                "nova_id": _NOVA_ID,
                "provider": _PROVIDER,
                "data_product_id": _DATA_PRODUCT_ID,
                "correlation_id": state["job_run"]["correlation_id"],
            },
            None,
        ),
    )


def _run_acquire(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
    status: dict[str, Any],
    fits_bytes: bytes,
) -> dict[str, Any]:
    """AcquireArtifact — HTTP download patched to return fits_bytes."""
    mock_resp = _make_http_response(fits_bytes)
    with patch("requests.Session.get", return_value=mock_resp):
        return cast(
            dict[str, Any],
            h["spectra_acquirer"].handle(
                {
                    "task_name": "AcquireArtifact",
                    "nova_id": _NOVA_ID,
                    "provider": _PROVIDER,
                    "data_product_id": _DATA_PRODUCT_ID,
                    "correlation_id": state["job_run"]["correlation_id"],
                    "data_product": status["data_product"],
                },
                None,
            ),
        )


def _run_validate(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
    status: dict[str, Any],
    acquisition: dict[str, Any],
) -> dict[str, Any]:
    """ValidateBytes — reads raw FITS from S3, runs profile, checks sha256 dedup."""
    return cast(
        dict[str, Any],
        h["spectra_validator"].handle(
            {
                "task_name": "ValidateBytes",
                "nova_id": _NOVA_ID,
                "provider": _PROVIDER,
                "data_product_id": _DATA_PRODUCT_ID,
                "correlation_id": state["job_run"]["correlation_id"],
                "data_product": status["data_product"],
                "acquisition": acquisition,
            },
            None,
        ),
    )


def _run_record_result(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
    status: dict[str, Any],
    acquisition: dict[str, Any],
    validation: dict[str, Any],
) -> dict[str, Any]:
    """RecordValidationResult or RecordDuplicateLinkage, per DuplicateByFingerprint routing."""
    task_name = (
        "RecordDuplicateLinkage" if validation.get("is_duplicate") else "RecordValidationResult"
    )
    return cast(
        dict[str, Any],
        h["spectra_validator"].handle(
            {
                "task_name": task_name,
                "nova_id": _NOVA_ID,
                "provider": _PROVIDER,
                "data_product_id": _DATA_PRODUCT_ID,
                "correlation_id": state["job_run"]["correlation_id"],
                "data_product": status["data_product"],
                "acquisition": acquisition,
                "validation": validation,
            },
            None,
        ),
    )


def _finalize_success(h: dict[str, types.ModuleType], state: dict[str, Any]) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        h["job_run_manager"].handle(
            {
                "task_name": "FinalizeJobRunSuccess",
                "workflow_name": "acquire_and_validate_spectra",
                "outcome": "COMPLETED",
                "nova_id": _NOVA_ID,
                "data_product_id": _DATA_PRODUCT_ID,
                "correlation_id": state["job_run"]["correlation_id"],
                "job_run_id": state["job_run"]["job_run_id"],
                "job_run": state["job_run"],
            },
            None,
        ),
    )


def _finalize_failed(
    h: dict[str, types.ModuleType], state: dict[str, Any], error: dict[str, Any]
) -> None:
    h["job_run_manager"].handle(
        {
            "task_name": "TerminalFailHandler",
            "workflow_name": "acquire_and_validate_spectra",
            "error": error,
            "correlation_id": state["job_run"]["correlation_id"],
            "job_run_id": state["job_run"]["job_run_id"],
            "job_run": state["job_run"],
        },
        None,
    )
    h["job_run_manager"].handle(
        {
            "task_name": "FinalizeJobRunFailed",
            "workflow_name": "acquire_and_validate_spectra",
            "error": error,
            "job_run": state["job_run"],
        },
        None,
    )


# ---------------------------------------------------------------------------
# Path 1: Happy path — VALID
# ---------------------------------------------------------------------------


class TestHappyPathValid:
    def test_product_written_as_valid_with_profile(self, aws_resources: tuple[Any, Any]) -> None:
        """
        Full ASL path: download succeeds, UVES profile matches, FITS passes
        sanity checks → DataProduct persisted with validation_status=VALID
        and fits_profile_id set.
        """
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            assert not status["already_validated"]
            assert not status["is_quarantined"]
            assert not status["cooldown_active"]
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)
            assert validation["validation_outcome"] == "VALID"
            assert not validation["is_duplicate"]
            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert dp["validation_status"] == "VALID"
        assert dp["fits_profile_id"] == "ESO_UVES"

        # ADR-031 Decision 7: WorkItem written for the regeneration pipeline
        wq_resp = table.query(
            KeyConditionExpression=(
                Key("PK").eq("WORKQUEUE") & Key("SK").begins_with(f"{_NOVA_ID}#spectra#")
            ),
        )
        assert len(wq_resp["Items"]) >= 1, (
            "No WorkItem found in WORKQUEUE for spectra after VALID validation"
        )
        wi = wq_resp["Items"][0]
        assert wi["dirty_type"] == "spectra"
        assert wi["source_workflow"] == "acquire_and_validate_spectra"
        assert wi["nova_id"] == _NOVA_ID

    def test_gsi1_keys_removed_after_valid(self, aws_resources: tuple[Any, Any]) -> None:
        """
        RecordValidationResult atomically removes GSI1PK and GSI1SK in the
        same update_item call so the product is no longer eligible for
        re-acquisition.
        """
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)
            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert "GSI1PK" not in dp
        assert "GSI1SK" not in dp

    def test_sha256_and_s3_coords_stored_on_data_product(
        self, aws_resources: tuple[Any, Any]
    ) -> None:
        """sha256 fingerprint and raw_s3_key from acquisition are persisted."""
        table, s3 = aws_resources
        fits_bytes = _make_uves_fits_bytes()
        expected_sha256 = hashlib.sha256(fits_bytes).hexdigest()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)

            # Raw object lands in S3 at expected key
            obj = s3.get_object(Bucket=acquisition["raw_s3_bucket"], Key=acquisition["raw_s3_key"])
            assert obj["Body"].read() == fits_bytes

            validation = _run_validate(h, state, status, acquisition)
            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert dp["sha256"] == expected_sha256
        assert dp.get("raw_s3_key") == acquisition["raw_s3_key"]

    def test_job_run_ends_succeeded(self, aws_resources: tuple[Any, Any]) -> None:
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)
            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        job_run = _get_job_run(table, state)
        assert job_run["status"] == "SUCCEEDED"
        assert job_run["outcome"] == "COMPLETED"

    def test_enrichment_fields_persisted_on_valid_product(
        self, aws_resources: tuple[Any, Any]
    ) -> None:
        """ADR-031 Decisions 2, 3, 5: instrument, telescope, observation_date_mjd,
        and flux_unit are extracted from the validated spectrum and persisted on
        the DataProduct DDB item by RecordValidationResult.

        Synthetic FITS headers: INSTRUME=UVES, TELESCOP=ESO-VLT-U2, MJD-OBS=56082.05.
        UVES profile flux_units comes from TUNIT on the BinTable FLUX column;
        the synthetic FITS does not set TUNIT, so flux_units is empty string,
        which the handler normalizes to None (absent from DDB item).
        """
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)

            # Verify enrichment fields flow through $.validation
            assert validation["instrument"] == "UVES"
            assert validation["telescope"] == "ESO-VLT-U2"
            assert validation["observation_date_mjd"] == pytest.approx(56082.05)

            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None

        # ADR-031 enrichment fields on the DDB item
        assert dp["instrument"] == "UVES"
        assert dp["telescope"] == "ESO-VLT-U2"
        assert dp["observation_date_mjd"] == pytest.approx(Decimal("56082.05"))

        # flux_unit: synthetic FITS has no TUNIT on FLUX column → empty string
        # → normalized to None → not written to DDB
        assert "flux_unit" not in dp


# ---------------------------------------------------------------------------
# S21: SNR provenance through validation path
# ---------------------------------------------------------------------------


def _make_uves_fits_bytes_with_snr_column(*, n: int = 1000) -> bytes:
    """Build a synthetic UVES FITS with an SNR_REDUCED column (source-provided SNR)."""
    primary = fits.PrimaryHDU()
    hdr = primary.header
    hdr["INSTRUME"] = "UVES"
    hdr["TELESCOP"] = "ESO-VLT-U2"
    hdr["MJD-OBS"] = 56082.05
    hdr["DATE-OBS"] = "2012-06-04"
    hdr["EXPTIME"] = 7200.0
    hdr["SPEC_RES"] = 42000.0
    hdr["FLUXCAL"] = "ABSOLUTE"

    wave = np.linspace(3200.0, 10000.0, n).astype(np.float32)
    flux = np.ones(n, dtype=np.float32) * 1.5e-16
    err = np.ones(n, dtype=np.float32) * 1.0e-17
    qual = np.zeros(n, dtype=np.int32)
    snr = np.ones(n, dtype=np.float32) * 25.0  # source-provided SNR

    cols = fits.ColDefs(
        [
            fits.Column(name="WAVE", format=f"{n}E", array=wave.reshape(1, n)),
            fits.Column(name="FLUX", format=f"{n}E", array=flux.reshape(1, n)),
            fits.Column(name="ERR", format=f"{n}E", array=err.reshape(1, n)),
            fits.Column(name="QUAL", format=f"{n}J", array=qual.reshape(1, n)),
            fits.Column(name="SNR_REDUCED", format=f"{n}E", array=snr.reshape(1, n)),
        ]
    )
    spectrum_hdu = fits.BinTableHDU.from_columns(cols, name="SPECTRUM")

    buf = io.BytesIO()
    fits.HDUList([primary, spectrum_hdu]).writeto(buf)
    return buf.getvalue()


def _make_uves_fits_bytes_noisy_flux(*, n: int = 1000) -> bytes:
    """Build a synthetic UVES FITS with noisy flux (no SNR column) → DER_SNR fallback."""
    primary = fits.PrimaryHDU()
    hdr = primary.header
    hdr["INSTRUME"] = "UVES"
    hdr["TELESCOP"] = "ESO-VLT-U2"
    hdr["MJD-OBS"] = 56082.05
    hdr["DATE-OBS"] = "2012-06-04"
    hdr["EXPTIME"] = 7200.0
    hdr["SPEC_RES"] = 42000.0
    hdr["FLUXCAL"] = "ABSOLUTE"

    rng = np.random.default_rng(42)
    wave = np.linspace(3200.0, 10000.0, n).astype(np.float32)
    flux = (1.5e-16 + rng.normal(0, 1.0e-18, n)).astype(np.float32)
    err = np.ones(n, dtype=np.float32) * 1.0e-17
    qual = np.zeros(n, dtype=np.int32)

    cols = fits.ColDefs(
        [
            fits.Column(name="WAVE", format=f"{n}E", array=wave.reshape(1, n)),
            fits.Column(name="FLUX", format=f"{n}E", array=flux.reshape(1, n)),
            fits.Column(name="ERR", format=f"{n}E", array=err.reshape(1, n)),
            fits.Column(name="QUAL", format=f"{n}J", array=qual.reshape(1, n)),
        ]
    )
    spectrum_hdu = fits.BinTableHDU.from_columns(cols, name="SPECTRUM")

    buf = io.BytesIO()
    fits.HDUList([primary, spectrum_hdu]).writeto(buf)
    return buf.getvalue()


class TestSnrProvenanceValidationPath:
    """S21: snr_provenance flows through the validation path into DDB."""

    def test_source_snr_provenance(self, aws_resources: tuple[Any, Any]) -> None:
        """FITS with SNR_REDUCED column → snr_provenance='source' on DDB item."""
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes_with_snr_column()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)

            assert validation["snr"] is not None
            assert validation["snr"] > 0.0
            assert validation["snr_provenance"] == "source"

            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert "snr" in dp
        assert float(dp["snr"]) > 0.0
        assert dp["snr_provenance"] == "source"

    def test_estimated_der_snr_provenance(self, aws_resources: tuple[Any, Any]) -> None:
        """FITS with noisy flux, no SNR column → snr_provenance='estimated_der_snr'."""
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes_noisy_flux()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)

            assert validation["snr"] is not None
            assert validation["snr"] > 0.0
            assert validation["snr_provenance"] == "estimated_der_snr"

            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert "snr" in dp
        assert float(dp["snr"]) > 0.0
        assert dp["snr_provenance"] == "estimated_der_snr"

    def test_no_snr_when_constant_flux(self, aws_resources: tuple[Any, Any]) -> None:
        """Constant flux, no SNR column → DER_SNR returns 0 → snr absent from DDB."""
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes()  # constant flux, no SNR column

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)

            assert validation["snr"] is None
            assert validation["snr_provenance"] is None

            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert "snr" not in dp
        assert "snr_provenance" not in dp


# ---------------------------------------------------------------------------
# Path 2–4: CheckOperationalStatusOutcome skip paths
# ---------------------------------------------------------------------------


class TestOperationalStatusSkips:
    def test_already_validated_skip(self, aws_resources: tuple[Any, Any]) -> None:
        """
        DataProduct already has validation_status=VALID.
        CheckOperationalStatus returns already_validated=True → workflow
        routes to AlreadyValidated Pass state and FinalizeJobRunSuccess.
        AcquireArtifact is never reached.
        """
        table, _ = aws_resources

        with mock_aws():
            _seed_data_product(table, validation_status="VALID", eligibility="NONE")
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)

            assert status["already_validated"] is True
            # ASL routes to AlreadyValidated Pass → FinalizeJobRunSuccess
            _finalize_success(h, state)

        job_run = _get_job_run(table, state)
        assert job_run["status"] == "SUCCEEDED"

    def test_quarantine_blocked_skip(self, aws_resources: tuple[Any, Any]) -> None:
        """
        DataProduct is QUARANTINED without operator clearance.
        CheckOperationalStatus returns is_quarantined=True.
        """
        table, _ = aws_resources

        with mock_aws():
            _seed_data_product(
                table,
                validation_status="QUARANTINED",
                manual_review_status="PENDING_REVIEW",
            )
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)

            assert status["is_quarantined"] is True
            assert status["already_validated"] is False
            _finalize_success(h, state)

        job_run = _get_job_run(table, state)
        assert job_run["status"] == "SUCCEEDED"

    def test_cooldown_active_skip(self, aws_resources: tuple[Any, Any]) -> None:
        """
        next_eligible_attempt_at is in the future — backoff window is active.
        CheckOperationalStatus returns cooldown_active=True.
        """
        table, _ = aws_resources
        future_iso = (datetime.now(UTC) + timedelta(hours=2)).isoformat()

        with mock_aws():
            _seed_data_product(table, next_eligible_attempt_at=future_iso, attempt_count=1)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)

            assert status["cooldown_active"] is True
            assert status["already_validated"] is False
            _finalize_success(h, state)

        job_run = _get_job_run(table, state)
        assert job_run["status"] == "SUCCEEDED"


# ---------------------------------------------------------------------------
# Path 5: QUARANTINED outcome
# ---------------------------------------------------------------------------


class TestQuarantinedOutcome:
    def test_failed_fits_produces_quarantined_status(self, aws_resources: tuple[Any, Any]) -> None:
        """
        Synthetic FITS with non-monotonic wavelengths fails the strict-
        monotonicity sanity check. RecordValidationResult persists
        validation_status=QUARANTINED.
        """
        table, _ = aws_resources
        fits_bytes = _make_quarantine_fits_bytes()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)
            assert validation["validation_outcome"] == "QUARANTINED"
            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert dp["validation_status"] == "QUARANTINED"

    def test_quarantined_product_gsi1_keys_removed(self, aws_resources: tuple[Any, Any]) -> None:
        """
        RecordValidationResult removes GSI1PK/GSI1SK even for QUARANTINED
        outcomes — the product is no longer eligible for immediate re-acquisition.
        """
        table, _ = aws_resources
        fits_bytes = _make_quarantine_fits_bytes()

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)
            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert "GSI1PK" not in dp
        assert "GSI1SK" not in dp


# ---------------------------------------------------------------------------
# Path 6: Duplicate detection
# ---------------------------------------------------------------------------


class TestDuplicateDetection:
    def test_sha256_collision_routes_to_duplicate_linkage(
        self, aws_resources: tuple[Any, Any]
    ) -> None:
        """
        When ValidateBytes detects a sha256 collision with an existing VALID
        product for the same nova, is_duplicate=True → DuplicateByFingerprint
        routes to RecordDuplicateLinkage.
        """
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes()
        sha256 = hashlib.sha256(fits_bytes).hexdigest()

        with mock_aws():
            # Pre-seed an existing VALID product with the same sha256
            _seed_data_product(
                table,
                data_product_id=_EXISTING_DP_ID,
                validation_status="VALID",
                eligibility="NONE",
                sha256=sha256,
            )
            # Seed the product under test (UNVALIDATED, will produce same bytes)
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)

            assert validation["is_duplicate"] is True
            assert validation["duplicate_of_data_product_id"] == _EXISTING_DP_ID

            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert dp["acquisition_status"] == "SKIPPED_DUPLICATE"
        assert dp["duplicate_of_data_product_id"] == _EXISTING_DP_ID

    def test_duplicate_product_not_marked_valid(self, aws_resources: tuple[Any, Any]) -> None:
        """
        RecordDuplicateLinkage must NOT set validation_status=VALID on the
        duplicate — only the canonical product holds VALID status.
        """
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes()
        sha256 = hashlib.sha256(fits_bytes).hexdigest()

        with mock_aws():
            _seed_data_product(
                table,
                data_product_id=_EXISTING_DP_ID,
                validation_status="VALID",
                eligibility="NONE",
                sha256=sha256,
            )
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            acquisition = _run_acquire(h, state, status, fits_bytes)
            validation = _run_validate(h, state, status, acquisition)
            _run_record_result(h, state, status, acquisition, validation)
            _finalize_success(h, state)

        dp = _get_data_product(table)
        assert dp is not None
        assert dp["validation_status"] != "VALID"


# ---------------------------------------------------------------------------
# Path 7–8: HTTP failure modes
# ---------------------------------------------------------------------------


class TestHttpFailures:
    def test_http_5xx_raises_retryable_error(self, aws_resources: tuple[Any, Any]) -> None:
        """
        Provider returns HTTP 503 → AcquireArtifact raises RetryableError.
        The ASL Retry block will re-attempt up to MaxAttempts times.
        """
        from nova_common.errors import RetryableError

        table, _ = aws_resources

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)

            mock_resp = _make_http_response(b"", status_code=503)
            with (
                patch("requests.Session.get", return_value=mock_resp),
                pytest.raises(RetryableError),
            ):
                h["spectra_acquirer"].handle(
                    {
                        "task_name": "AcquireArtifact",
                        "nova_id": _NOVA_ID,
                        "provider": _PROVIDER,
                        "data_product_id": _DATA_PRODUCT_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "data_product": status["data_product"],
                    },
                    None,
                )

    def test_http_404_raises_value_error(self, aws_resources: tuple[Any, Any]) -> None:
        """
        Provider returns HTTP 404 → AcquireArtifact raises ValueError.
        The ASL Catch block routes to TerminalFailHandler.
        """
        table, _ = aws_resources

        with mock_aws():
            _seed_data_product(table)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)

            mock_resp = _make_http_response(b"", status_code=404)
            with patch("requests.Session.get", return_value=mock_resp), pytest.raises(ValueError):
                h["spectra_acquirer"].handle(
                    {
                        "task_name": "AcquireArtifact",
                        "nova_id": _NOVA_ID,
                        "provider": _PROVIDER,
                        "data_product_id": _DATA_PRODUCT_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "data_product": status["data_product"],
                    },
                    None,
                )

            # ASL routes to TerminalFailHandler → FinalizeJobRunFailed
            _finalize_failed(h, state, {"Error": "ValueError", "Cause": "HTTP 404"})

        job_run = _get_job_run(table, state)
        assert job_run["status"] == "FAILED"


# ---------------------------------------------------------------------------
# Path 9: attempt_count
# ---------------------------------------------------------------------------


class TestAttemptCount:
    def test_attempt_count_incremented_before_download(
        self, aws_resources: tuple[Any, Any]
    ) -> None:
        """
        AcquireArtifact increments attempt_count on the DataProduct BEFORE
        issuing the HTTP request, so a Lambda timeout or mid-download crash
        counts against the backoff schedule.
        """
        table, _ = aws_resources
        fits_bytes = _make_uves_fits_bytes()

        with mock_aws():
            _seed_data_product(table, attempt_count=0)
            h = _load_handlers()
            state = _run_prefix(h)
            status = _run_check_status(h, state)
            _run_acquire(h, state, status, fits_bytes)

        dp = _get_data_product(table)
        assert dp is not None
        assert int(dp["attempt_count"]) == 1
