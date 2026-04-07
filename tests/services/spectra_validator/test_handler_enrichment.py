"""
tests/services/spectra_validator/test_handler_enrichment.py

Tests for wavelength_min_nm, wavelength_max_nm, and snr enrichment fields
flowing through ValidateBytes and RecordValidationResult.

Uses moto to mock DynamoDB and S3 — no real AWS calls.
Synthetic FITS files are built in-memory with astropy.
"""

from __future__ import annotations

import hashlib
import importlib
import io
import os
import pathlib
import sys
import types
from collections.abc import Generator
from decimal import Decimal
from typing import Any

import boto3
import numpy as np
import pytest
from moto import mock_aws


# Bootstrap astropy cache dirs before import
def _bootstrap() -> None:
    base = "/tmp/test_astropy"
    os.environ.setdefault("ASTROPY_CONFIGDIR", f"{base}/config")
    os.environ.setdefault("ASTROPY_CACHE_DIR", f"{base}/cache")
    os.environ.setdefault("XDG_CACHE_HOME", f"{base}/.cache")
    os.environ.setdefault("HOME", base)
    for p in (os.environ["ASTROPY_CONFIGDIR"], os.environ["ASTROPY_CACHE_DIR"]):
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)


_bootstrap()

import astropy.io.fits as fits  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_BUCKET_NAME = "nova-cat-private-test"
_REGION = "us-east-1"
_NOVA_ID = "test-nova-enrich-0001"
_PROVIDER = "ESO"
_DATA_PRODUCT_ID = "test-dp-enrich-0001"
_N = 200

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", _BUCKET_NAME)
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-public-test")
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:quarantine"
    )
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


def _load_handler() -> types.ModuleType:
    """Import spectra_validator.handler fresh to pick up env vars."""
    for mod_key in list(sys.modules):
        if mod_key.startswith("spectra_validator"):
            del sys.modules[mod_key]
    return importlib.import_module("spectra_validator.handler")


def _make_uves_fits_bytes(
    wave_angstrom: np.ndarray | None = None,
    flux: np.ndarray | None = None,
    include_snr: bool = False,
    snr_data: np.ndarray | None = None,
) -> bytes:
    """Build synthetic UVES FITS bytes in memory."""
    if wave_angstrom is None:
        wave_angstrom = np.linspace(5656.0, 9464.0, _N)
    if flux is None:
        rng = np.random.default_rng(42)
        flux = rng.uniform(0.1, 2.0, _N)

    n = len(wave_angstrom)

    header = fits.Header()
    header["SIMPLE"] = True
    header["BITPIX"] = 16
    header["NAXIS"] = 0
    header["INSTRUME"] = "UVES"
    header["TELESCOP"] = "ESO-VLT-U2"
    header["MJD-OBS"] = 56082.05467768
    header["DATE-OBS"] = "2012-06-04"
    header["RA"] = 267.725219
    header["DEC"] = -32.62309
    header["EXPTIME"] = 7199.9979
    header["SPEC_RES"] = 42310.0
    header["FLUXCAL"] = "ABSOLUTE"
    header["ORIGIN"] = "ESO"

    cols = [
        fits.Column(
            name="WAVE", format=f"{n}D", unit="angstrom", array=wave_angstrom.reshape(1, n)
        ),
        fits.Column(
            name="FLUX",
            format=f"{n}D",
            unit="10**(-16)erg.cm**(-2).s**(-1).angstrom**(-1)",
            array=flux.reshape(1, n),
        ),
        fits.Column(name="ERR", format=f"{n}D", unit="", array=(flux * 0.05).reshape(1, n)),
    ]

    if include_snr:
        if snr_data is None:
            snr_data = np.linspace(10.0, 50.0, n)
        cols.append(fits.Column(name="SNR", format=f"{n}D", unit="", array=snr_data.reshape(1, n)))

    spectrum_hdu = fits.BinTableHDU.from_columns(cols)
    spectrum_hdu.name = "SPECTRUM"
    primary = fits.PrimaryHDU(header=header)
    hdulist = fits.HDUList([primary, spectrum_hdu])

    buf = io.BytesIO()
    hdulist.writeto(buf, overwrite=True)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Tests: ValidateBytes returns wavelength range and SNR
# ---------------------------------------------------------------------------


class TestValidateBytesWavelengthAndSnr:
    @pytest.fixture
    def _moto_infra(self, aws_env: None) -> Generator[dict[str, Any], None, None]:
        with mock_aws():
            s3 = boto3.client("s3", region_name=_REGION)
            s3.create_bucket(Bucket=_BUCKET_NAME)

            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
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

            yield {"s3": s3, "table": table}

    def _run_validate_bytes(self, infra: dict[str, Any], fits_bytes: bytes) -> dict[str, Any]:
        s3 = infra["s3"]
        raw_key = f"raw/spectra/{_NOVA_ID}/{_DATA_PRODUCT_ID}/raw.fits"
        s3.put_object(Bucket=_BUCKET_NAME, Key=raw_key, Body=fits_bytes)
        sha256 = hashlib.sha256(fits_bytes).hexdigest()

        handler_mod = _load_handler()

        event = {
            "task_name": "ValidateBytes",
            "nova_id": _NOVA_ID,
            "provider": _PROVIDER,
            "data_product_id": _DATA_PRODUCT_ID,
            "data_product": {"hints": {}},
            "acquisition": {
                "raw_s3_bucket": _BUCKET_NAME,
                "raw_s3_key": raw_key,
                "sha256": sha256,
            },
        }
        result: dict[str, Any] = handler_mod.handle(event, None)
        return result

    def test_validate_bytes_returns_wavelength_range_nm(self, _moto_infra: dict[str, Any]) -> None:
        """Wavelength range extracted from angstrom WAVE column, converted to nm."""
        wave = np.linspace(5656.0, 9464.0, _N)
        fits_bytes = _make_uves_fits_bytes(wave_angstrom=wave)
        result = self._run_validate_bytes(_moto_infra, fits_bytes)

        assert result["validation_outcome"] == "VALID"
        assert result["wavelength_min_nm"] is not None
        assert result["wavelength_max_nm"] is not None
        # 5656 Å = 565.6 nm, 9464 Å = 946.4 nm
        assert abs(result["wavelength_min_nm"] - 565.6) < 0.1
        assert abs(result["wavelength_max_nm"] - 946.4) < 0.1

    def test_validate_bytes_returns_snr_when_present(self, _moto_infra: dict[str, Any]) -> None:
        """SNR column in FITS → snr field in return dict with median value."""
        snr_data = np.linspace(10.0, 50.0, _N)
        expected_median = float(np.median(snr_data))
        fits_bytes = _make_uves_fits_bytes(include_snr=True, snr_data=snr_data)
        result = self._run_validate_bytes(_moto_infra, fits_bytes)

        assert result["validation_outcome"] == "VALID"
        assert result["snr"] is not None
        assert abs(result["snr"] - expected_median) < 0.01

    def test_validate_bytes_returns_snr_none_when_absent(self, _moto_infra: dict[str, Any]) -> None:
        """No SNR column → snr is None."""
        fits_bytes = _make_uves_fits_bytes(include_snr=False)
        result = self._run_validate_bytes(_moto_infra, fits_bytes)

        assert result["validation_outcome"] == "VALID"
        assert result["snr"] is None


# ---------------------------------------------------------------------------
# Tests: RecordValidationResult persists wavelength and SNR to DDB
# ---------------------------------------------------------------------------


class TestRecordValidationResultPersistsEnrichment:
    @pytest.fixture
    def _moto_infra(self, aws_env: None) -> Generator[dict[str, Any], None, None]:
        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
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

            sk = f"PRODUCT#SPECTRA#{_PROVIDER}#{_DATA_PRODUCT_ID}"
            table.put_item(
                Item={
                    "PK": _NOVA_ID,
                    "SK": sk,
                    "data_product_id": _DATA_PRODUCT_ID,
                    "validation_status": "UNVALIDATED",
                    "GSI1PK": "ELIGIBLE",
                    "GSI1SK": "2024-01-01T00:00:00Z",
                }
            )

            yield {"table": table}

    def test_record_validation_result_persists_wavelength_and_snr(
        self, _moto_infra: dict[str, Any]
    ) -> None:
        table = _moto_infra["table"]
        handler_mod = _load_handler()

        event = {
            "task_name": "RecordValidationResult",
            "nova_id": _NOVA_ID,
            "provider": _PROVIDER,
            "data_product_id": _DATA_PRODUCT_ID,
            "acquisition": {
                "sha256": "abc123",
                "byte_length": 1024,
                "etag": "etag-test",
                "raw_s3_bucket": _BUCKET_NAME,
                "raw_s3_key": "raw/test.fits",
            },
            "validation": {
                "validation_outcome": "VALID",
                "fits_profile_id": "ESO_UVES",
                "header_signature_hash": "abcd1234abcd1234",
                "normalization_notes": [],
                "quarantine_reason_code": None,
                "quarantine_reason": None,
                "profile_selection_inputs": {},
                "instrument": "UVES",
                "telescope": "ESO-VLT-U2",
                "observation_date_mjd": 56082.05467768,
                "flux_unit": "10**(-16)erg.cm**(-2).s**(-1).angstrom**(-1)",
                "wavelength_min_nm": 565.6,
                "wavelength_max_nm": 946.4,
                "snr": 30.0,
            },
        }
        handler_mod.handle(event, None)

        sk = f"PRODUCT#SPECTRA#{_PROVIDER}#{_DATA_PRODUCT_ID}"
        item = table.get_item(Key={"PK": _NOVA_ID, "SK": sk})["Item"]

        assert item["wavelength_min_nm"] == Decimal("565.6")
        assert item["wavelength_max_nm"] == Decimal("946.4")
        assert item["snr"] == Decimal("30.0")
