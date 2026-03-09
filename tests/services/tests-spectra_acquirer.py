"""
tests/services/spectra_acquirer/test_handler.py

Unit tests for services/spectra_acquirer/handler.py

Uses moto to mock DynamoDB and S3, and unittest.mock.patch to intercept
requests.Session.get so no real HTTP calls are made.

Covers:
  AcquireArtifact (happy path):
    - downloads bytes, writes to S3, returns acquisition metadata
    - S3 key follows raw/{nova_id}/{provider}/{data_product_id}.fits pattern
    - sha256 and byte_length are correct
    - attempt_count is incremented before the download

  AcquireArtifact (locator resolution):
    - PRIMARY URL locator used when present
    - falls back to any URL locator when no PRIMARY exists
    - no URL locator → ValueError (terminal)

  AcquireArtifact (HTTP error handling):
    - HTTP 429 → RetryableError + FAILED_RETRYABLE persisted + backoff set
    - HTTP 500 → RetryableError + FAILED_RETRYABLE persisted + backoff set
    - HTTP 404 → ValueError (terminal) + TERMINAL_FAILURE persisted
    - HTTP 403 → ValueError (terminal)
    - requests.Timeout → RetryableError
    - requests.ConnectionError → RetryableError
    - mid-stream RequestException → RetryableError

  AcquireArtifact (backoff schedule):
    - attempt 1 → 60 s
    - attempt 2 → 300 s
    - attempt 3 → 3600 s
    - attempt 4+ → 86400 s (capped)

  Helpers:
    - _extract_primary_url: PRIMARY preferred over non-PRIMARY
    - _compute_backoff_seconds: schedule values and cap
    - _add_seconds: ISO-8601 arithmetic
    - _error_fingerprint: 12-char hex digest

  Dispatch:
    - unknown task_name → ValueError
    - missing task_name → ValueError
"""

from __future__ import annotations

import hashlib
import importlib
import sys
import types
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
import requests
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_BUCKET = "nova-cat-test-private"
_REGION = "us-east-1"

_NOVA_ID = "NOVA#v1324-sco"
_PROVIDER = "ESO"
_DPID = "dpid-acquirer-test-0001"
_SK = f"PRODUCT#SPECTRA#{_PROVIDER}#{_DPID}"
_DOWNLOAD_URL = "https://archive.eso.org/fits/test.fits"

_FAKE_FITS = b"SIMPLE  =                    T" + b"\x00" * 2850  # minimal FITS-shaped bytes

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", _BUCKET)
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-test-public")
    monkeypatch.setenv("NOVA_CAT_QUARANTINE_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def aws_resources(aws_env: None) -> Generator[tuple[Any, Any], None, None]:
    """Yield (table, s3_client) with mocked DynamoDB table and S3 bucket."""
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
        s3 = boto3.client("s3", region_name=_REGION)
        s3.create_bucket(Bucket=_BUCKET)
        yield table, s3


def _load_handler() -> types.ModuleType:
    """Reload so boto3 clients bind to the active mock_aws context."""
    for key in list(sys.modules):
        if key.startswith("spectra_acquirer"):
            del sys.modules[key]
    return importlib.import_module("spectra_acquirer.handler")


# ---------------------------------------------------------------------------
# HTTP mock helpers
# ---------------------------------------------------------------------------


def _mock_response(
    status_code: int = 200,
    content: bytes = _FAKE_FITS,
) -> MagicMock:
    """Build a mock requests.Response-like object."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    chunks = [content[i : i + 256 * 1024] for i in range(0, len(content), 256 * 1024)]
    mock_resp.iter_content = MagicMock(return_value=iter(chunks))
    return mock_resp


def _base_product_item() -> dict[str, Any]:
    return {
        "PK": _NOVA_ID,
        "SK": _SK,
        "data_product_id": _DPID,
        "provider": _PROVIDER,
        "validation_status": "UNVALIDATED",
        "eligibility": "ACQUIRE",
        "locators": [{"kind": "URL", "role": "PRIMARY", "value": _DOWNLOAD_URL}],
    }


def _base_event(locators: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "task_name": "AcquireArtifact",
        "nova_id": _NOVA_ID,
        "provider": _PROVIDER,
        "data_product_id": _DPID,
        "data_product": {
            "locators": locators
            if locators is not None
            else [{"kind": "URL", "role": "PRIMARY", "value": _DOWNLOAD_URL}],
        },
    }


# ---------------------------------------------------------------------------
# TestAcquireArtifactHappyPath
# ---------------------------------------------------------------------------


class TestAcquireArtifactHappyPath:
    def test_returns_acquisition_metadata(self, aws_resources: Any) -> None:
        table, s3 = aws_resources
        table.put_item(Item=_base_product_item())
        mock_resp = _mock_response(content=_FAKE_FITS)
        with mock_aws(), patch("requests.Session.get", return_value=mock_resp):
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
        assert result["raw_s3_bucket"] == _BUCKET
        assert "raw_s3_key" in result
        assert "sha256" in result
        assert "byte_length" in result
        assert "etag" in result

    def test_s3_key_follows_naming_convention(self, aws_resources: Any) -> None:
        table, s3 = aws_resources
        table.put_item(Item=_base_product_item())
        mock_resp = _mock_response(content=_FAKE_FITS)
        with mock_aws(), patch("requests.Session.get", return_value=mock_resp):
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
        expected_key = f"raw/{_NOVA_ID}/{_PROVIDER}/{_DPID}.fits"
        assert result["raw_s3_key"] == expected_key

    def test_sha256_is_correct(self, aws_resources: Any) -> None:
        table, s3 = aws_resources
        table.put_item(Item=_base_product_item())
        mock_resp = _mock_response(content=_FAKE_FITS)
        with mock_aws(), patch("requests.Session.get", return_value=mock_resp):
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
        expected = hashlib.sha256(_FAKE_FITS).hexdigest()
        assert result["sha256"] == expected

    def test_byte_length_is_correct(self, aws_resources: Any) -> None:
        table, s3 = aws_resources
        table.put_item(Item=_base_product_item())
        mock_resp = _mock_response(content=_FAKE_FITS)
        with mock_aws(), patch("requests.Session.get", return_value=mock_resp):
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
        assert result["byte_length"] == len(_FAKE_FITS)

    def test_bytes_written_to_s3(self, aws_resources: Any) -> None:
        table, s3 = aws_resources
        table.put_item(Item=_base_product_item())
        mock_resp = _mock_response(content=_FAKE_FITS)
        with mock_aws(), patch("requests.Session.get", return_value=mock_resp):
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            obj = s3.get_object(Bucket=_BUCKET, Key=result["raw_s3_key"])
            assert obj["Body"].read() == _FAKE_FITS

    def test_attempt_count_incremented(self, aws_resources: Any) -> None:
        table, s3 = aws_resources
        table.put_item(Item=_base_product_item())
        mock_resp = _mock_response(content=_FAKE_FITS)
        with mock_aws(), patch("requests.Session.get", return_value=mock_resp):
            handler = _load_handler()
            handler.handle(_base_event(), None)
        item = table.get_item(Key={"PK": _NOVA_ID, "SK": _SK}).get("Item", {})
        assert int(item["attempt_count"]) == 1

    def test_attempt_count_cumulative(self, aws_resources: Any) -> None:
        """Second successful call increments attempt_count to 2."""
        table, s3 = aws_resources
        table.put_item(Item=_base_product_item())
        mock_resp = _mock_response(content=_FAKE_FITS)
        with mock_aws(), patch("requests.Session.get", return_value=mock_resp):
            handler = _load_handler()
            handler.handle(_base_event(), None)
            handler.handle(_base_event(), None)
        item = table.get_item(Key={"PK": _NOVA_ID, "SK": _SK}).get("Item", {})
        assert int(item["attempt_count"]) == 2


# ---------------------------------------------------------------------------
# TestLocatorResolution
# ---------------------------------------------------------------------------


class TestLocatorResolution:
    def test_primary_url_locator_used(self, aws_resources: Any) -> None:
        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        locators = [
            {"kind": "URL", "role": "SECONDARY", "value": "https://mirror.example.com/test.fits"},
            {"kind": "URL", "role": "PRIMARY", "value": _DOWNLOAD_URL},
        ]
        captured: list[str] = []

        def _fake_get(url: str, **kwargs: Any) -> MagicMock:
            captured.append(url)
            return _mock_response(content=_FAKE_FITS)

        with mock_aws(), patch("requests.Session.get", side_effect=_fake_get):
            handler = _load_handler()
            handler.handle(_base_event(locators=locators), None)
        assert captured[0] == _DOWNLOAD_URL

    def test_fallback_to_non_primary_url(self, aws_resources: Any) -> None:
        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        fallback_url = "https://mirror.example.com/test.fits"
        locators = [{"kind": "URL", "role": "SECONDARY", "value": fallback_url}]
        captured: list[str] = []

        def _fake_get(url: str, **kwargs: Any) -> MagicMock:
            captured.append(url)
            return _mock_response(content=_FAKE_FITS)

        with mock_aws(), patch("requests.Session.get", side_effect=_fake_get):
            handler = _load_handler()
            handler.handle(_base_event(locators=locators), None)
        assert captured[0] == fallback_url

    def test_no_url_locator_raises_value_error(self, aws_resources: Any) -> None:
        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        locators: list[dict[str, Any]] = [{"kind": "DOI", "role": "PRIMARY", "value": "10.1/test"}]
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="No PRIMARY URL locator"):
                handler.handle(_base_event(locators=locators), None)

    def test_empty_locators_raises_value_error(self, aws_resources: Any) -> None:
        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="No PRIMARY URL locator"):
                handler.handle(_base_event(locators=[]), None)


# ---------------------------------------------------------------------------
# TestHttpErrorHandling
# ---------------------------------------------------------------------------


class TestHttpErrorHandling:
    def test_http_429_raises_retryable_error(self, aws_resources: Any) -> None:
        from nova_common.errors import RetryableError

        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with (
            mock_aws(),
            patch("requests.Session.get", return_value=_mock_response(status_code=429)),
        ):
            handler = _load_handler()
            with pytest.raises(RetryableError):
                handler.handle(_base_event(), None)

    def test_http_429_persists_failed_retryable_and_backoff(self, aws_resources: Any) -> None:
        from nova_common.errors import RetryableError

        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with (
            mock_aws(),
            patch("requests.Session.get", return_value=_mock_response(status_code=429)),
        ):
            handler = _load_handler()
            with pytest.raises(RetryableError):
                handler.handle(_base_event(), None)
        item = table.get_item(Key={"PK": _NOVA_ID, "SK": _SK}).get("Item", {})
        assert item["acquisition_status"] == "FAILED_RETRYABLE"
        assert item["last_attempt_outcome"] == "RETRYABLE_FAILURE"
        assert "next_eligible_attempt_at" in item
        assert "last_error_fingerprint" in item

    def test_http_500_raises_retryable_error(self, aws_resources: Any) -> None:
        from nova_common.errors import RetryableError

        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with (
            mock_aws(),
            patch("requests.Session.get", return_value=_mock_response(status_code=500)),
        ):
            handler = _load_handler()
            with pytest.raises(RetryableError):
                handler.handle(_base_event(), None)

    def test_http_404_raises_value_error(self, aws_resources: Any) -> None:
        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with (
            mock_aws(),
            patch("requests.Session.get", return_value=_mock_response(status_code=404)),
        ):
            handler = _load_handler()
            with pytest.raises(ValueError, match="Terminal download failure"):
                handler.handle(_base_event(), None)

    def test_http_404_persists_terminal_failure(self, aws_resources: Any) -> None:
        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with (
            mock_aws(),
            patch("requests.Session.get", return_value=_mock_response(status_code=404)),
        ):
            handler = _load_handler()
            with pytest.raises(ValueError):
                handler.handle(_base_event(), None)
        item = table.get_item(Key={"PK": _NOVA_ID, "SK": _SK}).get("Item", {})
        assert item["last_attempt_outcome"] == "TERMINAL_FAILURE"
        # Scientific state is still FAILED_RETRYABLE — terminal applies only to this attempt
        assert item["acquisition_status"] == "FAILED_RETRYABLE"

    def test_http_403_raises_value_error(self, aws_resources: Any) -> None:
        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with (
            mock_aws(),
            patch("requests.Session.get", return_value=_mock_response(status_code=403)),
        ):
            handler = _load_handler()
            with pytest.raises(ValueError):
                handler.handle(_base_event(), None)

    def test_requests_timeout_raises_retryable_error(self, aws_resources: Any) -> None:
        from nova_common.errors import RetryableError

        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with mock_aws(), patch("requests.Session.get", side_effect=requests.Timeout("timed out")):
            handler = _load_handler()
            with pytest.raises(RetryableError):
                handler.handle(_base_event(), None)

    def test_connection_error_raises_retryable_error(self, aws_resources: Any) -> None:
        from nova_common.errors import RetryableError

        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with (
            mock_aws(),
            patch(
                "requests.Session.get", side_effect=requests.ConnectionError("connection refused")
            ),
        ):
            handler = _load_handler()
            with pytest.raises(RetryableError):
                handler.handle(_base_event(), None)

    def test_mid_stream_exception_raises_retryable_error(self, aws_resources: Any) -> None:
        from nova_common.errors import RetryableError

        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.iter_content = MagicMock(
            side_effect=requests.RequestException("stream interrupted")
        )
        with mock_aws(), patch("requests.Session.get", return_value=mock_resp):
            handler = _load_handler()
            with pytest.raises(RetryableError):
                handler.handle(_base_event(), None)


# ---------------------------------------------------------------------------
# TestBackoffSchedule
# ---------------------------------------------------------------------------


class TestBackoffSchedule:
    """Tests for _compute_backoff_seconds — does not need AWS resources."""

    @pytest.fixture
    def handler(self, aws_resources: Any) -> types.ModuleType:
        with mock_aws():
            return _load_handler()

    def test_attempt_1_backoff(self, handler: types.ModuleType) -> None:
        assert handler._compute_backoff_seconds(1) == 60

    def test_attempt_2_backoff(self, handler: types.ModuleType) -> None:
        assert handler._compute_backoff_seconds(2) == 300

    def test_attempt_3_backoff(self, handler: types.ModuleType) -> None:
        assert handler._compute_backoff_seconds(3) == 3_600

    def test_attempt_4_backoff(self, handler: types.ModuleType) -> None:
        assert handler._compute_backoff_seconds(4) == 86_400

    def test_attempt_10_capped_at_max(self, handler: types.ModuleType) -> None:
        assert handler._compute_backoff_seconds(10) == 86_400

    def test_retryable_failure_sets_backoff_for_attempt_1(self, aws_resources: Any) -> None:
        """First retryable failure: next_eligible_attempt_at is ~60 s in the future."""
        from nova_common.errors import RetryableError

        table, _ = aws_resources
        table.put_item(Item=_base_product_item())
        with (
            mock_aws(),
            patch("requests.Session.get", return_value=_mock_response(status_code=429)),
        ):
            handler = _load_handler()
            with pytest.raises(RetryableError):
                handler.handle(_base_event(), None)
        item = table.get_item(Key={"PK": _NOVA_ID, "SK": _SK}).get("Item", {})
        # next_eligible_attempt_at should be ahead of last_attempt_at
        assert item["next_eligible_attempt_at"] > item["last_attempt_at"]


# ---------------------------------------------------------------------------
# TestHelpers
# ---------------------------------------------------------------------------


class TestHelpers:
    @pytest.fixture
    def handler(self, aws_resources: Any) -> types.ModuleType:
        with mock_aws():
            return _load_handler()

    def test_extract_primary_url_prefers_primary(self, handler: types.ModuleType) -> None:
        locators = [
            {"kind": "URL", "role": "SECONDARY", "value": "https://mirror.example.com/"},
            {"kind": "URL", "role": "PRIMARY", "value": "https://primary.example.com/"},
        ]
        assert handler._extract_primary_url(locators) == "https://primary.example.com/"

    def test_extract_primary_url_falls_back_to_any(self, handler: types.ModuleType) -> None:
        locators = [{"kind": "URL", "role": "SECONDARY", "value": "https://fallback.example.com/"}]
        assert handler._extract_primary_url(locators) == "https://fallback.example.com/"

    def test_extract_primary_url_ignores_non_url_kinds(self, handler: types.ModuleType) -> None:
        locators = [{"kind": "DOI", "role": "PRIMARY", "value": "10.1/foo"}]
        assert handler._extract_primary_url(locators) is None

    def test_extract_primary_url_empty_list(self, handler: types.ModuleType) -> None:
        assert handler._extract_primary_url([]) is None

    def test_compute_backoff_schedule(self, handler: types.ModuleType) -> None:
        expected = [60, 300, 3_600, 86_400]
        for i, exp in enumerate(expected, start=1):
            assert handler._compute_backoff_seconds(i) == exp

    def test_error_fingerprint_is_12_chars(self, handler: types.ModuleType) -> None:
        fp = handler._error_fingerprint("some error message")
        assert len(fp) == 12
        assert fp.isalnum()

    def test_error_fingerprint_is_deterministic(self, handler: types.ModuleType) -> None:
        msg = "HTTP 429 — throttling"
        assert handler._error_fingerprint(msg) == handler._error_fingerprint(msg)

    def test_add_seconds_arithmetic(self, handler: types.ModuleType) -> None:
        result = handler._add_seconds("2024-01-01T00:00:00Z", 3600)
        assert result == "2024-01-01T01:00:00Z"

    def test_add_seconds_day_boundary(self, handler: types.ModuleType) -> None:
        result = handler._add_seconds("2024-01-01T23:30:00Z", 3600)
        assert result == "2024-01-02T00:30:00Z"


# ---------------------------------------------------------------------------
# TestDispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises_value_error(self, aws_resources: Any) -> None:
        _, _ = aws_resources
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)

    def test_missing_task_name_raises_value_error(self, aws_resources: Any) -> None:
        _, _ = aws_resources
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Missing required field"):
                handler.handle({}, None)
