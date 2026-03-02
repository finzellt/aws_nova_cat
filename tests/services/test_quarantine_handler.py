"""
Unit tests for services/quarantine_handler/handler.py

Uses moto to mock DynamoDB and SNS — no real AWS calls are made.

Covers:
  - QuarantineHandler: persists quarantine fields onto existing JobRun record
  - QuarantineHandler: captures extra_context fields (e.g. min_sep_arcsec)
  - QuarantineHandler: publishes SNS notification
  - QuarantineHandler: SNS failure does not raise (best-effort)
  - QuarantineHandler: returns expected fields
  - QuarantineHandler: uses candidate_name as primary_id when nova_id absent
  - QuarantineHandler: uses nova_id as primary_id when present
  - Unknown task_name raises ValueError
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_TOPIC_NAME = "nova-cat-quarantine-test"
_REGION = "us-east-1"

_FAKE_PK = "WORKFLOW#corr-001"
_FAKE_SK = "JOBRUN#initialize_nova#2026-01-01T00:00:00Z#job-001"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required environment variables before handler import."""
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", f"arn:aws:sns:{_REGION}:123456789012:{_TOPIC_NAME}"
    )
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def table(aws_env: None) -> Generator[Any, None, None]:
    """Create a mocked DynamoDB table with a pre-existing JobRun item."""
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
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        # Pre-seed a JobRun item so update_item has a target
        tbl.put_item(
            Item={
                "PK": _FAKE_PK,
                "SK": _FAKE_SK,
                "entity_type": "JobRun",
                "status": "RUNNING",
                "workflow_name": "initialize_nova",
                "job_run_id": "job-001",
                "correlation_id": "corr-001",
            }
        )
        yield tbl


@pytest.fixture
def topic(aws_env: None) -> Generator[Any, None, None]:
    """Create a mocked SNS topic."""
    with mock_aws():
        sns = boto3.client("sns", region_name=_REGION)
        sns.create_topic(Name=_TOPIC_NAME)
        yield sns


def _load_handler() -> types.ModuleType:
    """Import handler fresh inside the moto context."""
    if "quarantine_handler.handler" in sys.modules:
        del sys.modules["quarantine_handler.handler"]
    return importlib.import_module("quarantine_handler.handler")


def _base_event(**kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "QuarantineHandler",
        "workflow_name": "initialize_nova",
        "quarantine_reason_code": "COORDINATE_AMBIGUITY",
        "candidate_name": "V1324 Sco",
        "correlation_id": "corr-001",
        "job_run_id": "job-001",
        "job_run": {
            "pk": _FAKE_PK,
            "sk": _FAKE_SK,
            "job_run_id": "job-001",
            "correlation_id": "corr-001",
        },
        **kwargs,
    }


# ---------------------------------------------------------------------------
# QuarantineHandler — DynamoDB persistence
# ---------------------------------------------------------------------------


class TestQuarantineHandlerPersistence:
    def test_writes_quarantine_reason_code(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(), None)
            item = table.get_item(Key={"PK": _FAKE_PK, "SK": _FAKE_SK}).get("Item")
            assert item["quarantine_reason_code"] == "COORDINATE_AMBIGUITY"

    def test_writes_error_fingerprint(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(), None)
            item = table.get_item(Key={"PK": _FAKE_PK, "SK": _FAKE_SK}).get("Item")
            assert "error_fingerprint" in item
            assert len(item["error_fingerprint"]) == 12  # truncated SHA-256

    def test_writes_classification_reason(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(), None)
            item = table.get_item(Key={"PK": _FAKE_PK, "SK": _FAKE_SK}).get("Item")
            assert "classification_reason" in item
            assert len(item["classification_reason"]) > 0

    def test_writes_quarantined_at(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(), None)
            item = table.get_item(Key={"PK": _FAKE_PK, "SK": _FAKE_SK}).get("Item")
            assert "quarantined_at" in item

    def test_captures_extra_context_min_sep_arcsec(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(min_sep_arcsec=5.3), None)
            item = table.get_item(Key={"PK": _FAKE_PK, "SK": _FAKE_SK}).get("Item")
            from decimal import Decimal

            assert item["extra_context"] == {"min_sep_arcsec": Decimal("5.3")}

    def test_no_extra_context_when_absent(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(), None)
            item = table.get_item(Key={"PK": _FAKE_PK, "SK": _FAKE_SK}).get("Item")
            assert "extra_context" not in item

    def test_unknown_reason_code_uses_fallback_classification(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(quarantine_reason_code="UNKNOWN_CODE"), None)
            item = table.get_item(Key={"PK": _FAKE_PK, "SK": _FAKE_SK}).get("Item")
            assert "error_fingerprint" in item
            assert len(item["classification_reason"]) > 0


# ---------------------------------------------------------------------------
# QuarantineHandler — return value
# ---------------------------------------------------------------------------


class TestQuarantineHandlerReturnValue:
    def test_returns_quarantine_reason_code(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            assert result["quarantine_reason_code"] == "COORDINATE_AMBIGUITY"

    def test_returns_error_fingerprint(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            assert "error_fingerprint" in result
            assert len(result["error_fingerprint"]) == 12

    def test_returns_quarantined_at(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            assert "quarantined_at" in result

    def test_error_fingerprint_is_stable(self, table: Any, topic: Any) -> None:
        """Same inputs should always produce the same fingerprint."""
        with mock_aws():
            handler = _load_handler()
            r1 = handler.handle(_base_event(), None)
            r2 = handler.handle(
                _base_event(quarantine_reason_code="COORDINATE_AMBIGUITY"),
                None,
            )
            assert r1["error_fingerprint"] == r2["error_fingerprint"]


# ---------------------------------------------------------------------------
# QuarantineHandler — SNS
# ---------------------------------------------------------------------------


class TestQuarantineHandlerSns:
    def test_sns_failure_does_not_raise(self, table: Any, topic: Any) -> None:
        """SNS publish errors must be swallowed — best-effort only."""
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sns") as mock_sns:
                mock_sns.publish.side_effect = Exception("SNS unavailable")
                # Should not raise
                result = handler.handle(_base_event(), None)
            assert "error_fingerprint" in result

    def test_uses_candidate_name_as_primary_id_when_no_nova_id(
        self, table: Any, topic: Any
    ) -> None:
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sns") as mock_sns:
                handler.handle(_base_event(), None)
                _, kwargs = mock_sns.publish.call_args
                payload = json.loads(kwargs["Message"])
            assert payload["primary_id"] == "V1324 Sco"

    def test_uses_nova_id_as_primary_id_when_present(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sns") as mock_sns:
                handler.handle(_base_event(nova_id="nova-uuid-001"), None)
                _, kwargs = mock_sns.publish.call_args
                payload = json.loads(kwargs["Message"])
            assert payload["primary_id"] == "nova-uuid-001"

    def test_sns_payload_contains_required_fields(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sns") as mock_sns:
                handler.handle(_base_event(), None)
                _, kwargs = mock_sns.publish.call_args
                payload = json.loads(kwargs["Message"])
            assert "workflow_name" in payload
            assert "primary_id" in payload
            assert "correlation_id" in payload
            assert "error_fingerprint" in payload
            assert "quarantine_reason_code" in payload
            assert "classification_reason" in payload


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, table: Any, topic: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)
