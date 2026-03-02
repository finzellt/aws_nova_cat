"""
Unit tests for services/job_run_manager/handler.py

Uses moto to mock DynamoDB — no real AWS calls are made.

Covers:
  - BeginJobRun: generates job_run_id and correlation_id
  - BeginJobRun: uses caller-supplied correlation_id when present
  - BeginJobRun: writes correct DynamoDB item
  - FinalizeJobRunSuccess: updates status and outcome
  - FinalizeJobRunFailed: updates status and captures error fields
  - FinalizeJobRunQuarantined: updates status to QUARANTINED
  - Unknown task_name raises ValueError
"""

from __future__ import annotations

import importlib
import sys
import types
from collections.abc import Generator
from typing import Any

import boto3
import pytest
from moto import mock_aws

# ---------------------------------------------------------------------------
# Test fixtures and helpers
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required environment variables before handler import."""
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def table(aws_env: None) -> Generator[Any, None, None]:
    """Create a mocked DynamoDB table and return it."""
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
        yield tbl


def _load_handler() -> types.ModuleType:
    """Import handler fresh inside the moto context."""
    if "job_run_manager.handler" in sys.modules:
        del sys.modules["job_run_manager.handler"]
    return importlib.import_module("job_run_manager.handler")


def _base_event(**kwargs: Any) -> dict[str, Any]:  # type: ignore[return]
    return {
        "task_name": "BeginJobRun",
        "workflow_name": "initialize_nova",
        "candidate_name": "V1324 Sco",
        "correlation_id": "corr-001",
        **kwargs,
    }


def _finalize_event(task_name: str, job_run: dict[str, Any], **kwargs: Any) -> dict[str, Any]:  # type: ignore[return]
    return {
        "task_name": task_name,
        "workflow_name": "initialize_nova",
        "job_run": job_run,
        **kwargs,
    }


# ---------------------------------------------------------------------------
# BeginJobRun
# ---------------------------------------------------------------------------


class TestBeginJobRun:
    def test_returns_job_run_id(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            assert "job_run_id" in result
            assert len(result["job_run_id"]) == 36  # UUID format

    def test_uses_supplied_correlation_id(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(correlation_id="my-corr-id"), None)
            assert result["correlation_id"] == "my-corr-id"

    def test_generates_correlation_id_when_absent(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            event = _base_event()
            del event["correlation_id"]
            result = handler.handle(event, None)
            assert "correlation_id" in result
            assert len(result["correlation_id"]) == 36

    def test_writes_dynamodb_item(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            item = table.get_item(Key={"PK": result["pk"], "SK": result["sk"]}).get("Item")
            assert item is not None
            assert item["status"] == "RUNNING"
            assert item["workflow_name"] == "initialize_nova"
            assert item["candidate_name"] == "V1324 Sco"
            assert item["job_run_id"] == result["job_run_id"]

    def test_pk_uses_workflow_correlation_prefix(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(correlation_id="corr-abc"), None)
            assert result["pk"] == "WORKFLOW#corr-abc"


# ---------------------------------------------------------------------------
# FinalizeJobRunSuccess
# ---------------------------------------------------------------------------


class TestFinalizeJobRunSuccess:
    def _setup_job_run(self, table: Any, handler: Any) -> dict[str, Any]:
        return handler.handle(_base_event(), None)  # type: ignore[no-any-return]

    def test_updates_status_to_succeeded(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            job_run = self._setup_job_run(table, handler)
            handler.handle(
                _finalize_event("FinalizeJobRunSuccess", job_run, outcome="CREATED_AND_LAUNCHED"),
                None,
            )
            item = table.get_item(Key={"PK": job_run["pk"], "SK": job_run["sk"]}).get("Item")
            assert item["status"] == "SUCCEEDED"
            assert item["outcome"] == "CREATED_AND_LAUNCHED"

    @pytest.mark.parametrize(
        "outcome",
        [
            "CREATED_AND_LAUNCHED",
            "EXISTS_AND_LAUNCHED",
            "NOT_FOUND",
            "NOT_A_CLASSICAL_NOVA",
        ],
    )
    def test_all_valid_outcomes(self, table: Any, outcome: str) -> None:
        with mock_aws():
            handler = _load_handler()
            job_run = self._setup_job_run(table, handler)
            result = handler.handle(
                _finalize_event("FinalizeJobRunSuccess", job_run, outcome=outcome), None
            )
            assert result["outcome"] == outcome


# ---------------------------------------------------------------------------
# FinalizeJobRunFailed
# ---------------------------------------------------------------------------


class TestFinalizeJobRunFailed:
    def test_updates_status_to_failed(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            job_run = handler.handle(_base_event(), None)
            handler.handle(
                _finalize_event(
                    "FinalizeJobRunFailed",
                    job_run,
                    error={"Error": "TerminalError", "Cause": "Missing field"},
                ),
                None,
            )
            item = table.get_item(Key={"PK": job_run["pk"], "SK": job_run["sk"]}).get("Item")
            assert item["status"] == "FAILED"
            assert item["error_type"] == "TerminalError"
            assert item["error_message"] == "Missing field"

    def test_handles_missing_error_field(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            job_run = handler.handle(_base_event(), None)
            result = handler.handle(_finalize_event("FinalizeJobRunFailed", job_run), None)
            assert result["status"] == "FAILED"


# ---------------------------------------------------------------------------
# FinalizeJobRunQuarantined
# ---------------------------------------------------------------------------


class TestFinalizeJobRunQuarantined:
    def test_updates_status_to_quarantined(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            job_run = handler.handle(_base_event(), None)
            handler.handle(_finalize_event("FinalizeJobRunQuarantined", job_run), None)
            item = table.get_item(Key={"PK": job_run["pk"], "SK": job_run["sk"]}).get("Item")
            assert item["status"] == "QUARANTINED"


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)
