"""
Unit tests for services/idempotency_guard/handler.py

Uses moto to mock DynamoDB — no real AWS calls are made.

Covers:
  - AcquireIdempotencyLock: acquires lock and writes DynamoDB item
  - AcquireIdempotencyLock: uses primary_id in idempotency key
  - AcquireIdempotencyLock: raises RetryableError if lock already held
  - AcquireIdempotencyLock: key is stable within the same hour
  - AcquireIdempotencyLock: works with nova_id as primary_id (ingest_new_nova pattern)
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

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def table(aws_env: None) -> Generator[Any, None, None]:
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
    if "idempotency_guard.handler" in sys.modules:
        del sys.modules["idempotency_guard.handler"]
    return importlib.import_module("idempotency_guard.handler")


def _base_event(**kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "AcquireIdempotencyLock",
        "workflow_name": "initialize_nova",
        "primary_id": "v1324 sco",
        "job_run_id": "job-001",
        "correlation_id": "corr-001",
        **kwargs,
    }


# ---------------------------------------------------------------------------
# AcquireIdempotencyLock
# ---------------------------------------------------------------------------


class TestAcquireIdempotencyLock:
    def test_acquires_lock_and_returns_key(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            assert "idempotency_key" in result
            assert "acquired_at" in result

    def test_key_contains_workflow_name_and_primary_id(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            key = result["idempotency_key"]
            assert "initialize_nova" in key
            assert "v1324 sco" in key

    def test_writes_dynamodb_item(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            pk = f"IDEMPOTENCY#{result['idempotency_key']}"
            item = table.get_item(Key={"PK": pk, "SK": "LOCK"}).get("Item")
            assert item is not None
            assert item["workflow_name"] == "initialize_nova"
            assert item["primary_id"] == "v1324 sco"
            assert item["job_run_id"] == "job-001"
            assert "ttl" in item

    def test_lock_already_held_raises_retryable_error(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            # First call acquires the lock
            handler.handle(_base_event(), None)
            # Second call within same hour — same key — should raise
            from nova_common.errors import RetryableError

            with pytest.raises(RetryableError, match="already held"):
                handler.handle(_base_event(), None)

    def test_key_is_stable_for_same_inputs(self, table: Any) -> None:
        """Same workflow + primary_id should produce the same key within an hour."""
        with mock_aws():
            handler = _load_handler()
            k1 = handler._compute_key("initialize_nova", "v1324 sco")
            k2 = handler._compute_key("initialize_nova", "v1324 sco")
            assert k1 == k2

    def test_different_primary_ids_produce_different_keys(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            k1 = handler._compute_key("initialize_nova", "v1324 sco")
            k2 = handler._compute_key("initialize_nova", "rs oph")
            assert k1 != k2

    def test_works_with_nova_id_as_primary_id(self, table: Any) -> None:
        """Verify ingest_new_nova pattern: nova_id as primary_id."""
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(
                _base_event(
                    workflow_name="ingest_new_nova",
                    primary_id="4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
                ),
                None,
            )
            assert "ingest_new_nova" in result["idempotency_key"]
            assert "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1" in result["idempotency_key"]

    def test_different_workflows_produce_different_keys(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            k1 = handler._compute_key("initialize_nova", "v1324 sco")
            k2 = handler._compute_key("ingest_new_nova", "v1324 sco")
            assert k1 != k2


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)
