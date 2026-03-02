"""
Unit tests for services/idempotency_guard/handler.py

Uses moto to mock DynamoDB — no real AWS calls are made.

Covers:
  - AcquireIdempotencyLock writes lock item with correct fields
  - AcquireIdempotencyLock raises RetryableError when lock already held
  - Idempotency key format includes workflow_name, candidate name, and time bucket
  - TTL is set to a future timestamp
  - Unknown task_name raises ValueError
"""

from __future__ import annotations

import importlib
import sys
import time
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


def _lock_event(**kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "AcquireIdempotencyLock",
        "workflow_name": "initialize_nova",
        "normalized_candidate_name": "v1324 sco",
        "job_run_id": "job-run-001",
        "correlation_id": "corr-001",
        **kwargs,
    }


class TestAcquireIdempotencyLock:
    def test_writes_lock_item(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_lock_event(), None)
            pk = f"IDEMPOTENCY#{result['idempotency_key']}"
            item = table.get_item(Key={"PK": pk, "SK": "LOCK"}).get("Item")
            assert item is not None
            assert item["job_run_id"] == "job-run-001"
            assert item["workflow_name"] == "initialize_nova"
            assert item["entity_type"] == "IdempotencyLock"

    def test_returns_idempotency_key_and_acquired_at(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_lock_event(), None)
            assert "idempotency_key" in result
            assert "acquired_at" in result

    def test_key_contains_workflow_and_candidate(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_lock_event(), None)
            key = result["idempotency_key"]
            assert "initialize_nova" in key
            assert "v1324 sco" in key

    def test_ttl_is_in_the_future(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_lock_event(), None)
            pk = f"IDEMPOTENCY#{result['idempotency_key']}"
            item = table.get_item(Key={"PK": pk, "SK": "LOCK"}).get("Item")
            assert item is not None
            assert int(item["ttl"]) > int(time.time())

    def test_raises_retryable_error_when_lock_held(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            # First acquisition should succeed
            handler.handle(_lock_event(), None)
            # Second acquisition on same key should raise RetryableError
            with pytest.raises(handler.RetryableError):
                handler.handle(_lock_event(), None)

    def test_different_candidates_get_different_locks(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result1 = handler.handle(_lock_event(normalized_candidate_name="v1324 sco"), None)
            result2 = handler.handle(_lock_event(normalized_candidate_name="rs oph"), None)
            assert result1["idempotency_key"] != result2["idempotency_key"]


class TestDispatch:
    def test_unknown_task_name_raises(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)
