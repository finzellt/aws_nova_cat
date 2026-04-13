"""Unit tests for services/artifact_finalizer/handler.py.

Uses moto to mock DynamoDB — no real AWS calls are made.

Covers:
  Dispatch:
  - Unknown task_name raises ValueError

  Finalize (happy path — all novae succeed):
  - Returns COMPLETED status
  - Deletes consumed WorkItems for succeeded novae
  - Writes observation counts to Nova DDB items
  - Sets plan status to COMPLETED with completed_at

  Finalize (partial failure — some novae fail):
  - Returns FAILED status
  - Deletes WorkItems only for succeeded novae
  - Retains WorkItems for failed novae
  - Writes counts only for succeeded novae
  - Does not write counts for failed novae

  Finalize (all novae fail):
  - Returns FAILED status
  - No WorkItems deleted
  - No counts written

  FailHandler (Fargate crash):
  - Returns FAILED status
  - Sets plan status to FAILED with completed_at
  - WorkItems not touched

  Internal functions:
  - _filter_sks_for_nova correctly filters by nova_id prefix
  - _filter_sks_for_nova returns empty for non-matching nova
  - WorkItem deletion batches correctly (>25 items)
  - _load_batch_plan raises ValueError if plan not found
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
from boto3.dynamodb.conditions import Key
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"
_PLAN_ID = "test-plan-00000000-0000-0000-0000-000000000001"

_NOVA_A = "aaaaaaaa-0000-0000-0000-000000000001"
_NOVA_B = "bbbbbbbb-0000-0000-0000-000000000002"

_WORKQUEUE_PK = "WORKQUEUE"
_REGEN_PLAN_PK = "REGEN_PLAN"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
    """Create a mocked DynamoDB table and yield it."""
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
    if "artifact_finalizer.handler" in sys.modules:
        del sys.modules["artifact_finalizer.handler"]
    return importlib.import_module("artifact_finalizer.handler")


# ---------------------------------------------------------------------------
# Helpers — seed data
# ---------------------------------------------------------------------------


def _seed_plan(
    table: Any,
    nova_results: list[dict[str, Any]],
    workitem_sks: list[str],
    plan_id: str = _PLAN_ID,
    status: str = "IN_PROGRESS",
) -> str:
    """Write a RegenBatchPlan item and return its SK."""
    created_at = "2026-04-01T00:00:00Z"
    sk = f"{created_at}#{plan_id}"
    table.put_item(
        Item={
            "PK": _REGEN_PLAN_PK,
            "SK": sk,
            "entity_type": "RegenBatchPlan",
            "schema_version": "1.0.0",
            "plan_id": plan_id,
            "status": status,
            "nova_manifests": {},
            "nova_count": len({r["nova_id"] for r in nova_results}),
            "workitem_sks": workitem_sks,
            "nova_results": nova_results,
            "created_at": created_at,
            "completed_at": None,
            "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:test",
            "ttl": int(time.time()) + 7 * 86_400,
        }
    )
    return sk


def _seed_work_item(table: Any, sk: str) -> None:
    """Write a WorkItem with the given SK."""
    table.put_item(
        Item={
            "PK": _WORKQUEUE_PK,
            "SK": sk,
            "entity_type": "WorkItem",
            "nova_id": sk.split("#")[0],
            "dirty_type": sk.split("#")[1],
            "created_at": "2026-04-01T00:00:00Z",
            "ttl": int(time.time()) + 30 * 86_400,
        }
    )


def _seed_nova(table: Any, nova_id: str) -> None:
    """Write a minimal Nova item."""
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": "NOVA",
            "entity_type": "Nova",
            "status": "ACTIVE",
            "primary_name": f"Test Nova {nova_id[:8]}",
        }
    )


def _get_nova(table: Any, nova_id: str) -> dict[str, Any]:
    """Read a Nova item from DDB."""
    item: dict[str, Any] = table.get_item(Key={"PK": nova_id, "SK": "NOVA"}).get("Item", {})
    return item


def _get_plan(table: Any, plan_sk: str) -> dict[str, Any]:
    """Read a plan item from DDB."""
    item: dict[str, Any] = table.get_item(Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk}).get("Item", {})
    return item


def _count_work_items(table: Any) -> int:
    """Count all WorkItems in the WORKQUEUE partition."""
    response = table.query(KeyConditionExpression=Key("PK").eq(_WORKQUEUE_PK))
    return len(response.get("Items", []))


def _finalize_event(plan_id: str = _PLAN_ID) -> dict[str, Any]:
    return {"task_name": "Finalize", "plan_id": plan_id}


def _fail_handler_event(plan_id: str = _PLAN_ID) -> dict[str, Any]:
    return {"task_name": "FailHandler", "plan_id": plan_id}


def _success_result(
    nova_id: str,
    spectra_count: int = 5,
    photometry_count: int = 12,
    references_count: int = 3,
    has_sparkline: bool = True,
    spectral_visits: int = 0,
) -> dict[str, Any]:
    return {
        "nova_id": nova_id,
        "success": True,
        "error": None,
        "spectra_count": spectra_count,
        "photometry_count": photometry_count,
        "references_count": references_count,
        "has_sparkline": has_sparkline,
        "spectral_visits": spectral_visits,
    }


def _failure_result(nova_id: str, error: str = "generator crashed") -> dict[str, Any]:
    return {
        "nova_id": nova_id,
        "success": False,
        "error": error,
        "spectra_count": None,
        "photometry_count": None,
        "references_count": None,
        "has_sparkline": None,
    }


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "Bogus"}, None)


# ---------------------------------------------------------------------------
# Finalize — all novae succeed
# ---------------------------------------------------------------------------


class TestFinalizeAllSucceed:
    def test_returns_completed_status(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sk_a = f"{_NOVA_A}#spectra#2026-04-01T00:00:00Z"
            _seed_work_item(table, sk_a)
            _seed_nova(table, _NOVA_A)
            _seed_plan(
                table,
                nova_results=[_success_result(_NOVA_A)],
                workitem_sks=[sk_a],
            )
            result = handler.handle(_finalize_event(), None)
        assert result["status"] == "COMPLETED"
        assert result["novae_succeeded"] == 1
        assert result["novae_failed"] == 0

    def test_deletes_consumed_work_items(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sk_a1 = f"{_NOVA_A}#spectra#2026-04-01T00:00:00Z"
            sk_a2 = f"{_NOVA_A}#photometry#2026-04-01T00:00:01Z"
            _seed_work_item(table, sk_a1)
            _seed_work_item(table, sk_a2)
            _seed_nova(table, _NOVA_A)
            _seed_plan(
                table,
                nova_results=[_success_result(_NOVA_A)],
                workitem_sks=[sk_a1, sk_a2],
            )
            handler.handle(_finalize_event(), None)
            remaining = _count_work_items(table)
        assert remaining == 0

    def test_writes_observation_counts_to_nova_item(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_nova(table, _NOVA_A)
            _seed_plan(
                table,
                nova_results=[
                    _success_result(
                        _NOVA_A,
                        spectra_count=7,
                        photometry_count=42,
                        references_count=3,
                        has_sparkline=True,
                        spectral_visits=4,
                    )
                ],
                workitem_sks=[],
            )
            handler.handle(_finalize_event(), None)
            nova = _get_nova(table, _NOVA_A)
        assert nova["spectra_count"] == 7
        assert nova["photometry_count"] == 42
        assert nova["references_count"] == 3
        assert nova["has_sparkline"] is True
        assert nova["spectral_visits"] == 4

    def test_sets_plan_status_completed(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_nova(table, _NOVA_A)
            plan_sk = _seed_plan(
                table,
                nova_results=[_success_result(_NOVA_A)],
                workitem_sks=[],
            )
            handler.handle(_finalize_event(), None)
            plan = _get_plan(table, plan_sk)
        assert plan["status"] == "COMPLETED"
        assert plan["completed_at"] is not None


# ---------------------------------------------------------------------------
# Finalize — partial failure
# ---------------------------------------------------------------------------


class TestFinalizePartialFailure:
    def test_returns_failed_status(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_nova(table, _NOVA_A)
            _seed_nova(table, _NOVA_B)
            _seed_plan(
                table,
                nova_results=[
                    _success_result(_NOVA_A),
                    _failure_result(_NOVA_B),
                ],
                workitem_sks=[],
            )
            result = handler.handle(_finalize_event(), None)
        assert result["status"] == "FAILED"
        assert result["novae_succeeded"] == 1
        assert result["novae_failed"] == 1

    def test_deletes_work_items_only_for_succeeded_nova(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sk_a = f"{_NOVA_A}#spectra#2026-04-01T00:00:00Z"
            sk_b = f"{_NOVA_B}#spectra#2026-04-01T00:00:01Z"
            _seed_work_item(table, sk_a)
            _seed_work_item(table, sk_b)
            _seed_nova(table, _NOVA_A)
            _seed_nova(table, _NOVA_B)
            _seed_plan(
                table,
                nova_results=[
                    _success_result(_NOVA_A),
                    _failure_result(_NOVA_B),
                ],
                workitem_sks=[sk_a, sk_b],
            )
            handler.handle(_finalize_event(), None)
            # Nova A's WorkItem deleted, Nova B's retained
            remaining = _count_work_items(table)
        assert remaining == 1
        # Verify it's Nova B's item that survived
        response = table.query(KeyConditionExpression=Key("PK").eq(_WORKQUEUE_PK))
        assert response["Items"][0]["SK"] == sk_b

    def test_writes_counts_only_for_succeeded_nova(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_nova(table, _NOVA_A)
            _seed_nova(table, _NOVA_B)
            _seed_plan(
                table,
                nova_results=[
                    _success_result(_NOVA_A, spectra_count=10),
                    _failure_result(_NOVA_B),
                ],
                workitem_sks=[],
            )
            handler.handle(_finalize_event(), None)
            nova_a = _get_nova(table, _NOVA_A)
            nova_b = _get_nova(table, _NOVA_B)
        assert nova_a["spectra_count"] == 10
        # Nova B should NOT have counts written
        assert "spectra_count" not in nova_b


# ---------------------------------------------------------------------------
# Finalize — all novae fail
# ---------------------------------------------------------------------------


class TestFinalizeAllFail:
    def test_returns_failed_status(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sk_a = f"{_NOVA_A}#spectra#2026-04-01T00:00:00Z"
            _seed_work_item(table, sk_a)
            _seed_plan(
                table,
                nova_results=[_failure_result(_NOVA_A)],
                workitem_sks=[sk_a],
            )
            result = handler.handle(_finalize_event(), None)
        assert result["status"] == "FAILED"
        assert result["novae_succeeded"] == 0
        assert result["novae_failed"] == 1

    def test_no_work_items_deleted(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sk_a = f"{_NOVA_A}#spectra#2026-04-01T00:00:00Z"
            _seed_work_item(table, sk_a)
            _seed_plan(
                table,
                nova_results=[_failure_result(_NOVA_A)],
                workitem_sks=[sk_a],
            )
            handler.handle(_finalize_event(), None)
            remaining = _count_work_items(table)
        assert remaining == 1


# ---------------------------------------------------------------------------
# FailHandler — Fargate crash
# ---------------------------------------------------------------------------


class TestFailHandler:
    def test_returns_failed_status(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_plan(
                table,
                nova_results=[],
                workitem_sks=[],
            )
            result = handler.handle(_fail_handler_event(), None)
        assert result["status"] == "FAILED"
        assert result["plan_id"] == _PLAN_ID

    def test_sets_plan_status_failed(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            plan_sk = _seed_plan(
                table,
                nova_results=[],
                workitem_sks=[],
            )
            handler.handle(_fail_handler_event(), None)
            plan = _get_plan(table, plan_sk)
        assert plan["status"] == "FAILED"
        assert plan["completed_at"] is not None

    def test_work_items_not_touched(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sk_a = f"{_NOVA_A}#spectra#2026-04-01T00:00:00Z"
            _seed_work_item(table, sk_a)
            _seed_plan(
                table,
                nova_results=[],
                workitem_sks=[sk_a],
            )
            handler.handle(_fail_handler_event(), None)
            remaining = _count_work_items(table)
        assert remaining == 1


# ---------------------------------------------------------------------------
# _filter_sks_for_nova
# ---------------------------------------------------------------------------


class TestFilterSksForNova:
    def test_returns_matching_sks(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sks = [
                f"{_NOVA_A}#spectra#2026-01-01T00:00:00Z",
                f"{_NOVA_A}#photometry#2026-01-01T00:00:01Z",
                f"{_NOVA_B}#references#2026-01-01T00:00:02Z",
            ]
            result = handler._filter_sks_for_nova(_NOVA_A, sks)
        assert len(result) == 2
        assert all(_NOVA_A in sk for sk in result)

    def test_returns_empty_for_non_matching_nova(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sks = [f"{_NOVA_A}#spectra#2026-01-01T00:00:00Z"]
            result = handler._filter_sks_for_nova(_NOVA_B, sks)
        assert result == []


# ---------------------------------------------------------------------------
# WorkItem batch deletion (>25 items)
# ---------------------------------------------------------------------------


class TestBatchDeletion:
    def test_deletes_more_than_25_items(self, table: Any) -> None:
        """Verify batch chunking works for >25 WorkItems."""
        with mock_aws():
            handler = _load_handler()
            _seed_nova(table, _NOVA_A)
            sks: list[str] = []
            for i in range(30):
                sk = f"{_NOVA_A}#spectra#2026-04-01T00:00:{i:02d}Z"
                _seed_work_item(table, sk)
                sks.append(sk)
            _seed_plan(
                table,
                nova_results=[_success_result(_NOVA_A)],
                workitem_sks=sks,
            )
            handler.handle(_finalize_event(), None)
            remaining = _count_work_items(table)
        assert remaining == 0


# ---------------------------------------------------------------------------
# _load_batch_plan — error case
# ---------------------------------------------------------------------------


class TestLoadBatchPlan:
    def test_raises_if_plan_not_found(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Batch plan not found"):
                handler._load_batch_plan("nonexistent-plan-id")


# ---------------------------------------------------------------------------
# Two-nova integration: both succeed
# ---------------------------------------------------------------------------


class TestTwoNovaIntegration:
    def test_both_novae_committed(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sk_a = f"{_NOVA_A}#spectra#2026-04-01T00:00:00Z"
            sk_b = f"{_NOVA_B}#photometry#2026-04-01T00:00:01Z"
            _seed_work_item(table, sk_a)
            _seed_work_item(table, sk_b)
            _seed_nova(table, _NOVA_A)
            _seed_nova(table, _NOVA_B)
            _seed_plan(
                table,
                nova_results=[
                    _success_result(_NOVA_A, spectra_count=5),
                    _success_result(_NOVA_B, photometry_count=20),
                ],
                workitem_sks=[sk_a, sk_b],
            )
            result = handler.handle(_finalize_event(), None)

            nova_a = _get_nova(table, _NOVA_A)
            nova_b = _get_nova(table, _NOVA_B)
            remaining = _count_work_items(table)

        assert result["status"] == "COMPLETED"
        assert result["novae_succeeded"] == 2
        assert remaining == 0
        assert nova_a["spectra_count"] == 5
        assert nova_b["photometry_count"] == 20
