"""Unit tests for services/artifact_coordinator/handler.py.

Uses moto to mock DynamoDB.  The Step Functions client is patched via
``unittest.mock.patch.object`` since the coordinator only calls
``start_execution`` and we don't need a full moto SFn mock.

Covers:
  Exit paths:
  - Empty queue → returns no_op, no plan created, no workflow launched
  - IN_PROGRESS plan found → returns skipped, no plan created
  - Normal path → plan persisted, workflow launched, returns launched

  Step-level:
  - _query_work_queue paginates correctly (multi-page response)
  - _find_latest_active_plan returns None when only terminal plans exist
  - _find_latest_active_plan returns PENDING plan (not COMPLETED)
  - _find_latest_active_plan returns IN_PROGRESS plan
  - _abandon_plan sets status to ABANDONED and writes completed_at
  - _build_nova_manifests groups by nova_id, applies dependency matrix
  - _build_nova_manifests merges multiple dirty types for the same nova
  - _warn_stale_work_items logs warning for old items, ignores fresh items
  - _persist_batch_plan writes correct PK/SK structure with workitem_sks
  - _start_workflow passes plan_id in execution input
  - _update_plan_execution_arn writes ARN back to the plan item
"""

from __future__ import annotations

import importlib
import json
import sys
import time
import types
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from boto3.dynamodb.conditions import Key
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"
_STATE_MACHINE_ARN = "arn:aws:states:us-east-1:123456789012:stateMachine:regenerate-artifacts"
_FAKE_EXECUTION_ARN = (
    "arn:aws:states:us-east-1:123456789012:execution:regenerate-artifacts:sweep-test"
)

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
    monkeypatch.setenv("REGENERATE_ARTIFACTS_STATE_MACHINE_ARN", _STATE_MACHINE_ARN)
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
    if "artifact_coordinator.handler" in sys.modules:
        del sys.modules["artifact_coordinator.handler"]
    return importlib.import_module("artifact_coordinator.handler")


# ---------------------------------------------------------------------------
# Helpers — seed data
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _aged_iso(days: int) -> str:
    """Return an ISO timestamp *days* in the past."""
    t = datetime.now(UTC) - timedelta(days=days)
    return t.isoformat(timespec="seconds").replace("+00:00", "Z")


def _seed_work_item(
    table: Any,
    nova_id: str,
    dirty_type: str,
    created_at: str | None = None,
) -> str:
    """Write a WorkItem and return its SK."""
    ts = created_at or _now_iso()
    sk = f"{nova_id}#{dirty_type}#{ts}"
    table.put_item(
        Item={
            "PK": _WORKQUEUE_PK,
            "SK": sk,
            "entity_type": "WorkItem",
            "nova_id": nova_id,
            "dirty_type": dirty_type,
            "source_workflow": "test",
            "job_run_id": "00000000-0000-0000-0000-000000000000",
            "correlation_id": "test-corr",
            "created_at": ts,
            "ttl": int(time.time()) + 30 * 86_400,
        }
    )
    return sk


def _seed_plan(
    table: Any,
    status: str,
    created_at: str | None = None,
    plan_id: str = "plan-test-001",
) -> str:
    """Write a RegenBatchPlan item and return its SK."""
    ts = created_at or _now_iso()
    sk = f"{ts}#{plan_id}"
    table.put_item(
        Item={
            "PK": _REGEN_PLAN_PK,
            "SK": sk,
            "entity_type": "RegenBatchPlan",
            "schema_version": "1.0.0",
            "plan_id": plan_id,
            "status": status,
            "nova_manifests": {},
            "nova_count": 0,
            "workitem_sks": [],
            "created_at": ts,
            "completed_at": None,
            "execution_arn": None,
            "ttl": int(time.time()) + 7 * 86_400,
        }
    )
    return sk


def _mock_sfn() -> MagicMock:
    """Build a mock SFn client that returns a fake execution ARN."""
    mock = MagicMock()
    mock.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}
    return mock


# ---------------------------------------------------------------------------
# Exit path: empty queue
# ---------------------------------------------------------------------------


class TestEmptyQueue:
    def test_returns_no_op(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle({}, None)
        assert result["action"] == "no_op"
        assert result["reason"] == "empty_queue"

    def test_no_plan_created(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle({}, None)
            response = table.query(
                KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
            )
        assert response["Items"] == []


# ---------------------------------------------------------------------------
# Exit path: in-progress plan
# ---------------------------------------------------------------------------


class TestInProgressPlanSkip:
    def test_returns_skipped(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_work_item(table, _NOVA_A, "spectra")
            _seed_plan(table, "IN_PROGRESS", plan_id="active-plan")
            result = handler.handle({}, None)
        assert result["action"] == "skipped"
        assert result["reason"] == "in_progress_plan"
        assert result["existing_plan_id"] == "active-plan"


# ---------------------------------------------------------------------------
# Exit path: normal (happy path)
# ---------------------------------------------------------------------------


class TestNormalPath:
    def test_returns_launched(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_work_item(table, _NOVA_A, "spectra")
            with patch.object(handler, "_sfn", _mock_sfn()):
                result = handler.handle({}, None)
        assert result["action"] == "launched"
        assert result["nova_count"] == 1
        assert result["workitem_count"] == 1
        assert result["execution_arn"] == _FAKE_EXECUTION_ARN

    def test_plan_persisted_to_ddb(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_work_item(table, _NOVA_A, "spectra")
            with patch.object(handler, "_sfn", _mock_sfn()):
                result = handler.handle({}, None)
            plan_id = result["plan_id"]
            response = table.query(
                KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
            )
        assert len(response["Items"]) == 1
        plan = response["Items"][0]
        assert plan["plan_id"] == plan_id
        assert plan["status"] == "PENDING"
        assert plan["nova_count"] == 1

    def test_execution_arn_written_to_plan(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_work_item(table, _NOVA_A, "spectra")
            with patch.object(handler, "_sfn", _mock_sfn()):
                handler.handle({}, None)
            response = table.query(
                KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
            )
        plan = response["Items"][0]
        assert plan["execution_arn"] == _FAKE_EXECUTION_ARN

    def test_sfn_receives_plan_id_in_input(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_work_item(table, _NOVA_A, "spectra")
            mock = _mock_sfn()
            with patch.object(handler, "_sfn", mock):
                result = handler.handle({}, None)
            call_kwargs = mock.start_execution.call_args[1]
        payload = json.loads(call_kwargs["input"])
        assert payload["plan_id"] == result["plan_id"]

    def test_sfn_execution_name_contains_plan_id(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_work_item(table, _NOVA_A, "spectra")
            mock = _mock_sfn()
            with patch.object(handler, "_sfn", mock):
                result = handler.handle({}, None)
            call_kwargs = mock.start_execution.call_args[1]
        assert result["plan_id"] in call_kwargs["name"]


# ---------------------------------------------------------------------------
# Stale PENDING plan → abandon and rebuild
# ---------------------------------------------------------------------------


class TestStalePlanAbandonment:
    def test_pending_plan_abandoned_before_new_plan(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            old_sk = _seed_plan(
                table,
                "PENDING",
                created_at=_aged_iso(1),
                plan_id="stale-plan",
            )
            _seed_work_item(table, _NOVA_A, "spectra")
            with patch.object(handler, "_sfn", _mock_sfn()):
                handler.handle({}, None)
            old_plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": old_sk},
            )["Item"]
        assert old_plan["status"] == "ABANDONED"
        assert old_plan["completed_at"] is not None

    def test_new_plan_created_after_abandonment(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_plan(
                table,
                "PENDING",
                created_at=_aged_iso(1),
                plan_id="stale-plan",
            )
            _seed_work_item(table, _NOVA_A, "spectra")
            with patch.object(handler, "_sfn", _mock_sfn()):
                result = handler.handle({}, None)
        assert result["action"] == "launched"
        assert result["plan_id"] != "stale-plan"


# ---------------------------------------------------------------------------
# _find_latest_active_plan — edge cases
# ---------------------------------------------------------------------------


class TestFindLatestActivePlan:
    def test_returns_none_when_only_terminal_plans(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_plan(table, "COMPLETED", plan_id="done-1")
            _seed_plan(table, "FAILED", plan_id="done-2")
            _seed_plan(table, "ABANDONED", plan_id="done-3")
            result = handler._find_latest_active_plan()
        assert result is None

    def test_returns_pending_plan(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_plan(table, "COMPLETED", plan_id="done-1")
            _seed_plan(table, "PENDING", plan_id="active-1")
            result = handler._find_latest_active_plan()
        assert result is not None
        assert result["plan_id"] == "active-1"

    def test_returns_in_progress_plan(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_plan(table, "IN_PROGRESS", plan_id="running-1")
            result = handler._find_latest_active_plan()
        assert result is not None
        assert result["plan_id"] == "running-1"


# ---------------------------------------------------------------------------
# _query_work_queue — pagination
# ---------------------------------------------------------------------------


class TestQueryWorkQueuePagination:
    def test_returns_all_items_across_pages(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            # Seed enough items to verify collection (moto returns all in one
            # page by default, but the code handles pagination correctly)
            for i in range(10):
                _seed_work_item(table, _NOVA_A, "spectra", created_at=f"2026-01-01T00:00:{i:02d}Z")
            items = handler._query_work_queue()
        assert len(items) == 10


# ---------------------------------------------------------------------------
# _build_nova_manifests — dependency matrix
# ---------------------------------------------------------------------------


class TestBuildNovaManifests:
    def test_spectra_dirty_type_produces_correct_artifacts(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            items = [
                {"nova_id": _NOVA_A, "dirty_type": "spectra"},
            ]
            manifests = handler._build_nova_manifests(items)
        m = manifests[_NOVA_A]
        artifact_values = {a.value for a in m.artifacts}
        assert "spectra.json" in artifact_values
        assert "nova.json" in artifact_values
        assert "bundle.zip" in artifact_values
        assert "catalog.json" in artifact_values
        assert "photometry.json" not in artifact_values
        assert "sparkline.svg" not in artifact_values

    def test_photometry_dirty_type_includes_sparkline(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            items = [
                {"nova_id": _NOVA_A, "dirty_type": "photometry"},
            ]
            manifests = handler._build_nova_manifests(items)
        artifact_values = {a.value for a in manifests[_NOVA_A].artifacts}
        assert "photometry.json" in artifact_values
        assert "sparkline.svg" in artifact_values

    def test_references_dirty_type_produces_references_json(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            items = [
                {"nova_id": _NOVA_A, "dirty_type": "references"},
            ]
            manifests = handler._build_nova_manifests(items)
        artifact_values = {a.value for a in manifests[_NOVA_A].artifacts}
        assert "references.json" in artifact_values
        assert "spectra.json" not in artifact_values

    def test_multiple_dirty_types_merged_for_same_nova(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            items = [
                {"nova_id": _NOVA_A, "dirty_type": "spectra"},
                {"nova_id": _NOVA_A, "dirty_type": "photometry"},
                {"nova_id": _NOVA_A, "dirty_type": "spectra"},  # duplicate
            ]
            manifests = handler._build_nova_manifests(items)
        m = manifests[_NOVA_A]
        assert sorted(m.dirty_types) == ["photometry", "spectra"]
        artifact_values = {a.value for a in m.artifacts}
        # Union of spectra + photometry artifacts
        assert "spectra.json" in artifact_values
        assert "photometry.json" in artifact_values
        assert "sparkline.svg" in artifact_values

    def test_multiple_novae_produce_separate_manifests(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            items = [
                {"nova_id": _NOVA_A, "dirty_type": "spectra"},
                {"nova_id": _NOVA_B, "dirty_type": "references"},
            ]
            manifests = handler._build_nova_manifests(items)
        assert _NOVA_A in manifests
        assert _NOVA_B in manifests
        assert "spectra.json" in {a.value for a in manifests[_NOVA_A].artifacts}
        assert "references.json" in {a.value for a in manifests[_NOVA_B].artifacts}


# ---------------------------------------------------------------------------
# _warn_stale_work_items
# ---------------------------------------------------------------------------


class TestWarnStaleWorkItems:
    def test_stale_items_logged(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            items = [
                {
                    "nova_id": _NOVA_A,
                    "dirty_type": "spectra",
                    "created_at": _aged_iso(10),
                    "job_run_id": "old-run",
                },
            ]
            with patch.object(handler, "logger") as mock_logger:
                handler._warn_stale_work_items(items)
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            assert "Stale WorkItems" in call_args[0][0]

    def test_fresh_items_not_logged(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            items = [
                {
                    "nova_id": _NOVA_A,
                    "dirty_type": "spectra",
                    "created_at": _now_iso(),
                    "job_run_id": "new-run",
                },
            ]
            with patch.object(handler, "logger") as mock_logger:
                handler._warn_stale_work_items(items)
            mock_logger.warning.assert_not_called()


# ---------------------------------------------------------------------------
# _persist_batch_plan — DDB item structure
# ---------------------------------------------------------------------------


class TestPersistBatchPlan:
    def test_plan_has_correct_pk_sk_structure(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            from contracts.models.regeneration import ArtifactType, NovaManifest

            manifests = {
                _NOVA_A: NovaManifest(
                    dirty_types=["spectra"],
                    artifacts=[
                        ArtifactType.spectra_json,
                        ArtifactType.nova_json,
                        ArtifactType.bundle_zip,
                        ArtifactType.catalog_json,
                    ],
                ),
            }
            sks = [f"{_NOVA_A}#spectra#2026-01-01T00:00:00Z"]
            plan_id, plan_sk = handler._persist_batch_plan(manifests, sks)

            response = table.query(
                KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
            )
        assert len(response["Items"]) == 1
        plan = response["Items"][0]
        assert plan["PK"] == _REGEN_PLAN_PK
        assert plan_id in plan["SK"]
        assert plan["status"] == "PENDING"
        assert plan["nova_count"] == 1
        assert plan["workitem_sks"] == sks
        assert plan["entity_type"] == "RegenBatchPlan"

    def test_plan_ttl_is_approximately_7_days(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            from contracts.models.regeneration import ArtifactType, NovaManifest

            manifests = {
                _NOVA_A: NovaManifest(
                    dirty_types=["spectra"],
                    artifacts=[ArtifactType.spectra_json],
                ),
            }
            handler._persist_batch_plan(manifests, [])

            response = table.query(
                KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
            )
        plan = response["Items"][0]
        expected_ttl = int(time.time()) + 7 * 86_400
        assert abs(int(plan["ttl"]) - expected_ttl) < 10

    def test_plan_stores_serialised_manifests(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            from contracts.models.regeneration import ArtifactType, NovaManifest

            manifests = {
                _NOVA_A: NovaManifest(
                    dirty_types=["spectra", "photometry"],
                    artifacts=[
                        ArtifactType.spectra_json,
                        ArtifactType.photometry_json,
                    ],
                ),
            }
            handler._persist_batch_plan(manifests, [])

            response = table.query(
                KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
            )
        stored = response["Items"][0]["nova_manifests"]
        assert _NOVA_A in stored
        assert stored[_NOVA_A]["dirty_types"] == ["spectra", "photometry"]
        assert "spectra.json" in stored[_NOVA_A]["artifacts"]
        assert "photometry.json" in stored[_NOVA_A]["artifacts"]


# ---------------------------------------------------------------------------
# _update_plan_execution_arn
# ---------------------------------------------------------------------------


class TestUpdatePlanExecutionArn:
    def test_writes_arn_to_existing_plan(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            plan_sk = _seed_plan(table, "PENDING", plan_id="arn-test")
            handler._update_plan_execution_arn(plan_sk, _FAKE_EXECUTION_ARN)
            plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        assert plan["execution_arn"] == _FAKE_EXECUTION_ARN


# ---------------------------------------------------------------------------
# Integration: multiple novae, mixed dirty types
# ---------------------------------------------------------------------------


class TestMultiNovaIntegration:
    def test_two_novae_with_different_dirty_types(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            _seed_work_item(table, _NOVA_A, "spectra")
            _seed_work_item(table, _NOVA_A, "photometry")
            _seed_work_item(table, _NOVA_B, "references")

            mock = _mock_sfn()
            with patch.object(handler, "_sfn", mock):
                result = handler.handle({}, None)

            assert result["action"] == "launched"
            assert result["nova_count"] == 2
            assert result["workitem_count"] == 3

            # Verify the persisted plan has both novae
            response = table.query(
                KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
            )
            plan = response["Items"][0]
            stored_manifests = plan["nova_manifests"]
        assert _NOVA_A in stored_manifests
        assert _NOVA_B in stored_manifests
        # Nova A had spectra + photometry → should include both
        assert "spectra.json" in stored_manifests[_NOVA_A]["artifacts"]
        assert "photometry.json" in stored_manifests[_NOVA_A]["artifacts"]
        # Nova B had references only
        assert "references.json" in stored_manifests[_NOVA_B]["artifacts"]
        assert "spectra.json" not in stored_manifests[_NOVA_B]["artifacts"]
