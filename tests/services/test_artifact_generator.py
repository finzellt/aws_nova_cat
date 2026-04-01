"""Unit tests for services/artifact_generator/main.py.

Uses moto to mock DynamoDB.  The Fargate task reads env vars at module
level (``PLAN_ID``, ``NOVA_CAT_TABLE_NAME``), so we use the standard
``_load_module()`` pattern — fresh import inside each ``mock_aws()``
context.

Covers:
  Generator stubs:
  - _generate_artifact_stub populates spectra_count, photometry_count,
    references_count, has_sparkline in nova_context

  Per-nova processing:
  - Successful nova returns NovaResult with success=True and counts
  - Generator exception produces NovaResult with success=False and error
  - Failed nova does not abort the batch (tested at main() level)

  Plan loading:
  - _load_batch_plan returns the plan item
  - _load_batch_plan exits if plan not found

  Result writeback:
  - _write_results_to_plan writes nova_results to the plan DDB item

  main() integration:
  - Single nova happy path
  - Multiple novae all succeed — results written, exit 0
  - Partial failure — one fails, others succeed, exit 0
  - All novae fail — exit 1
  - Generation order respected (artifacts processed in dependency order)
"""

from __future__ import annotations

import importlib
import sys
import time
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
_REGION = "us-east-1"
_PLAN_ID = "test-plan-00000000-0000-0000-0000-000000000001"

_NOVA_A = "aaaaaaaa-0000-0000-0000-000000000001"
_NOVA_B = "bbbbbbbb-0000-0000-0000-000000000002"
_NOVA_C = "cccccccc-0000-0000-0000-000000000003"

_REGEN_PLAN_PK = "REGEN_PLAN"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required environment variables before module import."""
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("PLAN_ID", _PLAN_ID)
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
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


def _load_module() -> types.ModuleType:
    """Import main.py fresh inside the moto context."""
    if "artifact_generator.main" in sys.modules:
        del sys.modules["artifact_generator.main"]
    return importlib.import_module("artifact_generator.main")


# ---------------------------------------------------------------------------
# Helpers — seed data
# ---------------------------------------------------------------------------


def _seed_plan(
    table: Any,
    nova_manifests: dict[str, Any],
    plan_id: str = _PLAN_ID,
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
            "status": "IN_PROGRESS",
            "nova_manifests": nova_manifests,
            "nova_count": len(nova_manifests),
            "workitem_sks": [],
            "created_at": created_at,
            "completed_at": None,
            "execution_arn": None,
            "ttl": int(time.time()) + 7 * 86_400,
        }
    )
    return sk


def _spectra_manifest() -> dict[str, Any]:
    """Manifest for a nova with spectra dirty type."""
    return {
        "dirty_types": ["spectra"],
        "artifacts": ["spectra.json", "nova.json", "bundle.zip", "catalog.json"],
    }


def _photometry_manifest() -> dict[str, Any]:
    """Manifest for a nova with photometry dirty type."""
    return {
        "dirty_types": ["photometry"],
        "artifacts": [
            "photometry.json",
            "sparkline.svg",
            "nova.json",
            "bundle.zip",
            "catalog.json",
        ],
    }


def _all_artifacts_manifest() -> dict[str, Any]:
    """Manifest for a nova with all dirty types."""
    return {
        "dirty_types": ["spectra", "photometry", "references"],
        "artifacts": [
            "references.json",
            "spectra.json",
            "photometry.json",
            "sparkline.svg",
            "nova.json",
            "bundle.zip",
            "catalog.json",
        ],
    }


# ---------------------------------------------------------------------------
# _generate_artifact_stub
# ---------------------------------------------------------------------------


class TestGenerateArtifactStub:
    def test_spectra_json_sets_spectra_count(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {}
            mod._generate_artifact_stub(_NOVA_A, mod.ArtifactType.spectra_json, ctx)
        assert ctx["spectra_count"] == 0

    def test_photometry_json_sets_photometry_count(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {}
            mod._generate_artifact_stub(_NOVA_A, mod.ArtifactType.photometry_json, ctx)
        assert ctx["photometry_count"] == 0

    def test_references_json_sets_references_count(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {}
            mod._generate_artifact_stub(_NOVA_A, mod.ArtifactType.references_json, ctx)
        assert ctx["references_count"] == 0

    def test_sparkline_svg_sets_has_sparkline(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {}
            mod._generate_artifact_stub(_NOVA_A, mod.ArtifactType.sparkline_svg, ctx)
        assert ctx["has_sparkline"] is False

    def test_nova_json_and_bundle_do_not_set_counts(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {}
            mod._generate_artifact_stub(_NOVA_A, mod.ArtifactType.nova_json, ctx)
            mod._generate_artifact_stub(_NOVA_A, mod.ArtifactType.bundle_zip, ctx)
        assert ctx == {}


# ---------------------------------------------------------------------------
# _process_nova
# ---------------------------------------------------------------------------


class TestProcessNova:
    def test_successful_nova_returns_success_true(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            result = mod._process_nova(_NOVA_A, _spectra_manifest())
        assert result.success is True
        assert result.nova_id == _NOVA_A
        assert result.error is None

    def test_spectra_manifest_produces_spectra_count(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            result = mod._process_nova(_NOVA_A, _spectra_manifest())
        assert result.spectra_count == 0
        assert result.photometry_count == 0  # default
        assert result.references_count == 0  # default

    def test_photometry_manifest_produces_photometry_count_and_sparkline(
        self,
        table: Any,
    ) -> None:
        with mock_aws():
            mod = _load_module()
            result = mod._process_nova(_NOVA_A, _photometry_manifest())
        assert result.photometry_count == 0
        assert result.has_sparkline is False

    def test_all_artifacts_manifest_populates_all_counts(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            result = mod._process_nova(_NOVA_A, _all_artifacts_manifest())
        assert result.spectra_count == 0
        assert result.photometry_count == 0
        assert result.references_count == 0
        assert result.has_sparkline is False

    def test_generator_exception_produces_failed_result(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            with patch.object(
                mod,
                "_generate_artifact_stub",
                side_effect=ValueError("boom"),
            ):
                result = mod._process_nova(_NOVA_A, _spectra_manifest())
        assert result.success is False
        assert result.nova_id == _NOVA_A
        assert "boom" in (result.error or "")

    def test_failed_nova_has_no_counts(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            with patch.object(
                mod,
                "_generate_artifact_stub",
                side_effect=RuntimeError("fail"),
            ):
                result = mod._process_nova(_NOVA_A, _spectra_manifest())
        assert result.spectra_count is None
        assert result.photometry_count is None
        assert result.references_count is None
        assert result.has_sparkline is None


# ---------------------------------------------------------------------------
# _load_batch_plan
# ---------------------------------------------------------------------------


class TestLoadBatchPlan:
    def test_loads_plan_by_plan_id(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            _seed_plan(table, {_NOVA_A: _spectra_manifest()})
            plan = mod._load_batch_plan()
        assert plan["plan_id"] == _PLAN_ID
        assert _NOVA_A in plan["nova_manifests"]

    def test_exits_if_plan_not_found(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            # No plan seeded — table is empty
            with pytest.raises(SystemExit) as exc_info:
                mod._load_batch_plan()
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# _write_results_to_plan
# ---------------------------------------------------------------------------


class TestWriteResultsToPlan:
    def test_writes_nova_results_to_plan(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            plan_sk = _seed_plan(table, {_NOVA_A: _spectra_manifest()})
            results = [
                mod.NovaResult(
                    nova_id=_NOVA_A,
                    success=True,
                    spectra_count=0,
                    photometry_count=0,
                    references_count=0,
                    has_sparkline=False,
                ),
            ]
            plan_item = {"PK": _REGEN_PLAN_PK, "SK": plan_sk}
            mod._write_results_to_plan(plan_item, results)

            updated = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        assert "nova_results" in updated
        assert len(updated["nova_results"]) == 1
        assert updated["nova_results"][0]["nova_id"] == _NOVA_A
        assert updated["nova_results"][0]["success"] is True

    def test_writes_failed_result(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            plan_sk = _seed_plan(table, {_NOVA_A: _spectra_manifest()})
            results = [
                mod.NovaResult(
                    nova_id=_NOVA_A,
                    success=False,
                    error="test error",
                ),
            ]
            plan_item = {"PK": _REGEN_PLAN_PK, "SK": plan_sk}
            mod._write_results_to_plan(plan_item, results)

            updated = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        stored = updated["nova_results"][0]
        assert stored["success"] is False
        assert stored["error"] == "test error"
        assert stored["spectra_count"] is None


# ---------------------------------------------------------------------------
# main() — integration
# ---------------------------------------------------------------------------


class TestMainHappyPath:
    def test_single_nova_succeeds(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            plan_sk = _seed_plan(table, {_NOVA_A: _spectra_manifest()})
            mod.main()
            plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        assert len(plan["nova_results"]) == 1
        assert plan["nova_results"][0]["success"] is True

    def test_multiple_novae_all_succeed(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            manifests = {
                _NOVA_A: _spectra_manifest(),
                _NOVA_B: _photometry_manifest(),
                _NOVA_C: _all_artifacts_manifest(),
            }
            plan_sk = _seed_plan(table, manifests)
            mod.main()
            plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        assert len(plan["nova_results"]) == 3
        assert all(r["success"] for r in plan["nova_results"])

    def test_results_include_correct_nova_ids(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            manifests = {_NOVA_A: _spectra_manifest(), _NOVA_B: _photometry_manifest()}
            plan_sk = _seed_plan(table, manifests)
            mod.main()
            plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        result_ids = {r["nova_id"] for r in plan["nova_results"]}
        assert result_ids == {_NOVA_A, _NOVA_B}


# ---------------------------------------------------------------------------
# main() — failure isolation
# ---------------------------------------------------------------------------


class TestMainFailureIsolation:
    def test_partial_failure_still_exits_zero(self, table: Any) -> None:
        """One nova fails, another succeeds → exit code 0."""
        with mock_aws():
            mod = _load_module()
            manifests = {_NOVA_A: _spectra_manifest(), _NOVA_B: _spectra_manifest()}
            _seed_plan(table, manifests)

            call_count = 0
            original_stub = mod._generate_artifact_stub

            def _fail_on_nova_a(
                nova_id: str,
                artifact: Any,
                ctx: dict[str, Any],
            ) -> None:
                nonlocal call_count
                if nova_id == _NOVA_A:
                    call_count += 1
                    raise RuntimeError("nova A broke")
                original_stub(nova_id, artifact, ctx)

            with patch.object(mod, "_generate_artifact_stub", _fail_on_nova_a):
                mod.main()  # should not raise SystemExit

    def test_partial_failure_records_both_results(self, table: Any) -> None:
        """Failed nova and succeeded nova both appear in results."""
        with mock_aws():
            mod = _load_module()
            manifests = {_NOVA_A: _spectra_manifest(), _NOVA_B: _spectra_manifest()}
            plan_sk = _seed_plan(table, manifests)

            original_stub = mod._generate_artifact_stub

            def _fail_on_nova_a(
                nova_id: str,
                artifact: Any,
                ctx: dict[str, Any],
            ) -> None:
                if nova_id == _NOVA_A:
                    raise RuntimeError("nova A broke")
                original_stub(nova_id, artifact, ctx)

            with patch.object(mod, "_generate_artifact_stub", _fail_on_nova_a):
                mod.main()

            plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        results_by_id = {r["nova_id"]: r for r in plan["nova_results"]}
        assert results_by_id[_NOVA_A]["success"] is False
        assert "nova A broke" in results_by_id[_NOVA_A]["error"]
        assert results_by_id[_NOVA_B]["success"] is True

    def test_all_novae_fail_exits_one(self, table: Any) -> None:
        """When every nova fails, main() calls sys.exit(1)."""
        with mock_aws():
            mod = _load_module()
            manifests = {_NOVA_A: _spectra_manifest(), _NOVA_B: _spectra_manifest()}
            _seed_plan(table, manifests)

            with (
                patch.object(
                    mod,
                    "_generate_artifact_stub",
                    side_effect=RuntimeError("all broken"),
                ),
                pytest.raises(SystemExit) as exc_info,
            ):
                mod.main()
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Generation order
# ---------------------------------------------------------------------------


class TestGenerationOrder:
    def test_artifacts_processed_in_dependency_order(self, table: Any) -> None:
        """Verify generators are called in GENERATION_ORDER sequence."""
        with mock_aws():
            mod = _load_module()
            manifests = {_NOVA_A: _all_artifacts_manifest()}
            _seed_plan(table, manifests)

            call_order: list[str] = []
            original_stub = mod._generate_artifact_stub

            def _tracking_stub(
                nova_id: str,
                artifact: Any,
                ctx: dict[str, Any],
            ) -> None:
                call_order.append(artifact.value)
                original_stub(nova_id, artifact, ctx)

            with patch.object(mod, "_generate_artifact_stub", _tracking_stub):
                mod.main()

        # GENERATION_ORDER: references → spectra → photometry → sparkline → nova → bundle
        assert call_order == [
            "references.json",
            "spectra.json",
            "photometry.json",
            "sparkline.svg",
            "nova.json",
            "bundle.zip",
        ]
