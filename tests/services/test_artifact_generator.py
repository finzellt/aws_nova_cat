"""Unit tests for services/artifact_generator/main.py (Epics 3–4).

Uses moto to mock DynamoDB and S3.  The Fargate task reads env vars at
module level, so we use the standard ``_load_module()`` pattern — fresh
import inside each ``mock_aws()`` context.

Covers:
  Per-nova setup:
  - _load_nova_item returns item for ACTIVE nova
  - _load_nova_item returns None for missing / non-ACTIVE nova
  - _collect_observation_epochs gathers from both tables

  Dispatcher:
  - _generate_and_publish routes each ArtifactType to its generator
  - Photometry skipped gracefully when table not configured

  Per-nova processing:
  - Successful nova returns NovaResult with success=True and counts
  - Nova not found produces NovaResult with success=False
  - Generator exception produces NovaResult with success=False and error
  - Failed nova does not abort the batch

  Plan loading:
  - _load_batch_plan returns the plan item
  - _load_batch_plan exits if plan not found

  Result writeback:
  - _write_results_to_plan writes nova_results to the plan DDB item

  main() integration:
  - Single nova happy path (with publication)
  - Multiple novae — partial failure, exit 0
  - All novae fail — exit 1
  - Generation order respected
"""

from __future__ import annotations

import importlib
import sys
import types
from collections.abc import Generator
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_PHOT_TABLE_NAME = "NovaCat-Photometry-Test"
_BUCKET_PRIVATE = "nova-cat-private-test"
_BUCKET_PUBLIC = "nova-cat-public-test"
_REGION = "us-east-1"
_PLAN_ID = "test-plan-00000000-0000-0000-0000-000000000001"

_NOVA_A = "aaaaaaaa-0000-0000-0000-000000000001"
_NOVA_B = "bbbbbbbb-0000-0000-0000-000000000002"

_REGEN_PLAN_PK = "REGEN_PLAN"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required environment variables before module import."""
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PHOTOMETRY_TABLE_NAME", _PHOT_TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", _BUCKET_PRIVATE)
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", _BUCKET_PUBLIC)
    monkeypatch.setenv("PLAN_ID", _PLAN_ID)
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("BAND_REGISTRY_PATH", "/nonexistent/band_registry.json")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture()
def table(aws_env: None) -> Generator[Any, None, None]:
    """Create mocked DDB tables (main + photometry) and S3 buckets, yield main table."""
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
        # Dedicated photometry table.
        dynamodb.create_table(
            TableName=_PHOT_TABLE_NAME,
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
        # S3 buckets — required for ReleasePublisher in main().
        s3 = boto3.client("s3", region_name=_REGION)
        s3.create_bucket(Bucket=_BUCKET_PUBLIC)
        s3.create_bucket(Bucket=_BUCKET_PRIVATE)
        yield tbl


# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------


def _load_module() -> types.ModuleType:
    import pathlib

    artifact_gen = str(pathlib.Path.cwd().parent / "services" / "artifact_generator")
    print(f"artifact_generator on sys.path: {artifact_gen in sys.path}")
    print(f"sys.path entries with 'artifact': {[p for p in sys.path if 'artifact' in p]}")
    """Reimport main.py so module-level AWS resources bind to the mock."""
    for key in list(sys.modules):
        if key == "main" or key.startswith("main."):
            del sys.modules[key]
    return importlib.import_module("main")


# ---------------------------------------------------------------------------
# DDB seed helpers
# ---------------------------------------------------------------------------


def _seed_nova(table: Any, nova_id: str) -> None:
    """Write an ACTIVE Nova item."""
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": "NOVA",
            "entity_type": "Nova",
            "nova_id": nova_id,
            "primary_name": "Test Nova",
            "aliases": ["Alias A"],
            "ra_deg": Decimal("52.799083"),
            "dec_deg": Decimal("43.904667"),
            "status": "ACTIVE",
            "discovery_date": "2000-01-01",
        }
    )


def _seed_plan(
    table: Any,
    nova_manifests: dict[str, Any],
) -> str:
    """Write a RegenBatchPlan item and return its SK."""
    sk = f"2026-01-01T00:00:00Z#{_PLAN_ID}"
    table.put_item(
        Item={
            "PK": _REGEN_PLAN_PK,
            "SK": sk,
            "plan_id": _PLAN_ID,
            "status": "IN_PROGRESS",
            "nova_manifests": nova_manifests,
        }
    )
    return sk


def _spectra_manifest() -> dict[str, Any]:
    return {
        "dirty_types": ["spectra"],
        "artifacts": ["spectra.json", "nova.json", "bundle.zip", "catalog.json"],
    }


def _all_artifacts_manifest() -> dict[str, Any]:
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
# Generator mock: a no-op that populates context like real generators do
# ---------------------------------------------------------------------------


def _noop_generate(
    nova_id: str,
    artifact: Any,
    nova_context: dict[str, Any],
    publisher: Any = None,
) -> None:
    """Populate context keys the way real generators do, without AWS calls."""
    artifact_val = artifact.value if hasattr(artifact, "value") else str(artifact)
    if artifact_val == "spectra.json":
        nova_context["spectra_count"] = 0
    elif artifact_val == "photometry.json":
        nova_context["photometry_count"] = 0
        nova_context["photometry_raw_items"] = []
        nova_context["photometry_observations"] = []
        nova_context["photometry_bands"] = []
    elif artifact_val == "references.json":
        nova_context["references_count"] = 0
        nova_context["references_output"] = []
    elif artifact_val == "sparkline.svg":
        nova_context["has_sparkline"] = False


# ---------------------------------------------------------------------------
# _load_nova_item
# ---------------------------------------------------------------------------


class TestLoadNovaItem:
    def test_returns_item_for_active_nova(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            _seed_nova(table, _NOVA_A)
            item = mod._load_nova_item(_NOVA_A)
        assert item is not None
        assert item["nova_id"] == _NOVA_A

    def test_returns_none_for_missing_nova(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            item = mod._load_nova_item("nonexistent-id")
        assert item is None

    def test_returns_none_for_non_active_nova(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            table.put_item(
                Item={
                    "PK": _NOVA_A,
                    "SK": "NOVA",
                    "nova_id": _NOVA_A,
                    "status": "QUARANTINED",
                    "ra_deg": Decimal("0"),
                    "dec_deg": Decimal("0"),
                }
            )
            item = mod._load_nova_item(_NOVA_A)
        assert item is None


# ---------------------------------------------------------------------------
# _collect_observation_epochs
# ---------------------------------------------------------------------------


class TestCollectObservationEpochs:
    def test_collects_spectra_epochs(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            table.put_item(
                Item={
                    "PK": _NOVA_A,
                    "SK": "PRODUCT#SPECTRA#dp-001",
                    "validation_status": "VALID",
                    "observation_date_mjd": Decimal("51544.0"),
                }
            )
            epochs = mod._collect_observation_epochs(_NOVA_A)
        assert len(epochs) == 1
        assert epochs[0] == pytest.approx(51544.0)

    def test_collects_photometry_epochs(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            phot_tbl = boto3.resource("dynamodb", region_name=_REGION).Table(
                _PHOT_TABLE_NAME,
            )
            phot_tbl.put_item(
                Item={
                    "PK": _NOVA_A,
                    "SK": "PHOT#r001",
                    "time_mjd": Decimal("51545.0"),
                }
            )
            epochs = mod._collect_observation_epochs(_NOVA_A)
        assert len(epochs) == 1
        assert epochs[0] == pytest.approx(51545.0)

    def test_empty_when_no_observations(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            epochs = mod._collect_observation_epochs(_NOVA_A)
        assert epochs == []


# ---------------------------------------------------------------------------
# _generate_and_publish dispatch
# ---------------------------------------------------------------------------


class TestGenerateAndPublish:
    def test_routes_references(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {"nova_item": {}}
            publisher = MagicMock()
            with patch.object(mod, "generate_references_json") as mock_gen:
                mod._generate_and_publish(_NOVA_A, mod.ArtifactType.references_json, ctx, publisher)
            mock_gen.assert_called_once()

    def test_routes_spectra(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {"nova_item": {}}
            publisher = MagicMock()
            with patch.object(mod, "generate_spectra_json") as mock_gen:
                mod._generate_and_publish(_NOVA_A, mod.ArtifactType.spectra_json, ctx, publisher)
            mock_gen.assert_called_once()

    def test_routes_photometry(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {"nova_item": {}}
            publisher = MagicMock()
            with patch.object(mod, "generate_photometry_json") as mock_gen:
                mod._generate_and_publish(_NOVA_A, mod.ArtifactType.photometry_json, ctx, publisher)
            mock_gen.assert_called_once()

    def test_routes_sparkline(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {"nova_item": {}}
            publisher = MagicMock()
            with patch.object(mod, "generate_sparkline_svg") as mock_gen:
                mod._generate_and_publish(_NOVA_A, mod.ArtifactType.sparkline_svg, ctx, publisher)
            mock_gen.assert_called_once()

    def test_routes_nova(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {"nova_item": {}}
            publisher = MagicMock()
            with patch.object(mod, "generate_nova_json") as mock_gen:
                mod._generate_and_publish(_NOVA_A, mod.ArtifactType.nova_json, ctx, publisher)
            mock_gen.assert_called_once()

    def test_routes_bundle(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            ctx: dict[str, Any] = {"nova_item": {}}
            publisher = MagicMock()
            with patch.object(mod, "generate_bundle_zip") as mock_gen:
                mock_gen.return_value = {
                    "s3_key": "nova/test/test_bundle_20260403.zip",
                    "bundle_filename": "test_bundle_20260403.zip",
                }
                mod._generate_and_publish(_NOVA_A, mod.ArtifactType.bundle_zip, ctx, publisher)
            mock_gen.assert_called_once()


# ---------------------------------------------------------------------------
# _process_nova
# ---------------------------------------------------------------------------


class TestProcessNova:
    def test_successful_nova_returns_success(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            _seed_nova(table, _NOVA_A)
            publisher = MagicMock()
            with patch.object(mod, "_generate_and_publish", _noop_generate):
                result = mod._process_nova(_NOVA_A, _spectra_manifest(), publisher)
        assert result.success is True
        assert result.nova_id == _NOVA_A
        assert result.error is None

    def test_counts_from_generators(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            _seed_nova(table, _NOVA_A)
            publisher = MagicMock()
            with patch.object(mod, "_generate_and_publish", _noop_generate):
                result = mod._process_nova(_NOVA_A, _spectra_manifest(), publisher)
        assert result.spectra_count == 0

    def test_nova_not_found_fails(self, table: Any) -> None:
        """Missing Nova item produces a failed NovaResult."""
        with mock_aws():
            mod = _load_module()
            publisher = MagicMock()
            result = mod._process_nova(_NOVA_A, _spectra_manifest(), publisher)
        assert result.success is False
        assert "not found" in (result.error or "").lower()

    def test_generator_exception_produces_failed_result(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            _seed_nova(table, _NOVA_A)
            publisher = MagicMock()

            def _boom(*args: Any, **kwargs: Any) -> None:
                raise ValueError("generator exploded")

            with patch.object(mod, "_generate_and_publish", _boom):
                result = mod._process_nova(_NOVA_A, _spectra_manifest(), publisher)
        assert result.success is False
        assert "generator exploded" in (result.error or "")

    def test_failed_nova_has_no_counts(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            publisher = MagicMock()
            # No Nova seeded → will fail at _load_nova_item
            result = mod._process_nova(_NOVA_A, _spectra_manifest(), publisher)
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
            plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]

            results = [
                mod.NovaResult(
                    nova_id=_NOVA_A,
                    success=True,
                    spectra_count=5,
                    photometry_count=10,
                    references_count=3,
                    has_sparkline=True,
                )
            ]
            mod._write_results_to_plan(plan, results)

            updated = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        assert len(updated["nova_results"]) == 1
        assert updated["nova_results"][0]["nova_id"] == _NOVA_A
        assert updated["nova_results"][0]["success"] is True


# ---------------------------------------------------------------------------
# main() integration
# ---------------------------------------------------------------------------


class TestMain:
    def test_single_nova_happy_path(self, table: Any) -> None:
        with mock_aws():
            mod = _load_module()
            _seed_nova(table, _NOVA_A)
            plan_sk = _seed_plan(table, {_NOVA_A: _spectra_manifest()})

            with patch.object(mod, "_generate_and_publish", _noop_generate):
                mod.main()

            plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        assert len(plan["nova_results"]) == 1
        assert plan["nova_results"][0]["success"] is True

    def test_partial_failure_exits_zero(self, table: Any) -> None:
        """One nova fails, one succeeds — exit 0 (not all failed)."""
        with mock_aws():
            mod = _load_module()
            _seed_nova(table, _NOVA_A)
            # Don't seed NOVA_B → it will fail at _load_nova_item
            manifests = {
                _NOVA_A: _spectra_manifest(),
                _NOVA_B: _spectra_manifest(),
            }
            plan_sk = _seed_plan(table, manifests)

            with patch.object(mod, "_generate_and_publish", _noop_generate):
                mod.main()

            plan = table.get_item(
                Key={"PK": _REGEN_PLAN_PK, "SK": plan_sk},
            )["Item"]
        results_by_id = {r["nova_id"]: r for r in plan["nova_results"]}
        assert results_by_id[_NOVA_A]["success"] is True
        assert results_by_id[_NOVA_B]["success"] is False

    def test_all_novae_fail_exits_one(self, table: Any) -> None:
        """When every nova fails, main() calls sys.exit(1)."""
        with mock_aws():
            mod = _load_module()
            # No Nova items seeded → both fail
            manifests = {
                _NOVA_A: _spectra_manifest(),
                _NOVA_B: _spectra_manifest(),
            }
            _seed_plan(table, manifests)

            with pytest.raises(SystemExit) as exc_info:
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
            _seed_nova(table, _NOVA_A)
            _seed_plan(table, {_NOVA_A: _all_artifacts_manifest()})

            call_order: list[str] = []

            def _tracking_generate(
                nova_id: str,
                artifact: Any,
                ctx: dict[str, Any],
                publisher: Any = None,
            ) -> None:
                call_order.append(artifact.value)
                _noop_generate(nova_id, artifact, ctx, publisher)

            with patch.object(mod, "_generate_and_publish", _tracking_generate):
                mod.main()

        assert call_order == [
            "references.json",
            "spectra.json",
            "photometry.json",
            "sparkline.svg",
            "nova.json",
            "bundle.zip",
        ]
