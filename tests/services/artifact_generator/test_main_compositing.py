"""Tests for compositing sweep integration in main._process_nova().

Verifies that run_compositing_sweep is called (or not) based on
dirty_types, and that failures do not abort the nova.
"""

from __future__ import annotations

import importlib
import sys
import types
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

_REGEN_PLAN_PK = "REGEN_PLAN"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
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
def table(aws_env: None) -> Any:
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
        s3 = boto3.client("s3", region_name=_REGION)
        s3.create_bucket(Bucket=_BUCKET_PUBLIC)
        s3.create_bucket(Bucket=_BUCKET_PRIVATE)
        yield tbl


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_module() -> types.ModuleType:
    for key in list(sys.modules):
        if key == "main" or key.startswith("main."):
            del sys.modules[key]
    return importlib.import_module("main")


def _seed_nova(table: Any, nova_id: str) -> None:
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


def _noop_generate(
    nova_id: str,
    artifact: Any,
    nova_context: dict[str, Any],
    publisher: Any = None,
) -> None:
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCompositingSweepIntegration:
    """Verify compositing sweep wiring in _process_nova()."""

    def test_compositing_runs_for_spectra_dirty_type(self, table: Any) -> None:
        """run_compositing_sweep is called when dirty_types includes 'spectra'."""
        _seed_nova(table, _NOVA_A)
        mod = _load_module()

        manifest = {
            "dirty_types": ["spectra"],
            "artifacts": ["spectra.json", "nova.json"],
        }
        publisher = MagicMock()
        publisher.copy_forward_missing_artifacts.return_value = []

        sweep_result = {
            "groups_found": 2,
            "skipped": 1,
            "built": 1,
            "degenerate": 0,
            "errors": 0,
        }

        with (
            patch.object(mod, "_generate_and_publish", side_effect=_noop_generate),
            patch(
                "generators.compositing.run_compositing_sweep",
                return_value=sweep_result,
            ) as mock_sweep,
        ):
            result = mod._process_nova(_NOVA_A, manifest, publisher)

        assert result.success is True
        mock_sweep.assert_called_once_with(_NOVA_A, mod._table, mod._s3, _BUCKET_PRIVATE)

    def test_compositing_skipped_for_non_spectra_dirty_types(self, table: Any) -> None:
        """run_compositing_sweep is NOT called for non-spectra dirty types."""
        _seed_nova(table, _NOVA_A)
        mod = _load_module()

        manifest = {
            "dirty_types": ["photometry"],
            "artifacts": ["photometry.json", "nova.json"],
        }
        publisher = MagicMock()
        publisher.copy_forward_missing_artifacts.return_value = []

        with (
            patch.object(mod, "_generate_and_publish", side_effect=_noop_generate),
            patch(
                "generators.compositing.run_compositing_sweep",
            ) as mock_sweep,
        ):
            result = mod._process_nova(_NOVA_A, manifest, publisher)

        assert result.success is True
        mock_sweep.assert_not_called()

    def test_compositing_failure_does_not_fail_nova(self, table: Any) -> None:
        """A compositing sweep exception does not prevent artifact generation."""
        _seed_nova(table, _NOVA_A)
        mod = _load_module()

        manifest = {
            "dirty_types": ["spectra"],
            "artifacts": ["spectra.json", "nova.json"],
        }
        publisher = MagicMock()
        publisher.copy_forward_missing_artifacts.return_value = []

        with (
            patch.object(mod, "_generate_and_publish", side_effect=_noop_generate) as mock_gen,
            patch(
                "generators.compositing.run_compositing_sweep",
                side_effect=RuntimeError("S3 timeout"),
            ) as mock_sweep,
        ):
            result = mod._process_nova(_NOVA_A, manifest, publisher)

            assert result.success is True
            mock_sweep.assert_called_once()
            # Verify artifact generation still ran despite compositing failure.
            assert mock_gen.call_count >= 1
