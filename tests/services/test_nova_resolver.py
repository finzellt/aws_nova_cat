"""
Unit tests for services/nova_resolver/handler.py

Uses moto to mock DynamoDB — no real AWS calls are made.

Covers:
  - NormalizeCandidateName: lowercases, strips, collapses whitespace
  - NormalizeCandidateName: raises TerminalError on empty input
  - CheckExistingNovaByName: returns exists=True with nova_id when found
  - CheckExistingNovaByName: returns exists=False when not found
  - CheckExistingNovaByCoordinates: DUPLICATE when separation < 2"
  - CheckExistingNovaByCoordinates: AMBIGUOUS when separation 2"–10"
  - CheckExistingNovaByCoordinates: NONE when separation > 10"
  - CheckExistingNovaByCoordinates: NONE when no novae in DB
  - CreateNovaId: writes Nova stub with PENDING status, returns UUID
  - UpsertMinimalNovaMetadata: updates Nova item and writes NameMapping
  - UpsertAliasForExistingNova: writes ALIAS NameMapping
  - Angular separation helper: known values
"""

from __future__ import annotations

import importlib
import sys
import types
import uuid
from collections.abc import Generator
from decimal import Decimal
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
    if "nova_resolver.handler" in sys.modules:
        del sys.modules["nova_resolver.handler"]
    return importlib.import_module("nova_resolver.handler")


def _seed_nova(table: Any, nova_id: str, ra: float, dec: float) -> None:
    """Write a minimal Nova item to the mocked table."""
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": "NOVA",
            "entity_type": "Nova",
            "nova_id": nova_id,
            "ra_deg": Decimal(str(ra)),
            "dec_deg": Decimal(str(dec)),
            "status": "ACTIVE",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }
    )


def _seed_name_mapping(table: Any, normalized_name: str, nova_id: str) -> None:
    """Write a NameMapping item to the mocked table."""
    table.put_item(
        Item={
            "PK": f"NAME#{normalized_name}",
            "SK": f"NOVA#{nova_id}",
            "entity_type": "NameMapping",
            "nova_id": nova_id,
            "name_normalized": normalized_name,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }
    )


# ---------------------------------------------------------------------------
# NormalizeCandidateName
# ---------------------------------------------------------------------------


class TestNormalizeCandidateName:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("V1324 Sco", "v1324 sco"),
            ("  RS  Oph  ", "rs oph"),
            ("V407Cyg", "v407cyg"),
            ("NOVA SCO 2012", "nova sco 2012"),
        ],
    )
    def test_normalization(self, table: Any, raw: str, expected: str) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(
                {
                    "task_name": "NormalizeCandidateName",
                    "candidate_name": raw,
                    "workflow_name": "initialize_nova",
                    "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                },
                None,
            )
            assert result["normalized_candidate_name"] == expected

    def test_empty_name_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(handler.TerminalError):
                handler.handle(
                    {
                        "task_name": "NormalizeCandidateName",
                        "candidate_name": "   ",
                        "workflow_name": "initialize_nova",
                        "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                    },
                    None,
                )


# ---------------------------------------------------------------------------
# CheckExistingNovaByName
# ---------------------------------------------------------------------------


class TestCheckExistingNovaByName:
    def test_returns_exists_true_when_found(self, table: Any) -> None:
        nova_id = str(uuid.uuid4())
        with mock_aws():
            _seed_name_mapping(table, "v1324 sco", nova_id)
            handler = _load_handler()
            result = handler.handle(
                {
                    "task_name": "CheckExistingNovaByName",
                    "normalized_candidate_name": "v1324 sco",
                    "workflow_name": "initialize_nova",
                    "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                },
                None,
            )
            assert result["exists"] is True
            assert result["nova_id"] == nova_id

    def test_returns_exists_false_when_not_found(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(
                {
                    "task_name": "CheckExistingNovaByName",
                    "normalized_candidate_name": "unknown nova",
                    "workflow_name": "initialize_nova",
                    "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                },
                None,
            )
            assert result["exists"] is False
            assert "nova_id" not in result


# ---------------------------------------------------------------------------
# CheckExistingNovaByCoordinates
# ---------------------------------------------------------------------------


class TestCheckExistingNovaByCoordinates:
    # V1324 Sco approximate coordinates
    _RA = 267.56
    _DEC = -32.55

    def _coord_event(self, ra: float, dec: float) -> dict[str, Any]:
        return {
            "task_name": "CheckExistingNovaByCoordinates",
            "resolved_ra": ra,
            "resolved_dec": dec,
            "resolved_epoch": "J2000",
            "workflow_name": "initialize_nova",
            "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
        }

    def test_duplicate_when_separation_under_2_arcsec(self, table: Any) -> None:
        nova_id = str(uuid.uuid4())
        with mock_aws():
            _seed_nova(table, nova_id, self._RA, self._DEC)
            handler = _load_handler()
            # Offset by ~0.5 arcsec in RA
            offset_ra = self._RA + (0.5 / 3600.0)
            result = handler.handle(self._coord_event(offset_ra, self._DEC), None)
            assert result["match_outcome"] == "DUPLICATE"
            assert result["matched_nova_id"] == nova_id

    def test_ambiguous_when_separation_2_to_10_arcsec(self, table: Any) -> None:
        nova_id = str(uuid.uuid4())
        with mock_aws():
            _seed_nova(table, nova_id, self._RA, self._DEC)
            handler = _load_handler()
            # Offset by ~5 arcsec in RA
            offset_ra = self._RA + (5.0 / 3600.0)
            result = handler.handle(self._coord_event(offset_ra, self._DEC), None)
            assert result["match_outcome"] == "AMBIGUOUS"

    def test_none_when_separation_over_10_arcsec(self, table: Any) -> None:
        nova_id = str(uuid.uuid4())
        with mock_aws():
            _seed_nova(table, nova_id, self._RA, self._DEC)
            handler = _load_handler()
            # Offset by ~30 arcsec in RA
            offset_ra = self._RA + (30.0 / 3600.0)
            result = handler.handle(self._coord_event(offset_ra, self._DEC), None)
            assert result["match_outcome"] == "NONE"

    def test_none_when_no_novae_in_db(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(self._coord_event(self._RA, self._DEC), None)
            assert result["match_outcome"] == "NONE"


# ---------------------------------------------------------------------------
# CreateNovaId
# ---------------------------------------------------------------------------


class TestCreateNovaId:
    def test_returns_nova_id(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(
                {
                    "task_name": "CreateNovaId",
                    "candidate_name": "V1324 Sco",
                    "normalized_candidate_name": "v1324 sco",
                    "job_run_id": "j1",
                    "workflow_name": "initialize_nova",
                    "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                },
                None,
            )
            assert "nova_id" in result
            assert len(result["nova_id"]) == 36

    def test_writes_nova_stub_with_pending_status(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(
                {
                    "task_name": "CreateNovaId",
                    "candidate_name": "V1324 Sco",
                    "normalized_candidate_name": "v1324 sco",
                    "job_run_id": "j1",
                    "workflow_name": "initialize_nova",
                    "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                },
                None,
            )
            nova_id = result["nova_id"]
            item = table.get_item(Key={"PK": nova_id, "SK": "NOVA"}).get("Item")
            assert item is not None
            assert item["status"] == "PENDING"
            assert item["primary_name"] == "V1324 Sco"
            assert item["entity_type"] == "Nova"


# ---------------------------------------------------------------------------
# UpsertMinimalNovaMetadata
# ---------------------------------------------------------------------------


class TestUpsertMinimalNovaMetadata:
    def _setup_nova(self, table: Any, handler: Any) -> str:  # type: ignore[return]
        result = handler.handle(
            {
                "task_name": "CreateNovaId",
                "candidate_name": "V1324 Sco",
                "normalized_candidate_name": "v1324 sco",
                "job_run_id": "j1",
                "workflow_name": "initialize_nova",
                "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
            },
            None,
        )
        return result["nova_id"]  # type: ignore[no-any-return]

    def test_promotes_nova_to_active(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            nova_id = self._setup_nova(table, handler)
            handler.handle(
                {
                    "task_name": "UpsertMinimalNovaMetadata",
                    "nova_id": nova_id,
                    "candidate_name": "V1324 Sco",
                    "normalized_candidate_name": "v1324 sco",
                    "resolved_ra": 267.56,
                    "resolved_dec": -32.55,
                    "resolved_epoch": "J2000",
                    "resolver_source": "SIMBAD",
                    "workflow_name": "initialize_nova",
                    "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                },
                None,
            )
            item = table.get_item(Key={"PK": nova_id, "SK": "NOVA"}).get("Item")
            assert item["status"] == "ACTIVE"
            assert float(item["ra_deg"]) == pytest.approx(267.56)

    def test_writes_primary_name_mapping(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            nova_id = self._setup_nova(table, handler)
            handler.handle(
                {
                    "task_name": "UpsertMinimalNovaMetadata",
                    "nova_id": nova_id,
                    "candidate_name": "V1324 Sco",
                    "normalized_candidate_name": "v1324 sco",
                    "resolved_ra": 267.56,
                    "resolved_dec": -32.55,
                    "resolved_epoch": "J2000",
                    "resolver_source": "SIMBAD",
                    "workflow_name": "initialize_nova",
                    "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                },
                None,
            )
            mapping = table.get_item(Key={"PK": "NAME#v1324 sco", "SK": f"NOVA#{nova_id}"}).get(
                "Item"
            )
            assert mapping is not None
            assert mapping["name_kind"] == "PRIMARY"
            assert mapping["nova_id"] == nova_id


# ---------------------------------------------------------------------------
# UpsertAliasForExistingNova
# ---------------------------------------------------------------------------


class TestUpsertAliasForExistingNova:
    def test_writes_alias_name_mapping(self, table: Any) -> None:
        nova_id = str(uuid.uuid4())
        with mock_aws():
            handler = _load_handler()
            handler.handle(
                {
                    "task_name": "UpsertAliasForExistingNova",
                    "candidate_name": "Nova Sco 2012",
                    "normalized_candidate_name": "nova sco 2012",
                    "nova_id": nova_id,
                    "workflow_name": "initialize_nova",
                    "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
                },
                None,
            )
            mapping = table.get_item(Key={"PK": "NAME#nova sco 2012", "SK": f"NOVA#{nova_id}"}).get(
                "Item"
            )
            assert mapping is not None
            assert mapping["name_kind"] == "ALIAS"
            assert mapping["nova_id"] == nova_id


# ---------------------------------------------------------------------------
# Angular separation helper
# ---------------------------------------------------------------------------


class TestAngularSeparation:
    def test_zero_separation(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            sep = handler._angular_separation_arcsec(10.0, 20.0, 10.0, 20.0)
            assert sep == pytest.approx(0.0, abs=1e-6)

    def test_one_arcsec_separation(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            # 1 arcsec in dec at any RA
            sep = handler._angular_separation_arcsec(0.0, 0.0, 0.0, 1 / 3600.0)
            assert sep == pytest.approx(1.0, abs=0.001)

    def test_known_separation(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            # 10 arcsec offset in dec
            sep = handler._angular_separation_arcsec(0.0, 0.0, 0.0, 10 / 3600.0)
            assert sep == pytest.approx(10.0, abs=0.01)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, table: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)
