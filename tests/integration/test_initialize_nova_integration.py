"""
Integration tests for the initialize_nova workflow.

Simulates the full Step Functions execution by calling each Lambda handler
directly in ASL order, sharing a single mocked DynamoDB instance. No real
AWS calls are made — archive_resolver's external queries are patched, and
workflow_launcher's SFN call is patched.

Paths covered:
  1. CREATED_AND_LAUNCHED   — new classical nova confirmed by SIMBAD
  2. EXISTS_AND_LAUNCHED    — candidate name already known in DynamoDB
  3. EXISTS_AND_LAUNCHED    — coordinate duplicate (< 2" separation)
  4. NOT_FOUND              — SIMBAD returns no nova classification
  5. NOT_A_CLASSICAL_NOVA   — recurrent nova (is_classical_nova="false")
  6. QUARANTINE             — coordinate ambiguity (2"–10" separation)
  7. QUARANTINE             — classification ambiguity (is_classical_nova="ambiguous")

Handler call order (per fixed ASL):
  BeginJobRun → NormalizeCandidateName → AcquireIdempotencyLock →
  CheckExistingNovaByName → [path-specific states] → FinalizeJobRun*
"""

from __future__ import annotations

import importlib
import sys
import types
from collections.abc import Generator
from typing import Any, cast
from unittest.mock import patch

import boto3
import pytest
from boto3.dynamodb.conditions import Key
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"
_ACCOUNT = "123456789012"
_QUARANTINE_TOPIC = "nova-cat-quarantine-test"
_SFN_ARN = f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-ingest-new-nova"
_FAKE_EXECUTION_ARN = f"arn:aws:states:{_REGION}:{_ACCOUNT}:execution:nova-cat-ingest-new-nova:test"

# Coordinates for a known nova — used to seed the DB for coordinate-match tests
_EXISTING_NOVA_RA = 270.0
_EXISTING_NOVA_DEC = -30.0
_EXISTING_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"

# Resolved coordinates close to the existing nova (< 2" away)
_DUPLICATE_RA = 270.000001
_DUPLICATE_DEC = -30.000001

# Resolved coordinates in the ambiguous band (~5" away)
_AMBIGUOUS_RA = 270.0015
_AMBIGUOUS_DEC = -30.0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", f"arn:aws:sns:{_REGION}:{_ACCOUNT}:{_QUARANTINE_TOPIC}"
    )
    monkeypatch.setenv("INGEST_NEW_NOVA_STATE_MACHINE_ARN", _SFN_ARN)
    monkeypatch.setenv(
        "REFRESH_REFERENCES_STATE_MACHINE_ARN",
        "arn:aws:states:us-east-1:123456789012:stateMachine:nova-cat-refresh-references",
    )
    monkeypatch.setenv(
        "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN",
        "arn:aws:states:us-east-1:123456789012:stateMachine:nova-cat-discover-spectra-products",
    )
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def table(aws_env: None) -> Generator[Any, None, None]:
    """Shared DynamoDB table for the full workflow."""
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
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "EligibilityIndex",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        yield tbl


# ---------------------------------------------------------------------------
# Handler loader
# ---------------------------------------------------------------------------


def _load_handlers() -> dict[str, types.ModuleType]:
    """Load all handlers fresh inside the current moto context."""
    handlers = {}
    for mod_name in [
        "job_run_manager.handler",
        "nova_resolver.handler",
        "idempotency_guard.handler",
        "archive_resolver.handler",
        "workflow_launcher.handler",
        "quarantine_handler.handler",
    ]:
        if mod_name in sys.modules:
            del sys.modules[mod_name]
        handlers[mod_name.split(".")[0]] = importlib.import_module(mod_name)
    return handlers


# ---------------------------------------------------------------------------
# Workflow runner helpers
# ---------------------------------------------------------------------------


def _base_input(candidate_name: str = "V1324 Sco") -> dict[str, Any]:
    return {
        "candidate_name": candidate_name,
        "correlation_id": "integ-corr-001",
        "source": "test",
    }


def _run_prefix(
    h: dict[str, types.ModuleType], candidate_name: str = "V1324 Sco"
) -> dict[str, Any]:
    """
    Run the common prefix shared by all initialize_nova paths:
      BeginJobRun → NormalizeCandidateName → AcquireIdempotencyLock
    Returns the accumulated state dict.
    """
    inp = _base_input(candidate_name)

    job_run = h["job_run_manager"].handle(
        {
            "task_name": "BeginJobRun",
            "workflow_name": "initialize_nova",
            "candidate_name": inp["candidate_name"],
            "correlation_id": inp["correlation_id"],
            "source": inp["source"],
        },
        None,
    )

    normalization = h["nova_resolver"].handle(
        {
            "task_name": "NormalizeCandidateName",
            "workflow_name": "initialize_nova",
            "candidate_name": inp["candidate_name"],
            "correlation_id": job_run["correlation_id"],
            "job_run_id": job_run["job_run_id"],
        },
        None,
    )

    h["idempotency_guard"].handle(
        {
            "task_name": "AcquireIdempotencyLock",
            "workflow_name": "initialize_nova",
            "primary_id": normalization["normalized_candidate_name"],
            "correlation_id": job_run["correlation_id"],
            "job_run_id": job_run["job_run_id"],
        },
        None,
    )

    return {
        "candidate_name": inp["candidate_name"],
        "job_run": job_run,
        "normalization": normalization,
    }


def _finalize_success(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
    outcome: str,
    nova_id: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "task_name": "FinalizeJobRunSuccess",
        "workflow_name": "initialize_nova",
        "outcome": outcome,
        "candidate_name": state["candidate_name"],
        "correlation_id": state["job_run"]["correlation_id"],
        "job_run_id": state["job_run"]["job_run_id"],
        "job_run": state["job_run"],
    }
    if nova_id:
        params["nova_id"] = nova_id
    return cast(dict[str, Any], h["job_run_manager"].handle(params, None))


def _finalize_quarantined(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        h["job_run_manager"].handle(
            {
                "task_name": "FinalizeJobRunQuarantined",
                "workflow_name": "initialize_nova",
                "candidate_name": state["candidate_name"],
                "correlation_id": state["job_run"]["correlation_id"],
                "job_run_id": state["job_run"]["job_run_id"],
                "job_run": state["job_run"],
            },
            None,
        ),
    )


def _get_job_run(table: Any, state: dict[str, Any]) -> dict[str, Any]:
    """Fetch the JobRun item from DynamoDB."""
    return cast(
        dict[str, Any],
        table.get_item(Key={"PK": state["job_run"]["pk"], "SK": state["job_run"]["sk"]})["Item"],
    )


# ---------------------------------------------------------------------------
# Path 1: CREATED_AND_LAUNCHED
# ---------------------------------------------------------------------------


class TestCreatedAndLaunched:
    def test_full_path_happy(self, table: Any) -> None:
        """
        New classical nova: SIMBAD confirms, no existing record, nova is created
        and ingest_new_nova is launched.
        """
        with mock_aws():
            h = _load_handlers()

            simbad_result = {
                "is_nova": True,
                "is_classical_nova": "true",
                "resolved_ra": 270.5,
                "resolved_dec": -30.5,
                "resolved_epoch": "J2000",
                "resolver_source": "SIMBAD",
                "aliases": ["NOVA Test 2026", "Gaia DR3 1234567890"],
            }

            with (
                patch.object(h["archive_resolver"], "_query_simbad", return_value=simbad_result),
                patch.object(h["archive_resolver"], "_query_tns", return_value=None),
                patch.object(h["workflow_launcher"], "_sfn") as mock_sfn,
            ):
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h)

                name_check = h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByName",
                        "workflow_name": "initialize_nova",
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert name_check["exists"] is False

                resolution = h["archive_resolver"].handle(
                    {
                        "task_name": "ResolveCandidateAgainstPublicArchives",
                        "workflow_name": "initialize_nova",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert resolution["is_nova"] is True
                assert resolution["is_classical_nova"] == "true"

                coord_check = h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByCoordinates",
                        "workflow_name": "initialize_nova",
                        "resolved_ra": resolution["resolved_ra"],
                        "resolved_dec": resolution["resolved_dec"],
                        "resolved_epoch": resolution["resolved_epoch"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert coord_check["match_outcome"] == "NONE"

                nova_creation = h["nova_resolver"].handle(
                    {
                        "task_name": "CreateNovaId",
                        "workflow_name": "initialize_nova",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                nova_id = nova_creation["nova_id"]
                assert nova_id is not None

                h["nova_resolver"].handle(
                    {
                        "task_name": "UpsertMinimalNovaMetadata",
                        "workflow_name": "initialize_nova",
                        "nova_id": nova_id,
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "resolved_ra": resolution["resolved_ra"],
                        "resolved_dec": resolution["resolved_dec"],
                        "resolved_epoch": resolution["resolved_epoch"],
                        "resolver_source": resolution["resolver_source"],
                        "aliases": resolution.get("aliases", []),
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                launch = h["workflow_launcher"].handle(
                    {
                        "task_name": "PublishIngestNewNova",
                        "workflow_name": "initialize_nova",
                        "outcome": "CREATED_AND_LAUNCHED",
                        "nova_id": nova_id,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert "execution_arn" in launch

                _finalize_success(h, state, "CREATED_AND_LAUNCHED", nova_id=nova_id)

            # Assert final DynamoDB state
            job_run_item = _get_job_run(table, state)
            assert job_run_item["status"] == "SUCCEEDED"
            assert job_run_item["outcome"] == "CREATED_AND_LAUNCHED"

            # Nova record exists
            nova_item = table.get_item(Key={"PK": nova_id, "SK": "NOVA"}).get("Item")
            assert nova_item is not None
            assert nova_item["status"] == "ACTIVE"
            assert nova_item["aliases"] == ["NOVA Test 2026", "Gaia DR3 1234567890"]

            # NameMapping — primary
            normalized = state["normalization"]["normalized_candidate_name"]
            name_item = table.get_item(
                Key={"PK": f"NAME#{normalized}", "SK": f"NOVA#{nova_id}"}
            ).get("Item")
            assert name_item is not None
            assert name_item["name_kind"] == "PRIMARY"

            # NameMapping — SIMBAD aliases written for CREATED_AND_LAUNCHED path
            alias1 = table.query(KeyConditionExpression=Key("PK").eq("NAME#nova test 2026"))[
                "Items"
            ]
            assert len(alias1) == 1
            assert alias1[0]["name_kind"] == "ALIAS"
            assert alias1[0]["name_raw"] == "NOVA Test 2026"
            assert alias1[0]["source"] == "SIMBAD"

            alias2 = table.query(KeyConditionExpression=Key("PK").eq("NAME#gaia dr3 1234567890"))[
                "Items"
            ]
            assert len(alias2) == 1
            assert alias2[0]["name_kind"] == "ALIAS"
            assert alias2[0]["name_raw"] == "Gaia DR3 1234567890"


# ---------------------------------------------------------------------------
# Path 2: EXISTS_AND_LAUNCHED (name check)
# ---------------------------------------------------------------------------


class TestExistsAndLaunchedByName:
    def test_name_already_known(self, table: Any) -> None:
        """Candidate name already exists in DynamoDB — skip archive resolution."""
        with mock_aws():
            h = _load_handlers()

            # Seed an existing Nova and NameMapping
            table.put_item(
                Item={
                    "PK": _EXISTING_NOVA_ID,
                    "SK": "NOVA",
                    "entity_type": "Nova",
                    "status": "ACTIVE",
                    "nova_id": _EXISTING_NOVA_ID,
                }
            )
            table.put_item(
                Item={
                    "PK": "NAME#v1324 sco",
                    "SK": f"NOVA#{_EXISTING_NOVA_ID}",
                    "entity_type": "NameMapping",
                    "nova_id": _EXISTING_NOVA_ID,
                }
            )

            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h)

                name_check = h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByName",
                        "workflow_name": "initialize_nova",
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert name_check["exists"] is True
                assert name_check["nova_id"] == _EXISTING_NOVA_ID

                launch = h["workflow_launcher"].handle(
                    {
                        "task_name": "PublishIngestNewNova",
                        "workflow_name": "initialize_nova",
                        "outcome": "EXISTS_AND_LAUNCHED",
                        "nova_id": _EXISTING_NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert "execution_arn" in launch

                _finalize_success(h, state, "EXISTS_AND_LAUNCHED", nova_id=_EXISTING_NOVA_ID)

            job_run_item = _get_job_run(table, state)
            assert job_run_item["status"] == "SUCCEEDED"
            assert job_run_item["outcome"] == "EXISTS_AND_LAUNCHED"


# ---------------------------------------------------------------------------
# Path 3: EXISTS_AND_LAUNCHED (coordinate match)
# ---------------------------------------------------------------------------


class TestExistsAndLaunchedByCoordinates:
    def test_coordinate_duplicate(self, table: Any) -> None:
        """Coordinates within 2" of existing nova — upsert alias and launch."""
        with mock_aws():
            h = _load_handlers()

            # Seed an existing Nova with coordinates
            from decimal import Decimal

            table.put_item(
                Item={
                    "PK": _EXISTING_NOVA_ID,
                    "SK": "NOVA",
                    "entity_type": "Nova",
                    "status": "ACTIVE",
                    "nova_id": _EXISTING_NOVA_ID,
                    "ra_deg": Decimal(str(_EXISTING_NOVA_RA)),
                    "dec_deg": Decimal(str(_EXISTING_NOVA_DEC)),
                    "frame": "ICRS",
                    "epoch": "J2000",
                }
            )

            simbad_result = {
                "is_nova": True,
                "is_classical_nova": "true",
                "resolved_ra": _DUPLICATE_RA,
                "resolved_dec": _DUPLICATE_DEC,
                "resolved_epoch": "J2000",
                "resolver_source": "SIMBAD",
                "aliases": [],
            }

            with (
                patch.object(h["archive_resolver"], "_query_simbad", return_value=simbad_result),
                patch.object(h["archive_resolver"], "_query_tns", return_value=None),
                patch.object(h["workflow_launcher"], "_sfn") as mock_sfn,
            ):
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h, candidate_name="V1324 Sco Alias")

                h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByName",
                        "workflow_name": "initialize_nova",
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                resolution = h["archive_resolver"].handle(
                    {
                        "task_name": "ResolveCandidateAgainstPublicArchives",
                        "workflow_name": "initialize_nova",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                coord_check = h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByCoordinates",
                        "workflow_name": "initialize_nova",
                        "resolved_ra": resolution["resolved_ra"],
                        "resolved_dec": resolution["resolved_dec"],
                        "resolved_epoch": resolution["resolved_epoch"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert coord_check["match_outcome"] == "DUPLICATE"
                assert coord_check["matched_nova_id"] == _EXISTING_NOVA_ID

                h["nova_resolver"].handle(
                    {
                        "task_name": "UpsertAliasForExistingNova",
                        "workflow_name": "initialize_nova",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "nova_id": coord_check["matched_nova_id"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                h["workflow_launcher"].handle(
                    {
                        "task_name": "PublishIngestNewNova",
                        "workflow_name": "initialize_nova",
                        "outcome": "EXISTS_AND_LAUNCHED",
                        "nova_id": _EXISTING_NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                _finalize_success(h, state, "EXISTS_AND_LAUNCHED", nova_id=_EXISTING_NOVA_ID)

            job_run_item = _get_job_run(table, state)
            assert job_run_item["status"] == "SUCCEEDED"
            assert job_run_item["outcome"] == "EXISTS_AND_LAUNCHED"

            # Alias NameMapping was written
            normalized = state["normalization"]["normalized_candidate_name"]
            alias_item = table.get_item(
                Key={"PK": f"NAME#{normalized}", "SK": f"NOVA#{_EXISTING_NOVA_ID}"}
            ).get("Item")
            assert alias_item is not None


# ---------------------------------------------------------------------------
# Path 4: NOT_FOUND
# ---------------------------------------------------------------------------


class TestNotFound:
    def test_not_a_nova(self, table: Any) -> None:
        """SIMBAD does not identify the candidate as a nova — terminal success."""
        with mock_aws():
            h = _load_handlers()

            simbad_result = {
                "is_nova": False,
                "is_classical_nova": "false",
                "resolver_source": "SIMBAD",
                "aliases": [],
            }

            with (
                patch.object(h["archive_resolver"], "_query_simbad", return_value=simbad_result),
                patch.object(h["archive_resolver"], "_query_tns", return_value=None),
            ):
                state = _run_prefix(h)

                h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByName",
                        "workflow_name": "initialize_nova",
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                resolution = h["archive_resolver"].handle(
                    {
                        "task_name": "ResolveCandidateAgainstPublicArchives",
                        "workflow_name": "initialize_nova",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert resolution["is_nova"] is False

                _finalize_success(h, state, "NOT_FOUND")

            job_run_item = _get_job_run(table, state)
            assert job_run_item["status"] == "SUCCEEDED"
            assert job_run_item["outcome"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# Path 5: NOT_A_CLASSICAL_NOVA
# ---------------------------------------------------------------------------


class TestNotAClassicalNova:
    def test_recurrent_nova(self, table: Any) -> None:
        """Candidate is a nova but not classical (recurrent) — terminal success."""
        with mock_aws():
            h = _load_handlers()

            simbad_result = {
                "is_nova": True,
                "is_classical_nova": "false",
                "resolved_ra": 270.5,
                "resolved_dec": -30.5,
                "resolved_epoch": "J2000",
                "resolver_source": "SIMBAD",
                "aliases": [],
            }

            with (
                patch.object(h["archive_resolver"], "_query_simbad", return_value=simbad_result),
                patch.object(h["archive_resolver"], "_query_tns", return_value=None),
            ):
                state = _run_prefix(h)

                h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByName",
                        "workflow_name": "initialize_nova",
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                resolution = h["archive_resolver"].handle(
                    {
                        "task_name": "ResolveCandidateAgainstPublicArchives",
                        "workflow_name": "initialize_nova",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert resolution["is_nova"] is True
                assert resolution["is_classical_nova"] == "false"

                coord_check = h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByCoordinates",
                        "workflow_name": "initialize_nova",
                        "resolved_ra": resolution["resolved_ra"],
                        "resolved_dec": resolution["resolved_dec"],
                        "resolved_epoch": resolution["resolved_epoch"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert coord_check["match_outcome"] == "NONE"

                _finalize_success(h, state, "NOT_A_CLASSICAL_NOVA")

            job_run_item = _get_job_run(table, state)
            assert job_run_item["status"] == "SUCCEEDED"
            assert job_run_item["outcome"] == "NOT_A_CLASSICAL_NOVA"


# ---------------------------------------------------------------------------
# Path 6: QUARANTINE (coordinate ambiguity)
# ---------------------------------------------------------------------------


class TestQuarantineCoordinateAmbiguity:
    def test_ambiguous_coordinates(self, table: Any) -> None:
        """Coordinates in the 2"–10" ambiguous band — quarantine."""
        with mock_aws():
            h = _load_handlers()

            from decimal import Decimal

            table.put_item(
                Item={
                    "PK": _EXISTING_NOVA_ID,
                    "SK": "NOVA",
                    "entity_type": "Nova",
                    "status": "ACTIVE",
                    "nova_id": _EXISTING_NOVA_ID,
                    "ra_deg": Decimal(str(_EXISTING_NOVA_RA)),
                    "dec_deg": Decimal(str(_EXISTING_NOVA_DEC)),
                    "frame": "ICRS",
                    "epoch": "J2000",
                }
            )

            simbad_result = {
                "is_nova": True,
                "is_classical_nova": "true",
                "resolved_ra": _AMBIGUOUS_RA,
                "resolved_dec": _AMBIGUOUS_DEC,
                "resolved_epoch": "J2000",
                "resolver_source": "SIMBAD",
                "aliases": [],
            }

            with (
                patch.object(h["archive_resolver"], "_query_simbad", return_value=simbad_result),
                patch.object(h["archive_resolver"], "_query_tns", return_value=None),
                patch.object(h["quarantine_handler"], "_sns") as mock_sns,
            ):
                mock_sns.publish.return_value = {}

                state = _run_prefix(h, candidate_name="Ambiguous Nova")

                h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByName",
                        "workflow_name": "initialize_nova",
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                resolution = h["archive_resolver"].handle(
                    {
                        "task_name": "ResolveCandidateAgainstPublicArchives",
                        "workflow_name": "initialize_nova",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                coord_check = h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByCoordinates",
                        "workflow_name": "initialize_nova",
                        "resolved_ra": resolution["resolved_ra"],
                        "resolved_dec": resolution["resolved_dec"],
                        "resolved_epoch": resolution["resolved_epoch"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert coord_check["match_outcome"] == "AMBIGUOUS"

                quarantine_result = h["quarantine_handler"].handle(
                    {
                        "task_name": "QuarantineHandler",
                        "workflow_name": "initialize_nova",
                        "quarantine_reason_code": "COORDINATE_AMBIGUITY",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "min_sep_arcsec": coord_check["min_sep_arcsec"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                        "job_run": state["job_run"],
                    },
                    None,
                )
                assert "error_fingerprint" in quarantine_result

                _finalize_quarantined(h, state)

            job_run_item = _get_job_run(table, state)
            assert job_run_item["status"] == "QUARANTINED"
            assert job_run_item["quarantine_reason_code"] == "COORDINATE_AMBIGUITY"
            assert "error_fingerprint" in job_run_item


# ---------------------------------------------------------------------------
# Path 7: QUARANTINE (classification ambiguity)
# ---------------------------------------------------------------------------


class TestQuarantineClassificationAmbiguity:
    def test_ambiguous_classification(self, table: Any) -> None:
        """Archive resolver returns ambiguous nova classification — quarantine."""
        with mock_aws():
            h = _load_handlers()

            simbad_result = {
                "is_nova": True,
                "is_classical_nova": "ambiguous",
                "resolved_ra": 270.5,
                "resolved_dec": -30.5,
                "resolved_epoch": "J2000",
                "resolver_source": "SIMBAD",
                "aliases": [],
            }

            with (
                patch.object(h["archive_resolver"], "_query_simbad", return_value=simbad_result),
                patch.object(h["archive_resolver"], "_query_tns", return_value=None),
                patch.object(h["quarantine_handler"], "_sns") as mock_sns,
            ):
                mock_sns.publish.return_value = {}

                state = _run_prefix(h, candidate_name="Ambiguous Classic")

                h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByName",
                        "workflow_name": "initialize_nova",
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                resolution = h["archive_resolver"].handle(
                    {
                        "task_name": "ResolveCandidateAgainstPublicArchives",
                        "workflow_name": "initialize_nova",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert resolution["is_nova"] is True
                assert resolution["is_classical_nova"] == "ambiguous"

                coord_check = h["nova_resolver"].handle(
                    {
                        "task_name": "CheckExistingNovaByCoordinates",
                        "workflow_name": "initialize_nova",
                        "resolved_ra": resolution["resolved_ra"],
                        "resolved_dec": resolution["resolved_dec"],
                        "resolved_epoch": resolution["resolved_epoch"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert coord_check["match_outcome"] == "NONE"

                quarantine_result = h["quarantine_handler"].handle(
                    {
                        "task_name": "QuarantineHandler",
                        "workflow_name": "initialize_nova",
                        "quarantine_reason_code": "OTHER",
                        "candidate_name": state["candidate_name"],
                        "normalized_candidate_name": state["normalization"][
                            "normalized_candidate_name"
                        ],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                        "job_run": state["job_run"],
                    },
                    None,
                )
                assert "error_fingerprint" in quarantine_result

                _finalize_quarantined(h, state)

            job_run_item = _get_job_run(table, state)
            assert job_run_item["status"] == "QUARANTINED"
            assert job_run_item["quarantine_reason_code"] == "OTHER"
            assert "error_fingerprint" in job_run_item
