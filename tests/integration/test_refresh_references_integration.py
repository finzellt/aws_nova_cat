"""
Integration tests for the refresh_references workflow.

Simulates the full Step Functions execution by calling each Lambda handler
directly in ASL order, sharing a single mocked AWS environment (DynamoDB,
Secrets Manager, SNS) via moto. ADS HTTP calls are patched via requests.

Workflow order (per refresh_references.asl.json):
  BeginJobRun → AcquireIdempotencyLock → FetchReferenceCandidates →
  ReconcileReferences (Map: NormalizeReference → UpsertReferenceEntity →
  LinkNovaReference | ItemFailureHandler) → ComputeDiscoveryDate →
  UpsertDiscoveryDateMetadata → FinalizeJobRunSuccess |
  TerminalFailHandler → FinalizeJobRunFailed

Map ResultPath notes (from ASL):
  NormalizeReference:      no ResultPath → replaces entire item state
  UpsertReferenceEntity:   no ResultPath → replaces entire item state
  LinkNovaReference:       no ResultPath → replaces entire item state (End)
  ItemFailureHandler:      ResultPath=$.quarantine → preserves item state

Top-level ResultPath notes:
  FetchReferenceCandidates:    ResultPath=$.fetch
  ReconcileReferences:         ResultPath=$.reconcile
  ComputeDiscoveryDate:        ResultPath=$.discovery
  UpsertDiscoveryDateMetadata: ResultPath=$.discovery_upsert
  FinalizeJobRunSuccess:       ResultPath=$.finalize

Paths covered:
  1. Happy path — ADS returns two candidates; all Map items succeed; earlier
     discovery date is written to the Nova item; JobRun ends SUCCEEDED
  2. Empty candidates — ADS returns no results; Map is a no-op; workflow
     still reaches FinalizeJobRunSuccess; no discovery date is set
  3. Discovery date no-op — Nova already has an earlier date;
     UpsertDiscoveryDateMetadata is a no-op (updated: False)
  4. Item-level quarantine — one candidate fails NormalizeReference;
     ItemFailureHandler (QuarantineHandler) fires and continues; remaining
     items succeed; JobRun ends SUCCEEDED
  5. Terminal failure — FetchReferenceCandidates raises TerminalError; ASL
     routes to TerminalFailHandler → FinalizeJobRunFailed; JobRun ends FAILED
  6. Idempotency — running the Map tasks twice for the same nova does not
     produce duplicate NOVAREF items (LinkNovaReference is idempotent)
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from collections.abc import Generator
from typing import Any, cast
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
_ACCOUNT = "123456789012"

_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000002"
_CORRELATION_ID = "integ-refresh-corr-001"
_QUARANTINE_TOPIC_ARN = f"arn:aws:sns:{_REGION}:{_ACCOUNT}:nova-cat-quarantine"
_ADS_SECRET_VALUE = json.dumps({"token": "test-ads-token-integ"})

# Two sample ADS docs. _BIBCODE_B has an earlier publication date.
_BIBCODE_A = "2013ATel.5073....1S"  # 2013-06-00
_BIBCODE_B = "1992IAUC.5608....1W"  # 1992-01-00

_RAW_DOC_A: dict[str, Any] = {
    "bibcode": _BIBCODE_A,
    "doctype": "telegram",
    "title": ["Discovery of Nova V1324 Sco"],
    "date": "2013-06-01T00:00:00Z",
    "author": ["Stanek, K. Z.", "Kochanek, C. S."],
    "doi": None,
    "identifier": [],
}

_RAW_DOC_B: dict[str, Any] = {
    "bibcode": _BIBCODE_B,
    "doctype": "circular",
    "title": ["Nova V1324 Sco — IAUC notice"],
    "date": "1992-01-01T00:00:00Z",
    "author": ["Williams, R."],
    "doi": None,
    "identifier": [],
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", "nova-cat-private-test")
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-public-test")
    monkeypatch.setenv("NOVA_CAT_QUARANTINE_TOPIC_ARN", _QUARANTINE_TOPIC_ARN)
    monkeypatch.setenv("ADS_SECRET_NAME", "ADSQueryToken")
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def table(aws_env: None) -> Generator[Any, None, None]:
    """
    Provision the full mocked AWS environment needed by this workflow:
      - DynamoDB table
      - SNS topic (consumed by quarantine_handler; best-effort, but should exist)
      - Secrets Manager secret (consumed by reference_manager for ADS token)
    """
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

        sns = boto3.client("sns", region_name=_REGION)
        sns.create_topic(Name="nova-cat-quarantine")

        sm = boto3.client("secretsmanager", region_name=_REGION)
        sm.create_secret(Name="ADSQueryToken", SecretString=_ADS_SECRET_VALUE)

        yield tbl


# ---------------------------------------------------------------------------
# Handler loader
# ---------------------------------------------------------------------------


def _load_handlers() -> dict[str, types.ModuleType]:
    """
    Fresh import of every handler used by this workflow.
    Module cache is cleared so moto patches apply cleanly on each test.
    """
    for mod_name in [
        "job_run_manager.handler",
        "idempotency_guard.handler",
        "reference_manager.handler",
        "quarantine_handler.handler",
    ]:
        if mod_name in sys.modules:
            del sys.modules[mod_name]
    return {
        "job_run_manager": importlib.import_module("job_run_manager.handler"),
        "idempotency_guard": importlib.import_module("idempotency_guard.handler"),
        "reference_manager": importlib.import_module("reference_manager.handler"),
        "quarantine_handler": importlib.import_module("quarantine_handler.handler"),
    }


# ---------------------------------------------------------------------------
# DDB seed helpers
# ---------------------------------------------------------------------------


def _seed_nova(
    table: Any,
    nova_id: str = _NOVA_ID,
    aliases: list[str] | None = None,
    discovery_date: str | None = None,
) -> None:
    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": "NOVA",
        "entity_type": "Nova",
        "schema_version": "1.0.0",
        "nova_id": nova_id,
        "primary_name": "V1324 Sco",
        "primary_name_normalized": "v1324 sco",
        "status": "ACTIVE",
        "aliases": aliases if aliases is not None else ["NOVA Sco 2012"],
    }
    if discovery_date is not None:
        item["discovery_date"] = discovery_date
    table.put_item(Item=item)


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _mock_ads_response(docs: list[dict[str, Any]]) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"response": {"docs": docs}}
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


# ---------------------------------------------------------------------------
# Workflow step helpers
# ---------------------------------------------------------------------------


def _run_prefix(h: dict[str, types.ModuleType]) -> dict[str, Any]:
    """
    Run the common workflow prefix: BeginJobRun → AcquireIdempotencyLock.
    Returns a top-level state dict mirroring the ASL execution context:
      { "nova_id": ..., "correlation_id": ..., "job_run": { ... } }
    """
    job_run = cast(
        dict[str, Any],
        h["job_run_manager"].handle(
            {
                "task_name": "BeginJobRun",
                "workflow_name": "refresh_references",
                "nova_id": _NOVA_ID,
                "correlation_id": _CORRELATION_ID,
            },
            None,
        ),
    )

    h["idempotency_guard"].handle(
        {
            "task_name": "AcquireIdempotencyLock",
            "workflow_name": "refresh_references",
            "primary_id": _NOVA_ID,
            "correlation_id": job_run["correlation_id"],
            "job_run_id": job_run["job_run_id"],
        },
        None,
    )

    return {
        "nova_id": _NOVA_ID,
        "correlation_id": job_run["correlation_id"],
        "job_run": job_run,
    }


def _run_map_item(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
    raw_doc: dict[str, Any],
) -> None:
    """
    Run one Map item through the full happy path:
      NormalizeReference → UpsertReferenceEntity → LinkNovaReference.

    Mirrors the ASL chain: each task's output (with no ResultPath) becomes
    the next task's input. task_name is injected at each step as the ASL
    Parameters block would.
    """
    # NormalizeReference: receives ItemSelector fields (no ResultPath → replaces state)
    normalized = cast(
        dict[str, Any],
        h["reference_manager"].handle(
            {
                "task_name": "NormalizeReference",
                "nova_id": state["nova_id"],
                "correlation_id": state["job_run"]["correlation_id"],
                "job_run": state["job_run"],
                **raw_doc,
            },
            None,
        ),
    )

    # UpsertReferenceEntity: receives NormalizeReference output (no ResultPath → replaces state)
    upserted = cast(
        dict[str, Any],
        h["reference_manager"].handle(
            {
                "task_name": "UpsertReferenceEntity",
                **normalized,
            },
            None,
        ),
    )

    # LinkNovaReference: receives UpsertReferenceEntity output (no ResultPath, End: true)
    h["reference_manager"].handle(
        {
            "task_name": "LinkNovaReference",
            **upserted,
        },
        None,
    )


def _run_post_map(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
) -> dict[str, Any]:
    """
    Run the post-Map states: ComputeDiscoveryDate → UpsertDiscoveryDateMetadata.

    Both use ResultPath so the top-level state is preserved. Returns the
    UpsertDiscoveryDateMetadata result for assertion.
    """
    discovery = cast(
        dict[str, Any],
        h["reference_manager"].handle(
            {
                "task_name": "ComputeDiscoveryDate",
                "workflow_name": "refresh_references",
                "nova_id": state["nova_id"],
                "correlation_id": state["job_run"]["correlation_id"],
                "job_run_id": state["job_run"]["job_run_id"],
            },
            None,
        ),
    )

    upsert_result = cast(
        dict[str, Any],
        h["reference_manager"].handle(
            {
                "task_name": "UpsertDiscoveryDateMetadata",
                "workflow_name": "refresh_references",
                "nova_id": state["nova_id"],
                "correlation_id": state["job_run"]["correlation_id"],
                "job_run_id": state["job_run"]["job_run_id"],
                "earliest_bibcode": discovery["earliest_bibcode"],
                "earliest_publication_date": discovery["earliest_publication_date"],
            },
            None,
        ),
    )

    return upsert_result


def _run_finalize_success(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
) -> None:
    h["job_run_manager"].handle(
        {
            "task_name": "FinalizeJobRunSuccess",
            "workflow_name": "refresh_references",
            "outcome": "SUCCEEDED",
            "nova_id": state["nova_id"],
            "correlation_id": state["job_run"]["correlation_id"],
            "job_run_id": state["job_run"]["job_run_id"],
            "job_run": state["job_run"],
        },
        None,
    )


def _get_job_run(table: Any, state: dict[str, Any]) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        table.get_item(Key={"PK": state["job_run"]["pk"], "SK": state["job_run"]["sk"]})["Item"],
    )


def _get_nova(table: Any, nova_id: str = _NOVA_ID) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        table.get_item(Key={"PK": nova_id, "SK": "NOVA"})["Item"],
    )


def _list_novarefs(table: Any, nova_id: str = _NOVA_ID) -> list[dict[str, Any]]:
    resp = table.query(
        KeyConditionExpression=Key("PK").eq(nova_id) & Key("SK").begins_with("NOVAREF#")
    )
    return cast(list[dict[str, Any]], resp["Items"])


# ---------------------------------------------------------------------------
# Path 1: Happy path — two candidates, both succeed, discovery date written
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_job_run_ends_succeeded(self, table: Any) -> None:
        """Full workflow run ends with JobRun in SUCCEEDED state."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_A, _RAW_DOC_B])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)

                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                _run_map_item(h, state, _RAW_DOC_A)
                _run_map_item(h, state, _RAW_DOC_B)
                _run_post_map(h, state)
                _run_finalize_success(h, state)

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"

    def test_both_references_written_to_ddb(self, table: Any) -> None:
        """Both Reference entities are upserted to their global REFERENCE# partitions."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_A, _RAW_DOC_B])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                _run_map_item(h, state, _RAW_DOC_A)
                _run_map_item(h, state, _RAW_DOC_B)

        ref_a = table.get_item(Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"})["Item"]
        ref_b = table.get_item(Key={"PK": f"REFERENCE#{_BIBCODE_B}", "SK": "METADATA"})["Item"]
        assert ref_a["bibcode"] == _BIBCODE_A
        assert ref_a["reference_type"] == "atel"
        assert ref_b["bibcode"] == _BIBCODE_B
        assert ref_b["reference_type"] == "cbat_circular"

    def test_both_novaref_links_written(self, table: Any) -> None:
        """Both NovaReference link items are written to the nova's partition."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_A, _RAW_DOC_B])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                _run_map_item(h, state, _RAW_DOC_A)
                _run_map_item(h, state, _RAW_DOC_B)

        novarefs = _list_novarefs(table)
        bibcodes = {item["bibcode"] for item in novarefs}
        assert bibcodes == {_BIBCODE_A, _BIBCODE_B}

    def test_earlier_discovery_date_written_to_nova(self, table: Any) -> None:
        """
        ComputeDiscoveryDate picks the earlier bibcode (_BIBCODE_B, 1992-01-00)
        and UpsertDiscoveryDateMetadata writes it to the Nova item.
        """
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_A, _RAW_DOC_B])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                _run_map_item(h, state, _RAW_DOC_A)
                _run_map_item(h, state, _RAW_DOC_B)
                _run_post_map(h, state)
                _run_finalize_success(h, state)

        nova = _get_nova(table)
        assert nova["discovery_date"] == "1992-01-00"

    def test_publication_dates_normalized_correctly(self, table: Any) -> None:
        """ADS date strings are stored as YYYY-MM-00 (day discarded)."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_A])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                _run_map_item(h, state, _RAW_DOC_A)

        ref = table.get_item(Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"})["Item"]
        assert ref["publication_date"] == "2013-06-00"


# ---------------------------------------------------------------------------
# Path 2: Empty candidates — ADS returns no results
# ---------------------------------------------------------------------------


class TestEmptyCandidates:
    def test_job_run_ends_succeeded(self, table: Any) -> None:
        """Workflow succeeds even when ADS returns no candidates."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                fetch = cast(
                    dict[str, Any],
                    h["reference_manager"].handle(
                        {
                            "task_name": "FetchReferenceCandidates",
                            "workflow_name": "refresh_references",
                            "nova_id": _NOVA_ID,
                            "correlation_id": state["job_run"]["correlation_id"],
                            "job_run_id": state["job_run"]["job_run_id"],
                        },
                        None,
                    ),
                )

            # Map receives zero items — skip directly to post-Map states
            assert fetch["candidate_count"] == 0
            _run_post_map(h, state)
            _run_finalize_success(h, state)

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"

    def test_no_references_written(self, table: Any) -> None:
        """No Reference or NovaReference items are written when candidates is empty."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                # No Map items to run
                _run_post_map(h, state)

        assert _list_novarefs(table) == []

    def test_no_discovery_date_set(self, table: Any) -> None:
        """ComputeDiscoveryDate returns None when no references exist."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                upsert_result = _run_post_map(h, state)

        assert upsert_result["updated"] is False
        assert upsert_result["discovery_date"] is None
        nova = _get_nova(table)
        assert "discovery_date" not in nova


# ---------------------------------------------------------------------------
# Path 3: Discovery date no-op — Nova already has an earlier date
# ---------------------------------------------------------------------------


class TestDiscoveryDateNoOp:
    def test_earlier_existing_date_is_not_overwritten(self, table: Any) -> None:
        """
        If the Nova already has a discovery_date earlier than any linked
        reference, UpsertDiscoveryDateMetadata must not overwrite it.
        The monotonically-earlier invariant must hold.
        """
        with mock_aws():
            # Seed nova with a discovery_date earlier than _RAW_DOC_B (1992-01-00)
            _seed_nova(table, discovery_date="1800-01-00")
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_A, _RAW_DOC_B])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                _run_map_item(h, state, _RAW_DOC_A)
                _run_map_item(h, state, _RAW_DOC_B)
                upsert_result = _run_post_map(h, state)

        assert upsert_result["updated"] is False
        nova = _get_nova(table)
        assert nova["discovery_date"] == "1800-01-00"

    def test_returns_updated_false_when_date_unchanged(self, table: Any) -> None:
        """UpsertDiscoveryDateMetadata returns updated=False when no-op."""
        with mock_aws():
            _seed_nova(table, discovery_date="1800-01-00")
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_B])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                _run_map_item(h, state, _RAW_DOC_B)
                upsert_result = _run_post_map(h, state)

        assert upsert_result["updated"] is False


# ---------------------------------------------------------------------------
# Path 4: Item-level quarantine — one candidate fails, Map continues
# ---------------------------------------------------------------------------


class TestItemLevelQuarantine:
    def test_quarantine_handler_fires_on_normalize_failure(self, table: Any) -> None:
        """
        When NormalizeReference raises (simulated by passing a doc with no
        bibcode), the ASL routes to ItemFailureHandler (QuarantineHandler).
        The handler should persist quarantine diagnostics onto the JobRun.
        """
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            state = _run_prefix(h)

            # Simulate NormalizeReference failing — bad_doc has no bibcode
            bad_doc: dict[str, Any] = {
                "bibcode": None,
                "doctype": "article",
                "title": None,
                "date": None,
                "author": [],
                "doi": None,
                "identifier": [],
            }

            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="bibcode"):
                h["reference_manager"].handle(
                    {
                        "task_name": "NormalizeReference",
                        "nova_id": state["nova_id"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run": state["job_run"],
                        **bad_doc,
                    },
                    None,
                )

            # ASL Catch routes to ItemFailureHandler (QuarantineHandler).
            # The item state at this point still has nova_id, bibcode (None here),
            # correlation_id, and job_run from the ItemSelector projection.
            quarantine_result = cast(
                dict[str, Any],
                h["quarantine_handler"].handle(
                    {
                        "task_name": "QuarantineHandler",
                        "workflow_name": "refresh_references",
                        "quarantine_reason_code": "OTHER",
                        "nova_id": state["nova_id"],
                        "bibcode": bad_doc["bibcode"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run": state["job_run"],
                    },
                    None,
                ),
            )

        assert quarantine_result["quarantine_reason_code"] == "OTHER"
        assert "error_fingerprint" in quarantine_result
        assert "quarantined_at" in quarantine_result

    def test_quarantine_diagnostics_written_to_job_run(self, table: Any) -> None:
        """QuarantineHandler persists quarantine fields onto the existing JobRun."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            state = _run_prefix(h)

            h["quarantine_handler"].handle(
                {
                    "task_name": "QuarantineHandler",
                    "workflow_name": "refresh_references",
                    "quarantine_reason_code": "OTHER",
                    "nova_id": state["nova_id"],
                    "bibcode": _BIBCODE_A,
                    "correlation_id": state["job_run"]["correlation_id"],
                    "job_run": state["job_run"],
                },
                None,
            )

        job_run_item = _get_job_run(table, state)
        assert job_run_item["quarantine_reason_code"] == "OTHER"
        assert "error_fingerprint" in job_run_item
        assert "quarantined_at" in job_run_item

    def test_remaining_items_succeed_after_quarantine(self, table: Any) -> None:
        """
        After one item is quarantined, the Map continues. The remaining
        good candidate (_RAW_DOC_B) is linked and its date is computed.
        """
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_B])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)
                h["reference_manager"].handle(
                    {
                        "task_name": "FetchReferenceCandidates",
                        "workflow_name": "refresh_references",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

            # Simulate first item quarantined — skip its Map steps
            h["quarantine_handler"].handle(
                {
                    "task_name": "QuarantineHandler",
                    "workflow_name": "refresh_references",
                    "quarantine_reason_code": "OTHER",
                    "nova_id": state["nova_id"],
                    "bibcode": _BIBCODE_A,
                    "correlation_id": state["job_run"]["correlation_id"],
                    "job_run": state["job_run"],
                },
                None,
            )

            # Second item (_RAW_DOC_B) succeeds
            _run_map_item(h, state, _RAW_DOC_B)
            _run_post_map(h, state)
            _run_finalize_success(h, state)

        # Only _BIBCODE_B should be linked
        novarefs = _list_novarefs(table)
        assert len(novarefs) == 1
        assert novarefs[0]["bibcode"] == _BIBCODE_B

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"

    def test_job_run_ends_succeeded_despite_quarantined_item(self, table: Any) -> None:
        """
        The Map's item-level quarantine path does not fail the workflow.
        FinalizeJobRunSuccess is still reachable after a quarantined item.
        """
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()
            state = _run_prefix(h)

            h["quarantine_handler"].handle(
                {
                    "task_name": "QuarantineHandler",
                    "workflow_name": "refresh_references",
                    "quarantine_reason_code": "OTHER",
                    "nova_id": state["nova_id"],
                    "bibcode": _BIBCODE_A,
                    "correlation_id": state["job_run"]["correlation_id"],
                    "job_run": state["job_run"],
                },
                None,
            )

            _run_post_map(h, state)
            _run_finalize_success(h, state)

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"


# ---------------------------------------------------------------------------
# Path 5: Terminal failure — FetchReferenceCandidates raises TerminalError
# ---------------------------------------------------------------------------


class TestTerminalFailure:
    def test_missing_nova_raises_terminal_error(self, table: Any) -> None:
        """
        FetchReferenceCandidates raises TerminalError when the nova does not
        exist in DDB. The ASL Catch routes to TerminalFailHandler.
        """
        with mock_aws():
            # Deliberately do NOT seed the nova
            h = _load_handlers()

            with patch.object(h["reference_manager"], "requests") as mock_requests:
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception

                state = _run_prefix(h)

                from nova_common.errors import TerminalError

                with pytest.raises(TerminalError, match="Nova not found"):
                    h["reference_manager"].handle(
                        {
                            "task_name": "FetchReferenceCandidates",
                            "workflow_name": "refresh_references",
                            "nova_id": _NOVA_ID,
                            "correlation_id": state["job_run"]["correlation_id"],
                            "job_run_id": state["job_run"]["job_run_id"],
                        },
                        None,
                    )

            # ASL routes to TerminalFailHandler then FinalizeJobRunFailed
            h["job_run_manager"].handle(
                {
                    "task_name": "TerminalFailHandler",
                    "workflow_name": "refresh_references",
                    "error": {"Error": "TerminalError", "Cause": "Nova not found"},
                    "correlation_id": state["job_run"]["correlation_id"],
                    "job_run_id": state["job_run"]["job_run_id"],
                    "job_run": state["job_run"],
                },
                None,
            )
            h["job_run_manager"].handle(
                {
                    "task_name": "FinalizeJobRunFailed",
                    "workflow_name": "refresh_references",
                    "error": {"Error": "TerminalError", "Cause": "Nova not found"},
                    "job_run": state["job_run"],
                },
                None,
            )

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "FAILED"

    def test_job_run_ends_failed_with_error_type(self, table: Any) -> None:
        """FinalizeJobRunFailed persists error_type onto the JobRun."""
        with mock_aws():
            h = _load_handlers()
            state = _run_prefix(h)

            h["job_run_manager"].handle(
                {
                    "task_name": "FinalizeJobRunFailed",
                    "workflow_name": "refresh_references",
                    "error": {"Error": "TerminalError", "Cause": "Nova not found in DDB"},
                    "job_run": state["job_run"],
                },
                None,
            )

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "FAILED"
        assert job_run_item["error_type"] == "TerminalError"

    def test_terminal_fail_handler_persists_fingerprint(self, table: Any) -> None:
        """
        TerminalFailHandler persists error_classification and error_fingerprint
        before FinalizeJobRunFailed runs. Both fields should be present on the
        final JobRun item.
        """
        with mock_aws():
            h = _load_handlers()
            state = _run_prefix(h)

            h["job_run_manager"].handle(
                {
                    "task_name": "TerminalFailHandler",
                    "workflow_name": "refresh_references",
                    "error": {"Error": "TerminalError", "Cause": "Nova not found"},
                    "correlation_id": state["job_run"]["correlation_id"],
                    "job_run_id": state["job_run"]["job_run_id"],
                    "job_run": state["job_run"],
                },
                None,
            )
            h["job_run_manager"].handle(
                {
                    "task_name": "FinalizeJobRunFailed",
                    "workflow_name": "refresh_references",
                    "error": {"Error": "TerminalError", "Cause": "Nova not found"},
                    "job_run": state["job_run"],
                },
                None,
            )

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "FAILED"
        assert job_run_item["error_classification"] == "TERMINAL"
        assert "error_fingerprint" in job_run_item


# ---------------------------------------------------------------------------
# Path 6: Idempotency — Map tasks are safe to run twice
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_running_map_items_twice_does_not_duplicate_novaref(self, table: Any) -> None:
        """
        LinkNovaReference uses a conditional put. Running the same item through
        the Map chain twice produces exactly one NOVAREF item, not two.
        This covers the case where a Map execution is retried.
        """
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()
            state = _run_prefix(h)

            # First pass
            _run_map_item(h, state, _RAW_DOC_A)
            # Second pass — simulates a Map retry
            _run_map_item(h, state, _RAW_DOC_A)

        novarefs = _list_novarefs(table)
        assert len(novarefs) == 1
        assert novarefs[0]["bibcode"] == _BIBCODE_A

    def test_upsert_reference_entity_is_idempotent(self, table: Any) -> None:
        """
        UpsertReferenceEntity preserves created_at across repeated calls.
        The Reference item is not duplicated on a retry.
        """
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()
            state = _run_prefix(h)

            _run_map_item(h, state, _RAW_DOC_A)
            original_created = table.get_item(
                Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"}
            )["Item"]["created_at"]

            # Second pass
            _run_map_item(h, state, _RAW_DOC_A)
            item_after_retry = table.get_item(
                Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"}
            )["Item"]

        assert item_after_retry["created_at"] == original_created

    def test_discovery_date_not_regressed_on_second_run(self, table: Any) -> None:
        """
        Running UpsertDiscoveryDateMetadata a second time with the same date
        is a no-op — the existing date is not overwritten with itself.
        """
        with mock_aws():
            _seed_nova(table)
            h = _load_handlers()
            state = _run_prefix(h)

            _run_map_item(h, state, _RAW_DOC_B)

            # First post-map run writes the date
            first_result = _run_post_map(h, state)
            assert first_result["updated"] is True
            assert first_result["discovery_date"] == "1992-01-00"

            # Second post-map run with the same date is a no-op
            second_result = _run_post_map(h, state)
            assert second_result["updated"] is False
            assert second_result["discovery_date"] == "1992-01-00"
