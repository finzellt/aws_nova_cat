"""
Integration tests for the ingest_new_nova workflow.

Simulates the full Step Functions execution by calling each Lambda handler
directly in ASL order, sharing a single mocked DynamoDB instance.
workflow_launcher's SFN calls are patched throughout.

Workflow order (per ingest_new_nova.asl.json):
  BeginJobRun → AcquireIdempotencyLock → LaunchDownstream (Parallel) →
  FinalizeJobRunSuccess | TerminalFailHandler

Paths covered:
  1. Happy path — both LaunchRefreshReferences and LaunchDiscoverSpectraProducts
     succeed; JobRun ends SUCCEEDED with outcome LAUNCHED
  2. Failure path — one downstream launch raises a terminal error; ASL routes
     to TerminalFailHandler; JobRun ends FAILED
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
from botocore.exceptions import ClientError
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"
_ACCOUNT = "123456789012"

_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"
_CORRELATION_ID = "integ-ingest-corr-001"

_SM_ARNS = {
    "ingest_new_nova": f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-ingest-new-nova",
    "refresh_references": f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-refresh-references",
    "discover_spectra_products": f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-discover-spectra-products",
}
_FAKE_EXECUTION_ARN = (
    f"arn:aws:states:{_REGION}:{_ACCOUNT}:execution:nova-cat-refresh-references:test"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", f"arn:aws:sns:{_REGION}:{_ACCOUNT}:quarantine"
    )
    monkeypatch.setenv("INGEST_NEW_NOVA_STATE_MACHINE_ARN", _SM_ARNS["ingest_new_nova"])
    monkeypatch.setenv("REFRESH_REFERENCES_STATE_MACHINE_ARN", _SM_ARNS["refresh_references"])
    monkeypatch.setenv(
        "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN", _SM_ARNS["discover_spectra_products"]
    )
    monkeypatch.setenv(
        "ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN",
        f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-acquire-and-validate-spectra",
    )
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", "nova-cat-private-test")
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-public-test")
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


# ---------------------------------------------------------------------------
# Handler loader
# ---------------------------------------------------------------------------


def _load_handlers() -> dict[str, types.ModuleType]:
    for mod_name in [
        "job_run_manager.handler",
        "idempotency_guard.handler",
        "workflow_launcher.handler",
    ]:
        if mod_name in sys.modules:
            del sys.modules[mod_name]
    return {
        "job_run_manager": importlib.import_module("job_run_manager.handler"),
        "idempotency_guard": importlib.import_module("idempotency_guard.handler"),
        "workflow_launcher": importlib.import_module("workflow_launcher.handler"),
    }


# ---------------------------------------------------------------------------
# Workflow runner helpers
# ---------------------------------------------------------------------------


def _run_prefix(h: dict[str, types.ModuleType]) -> dict[str, Any]:
    """
    Run the common prefix: BeginJobRun → AcquireIdempotencyLock.
    Returns accumulated state dict.
    """
    job_run = cast(
        dict[str, Any],
        h["job_run_manager"].handle(
            {
                "task_name": "BeginJobRun",
                "workflow_name": "ingest_new_nova",
                "nova_id": _NOVA_ID,
                "correlation_id": _CORRELATION_ID,
            },
            None,
        ),
    )

    h["idempotency_guard"].handle(
        {
            "task_name": "AcquireIdempotencyLock",
            "workflow_name": "ingest_new_nova",
            "primary_id": _NOVA_ID,
            "correlation_id": job_run["correlation_id"],
            "job_run_id": job_run["job_run_id"],
        },
        None,
    )

    return {"nova_id": _NOVA_ID, "job_run": job_run}


def _get_job_run(table: Any, state: dict[str, Any]) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        table.get_item(Key={"PK": state["job_run"]["pk"], "SK": state["job_run"]["sk"]})["Item"],
    )


# ---------------------------------------------------------------------------
# Path 1: Happy path — both branches succeed
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_both_branches_succeed(self, table: Any) -> None:
        """
        Both LaunchRefreshReferences and LaunchDiscoverSpectraProducts start
        their executions successfully. JobRun ends SUCCEEDED with outcome LAUNCHED.
        """
        with mock_aws():
            h = _load_handlers()

            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h)

                rr_result = cast(
                    dict[str, Any],
                    h["workflow_launcher"].handle(
                        {
                            "task_name": "LaunchRefreshReferences",
                            "workflow_name": "ingest_new_nova",
                            "nova_id": _NOVA_ID,
                            "correlation_id": state["job_run"]["correlation_id"],
                            "job_run_id": state["job_run"]["job_run_id"],
                        },
                        None,
                    ),
                )
                assert "execution_arn" in rr_result

                dsp_result = cast(
                    dict[str, Any],
                    h["workflow_launcher"].handle(
                        {
                            "task_name": "LaunchDiscoverSpectraProducts",
                            "workflow_name": "ingest_new_nova",
                            "nova_id": _NOVA_ID,
                            "correlation_id": state["job_run"]["correlation_id"],
                            "job_run_id": state["job_run"]["job_run_id"],
                        },
                        None,
                    ),
                )
                assert "execution_arn" in dsp_result

                h["job_run_manager"].handle(
                    {
                        "task_name": "FinalizeJobRunSuccess",
                        "workflow_name": "ingest_new_nova",
                        "outcome": "LAUNCHED",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                        "job_run": state["job_run"],
                    },
                    None,
                )

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["outcome"] == "LAUNCHED"

    def test_sfn_called_twice_for_both_branches(self, table: Any) -> None:
        """Confirms both downstream executions are started, not just one."""
        with mock_aws():
            h = _load_handlers()

            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h)

                h["workflow_launcher"].handle(
                    {
                        "task_name": "LaunchRefreshReferences",
                        "workflow_name": "ingest_new_nova",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                h["workflow_launcher"].handle(
                    {
                        "task_name": "LaunchDiscoverSpectraProducts",
                        "workflow_name": "ingest_new_nova",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

            assert mock_sfn.start_execution.call_count == 2

    def test_both_branches_idempotent_on_retry(self, table: Any) -> None:
        """
        If both launches return ExecutionAlreadyExists (retry of the Parallel
        state), both are treated as success and the workflow completes normally.
        """
        with mock_aws():
            h = _load_handlers()

            already_exists = ClientError(
                {"Error": {"Code": "ExecutionAlreadyExists", "Message": "exists"}},
                "StartExecution",
            )

            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.side_effect = already_exists

                state = _run_prefix(h)

                rr = cast(
                    dict[str, Any],
                    h["workflow_launcher"].handle(
                        {
                            "task_name": "LaunchRefreshReferences",
                            "workflow_name": "ingest_new_nova",
                            "nova_id": _NOVA_ID,
                            "correlation_id": state["job_run"]["correlation_id"],
                            "job_run_id": state["job_run"]["job_run_id"],
                        },
                        None,
                    ),
                )
                dsp = cast(
                    dict[str, Any],
                    h["workflow_launcher"].handle(
                        {
                            "task_name": "LaunchDiscoverSpectraProducts",
                            "workflow_name": "ingest_new_nova",
                            "nova_id": _NOVA_ID,
                            "correlation_id": state["job_run"]["correlation_id"],
                            "job_run_id": state["job_run"]["job_run_id"],
                        },
                        None,
                    ),
                )

            assert rr["already_existed"] is True
            assert dsp["already_existed"] is True

            h["job_run_manager"].handle(
                {
                    "task_name": "FinalizeJobRunSuccess",
                    "workflow_name": "ingest_new_nova",
                    "outcome": "LAUNCHED",
                    "nova_id": _NOVA_ID,
                    "correlation_id": state["job_run"]["correlation_id"],
                    "job_run_id": state["job_run"]["job_run_id"],
                    "job_run": state["job_run"],
                },
                None,
            )

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["outcome"] == "LAUNCHED"


# ---------------------------------------------------------------------------
# Path 2: Failure path — one branch raises a terminal error
# ---------------------------------------------------------------------------


class TestFailurePath:
    def test_terminal_error_routes_to_fail_handler(self, table: Any) -> None:
        """
        If one branch raises a non-retryable error, the ASL routes to
        TerminalFailHandler. JobRun ends FAILED.

        In the real ASL the Parallel state catches States.ALL and routes to
        TerminalFailHandler. Here we simulate that by catching the exception
        from the handler call and calling FinalizeJobRunFailed directly.
        """
        with mock_aws():
            h = _load_handlers()

            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                # First call (LaunchRefreshReferences) succeeds
                # Second call (LaunchDiscoverSpectraProducts) raises terminal error
                mock_sfn.start_execution.side_effect = [
                    {"executionArn": _FAKE_EXECUTION_ARN},
                    ClientError(
                        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
                        "StartExecution",
                    ),
                ]

                state = _run_prefix(h)

                h["workflow_launcher"].handle(
                    {
                        "task_name": "LaunchRefreshReferences",
                        "workflow_name": "ingest_new_nova",
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                with pytest.raises(ClientError):
                    h["workflow_launcher"].handle(
                        {
                            "task_name": "LaunchDiscoverSpectraProducts",
                            "workflow_name": "ingest_new_nova",
                            "nova_id": _NOVA_ID,
                            "correlation_id": state["job_run"]["correlation_id"],
                            "job_run_id": state["job_run"]["job_run_id"],
                        },
                        None,
                    )

            # ASL would route to TerminalFailHandler — simulate that here
            h["job_run_manager"].handle(
                {
                    "task_name": "FinalizeJobRunFailed",
                    "workflow_name": "ingest_new_nova",
                    "error": {
                        "Error": "ClientError",
                        "Cause": "AccessDeniedException: denied",
                    },
                    "job_run": state["job_run"],
                },
                None,
            )

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "FAILED"
        assert job_run_item["error_type"] == "ClientError"
