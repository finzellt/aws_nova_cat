"""
Unit tests for services/workflow_launcher/handler.py

Uses moto to mock Step Functions — no real AWS calls are made.

Covers:
  - PublishIngestNewNova: starts ingest_new_nova execution
  - PublishIngestNewNova: execution name is derived from nova_id + job_run_id
  - PublishIngestNewNova: execution input contains nova_id and correlation_id
  - PublishIngestNewNova: ExecutionAlreadyExists (ClientError) treated as success
  - PublishIngestNewNova: ThrottlingException (ClientError) raises RetryableError
  - Stub tasks raise NotImplementedError
  - Unknown task_name raises ValueError
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REGION = "us-east-1"
_ACCOUNT = "123456789012"
_STATE_MACHINE_NAME = "nova-cat-ingest-new-nova"
_STATE_MACHINE_ARN = f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:{_STATE_MACHINE_NAME}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required environment variables before handler import."""
    monkeypatch.setenv("INGEST_NEW_NOVA_STATE_MACHINE_ARN", _STATE_MACHINE_ARN)
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", "NovaCat-Test")
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN",
        f"arn:aws:sns:{_REGION}:{_ACCOUNT}:quarantine",
    )
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def state_machine(aws_env: None) -> Generator[Any, None, None]:
    """Create a mocked Step Functions state machine."""
    with mock_aws():
        sfn = boto3.client("stepfunctions", region_name=_REGION)
        sfn.create_state_machine(
            name=_STATE_MACHINE_NAME,
            definition=json.dumps(
                {
                    "StartAt": "Done",
                    "States": {"Done": {"Type": "Succeed"}},
                }
            ),
            roleArn=f"arn:aws:iam::{_ACCOUNT}:role/test-role",
        )
        yield sfn


def _load_handler() -> types.ModuleType:
    """Import handler fresh inside the moto context."""
    if "workflow_launcher.handler" in sys.modules:
        del sys.modules["workflow_launcher.handler"]
    return importlib.import_module("workflow_launcher.handler")


def _base_event(**kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "PublishIngestNewNova",
        "workflow_name": "initialize_nova",
        "nova_id": "nova-uuid-001",
        "correlation_id": "corr-001",
        "job_run_id": "job-run-abcdef12",
        **kwargs,
    }


# ---------------------------------------------------------------------------
# PublishIngestNewNova
# ---------------------------------------------------------------------------


class TestPublishIngestNewNova:
    def test_starts_execution_and_returns_arn(self, state_machine: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            assert "execution_arn" in result
            assert _STATE_MACHINE_NAME in result["execution_arn"]

    def test_returns_nova_id(self, state_machine: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            assert result["nova_id"] == "nova-uuid-001"

    def test_execution_name_derived_from_nova_id_and_job_run_id(self, state_machine: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(), None)
            # Format: "<nova_id>-<job_run_id[:8]>" -- "job-run-abcdef12"[:8] = "job-run-"
            assert result["execution_name"] == "nova-uuid-001-job-run-"

    def test_execution_input_contains_nova_id_and_correlation_id(self, state_machine: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(), None)
            sfn = boto3.client("stepfunctions", region_name=_REGION)
            executions = sfn.list_executions(stateMachineArn=_STATE_MACHINE_ARN)["executions"]
            assert len(executions) == 1
            desc = sfn.describe_execution(executionArn=executions[0]["executionArn"])
            payload = json.loads(desc["input"])
            assert payload["nova_id"] == "nova-uuid-001"
            assert payload["correlation_id"] == "corr-001"

    def test_execution_already_exists_treated_as_success(self, state_machine: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            already_exists_error = ClientError(
                {"Error": {"Code": "ExecutionAlreadyExists", "Message": "already exists"}},
                "StartExecution",
            )
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.side_effect = already_exists_error
                result = handler.handle(_base_event(), None)
            assert result["already_existed"] is True
            assert result["nova_id"] == "nova-uuid-001"

    def test_throttling_raises_retryable_error(self, state_machine: Any) -> None:
        from nova_common.errors import RetryableError

        with mock_aws():
            handler = _load_handler()
            throttle_error = ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
                "StartExecution",
            )
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.side_effect = throttle_error
                with pytest.raises(RetryableError):
                    handler.handle(_base_event(), None)


# ---------------------------------------------------------------------------
# Stub tasks raise NotImplementedError
# ---------------------------------------------------------------------------


class TestStubTasks:
    @pytest.mark.parametrize(
        "task_name",
        [
            "LaunchRefreshReferences",
            "LaunchDiscoverSpectraProducts",
            "PublishAcquireAndValidateSpectraRequests",
        ],
    )
    def test_stub_tasks_raise_not_implemented(self, state_machine: Any, task_name: str) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(NotImplementedError):
                handler.handle({"task_name": task_name}, None)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, state_machine: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)
