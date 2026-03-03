"""
Unit tests for services/workflow_launcher/handler.py

Uses moto to mock Step Functions — no real AWS calls are made.

Covers:
  - PublishIngestNewNova: starts ingest_new_nova execution
  - PublishIngestNewNova: ExecutionAlreadyExists (ClientError) treated as success
  - PublishIngestNewNova: ThrottlingException (ClientError) raises RetryableError
  - LaunchRefreshReferences: starts refresh_references execution
  - LaunchRefreshReferences: ExecutionAlreadyExists treated as success
  - LaunchDiscoverSpectraProducts: starts discover_spectra_products execution
  - LaunchDiscoverSpectraProducts: ExecutionAlreadyExists treated as success
  - All three tasks: execution name is derived from nova_id + job_run_id
  - All three tasks: execution input contains nova_id and correlation_id
  - PublishAcquireAndValidateSpectraRequests raises NotImplementedError
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

_SM_NAMES = {
    "ingest_new_nova": "nova-cat-ingest-new-nova",
    "refresh_references": "nova-cat-refresh-references",
    "discover_spectra_products": "nova-cat-discover-spectra-products",
}
_SM_ARNS = {
    k: f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:{v}" for k, v in _SM_NAMES.items()
}
_FAKE_NOVA_ID = "nova-uuid-001"
_FAKE_JOB_RUN_ID = "job-run-abcdef12"
_EXPECTED_EXECUTION_NAME = f"{_FAKE_NOVA_ID}-{_FAKE_JOB_RUN_ID[:8]}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_NEW_NOVA_STATE_MACHINE_ARN", _SM_ARNS["ingest_new_nova"])
    monkeypatch.setenv("REFRESH_REFERENCES_STATE_MACHINE_ARN", _SM_ARNS["refresh_references"])
    monkeypatch.setenv(
        "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN", _SM_ARNS["discover_spectra_products"]
    )
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", "NovaCat-Test")
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", f"arn:aws:sns:{_REGION}:{_ACCOUNT}:quarantine"
    )
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def state_machines(aws_env: None) -> Generator[Any, None, None]:
    """Create all three mocked Step Functions state machines."""
    with mock_aws():
        sfn = boto3.client("stepfunctions", region_name=_REGION)
        for name in _SM_NAMES.values():
            sfn.create_state_machine(
                name=name,
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
    if "workflow_launcher.handler" in sys.modules:
        del sys.modules["workflow_launcher.handler"]
    return importlib.import_module("workflow_launcher.handler")


def _base_event(task_name: str, **kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": task_name,
        "workflow_name": "ingest_new_nova",
        "nova_id": _FAKE_NOVA_ID,
        "correlation_id": "corr-001",
        "job_run_id": _FAKE_JOB_RUN_ID,
        **kwargs,
    }


def _already_exists_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "ExecutionAlreadyExists", "Message": "already exists"}},
        "StartExecution",
    )


def _throttle_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
        "StartExecution",
    )


# ---------------------------------------------------------------------------
# Shared behaviour — parametrized across all three real launch tasks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "task_name,sm_key",
    [
        ("PublishIngestNewNova", "ingest_new_nova"),
        ("LaunchRefreshReferences", "refresh_references"),
        ("LaunchDiscoverSpectraProducts", "discover_spectra_products"),
    ],
)
class TestLaunchTasks:
    def test_starts_execution_and_returns_arn(
        self, state_machines: Any, task_name: str, sm_key: str
    ) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(task_name), None)
            assert "execution_arn" in result
            assert _SM_NAMES[sm_key] in result["execution_arn"]

    def test_returns_nova_id(self, state_machines: Any, task_name: str, sm_key: str) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(task_name), None)
            assert result["nova_id"] == _FAKE_NOVA_ID

    def test_execution_name_derived_from_nova_id_and_job_run_id(
        self, state_machines: Any, task_name: str, sm_key: str
    ) -> None:
        with mock_aws():
            handler = _load_handler()
            result = handler.handle(_base_event(task_name), None)
            assert result["execution_name"] == _EXPECTED_EXECUTION_NAME

    def test_execution_input_contains_nova_id_and_correlation_id(
        self, state_machines: Any, task_name: str, sm_key: str
    ) -> None:
        with mock_aws():
            handler = _load_handler()
            handler.handle(_base_event(task_name), None)
            sfn = boto3.client("stepfunctions", region_name=_REGION)
            executions = sfn.list_executions(stateMachineArn=_SM_ARNS[sm_key])["executions"]
            assert len(executions) == 1
            desc = sfn.describe_execution(executionArn=executions[0]["executionArn"])
            payload = json.loads(desc["input"])
            assert payload["nova_id"] == _FAKE_NOVA_ID
            assert payload["correlation_id"] == "corr-001"

    def test_execution_already_exists_treated_as_success(
        self, state_machines: Any, task_name: str, sm_key: str
    ) -> None:
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.side_effect = _already_exists_error()
                result = handler.handle(_base_event(task_name), None)
            assert result["already_existed"] is True
            assert result["nova_id"] == _FAKE_NOVA_ID


# ---------------------------------------------------------------------------
# Throttling — spot check on one task (shared _start_execution path)
# ---------------------------------------------------------------------------


class TestThrottling:
    def test_throttling_raises_retryable_error(self, state_machines: Any) -> None:
        from nova_common.errors import RetryableError

        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.side_effect = _throttle_error()
                with pytest.raises(RetryableError):
                    handler.handle(_base_event("LaunchRefreshReferences"), None)


# ---------------------------------------------------------------------------
# Stub task
# ---------------------------------------------------------------------------


class TestStubTasks:
    def test_publish_acquire_and_validate_raises_not_implemented(self, state_machines: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(NotImplementedError):
                handler.handle({"task_name": "PublishAcquireAndValidateSpectraRequests"}, None)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, state_machines: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)
