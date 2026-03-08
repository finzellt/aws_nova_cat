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
  - PublishAcquireAndValidateSpectraRequests: empty list, single product,
    multiple products, ExecutionAlreadyExists as success, partial failure,
    execution name format, continuation event payload, target ARN
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
    "acquire_and_validate_spectra": "nova-cat-acquire-and-validate-spectra",
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
    monkeypatch.setenv(
        "ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN",
        _SM_ARNS["acquire_and_validate_spectra"],
    )
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", "NovaCat-Test")
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", "nova-cat-private-test")
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-public-test")
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
    """Create all mocked Step Functions state machines."""
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
# PublishAcquireAndValidateSpectraRequests
# ---------------------------------------------------------------------------

_FAKE_CORRELATION_ID = "corr-001"


def _publish_event(persisted_products: list[dict], **kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "PublishAcquireAndValidateSpectraRequests",
        "nova_id": _FAKE_NOVA_ID,
        "correlation_id": _FAKE_CORRELATION_ID,
        "job_run": {
            "job_run_id": _FAKE_JOB_RUN_ID,
            "correlation_id": _FAKE_CORRELATION_ID,
        },
        "persisted_products": persisted_products,
        **kwargs,
    }


def _product(data_product_id: str, provider: str = "ESO") -> dict[str, Any]:
    return {"data_product_id": data_product_id, "provider": provider, "nova_id": _FAKE_NOVA_ID}


def _sfn_success_response(execution_arn: str) -> dict[str, Any]:
    return {"executionArn": execution_arn, "startDate": "2026-01-01T00:00:00Z"}


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "StartExecution")


class TestPublishAcquireAndValidateSpectraRequests:
    def test_empty_list_returns_zero_counts(self, state_machines: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                result = handler.handle(_publish_event([]), None)
            mock_sfn.start_execution.assert_not_called()
        assert result["total"] == 0
        assert result["launched"] == []
        assert result["failed"] == []

    def test_single_product_launches_one_execution(self, state_machines: Any) -> None:
        dp_id = "aaaaaaaa-0000-0000-0000-000000000001"
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = _sfn_success_response(
                    f"arn:aws:states:::execution:acquire:{dp_id}"
                )
                result = handler.handle(_publish_event([_product(dp_id)]), None)
            mock_sfn.start_execution.assert_called_once()
        assert result["total"] == 1
        assert len(result["launched"]) == 1
        assert result["failed"] == []

    def test_multiple_products_all_launched(self, state_machines: Any) -> None:
        products = [_product(f"aaaaaaaa-0000-0000-0000-{i:012d}") for i in range(3)]
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = _sfn_success_response(
                    "arn:aws:states:::execution:acquire:test"
                )
                result = handler.handle(_publish_event(products), None)
            assert mock_sfn.start_execution.call_count == 3
        assert result["total"] == 3
        assert len(result["launched"]) == 3
        assert result["failed"] == []

    def test_execution_already_exists_treated_as_success(self, state_machines: Any) -> None:
        dp_id = "aaaaaaaa-0000-0000-0000-000000000001"
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.side_effect = _client_error("ExecutionAlreadyExists")
                result = handler.handle(_publish_event([_product(dp_id)]), None)
        assert result["total"] == 1
        assert len(result["launched"]) == 1
        assert result["launched"][0]["already_existed"] is True
        assert result["failed"] == []

    def test_partial_sfn_failure_collected_without_raising(self, state_machines: Any) -> None:
        good_id = "aaaaaaaa-0000-0000-0000-000000000001"
        bad_id = "bbbbbbbb-0000-0000-0000-000000000002"

        def _side_effect(**kwargs: Any) -> dict:
            if bad_id in kwargs.get("name", ""):
                raise _client_error("InternalServerError")
            return _sfn_success_response("arn:aws:states:::execution:acquire:test")

        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.side_effect = _side_effect
                result = handler.handle(_publish_event([_product(good_id), _product(bad_id)]), None)
        assert result["total"] == 2
        assert len(result["launched"]) == 1
        assert len(result["failed"]) == 1
        assert result["failed"][0]["data_product_id"] == bad_id

    def test_execution_name_uses_data_product_id_not_nova_id(self, state_machines: Any) -> None:
        dp_id = "aaaaaaaa-0000-0000-0000-000000000001"
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = _sfn_success_response(
                    "arn:aws:states:::execution:acquire:test"
                )
                handler.handle(_publish_event([_product(dp_id)]), None)
            _, kwargs = mock_sfn.start_execution.call_args
        assert dp_id in kwargs["name"]
        assert _FAKE_NOVA_ID not in kwargs["name"]
        assert _FAKE_JOB_RUN_ID[:8] in kwargs["name"]

    def test_continuation_event_contains_required_fields(self, state_machines: Any) -> None:
        dp_id = "aaaaaaaa-0000-0000-0000-000000000001"
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = _sfn_success_response(
                    "arn:aws:states:::execution:acquire:test"
                )
                handler.handle(_publish_event([_product(dp_id)]), None)
            _, kwargs = mock_sfn.start_execution.call_args
        payload = json.loads(kwargs["input"])
        assert payload["nova_id"] == _FAKE_NOVA_ID
        assert payload["data_product_id"] == dp_id
        assert payload["provider"] == "ESO"
        assert payload["correlation_id"] == _FAKE_CORRELATION_ID

    def test_targets_acquire_and_validate_spectra_state_machine(self, state_machines: Any) -> None:
        dp_id = "aaaaaaaa-0000-0000-0000-000000000001"
        with mock_aws():
            handler = _load_handler()
            with patch.object(handler, "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = _sfn_success_response(
                    "arn:aws:states:::execution:acquire:test"
                )
                handler.handle(_publish_event([_product(dp_id)]), None)
            _, kwargs = mock_sfn.start_execution.call_args
        assert kwargs["stateMachineArn"] == _SM_ARNS["acquire_and_validate_spectra"]


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, state_machines: Any) -> None:
        with mock_aws():
            handler = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)
