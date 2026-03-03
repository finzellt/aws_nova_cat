"""
workflow_launcher Lambda handler

Starts downstream Step Functions executions for Nova Cat workflows.

Task dispatch table:
  PublishIngestNewNova                     — start ingest_new_nova (initialize_nova)
  LaunchRefreshReferences                  — start refresh_references (ingest_new_nova)
  LaunchDiscoverSpectraProducts            — start discover_spectra_products (ingest_new_nova)
  PublishAcquireAndValidateSpectraRequests — start acquire_and_validate_spectra (discover_spectra_products)

Design notes:
  - Each task calls sfn:StartExecution on the appropriate state machine, passing
    a minimal continuation event: nova_id + correlation_id. Downstream workflows
    fetch any additional data they need from DynamoDB rather than receiving it
    in the continuation payload.
  - Execution names are derived from nova_id + job_run_id to be unique, stable,
    and traceable. SFN execution names must be ≤80 chars and unique per state
    machine — we use "<nova_id>-<job_run_id[:8]>" (36 + 1 + 8 = 45 chars).
  - ExecutionAlreadyExists is treated as success — if a retry of a launch task
    hits this, the execution is already running and the goal is achieved.
  - PublishAcquireAndValidateSpectraRequests is stubbed pending Epic 12+.

Environment variables (injected by CDK):
  NOVA_CAT_TABLE_NAME                        — DynamoDB table name (standard env)
  NOVA_CAT_PRIVATE_BUCKET                    — private S3 bucket (standard env)
  NOVA_CAT_PUBLIC_SITE_BUCKET                — public site S3 bucket (standard env)
  NOVA_CAT_QUARANTINE_TOPIC_ARN              — quarantine SNS topic ARN (standard env)
  INGEST_NEW_NOVA_STATE_MACHINE_ARN          — ARN of the ingest_new_nova state machine
  REFRESH_REFERENCES_STATE_MACHINE_ARN       — ARN of the refresh_references state machine
  DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN — ARN of the discover_spectra_products state machine
  LOG_LEVEL                                  — logging level (default INFO)
  POWERTOOLS_SERVICE_NAME                    — AWS Lambda Powertools service name
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError
from nova_common.errors import RetryableError
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

_INGEST_NEW_NOVA_STATE_MACHINE_ARN = os.environ["INGEST_NEW_NOVA_STATE_MACHINE_ARN"]
_REFRESH_REFERENCES_STATE_MACHINE_ARN = os.environ["REFRESH_REFERENCES_STATE_MACHINE_ARN"]
_DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN = os.environ[
    "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN"
]

_sfn = boto3.client("stepfunctions")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    handler_fn = _TASK_HANDLERS.get(task_name)  # type: ignore[arg-type]
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}")
    return handler_fn(event, context)


# ---------------------------------------------------------------------------
# Task implementations
# ---------------------------------------------------------------------------


@tracer.capture_method
def _publish_ingest_new_nova(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Start an ingest_new_nova Step Functions execution for the given nova_id.

    Called from initialize_nova on three paths:
      - EXISTS_AND_LAUNCHED  (name already known)
      - EXISTS_AND_LAUNCHED  (coordinate duplicate confirmed)
      - CREATED_AND_LAUNCHED (new nova established)
    """
    return _start_execution(
        state_machine_arn=_INGEST_NEW_NOVA_STATE_MACHINE_ARN,
        workflow_label="ingest_new_nova",
        nova_id=event["nova_id"],
        correlation_id=event["correlation_id"],
        job_run_id=event["job_run_id"],
    )


@tracer.capture_method
def _launch_refresh_references(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Start a refresh_references Step Functions execution for the given nova_id.

    Called from ingest_new_nova's LaunchDownstream Parallel state.
    Failure of this branch does NOT retroactively fail ingest_new_nova.
    """
    return _start_execution(
        state_machine_arn=_REFRESH_REFERENCES_STATE_MACHINE_ARN,
        workflow_label="refresh_references",
        nova_id=event["nova_id"],
        correlation_id=event["correlation_id"],
        job_run_id=event["job_run_id"],
    )


@tracer.capture_method
def _launch_discover_spectra_products(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Start a discover_spectra_products Step Functions execution for the given nova_id.

    Called from ingest_new_nova's LaunchDownstream Parallel state.
    Failure of this branch does NOT retroactively fail ingest_new_nova.
    """
    return _start_execution(
        state_machine_arn=_DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN,
        workflow_label="discover_spectra_products",
        nova_id=event["nova_id"],
        correlation_id=event["correlation_id"],
        job_run_id=event["job_run_id"],
    )


@tracer.capture_method
def _publish_acquire_and_validate_spectra_requests(
    event: dict[str, Any], context: object
) -> dict[str, Any]:
    raise NotImplementedError("PublishAcquireAndValidateSpectraRequests not yet implemented")


# ---------------------------------------------------------------------------
# Shared SFN helper
# ---------------------------------------------------------------------------


def _start_execution(
    *,
    state_machine_arn: str,
    workflow_label: str,
    nova_id: str,
    correlation_id: str,
    job_run_id: str,
) -> dict[str, Any]:
    """
    Start a Step Functions execution with a minimal continuation event.

    Execution name format: "<nova_id>-<job_run_id[:8]>"
    = 36 + 1 + 8 = 45 chars (well under the 80-char SFN limit).

    ExecutionAlreadyExists is treated as idempotent success — if a task
    retry hits this, the execution is already running and the goal is met.

    Raises RetryableError on throttling or transient SFN failures.
    """
    execution_name = f"{nova_id}-{job_run_id[:8]}"
    execution_input = {
        "nova_id": nova_id,
        "correlation_id": correlation_id,
    }

    try:
        response = _sfn.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(execution_input),
        )
    except _sfn.exceptions.ExecutionAlreadyExists:
        logger.info(
            f"{workflow_label} execution already exists — treating as success",
            extra={"execution_name": execution_name, "nova_id": nova_id},
        )
        return {
            "nova_id": nova_id,
            "execution_name": execution_name,
            "already_existed": True,
        }
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ExecutionAlreadyExists":
            # moto raises ClientError rather than the typed exception —
            # handle both for test compatibility.
            logger.info(
                f"{workflow_label} execution already exists — treating as success",
                extra={"execution_name": execution_name, "nova_id": nova_id},
            )
            return {
                "nova_id": nova_id,
                "execution_name": execution_name,
                "already_existed": True,
            }
        if code in {"ThrottlingException", "ServiceUnavailable", "InternalServerError"}:
            raise RetryableError(
                f"SFN StartExecution transient failure ({code}) for "
                f"{workflow_label} nova_id={nova_id}"
            ) from e
        raise

    execution_arn: str = response["executionArn"]

    logger.info(
        f"{workflow_label} execution started",
        extra={"nova_id": nova_id, "execution_arn": execution_arn},
    )

    return {
        "nova_id": nova_id,
        "execution_arn": execution_arn,
        "execution_name": execution_name,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "PublishIngestNewNova": _publish_ingest_new_nova,
    "LaunchRefreshReferences": _launch_refresh_references,
    "LaunchDiscoverSpectraProducts": _launch_discover_spectra_products,
    "PublishAcquireAndValidateSpectraRequests": _publish_acquire_and_validate_spectra_requests,
}
