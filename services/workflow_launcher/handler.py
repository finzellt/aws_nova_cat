"""
workflow_launcher Lambda handler

Starts downstream Step Functions executions for Nova Cat workflows.

Task dispatch table:
  PublishIngestNewNova                    — start ingest_new_nova execution (initialize_nova)
  LaunchRefreshReferences                 — start refresh_references execution (ingest_new_nova)
  LaunchDiscoverSpectraProducts           — start discover_spectra_products execution (ingest_new_nova)
  PublishAcquireAndValidateSpectraRequests — start acquire_and_validate_spectra executions (discover_spectra_products)

Design notes:
  - Each task calls sfn:StartExecution on the appropriate state machine, passing
    a continuation event as the execution input. The continuation event always
    includes nova_id and correlation_id so downstream workflows can propagate
    the same operational context.
  - Execution names are derived from nova_id + job_run_id to be unique, stable,
    and traceable. SFN execution names must be ≤80 chars and unique per state
    machine — we use "<nova_id>-<job_run_id[:8]>" (36 + 1 + 8 = 45 chars).
  - LaunchRefreshReferences, LaunchDiscoverSpectraProducts, and
    PublishAcquireAndValidateSpectraRequests are stubs pending their respective
    workflow implementations (Epic 11+).

Environment variables (injected by CDK):
  NOVA_CAT_TABLE_NAME                — DynamoDB table name (unused here; standard env)
  NOVA_CAT_PRIVATE_BUCKET            — private S3 bucket (unused here; standard env)
  NOVA_CAT_PUBLIC_SITE_BUCKET        — public site S3 bucket (unused here; standard env)
  NOVA_CAT_QUARANTINE_TOPIC_ARN      — quarantine SNS topic ARN (unused here; standard env)
  INGEST_NEW_NOVA_STATE_MACHINE_ARN  — ARN of the ingest_new_nova state machine
  LOG_LEVEL                          — logging level (default INFO)
  POWERTOOLS_SERVICE_NAME            — AWS Lambda Powertools service name
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

    The execution input is a continuation event containing nova_id and
    correlation_id, so ingest_new_nova can propagate the same operational
    context through its own JobRun and downstream workflows.

    Raises RetryableError on SFN throttling or transient failures so that
    Step Functions retries per the ASL policy.

    Returns:
        execution_arn  — ARN of the started ingest_new_nova execution
        nova_id        — echoed from event (for downstream ResultPath merging)
    """
    nova_id: str = event["nova_id"]
    correlation_id: str = event["correlation_id"]
    job_run_id: str = event["job_run_id"]

    # Execution name: unique, stable, traceable.
    # Format: "<nova_id>-<job_run_id[:8]>" = 36 + 1 + 8 = 45 chars (well under 80).
    execution_name = f"{nova_id}-{job_run_id[:8]}"

    # Continuation event passed as input to ingest_new_nova.
    # Includes nova_id (required by IngestNewNovaEvent) and correlation_id
    # so the downstream workflow propagates the same operational context.
    execution_input = {
        "nova_id": nova_id,
        "correlation_id": correlation_id,
    }

    try:
        response = _sfn.start_execution(
            stateMachineArn=_INGEST_NEW_NOVA_STATE_MACHINE_ARN,
            name=execution_name,
            input=json.dumps(execution_input),
        )
    except _sfn.exceptions.ExecutionAlreadyExists:
        # Idempotent: if this execution name already exists (e.g. a retry of
        # PublishIngestNewNova after a transient failure), treat it as success.
        logger.info(
            "ingest_new_nova execution already exists — treating as success",
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
                "ingest_new_nova execution already exists — treating as success",
                extra={"execution_name": execution_name, "nova_id": nova_id},
            )
            return {
                "nova_id": nova_id,
                "execution_name": execution_name,
                "already_existed": True,
            }
        if code in {"ThrottlingException", "ServiceUnavailable", "InternalServerError"}:
            raise RetryableError(
                f"SFN StartExecution transient failure ({code}) for nova_id={nova_id}"
            ) from e
        raise

    execution_arn: str = response["executionArn"]

    logger.info(
        "ingest_new_nova execution started",
        extra={"nova_id": nova_id, "execution_arn": execution_arn},
    )

    return {
        "nova_id": nova_id,
        "execution_arn": execution_arn,
        "execution_name": execution_name,
    }


@tracer.capture_method
def _launch_refresh_references(event: dict[str, Any], context: object) -> dict[str, Any]:
    raise NotImplementedError("LaunchRefreshReferences not yet implemented")


@tracer.capture_method
def _launch_discover_spectra_products(event: dict[str, Any], context: object) -> dict[str, Any]:
    raise NotImplementedError("LaunchDiscoverSpectraProducts not yet implemented")


@tracer.capture_method
def _publish_acquire_and_validate_spectra_requests(
    event: dict[str, Any], context: object
) -> dict[str, Any]:
    raise NotImplementedError("PublishAcquireAndValidateSpectraRequests not yet implemented")


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
