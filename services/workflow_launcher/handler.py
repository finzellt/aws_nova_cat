"""
workflow_launcher Lambda handler

Starts downstream Step Functions executions for Nova Cat workflows.

Task dispatch table:
  PublishIngestNewNova                     — start ingest_new_nova (initialize_nova)
  LaunchRefreshReferences                  — start refresh_references (ingest_new_nova)
  LaunchDiscoverSpectraProducts            — start discover_spectra_products (ingest_new_nova)
  PublishAcquireAndValidateSpectraRequests — start one acquire_and_validate_spectra execution
                                            per eligible data_product_id (discover_spectra_products)

Design notes:
  - Each task calls sfn:StartExecution on the appropriate state machine, passing
    a minimal continuation event. Downstream workflows fetch any additional data
    they need from DynamoDB rather than receiving it in the continuation payload.
  - For PublishAcquireAndValidateSpectraRequests, sfn:StartExecution is called
    once per product in a Python loop. All N executions are non-blocking and run
    in parallel as independent Standard Workflow state machines — no SNS/SQS fan-out
    is required.
  - Execution names:
      Single-product workflows: "<nova_id>-<job_run_id[:8]>" (45 chars)
      Per-product launches:     "<data_product_id>-<job_run_id[:8]>" (45 chars)
    Both are well under the SFN 80-char execution name limit and are unique + traceable.
  - ExecutionAlreadyExists is treated as idempotent success — if a retry of a
    launch task hits this, the execution is already running and the goal is met.

Environment variables (injected by CDK):
  NOVA_CAT_TABLE_NAME                          — DynamoDB table name (standard env)
  NOVA_CAT_PRIVATE_BUCKET                      — private S3 bucket (standard env)
  NOVA_CAT_PUBLIC_SITE_BUCKET                  — public site S3 bucket (standard env)
  NOVA_CAT_QUARANTINE_TOPIC_ARN                — quarantine SNS topic ARN (standard env)
  INGEST_NEW_NOVA_STATE_MACHINE_ARN            — ARN of the ingest_new_nova state machine
  REFRESH_REFERENCES_STATE_MACHINE_ARN         — ARN of the refresh_references state machine
  DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN  — ARN of the discover_spectra_products state machine
  ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN — ARN of the acquire_and_validate_spectra state machine
  LOG_LEVEL                                    — logging level (default INFO)
  POWERTOOLS_SERVICE_NAME                      — AWS Lambda Powertools service name
"""

from __future__ import annotations

import json
import os
import time
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
_ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN = os.environ[
    "ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN"
]

_sfn = boto3.client("stepfunctions")
_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(os.environ["NOVA_CAT_TABLE_NAME"])


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
    """
    Start one acquire_and_validate_spectra execution per newly eligible data_product_id.

    sfn:StartExecution is non-blocking — all N executions are dispatched in a
    Python loop and run in parallel as independent Standard Workflow state machines.
    No SNS/SQS fan-out is required.

    Each execution receives a minimal continuation event containing the fields
    required by AcquireAndValidateSpectraEvent:
        nova_id, provider, data_product_id, correlation_id

    Individual launch failures are logged and collected but do NOT raise —
    the task returns a summary of launched vs failed so the caller can
    observe partial failures without aborting the Map iteration.

    Returns:
        launched — list of successfully started executions
        failed   — list of products that could not be launched (with error)
        total    — total products attempted
    """
    nova_id: str = event["nova_id"]
    correlation_id: str = event["correlation_id"]
    job_run_id: str = event["job_run"]["job_run_id"]
    # NOTE: "persisted_products" includes both newly-stubbed products (is_new=True)
    # and existing UNVALIDATED products re-queued for acquisition (is_new=False).
    # The name is historical; treat it as "eligible_for_acquisition".
    persisted_products: list[dict[str, Any]] = event["persisted_products"]

    launched: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    for product in persisted_products:
        data_product_id: str = product["data_product_id"]
        provider: str = product["provider"]

        try:
            result = _start_execution(
                state_machine_arn=_ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN,
                workflow_label="acquire_and_validate_spectra",
                nova_id=nova_id,
                correlation_id=correlation_id,
                job_run_id=job_run_id,
                data_product_id=data_product_id,
                provider=provider,
            )
            # Persist the execution ARN on the DataProduct so failed executions
            # can be looked up directly without scanning all SFN executions.
            execution_arn = result.get("execution_arn")
            if execution_arn:
                _record_execution_arn(
                    nova_id=nova_id,
                    provider=provider,
                    data_product_id=data_product_id,
                    execution_arn=execution_arn,
                )
            launched.append({"data_product_id": data_product_id, "provider": provider, **result})
            time.sleep(0.25)  # stagger launches to avoid Lambda concurrency burst
        except Exception as exc:
            logger.error(
                "Failed to launch acquire_and_validate_spectra execution",
                extra={
                    "data_product_id": data_product_id,
                    "provider": provider,
                    "error": str(exc),
                },
            )
            failed.append(
                {
                    "data_product_id": data_product_id,
                    "provider": provider,
                    "error": str(exc),
                }
            )

    logger.info(
        "PublishAcquireAndValidateSpectraRequests complete",
        extra={
            "nova_id": nova_id,
            "launched": len(launched),
            "failed": len(failed),
            "total": len(persisted_products),
        },
    )

    return {
        "launched": launched,
        "failed": failed,
        "total": len(persisted_products),
    }


# ---------------------------------------------------------------------------
# Shared SFN helper
# ---------------------------------------------------------------------------


def _record_execution_arn(
    *,
    nova_id: str,
    provider: str,
    data_product_id: str,
    execution_arn: str,
) -> None:
    """
    Persist the SFN execution ARN on the DataProduct item.

    Written as last_execution_arn so that repeated re-runs always reflect
    the most recent launch. Failures are logged but not raised — an ARN
    write failure must never abort the fan-out loop.
    """
    try:
        _table.update_item(
            Key={
                "PK": nova_id,
                "SK": f"PRODUCT#SPECTRA#{provider}#{data_product_id}",
            },
            UpdateExpression="SET last_execution_arn = :arn",
            ExpressionAttributeValues={":arn": execution_arn},
        )
    except Exception as exc:
        logger.warning(
            "Failed to record execution_arn on DataProduct — non-fatal",
            extra={
                "data_product_id": data_product_id,
                "execution_arn": execution_arn,
                "error": str(exc),
            },
        )


def _start_execution(
    *,
    state_machine_arn: str,
    workflow_label: str,
    nova_id: str,
    correlation_id: str,
    job_run_id: str,
    data_product_id: str | None = None,
    provider: str | None = None,
) -> dict[str, Any]:
    """
    Start a Step Functions execution with a minimal continuation event.

    Execution name format:
      - Single-product workflows: "<nova_id>-<job_run_id[:8]>"  (45 chars)
      - Per-product launches:     "<data_product_id>-<job_run_id[:8]>"  (45 chars)
    Both are well under the SFN 80-char limit and are unique + traceable.

    ExecutionAlreadyExists is treated as idempotent success — if a task
    retry hits this, the execution is already running and the goal is met.

    Raises RetryableError on throttling or transient SFN failures.
    """
    if data_product_id:
        # Per-product launch: execution name scoped to the product so that
        # N products for the same nova all get distinct execution names.
        execution_name = f"{data_product_id}-{job_run_id[:8]}"
    else:
        execution_name = f"{nova_id}-{job_run_id[:8]}"

    execution_input: dict[str, Any] = {
        "nova_id": nova_id,
        "correlation_id": correlation_id,
    }
    if data_product_id:
        execution_input["data_product_id"] = data_product_id
    if provider:
        execution_input["provider"] = provider

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
