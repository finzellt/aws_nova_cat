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
    once per product in a Python loop. Launches are batched (_FANOUT_BATCH_SIZE
    executions per batch, _FANOUT_BATCH_DELAY_S seconds between batches) to
    prevent overwhelming Lambda concurrent execution capacity. All N executions
    are non-blocking and run in parallel as independent Express Workflow state
    machines — no SNS/SQS fan-out is required.
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
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from nova_common.errors import RetryableError
from nova_common.logging import configure_logging, logger
from nova_common.timing import log_duration
from nova_common.tracing import tracer

_INGEST_NEW_NOVA_STATE_MACHINE_ARN = os.environ["INGEST_NEW_NOVA_STATE_MACHINE_ARN"]
_REFRESH_REFERENCES_STATE_MACHINE_ARN = os.environ["REFRESH_REFERENCES_STATE_MACHINE_ARN"]
_DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN = os.environ[
    "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN"
]
_ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN = os.environ[
    "ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN"
]

_FANOUT_BATCH_SIZE = 10  # executions per batch
_FANOUT_BATCH_DELAY_S = 2.0  # seconds between batches

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
    logger.info("Task started", extra={"task_name": task_name})
    with log_duration(f"task:{task_name}"):
        result = handler_fn(event, context)
    return result


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
    Python loop and run in parallel as independent Express Workflow state machines.
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
    provider: str = event["provider"]
    # Support both the new ASL shape (job_run_id as top-level field) and the
    # legacy shape (nested under job_run.job_run_id) for backward compatibility.
    job_run_id: str = event.get("job_run_id") or event["job_run"]["job_run_id"]

    # Query DDB for eligible products instead of reading from the event payload.
    eligible_products = _query_eligible_products(nova_id=nova_id, provider=provider)

    launched: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    # Split products into batches to avoid overwhelming Lambda concurrency.
    total_batches = (
        (len(eligible_products) + _FANOUT_BATCH_SIZE - 1) // _FANOUT_BATCH_SIZE
        if eligible_products
        else 0
    )

    for batch_idx in range(total_batches):
        start = batch_idx * _FANOUT_BATCH_SIZE
        batch = eligible_products[start : start + _FANOUT_BATCH_SIZE]

        logger.info(
            "Launching fan-out batch",
            extra={
                "batch_number": batch_idx + 1,
                "batch_size": len(batch),
                "total_batches": total_batches,
                "total_products": len(eligible_products),
            },
        )

        for product in batch:
            data_product_id: str = product["data_product_id"]

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
                launched.append(
                    {"data_product_id": data_product_id, "provider": provider, **result}
                )
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

        # Delay between batches — but not after the final batch.
        if batch_idx < total_batches - 1:
            time.sleep(_FANOUT_BATCH_DELAY_S)

    logger.info(
        "PublishAcquireAndValidateSpectraRequests complete",
        extra={
            "nova_id": nova_id,
            "primary_name": event.get("primary_name", "unknown"),
            "launched": len(launched),
            "failed": len(failed),
            "total": len(eligible_products),
        },
    )

    return {
        "launched": launched,
        "failed": failed,
        "total": len(eligible_products),
    }


# ---------------------------------------------------------------------------
# Shared SFN helper
# ---------------------------------------------------------------------------


def _query_eligible_products(*, nova_id: str, provider: str) -> list[dict[str, Any]]:
    """
    Query DDB for DataProduct items with eligibility=ACQUIRE for the given
    nova_id and provider.

    Uses a query on PK=nova_id with SK begins_with("PRODUCT#SPECTRA#{provider}#")
    and filters for eligibility=ACQUIRE.

    Paginates to collect all matching items. Raises RetryableError on
    DynamoDB transient failures.
    """
    sk_prefix = f"PRODUCT#SPECTRA#{provider}#"
    products: list[dict[str, Any]] = []

    try:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("PK").eq(nova_id) & Key("SK").begins_with(sk_prefix),
            "FilterExpression": "eligibility = :elig",
            "ExpressionAttributeValues": {":elig": "ACQUIRE"},
            "ProjectionExpression": "data_product_id, provider",
        }
        while True:
            response = _table.query(**kwargs)
            products.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except ClientError as exc:
        raise RetryableError(
            f"DynamoDB query failed fetching eligible products "
            f"for nova_id={nova_id!r} provider={provider!r}: {exc}"
        ) from exc

    logger.info(
        "Queried eligible products from DDB",
        extra={
            "nova_id": nova_id,
            "provider": provider,
            "eligible_count": len(products),
        },
    )
    return products


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
