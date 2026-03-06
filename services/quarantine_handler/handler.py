"""
quarantine_handler Lambda handler

Persists quarantine diagnostics onto the existing JobRun record and publishes
a best-effort SNS notification for operator review.

Used by all workflows as a shared quarantine sink. The handler does NOT create
a new DynamoDB item — it updates the existing JobRun record (written by
job_run_manager.BeginJobRun) with quarantine context fields.

Task dispatch table:
  QuarantineHandler — update JobRun with quarantine diagnostics + publish SNS

DynamoDB update target:
  PK = job_run["pk"]   (e.g. "WORKFLOW#<correlation_id>" or "<nova_id>")
  SK = job_run["sk"]   (e.g. "JOBRUN#<workflow_name>#<started_at>#<job_run_id>")

Fields written to JobRun:
  quarantine_reason_code   — caller-supplied reason code (e.g. COORDINATE_AMBIGUITY)
  classification_reason    — human-readable description derived from reason code
  error_fingerprint        — short hex digest for cross-referencing logs and SNS
  quarantined_at           — ISO-8601 UTC timestamp
  extra_context            — optional dict of additional diagnostic fields from event
                             (e.g. min_sep_arcsec for COORDINATE_AMBIGUITY)

Primary identifier resolution:
  The handler accepts whichever identifier is present in the event, in priority
  order: nova_id → data_product_id → candidate_name. This allows the handler
  to serve all workflows regardless of which identifiers are available at the
  point of quarantine.

SNS notification:
  Published to NOVA_CAT_QUARANTINE_TOPIC_ARN. Errors are caught and logged —
  SNS failure MUST NOT cause the workflow to fail. The JobRun update is
  authoritative; SNS is best-effort operational alerting only.

SNS payload fields:
  workflow_name            — from event
  primary_id               — nova_id, data_product_id, or candidate_name
                             (resolved in that priority order)
  correlation_id           — from event
  error_fingerprint        — computed here
  classification_reason    — derived from quarantine_reason_code
  quarantine_reason_code   — from event
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import boto3
from nova_common.errors import RetryableError  # noqa: F401
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_QUARANTINE_TOPIC_ARN = os.environ["NOVA_CAT_QUARANTINE_TOPIC_ARN"]

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)
_sns = boto3.client("sns")

# Human-readable descriptions keyed by quarantine_reason_code.
# Extend as new reason codes are introduced across workflows.
_CLASSIFICATION_REASONS: dict[str, str] = {
    # initialize_nova codes (NovaQuarantineReasonCode)
    "COORDINATE_AMBIGUITY": (
        'Candidate coordinates fall in the ambiguous 2"-10" separation band '
        "relative to an existing nova. Manual review required to confirm identity."
    ),
    # spectra codes (SpectraQuarantineReasonCode)
    "UNKNOWN_PROFILE": (
        "No FITS profile matched this spectra product. Manual review required "
        "to identify the correct profile or add a new one."
    ),
    "MISSING_CRITICAL_METADATA": (
        "Spectra product is missing one or more critical FITS header fields "
        "required for canonical normalization. Manual review required."
    ),
    "CHECKSUM_MISMATCH": (
        "Acquired bytes do not match the expected checksum. The product may "
        "have been corrupted in transit. Manual review required."
    ),
    "COORDINATE_PROXIMITY": (
        "Spectra product coordinates do not match the expected nova position "
        "within the allowed tolerance. Manual review required."
    ),
    # shared fallback
    "OTHER": ("Quarantine triggered — ambiguous or inconclusive results. Manual review required."),
}

_CLASSIFICATION_REASON_FALLBACK = "Quarantine triggered — see error_fingerprint for details."

# Fields that are passed as dedicated event keys on specific quarantine paths
# and should be captured into extra_context on the JobRun.
_EXTRA_CONTEXT_FIELDS = frozenset({"min_sep_arcsec"})


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
def _quarantine_handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Persist quarantine diagnostics onto the existing JobRun and publish SNS.

    The JobRun record was created by job_run_manager.BeginJobRun. This handler
    appends quarantine context fields to that record via update_item, then
    publishes a best-effort SNS notification.

    Returns:
        quarantine_reason_code  — echoed from event
        error_fingerprint       — computed short hex digest
        quarantined_at          — ISO-8601 UTC timestamp
    """
    workflow_name: str = event["workflow_name"]
    quarantine_reason_code: str = event["quarantine_reason_code"]
    correlation_id: str = event["correlation_id"]
    job_run: dict[str, Any] = event["job_run"]

    # Resolve primary_id from whichever identifier is present — workflows
    # supply different identifiers depending on context:
    #   initialize_nova         → candidate_name (no nova_id yet)
    #   spectra/photometry      → nova_id and/or data_product_id
    nova_id: str | None = event.get("nova_id")
    data_product_id: str | None = event.get("data_product_id")
    candidate_name: str | None = event.get("candidate_name")
    primary_id: str = nova_id or data_product_id or candidate_name or "unknown"

    quarantined_at = _now()
    error_fingerprint = _compute_error_fingerprint(
        quarantine_reason_code, workflow_name, primary_id
    )
    classification_reason = _CLASSIFICATION_REASONS.get(
        quarantine_reason_code, _CLASSIFICATION_REASON_FALLBACK
    )

    # Collect any extra diagnostic fields present in the event.
    # Float values are converted to Decimal — boto3 rejects plain floats
    # for DynamoDB numeric attributes.
    extra_context = {
        field: Decimal(str(event[field])) if isinstance(event[field], float) else event[field]
        for field in _EXTRA_CONTEXT_FIELDS
        if field in event
    }

    # ------------------------------------------------------------------
    # Persist quarantine diagnostics onto the existing JobRun record
    # ------------------------------------------------------------------
    update_expr_parts = [
        "quarantine_reason_code = :reason_code",
        "classification_reason = :classification_reason",
        "error_fingerprint = :error_fingerprint",
        "quarantined_at = :quarantined_at",
        "updated_at = :updated_at",
    ]
    expr_values: dict[str, Any] = {
        ":reason_code": quarantine_reason_code,
        ":classification_reason": classification_reason,
        ":error_fingerprint": error_fingerprint,
        ":quarantined_at": quarantined_at,
        ":updated_at": quarantined_at,
    }

    if extra_context:
        update_expr_parts.append("extra_context = :extra_context")
        expr_values[":extra_context"] = extra_context

    _table.update_item(
        Key={"PK": job_run["pk"], "SK": job_run["sk"]},
        UpdateExpression="SET " + ", ".join(update_expr_parts),
        ExpressionAttributeValues=expr_values,
    )

    logger.warning(
        "JobRun quarantine diagnostics persisted",
        extra={
            "quarantine_reason_code": quarantine_reason_code,
            "error_fingerprint": error_fingerprint,
            "primary_id": primary_id,
        },
    )

    # ------------------------------------------------------------------
    # Best-effort SNS notification — MUST NOT fail the workflow
    # ------------------------------------------------------------------
    _publish_quarantine_notification(
        workflow_name=workflow_name,
        primary_id=primary_id,
        correlation_id=correlation_id,
        error_fingerprint=error_fingerprint,
        classification_reason=classification_reason,
        quarantine_reason_code=quarantine_reason_code,
    )

    return {
        "quarantine_reason_code": quarantine_reason_code,
        "error_fingerprint": error_fingerprint,
        "quarantined_at": quarantined_at,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_error_fingerprint(
    quarantine_reason_code: str,
    workflow_name: str,
    primary_id: str,
) -> str:
    """
    Compute a short, stable hex digest for cross-referencing logs and SNS.

    The fingerprint is derived from the quarantine reason, workflow, and
    primary_id (nova_id, data_product_id, or candidate_name depending on
    workflow context) — stable across retries for the same logical event.
    Truncated to 12 hex chars (48 bits) for readability.
    """
    raw = f"{quarantine_reason_code}:{workflow_name}:{primary_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _publish_quarantine_notification(
    *,
    workflow_name: str,
    primary_id: str,
    correlation_id: str,
    error_fingerprint: str,
    classification_reason: str,
    quarantine_reason_code: str,
) -> None:
    """
    Publish a quarantine notification to SNS.

    Errors are caught and logged — SNS failure must not propagate to the
    caller or cause the workflow to fail.
    """
    try:
        payload = {
            "workflow_name": workflow_name,
            "primary_id": primary_id,
            "correlation_id": correlation_id,
            "error_fingerprint": error_fingerprint,
            "quarantine_reason_code": quarantine_reason_code,
            "classification_reason": classification_reason,
        }
        _sns.publish(
            TopicArn=_QUARANTINE_TOPIC_ARN,
            Subject=f"[NovaCat] Quarantine: {workflow_name} — {quarantine_reason_code}",
            Message=json.dumps(payload, indent=2),
        )
        logger.info(
            "Quarantine SNS notification published",
            extra={"error_fingerprint": error_fingerprint},
        )
    except Exception:
        logger.exception(
            "Failed to publish quarantine SNS notification (best-effort — suppressed)",
            extra={"error_fingerprint": error_fingerprint},
        )


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "QuarantineHandler": _quarantine_handler,
}
