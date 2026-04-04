"""
spectra_acquirer — Lambda handler

Description: Bytes download, SHA-256 fingerprint, raw S3 write, attempt tracking
Workflows:   acquire_and_validate_spectra
Tasks:       AcquireArtifact

Acquisition assumptions (confirmed by ESO probe):
  - ESO FITS files are served via plain unauthenticated HTTP GET
  - No ZIP bundles for ESO MVP (direct FITS)
  - Files are typically < 50 MB; single put_object is appropriate
  - HTTP 429 and 5xx are RETRYABLE; 4xx (except 429) are TERMINAL

Backoff schedule (seconds, indexed by post-increment attempt_count):
  Attempt 1 → 60 s
  Attempt 2 → 300 s (5 min)
  Attempt 3 → 3 600 s (1 hr)
  Attempt 4+ → 86 400 s (24 hr, capped)

Environment variables (injected by CDK):
    NOVA_CAT_TABLE_NAME           — DynamoDB table name
    NOVA_CAT_PRIVATE_BUCKET       — private data S3 bucket name
    NOVA_CAT_PUBLIC_SITE_BUCKET   — public site S3 bucket name (unused here)
    NOVA_CAT_QUARANTINE_TOPIC_ARN — quarantine notifications SNS topic ARN (unused here)
    LOG_LEVEL                     — logging level (default INFO)
    POWERTOOLS_SERVICE_NAME       — AWS Lambda Powertools service name
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import requests
from botocore.exceptions import ClientError
from nova_common.errors import RetryableError
from nova_common.logging import configure_logging, logger
from nova_common.timing import log_duration
from nova_common.tracing import tracer

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_PRIVATE_BUCKET = os.environ["NOVA_CAT_PRIVATE_BUCKET"]
_SCHEMA_VERSION = "1"

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)
_s3 = boto3.client("s3")

# Download tuning
_DOWNLOAD_TIMEOUT_S = 60 * 14  # 14 min — safely within Lambda 15-min hard limit
_CHUNK_SIZE = 256 * 1024  # 256 KB streaming chunks

# Backoff schedule: index is (attempt_count - 1), capped at last entry.
_BACKOFF_SCHEDULE_S = [60, 300, 3_600, 86_400]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    if not task_name:
        raise ValueError("Missing required field: task_name")
    handler_fn = _TASK_HANDLERS.get(task_name)
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}. Known tasks: {list(_TASK_HANDLERS)}")
    logger.info(
        "Dispatching task",
        extra={
            "task_name": task_name,
            "correlation_id": event.get("correlation_id"),
            "nova_id": event.get("nova_id"),
            "data_product_id": event.get("data_product_id"),
        },
    )
    return handler_fn(event, context)


# ---------------------------------------------------------------------------
# Task: AcquireArtifact
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_acquire_artifact(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Download spectra bytes from the product's primary URL locator,
    write raw bytes to S3, and compute a SHA-256 content fingerprint.

    Attempt tracking contract:
      - attempt_count is incremented and last_attempt_at set at the START of
        acquisition (before the download), so even a Lambda timeout counts.
      - On retryable failure: sets next_eligible_attempt_at (capped exponential
        backoff), last_error_fingerprint, acquisition_status=FAILED_RETRYABLE,
        last_attempt_outcome=RETRYABLE_FAILURE, then re-raises RetryableError.
      - On terminal failure: sets last_error_fingerprint,
        last_attempt_outcome=TERMINAL_FAILURE, then re-raises ValueError.
      - On success: returns acquisition metadata for ValidateBytes; final
        lifecycle state (ACQUIRED, eligibility=NONE, etc.) is persisted by
        RecordValidationResult.

    S3 raw object key: raw/{nova_id}/{provider}/{data_product_id}.fits

    Event inputs (from ASL state machine):
      nova_id, provider, data_product_id — product identity
      data_product                       — full product record from CheckOperationalStatus
                                           (must contain `locators` list)

    Returns:
      raw_s3_bucket     — S3 bucket holding raw bytes
      raw_s3_key        — S3 key for raw FITS object
      sha256            — hex-encoded SHA-256 digest of raw bytes
      byte_length       — total byte count
      etag              — S3 ETag (for integrity cross-reference)
    """
    nova_id: str = event["nova_id"]
    provider: str = event["provider"]
    data_product_id: str = event["data_product_id"]
    data_product: dict[str, Any] = event["data_product"]

    url = _extract_primary_url(data_product.get("locators", []))
    if not url:
        # No URL locator: terminal — we cannot acquire without a download target.
        raise ValueError(
            f"No PRIMARY URL locator found for data_product_id={data_product_id!r}. "
            "Cannot acquire without a download URL."
        )

    now = _now()

    # Increment attempt_count BEFORE the download so that even a Lambda timeout
    # is counted as an attempt.
    attempt_count = _increment_attempt_count(
        nova_id=nova_id,
        provider=provider,
        data_product_id=data_product_id,
        now=now,
    )

    raw_s3_key = f"raw/{nova_id}/{provider}/{data_product_id}.fits"

    logger.info(
        "Starting AcquireArtifact",
        extra={
            "data_product_id": data_product_id,
            "provider": provider,
            "nova_id": nova_id,
            "attempt_count": attempt_count,
            "url": url,
        },
    )

    try:
        sha256, byte_length, etag = _stream_to_s3(
            url=url,
            bucket=_PRIVATE_BUCKET,
            key=raw_s3_key,
            data_product_id=data_product_id,
        )
    except _RetryableDownloadError as exc:
        backoff_s = _compute_backoff_seconds(attempt_count)
        next_eligible = _add_seconds(now, backoff_s)
        _persist_failure(
            nova_id=nova_id,
            provider=provider,
            data_product_id=data_product_id,
            acquisition_status="FAILED_RETRYABLE",
            last_attempt_outcome="RETRYABLE_FAILURE",
            error_fingerprint=_error_fingerprint(str(exc)),
            next_eligible_attempt_at=next_eligible,
            now=now,
        )
        logger.warning(
            "Retryable acquisition failure",
            extra={
                "data_product_id": data_product_id,
                "error": str(exc),
                "next_eligible_attempt_at": next_eligible,
            },
        )
        raise RetryableError(
            f"Retryable download failure for data_product_id={data_product_id!r}: {exc}"
        ) from exc

    except _TerminalDownloadError as exc:
        _persist_failure(
            nova_id=nova_id,
            provider=provider,
            data_product_id=data_product_id,
            acquisition_status="FAILED_RETRYABLE",  # scientific state: still retryable
            last_attempt_outcome="TERMINAL_FAILURE",  # operational state: this attempt was terminal
            error_fingerprint=_error_fingerprint(str(exc)),
            next_eligible_attempt_at=None,
            now=now,
        )
        logger.error(
            "Terminal acquisition failure",
            extra={"data_product_id": data_product_id, "error": str(exc)},
        )
        raise ValueError(
            f"Terminal download failure for data_product_id={data_product_id!r}: {exc}"
        ) from exc

    logger.info(
        "AcquireArtifact complete",
        extra={
            "data_product_id": data_product_id,
            "provider": provider,
            "byte_length": byte_length,
            "sha256_prefix": sha256[:16],
            "raw_s3_key": raw_s3_key,
        },
    )

    return {
        "raw_s3_bucket": _PRIVATE_BUCKET,
        "raw_s3_key": raw_s3_key,
        "sha256": sha256,
        "byte_length": byte_length,
        "etag": etag,
    }


# ---------------------------------------------------------------------------
# Download internals
# ---------------------------------------------------------------------------


class _RetryableDownloadError(Exception):
    """Transient failure: throttling, timeout, 5xx, mid-stream interruption."""


class _TerminalDownloadError(Exception):
    """Non-recoverable failure: 4xx (not 429), persistent S3 write error."""


def _stream_to_s3(
    *, url: str, bucket: str, key: str, data_product_id: str = ""
) -> tuple[str, int, str]:
    """
    Stream bytes from `url` into memory, compute SHA-256 in flight,
    then write to S3 via a single put_object call.

    For ESO MVP: FITS files are < 50 MB, making a single put_object appropriate.
    A future provider with larger files should switch to multipart upload.

    Returns: (sha256_hex, byte_length, etag)
    Raises: _RetryableDownloadError | _TerminalDownloadError
    """
    session = requests.Session()
    session.headers["User-Agent"] = "NovaCat-Acquirer/1.0"

    try:
        with log_duration("fits_download", data_product_id=data_product_id):
            resp = session.get(url, timeout=_DOWNLOAD_TIMEOUT_S, stream=True, allow_redirects=True)
    except requests.Timeout as exc:
        raise _RetryableDownloadError(f"Request timed out: {exc}") from exc
    except requests.ConnectionError as exc:
        raise _RetryableDownloadError(f"Connection error: {exc}") from exc
    except requests.RequestException as exc:
        raise _RetryableDownloadError(f"Request failed unexpectedly: {exc}") from exc

    if resp.status_code == 429 or resp.status_code >= 500:
        raise _RetryableDownloadError(
            f"HTTP {resp.status_code} — throttling or server error (retryable)"
        )
    if resp.status_code >= 400:
        raise _TerminalDownloadError(
            f"HTTP {resp.status_code} — client error (terminal, will not retry)"
        )
    if resp.status_code != 200:
        raise _RetryableDownloadError(f"Unexpected HTTP {resp.status_code} (treating as retryable)")

    hasher = hashlib.sha256()
    buf = bytearray()
    try:
        for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
            if chunk:
                buf.extend(chunk)
                hasher.update(chunk)
    except requests.RequestException as exc:
        raise _RetryableDownloadError(f"Stream interrupted mid-download: {exc}") from exc

    data = bytes(buf)
    sha256_hex = hasher.hexdigest()
    byte_length = len(data)

    try:
        with log_duration("s3_upload", data_product_id=data_product_id):
            s3_resp = _s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                ContentType="application/fits",
            )
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in {"ServiceUnavailable", "InternalError", "RequestTimeout", "SlowDown"}:
            raise _RetryableDownloadError(
                f"S3 put_object transient failure ({code}): {exc}"
            ) from exc
        raise _TerminalDownloadError(f"S3 put_object failed ({code}): {exc}") from exc

    etag: str = s3_resp.get("ETag", "").strip('"')
    return sha256_hex, byte_length, etag


def _extract_primary_url(locators: list[dict[str, Any]]) -> str | None:
    """
    Return the value of the first PRIMARY URL locator.
    Falls back to any URL locator if no PRIMARY is found.
    Returns None if no URL locator exists.
    """
    for locator in locators:
        if locator.get("kind") == "URL" and locator.get("role") == "PRIMARY":
            return str(locator["value"])
    for locator in locators:
        if locator.get("kind") == "URL":
            return str(locator["value"])
    return None


# ---------------------------------------------------------------------------
# Attempt tracking
# ---------------------------------------------------------------------------


def _increment_attempt_count(
    *,
    nova_id: str,
    provider: str,
    data_product_id: str,
    now: str,
) -> int:
    """
    Atomically increment attempt_count and set last_attempt_at on the DataProduct.

    Uses ADD for attempt_count (atomic increment, initialises to 0 if absent).
    Returns the new attempt_count value.
    Raises RetryableError on DynamoDB transient failures.
    """
    sk = f"PRODUCT#SPECTRA#{provider}#{data_product_id}"
    try:
        resp = _table.update_item(
            Key={"PK": nova_id, "SK": sk},
            UpdateExpression=(
                "SET last_attempt_at = :now, updated_at = :now ADD attempt_count :one"
            ),
            ExpressionAttributeValues={":now": now, ":one": 1},
            ReturnValues="UPDATED_NEW",
        )
        raw = resp["Attributes"].get("attempt_count", 1)
        return int(raw) if isinstance(raw, int | float | str) else 1
    except ClientError as exc:
        raise RetryableError(
            f"DynamoDB update failed incrementing attempt_count "
            f"for data_product_id={data_product_id!r}: {exc}"
        ) from exc


def _persist_failure(
    *,
    nova_id: str,
    provider: str,
    data_product_id: str,
    acquisition_status: str,
    last_attempt_outcome: str,
    error_fingerprint: str,
    next_eligible_attempt_at: str | None,
    now: str,
) -> None:
    """
    Persist failure metadata onto the DataProduct after a failed acquisition attempt.

    Scientific state (acquisition_status) vs operational state (last_attempt_outcome)
    are kept strictly separate per the execution governance invariant.

    Errors here are logged but not re-raised: the caller's primary error is already
    being propagated and we must not swallow it.
    """
    sk = f"PRODUCT#SPECTRA#{provider}#{data_product_id}"
    expr = (
        "SET acquisition_status = :acq, "
        "last_attempt_outcome = :outcome, "
        "last_error_fingerprint = :fp, "
        "updated_at = :now"
    )
    values: dict[str, Any] = {
        ":acq": acquisition_status,
        ":outcome": last_attempt_outcome,
        ":fp": error_fingerprint,
        ":now": now,
    }
    if next_eligible_attempt_at is not None:
        expr += ", next_eligible_attempt_at = :next"
        values[":next"] = next_eligible_attempt_at

    try:
        _table.update_item(
            Key={"PK": nova_id, "SK": sk},
            UpdateExpression=expr,
            ExpressionAttributeValues=values,
        )
    except ClientError as exc:
        logger.error(
            "Failed to persist acquisition failure metadata to DynamoDB",
            extra={
                "data_product_id": data_product_id,
                "last_attempt_outcome": last_attempt_outcome,
                "error": str(exc),
            },
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_backoff_seconds(attempt_count: int) -> int:
    """Return cooldown window in seconds for the given (post-increment) attempt_count."""
    idx = min(attempt_count - 1, len(_BACKOFF_SCHEDULE_S) - 1)
    return _BACKOFF_SCHEDULE_S[idx]


def _error_fingerprint(msg: str) -> str:
    """Produce a 12-char SHA-256 hex digest for stable error cross-referencing."""
    return hashlib.sha256(msg.encode()).hexdigest()[:12]


def _add_seconds(iso_utc: str, seconds: int) -> str:
    """Add `seconds` to an ISO-8601 UTC string and return a new ISO-8601 UTC string."""
    dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
    return (dt + timedelta(seconds=seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "AcquireArtifact": _handle_acquire_artifact,
}
