"""
Structured JSON logging utilities.

This module provides lightweight structured logging helpers for all
workflow tasks and runtime operations.

Logs are emitted as JSON objects and automatically include standard
context fields when an envelope is provided, including:

- workflow_name
- state_name
- execution_arn
- correlation_id
- job_run_id
- attempt_number
- nova_id
- data_product_id
- reference_id
- idempotency_key

Additional fields such as:
- error_classification
- error_fingerprint
- duration_ms

are included when applicable.

This module supports the observability plan by ensuring consistent
runtime metadata appears in every log entry.

It intentionally avoids heavy dependencies (e.g., Lambda Powertools).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from .config import log_level
from .types import Envelope

_STANDARD_CTX_FIELDS = (
    "workflow_name",
    "state_name",
    "execution_arn",
    "correlation_id",
    "job_run_id",
    "attempt_number",
    "nova_id",
    "data_product_id",
    "reference_id",
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("nova.workflow_runtime")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, log_level().upper(), logging.INFO))
    return logger


def _extract_ctx(envelope: Envelope | None) -> dict[str, Any]:
    if not envelope:
        return {}
    ctx = envelope.get("context")
    if not isinstance(ctx, dict):
        return {}
    out: dict[str, Any] = {}
    for k in _STANDARD_CTX_FIELDS:
        v = ctx.get(k)
        if v is not None:
            out[k] = v
    return out


def _emit(level: int, event: str, *, envelope: Envelope | None = None, **fields: Any) -> None:
    payload: dict[str, Any] = {
        "ts": _utc_now_iso(),
        "level": logging.getLevelName(level),
        "event": event,
    }
    payload.update(_extract_ctx(envelope))
    # user fields override (lets callers explicitly set/patch)
    payload.update({k: v for k, v in fields.items() if v is not None})
    _get_logger().log(level, json.dumps(payload, separators=(",", ":"), sort_keys=True))


def log_info(event: str, *, envelope: Envelope | None = None, **fields: Any) -> None:
    _emit(logging.INFO, event, envelope=envelope, **fields)


def log_warn(event: str, *, envelope: Envelope | None = None, **fields: Any) -> None:
    _emit(logging.WARNING, event, envelope=envelope, **fields)


def log_error(event: str, *, envelope: Envelope | None = None, **fields: Any) -> None:
    _emit(logging.ERROR, event, envelope=envelope, **fields)


def log_task_start(*, envelope: Envelope | None = None, **fields: Any) -> float:
    """Log task start and return a monotonic start time for duration calculation."""
    start = time.monotonic()
    log_info("task_start", envelope=envelope, **fields)
    return start


def log_task_end(
    start_monotonic: float,
    *,
    envelope: Envelope | None = None,
    outcome: str | None = None,
    **fields: Any,
) -> int:
    """Log task end and return duration_ms."""
    duration_ms = int((time.monotonic() - start_monotonic) * 1000)
    log_info(
        "task_end",
        envelope=envelope,
        duration_ms=duration_ms,
        outcome=outcome,
        **fields,
    )
    return duration_ms
