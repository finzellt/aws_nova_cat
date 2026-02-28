from __future__ import annotations

import hashlib
import re
from enum import Enum
from typing import Any

from botocore.exceptions import ClientError


class ErrorClassification(str, Enum):
    RETRYABLE = "RETRYABLE"
    TERMINAL = "TERMINAL"
    QUARANTINE = "QUARANTINE"


class WorkflowRuntimeError(Exception):
    """Base error type for runtime-level failures."""


class SuspectDataError(WorkflowRuntimeError):
    """Raise when upstream data is suspect and should be quarantined."""


class ValidationError(WorkflowRuntimeError):
    """Raise for validation failures that should not be retried."""


_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_message(msg: str) -> str:
    return _WHITESPACE_RE.sub(" ", msg.strip())


def fingerprint_error(exc_or_string: Any) -> str:
    """Return a stable fingerprint for an exception or message.

    We intentionally avoid including traceback / stack frames.
    """
    if isinstance(exc_or_string, BaseException):
        exc = exc_or_string
        kind = exc.__class__.__name__
        msg = _normalize_message(str(exc))
        material = f"{kind}:{msg}"

        # If it's a boto ClientError, include its error code for stability.
        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            material = f"{kind}:{code}:{msg}"
    else:
        material = _normalize_message(str(exc_or_string))

    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    # Shorten to keep logs compact while still collision-resistant enough for ops.
    return digest[:16]


def _is_throttling(code: str) -> bool:
    return code in {
        "Throttling",
        "ThrottlingException",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "TooManyRequestsException",
        "SlowDown",
    }


def classify_exception(exc: BaseException) -> tuple[ErrorClassification, str, str]:
    """Classify an exception, returning (classification, fingerprint, message)."""

    # Domain: suspect data -> quarantine
    if isinstance(exc, SuspectDataError):
        return (
            ErrorClassification.QUARANTINE,
            fingerprint_error(exc),
            str(exc) or exc.__class__.__name__,
        )

    # Validation -> terminal
    if isinstance(exc, ValidationError | ValueError | TypeError):
        return (
            ErrorClassification.TERMINAL,
            fingerprint_error(exc),
            str(exc) or exc.__class__.__name__,
        )

    # AWS client errors: throttling is retryable; other 5xx-ish / transient are retryable.
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        http_status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if _is_throttling(code) or http_status in {429, 500, 502, 503, 504}:
            return (
                ErrorClassification.RETRYABLE,
                fingerprint_error(exc),
                str(exc) or code or exc.__class__.__name__,
            )
        return (
            ErrorClassification.TERMINAL,
            fingerprint_error(exc),
            str(exc) or code or exc.__class__.__name__,
        )

    # Default: retryable (safe by default for unexpected infra issues)
    return (
        ErrorClassification.RETRYABLE,
        fingerprint_error(exc),
        str(exc) or exc.__class__.__name__,
    )
