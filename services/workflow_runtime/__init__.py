"""Nova Cat V2 - Shared workflow runtime primitives.

This package is intentionally pure-Python so it can be reused by both
zip-based and container-based Lambda packaging.
"""

from .envelope import (
    ensure_correlation_id,
    envelope_ok,
    get_context,
    with_context,
)
from .errors import (
    ErrorClassification,
    SuspectDataError,
    WorkflowRuntimeError,
    classify_exception,
    fingerprint_error,
)
from .logging import (
    log_error,
    log_info,
    log_task_end,
    log_task_start,
    log_warn,
)

__all__ = [
    # envelope
    "ensure_correlation_id",
    "envelope_ok",
    "get_context",
    "with_context",
    # errors
    "ErrorClassification",
    "WorkflowRuntimeError",
    "SuspectDataError",
    "classify_exception",
    "fingerprint_error",
    # logging
    "log_info",
    "log_warn",
    "log_error",
    "log_task_start",
    "log_task_end",
]
