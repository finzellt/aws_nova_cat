"""Structured JSON logging for the Fargate artifact generator.

Provides Powertools-compatible structured JSON log output using stdlib
``logging``, so CloudWatch log viewer enrichment treats Fargate log
lines identically to Lambda Powertools lines.

This module intentionally avoids any dependency on ``aws_lambda_powertools``
— the Fargate container has no Lambda context.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import sys
import traceback
from datetime import UTC, datetime
from typing import Any

# Fields that ``clear_nova_context()`` removes between per-nova iterations.
_NOVA_CONTEXT_KEYS = frozenset({"nova_id", "artifact", "phase"})


class LogContext:
    """Persistent key-value store merged into every log record.

    Analogous to Powertools' ``logger.append_keys()`` — call
    ``set_context()`` to add/update fields, and
    ``clear_nova_context()`` to strip per-nova fields between loop
    iterations while retaining plan-level fields like ``plan_id``.
    """

    def __init__(self) -> None:
        self._fields: dict[str, Any] = {}

    def set_context(self, **kwargs: Any) -> None:
        self._fields.update(kwargs)

    def clear_nova_context(self) -> None:
        for key in _NOVA_CONTEXT_KEYS:
            self._fields.pop(key, None)

    def get_fields(self) -> dict[str, Any]:
        return dict(self._fields)


class StructuredJsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object.

    Fields match the Powertools convention so the CloudWatch log
    viewer enrichment layer handles Lambda and Fargate rows
    identically.
    """

    _seq_counter = itertools.count()

    def __init__(self, log_context: LogContext | None = None) -> None:
        super().__init__()
        self._log_context = log_context or LogContext()

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "seq": next(self._seq_counter),
            "level": record.levelname,
            "message": record.getMessage(),
            "service": "nova-cat",
            "function_name": "artifact-generator",
        }

        # Merge persistent context fields.
        entry.update(self._log_context.get_fields())

        # Merge per-call extra fields (override persistent context).
        for key, value in vars(record).items():
            if key in _STDLIB_LOG_RECORD_ATTRS:
                continue
            entry[key] = value

        # Exception handling.
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = "".join(traceback.format_exception(*record.exc_info))

        return json.dumps(entry, default=str)


# Fields that are part of the stdlib LogRecord — we skip these when
# extracting per-call extra fields.
_STDLIB_LOG_RECORD_ATTRS = frozenset(
    {
        "name",
        "msg",
        "args",
        "created",
        "relativeCreated",
        "msecs",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "levelname",
        "levelno",
        "processName",
        "process",
        "threadName",
        "thread",
        "message",
        "taskName",
    }
)


def configure_fargate_logging(log_context: LogContext | None = None) -> logging.Logger:
    """Configure the ``artifact_generator`` logger with structured JSON output.

    Removes any existing handlers (including those from ``basicConfig``)
    on both the root logger and the ``artifact_generator`` logger, then
    installs a single ``StreamHandler(sys.stdout)`` with the
    :class:`StructuredJsonFormatter`.

    Returns the configured ``artifact_generator`` logger.
    """
    level = os.environ.get("LOG_LEVEL", "INFO")

    ctx = log_context or LogContext()
    formatter = StructuredJsonFormatter(ctx)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # Configure root logger — captures library output (boto3, etc.)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.WARNING)

    # Configure artifact_generator logger
    logger = logging.getLogger("artifact_generator")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False

    # Catch uncaught exceptions in structured format
    def _structured_excepthook(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_tb: Any,
    ) -> None:
        logger.critical(
            "Uncaught exception — process terminating",
            exc_info=(exc_type, exc_value, exc_tb),
        )

    sys.excepthook = _structured_excepthook

    return logger
