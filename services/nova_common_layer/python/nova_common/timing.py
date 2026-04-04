"""nova_common.timing — lightweight operation timing for structured logs.

Provides a context manager that measures wall-clock duration and emits
a structured log line with ``duration_ms`` and ``operation`` fields.
These fields enable CloudWatch Insights queries like:

    fields @timestamp, operation, duration_ms
    | filter operation = "fits_validation"
    | stats avg(duration_ms), max(duration_ms), p99(duration_ms)
"""

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from nova_common.logging import logger


@contextmanager
def log_duration(operation: str, **extra: Any) -> Generator[None, None, None]:
    """Context manager that logs operation duration in milliseconds.

    Usage:
        with log_duration("s3_read", data_product_id=dpid):
            data = s3.get_object(...)
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            f"Operation completed: {operation}",
            extra={"operation": operation, "duration_ms": round(duration_ms, 1), **extra},
        )
