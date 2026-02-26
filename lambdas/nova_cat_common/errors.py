from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClassifiedError:
    error_type: str  # "RETRYABLE" | "TERMINAL" | "QUARANTINE" (we’ll refine later)
    message: str


def classify_exception(exc: Exception) -> ClassifiedError:
    # Skeleton behavior: everything is terminal by default.
    # We’ll upgrade this later to match execution-governance taxonomy.
    return ClassifiedError(error_type="TERMINAL", message=str(exc))
