"""
Internal envelope model

This module implements the standardized internal envelope shape used between
all Step Functions Task Lambdas in Nova Cat V2.

Envelope structure:

{
  "input": { ...domain event... },
  "context": {
      "workflow_name": "...",
      "state_name": "...",
      "execution_arn": "...",
      "correlation_id": "...",
      "job_run_id": "...",
      "attempt_number": 1,
      "nova_id": "...",
      "data_product_id": "...",
      "reference_id": "...",
      "idempotency_key": "..."
  }
}

Key guarantees:

- `correlation_id` is always present (auto-generated if missing).
- The envelope remains stable even if domain schemas evolve.
- Context updates are explicit and controlled.

This module does not validate domain schemas and does not depend on
any specific workflow implementation.
"""

from __future__ import annotations

import copy
import uuid
from typing import Any, cast

from .types import Envelope, EnvelopeContext


class EnvelopeValidationError(ValueError):
    """Raised when an envelope is missing required structure/fields."""


def _is_dict(obj: Any) -> bool:
    return isinstance(obj, dict)


def envelope_ok(envelope: Any) -> bool:
    """Validate the envelope shape.

    Returns True if OK, otherwise raises EnvelopeValidationError.
    """
    if not _is_dict(envelope):
        raise EnvelopeValidationError("Envelope must be a dict")

    if "input" not in envelope:
        raise EnvelopeValidationError("Envelope missing required key: 'input'")
    if "context" not in envelope:
        raise EnvelopeValidationError("Envelope missing required key: 'context'")

    if not _is_dict(envelope["input"]):
        raise EnvelopeValidationError("Envelope['input'] must be a dict")

    if not _is_dict(envelope["context"]):
        raise EnvelopeValidationError("Envelope['context'] must be a dict")

    ctx = envelope["context"]
    if not ctx.get("correlation_id"):
        raise EnvelopeValidationError("Envelope['context'].correlation_id is required")

    return True


def ensure_correlation_id(envelope: Envelope) -> Envelope:
    """Ensure envelope.context.correlation_id exists.

    If missing, generates a UUID4 string and returns a *new* envelope.
    """
    if "context" not in envelope or not isinstance(envelope["context"], dict):
        # be permissive: create context if absent
        new_env: Envelope = copy.deepcopy(envelope)
        new_env["context"] = {}
        envelope = new_env

    if not envelope["context"].get("correlation_id"):
        new_env = copy.deepcopy(envelope)
        new_env["context"]["correlation_id"] = str(uuid.uuid4())
        return new_env

    return envelope


def get_context(envelope: Envelope) -> EnvelopeContext:
    if (
        not isinstance(envelope, dict)
        or "context" not in envelope
        or not isinstance(envelope["context"], dict)
    ):
        raise EnvelopeValidationError("Envelope missing 'context' dict")
    return cast(EnvelopeContext, envelope["context"])


def with_context(envelope: Envelope, **updates: Any) -> Envelope:
    """Return a new envelope with context updates applied."""
    new_env: Envelope = copy.deepcopy(envelope)
    ctx: dict[str, Any] = dict(new_env.get("context", {}))
    ctx.update({k: v for k, v in updates.items() if v is not None})
    new_env["context"] = cast(EnvelopeContext, ctx)
    return new_env
