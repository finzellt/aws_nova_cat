import uuid
from typing import Any

import pytest

from services.workflow_runtime.envelope import (
    EnvelopeValidationError,
    ensure_correlation_id,
    envelope_ok,
    get_context,
    with_context,
)
from services.workflow_runtime.types import Envelope


def test_ensure_correlation_id_injects_when_missing() -> None:
    env: Envelope = {"input": {}, "context": {"workflow_name": "W"}}
    out = ensure_correlation_id(env)
    assert out is not env
    cid = out["context"]["correlation_id"]
    uuid.UUID(cid)  # will raise if invalid


def test_ensure_correlation_id_noop_when_present() -> None:
    env: Envelope = {"input": {}, "context": {"correlation_id": "abc"}}
    out = ensure_correlation_id(env)
    assert out is env


def test_with_context_applies_updates() -> None:
    env: Envelope = {"input": {"x": 1}, "context": {"correlation_id": "c1"}}
    out = with_context(env, job_run_id="jr1", attempt_number=2)
    assert out["context"]["job_run_id"] == "jr1"
    assert out["context"]["attempt_number"] == 2
    assert env["context"].get("job_run_id") is None  # original unchanged


def test_envelope_ok_validates_structure_and_requires_correlation_id() -> None:
    with pytest.raises(EnvelopeValidationError):
        envelope_ok({"context": {}})

    with pytest.raises(EnvelopeValidationError):
        envelope_ok({"input": {}, "context": {}})

    assert envelope_ok({"input": {}, "context": {"correlation_id": "c"}}) is True


def test_get_context_requires_context_dict() -> None:
    with pytest.raises(EnvelopeValidationError):
        bad_env: Any = {"input": {}}
        get_context(bad_env)
