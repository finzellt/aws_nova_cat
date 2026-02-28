import json
from typing import Any

from services.workflow_runtime.logging import log_error, log_task_end, log_task_start
from services.workflow_runtime.types import Envelope


def test_structured_logs_include_context_fields(caplog: Any, log_level_env: Any) -> None:
    env: Envelope = {
        "input": {},
        "context": {
            "workflow_name": "WF",
            "state_name": "State",
            "execution_arn": "arn:aws:states:...",
            "correlation_id": "cid",
            "job_run_id": "jr",
            "attempt_number": 1,
            "nova_id": "N1",
        },
    }

    with caplog.at_level("INFO"):
        start = log_task_start(envelope=env)
        _ = log_task_end(start, envelope=env, outcome="ok")

    # Last record should be task_end
    msg = caplog.records[-1].message
    payload = json.loads(msg)
    assert payload["event"] == "task_end"
    assert payload["workflow_name"] == "WF"
    assert payload["state_name"] == "State"
    assert payload["correlation_id"] == "cid"
    assert payload["job_run_id"] == "jr"
    assert payload["attempt_number"] == 1
    assert payload["nova_id"] == "N1"
    assert "duration_ms" in payload


def test_log_error_includes_custom_fields(caplog: Any, log_level_env: Any) -> None:
    env: Envelope = {"input": {}, "context": {"correlation_id": "c"}}
    with caplog.at_level("ERROR"):
        log_error(
            "something_failed",
            envelope=env,
            error_classification="TERMINAL",
            error_fingerprint="abc",
        )

    payload = json.loads(caplog.records[-1].message)
    assert payload["event"] == "something_failed"
    assert payload["error_classification"] == "TERMINAL"
    assert payload["error_fingerprint"] == "abc"
    assert payload["correlation_id"] == "c"
