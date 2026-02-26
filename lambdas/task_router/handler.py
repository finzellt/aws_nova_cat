from __future__ import annotations

from typing import Any

from lambdas.nova_cat_common.errors import classify_exception
from lambdas.nova_cat_common.jobrun import JobRunRecorder
from lambdas.nova_cat_common.log import get_logger
from lambdas.nova_cat_common.manifest import resolve_task

log = get_logger()


def main(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    state_name = str(event["context"]["state_name"])
    execution_arn = str(event["context"]["execution_arn"])
    attempt_no = int(event["context"].get("retry_count", 0)) + 1

    payload_obj = event["input"]
    if not isinstance(payload_obj, dict):
        raise TypeError("event['input'] must be a dict")
    payload: dict[str, Any] = payload_obj

    log.info(
        "task_start",
        extra={
            "state_name": state_name,
            "execution_arn": execution_arn,
            "attempt_number": attempt_no,
        },
    )

    recorder = JobRunRecorder.from_payload(payload, execution_arn=execution_arn)
    recorder.attempt_started(task_name=state_name, attempt_no=attempt_no)

    try:
        fn = resolve_task(state_name)
        out: dict[str, Any] = fn(payload, recorder)

        recorder.attempt_succeeded(task_name=state_name, attempt_no=attempt_no)
        log.info(
            "task_success",
            extra={
                "state_name": state_name,
                "execution_arn": execution_arn,
                "attempt_number": attempt_no,
            },
        )
        return out

    except Exception as e:  # noqa: BLE001
        err = classify_exception(e)
        recorder.attempt_failed(
            task_name=state_name,
            attempt_no=attempt_no,
            error_type=err.error_type,
            error_message=err.message,
        )
        log.exception(
            "task_failed",
            extra={
                "state_name": state_name,
                "execution_arn": execution_arn,
                "attempt_number": attempt_no,
                "error_type": err.error_type,
            },
        )
        raise
