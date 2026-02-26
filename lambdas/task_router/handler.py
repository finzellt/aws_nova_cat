from __future__ import annotations

from typing import Any

from nova_cat_common.errors import classify_exception
from nova_cat_common.jobrun import JobRunRecorder
from nova_cat_common.log import get_logger
from nova_cat_common.manifest import resolve_task

log = get_logger()


def main(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """
    Expected payload from Step Functions:
      {
        "input": <original workflow input or accumulated payload>,
        "context": {
          "state_name": "...",
          "execution_arn": "...",
          "entered_time": "...",
          "retry_count": 0
        }
      }
    """
    state_name = event["context"]["state_name"]
    execution_arn = event["context"]["execution_arn"]
    attempt_no = int(event["context"].get("retry_count", 0)) + 1

    payload = event["input"]

    # Router-level log (workflow/task fields are required by specs; we’ll add the full set in common/log.py next)
    log.info(
        "task_start",
        extra={
            "state_name": state_name,
            "execution_arn": execution_arn,
            "attempt_number": attempt_no,
        },
    )

    recorder = JobRunRecorder.from_payload(payload, execution_arn=execution_arn)

    # Emit Attempt STARTED (normative operational record type) :contentReference[oaicite:13]{index=13}
    recorder.attempt_started(task_name=state_name, attempt_no=attempt_no)

    try:
        fn = resolve_task(state_name)
        out = fn(payload, recorder)

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

    except Exception as e:  # noqa: BLE001 (intentional)
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
