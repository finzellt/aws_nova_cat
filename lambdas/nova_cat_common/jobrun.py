from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .ddb import put_item


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class JobRunRecorder:
    nova_id: str
    workflow_name: str
    job_run_id: str
    started_at: str
    execution_arn: str
    schema_version: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any], execution_arn: str) -> JobRunRecorder:
        # We’ll standardize “schema_version” once we reconcile event_version vs schema_version in the payload;
        # for now default to "1" for operational records.
        schema_version = str(payload.get("event_version", "1"))
        workflow_name = (
            payload.get("workflow_name") or payload.get("workflow") or "UNKNOWN_WORKFLOW"
        )

        nova_id = payload.get("nova_id")
        if not nova_id:
            # initialize_nova starts name-only; JobRun still exists, but we can’t key it to a nova partition.
            # For the thin slice, we’ll require nova_id to be present by BeginJobRun states except initialize_nova.
            nova_id = (
                payload.get("resolved_nova_id")
                or payload.get("nova_id_placeholder")
                or "UNKNOWN_NOVA"
            )

        return cls(
            nova_id=nova_id,
            workflow_name=workflow_name,
            job_run_id=str(uuid.uuid4()),
            started_at=utc_now_iso(),
            execution_arn=execution_arn,
            schema_version=schema_version,
        )

    def jobrun_started(self) -> None:
        # SK = "JOBRUN#<workflow_name>#<started_at>#<job_run_id>" :contentReference[oaicite:15]{index=15}
        put_item(
            {
                "PK": self.nova_id,
                "SK": f"JOBRUN#{self.workflow_name}#{self.started_at}#{self.job_run_id}",
                "entity_type": "JobRun",
                "schema_version": "1",
                "job_run_id": self.job_run_id,
                "workflow_name": self.workflow_name,
                "execution_arn": self.execution_arn,
                "status": "RUNNING",
                "started_at": self.started_at,
                "created_at": self.started_at,
                "updated_at": self.started_at,
            }
        )

    def attempt_started(self, task_name: str, attempt_no: int) -> None:
        ts = utc_now_iso()
        # SK = "ATTEMPT#<job_run_id>#<task_name>#<attempt_no>#<timestamp>" :contentReference[oaicite:16]{index=16}
        put_item(
            {
                "PK": self.nova_id,
                "SK": f"ATTEMPT#{self.job_run_id}#{task_name}#{attempt_no}#{ts}",
                "entity_type": "Attempt",
                "schema_version": "1",
                "job_run_id": self.job_run_id,
                "task_name": task_name,
                "attempt_no": attempt_no,
                "status": "STARTED",
                "created_at": ts,
                "updated_at": ts,
            }
        )

    def attempt_succeeded(self, task_name: str, attempt_no: int) -> None:
        # For the skeleton: we record STARTED and SUCCEEDED as separate Attempt items (simple).
        # If you prefer “update in place”, we can do that next once we add conditional writes.
        ts = utc_now_iso()
        put_item(
            {
                "PK": self.nova_id,
                "SK": f"ATTEMPT#{self.job_run_id}#{task_name}#{attempt_no}#{ts}",
                "entity_type": "Attempt",
                "schema_version": "1",
                "job_run_id": self.job_run_id,
                "task_name": task_name,
                "attempt_no": attempt_no,
                "status": "SUCCEEDED",
                "created_at": ts,
                "updated_at": ts,
            }
        )

    def attempt_failed(
        self, task_name: str, attempt_no: int, error_type: str, error_message: str
    ) -> None:
        ts = utc_now_iso()
        put_item(
            {
                "PK": self.nova_id,
                "SK": f"ATTEMPT#{self.job_run_id}#{task_name}#{attempt_no}#{ts}",
                "entity_type": "Attempt",
                "schema_version": "1",
                "job_run_id": self.job_run_id,
                "task_name": task_name,
                "attempt_no": attempt_no,
                "status": "FAILED",
                "error_type": error_type,
                "error_message": error_message[:400],
                "created_at": ts,
                "updated_at": ts,
            }
        )
