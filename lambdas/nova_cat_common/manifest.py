from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from lambdas.nova_cat_common.initialize import (
    begin_jobrun,
    check_existing_nova_by_name,
    create_nova_id,
    finalize_jobrun_success,
    normalize_candidate_name,
    publish_ingest_new_nova,
    upsert_minimal_nova_metadata,
)


class Recorder(Protocol):
    @property
    def job_run_id(self) -> str: ...

    def jobrun_started(self) -> None: ...
    def attempt_started(self, task_name: str, attempt_no: int) -> None: ...
    def attempt_succeeded(self, task_name: str, attempt_no: int) -> None: ...
    def attempt_failed(
        self, task_name: str, attempt_no: int, error_type: str, error_message: str
    ) -> None: ...


TaskFn = Callable[[dict[str, Any], Recorder], dict[str, Any]]

_TASKS: dict[str, TaskFn] = {
    "BeginJobRun": begin_jobrun,
    "NormalizeCandidateName": normalize_candidate_name,
    "CheckExistingNovaByName": check_existing_nova_by_name,
    "CreateNovaId": create_nova_id,
    "UpsertMinimalNovaMetadata": upsert_minimal_nova_metadata,
    "PublishIngestNewNova": publish_ingest_new_nova,
    "FinalizeJobRunSuccess": finalize_jobrun_success,
}


def resolve_task(state_name: str) -> TaskFn:
    try:
        return _TASKS[state_name]
    except KeyError as e:
        raise KeyError(f"Unregistered task: {state_name}") from e
