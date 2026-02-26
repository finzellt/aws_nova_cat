from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nova_cat_common.initialize import (
    begin_jobrun,
    check_existing_nova_by_name,
    create_nova_id,
    finalize_jobrun_success,
    normalize_candidate_name,
    publish_ingest_new_nova,
    upsert_minimal_nova_metadata,
)

# Add more task modules as we go (ingest/discover/acquire families).

_TASKS: dict[str, Callable[[dict, Any], dict]] = {
    "BeginJobRun": begin_jobrun,
    "NormalizeCandidateName": normalize_candidate_name,
    "CheckExistingNovaByName": check_existing_nova_by_name,
    "CreateNovaId": create_nova_id,
    "UpsertMinimalNovaMetadata": upsert_minimal_nova_metadata,
    "PublishIngestNewNova": publish_ingest_new_nova,
    "FinalizeJobRunSuccess": finalize_jobrun_success,
    # ... also map the ingest/discover/acquire state names used in CDK
}


def resolve_task(state_name: str) -> Callable[[dict, Any], dict]:
    if state_name not in _TASKS:
        raise KeyError(f"Unregistered task: {state_name}")
    return _TASKS[state_name]
