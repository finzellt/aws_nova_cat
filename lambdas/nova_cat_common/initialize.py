from __future__ import annotations

import uuid
from typing import Any, Protocol

from lambdas.nova_cat_common.ddb import put_item
from lambdas.nova_cat_common.jobrun import utc_now_iso


class Recorder(Protocol):
    @property
    def job_run_id(self) -> str: ...
    def jobrun_started(self) -> None: ...


Payload = dict[str, Any]


def begin_jobrun(payload: Payload, recorder: Recorder) -> Payload:
    payload = dict(payload)
    payload["workflow_name"] = payload.get("workflow_name", "initialize_nova")
    recorder.jobrun_started()
    payload["job_run_id"] = recorder.job_run_id
    return payload


def normalize_candidate_name(payload: Payload, recorder: Recorder) -> Payload:
    payload = dict(payload)
    name = str(payload["candidate_name"])
    payload["normalized_candidate_name"] = name.strip().lower()
    return payload


def check_existing_nova_by_name(payload: Payload, recorder: Recorder) -> Payload:
    payload = dict(payload)
    payload["exists_in_db"] = False
    payload["resolved_nova_id"] = None
    return payload


def create_nova_id(payload: Payload, recorder: Recorder) -> Payload:
    payload = dict(payload)
    payload["nova_id"] = str(uuid.uuid4())
    return payload


def upsert_minimal_nova_metadata(payload: Payload, recorder: Recorder) -> Payload:
    now = utc_now_iso()
    nova_id = str(payload["nova_id"])
    candidate_name = str(payload["candidate_name"])
    normalized = str(payload["normalized_candidate_name"])

    put_item(
        {
            "PK": nova_id,
            "SK": "NOVA",
            "entity_type": "Nova",
            "schema_version": "1",
            "nova_id": nova_id,
            "primary_name": candidate_name,
            "primary_name_normalized": normalized,
            "status": "ACTIVE",
            "created_at": now,
            "updated_at": now,
        }
    )

    put_item(
        {
            "PK": f"NAME#{normalized}",
            "SK": f"NOVA#{nova_id}",
            "entity_type": "NameMapping",
            "schema_version": "1",
            "name_raw": candidate_name,
            "name_normalized": normalized,
            "name_kind": "PRIMARY",
            "nova_id": nova_id,
            "source": "INGESTION",
            "created_at": now,
            "updated_at": now,
        }
    )
    return payload


def publish_ingest_new_nova(payload: Payload, recorder: Recorder) -> Payload:
    payload = dict(payload)
    payload["launched"] = ["ingest_new_nova"]
    return payload


def finalize_jobrun_success(payload: Payload, recorder: Recorder) -> Payload:
    payload = dict(payload)
    payload["outcome"] = payload.get("outcome", "CREATED_AND_LAUNCHED")
    return payload
