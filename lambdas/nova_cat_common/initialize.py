from __future__ import annotations

import uuid

from nova_cat_common.ddb import put_item
from nova_cat_common.jobrun import utc_now_iso


def begin_jobrun(payload: dict, recorder) -> dict:
    # For thin slice: attach workflow_name into payload so recorder can use it consistently
    payload = dict(payload)
    payload["workflow_name"] = payload.get("workflow_name", "initialize_nova")
    recorder = recorder  # explicit
    recorder.jobrun_started()
    payload["job_run_id"] = recorder.job_run_id
    return payload


def normalize_candidate_name(payload: dict, recorder) -> dict:
    payload = dict(payload)
    name = payload["candidate_name"]
    payload["normalized_candidate_name"] = name.strip().lower()
    return payload


def check_existing_nova_by_name(payload: dict, recorder) -> dict:
    payload = dict(payload)
    norm = payload["normalized_candidate_name"]
    pk = f"NAME#{norm}"  # :contentReference[oaicite:17]{index=17}
    print(pk)
    # In DynamoDB model, NameMapping uses PK NAME#..., SK includes nova id.
    # For thin slice: do a best-effort “get” against a single known SK pattern is not possible,
    # so we return “not found” and thicken later with Query.
    payload["exists_in_db"] = False
    payload["resolved_nova_id"] = None
    return payload


def create_nova_id(payload: dict, recorder) -> dict:
    payload = dict(payload)
    payload["nova_id"] = str(uuid.uuid4())
    return payload


def upsert_minimal_nova_metadata(payload: dict, recorder) -> dict:
    # Persist Nova item PK=<nova_id>, SK="NOVA" :contentReference[oaicite:18]{index=18}
    now = utc_now_iso()
    nova_id = payload["nova_id"]
    put_item(
        {
            "PK": nova_id,
            "SK": "NOVA",
            "entity_type": "Nova",
            "schema_version": "1",
            "nova_id": nova_id,
            "primary_name": payload["candidate_name"],
            "primary_name_normalized": payload["normalized_candidate_name"],
            "status": "ACTIVE",
            "created_at": now,
            "updated_at": now,
        }
    )
    # Also write NameMapping (primary) :contentReference[oaicite:19]{index=19}
    put_item(
        {
            "PK": f"NAME#{payload['normalized_candidate_name']}",
            "SK": f"NOVA#{nova_id}",
            "entity_type": "NameMapping",
            "schema_version": "1",
            "name_raw": payload["candidate_name"],
            "name_normalized": payload["normalized_candidate_name"],
            "name_kind": "PRIMARY",
            "nova_id": nova_id,
            "source": "INGESTION",
            "created_at": now,
            "updated_at": now,
        }
    )
    return payload


def publish_ingest_new_nova(payload: dict, recorder) -> dict:
    # This task will call states:StartExecution for ingest_new_nova (CDK already grants permission).
    # We’ll implement the StartExecution call in the next step (after we wire the SM ARNs into env).
    payload = dict(payload)
    payload["launched"] = ["ingest_new_nova"]
    return payload


def finalize_jobrun_success(payload: dict, recorder) -> dict:
    payload = dict(payload)
    payload["outcome"] = payload.get("outcome", "CREATED_AND_LAUNCHED")
    return payload
