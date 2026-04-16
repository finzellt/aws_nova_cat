"""
reference_manager — Lambda handler

Description: ADS reference fetch, upsert, link, and discovery_date computation
Workflows:   refresh_references
Tasks:       FetchReferenceCandidates, NormalizeReference, UpsertReferenceEntity,
             LinkNovaReference, ComputeDiscoveryDate, UpsertDiscoveryDateMetadata

Step Functions passes a `task_name` field in the event payload so this
single Lambda can serve multiple state machine task states. Each task
maps to a private _handle_<taskName> function below.

Environment variables (injected by CDK):
    NOVA_CAT_TABLE_NAME           — DynamoDB table name
    NOVA_CAT_PRIVATE_BUCKET       — private data S3 bucket name
    NOVA_CAT_PUBLIC_SITE_BUCKET   — public site S3 bucket name
    NOVA_CAT_QUARANTINE_TOPIC_ARN — quarantine notifications SNS topic ARN
    ADS_SECRET_NAME               — Secrets Manager secret name for ADS token
                                    (default: "ADSQueryToken")
    LOG_LEVEL                     — logging level (default INFO)
    POWERTOOLS_SERVICE_NAME       — AWS Lambda Powertools service name
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import random
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from urllib.parse import urlencode

import boto3
import requests
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from nova_common.errors import RetryableError, TerminalError
from nova_common.logging import configure_logging, logger
from nova_common.timing import log_duration
from nova_common.tracing import tracer
from nova_common.work_item import DirtyType, write_work_item

# ---------------------------------------------------------------------------
# AWS clients — module-level so moto patches them on fresh import in tests
# ---------------------------------------------------------------------------

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_ADS_SECRET_NAME = os.environ.get("ADS_SECRET_NAME", "ADSQueryToken")
_ADS_API_URL = "https://api.adsabs.harvard.edu/v1/search/query"
_ADS_FIELDS = ["bibcode", "doctype", "title", "date", "author", "doi", "identifier"]
_ADS_COLLECTION_FILTER = "collection:(astronomy OR physics)"

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)
_secretsmanager = boto3.client("secretsmanager")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ARXIV_RE = re.compile(r"^arXiv:(.+)$", re.IGNORECASE)


def _date_sort_key(date_str: str) -> tuple[str, str]:
    """Extract ``(YYYY, MM)`` for month-granularity comparison.

    ADS publication dates use the ``00`` convention for unknown day
    precision.  When *any* date in a comparison has day ``00``, the day
    component is meaningless — only year and month distinguish dates.
    Ignoring the day entirely is the simplest correct strategy.
    """
    parts = date_str.split("-")
    return (parts[0], parts[1])


_DOCTYPE_TO_REFERENCE_TYPE: dict[str, str] = {
    "article": "journal_article",
    "eprint": "arxiv_preprint",
    "inproceedings": "conference_abstract",
    "abstract": "conference_abstract",
    "circular": "cbat_circular",
    "telegram": "atel",
    "catalog": "catalog",
    "software": "software",
}

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _get_ads_token() -> str:
    """Retrieve ADS Bearer token from Secrets Manager."""
    resp = _secretsmanager.get_secret_value(SecretId=_ADS_SECRET_NAME)
    val = resp.get("SecretString") or ""
    try:
        obj = json.loads(val)
        return obj.get("token") or obj.get("ADS_TOKEN") or val
    except Exception:
        return val


def _quote(name: str) -> str:
    """Wrap a name in double-quotes for an ADS query; escape internal quotes."""
    name = (name or "").strip()
    return '"' + name.replace('"', r"\"") + '"' if name else ""


def _build_ads_query(names: list[str]) -> str:
    """OR-join individually-quoted names into an ADS search query string."""
    quoted = [_quote(n) for n in names if (n or "").strip()]
    if not quoted:
        raise TerminalError("No names available to build ADS query")
    return " OR ".join(quoted)


def _ads_request(query: str, token: str) -> list[dict]:
    """Execute ADS search query and return raw doc list.

    Results are restricted to the astronomy and physics collections via ``fq``
    to match the ADS web-portal default and avoid spurious non-astronomical hits.
    """
    encoded = urlencode(
        {
            "q": query,
            "fq": _ADS_COLLECTION_FILTER,
            "fl": ",".join(_ADS_FIELDS),
            "rows": 2000,
            "sort": "date asc",
        }
    )
    url = f"{_ADS_API_URL}?{encoded}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=25)
    except requests.exceptions.Timeout as exc:
        raise RetryableError(f"ADS request timed out: {exc}") from exc
    except requests.exceptions.RequestException as exc:
        raise RetryableError(f"ADS network error: {exc}") from exc

    if resp.status_code == 429:
        raise RetryableError("ADS rate limit exceeded (HTTP 429)")
    if resp.status_code == 401:
        raise TerminalError("ADS authentication failed — check ADSQueryToken secret")
    if resp.status_code >= 500:
        raise RetryableError(f"ADS server error (HTTP {resp.status_code})")

    resp.raise_for_status()
    return resp.json().get("response", {}).get("docs", []) or []


def _normalize_publication_date(ads_date: str | None) -> str | None:
    """
    Normalize an ADS date string to YYYY-MM-00.

    ADS returns YYYY-MM-01T00:00:00Z when only month precision is available
    (day is always a placeholder, never meaningful). We discard the day
    unconditionally and store month-only precision as YYYY-MM-00.
    """
    if not ads_date:
        return None
    # Strip time component if present: "YYYY-MM-01T00:00:00Z" → "YYYY-MM-01"
    date_part = ads_date.strip().split("T")[0]
    parts = date_part.split("-")
    if len(parts) < 2:
        return None
    year, month = parts[0], parts[1].zfill(2)
    if len(year) != 4 or not year.isdigit():
        return None
    if not month.isdigit() or not (1 <= int(month) <= 12):
        return None
    return f"{year}-{month}-00"


def _extract_arxiv_id(identifiers: list | None) -> str | None:
    """Return bare arXiv ID (strip 'arXiv:' prefix) from ADS identifier list."""
    for ident in identifiers or []:
        m = _ARXIV_RE.match(str(ident))
        if m:
            return m.group(1)
    return None


def _map_doctype(doctype: str | None) -> str:
    if not doctype:
        return "other"
    return _DOCTYPE_TO_REFERENCE_TYPE.get(doctype.strip().lower(), "other")


# ---------------------------------------------------------------------------
# Per-task handler implementations
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_fetchReferenceCandidates(event: dict, context: object) -> dict:
    """
    Load nova aliases from DDB, build ADS name query, return raw candidate docs.

    The Nova item carries a denormalized `aliases` list so we can retrieve
    all known names in a single get_item (ADR-005).

    Output shape (feeds into the ReconcileReferences Map state):
        {
            "nova_id": "<uuid>",
            "candidates": [ <raw ADS doc>, ... ],   # ASL ItemsPath
            "candidate_count": <int>,
        }
    """
    nova_id: str | None = event.get("nova_id")
    if not nova_id:
        raise TerminalError("Missing required field: nova_id")

    ads_name_hints: list[str] = (event.get("attributes") or {}).get("ads_name_hints") or []

    nova_item = _table.get_item(Key={"PK": nova_id, "SK": "NOVA"}).get("Item")
    if not nova_item:
        raise TerminalError(f"Nova not found in DDB: {nova_id}")

    primary_name: str = str(nova_item.get("primary_name") or "")
    raw_aliases = nova_item.get("aliases")
    aliases: list[str] = [str(a) for a in raw_aliases] if isinstance(raw_aliases, list) else []

    # Deduplicate preserving insertion order; case-insensitive
    seen: set[str] = set()
    names: list[str] = []
    for name in [primary_name, *aliases, *ads_name_hints]:
        key = name.strip().lower()
        if key and key not in seen:
            names.append(name.strip())
            seen.add(key)

    if not names:
        raise TerminalError(f"No names available for ADS query — nova_id={nova_id}")

    token = _get_ads_token()
    query = _build_ads_query(names)

    logger.info(
        "Querying ADS",
        extra={"query": query, "name_count": len(names), "nova_id": nova_id},
    )

    docs = _ads_request(query, token)

    # Normalize: ensure every ADS_FIELD is present in every doc.
    # ADS omits fields when absent; the ReconcileReferences ItemSelector
    # requires all keys to exist (JSONPath fails on missing fields).
    normalized_docs = [{field: doc.get(field) for field in _ADS_FIELDS} for doc in docs]

    logger.info(
        "ADS returned candidates",
        extra={"candidate_count": len(normalized_docs), "nova_id": nova_id},
    )

    return {
        "nova_id": nova_id,
        "candidates": normalized_docs,
        "candidate_count": len(normalized_docs),
    }


@tracer.capture_method
def _handle_normalizeReference(event: dict, context: object) -> dict:
    """
    Map one raw ADS doc to the Reference schema.

    Called once per Map iteration. Step Functions delivers each candidate doc
    as the Map item; nova_id is injected alongside it via ASL Parameters.

    Input:  raw ADS doc fields + nova_id (from ASL Parameters)
    Output: normalized reference fields for UpsertReferenceEntity
    """
    bibcode: str | None = event.get("bibcode")
    if not bibcode:
        raise TerminalError("ADS doc is missing bibcode — cannot normalize")

    publication_date = _normalize_publication_date(event.get("date"))

    year: int | None = None
    if publication_date:
        with contextlib.suppress(ValueError, IndexError):
            year = int(publication_date[:4])

    # ADS title is a list; take the first element
    raw_title = event.get("title")
    title: str | None = (
        raw_title[0] if isinstance(raw_title, list) and raw_title else raw_title or None
    )

    # ADS doi is sometimes a list too
    raw_doi = event.get("doi")
    doi: str | None = raw_doi[0] if isinstance(raw_doi, list) and raw_doi else raw_doi or None

    authors: list[str] = event.get("author") or []
    arxiv_id = _extract_arxiv_id(event.get("identifier"))
    reference_type = _map_doctype(event.get("doctype"))

    return {
        # pass-through for downstream tasks in the Map chain
        "nova_id": event.get("nova_id"),
        # normalized reference fields
        "bibcode": bibcode,
        "reference_type": reference_type,
        "title": title,
        "year": year,
        "publication_date": publication_date,
        "authors": authors,
        "doi": doi,
        "arxiv_id": arxiv_id,
    }


@tracer.capture_method
def _handle_upsertReferenceEntity(event: dict, context: object) -> dict:
    """
    Write or update the global Reference entity.

    PK = REFERENCE#<bibcode>, SK = METADATA
    Preserves created_at from the existing item when updating.

    Input:  output of NormalizeReference
    Output: {nova_id, bibcode, publication_date} for LinkNovaReference
    """
    bibcode: str | None = event.get("bibcode")
    if not bibcode:
        raise TerminalError("Missing bibcode in UpsertReferenceEntity")

    pk = f"REFERENCE#{bibcode}"
    now = _utcnow_iso()

    existing = _table.get_item(Key={"PK": pk, "SK": "METADATA"}).get("Item")
    created_at = existing["created_at"] if existing else now

    # Build item; omit None-valued optional fields (DDB rejects None)
    item: dict = {
        "PK": pk,
        "SK": "METADATA",
        "entity_type": "Reference",
        "schema_version": "1.0.0",
        "bibcode": bibcode,
        "reference_type": event.get("reference_type") or "other",
        "authors": event.get("authors") or [],
        "created_at": created_at,
        "updated_at": now,
    }
    for field in ("title", "year", "publication_date", "doi", "arxiv_id"):
        val = event.get(field)
        if val is not None:
            item[field] = val

    _table.put_item(Item=item)

    logger.info(
        "Upserted Reference entity",
        extra={"bibcode": bibcode, "action": "update" if existing else "create"},
    )

    return {
        "nova_id": event.get("nova_id"),
        "bibcode": bibcode,
        "publication_date": event.get("publication_date"),
    }


@tracer.capture_method
def _handle_linkNovaReference(event: dict, context: object) -> dict:
    """
    Create the NOVAREF link between a nova and a reference.

    PK = <nova_id>, SK = NOVAREF#<bibcode>
    Idempotent: ConditionalCheckFailedException → link already exists → no-op.

    Input:  output of UpsertReferenceEntity
    Output: {nova_id, bibcode, publication_date, linked: bool}
    """
    nova_id: str | None = event.get("nova_id")
    bibcode: str | None = event.get("bibcode")

    if not nova_id or not bibcode:
        raise TerminalError(
            f"Missing required fields in LinkNovaReference — "
            f"nova_id={nova_id!r} bibcode={bibcode!r}"
        )

    now = _utcnow_iso()

    try:
        _table.put_item(
            Item={
                "PK": nova_id,
                "SK": f"NOVAREF#{bibcode}",
                "entity_type": "NovaReference",
                "schema_version": "1.0.0",
                "nova_id": nova_id,
                "bibcode": bibcode,
                "role": "OTHER",
                "added_by_workflow": "refresh_references",
                "created_at": now,
                "updated_at": now,
            },
            ConditionExpression=Attr("PK").not_exists(),
        )
        logger.info(
            "Linked NovaReference",
            extra={"nova_id": nova_id, "bibcode": bibcode},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(
                "NovaReference already exists — skipping",
                extra={"nova_id": nova_id, "bibcode": bibcode},
            )
        else:
            raise RetryableError(f"DDB error in LinkNovaReference: {exc}") from exc

    return {
        "nova_id": nova_id,
        "bibcode": bibcode,
        "publication_date": event.get("publication_date"),
        "linked": True,
    }


@tracer.capture_method
def _handle_computeDiscoveryDate(event: dict, context: object) -> dict:
    """
    Query all NOVAREF links for the nova, batch-fetch their Reference
    publication_dates, and return the earliest.

    Tiebreaker: lexicographically smallest bibcode (ADR-005 §4).
    Lexicographic comparison on YYYY-MM-00 strings is correct by design.

    Output: {nova_id, earliest_bibcode, earliest_publication_date}
    """
    nova_id: str | None = event.get("nova_id")
    if not nova_id:
        raise TerminalError("Missing required field: nova_id")

    novaref_resp = _table.query(
        KeyConditionExpression=Key("PK").eq(nova_id) & Key("SK").begins_with("NOVAREF#")
    )
    novarefs = novaref_resp.get("Items", [])

    if not novarefs:
        logger.info("No references linked to nova", extra={"nova_id": nova_id})
        return {
            "nova_id": nova_id,
            "earliest_bibcode": None,
            "earliest_publication_date": None,
        }

    bibcodes: list[str] = [str(item["bibcode"]) for item in novarefs if item.get("bibcode")]

    # Batch-get Reference entities; DDB limit is 100 keys per call
    pub_dates: dict[str, str | None] = {}
    for i in range(0, len(bibcodes), 100):
        chunk = bibcodes[i : i + 100]
        keys = [{"PK": f"REFERENCE#{bib}", "SK": "METADATA"} for bib in chunk]
        batch_resp = _dynamodb.batch_get_item(RequestItems={_TABLE_NAME: {"Keys": keys}})
        for ref_item in batch_resp.get("Responses", {}).get(_TABLE_NAME, []):
            bib = ref_item.get("bibcode")
            if bib:
                pub_dates[str(bib)] = (
                    str(ref_item["publication_date"])
                    if ref_item.get("publication_date") is not None
                    else None
                )

    dated = [(bib, pd) for bib, pd in pub_dates.items() if pd]

    if not dated:
        logger.info(
            "No dated references found",
            extra={"nova_id": nova_id, "bibcode_count": len(bibcodes)},
        )
        return {
            "nova_id": nova_id,
            "earliest_bibcode": None,
            "earliest_publication_date": None,
        }

    # min by (year-month, bibcode) — month granularity avoids day-00 artefacts
    earliest_bibcode, earliest_date = min(dated, key=lambda x: (_date_sort_key(x[1]), x[0]))

    logger.info(
        "Computed discovery date",
        extra={
            "nova_id": nova_id,
            "earliest_bibcode": earliest_bibcode,
            "earliest_publication_date": earliest_date,
        },
    )

    return {
        "nova_id": nova_id,
        "earliest_bibcode": earliest_bibcode,
        "earliest_publication_date": earliest_date,
    }


@tracer.capture_method
def _handle_upsertDiscoveryDateMetadata(event: dict, context: object) -> dict:
    """
    Update Nova.discovery_date — only if the new date is strictly earlier than
    the current value (monotonically earlier invariant, ADR-005 §4).

    No-op when: no date was computed, or Nova already has an equal/earlier date.

    Output: {nova_id, updated: bool, discovery_date, discovery_date_old?}
    """
    nova_id: str | None = event.get("nova_id")
    if not nova_id:
        raise TerminalError("Missing required field: nova_id")

    # --- ADR-031 Decision 7: WorkItem for the regeneration pipeline ---
    # Written unconditionally: by the time UpsertDiscoveryDateMetadata runs,
    # the ReconcileReferences Map state has already linked all references.
    # The WorkItem signals "this nova has new reference data" regardless of
    # whether the discovery date was updated.
    write_work_item(
        _table,
        nova_id=nova_id,
        dirty_type=DirtyType.references,
        source_workflow="refresh_references",
        job_run_id=str(event.get("job_run_id", event.get("correlation_id", "unknown"))),
        correlation_id=str(event.get("correlation_id", "unknown")),
    )

    new_date: str | None = event.get("earliest_publication_date")
    earliest_bibcode: str | None = event.get("earliest_bibcode")

    if not new_date:
        logger.info(
            "No discovery date to upsert — skipping",
            extra={"nova_id": nova_id},
        )
        return {"nova_id": nova_id, "updated": False, "discovery_date": None}

    nova_item = _table.get_item(Key={"PK": nova_id, "SK": "NOVA"}).get("Item")
    if not nova_item:
        raise TerminalError(f"Nova not found: {nova_id}")

    raw_date = nova_item.get("discovery_date")
    current_date: str | None = str(raw_date) if raw_date is not None else None

    # Monotonically earlier invariant: only overwrite with a strictly earlier
    # month.  Day-00 (month-only precision) dates are treated as equal to any
    # day-precise date in the same month — no overwrite.
    if current_date is not None and _date_sort_key(new_date) >= _date_sort_key(current_date):
        logger.info(
            "Discovery date not earlier — no-op",
            extra={
                "nova_id": nova_id,
                "discovery_date_old": current_date,
                "discovery_date_new": new_date,
            },
        )
        return {"nova_id": nova_id, "updated": False, "discovery_date": current_date}

    now = _utcnow_iso()
    _table.update_item(
        Key={"PK": nova_id, "SK": "NOVA"},
        UpdateExpression="SET discovery_date = :dd, updated_at = :ua",
        ExpressionAttributeValues={":dd": new_date, ":ua": now},
    )

    logger.info(
        "Updated discovery date",
        extra={
            "nova_id": nova_id,
            "discovery_date_old": current_date,
            "discovery_date_new": new_date,
            "earliest_bibcode": earliest_bibcode,
        },
    )

    return {
        "nova_id": nova_id,
        "updated": True,
        "discovery_date": new_date,
        "discovery_date_old": current_date,
    }


# ---------------------------------------------------------------------------
# Combined task: FetchAndReconcileReferences
# ---------------------------------------------------------------------------

# Quarantine classification reason for refresh_references item-level failures.
# Matches the "OTHER" reason code used by the ASL ItemFailureHandler.
_QUARANTINE_REASON_CODE = "OTHER"
_QUARANTINE_CLASSIFICATION_REASON = (
    "Quarantine triggered — ambiguous or inconclusive results. Manual review required."
)

# Per-item retry parameters — matches the ASL Map Iterator retry policy:
#   MaxAttempts=2, IntervalSeconds=2, BackoffRate=4, JitterStrategy=FULL
_ITEM_MAX_RETRIES = 2
_ITEM_RETRY_BASE_SECONDS = 2
_ITEM_RETRY_BACKOFF_RATE = 4


def _compute_quarantine_fingerprint(reason_code: str, workflow_name: str, primary_id: str) -> str:
    """Short hex digest for cross-referencing quarantine logs.

    Mirrors quarantine_handler._compute_error_fingerprint — same algorithm,
    kept local to avoid cross-service import.
    """
    raw = f"{reason_code}:{workflow_name}:{primary_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _persist_quarantine_to_job_run(
    *,
    job_run: dict,
    nova_id: str,
    workflow_name: str = "refresh_references",
) -> str:
    """Write quarantine diagnostics onto the JobRun DDB record.

    Returns the computed error_fingerprint for inclusion in the summary.
    """
    now = _utcnow_iso()
    fingerprint = _compute_quarantine_fingerprint(_QUARANTINE_REASON_CODE, workflow_name, nova_id)

    _table.update_item(
        Key={"PK": job_run["pk"], "SK": job_run["sk"]},
        UpdateExpression=(
            "SET quarantine_reason_code = :reason_code, "
            "classification_reason = :classification_reason, "
            "error_fingerprint = :error_fingerprint, "
            "quarantined_at = :quarantined_at, "
            "updated_at = :updated_at"
        ),
        ExpressionAttributeValues={
            ":reason_code": _QUARANTINE_REASON_CODE,
            ":classification_reason": _QUARANTINE_CLASSIFICATION_REASON,
            ":error_fingerprint": fingerprint,
            ":quarantined_at": now,
            ":updated_at": now,
        },
    )
    return fingerprint


def _reconcile_one_candidate(candidate: dict, nova_id: str, context: object) -> dict:
    """Run normalize → upsert → link for a single ADS candidate.

    Wraps each phase with the per-item retry policy (2 retries, exponential
    backoff with factor 4, full jitter) for RetryableError only.

    Returns the LinkNovaReference output dict on success.
    Raises on terminal (non-retryable) errors.
    """

    def _with_retry(fn: Callable[..., dict], event: dict) -> dict:
        last_exc: Exception | None = None
        for attempt in range(_ITEM_MAX_RETRIES + 1):
            try:
                return fn(event, context)
            except RetryableError as exc:
                last_exc = exc
                if attempt < _ITEM_MAX_RETRIES:
                    delay = _ITEM_RETRY_BASE_SECONDS * (_ITEM_RETRY_BACKOFF_RATE**attempt)
                    time.sleep(random.uniform(0, delay))  # noqa: S311
                    logger.warning(
                        "Retrying after RetryableError",
                        extra={
                            "attempt": attempt + 1,
                            "max_retries": _ITEM_MAX_RETRIES,
                            "error": str(exc),
                        },
                    )
        raise last_exc  # type: ignore[misc]

    # Step 1: Normalize
    normalize_event = {**candidate, "nova_id": nova_id}
    normalized = _with_retry(_handle_normalizeReference, normalize_event)

    # Step 2: Upsert Reference entity
    upserted = _with_retry(_handle_upsertReferenceEntity, normalized)

    # Step 3: Link NovaReference
    linked = _with_retry(_handle_linkNovaReference, upserted)

    return linked


@tracer.capture_method
def _handle_fetchAndReconcileReferences(event: dict, context: object) -> dict:
    """
    Combined task: fetch ADS candidates then reconcile each one internally.

    Replaces the FetchReferenceCandidates → ReconcileReferences Map pattern.
    The full ADS candidate list never transits through Step Functions state,
    eliminating the 256KB payload limit bottleneck.

    Per-item error isolation: a single candidate's failure is quarantined
    (logged + persisted to JobRun) without aborting the loop. Processing
    continues to the next candidate.

    Output shape (lightweight summary only):
        {
            "nova_id": "<uuid>",
            "total_candidates": N,
            "reconciled": M,
            "quarantined": Q,
            "quarantined_bibcodes": ["bibcode1", ...],
        }
    """
    nova_id: str | None = event.get("nova_id")
    if not nova_id:
        raise TerminalError("Missing required field: nova_id")

    correlation_id: str = event.get("correlation_id", "unknown")
    job_run: dict | None = event.get("job_run")

    # Phase 1: Fetch ADS candidates (reuse existing logic)
    fetch_result = _handle_fetchReferenceCandidates(event, context)
    candidates: list[dict] = fetch_result["candidates"]

    # Phase 2: Reconcile each candidate sequentially
    reconciled = 0
    quarantined_bibcodes: list[str] = []

    for candidate in candidates:
        bibcode = candidate.get("bibcode") or "unknown"
        try:
            _reconcile_one_candidate(candidate, nova_id, context)
            reconciled += 1
        except Exception as exc:
            quarantined_bibcodes.append(bibcode)

            # Structured quarantine log line — replaces SNS notification
            logger.warning(
                "Reference candidate quarantined",
                extra={
                    "bibcode": bibcode,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "nova_id": nova_id,
                    "correlation_id": correlation_id,
                },
            )

            # Persist quarantine diagnostics to the JobRun record
            if job_run:
                _persist_quarantine_to_job_run(
                    job_run=job_run,
                    nova_id=nova_id,
                )

    logger.info(
        "FetchAndReconcileReferences complete",
        extra={
            "nova_id": nova_id,
            "total_candidates": len(candidates),
            "reconciled": reconciled,
            "quarantined": len(quarantined_bibcodes),
            "quarantined_bibcodes": quarantined_bibcodes,
        },
    )

    return {
        "nova_id": nova_id,
        "total_candidates": len(candidates),
        "reconciled": reconciled,
        "quarantined": len(quarantined_bibcodes),
        "quarantined_bibcodes": quarantined_bibcodes,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict, context: object) -> dict:
    """
    Lambda entry point.

    Expected event shape (minimum):
        {
            "task_name": "<StateName>",
            "correlation_id": "<uuid>",
            "nova_id": "<uuid>",
            ... task-specific fields ...
        }
    """
    configure_logging(event)

    task_name = event.get("task_name")
    if not task_name:
        raise ValueError("Missing required field: task_name")

    handler_fn = _TASK_HANDLERS.get(task_name)
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}. Known tasks: {list(_TASK_HANDLERS)}")

    logger.info("Task started", extra={"task_name": task_name})
    with log_duration(f"task:{task_name}"):
        result = handler_fn(event, context)
    return result


# ---------------------------------------------------------------------------
# Dispatch table — defined after implementations to avoid forward references
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict, object], dict]] = {
    "FetchReferenceCandidates": _handle_fetchReferenceCandidates,
    "NormalizeReference": _handle_normalizeReference,
    "UpsertReferenceEntity": _handle_upsertReferenceEntity,
    "LinkNovaReference": _handle_linkNovaReference,
    "FetchAndReconcileReferences": _handle_fetchAndReconcileReferences,
    "ComputeDiscoveryDate": _handle_computeDiscoveryDate,
    "UpsertDiscoveryDateMetadata": _handle_upsertDiscoveryDateMetadata,
}
