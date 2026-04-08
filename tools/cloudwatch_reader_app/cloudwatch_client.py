"""
CloudWatch client for the Nova Cat log viewer.

Discovers Lambda, Step Functions, and ECS/Fargate log groups, runs
CloudWatch Logs Insights queries across them, and returns structured,
enriched results suitable for the Flask API layer.

Enrichment (applied in Python after Insights returns):
  - START/END RequestId platform lines are filtered out in the query.
  - REPORT lines are parsed into duration_ms, billed_ms, memory_mb,
    memory_used_mb, and cold_start fields, with a synthetic message.
  - Each row is tagged with a human-readable `source` field derived
    from @log (e.g. "lambda:ticket-parser", "fargate:artifact-generator",
    "sfn:initialize-nova").
  - Fargate stdlib-JSON logs (which use `levelname` instead of `level`)
    are normalized to use the same field names as Powertools logs.

Personal operator tooling — not production code.
"""

from __future__ import annotations

import json
import os
import re
import time

import boto3

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Prefixes used to discover Nova Cat log groups automatically.
LOG_GROUP_PREFIXES = [
    "/aws/lambda/nova-cat",  # Lambda functions
    "/aws/vendedlogs/states/nova-cat",  # Step Functions (Express)
]

# Additional prefixes for ECS/Fargate log groups. CDK auto-generates
# log group names from the construct tree path, so the exact name
# depends on your stack. Add your Fargate log group prefix here if
# auto-discovery misses it.
#
# You can also set the LOG_VIEWER_EXTRA_PREFIXES env var to a
# comma-separated list of additional prefixes to discover.
ECS_LOG_PREFIXES = [
    "NovaCat",  # Catches CDK-generated names like NovaCatStack-Workflows...
]

POLL_INTERVAL_SECONDS = 0.5
QUERY_TIMEOUT_SECONDS = 30

# Regex for parsing Lambda REPORT lines.
_REPORT_RE = re.compile(
    r"REPORT RequestId:\s*(?P<request_id>\S+)\s+"
    r"Duration:\s*(?P<duration>[\d.]+)\s*ms\s+"
    r"Billed Duration:\s*(?P<billed>[\d.]+)\s*ms\s+"
    r"Memory Size:\s*(?P<mem_size>\d+)\s*MB\s+"
    r"Max Memory Used:\s*(?P<mem_used>\d+)\s*MB"
    r"(?:\s+Init Duration:\s*(?P<init_duration>[\d.]+)\s*ms)?"
)

# Regex for START/END lines — these are filtered in the query, but
# we keep the regex as a fallback if someone passes a custom query.
_PLATFORM_NOISE_RE = re.compile(r"^(START|END) RequestId:")


def _get_client():
    return boto3.client("logs", region_name=REGION)


# ── Log group discovery ──────────────────────────────────────────────


def discover_log_groups() -> list[str]:
    """Return all Nova Cat log group names (Lambda + SFN + ECS)."""
    client = _get_client()
    groups: list[str] = []

    all_prefixes = list(LOG_GROUP_PREFIXES) + list(ECS_LOG_PREFIXES)

    # Extra prefixes from env var (comma-separated).
    extra = os.environ.get("LOG_VIEWER_EXTRA_PREFIXES", "")
    if extra.strip():
        all_prefixes.extend(p.strip() for p in extra.split(",") if p.strip())

    seen: set[str] = set()
    for prefix in all_prefixes:
        paginator = client.get_paginator("describe_log_groups")
        for page in paginator.paginate(logGroupNamePrefix=prefix):
            for group in page.get("logGroups", []):
                name = group["logGroupName"]
                if name not in seen:
                    seen.add(name)
                    groups.append(name)

    groups.sort()
    return groups


# ── Logs Insights query execution ────────────────────────────────────


def _build_default_query(limit: int = 2000) -> str:
    """
    Default Insights query with enrichment-friendly field selection.

    Key changes from the naive query:
      - Filters out START/END RequestId platform noise at the query
        level (saves bandwidth and row budget).
      - Pulls @message for raw log access and @log for source tagging.
      - Pulls both Powertools (`level`) and stdlib (`levelname`) field
        names so the Python enrichment layer can normalize them.
      - Pulls Fargate-specific fields (plan_id, phase, artifact,
        release_id) that the artifact generator emits.
    """
    return "\n".join(
        [
            "fields @timestamp, @logStream, @log, @message,",
            "  level, levelname, message, function_name, state_name,",
            "  workflow_name, correlation_id, job_run_id, nova_id,",
            "  error_classification, error_fingerprint, duration_ms,",
            "  attempt_number, candidate_name,",
            "  plan_id, phase, artifact, release_id",
            "| filter @message not like /^(START|END) RequestId/",
            "| sort @timestamp desc",
            f"| limit {limit}",
        ]
    )


def _build_trace_query(correlation_id: str) -> str:
    """
    Insights query to trace a workflow by correlation_id.
    """
    safe_id = correlation_id.replace("'", "\\'")
    return "\n".join(
        [
            "fields @timestamp, @logStream, @log, @message,",
            "  level, levelname, message, function_name, state_name,",
            "  workflow_name, correlation_id, job_run_id, nova_id,",
            "  error_classification, error_fingerprint, duration_ms,",
            "  attempt_number, candidate_name,",
            "  plan_id, phase, artifact, release_id",
            f"| filter correlation_id = '{safe_id}'",
            "| filter @message not like /^(START|END) RequestId/",
            "| sort @timestamp asc",
            "| limit 5000",
        ]
    )


def run_query(
    log_groups: list[str],
    start_epoch: int,
    end_epoch: int,
    query_string: str | None = None,
) -> list[dict[str, str]]:
    """
    Run a CloudWatch Logs Insights query, block until results arrive,
    then enrich the rows with source tagging and REPORT parsing.
    """
    if not log_groups:
        return []

    client = _get_client()
    query_string = query_string or _build_default_query()

    response = client.start_query(
        logGroupNames=log_groups,
        startTime=start_epoch,
        endTime=end_epoch,
        queryString=query_string,
    )
    query_id = response["queryId"]

    raw_rows = _poll_for_results(client, query_id)
    return _enrich_rows(raw_rows)


def _poll_for_results(client, query_id: str) -> list[dict[str, str]]:
    """Poll until the Insights query completes or times out."""
    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        response = client.get_query_results(queryId=query_id)
        status = response["status"]

        if status == "Complete":
            return _parse_results(response["results"])
        if status in ("Failed", "Cancelled", "Timeout", "Unknown"):
            raise RuntimeError(f"CloudWatch query {status}: {query_id}")

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Query {query_id} did not complete within {QUERY_TIMEOUT_SECONDS}s")


def _parse_results(raw_results: list[list[dict]]) -> list[dict[str, str]]:
    """
    Convert Insights results format into plain dicts.

    Insights returns each row as a list of {"field": "name", "value": "val"}.
    We flatten into {"name": "val", ...} and skip @ptr.
    """
    rows: list[dict[str, str]] = []
    for raw_row in raw_results:
        row: dict[str, str] = {}
        for entry in raw_row:
            field = entry["field"]
            if field == "@ptr":
                continue
            row[field] = entry.get("value", "")
        rows.append(row)
    return rows


# ── Row enrichment ───────────────────────────────────────────────────


def _enrich_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Post-process rows returned by Insights:
      1. Tag each row with a human-readable `source`.
      2. Normalize Fargate stdlib fields to Powertools names.
      3. Parse REPORT lines into structured fields.
      4. Extract fields from raw JSON for rows Insights didn't parse.
      5. Drop any remaining START/END noise (fallback for custom queries).
    """
    enriched: list[dict[str, str]] = []

    for row in rows:
        msg = row.get("@message", "")

        # Skip START/END lines that slipped through (custom queries).
        if _PLATFORM_NOISE_RE.match(msg):
            continue

        # 1. Source tagging from @log.
        row["source"] = _extract_source(row.get("@log", ""))

        # 2. Try to extract fields from @message JSON for rows that
        #    Insights didn't auto-parse (e.g. Fargate stdlib logs).
        if not row.get("message") and msg.startswith("{"):
            _extract_from_raw_json(row, msg)

        # 3. Normalize Fargate stdlib fields.
        _normalize_fargate_fields(row)

        # 4. Parse REPORT lines.
        if msg.startswith("REPORT RequestId:"):
            _parse_report_line(row, msg)

        enriched.append(row)

    return enriched


def _extract_source(log_field: str) -> str:
    """
    Derive a human-readable source label from the @log field.

    @log looks like:
        "123456789012:log-group:/aws/lambda/nova-cat-ticket-parser:*"
        "123456789012:log-group:/aws/vendedlogs/states/nova-cat-init-nova:*"
        "123456789012:log-group:NovaCatStack-WorkflowsArtifactGenerator...:*"

    Returns labels like:
        "lambda:ticket-parser"
        "sfn:init-nova"
        "fargate:artifact-generator"
        "unknown"
    """
    if not log_field:
        return "unknown"

    # Extract the log group name from the @log field.
    # Format: "account:log-group:GROUP_NAME:*"
    parts = log_field.split(":log-group:", 1)
    if len(parts) < 2:
        return "unknown"
    group_name = parts[1].rstrip(":*").rstrip("*").rstrip(":")

    # Lambda logs
    if group_name.startswith("/aws/lambda/nova-cat-"):
        fn_name = group_name.replace("/aws/lambda/nova-cat-", "")
        return f"lambda:{fn_name}"
    if group_name.startswith("/aws/lambda/nova-cat"):
        fn_name = group_name.replace("/aws/lambda/", "")
        return f"lambda:{fn_name}"

    # Step Functions logs
    if group_name.startswith("/aws/vendedlogs/states/nova-cat-"):
        sfn_name = group_name.replace("/aws/vendedlogs/states/nova-cat-", "")
        return f"sfn:{sfn_name}"
    if group_name.startswith("/aws/vendedlogs/states/"):
        sfn_name = group_name.replace("/aws/vendedlogs/states/", "")
        return f"sfn:{sfn_name}"

    # ECS/Fargate logs — CDK-generated names contain "ArtifactGenerator"
    # or the task family name.
    lower = group_name.lower()
    if "artifactgenerator" in lower or "artifact-generator" in lower:
        return "fargate:artifact-generator"

    # Generic ECS
    if "ecs" in lower or "fargate" in lower:
        return f"fargate:{group_name.split('/')[-1][:30]}"

    return f"other:{group_name.split('/')[-1][:30]}"


def _normalize_fargate_fields(row: dict[str, str]) -> None:
    """
    Fargate's stdlib JSON logging uses different field names than
    Powertools. Normalize to Powertools conventions so the UI works
    uniformly.

    stdlib → Powertools mapping:
        levelname → level
        (message is the same in both)
    """
    # Normalize level: prefer Powertools `level`, fall back to `levelname`.
    if not row.get("level") and row.get("levelname"):
        row["level"] = row["levelname"]


def _parse_report_line(row: dict[str, str], msg: str) -> None:
    """
    Parse a Lambda REPORT line into structured fields and synthesize
    a human-readable message.

    Input:  "REPORT RequestId: abc Duration: 45.2 ms Billed Duration: ..."
    Output: Sets message, level, report_* fields on the row.
    """
    match = _REPORT_RE.search(msg)
    if not match:
        row["message"] = msg
        row["level"] = "INFO"
        return

    duration = match.group("duration")
    billed = match.group("billed")
    mem_size = match.group("mem_size")
    mem_used = match.group("mem_used")
    init_dur = match.group("init_duration")

    row["level"] = "INFO"
    row["report_duration_ms"] = duration
    row["report_billed_ms"] = billed
    row["report_memory_size_mb"] = mem_size
    row["report_memory_used_mb"] = mem_used

    # Build a human-readable summary.
    cold = ""
    if init_dur:
        row["report_init_duration_ms"] = init_dur
        row["cold_start"] = "true"
        cold = f" COLD START (init {init_dur}ms)"
    else:
        row["cold_start"] = "false"

    mem_pct = int(100 * float(mem_used) / float(mem_size)) if float(mem_size) > 0 else 0
    row["message"] = (
        f"Invocation complete: {duration}ms"
        f" (billed {billed}ms)"
        f", memory {mem_used}/{mem_size} MB ({mem_pct}%)"
        f"{cold}"
    )

    # If the source is a Lambda, populate function_name from source.
    source = row.get("source", "")
    if source.startswith("lambda:") and not row.get("function_name"):
        row["function_name"] = "nova-cat-" + source.split(":", 1)[1]


def _extract_from_raw_json(row: dict[str, str], raw: str) -> None:
    """
    For rows where Insights didn't auto-parse fields (common with
    Fargate stdlib JSON logs), try to parse @message as JSON and
    extract useful fields into the row.
    """
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return

    if not isinstance(parsed, dict):
        return

    # Map of JSON keys → row keys. Only set if not already present.
    field_map = {
        "message": "message",
        "levelname": "level",
        "level": "level",
        "workflow_name": "workflow_name",
        "nova_id": "nova_id",
        "plan_id": "plan_id",
        "phase": "phase",
        "artifact": "artifact",
        "correlation_id": "correlation_id",
        "job_run_id": "job_run_id",
        "duration_ms": "duration_ms",
        "error": "error_detail",
        "error_classification": "error_classification",
        "release_id": "release_id",
        "nova_count": "nova_count",
        "spectra_count": "spectra_count",
        "photometry_count": "photometry_count",
    }

    for json_key, row_key in field_map.items():
        if not row.get(row_key):
            val = parsed.get(json_key)
            if val is not None:
                row[row_key] = str(val)


# ── Convenience wrappers for the Flask layer ─────────────────────────


def trace_by_correlation_id(
    correlation_id: str,
    hours_back: int = 24,
    log_groups: list[str] | None = None,
) -> dict:
    """
    Fetch every log line sharing a correlation_id, sorted chronologically.
    """
    end_epoch = int(time.time())
    start_epoch = end_epoch - (hours_back * 3600)

    if log_groups is None:
        log_groups = discover_log_groups()

    query_string = _build_trace_query(correlation_id)
    rows = run_query(log_groups, start_epoch, end_epoch, query_string)

    return {
        "correlation_id": correlation_id,
        "log_groups": log_groups,
        "row_count": len(rows),
        "rows": rows,
        "query": query_string,
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "hours_back": hours_back,
    }


def fetch_recent_logs(
    minutes: int = 30,
    query_string: str | None = None,
    log_groups: list[str] | None = None,
    limit: int = 2000,
) -> dict:
    """
    High-level entry point: fetch logs from the last N minutes.
    """
    end_epoch = int(time.time())
    start_epoch = end_epoch - (minutes * 60)

    if log_groups is None:
        log_groups = discover_log_groups()

    query_used = query_string or _build_default_query(limit=limit)
    rows = run_query(log_groups, start_epoch, end_epoch, query_used)

    return {
        "log_groups": log_groups,
        "row_count": len(rows),
        "rows": rows,
        "query": query_used,
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "minutes": minutes,
        "limit": limit,
    }


def fetch_logs_range(
    start_epoch: int,
    end_epoch: int,
    query_string: str | None = None,
    log_groups: list[str] | None = None,
    limit: int = 2000,
) -> dict:
    """
    Fetch logs for an explicit time range (epoch seconds).
    """
    if log_groups is None:
        log_groups = discover_log_groups()

    query_used = query_string or _build_default_query(limit=limit)
    rows = run_query(log_groups, start_epoch, end_epoch, query_used)

    return {
        "log_groups": log_groups,
        "row_count": len(rows),
        "rows": rows,
        "query": query_used,
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "limit": limit,
    }


# ── CLI smoke test ───────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Region: {REGION}")
    print("Discovering log groups...")
    groups = discover_log_groups()
    for g in groups:
        print(f"  {g}")

    print(f"\nQuerying last 30 minutes across {len(groups)} groups...")
    result = fetch_recent_logs(minutes=30)
    print(f"Got {result['row_count']} rows.")

    # Show source distribution.
    sources: dict[str, int] = {}
    for row in result["rows"]:
        src = row.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1
    print("\nRows by source:")
    for src, count in sorted(sources.items(), key=lambda x: -x[1]):
        print(f"  {src}: {count}")

    if result["rows"]:
        print("\nFirst row:")
        print(json.dumps(result["rows"][0], indent=2))
