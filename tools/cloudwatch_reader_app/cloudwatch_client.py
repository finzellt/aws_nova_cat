"""
CloudWatch client for the Nova Cat log viewer.

Discovers Lambda and Step Functions log groups, runs CloudWatch Logs
Insights queries across them, and returns structured results suitable
for the Flask API layer.

Personal operator tooling — not production code.
"""

import json
import os
import time

import boto3

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Prefixes used to discover Nova Cat log groups automatically.
LAMBDA_LOG_PREFIX = "/aws/lambda/nova-cat"
SFN_LOG_PREFIX = "/aws/vendedlogs/states/nova-cat"

# How long to wait between polls when a Logs Insights query is running.
POLL_INTERVAL_SECONDS = 0.5

# Maximum time to wait for a query before giving up.
QUERY_TIMEOUT_SECONDS = 30


def _get_client():
    return boto3.client("logs", region_name=REGION)


# ── Log group discovery ──────────────────────────────────────────────


def discover_log_groups() -> list[str]:
    """Return all Nova Cat log group names (Lambda + Step Functions)."""
    client = _get_client()
    groups: list[str] = []

    for prefix in (LAMBDA_LOG_PREFIX, SFN_LOG_PREFIX):
        paginator = client.get_paginator("describe_log_groups")
        for page in paginator.paginate(logGroupNamePrefix=prefix):
            for group in page.get("logGroups", []):
                groups.append(group["logGroupName"])

    groups.sort()
    return groups


# ── Logs Insights query execution ────────────────────────────────────


def _build_default_query() -> str:
    """
    Default Insights query that pulls the structured fields your
    Powertools-based Lambdas emit, plus the raw message.

    This matches the canonical query from nova_common.logging:
        fields @timestamp, function_name, state_name, level, message
        | filter correlation_id = "<id>"
        | sort @timestamp asc

    But without a correlation_id filter, so we get everything.

    @message is the full raw log line (the complete JSON blob for
    Powertools logs). It's included so the detail panel can show
    every field without a second API call.
    """
    return "\n".join(
        [
            "fields @timestamp, @logStream, @log, @message,",
            "  level, message, function_name, state_name,",
            "  workflow_name, correlation_id, job_run_id, nova_id,",
            "  error_classification, error_fingerprint, duration_ms,",
            "  attempt_number, candidate_name",
            "| sort @timestamp desc",
            "| limit 2000",
        ]
    )


def _build_trace_query(correlation_id: str) -> str:
    """
    Insights query to fetch every log line sharing a correlation_id,
    sorted chronologically. This reconstructs the full workflow narrative.
    """
    # Escape any quotes in the correlation_id to be safe
    safe_id = correlation_id.replace("'", "\\'")
    return "\n".join(
        [
            "fields @timestamp, @logStream, @log, @message,",
            "  level, message, function_name, state_name,",
            "  workflow_name, correlation_id, job_run_id, nova_id,",
            "  error_classification, error_fingerprint, duration_ms,",
            "  attempt_number, candidate_name",
            f"| filter correlation_id = '{safe_id}'",
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
    Run a CloudWatch Logs Insights query and block until results arrive.

    Args:
        log_groups: Log group names to search across.
        start_epoch: Start time as Unix epoch seconds.
        end_epoch: End time as Unix epoch seconds.
        query_string: CW Insights query. Uses a sensible default if None.

    Returns:
        List of dicts, one per log line. Keys are the field names from
        the query; values are strings.
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

    return _poll_for_results(client, query_id)


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
    Convert the Insights results format into plain dicts.

    Insights returns each row as a list of {"field": "name", "value": "val"}
    dicts. We flatten that into {"name": "val", ...} per row, and skip the
    internal @ptr field.
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


# ── Convenience wrappers for the Flask layer ─────────────────────────


def trace_by_correlation_id(
    correlation_id: str,
    hours_back: int = 24,
    log_groups: list[str] | None = None,
) -> dict:
    """
    Fetch every log line sharing a correlation_id, sorted chronologically.

    Searches further back than the default (24 hours) because workflow
    chains can span long time periods.

    Returns a dict suitable for JSON serialization with keys:
        correlation_id: the ID that was traced
        log_groups:     list of group names that were queried
        row_count:      number of log rows returned
        rows:           the log rows, oldest first
        query:          the Insights query that was used
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
) -> dict:
    """
    High-level entry point: fetch logs from the last N minutes.

    Returns a dict suitable for JSON serialization with keys:
        log_groups: list of group names that were queried
        row_count:  number of log rows returned
        rows:       the log rows themselves
        query:      the Insights query that was used
    """
    end_epoch = int(time.time())
    start_epoch = end_epoch - (minutes * 60)

    if log_groups is None:
        log_groups = discover_log_groups()

    query_used = query_string or _build_default_query()
    rows = run_query(log_groups, start_epoch, end_epoch, query_string)

    return {
        "log_groups": log_groups,
        "row_count": len(rows),
        "rows": rows,
        "query": query_used,
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "minutes": minutes,
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

    if result["rows"]:
        print("\nFirst row:")
        print(json.dumps(result["rows"][0], indent=2))
