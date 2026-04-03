#!/usr/bin/env python3
"""Fetch and search recent CloudWatch logs for nova-cat-smoke-reference-manager.

Usage:
    python fetch_smoke_logs.py                  # last 30 minutes, all log groups
    python fetch_smoke_logs.py --minutes 60     # last 60 minutes
    python fetch_smoke_logs.py --nova-id abc123  # filter by nova_id
    python fetch_smoke_logs.py --query error     # case-insensitive text search
    python fetch_smoke_logs.py --all-lambdas     # search ALL smoke Lambdas, not just reference_manager
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

import boto3

REGION = "us-east-1"
SMOKE_PREFIX = "nova-cat-smoke"

# Log groups to search (reference_manager by default, --all-lambdas for everything)
REFERENCE_MANAGER_LOG_GROUP = f"/aws/lambda/{SMOKE_PREFIX}-reference-manager"

ALL_SMOKE_LOG_GROUPS = [
    f"/aws/lambda/{SMOKE_PREFIX}-reference-manager",
    f"/aws/lambda/{SMOKE_PREFIX}-job-run-manager",
    f"/aws/lambda/{SMOKE_PREFIX}-idempotency-guard",
    f"/aws/lambda/{SMOKE_PREFIX}-workflow-launcher",
    f"/aws/lambda/{SMOKE_PREFIX}-nova-resolver",
    f"/aws/lambda/{SMOKE_PREFIX}-archive-resolver",
    f"/aws/lambda/{SMOKE_PREFIX}-quarantine-handler",
]

# Patterns that indicate problems
ERROR_PATTERNS = [
    "error",
    "Error",
    "ERROR",
    "429",
    "rate limit",
    "Rate Exceeded",
    "ThrottlingException",
    "TooManyRequests",
    "FAILED",
    "TerminalError",
    "RetryableError",
    "Traceback",
    "timed out",
    "timeout",
]


def fetch_logs(
    log_group: str,
    start_time: int,
    end_time: int,
    client: object,
) -> list[dict[str, object]]:
    """Fetch all log events from a log group within the time range."""
    events: list[dict[str, object]] = []
    kwargs = {
        "logGroupName": log_group,
        "startTime": start_time,
        "endTime": end_time,
        "interleaved": True,
    }
    try:
        while True:
            resp = client.filter_log_events(**kwargs)  # type: ignore[union-attr]
            events.extend(resp.get("events", []))
            next_token = resp.get("nextToken")
            if not next_token:
                break
            kwargs["nextToken"] = next_token
    except client.exceptions.ResourceNotFoundException:  # type: ignore[union-attr]
        print(f"  ⚠ Log group not found: {log_group}")
    return events


def classify_event(message: str) -> list[str]:
    """Return list of matching error patterns found in the message."""
    matches = []
    for pattern in ERROR_PATTERNS:
        if pattern in message:
            matches.append(pattern)
    return matches


def format_timestamp(ts_ms: int) -> str:
    """Convert millisecond timestamp to human-readable UTC string."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    parser = argparse.ArgumentParser(description="Search smoke stack CloudWatch logs")
    parser.add_argument("--minutes", type=int, default=30, help="How many minutes back to search")
    parser.add_argument("--nova-id", type=str, help="Filter events containing this nova_id")
    parser.add_argument("--query", type=str, help="Case-insensitive text search")
    parser.add_argument("--all-lambdas", action="store_true", help="Search all smoke Lambdas")
    parser.add_argument(
        "--errors-only", action="store_true", help="Only show events matching error patterns"
    )
    parser.add_argument("--raw", action="store_true", help="Print raw messages without formatting")
    args = parser.parse_args()

    client = boto3.client("logs", region_name=REGION)
    now = int(time.time() * 1000)
    start = now - (args.minutes * 60 * 1000)

    log_groups = ALL_SMOKE_LOG_GROUPS if args.all_lambdas else [REFERENCE_MANAGER_LOG_GROUP]

    print(f"Searching logs from the last {args.minutes} minutes")
    print(f"Log groups: {len(log_groups)}")
    if args.nova_id:
        print(f"Filtering by nova_id: {args.nova_id}")
    if args.query:
        print(f"Text search: {args.query!r}")
    print()

    all_events: list[tuple[str, dict[str, object]]] = []

    for lg in log_groups:
        short_name = lg.split("/")[-1]
        events = fetch_logs(lg, start, now, client)
        print(f"  {short_name}: {len(events)} events")
        for e in events:
            all_events.append((short_name, e))

    # Sort all events by timestamp across log groups
    all_events.sort(key=lambda x: x[1].get("timestamp", 0))

    print(f"\nTotal events: {len(all_events)}")
    print()

    # Filter and display
    displayed = 0
    error_events = 0

    for fn_name, event in all_events:
        message = str(event.get("message", ""))
        ts = event.get("timestamp", 0)

        # Apply filters
        if args.nova_id and args.nova_id not in message:
            continue
        if args.query and args.query.lower() not in message.lower():
            continue

        matches = classify_event(message)
        if matches:
            error_events += 1

        if args.errors_only and not matches:
            continue

        displayed += 1

        if args.raw:
            print(message.rstrip())
            continue

        # Pretty print
        ts_str = format_timestamp(ts) if isinstance(ts, int) else "?"
        prefix = f"[{ts_str}] [{fn_name}]"

        if matches:
            match_str = ", ".join(sorted(set(matches)))
            prefix += f" ⚠ MATCHES: {match_str}"

        # Try to parse as JSON for structured logs
        try:
            # Skip START/END/REPORT lines
            if message.startswith(("START ", "END ", "REPORT ")):
                if not args.errors_only:
                    print(f"{prefix} {message.strip()[:120]}")
                continue

            parsed = json.loads(message)
            level = parsed.get("level", "?")
            msg = parsed.get("message", "?")
            task = parsed.get("task_name") or parsed.get("state_name") or ""
            nova = parsed.get("nova_id", "")
            extra = ""

            # Pull out interesting fields
            for key in [
                "candidate_count",
                "error",
                "cause",
                "status",
                "outcome",
                "error_classification",
                "earliest_publication_date",
                "updated",
                "discovery_date",
            ]:
                if key in parsed:
                    extra += f" {key}={parsed[key]!r}"

            task_str = f" [{task}]" if task else ""
            nova_str = f" nova={nova[:12]}…" if nova else ""
            print(f"{prefix} {level}{task_str}{nova_str} — {msg}{extra}")

        except (json.JSONDecodeError, TypeError):
            print(f"{prefix} {message.strip()[:200]}")

    print(f"\n{'─' * 60}")
    print(f"Displayed: {displayed} events")
    print(f"Error pattern matches: {error_events}")

    if error_events == 0 and not args.errors_only:
        print("\n✓ No error patterns detected in the search window.")
    elif error_events > 0:
        print(f"\n⚠ Found {error_events} events matching error patterns.")
        print("  Re-run with --errors-only for a focused view.")


if __name__ == "__main__":
    main()
