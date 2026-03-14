"""
novacat_logs.py — CloudWatch Logs Insights interface for NovaCat.

Two query modes:
  1. trace(correlation_id)  — full execution chain trace for one run,
                               across all Lambda log groups, in time order.
  2. recent(minutes)        — everything logged in the last N minutes,
                               useful for watching active ingestions.

Usage (notebook):
    from novacat_logs import NovaCatLogs
    logs = NovaCatLogs()

    # Trace a full execution chain by correlation_id
    df = logs.trace("a1b2c3d4-9e8f-7a6b-5c4d-3e2f1a0b9c8d")

    # See what's been running in the last 10 minutes
    df = logs.recent(minutes=10)

    # Filter to a specific workflow
    df = logs.recent(minutes=30, workflow="refresh_references")

Usage (CLI):
    python novacat_logs.py --trace <correlation_id>
    python novacat_logs.py --recent 10
    python novacat_logs.py --recent 30 --workflow refresh_references
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime, timedelta

import boto3
import pandas as pd

# ── Config ─────────────────────────────────────────────────────────────────────
REGION = "us-east-1"

# All NovaCat Lambda log group names follow this prefix.
# Add or remove entries here if the Lambda inventory changes.
LOG_GROUP_PREFIX = "/aws/lambda/nova-cat-"

# Known Lambda service names — determines which log groups are searched.
# Keeping this explicit avoids surprises from unrelated log groups.
LAMBDA_SERVICES = [
    "nova-resolver",
    "archive-resolver",
    "idempotency-guard",
    "quarantine-handler",
    "workflow-launcher",
    "reference-manager",
    "spectra-discoverer",
    "spectra-acquirer",
    "spectra-validator",
]

LOG_GROUPS = [f"{LOG_GROUP_PREFIX}{svc}" for svc in LAMBDA_SERVICES]

# How long to wait between Logs Insights polling intervals (seconds)
POLL_INTERVAL = 2
# Maximum time to wait for a query to complete (seconds)
QUERY_TIMEOUT = 120


class NovaCatLogs:
    """
    CloudWatch Logs Insights interface for NovaCat.

    All methods return a pandas DataFrame of log events, sorted by timestamp.
    Each row represents one structured log entry from a Lambda invocation.
    """

    def __init__(self, region: str = REGION, log_groups: list[str] = LOG_GROUPS):
        self.client = boto3.client("logs", region_name=region)
        self.log_groups = log_groups

    # ── Primary query modes ────────────────────────────────────────────────────

    def trace(
        self,
        correlation_id: str,
        lookback_hours: int = 24,
    ) -> pd.DataFrame:
        """
        Trace a full NovaCat execution chain by correlation_id.

        Searches all Lambda log groups for structured log events containing
        the given correlation_id. Returns events in chronological order,
        spanning all workflows in the chain (initialize_nova → ingest_new_nova
        → refresh_references, discover_spectra_products → acquire_and_validate).

        Args:
            correlation_id: The UUID printed by nova-ingest.sh at launch time.
            lookback_hours: How far back to search (default 24h). Increase for
                            older executions.

        Returns:
            DataFrame with columns: timestamp, workflow_name, state_name,
            level, message, nova_id, job_run_id, attempt_number,
            error_classification, log_group, log_stream
        """
        query = f"""
fields @timestamp, @message, @logStream, @log
| filter correlation_id = "{correlation_id}"
| sort @timestamp asc
| limit 1000
"""
        end = datetime.now(UTC)
        start = end - timedelta(hours=lookback_hours)

        print(f"Tracing correlation_id: {correlation_id}")
        print(
            f"Search window: last {lookback_hours}h ({start.strftime('%Y-%m-%d %H:%M')} UTC → now)"
        )
        print(f"Log groups: {len(self.log_groups)}")

        return self._run_query(query, start, end)

    def recent(
        self,
        minutes: int = 10,
        workflow: str | None = None,
        level: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch recent log events across all NovaCat Lambdas.

        Useful for watching active ingestions or diagnosing recent failures.

        Args:
            minutes:  How many minutes back to look (default 10).
            workflow: Optional filter to a specific workflow_name
                      (e.g. "refresh_references", "acquire_and_validate_spectra").
            level:    Optional filter to a log level (e.g. "ERROR", "WARNING").

        Returns:
            DataFrame with same columns as trace().
        """
        filter_clauses = []
        if workflow:
            filter_clauses.append(f'| filter workflow_name = "{workflow}"')
        if level:
            filter_clauses.append(f'| filter level = "{level}"')

        filter_str = "\n".join(filter_clauses)

        query = f"""
fields @timestamp, @message, @logStream, @log
{filter_str}
| sort @timestamp desc
| limit 500
"""
        end = datetime.now(UTC)
        start = end - timedelta(minutes=minutes)

        label = f"last {minutes} minutes"
        if workflow:
            label += f" | workflow={workflow}"
        if level:
            label += f" | level={level}"
        print(f"Fetching recent logs: {label}")

        df = self._run_query(query, start, end)
        # Return chronological for consistency even though we queried desc
        if not df.empty and "timestamp" in df.columns:
            df = df.sort_values("timestamp", ascending=True).reset_index(drop=True)
        return df

    def errors(self, hours: int = 1) -> pd.DataFrame:
        """
        Convenience: fetch all ERROR-level events in the last N hours.
        Useful as a quick health check after an ingestion run.
        """
        query = """
fields @timestamp, @message, @logStream, @log
| filter level = "ERROR" or level = "CRITICAL"
| sort @timestamp desc
| limit 200
"""
        end = datetime.now(UTC)
        start = end - timedelta(hours=hours)

        print(f"Fetching errors: last {hours}h")
        df = self._run_query(query, start, end)
        if not df.empty and "timestamp" in df.columns:
            df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
        return df

    def job_runs(self, hours: int = 24) -> pd.DataFrame:
        """
        List all JobRun-level log events in the last N hours.
        Gives a quick view of which workflows ran and their outcomes.
        Filters to log entries that contain a job_run_id field.
        """
        query = """
fields @timestamp, @message, @logStream
| filter ispresent(job_run_id) and ispresent(workflow_name)
| sort @timestamp desc
| limit 200
"""
        end = datetime.now(UTC)
        start = end - timedelta(hours=hours)

        print(f"Fetching job run events: last {hours}h")
        df = self._run_query(query, start, end)
        if not df.empty and "timestamp" in df.columns:
            df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
        return df

    # ── Internal: Logs Insights execution ─────────────────────────────────────

    def _run_query(
        self,
        query_string: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """
        Execute a Logs Insights query, poll until complete, return a DataFrame.

        CloudWatch Logs Insights is asynchronous: we start a query, poll until
        it completes, then parse the results.
        """
        # Filter to log groups that actually exist (avoids hard errors on
        # log groups that haven't received any traffic yet)
        existing_groups = self._existing_log_groups()
        if not existing_groups:
            print("WARNING: No NovaCat Lambda log groups found. Have any Lambdas been invoked yet?")
            return pd.DataFrame()

        # Start the query
        resp = self.client.start_query(
            logGroupNames=existing_groups,
            startTime=int(start.timestamp()),
            endTime=int(end.timestamp()),
            queryString=query_string,
        )
        query_id = resp["queryId"]

        # Poll until complete
        elapsed = 0
        while elapsed < QUERY_TIMEOUT:
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            status_resp = self.client.get_query_results(queryId=query_id)
            status = status_resp["status"]

            if status == "Complete":
                results = status_resp.get("results", [])
                stats = status_resp.get("statistics", {})
                print(
                    f"Query complete — {len(results)} events "
                    f"(scanned {stats.get('recordsScanned', '?'):.0f} records "
                    f"in {elapsed}s)"
                )
                return self._parse_results(results)

            elif status in ("Failed", "Cancelled", "Timeout"):
                print(f"Query ended with status: {status}")
                return pd.DataFrame()

            else:
                # Still Running or Scheduled
                print(f"  ... {status} ({elapsed}s elapsed)")

        print(f"Query timed out after {QUERY_TIMEOUT}s")
        return pd.DataFrame()

    def _parse_results(self, results: list[list[dict]]) -> pd.DataFrame:
        """
        Parse raw Logs Insights results into a clean DataFrame.

        Each result is a list of {field, value} dicts. We parse the @message
        field as JSON (NovaCat uses structured logging) and flatten it alongside
        the Logs Insights metadata fields.
        """
        import json

        rows = []
        for result in results:
            # Build a flat dict from Logs Insights field/value pairs
            raw: dict = {r["field"]: r["value"] for r in result}

            # Parse @message as JSON if possible (structured log line)
            message_raw = raw.get("@message", "")
            try:
                msg = json.loads(message_raw)
            except (json.JSONDecodeError, TypeError):
                msg = {"message": message_raw}

            # Merge: Logs Insights metadata + structured log fields
            # Structured log fields win on conflict
            row = {
                "timestamp": raw.get("@timestamp"),
                "log_group": _short_log_group(raw.get("@log", "")),
                "log_stream": raw.get("@logStream"),
                # Key structured log fields (present when available)
                "workflow_name": msg.get("workflow_name"),
                "state_name": msg.get("state_name"),
                "level": msg.get("level") or msg.get("levelname"),
                "message": msg.get("message") or msg.get("msg"),
                "nova_id": msg.get("nova_id"),
                "job_run_id": msg.get("job_run_id"),
                "correlation_id": msg.get("correlation_id"),
                "attempt_number": msg.get("attempt_number"),
                "error_classification": msg.get("error_classification"),
                "error_fingerprint": msg.get("error_fingerprint"),
                # Keep raw message for debugging
                "_raw": message_raw,
            }
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Parse timestamp string to datetime
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

        return df

    def _existing_log_groups(self) -> list[str]:
        """Return the subset of LOG_GROUPS that exist in CloudWatch."""
        existing = []
        for group in self.log_groups:
            try:
                resp = self.client.describe_log_groups(logGroupNamePrefix=group, limit=1)
                if resp.get("logGroups"):
                    existing.append(group)
            except Exception:
                pass
        return existing


# ── Helpers ────────────────────────────────────────────────────────────────────


def _short_log_group(log_field: str) -> str:
    """
    CloudWatch Logs Insights returns @log as '<account_id>:<log_group_name>'.
    Strip the account prefix for cleaner display.
    """
    if ":" in log_field:
        return log_field.split(":", 1)[1]
    return log_field


# ── CLI ────────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NovaCat CloudWatch Logs Insights interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python novacat_logs.py --trace a1b2c3d4-9e8f-7a6b-5c4d-3e2f1a0b9c8d
  python novacat_logs.py --recent 10
  python novacat_logs.py --recent 30 --workflow refresh_references
  python novacat_logs.py --errors 1
  python novacat_logs.py --jobs 24
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--trace", metavar="CORRELATION_ID", help="Trace full execution chain by correlation_id"
    )
    group.add_argument(
        "--recent", metavar="MINUTES", type=int, help="Show recent logs across all Lambdas"
    )
    group.add_argument(
        "--errors", metavar="HOURS", type=int, help="Show ERROR-level events in the last N hours"
    )
    group.add_argument(
        "--jobs", metavar="HOURS", type=int, help="Show job run events in the last N hours"
    )

    parser.add_argument("--workflow", type=str, help="Filter --recent to a specific workflow_name")
    parser.add_argument(
        "--lookback", type=int, default=24, help="Hours to look back for --trace (default 24)"
    )

    args = parser.parse_args()
    logs = NovaCatLogs()

    if args.trace:
        df = logs.trace(args.trace, lookback_hours=args.lookback)
    elif args.recent:
        df = logs.recent(minutes=args.recent, workflow=args.workflow)
    elif args.errors:
        df = logs.errors(hours=args.errors)
    elif args.jobs:
        df = logs.job_runs(hours=args.jobs)

    if df.empty:
        print("No results.")
        return

    # CLI display: drop _raw for readability
    display_cols = [c for c in df.columns if c != "_raw"]
    with pd.option_context(
        "display.max_columns", None, "display.width", 200, "display.max_colwidth", 80
    ):
        print(df[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
