#!/usr/bin/env python3
"""Prune stale CloudWatch log groups.

Scans for log groups matching a prefix, reports last ingestion time,
and optionally deletes groups with no recent activity.

Dry-run by default — pass --delete to actually remove groups.

Usage:
    # List all NovaCat log groups with staleness info
    python tools/prune_log_groups.py

    # Custom prefix and staleness threshold
    python tools/prune_log_groups.py --prefix /aws/lambda/NovaCat --days 30

    # Actually delete stale groups (after reviewing dry-run output)
    python tools/prune_log_groups.py --delete

    # Protect specific log groups from deletion
    python tools/prune_log_groups.py --delete --keep artifact-generator --keep regenerate-artifacts
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta

import boto3


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)


def _format_age(dt: datetime) -> str:
    delta = datetime.now(UTC) - dt
    if delta.days > 365:
        years = delta.days // 365
        return f"{years}y ago"
    if delta.days > 30:
        months = delta.days // 30
        return f"{months}mo ago"
    if delta.days > 0:
        return f"{delta.days}d ago"
    hours = delta.seconds // 3600
    if hours > 0:
        return f"{hours}h ago"
    return "just now"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find and prune stale CloudWatch log groups.",
    )
    parser.add_argument(
        "--prefix",
        default="NovaCat",
        help="Log group name prefix to scan (default: NovaCat)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Groups with no ingestion in this many days are considered stale (default: 7)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete stale groups (default: dry-run only)",
    )
    parser.add_argument(
        "--keep",
        action="append",
        default=[],
        help="Substring match — protect log groups whose name contains this string. "
        "Can be specified multiple times.",
    )
    args = parser.parse_args()

    client = boto3.client("logs")
    cutoff = datetime.now(UTC) - timedelta(days=args.days)

    # -- Collect log groups matching prefix --
    log_groups: list[dict] = []
    paginator = client.get_paginator("describe_log_groups")
    for page in paginator.paginate(logGroupNamePrefix=args.prefix):
        log_groups.extend(page.get("logGroups", []))

    if not log_groups:
        print(f"No log groups found with prefix '{args.prefix}'.")
        return

    # -- Classify each group --
    active: list[tuple[str, str]] = []
    stale: list[tuple[str, str]] = []
    never_ingested: list[str] = []
    protected: list[tuple[str, str]] = []

    for lg in sorted(log_groups, key=lambda g: g["logGroupName"]):
        name = lg["logGroupName"]

        # Check last ingestion
        last_ingestion_ms = lg.get("lastIngestionTime")

        if last_ingestion_ms is None:
            # Check if --keep applies
            if any(k in name for k in args.keep):
                protected.append((name, "never ingested"))
            else:
                never_ingested.append(name)
            continue

        last_dt = _ms_to_dt(last_ingestion_ms)
        age_str = _format_age(last_dt)

        if any(k in name for k in args.keep):
            protected.append((name, age_str))
        elif last_dt < cutoff:
            stale.append((name, age_str))
        else:
            active.append((name, age_str))

    # -- Report --
    print(f"Log groups with prefix '{args.prefix}': {len(log_groups)} total\n")

    if active:
        print(f"  ACTIVE ({len(active)}):")
        for name, age in active:
            print(f"    ✓ {name}  (last ingestion: {age})")
        print()

    if protected:
        print(f"  PROTECTED via --keep ({len(protected)}):")
        for name, age in protected:
            print(f"    🛡 {name}  (last ingestion: {age})")
        print()

    if never_ingested:
        print(f"  NEVER INGESTED ({len(never_ingested)}):")
        for name in never_ingested:
            print(f"    ⊘ {name}")
        print()

    if stale:
        print(f"  STALE — no ingestion in {args.days}+ days ({len(stale)}):")
        for name, age in stale:
            print(f"    ✗ {name}  (last ingestion: {age})")
        print()

    # -- Candidates for deletion: stale + never-ingested --
    to_delete = [name for name, _ in stale] + never_ingested

    if not to_delete:
        print("Nothing to clean up.")
        return

    print(f"Candidates for deletion: {len(to_delete)}")

    if not args.delete:
        print("\n  *** DRY RUN — pass --delete to actually remove these groups ***")
        return

    # -- Delete --
    print()
    failed = 0
    for name in to_delete:
        try:
            client.delete_log_group(logGroupName=name)
            print(f"  Deleted: {name}")
        except client.exceptions.ResourceNotFoundException:
            print(f"  Already gone: {name}")
        except Exception as e:
            print(f"  FAILED: {name} — {e}")
            failed += 1

    deleted = len(to_delete) - failed
    print(f"\nDone. Deleted {deleted}/{len(to_delete)} log groups.")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
