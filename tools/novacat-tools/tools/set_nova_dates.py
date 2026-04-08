#!/usr/bin/env python3
"""
set_nova_dates.py — Batch-assign outburst_date and/or discovery_date on Nova DDB items.

Usage (single nova):
    python tools/novacat-tools/tools/set_nova_dates.py --nova "V1674 Her" --outburst-date 2021-06-12
    python tools/novacat-tools/tools/set_nova_dates.py --nova "V1674 Her" --discovery-date 2021-06-12
    python tools/novacat-tools/tools/set_nova_dates.py --nova "V1674 Her" --outburst-date 2021-06-12 --discovery-date 2021-06-00

Usage (batch from CSV):
    python tools/novacat-tools/tools/set_nova_dates.py --csv dates.csv

    CSV format (header required, columns: nova_name, outburst_date, discovery_date):
        nova_name,outburst_date,discovery_date
        V1674 Her,2021-06-12,
        V1324 Sco,,2012-06-01
        RS Oph,2021-08-08,2021-08-09

    Empty cells are skipped (field not touched). Both date columns are optional
    but at least one must be present in the header.

Usage (clear a date):
    python tools/novacat-tools/tools/set_nova_dates.py --nova "V1674 Her" --clear outburst_date

Dry-run mode (no writes):
    python tools/novacat-tools/tools/set_nova_dates.py --csv dates.csv --dry-run

Skip regeneration seeding (just set dates, don't trigger sweep):
    python tools/novacat-tools/tools/set_nova_dates.py --csv dates.csv --no-reseed

By default, each successful date update writes spectra + photometry
WorkItems so the next artifact regeneration sweep picks up the change.

Dates can be YYYY-MM-DD or MM-DD-YYYY (auto-detected). The 00 convention
is allowed for imprecise dates:
    2021-06-00   (day unknown)
    2021-00-00   (month and day unknown)

Operator tooling — no CI requirements (mypy / ruff not enforced).
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import uuid
from datetime import UTC, datetime

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = "NovaCat"
REGION = "us-east-1"

CLEARABLE_FIELDS = {"outburst_date", "discovery_date"}

# WorkItem constants (mirrors nova_common.work_item)
_WORKQUEUE_PK = "WORKQUEUE"
_WORKITEM_TTL_DAYS = 30
# Changing outburst/discovery date affects DPO on spectra and photometry artifacts.
_DATE_CHANGE_DIRTY_TYPES = ["spectra", "photometry"]


dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


# ---------------------------------------------------------------------------
# Name resolution (mirrors novacat_query.py)
# ---------------------------------------------------------------------------


def _normalize_name(name):
    return " ".join(name.lower().split())


def resolve_nova_id(name):
    """Resolve a nova name (primary or alias) to its nova_id via NameMapping."""
    normalized = _normalize_name(name)
    resp = table.query(
        KeyConditionExpression=(
            Key("PK").eq(f"NAME#{normalized}") & Key("SK").begins_with("NOVA#")
        ),
        Limit=1,
    )
    items = resp.get("Items", [])
    if not items:
        return None
    return items[0].get("nova_id")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def normalize_date(date_str):
    """Accept flexible date input, return canonical YYYY-MM-DD or None on failure.

    Accepted formats (hyphens or slashes, with or without zero-padding):
        YYYY-MM-DD, YYYY/MM/DD   (ISO-ish)
        MM-DD-YYYY, MM/DD/YYYY   (US)
        M/D/YYYY, M-D-YYYY       (US, no padding)

    The 00 convention for imprecise dates is preserved in YYYY-MM-DD input.
    Output is always zero-padded YYYY-MM-DD.
    """
    date_str = date_str.strip()
    parts = re.split(r"[/-]", date_str)
    if len(parts) != 3:
        return None

    # Determine which part is the year (4-digit component)
    if len(parts[0]) == 4:
        # YYYY-MM-DD
        year_s, month_s, day_s = parts
    elif len(parts[2]) == 4:
        # MM-DD-YYYY
        month_s, day_s, year_s = parts
    else:
        return None

    # Validate ranges
    try:
        year = int(year_s)
        month = int(month_s)
        day = int(day_s)
    except ValueError:
        return None

    if not (1800 <= year <= 2100):
        return None
    if not (0 <= month <= 12):
        return None
    if not (0 <= day <= 31):
        return None

    return f"{year:04d}-{month:02d}-{day:02d}"


# ---------------------------------------------------------------------------
# WorkItem seeding for regeneration sweep
# ---------------------------------------------------------------------------


def seed_work_items(nova_id, dry_run=False):
    """Write spectra + photometry WorkItems so the next sweep regenerates artifacts.

    Returns count of items written (or would-write in dry-run).
    """
    now = datetime.now(UTC)
    created_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    ttl = int(now.timestamp()) + (_WORKITEM_TTL_DAYS * 86_400)
    correlation_id = f"set-nova-dates-{uuid.uuid4().hex[:12]}"
    count = 0

    for dirty_type in _DATE_CHANGE_DIRTY_TYPES:
        sk = f"{nova_id}#{dirty_type}#{created_at}"
        if dry_run:
            print(f"    [DRY RUN] would seed WorkItem: {dirty_type}")
            count += 1
            continue
        try:
            table.put_item(
                Item={
                    "PK": _WORKQUEUE_PK,
                    "SK": sk,
                    "entity_type": "WorkItem",
                    "schema_version": "1.0.0",
                    "nova_id": nova_id,
                    "dirty_type": dirty_type,
                    "source_workflow": "set_nova_dates",
                    "job_run_id": "00000000-0000-0000-0000-000000000000",
                    "correlation_id": correlation_id,
                    "created_at": created_at,
                    "ttl": ttl,
                }
            )
            count += 1
        except Exception as e:
            print(f"    WARNING: failed to seed {dirty_type} WorkItem: {e}")

    return count


# ---------------------------------------------------------------------------
# Core update
# ---------------------------------------------------------------------------


def update_nova_dates(nova_id, nova_name, outburst_date=None, discovery_date=None, dry_run=False):
    """Update outburst_date and/or discovery_date on a Nova DDB item.

    Returns (success: bool, message: str).
    """
    set_parts = []
    values = {}
    now_iso = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    if outburst_date is not None:
        set_parts.append("outburst_date = :ob")
        values[":ob"] = outburst_date
    if discovery_date is not None:
        set_parts.append("discovery_date = :dd")
        values[":dd"] = discovery_date

    if not set_parts:
        return False, "nothing to update"

    set_parts.append("updated_at = :ua")
    values[":ua"] = now_iso

    update_expr = "SET " + ", ".join(set_parts)

    fields_desc = []
    if outburst_date is not None:
        fields_desc.append(f"outburst_date={outburst_date}")
    if discovery_date is not None:
        fields_desc.append(f"discovery_date={discovery_date}")
    desc = ", ".join(fields_desc)

    if dry_run:
        return True, f"[DRY RUN] would set {desc}"

    try:
        table.update_item(
            Key={"PK": nova_id, "SK": "NOVA"},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=values,
            ConditionExpression="attribute_exists(SK)",  # guard: item must exist
        )
        return True, f"set {desc}"
    except Exception as e:
        return False, f"DDB error: {e}"


def clear_nova_field(nova_id, nova_name, field, dry_run=False):
    """Remove a date field from a Nova DDB item.

    Returns (success: bool, message: str).
    """
    now_iso = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    if dry_run:
        return True, f"[DRY RUN] would clear {field}"

    try:
        table.update_item(
            Key={"PK": nova_id, "SK": "NOVA"},
            UpdateExpression=f"REMOVE {field} SET updated_at = :ua",
            ExpressionAttributeValues={":ua": now_iso},
            ConditionExpression="attribute_exists(SK)",
        )
        return True, f"cleared {field}"
    except Exception as e:
        return False, f"DDB error: {e}"


# ---------------------------------------------------------------------------
# Batch from CSV
# ---------------------------------------------------------------------------


def process_csv(csv_path, dry_run=False, reseed=True):
    """Read a CSV and apply date updates. Returns (successes, failures)."""
    successes = 0
    failures = 0

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)

        # Validate header
        if "nova_name" not in reader.fieldnames:
            print("ERROR: CSV must have a 'nova_name' column.")
            sys.exit(1)

        has_outburst = "outburst_date" in reader.fieldnames
        has_discovery = "discovery_date" in reader.fieldnames
        if not has_outburst and not has_discovery:
            print(
                "ERROR: CSV must have at least one of 'outburst_date' or 'discovery_date' columns."
            )
            sys.exit(1)

        for i, row in enumerate(reader, start=2):  # line 2 = first data row
            nova_name = row["nova_name"].strip()
            if not nova_name:
                print(f"  line {i}: SKIP (empty nova_name)")
                continue

            # Parse dates (empty string → None → skip)
            outburst_raw = row.get("outburst_date", "").strip() or None
            discovery_raw = row.get("discovery_date", "").strip() or None

            # Normalize (accepts YYYY-MM-DD or MM-DD-YYYY)
            errors = []
            outburst = None
            discovery = None
            if outburst_raw:
                outburst = normalize_date(outburst_raw)
                if outburst is None:
                    errors.append(f"invalid outburst_date '{outburst_raw}'")
            if discovery_raw:
                discovery = normalize_date(discovery_raw)
                if discovery is None:
                    errors.append(f"invalid discovery_date '{discovery_raw}'")
            if errors:
                print(f"  line {i}: FAIL {nova_name} — {'; '.join(errors)}")
                failures += 1
                continue

            if not outburst and not discovery:
                print(f"  line {i}: SKIP {nova_name} (no dates provided)")
                continue

            # Resolve
            nova_id = resolve_nova_id(nova_name)
            if not nova_id:
                print(f"  line {i}: FAIL {nova_name} — not found in DDB")
                failures += 1
                continue

            ok, msg = update_nova_dates(nova_id, nova_name, outburst, discovery, dry_run)
            status = "OK" if ok else "FAIL"
            print(f"  line {i}: {status} {nova_name} ({nova_id[:8]}…) — {msg}")
            if ok:
                successes += 1
                if reseed:
                    seed_work_items(nova_id, dry_run)
            else:
                failures += 1

    return successes, failures


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Set outburst_date / discovery_date on Nova DDB items.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools/novacat-tools/tools/set_nova_dates.py --nova "V1674 Her" --outburst-date 2021-06-12
  python tools/novacat-tools/tools/set_nova_dates.py --nova "V1674 Her" --outburst-date 06-12-2021
  python tools/novacat-tools/tools/set_nova_dates.py --csv dates.csv --dry-run
  python tools/novacat-tools/tools/set_nova_dates.py --nova "RS Oph" --clear outburst_date
        """,
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--nova", type=str, help="Single nova name (primary or alias)")
    mode.add_argument("--csv", type=str, metavar="FILE", help="CSV file for batch updates")

    parser.add_argument("--outburst-date", type=str, help="YYYY-MM-DD outburst date")
    parser.add_argument("--discovery-date", type=str, help="YYYY-MM-DD discovery date")
    parser.add_argument(
        "--clear",
        type=str,
        choices=sorted(CLEARABLE_FIELDS),
        help="Remove a date field from the Nova item",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print what would be done, no writes"
    )
    parser.add_argument(
        "--no-reseed",
        action="store_true",
        help="Skip WorkItem creation (set dates without triggering regeneration)",
    )

    args = parser.parse_args()

    # ── Single-nova mode ──────────────────────────────────────────────────
    if args.nova:
        if args.clear:
            nova_id = resolve_nova_id(args.nova)
            if not nova_id:
                print(f"ERROR: Nova not found: {args.nova!r}")
                sys.exit(1)
            ok, msg = clear_nova_field(nova_id, args.nova, args.clear, args.dry_run)
            print(f"{'OK' if ok else 'FAIL'}: {args.nova} ({nova_id[:8]}…) — {msg}")
            if ok and not args.no_reseed:
                n = seed_work_items(nova_id, args.dry_run)
                print(f"  Seeded {n} WorkItem(s) for regeneration")
            sys.exit(0 if ok else 1)

        outburst = args.outburst_date
        discovery = args.discovery_date

        if not outburst and not discovery:
            parser.error("--outburst-date and/or --discovery-date required (or --clear)")

        # Normalize (accepts YYYY-MM-DD or MM-DD-YYYY)
        if outburst:
            outburst = normalize_date(outburst)
            if outburst is None:
                print(f"ERROR: Invalid outburst date: {args.outburst_date!r}")
                sys.exit(1)
        if discovery:
            discovery = normalize_date(discovery)
            if discovery is None:
                print(f"ERROR: Invalid discovery date: {args.discovery_date!r}")
                sys.exit(1)

        nova_id = resolve_nova_id(args.nova)
        if not nova_id:
            print(f"ERROR: Nova not found: {args.nova!r}")
            sys.exit(1)

        ok, msg = update_nova_dates(nova_id, args.nova, outburst, discovery, args.dry_run)
        print(f"{'OK' if ok else 'FAIL'}: {args.nova} ({nova_id[:8]}…) — {msg}")
        if ok and not args.no_reseed:
            n = seed_work_items(nova_id, args.dry_run)
            print(f"  Seeded {n} WorkItem(s) for regeneration")
        sys.exit(0 if ok else 1)

    # ── Batch CSV mode ────────────────────────────────────────────────────
    if args.csv:
        if args.outburst_date or args.discovery_date or args.clear:
            parser.error("--outburst-date, --discovery-date, and --clear are not used with --csv")

        print(f"Processing: {args.csv}")
        if args.dry_run:
            print("  (dry-run mode — no writes)")
        successes, failures = process_csv(args.csv, args.dry_run, reseed=not args.no_reseed)
        print(f"\nDone: {successes} succeeded, {failures} failed.")
        sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
