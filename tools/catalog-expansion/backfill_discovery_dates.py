#!/usr/bin/env python3
"""
backfill_discovery_dates.py — Match catalog novae against a candidate CSV
and update discovery_date on Nova DDB items.

Pulls all ACTIVE Nova items from DDB, cross-references them against the
final CSV from the nova candidate processor notebook, and updates
discovery_date where a match is found and the date is missing or different.

Matching strategy:
    For each DDB nova, build a set of all known names (primary_name +
    aliases, all lowercased). For each CSV row, build a set of all known
    names (Nova_Name + SIMBAD_Name + Nova_Aliases, all lowercased).
    A match is any intersection between the two sets.

Usage:
    # Dry-run (default) — show what would change
    python tools/catalog-expansion/backfill_discovery_dates.py --csv tools/catalog-expansion/nova_discovery_and_type_list.csv

    # Execute — push changes to DDB
    python tools/catalog-expansion/backfill_discovery_dates.py --csv nova_discovery_and_type_list.csv --execute

    # Skip WorkItem seeding (just set dates, don't trigger sweep)
    python tools/catalog-expansion/backfill_discovery_dates.py --csv nova_discovery_and_type_list.csv --execute --no-reseed

Operator tooling — no CI requirements (mypy / ruff not enforced).
"""

from __future__ import annotations

import argparse
import csv
import uuid
from datetime import UTC, datetime

import boto3
from boto3.dynamodb.conditions import Attr

TABLE_NAME = "NovaCat"
REGION = "us-east-1"

# WorkItem constants (mirrors set_nova_dates.py)
_WORKQUEUE_PK = "WORKQUEUE"
_WORKITEM_TTL_DAYS = 30
_DATE_CHANGE_DIRTY_TYPES = ["spectra", "photometry"]


# ---------------------------------------------------------------------------
# DDB helpers
# ---------------------------------------------------------------------------


def scan_active_novae(table) -> list[dict]:
    """Scan all ACTIVE Nova items from DDB."""
    items = []
    kwargs = {
        "FilterExpression": Attr("status").eq("ACTIVE") & Attr("SK").eq("NOVA"),
        "ProjectionExpression": "PK, nova_id, primary_name, aliases, discovery_date",
    }
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key

    return items


def normalize(name: str) -> str:
    """Lowercase, collapse whitespace."""
    return " ".join(name.strip().lower().split())


def build_ddb_name_index(novae: list[dict]) -> dict[str, dict]:
    """Build a lookup: normalized_name -> nova DDB item.

    Includes primary_name and all aliases.
    """
    index: dict[str, dict] = {}
    for nova in novae:
        names = [nova.get("primary_name", "")]
        names.extend(nova.get("aliases", []))
        for name in names:
            if name:
                key = normalize(name)
                # First writer wins — primary_name is added first
                if key not in index:
                    index[key] = nova
    return index


def build_csv_name_set(row: dict) -> set[str]:
    """Build a set of all normalized names for a CSV row."""
    names: set[str] = set()

    for col in ("Nova_Name", "Input_Name", "SIMBAD_Name"):
        val = row.get(col, "").strip()
        if val:
            names.add(normalize(val))

    aliases_raw = row.get("Nova_Aliases", "").strip()
    if aliases_raw:
        for alias in aliases_raw.split("|"):
            alias = alias.strip()
            if alias:
                names.add(normalize(alias))

    return names


# ---------------------------------------------------------------------------
# DDB update
# ---------------------------------------------------------------------------


def update_discovery_date(table, nova_id: str, discovery_date: str, dry_run: bool) -> bool:
    """Set discovery_date on a Nova item. Returns True on success."""
    now_iso = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    if dry_run:
        return True

    try:
        table.update_item(
            Key={"PK": nova_id, "SK": "NOVA"},
            UpdateExpression="SET discovery_date = :dd, updated_at = :ua",
            ExpressionAttributeValues={":dd": discovery_date, ":ua": now_iso},
            ConditionExpression="attribute_exists(SK)",
        )
        return True
    except Exception as e:
        print(f"    ERROR: DDB update failed: {e}")
        return False


def seed_work_items(table, nova_id: str, dry_run: bool) -> int:
    """Write spectra + photometry WorkItems for regeneration sweep."""
    now = datetime.now(UTC)
    created_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    ttl = int(now.timestamp()) + (_WORKITEM_TTL_DAYS * 86_400)
    correlation_id = f"backfill-dates-{uuid.uuid4().hex[:12]}"
    count = 0

    for dirty_type in _DATE_CHANGE_DIRTY_TYPES:
        sk = f"{nova_id}#{dirty_type}#{created_at}"
        if dry_run:
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
                    "source_workflow": "backfill_discovery_dates",
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
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--csv", required=True, help="Path to the final candidate CSV")
    parser.add_argument(
        "--execute", action="store_true", help="Actually write to DDB (default: dry-run)"
    )
    parser.add_argument("--no-reseed", action="store_true", help="Skip WorkItem seeding")
    parser.add_argument(
        "--table", default=TABLE_NAME, help=f"DDB table name (default: {TABLE_NAME})"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing discovery_date values (default: only set if missing)",
    )
    args = parser.parse_args()

    dry_run = not args.execute

    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(args.table)

    # ── Step 1: Scan DDB ──────────────────────────────────────────────
    print(f"Table:  {args.table}")
    print(f"Mode:   {'DRY RUN' if dry_run else 'EXECUTE'}")
    print()

    print("Scanning ACTIVE novae from DDB...")
    ddb_novae = scan_active_novae(table)
    print(f"  Found {len(ddb_novae)} ACTIVE novae\n")

    name_index = build_ddb_name_index(ddb_novae)

    # ── Step 2: Read CSV ──────────────────────────────────────────────
    print(f"Reading CSV: {args.csv}")
    csv_rows = []
    with open(args.csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_rows.append(row)
    print(f"  {len(csv_rows)} rows\n")

    # ── Step 3: Match and plan updates ────────────────────────────────
    matched = 0
    unmatched_csv = []
    updates = []  # (nova_id, primary_name, current_date, new_date)
    skipped_same = 0
    skipped_exists = 0

    for row in csv_rows:
        csv_names = build_csv_name_set(row)
        csv_date = row.get("Discovery_Date", "").strip()

        if not csv_date:
            continue

        # Find a match in the DDB name index
        match = None
        for name in csv_names:
            if name in name_index:
                match = name_index[name]
                break

        if match is None:
            unmatched_csv.append(row.get("Nova_Name", row.get("Input_Name", "???")))
            continue

        matched += 1
        nova_id = match["nova_id"]
        primary_name = match.get("primary_name", "???")
        current_date = match.get("discovery_date")

        # Skip if date already matches
        if current_date == csv_date:
            skipped_same += 1
            continue

        # Skip if date exists and --force not set
        if current_date and not args.force:
            skipped_exists += 1
            continue

        updates.append((nova_id, primary_name, current_date, csv_date))

    # ── Step 4: Report ────────────────────────────────────────────────
    print("=" * 65)
    print("Match summary")
    print("=" * 65)
    print(
        f"  CSV rows with dates:   {sum(1 for r in csv_rows if r.get('Discovery_Date', '').strip())}"
    )
    print(f"  Matched to DDB novae:  {matched}")
    print(f"  Already correct:       {skipped_same}")
    print(f"  Has date (use --force): {skipped_exists}")
    print(f"  To update:             {len(updates)}")
    print(f"  Unmatched CSV rows:    {len(unmatched_csv)}")
    print()

    if unmatched_csv:
        print("Unmatched CSV entries (not in catalog):")
        for name in sorted(unmatched_csv):
            print(f"    {name}")
        print()

    if not updates:
        print("Nothing to update.")
        return

    print("Planned updates:")
    for _nova_id, name, old, new in updates:
        old_str = old or "(none)"
        tag = "DRY RUN" if dry_run else "UPDATE"
        print(f"  [{tag}] {name:25s}  {old_str:>12s} → {new}")
    print()

    # ── Step 5: Apply ─────────────────────────────────────────────────
    successes = 0
    failures = 0

    for nova_id, name, _old, new in updates:
        ok = update_discovery_date(table, nova_id, new, dry_run)
        if ok:
            successes += 1
            if not args.no_reseed:
                n = seed_work_items(table, nova_id, dry_run)
                if not dry_run:
                    print(f"  {name}: seeded {n} WorkItem(s)")
        else:
            failures += 1

    # ── Step 6: Summary ───────────────────────────────────────────────
    print()
    print("=" * 65)
    if dry_run:
        print(f"DRY RUN complete. Would update {successes} novae.")
        print("\nRe-run with --execute to apply changes.")
    else:
        print(f"Done. Updated {successes}, failed {failures}.")
        if not args.no_reseed:
            print("WorkItems seeded for regeneration sweep.")


if __name__ == "__main__":
    main()
