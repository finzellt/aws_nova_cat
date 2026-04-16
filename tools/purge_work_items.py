#!/usr/bin/env python3
"""purge_work_items.py — Remove WorkItems from the WORKQUEUE partition.

Deletes pending WorkItems so the next sweep no longer picks them up.
Primary use case: novae that have been hidden (HIDDEN status) but still
have WorkItems triggering every sweep cycle. The artifact generator
skips non-ACTIVE novae, so these WorkItems are wasted work that persist
until their 30-day TTL expires.

Usage (single nova by name):
    python tools/purge_work_items.py --name "IM Nor" --dry-run
    python tools/purge_work_items.py --name "IM Nor"

Usage (single nova by ID):
    python tools/purge_work_items.py --nova-id 64c5c516-... --dry-run
    python tools/purge_work_items.py --nova-id 64c5c516-...

Usage (all non-ACTIVE novae — the main use case):
    python tools/purge_work_items.py --all-inactive --dry-run
    python tools/purge_work_items.py --all-inactive

Usage (specific dirty types only):
    python tools/purge_work_items.py --name "IM Nor" --dirty spectra

Environment variables:
    NOVACAT_TABLE_NAME  — DynamoDB table name (default: NovaCat)

Operator tooling — no CI requirements (mypy / ruff not enforced).
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3
from boto3.dynamodb.conditions import Key

_WORKQUEUE_PK = "WORKQUEUE"
_ALL_DIRTY_TYPES = ["spectra", "photometry", "references"]

_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_name(table, name):
    """Resolve a nova name to a nova_id via NameMapping."""
    normalized = name.strip().lower().replace("_", " ")
    pk = f"NAME#{normalized}"
    resp = table.query(KeyConditionExpression=Key("PK").eq(pk), Limit=1)
    items = resp.get("Items", [])
    return items[0].get("nova_id") if items else None


def _get_nova_status(table, nova_id):
    """Return the status of a Nova item, or None if not found."""
    resp = table.get_item(
        Key={"PK": nova_id, "SK": "NOVA"},
        ProjectionExpression="#s, primary_name",
        ExpressionAttributeNames={"#s": "status"},
    )
    item = resp.get("Item")
    if not item:
        return None, None
    return item.get("status", "unknown"), item.get("primary_name", "unknown")


def _query_work_items_for_nova(table, nova_id, dirty_types=None):
    """Return all WorkItems for a specific nova, optionally filtered by dirty type."""
    items = []
    kwargs = {
        "KeyConditionExpression": Key("PK").eq(_WORKQUEUE_PK)
        & Key("SK").begins_with(f"{nova_id}#"),
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if resp.get("LastEvaluatedKey") is None:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if dirty_types:
        items = [i for i in items if i.get("dirty_type") in dirty_types]

    return items


def _query_all_work_items(table):
    """Return all WorkItems from the WORKQUEUE partition."""
    items = []
    kwargs = {"KeyConditionExpression": Key("PK").eq(_WORKQUEUE_PK)}
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if resp.get("LastEvaluatedKey") is None:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def _batch_delete(table, items, dry_run=False):
    """Delete a list of WorkItems using BatchWriteItem (25-item chunks)."""
    if not items:
        return 0

    deleted = 0
    # BatchWriteItem accepts max 25 operations per call
    for i in range(0, len(items), 25):
        chunk = items[i : i + 25]
        if dry_run:
            for item in chunk:
                print(f"  {_DIM}[DRY RUN] Would delete: SK={item['SK']}{_RESET}")
            deleted += len(chunk)
        else:
            request_items = {
                table.name: [
                    {"DeleteRequest": {"Key": {"PK": _WORKQUEUE_PK, "SK": item["SK"]}}}
                    for item in chunk
                ]
            }
            # Use the underlying client for BatchWriteItem
            client = table.meta.client
            resp = client.batch_write_item(RequestItems=request_items)
            deleted += len(chunk)

            # Handle unprocessed items (retry once)
            unprocessed = resp.get("UnprocessedItems", {})
            if unprocessed:
                print(
                    f"  {_YELLOW}⚠ Retrying {len(unprocessed.get(table.name, []))} unprocessed items...{_RESET}"
                )
                client.batch_write_item(RequestItems=unprocessed)

            for item in chunk:
                print(f"  {_GREEN}✓{_RESET} Deleted: SK={item['SK']}")

    return deleted


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------


def _purge_for_nova(table, nova_id, dirty_types, dry_run):
    """Purge WorkItems for a single nova."""
    status, name = _get_nova_status(table, nova_id)
    print(f"Nova:   {name or 'unknown'} ({nova_id})")
    print(f"Status: {status or 'NOT FOUND'}")

    items = _query_work_items_for_nova(table, nova_id, dirty_types)
    if not items:
        print(f"\n{_DIM}No matching WorkItems found — nothing to purge.{_RESET}")
        return

    print(f"Found:  {len(items)} WorkItem(s)")
    if dirty_types:
        print(f"Filter: {', '.join(dirty_types)}")
    if dry_run:
        print(f"Mode:   {_YELLOW}DRY RUN{_RESET}")
    print()

    deleted = _batch_delete(table, items, dry_run=dry_run)

    if dry_run:
        print(f"\n{_YELLOW}[DRY RUN]{_RESET} Would delete {deleted} WorkItem(s).")
    else:
        print(f"\n{_GREEN}Deleted {deleted} WorkItem(s).{_RESET}")


def _purge_all_inactive(table, dirty_types, dry_run):
    """Purge WorkItems for all novae that are not ACTIVE."""
    print(f"{_BOLD}Scanning WORKQUEUE for all pending WorkItems...{_RESET}")
    all_items = _query_all_work_items(table)

    if not all_items:
        print(f"\n{_DIM}Work queue is empty — nothing to purge.{_RESET}")
        return

    # Extract unique nova_ids from WorkItem SKs
    nova_ids = set()
    for item in all_items:
        # SK format: <nova_id>#<dirty_type>#<created_at>
        parts = item["SK"].split("#", 1)
        if parts:
            nova_ids.add(parts[0])

    print(f"Found {len(all_items)} WorkItem(s) across {len(nova_ids)} nova(e)")
    print()

    # Check each nova's status
    inactive_nova_ids = set()
    for nova_id in sorted(nova_ids):
        status, name = _get_nova_status(table, nova_id)
        if status != "ACTIVE":
            inactive_nova_ids.add(nova_id)
            print(
                f"  {_RED}✗{_RESET} {name or 'unknown':20s} ({nova_id[:12]}…) — {status or 'NOT FOUND'}"
            )
        else:
            print(
                f"  {_GREEN}✓{_RESET} {name or 'unknown':20s} ({nova_id[:12]}…) — ACTIVE (keeping)"
            )

    if not inactive_nova_ids:
        print(f"\n{_DIM}All novae with WorkItems are ACTIVE — nothing to purge.{_RESET}")
        return

    # Filter to only WorkItems belonging to non-ACTIVE novae
    to_delete = [item for item in all_items if item["SK"].split("#", 1)[0] in inactive_nova_ids]

    if dirty_types:
        to_delete = [item for item in to_delete if item.get("dirty_type") in dirty_types]
        print(f"\nDirty type filter: {', '.join(dirty_types)}")

    print(
        f"\n{_BOLD}Will purge {len(to_delete)} WorkItem(s) for {len(inactive_nova_ids)} inactive nova(e){_RESET}"
    )
    if dry_run:
        print(f"Mode: {_YELLOW}DRY RUN{_RESET}")
    print()

    deleted = _batch_delete(table, to_delete, dry_run=dry_run)

    if dry_run:
        print(
            f"\n{_YELLOW}[DRY RUN]{_RESET} Would delete {deleted} WorkItem(s) for {len(inactive_nova_ids)} inactive nova(e)."
        )
    else:
        print(
            f"\n{_GREEN}Deleted {deleted} WorkItem(s) for {len(inactive_nova_ids)} inactive nova(e).{_RESET}"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Purge WorkItems from the WORKQUEUE partition.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Preview what would be purged for all non-ACTIVE novae
  python tools/purge_work_items.py --all-inactive --dry-run

  # Purge all WorkItems for a hidden nova
  python tools/purge_work_items.py --name "IM Nor"

  # Purge only spectra WorkItems for a nova
  python tools/purge_work_items.py --name "IM Nor" --dirty spectra
""",
    )

    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument("--nova-id", help="Nova UUID")
    id_group.add_argument("--name", help="Nova name (resolved via NameMapping)")
    id_group.add_argument(
        "--all-inactive",
        action="store_true",
        help="Purge WorkItems for ALL non-ACTIVE novae",
    )

    parser.add_argument(
        "--dirty",
        nargs="+",
        choices=_ALL_DIRTY_TYPES,
        help="Only purge specific dirty type(s). Default: all types.",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("NOVACAT_TABLE_NAME", "NovaCat"),
        help="DynamoDB table name (default: $NOVACAT_TABLE_NAME or 'NovaCat')",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted")

    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.table)

    print(f"Table: {args.table}")
    print()

    if args.all_inactive:
        _purge_all_inactive(table, args.dirty, args.dry_run)
    else:
        if args.name:
            nova_id = _resolve_name(table, args.name)
            if not nova_id:
                print(
                    f"{_RED}ERROR:{_RESET} Could not resolve name '{args.name}' to a nova_id.",
                    file=sys.stderr,
                )
                sys.exit(1)
            print(f"Resolved '{args.name}' → {nova_id}")
        else:
            nova_id = args.nova_id

        _purge_for_nova(table, nova_id, args.dirty, args.dry_run)


if __name__ == "__main__":
    main()
