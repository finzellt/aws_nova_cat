#!/usr/bin/env python3
"""Clear idempotency locks for a nova (or all novae).

Scans the NovaCat DynamoDB table for IDEMPOTENCY# items matching the
given nova_id (or normalized name), and deletes them.

Usage:
  python tools/clear_idempotency_locks.py --nova <nova_id>
  python tools/clear_idempotency_locks.py --nova <nova_id> --dry-run
  python tools/clear_idempotency_locks.py --all --dry-run
  python tools/clear_idempotency_locks.py --name "v1324 sco"

Environment variables:
  NOVACAT_TABLE_NAME  — DynamoDB table name (set by deploy.sh)
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3


def scan_idempotency_locks(
    table_name: str,
    nova_id: str | None = None,
    name: str | None = None,
) -> list[dict]:
    """Scan for IDEMPOTENCY# items, optionally filtered by nova_id or name."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    all_locks: list[dict] = []
    kwargs: dict = {
        "FilterExpression": boto3.dynamodb.conditions.Attr("PK").begins_with("IDEMPOTENCY#"),
    }

    while True:
        response = table.scan(**kwargs)
        all_locks.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key

    # Filter by nova_id or name if provided
    if nova_id:
        all_locks = [
            lock
            for lock in all_locks
            if nova_id in lock.get("primary_id", "") or nova_id in lock.get("PK", "")
        ]
    elif name:
        normalized = name.lower().replace("_", " ").strip()
        all_locks = [lock for lock in all_locks if normalized in lock.get("primary_id", "").lower()]

    return all_locks


def delete_locks(table_name: str, locks: list[dict], dry_run: bool) -> int:
    """Delete the given lock items. Returns count deleted."""
    if not locks:
        return 0

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    deleted = 0

    for lock in locks:
        pk = lock["PK"]
        sk = lock["SK"]
        workflow = lock.get("workflow_name", "?")
        primary_id = lock.get("primary_id", "?")
        acquired = lock.get("acquired_at", "?")

        if dry_run:
            print(f"  [DRY RUN] Would delete: {workflow} | {primary_id} | acquired {acquired}")
        else:
            table.delete_item(Key={"PK": pk, "SK": sk})
            print(f"  Deleted: {workflow} | {primary_id} | acquired {acquired}")
            deleted += 1

    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--nova", type=str, help="Nova UUID to clear locks for")
    group.add_argument("--name", type=str, help="Normalized nova name (e.g. 'v1324 sco')")
    group.add_argument("--all", action="store_true", help="Clear ALL idempotency locks")

    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted")
    parser.add_argument(
        "--table",
        type=str,
        default=os.environ.get("NOVACAT_TABLE_NAME"),
        help="DynamoDB table name (default: $NOVACAT_TABLE_NAME)",
    )
    args = parser.parse_args()

    if not args.table:
        print("ERROR: --table or NOVACAT_TABLE_NAME env var required.", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning for idempotency locks (table={args.table})...")

    if args.all:
        locks = scan_idempotency_locks(args.table)
    elif args.nova:
        locks = scan_idempotency_locks(args.table, nova_id=args.nova)
    else:
        locks = scan_idempotency_locks(args.table, name=args.name)

    print(f"Found {len(locks)} lock(s).\n")

    if not locks:
        print("Nothing to delete.")
        return

    deleted = delete_locks(args.table, locks, dry_run=args.dry_run)

    if args.dry_run:
        print(f"\n[DRY RUN] Would delete {len(locks)} lock(s).")
    else:
        print(f"\nDeleted {deleted} lock(s).")


if __name__ == "__main__":
    main()
