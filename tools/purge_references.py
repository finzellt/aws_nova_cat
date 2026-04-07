#!/usr/bin/env python3
"""purge_references.py — Delete all NovaReference link items for a nova.

Queries the NovaCat DynamoDB table for NOVAREF# items under the given
nova_id partition and deletes them. The global REFERENCE#<bibcode>
items are left untouched (they may be shared across novae).

After purging, re-run refresh_references to rebuild from a fresh ADS
query, then trigger a sweep to regenerate references.json.

Usage:
    python tools/purge_references.py --nova <nova_id>
    python tools/purge_references.py --nova <nova_id> --dry-run
    python tools/purge_references.py --name "V5668 Sgr" --dry-run

Environment variables:
    NOVACAT_TABLE_NAME  — DynamoDB table name (default: NovaCat)
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3
from boto3.dynamodb.conditions import Key

_NOVAREF_PREFIX = "NOVAREF#"
_BATCH_DELETE_LIMIT = 25


def _resolve_name(table_resource, name: str) -> str | None:
    """Resolve a nova name to a nova_id via NameMapping."""
    normalized = name.strip().lower().replace("_", " ")
    pk = f"NAME#{normalized}"
    resp = table_resource.query(
        KeyConditionExpression=Key("PK").eq(pk),
        Limit=1,
    )
    items = resp.get("Items", [])
    if not items:
        return None
    return items[0].get("nova_id")


def _query_novaref_items(table_resource, nova_id: str) -> list[dict]:
    """Query all NOVAREF# items under a nova_id partition."""
    items: list[dict] = []
    kwargs = {
        "KeyConditionExpression": (Key("PK").eq(nova_id) & Key("SK").begins_with(_NOVAREF_PREFIX)),
        "ProjectionExpression": "PK, SK, bibcode",
    }

    while True:
        resp = table_resource.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key

    return items


def _delete_items(table_resource, items: list[dict], dry_run: bool) -> int:
    """Delete NOVAREF items. Returns count deleted."""
    deleted = 0

    for item in items:
        pk = item["PK"]
        sk = item["SK"]
        bibcode = item.get("bibcode", sk.removeprefix(_NOVAREF_PREFIX))

        if dry_run:
            print(f"  [DRY RUN] Would delete: {bibcode}")
        else:
            table_resource.delete_item(Key={"PK": pk, "SK": sk})
            deleted += 1
            print(f"  Deleted: {bibcode}")

    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument("--nova", type=str, help="Nova UUID")
    id_group.add_argument("--name", type=str, help="Nova name (resolved via NameMapping)")

    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted")
    parser.add_argument(
        "--table",
        type=str,
        default=os.environ.get("NOVACAT_TABLE_NAME", "NovaCat"),
        help="DynamoDB table name (default: $NOVACAT_TABLE_NAME or 'NovaCat')",
    )
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.table)

    # Resolve nova_id
    if args.name:
        nova_id = _resolve_name(table, args.name)
        if not nova_id:
            print(f"ERROR: Could not resolve name '{args.name}' to a nova_id.", file=sys.stderr)
            sys.exit(1)
        print(f"Resolved '{args.name}' → {nova_id}")
    else:
        nova_id = args.nova

    print(f"Table:   {args.table}")
    print(f"Nova:    {nova_id}")
    if args.dry_run:
        print("Mode:    DRY RUN")
    print()

    # Query NOVAREF# items
    items = _query_novaref_items(table, nova_id)
    print(f"Found {len(items)} NovaReference item(s).\n")

    if not items:
        print("Nothing to delete.")
        return

    deleted = _delete_items(table, items, dry_run=args.dry_run)

    if args.dry_run:
        print(f"\n[DRY RUN] Would delete {len(items)} NovaReference item(s).")
    else:
        print(f"\nDeleted {deleted} NovaReference item(s).")
    print("\nNext steps:")
    print("  1. Clear idempotency lock:  python tools/clear_idempotency_locks.py --nova <id>")
    print("  2. Re-run refresh_references workflow for this nova")
    print(
        "  3. Reseed work item:        python tools/reseed_work_items.py --nova-id <id> --dirty references"
    )
    print("  4. Trigger sweep:           python tools/sweep.py")


if __name__ == "__main__":
    main()
