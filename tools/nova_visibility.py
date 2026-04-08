#!/usr/bin/env python3
"""nova_visibility.py — Show or hide a nova from the published catalog.

Sets the Nova item's `status` field to ACTIVE (visible) or HIDDEN
(excluded from artifact generation and catalog). All DDB data is
preserved — hiding a nova is non-destructive and reversible.

The artifact generator skips non-ACTIVE novae, so a hidden nova drops
out of the next sweep's catalog.json and its artifacts are not copied
forward to new releases.

Usage:
    python tools/nova_visibility.py hide --nova <nova_id>
    python tools/nova_visibility.py hide --name "IM Nor"
    python tools/nova_visibility.py show --name "IM Nor"
    python tools/nova_visibility.py status --name "IM Nor"

    # With dry-run
    python tools/nova_visibility.py hide --name "IM Nor" --dry-run

Environment variables:
    NOVACAT_TABLE_NAME  — DynamoDB table name (default: NovaCat)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

import boto3
from boto3.dynamodb.conditions import Key

_STATUS_ACTIVE = "ACTIVE"
_STATUS_HIDDEN = "HIDDEN"


def _resolve_name(table, name: str) -> str | None:
    normalized = name.strip().lower().replace("_", " ")
    pk = f"NAME#{normalized}"
    resp = table.query(KeyConditionExpression=Key("PK").eq(pk), Limit=1)
    items = resp.get("Items", [])
    return items[0].get("nova_id") if items else None


def _get_nova(table, nova_id: str) -> dict | None:
    resp = table.get_item(Key={"PK": nova_id, "SK": "NOVA"})
    return resp.get("Item")


def _set_status(table, nova_id: str, new_status: str, dry_run: bool) -> dict:
    """Update the Nova item's status. Returns the Nova item."""
    nova = _get_nova(table, nova_id)
    if not nova:
        print(f"ERROR: Nova item not found for {nova_id}", file=sys.stderr)
        sys.exit(1)

    current_status = nova.get("status", "unknown")
    primary_name = nova.get("primary_name", "unknown")

    print(f"Nova:    {primary_name} ({nova_id})")
    print(f"Current: {current_status}")
    print(f"Target:  {new_status}")

    if current_status == new_status:
        print(f"\nAlready {new_status} — nothing to do.")
        return nova

    if dry_run:
        print(f"\n[DRY RUN] Would change status: {current_status} → {new_status}")
        return nova

    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    table.update_item(
        Key={"PK": nova_id, "SK": "NOVA"},
        UpdateExpression="SET #s = :s, updated_at = :now",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": new_status, ":now": now},
    )

    print(f"\nStatus changed: {current_status} → {new_status}")

    if new_status == _STATUS_HIDDEN:
        print("\nNext steps:")
        print("  1. Trigger a sweep to remove from catalog (or wait for scheduled sweep)")
        print("  2. The nova will drop out of catalog.json and no artifacts will be copied forward")
    elif new_status == _STATUS_ACTIVE:
        print("\nNext steps:")
        print(
            "  1. Reseed WorkItems:  python tools/reseed_work_items.py --nova-id <id> --dirty all"
        )
        print("  2. Trigger a sweep to regenerate artifacts")

    return nova


def _show_status(table, nova_id: str) -> None:
    """Print the current status of a nova."""
    nova = _get_nova(table, nova_id)
    if not nova:
        print(f"ERROR: Nova item not found for {nova_id}", file=sys.stderr)
        sys.exit(1)

    primary_name = nova.get("primary_name", "unknown")
    status = nova.get("status", "unknown")
    print(f"{primary_name} ({nova_id}): {status}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    for action in ["hide", "show", "status"]:
        sub = subparsers.add_parser(action)
        id_group = sub.add_mutually_exclusive_group(required=True)
        id_group.add_argument("--nova", type=str, help="Nova UUID")
        id_group.add_argument("--name", type=str, help="Nova name (resolved via NameMapping)")
        if action != "status":
            sub.add_argument("--dry-run", action="store_true", help="Show what would change")
        sub.add_argument(
            "--table",
            default=os.environ.get("NOVACAT_TABLE_NAME", "NovaCat"),
            help="DynamoDB table name",
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
        print(f"Resolved '{args.name}' → {nova_id}\n")
    else:
        nova_id = args.nova

    if args.action == "hide":
        _set_status(table, nova_id, _STATUS_HIDDEN, args.dry_run)
    elif args.action == "show":
        _set_status(table, nova_id, _STATUS_ACTIVE, args.dry_run)
    elif args.action == "status":
        _show_status(table, nova_id)


if __name__ == "__main__":
    main()
