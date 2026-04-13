#!/usr/bin/env python3
"""
Replace underscores with spaces in Nova primary_name fields.

Scans all ACTIVE Nova items, finds any with underscores in primary_name,
and updates the display name. Does NOT touch primary_name_normalized or
NameMappings (those already have spaces).

Dry-run by default. Pass --execute to apply.

Usage:
    python tools/fix_underscore_names.py
    python tools/fix_underscore_names.py --execute

Operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

import boto3

TABLE_NAME = "NovaCat"
REGION = "us-east-1"

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def main():
    parser = argparse.ArgumentParser(description="Fix underscores in Nova primary_name.")
    parser.add_argument("--execute", action="store_true", help="Apply changes.")
    parser.add_argument("--region", default=REGION)
    args = parser.parse_args()

    dry_run = not args.execute
    table = boto3.resource("dynamodb", region_name=args.region).Table(TABLE_NAME)

    # Scan all Nova items
    items = []
    kwargs = {
        "FilterExpression": "entity_type = :et AND SK = :sk",
        "ExpressionAttributeValues": {":et": "Nova", ":sk": "NOVA"},
        "ProjectionExpression": "PK, primary_name, nova_id",
    }
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if resp.get("LastEvaluatedKey") is None:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    items.sort(key=lambda x: x.get("primary_name", ""))

    if dry_run:
        print(f"\n  {YELLOW}DRY RUN — pass --execute to apply.{RESET}\n")

    needs_fix = [i for i in items if "_" in i.get("primary_name", "")]
    no_fix = [i for i in items if "_" not in i.get("primary_name", "")]

    print(f"  Total novae: {len(items)}")
    print(f"  Need fixing: {len(needs_fix)}")
    print(f"  Already OK:  {len(no_fix)}\n")

    if not needs_fix:
        print(f"  {GREEN}Nothing to fix.{RESET}\n")
        return

    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    fixed = 0

    for item in needs_fix:
        old_name = item["primary_name"]
        new_name = old_name.replace("_", " ")
        nova_id = item["nova_id"]

        print(f"  {old_name:25s} → {new_name}", end="")

        if dry_run:
            print(f"  {DIM}(dry run){RESET}")
        else:
            table.update_item(
                Key={"PK": nova_id, "SK": "NOVA"},
                UpdateExpression="SET primary_name = :pn, updated_at = :now",
                ExpressionAttributeValues={":pn": new_name, ":now": now},
            )
            print(f"  {GREEN}✓{RESET}")
            fixed += 1

    print(f"\n  {BOLD}{'Would fix' if dry_run else 'Fixed'}: {len(needs_fix)} novae{RESET}")
    if dry_run:
        print(f"  {YELLOW}Run with --execute to apply.{RESET}")
    print()


if __name__ == "__main__":
    main()
