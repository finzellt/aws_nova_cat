#!/usr/bin/env python3
"""
purge_table.py — Delete every item in a NovaCat DynamoDB table.

Usage:
    python purge_table_phot.py                      # purge main NovaCat table
    python purge_table_phot.py --photometry         # purge photometry table
    python purge_table_phot.py --both               # purge both tables

Scans the full table, batch-deletes all items in chunks of 25 (DDB max),
and prints a running count. No recovery — run only when you mean it.

Operator tooling — no CI requirements (mypy / ruff not enforced).
"""

from __future__ import annotations

import argparse

import boto3

TABLE_NAME = "NovaCat"
PHOTOMETRY_TABLE_NAME = "NovaCatPhotometry"
REGION = "us-east-1"

dynamodb = boto3.resource("dynamodb", region_name=REGION)


def purge(table_name: str) -> int:
    """Purge all items from the given table. Returns count of items deleted."""
    table = dynamodb.Table(table_name)

    # Quick item count check
    table.reload()
    approx_count = table.item_count  # eventually consistent, but good enough
    if approx_count == 0:
        print(f"  {table_name}: table reports 0 items — nothing to do.")
        return 0

    print(f"  {table_name}: ~{approx_count} items (approximate). Scanning...")

    total_deleted = 0
    scan_kwargs: dict = {
        "ProjectionExpression": "PK, SK",
    }

    while True:
        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])

        if not items:
            break

        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={"PK": str(item["PK"]), "SK": str(item["SK"])})
                total_deleted += 1
                if total_deleted % 100 == 0:
                    print(f"    Deleted {total_deleted} items...", end="\r")

        # Handle pagination
        last: dict | None = resp.get("LastEvaluatedKey")
        if not last:
            break
        scan_kwargs["ExclusiveStartKey"] = last

    print(f"    Deleted {total_deleted} items from {table_name}." + " " * 20)
    return total_deleted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Purge all items from NovaCat DynamoDB table(s).",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--photometry",
        action="store_true",
        help=f"Purge the photometry table ({PHOTOMETRY_TABLE_NAME}) instead of the main table",
    )
    group.add_argument(
        "--both",
        action="store_true",
        help="Purge both the main table and the photometry table",
    )
    args = parser.parse_args()

    tables: list[str] = []
    if args.both:
        tables = [TABLE_NAME, PHOTOMETRY_TABLE_NAME]
    elif args.photometry:
        tables = [PHOTOMETRY_TABLE_NAME]
    else:
        tables = [TABLE_NAME]

    print(f"Purging: {', '.join(tables)}")
    print("This will delete ALL items. No recovery. Press Ctrl+C to abort.\n")

    grand_total = 0
    for t in tables:
        grand_total += purge(t)

    print(f"\nDone. {grand_total} total items deleted.")


if __name__ == "__main__":
    main()
