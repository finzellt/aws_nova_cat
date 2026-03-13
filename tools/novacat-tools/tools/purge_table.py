#!/usr/bin/env python3
"""
purge_table.py — Delete every item in the NovaCat DynamoDB table.

Usage:
    python purge_table.py

Scans the full table, batch-deletes all items in chunks of 25 (DDB max),
and prints a running count. No recovery — run only when you mean it.
"""

from __future__ import annotations

import boto3

TABLE_NAME = "NovaCat"
REGION = "us-east-1"

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


def purge() -> None:
    print(f"Scanning {TABLE_NAME}...")

    total_deleted = 0
    scan_kwargs: dict = {
        "ProjectionExpression": "PK, SK",
    }

    while True:
        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])

        if not items:
            break

        # Batch delete in chunks of 25 (DynamoDB BatchWriteItem limit)
        for i in range(0, len(items), 25):
            chunk = items[i : i + 25]
            with table.batch_writer() as batch:
                for item in chunk:
                    batch.delete_item(Key={"PK": str(item["PK"]), "SK": str(item["SK"])})
            total_deleted += len(chunk)
            print(f"  Deleted {total_deleted} items...", end="\r")

        # Handle pagination
        last: dict | None = resp.get("LastEvaluatedKey")
        if not last:
            break
        scan_kwargs["ExclusiveStartKey"] = last

    print(f"\nDone. {total_deleted} items deleted from {TABLE_NAME}.")


if __name__ == "__main__":
    purge()
