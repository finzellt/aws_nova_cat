#!/usr/bin/env python3
"""nuke_nova.py — Delete all DynamoDB items for a nova.

Removes every trace of a nova from both the main NovaCat table and the
dedicated photometry table:

  1. Nova partition (PK = nova_id): Nova, PRODUCT#, NOVAREF#, JOBRUN#,
     ATTEMPT#, FILE# items
  2. NameMapping items (PK = NAME#<name>): primary name + all aliases
  3. LocatorAlias items (PK = LOCATOR#<provider>#<identity>): found via
     DataProduct items before they're deleted
  4. WORKQUEUE items (PK = WORKQUEUE, SK begins_with nova_id)
  5. IDEMPOTENCY# items matching the nova_id
  6. PhotometryRow items in the dedicated photometry table (PK = nova_id)

Does NOT touch S3 (raw FITS, derived CSVs, published artifacts). Use
--include-s3 if you want S3 cleanup too (future enhancement).

Usage:
    python tools/nuke_nova.py --nova <nova_id> --dry-run
    python tools/nuke_nova.py --name "IM Nor" --dry-run
    python tools/nuke_nova.py --nova <nova_id>

Environment variables:
    NOVACAT_TABLE_NAME            — main DynamoDB table (default: NovaCat)
    NOVACAT_PHOTOMETRY_TABLE_NAME — photometry table (default: NovaCatPhotometry)
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3
from boto3.dynamodb.conditions import Attr, Key

_BATCH_DELETE_LIMIT = 25


# ---------------------------------------------------------------------------
# Name resolution
# ---------------------------------------------------------------------------


def _resolve_name(table, name: str) -> str | None:
    normalized = name.strip().lower().replace("_", " ")
    pk = f"NAME#{normalized}"
    resp = table.query(KeyConditionExpression=Key("PK").eq(pk), Limit=1)
    items = resp.get("Items", [])
    return items[0].get("nova_id") if items else None


# ---------------------------------------------------------------------------
# Discovery: find all items to delete BEFORE deleting anything
# ---------------------------------------------------------------------------


def _query_nova_partition(table, nova_id: str) -> list[dict]:
    """All items in PK = nova_id."""
    items = []
    kwargs = {"KeyConditionExpression": Key("PK").eq(nova_id)}
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def _find_name_mapping_items(table, nova_item: dict) -> list[dict]:
    """Find NAME# items for primary name + all aliases."""
    items = []
    names = []

    primary = nova_item.get("primary_name_normalized") or nova_item.get("primary_name", "")
    if primary:
        names.append(primary.strip().lower().replace("_", " "))

    for alias in nova_item.get("aliases", []):
        normalized = alias.strip().lower().replace("_", " ")
        if normalized and normalized not in names:
            names.append(normalized)

    nova_id = nova_item.get("nova_id", "")

    for name in names:
        pk = f"NAME#{name}"
        resp = table.query(KeyConditionExpression=Key("PK").eq(pk))
        for item in resp.get("Items", []):
            if item.get("nova_id") == nova_id:
                items.append(item)

    return items


def _find_locator_items(table, nova_partition_items: list[dict]) -> list[dict]:
    """Find LOCATOR# items by reading provider + locator data from DataProducts."""
    locator_items = []

    for item in nova_partition_items:
        sk = item.get("SK", "")
        if not sk.startswith("PRODUCT#SPECTRA#"):
            continue

        dp_id = item.get("data_product_id", "")
        provider = item.get("provider", "")
        locator_identity = item.get("locator_identity", "")

        if not provider or not dp_id:
            continue

        # Try the LOCATOR# partition
        if locator_identity:
            pk = f"LOCATOR#{provider}#{locator_identity}"
            resp = table.query(KeyConditionExpression=Key("PK").eq(pk))
            locator_items.extend(resp.get("Items", []))

    return locator_items


def _find_workqueue_items(table, nova_id: str) -> list[dict]:
    """Find WORKQUEUE items for this nova."""
    items = []
    kwargs = {
        "KeyConditionExpression": Key("PK").eq("WORKQUEUE") & Key("SK").begins_with(nova_id),
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def _find_idempotency_items(table, nova_id: str) -> list[dict]:
    """Scan for IDEMPOTENCY# items referencing this nova_id."""
    items = []
    kwargs = {
        "FilterExpression": Attr("PK").begins_with("IDEMPOTENCY#"),
    }
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            if nova_id in item.get("primary_id", "") or nova_id in item.get("PK", ""):
                items.append(item)
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def _find_photometry_items(phot_table, nova_id: str) -> list[dict]:
    """All items in the photometry table for this nova."""
    items = []
    kwargs = {
        "KeyConditionExpression": Key("PK").eq(nova_id),
        "ProjectionExpression": "PK, SK",
    }
    while True:
        resp = phot_table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------


def _batch_delete(table, items: list[dict], label: str, dry_run: bool) -> int:
    """Delete items from a table. Returns count deleted."""
    if not items:
        return 0

    if dry_run:
        print(f"  [DRY RUN] Would delete {len(items)} {label} item(s)")
        for item in items[:10]:
            print(f"    PK={item['PK']}  SK={item['SK']}")
        if len(items) > 10:
            print(f"    ... and {len(items) - 10} more")
        return 0

    deleted = 0
    for i in range(0, len(items), _BATCH_DELETE_LIMIT):
        batch = items[i : i + _BATCH_DELETE_LIMIT]
        with table.batch_writer() as writer:
            for item in batch:
                writer.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
                deleted += 1

    print(f"  Deleted {deleted} {label} item(s)")
    return deleted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


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
        default=os.environ.get("NOVACAT_TABLE_NAME", "NovaCat"),
        help="Main DynamoDB table name",
    )
    parser.add_argument(
        "--phot-table",
        default=os.environ.get("NOVACAT_PHOTOMETRY_TABLE_NAME", "NovaCatPhotometry"),
        help="Photometry DynamoDB table name",
    )
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.table)
    phot_table = dynamodb.Table(args.phot_table)

    # Resolve nova_id
    if args.name:
        nova_id = _resolve_name(table, args.name)
        if not nova_id:
            print(f"ERROR: Could not resolve name '{args.name}' to a nova_id.", file=sys.stderr)
            sys.exit(1)
        print(f"Resolved '{args.name}' → {nova_id}")
    else:
        nova_id = args.nova

    print(f"\nNuking nova: {nova_id}")
    print(f"Main table:  {args.table}")
    print(f"Phot table:  {args.phot_table}")
    if args.dry_run:
        print("Mode:        DRY RUN")
    print()

    # --- Phase 1: Discover all items ---
    print("Discovering items...")

    nova_partition = _query_nova_partition(table, nova_id)
    nova_item = next((i for i in nova_partition if i.get("SK") == "NOVA"), None)

    if not nova_item:
        print(f"WARNING: No Nova item found for {nova_id}. Proceeding with partial cleanup.")

    name_items = _find_name_mapping_items(table, nova_item) if nova_item else []
    locator_items = _find_locator_items(table, nova_partition)
    workqueue_items = _find_workqueue_items(table, nova_id)
    idempotency_items = _find_idempotency_items(table, nova_id)
    photometry_items = _find_photometry_items(phot_table, nova_id)

    # Summarize
    print(f"  Nova partition:    {len(nova_partition)} items")
    print(f"  NameMapping:       {len(name_items)} items")
    print(f"  LocatorAlias:      {len(locator_items)} items")
    print(f"  WORKQUEUE:         {len(workqueue_items)} items")
    print(f"  IDEMPOTENCY:       {len(idempotency_items)} items")
    print(f"  Photometry:        {len(photometry_items)} items")

    total = (
        len(nova_partition)
        + len(name_items)
        + len(locator_items)
        + len(workqueue_items)
        + len(idempotency_items)
        + len(photometry_items)
    )
    print(f"  TOTAL:             {total} items")
    print()

    if total == 0:
        print("Nothing to delete.")
        return

    if not args.dry_run:
        confirm = input(f"Delete all {total} items for this nova? [y/N] ")
        if confirm.lower() != "y":
            print("Aborted.")
            return
        print()

    # --- Phase 2: Delete (order matters — read locators before deleting products) ---
    deleted = 0
    deleted += _batch_delete(table, locator_items, "LocatorAlias", args.dry_run)
    deleted += _batch_delete(table, name_items, "NameMapping", args.dry_run)
    deleted += _batch_delete(table, workqueue_items, "WORKQUEUE", args.dry_run)
    deleted += _batch_delete(table, idempotency_items, "IDEMPOTENCY", args.dry_run)
    deleted += _batch_delete(table, nova_partition, "nova partition", args.dry_run)
    deleted += _batch_delete(phot_table, photometry_items, "photometry", args.dry_run)

    print()
    if args.dry_run:
        print(f"[DRY RUN] Would delete {total} items total.")
    else:
        print(f"Deleted {deleted} items total. Nova {nova_id} has been nuked.")
    print()
    print("NOTE: S3 objects (raw FITS, derived CSVs, published artifacts) were NOT deleted.")
    print("The next artifact sweep will exclude this nova (no Nova item = no processing).")
    print("S3 objects will be orphaned until manually cleaned up.")


if __name__ == "__main__":
    main()
