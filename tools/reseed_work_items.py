#!/usr/bin/env python3
"""reseed_work_items.py — Reseed WorkItems for one or all novae.

Writes WorkItem(s) to the WORKQUEUE partition so that the next
artifact regeneration sweep picks up the nova(e) and regenerates
their artifacts.

Usage:
    # Reseed spectra WorkItem for V906 Car (by nova_id)
    python reseed_work_items.py --nova-id 64c5c516-... --dirty spectra

    # Reseed both spectra and photometry
    python reseed_work_items.py --nova-id 64c5c516-... --dirty spectra photometry

    # Reseed all dirty types for a single nova
    python reseed_work_items.py --nova-id 64c5c516-... --dirty all

    # Resolve by name instead of nova_id
    python reseed_work_items.py --name "V906 Car" --dirty spectra

    # Reseed ALL ACTIVE novae with all dirty types (full catalog rebuild)
    python reseed_work_items.py --all --dirty all

    # Reseed ALL ACTIVE novae for spectra only
    python reseed_work_items.py --all --dirty spectra

    # Dry run — show what would be written
    python reseed_work_items.py --all --dirty all --dry-run

    # Use a specific table (default: reads NOVACAT_TABLE_NAME from env)
    python reseed_work_items.py --nova-id abc123 --dirty spectra --table NovaCat

Operator tooling — no CI requirements.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import UTC, datetime

import boto3
from boto3.dynamodb.conditions import Key

_WORKQUEUE_PK = "WORKQUEUE"
_TTL_DAYS = 30
_ALL_DIRTY_TYPES = ["spectra", "photometry", "references"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _scan_active_novae(table_resource) -> list[dict]:
    """Scan for all ACTIVE Nova items, returning nova_id + primary_name."""
    items: list[dict] = []
    kwargs: dict = {
        "FilterExpression": "entity_type = :et AND #s = :status",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":et": "Nova", ":status": "ACTIVE"},
        "ProjectionExpression": "nova_id, primary_name",
    }
    while True:
        resp = table_resource.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if resp.get("LastEvaluatedKey") is None:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    items.sort(key=lambda x: x.get("primary_name", ""))
    return items


def _write_work_item(
    table_resource,
    nova_id: str,
    dirty_type: str,
    dry_run: bool = False,
) -> dict:
    now = datetime.now(UTC)
    created_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    sk = f"{nova_id}#{dirty_type}#{created_at}"
    ttl = int(now.timestamp()) + (_TTL_DAYS * 86_400)
    correlation_id = f"manual-reseed-{uuid.uuid4().hex[:12]}"

    item = {
        "PK": _WORKQUEUE_PK,
        "SK": sk,
        "entity_type": "WorkItem",
        "schema_version": "1.0.0",
        "nova_id": nova_id,
        "dirty_type": dirty_type,
        "source_workflow": "manual_reseed",
        "job_run_id": "00000000-0000-0000-0000-000000000000",
        "correlation_id": correlation_id,
        "created_at": created_at,
        "ttl": ttl,
    }

    if dry_run:
        print(f"  [DRY RUN] Would write: PK={_WORKQUEUE_PK}  SK={sk}")
    else:
        table_resource.put_item(Item=item)
        print(f"  Written: PK={_WORKQUEUE_PK}  SK={sk}")

    return item


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    import os

    parser = argparse.ArgumentParser(
        description="Reseed WorkItems so the regeneration sweep picks up novae.",
    )
    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument("--nova-id", help="Nova UUID")
    id_group.add_argument("--name", help="Nova name (resolved via NameMapping)")
    id_group.add_argument(
        "--all",
        action="store_true",
        help="Reseed all ACTIVE novae",
    )

    parser.add_argument(
        "--dirty",
        nargs="+",
        required=True,
        choices=_ALL_DIRTY_TYPES + ["all"],
        help="Dirty type(s) to seed. Use 'all' for spectra + photometry + references.",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("NOVACAT_TABLE_NAME", "NovaCat"),
        help="DynamoDB table name (default: $NOVACAT_TABLE_NAME or 'NovaCat')",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be written")

    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.table)

    # Expand dirty types
    dirty_types = _ALL_DIRTY_TYPES if "all" in args.dirty else args.dirty

    if args.all:
        # ── Reseed all ACTIVE novae ──────────────────────────────────
        novae = _scan_active_novae(table)
        if not novae:
            print("ERROR: No ACTIVE novae found.", file=sys.stderr)
            sys.exit(1)

        print(f"Found {len(novae)} ACTIVE novae")
        print(f"Table:       {args.table}")
        print(f"Dirty types: {', '.join(dirty_types)}")
        if args.dry_run:
            print("Mode:        DRY RUN")
        print()

        total = 0
        for nova in novae:
            nova_id = nova["nova_id"]
            name = nova.get("primary_name", nova_id)
            print(f"── {name} ({nova_id})")
            for dt in dirty_types:
                _write_work_item(table, nova_id, dt, dry_run=args.dry_run)
                total += 1
            print()

        print(f"Done. Seeded {total} WorkItems for {len(novae)} novae.")

    else:
        # ── Reseed a single nova ─────────────────────────────────────
        if args.name:
            nova_id = _resolve_name(table, args.name)
            if not nova_id:
                print(f"ERROR: Could not resolve name '{args.name}' to a nova_id.", file=sys.stderr)
                sys.exit(1)
            print(f"Resolved '{args.name}' → {nova_id}")
        else:
            nova_id = args.nova_id

        print(f"Nova:        {nova_id}")
        print(f"Table:       {args.table}")
        print(f"Dirty types: {', '.join(dirty_types)}")
        if args.dry_run:
            print("Mode:        DRY RUN")
        print()

        for dt in dirty_types:
            _write_work_item(table, nova_id, dt, dry_run=args.dry_run)

        print()
        print("Done. Run the sweep to trigger regeneration.")


if __name__ == "__main__":
    main()
