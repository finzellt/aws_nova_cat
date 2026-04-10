#!/usr/bin/env python3
"""backfill_snr_and_wavelength.py — Populate snr, wavelength_min_nm, wavelength_max_nm on DataProducts.

Two-pass backfill using data that's ALREADY in the system — no FITS re-reading needed.

Pass 1 — SNR from provider hints:
    ESO SSAP discovery stores `hints.snr` on every DataProduct. This pass
    copies `hints.snr` → top-level `snr` for any item where `snr` is missing.

Pass 2 — Wavelength range from hints or web-ready CSV:
    ESO SSAP stores `hints.em_min_m` / `hints.em_max_m` (metres). This pass
    converts to nm and writes `wavelength_min_nm` / `wavelength_max_nm`.
    If hints aren't available (e.g., ticket-ingested spectra), falls back to
    reading the first/last row of the web-ready CSV from S3.

Usage:
    # Dry run — see what would change
    python backfill_snr_and_wavelength.py --dry-run

    # Run for real
    python backfill_snr_and_wavelength.py

    # Single nova only
    python backfill_snr_and_wavelength.py --nova-id abc123

    # Specify table/bucket
    python backfill_snr_and_wavelength.py --table NovaCat --bucket nova-cat-private

Operator tooling — no CI requirements.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

# Metres → nanometres
_M_TO_NM = 1e9


def _scan_active_novae(table) -> list[dict]:
    """Return all ACTIVE Nova items."""
    items: list[dict] = []
    kwargs: dict = {
        "FilterExpression": Attr("entity_type").eq("Nova") & Attr("status").eq("ACTIVE"),
        "ProjectionExpression": "nova_id, primary_name",
    }
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    items.sort(key=lambda x: x.get("primary_name", ""))
    return items


def _query_valid_spectra(nova_id: str, table) -> list[dict]:
    """Query all VALID spectra DataProduct items for a nova."""
    items: list[dict] = []
    kwargs: dict = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
        ),
        "FilterExpression": Attr("validation_status").eq("VALID"),
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def _read_csv_wavelength_range(
    s3, bucket: str, nova_id: str, dp_id: str
) -> tuple[float, float] | None:
    """Read first/last wavelength from the web-ready CSV in S3."""
    key = f"derived/spectra/{nova_id}/{dp_id}/web_ready.csv"
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
    except ClientError:
        return None

    reader = csv.reader(io.StringIO(body))
    next(reader, None)  # skip header

    first_wl: float | None = None
    last_wl: float | None = None
    for row in reader:
        if len(row) >= 1:
            try:
                wl = float(row[0])
                if first_wl is None:
                    first_wl = wl
                last_wl = wl
            except ValueError:
                continue

    if first_wl is not None and last_wl is not None:
        return (first_wl, last_wl)
    return None


def _backfill_item(
    table,
    s3,
    bucket: str,
    item: dict,
    dry_run: bool,
) -> dict:
    """Backfill a single DataProduct item. Returns a stats dict."""
    nova_id = item["PK"]
    sk = item["SK"]
    dp_id = item["data_product_id"]
    hints = item.get("hints", {})

    updates: dict[str, Decimal] = {}
    sources: list[str] = []

    # --- SNR ---
    existing_snr = item.get("snr")
    if existing_snr is None:
        hint_snr = hints.get("snr")
        if hint_snr is not None:
            try:
                snr_val = float(hint_snr)
                if snr_val > 0:
                    updates["snr"] = Decimal(str(round(snr_val, 2)))
                    sources.append(f"snr={snr_val:.1f} (from hints)")
            except (ValueError, TypeError):
                pass

    # --- Wavelength range ---
    existing_wl_min = item.get("wavelength_min_nm")
    if existing_wl_min is None:
        # Try hints first (ESO SSAP: em_min_m / em_max_m in metres)
        em_min = hints.get("em_min_m")
        em_max = hints.get("em_max_m")
        if em_min is not None and em_max is not None:
            try:
                wl_min = float(em_min) * _M_TO_NM
                wl_max = float(em_max) * _M_TO_NM
                if 0 < wl_min < wl_max:
                    updates["wavelength_min_nm"] = Decimal(str(round(wl_min, 2)))
                    updates["wavelength_max_nm"] = Decimal(str(round(wl_max, 2)))
                    sources.append(f"wl={wl_min:.1f}-{wl_max:.1f}nm (from hints)")
            except (ValueError, TypeError):
                pass

        # Fall back to web-ready CSV
        if "wavelength_min_nm" not in updates:
            csv_range = _read_csv_wavelength_range(s3, bucket, nova_id, dp_id)
            if csv_range is not None:
                wl_min, wl_max = csv_range
                updates["wavelength_min_nm"] = Decimal(str(round(wl_min, 2)))
                updates["wavelength_max_nm"] = Decimal(str(round(wl_max, 2)))
                sources.append(f"wl={wl_min:.1f}-{wl_max:.1f}nm (from CSV)")

    if not updates:
        return {"skipped": 1}

    if dry_run:
        print(f"    [DRY RUN] {dp_id[:12]}... → {', '.join(sources)}")
        return {"would_update": 1}

    # Build update expression
    set_parts: list[str] = []
    values: dict[str, Decimal] = {}
    for field_name, value in updates.items():
        alias = f":{field_name.replace('.', '_')}"
        set_parts.append(f"{field_name} = {alias}")
        values[alias] = value

    table.update_item(
        Key={"PK": nova_id, "SK": sk},
        UpdateExpression="SET " + ", ".join(set_parts),
        ExpressionAttributeValues=values,
    )
    print(f"    {dp_id[:12]}... → {', '.join(sources)}")
    return {"updated": 1}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill snr and wavelength range on VALID SPECTRA DataProducts.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would change")
    parser.add_argument("--nova-id", help="Backfill a single nova only")
    parser.add_argument(
        "--table",
        default=os.environ.get("NOVACAT_TABLE_NAME", "NovaCat"),
        help="DynamoDB table name",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("NOVACAT_PRIVATE_BUCKET", ""),
        help="Private S3 bucket (for CSV fallback)",
    )
    args = parser.parse_args()

    if not args.bucket:
        print("ERROR: --bucket or NOVACAT_PRIVATE_BUCKET env var required.", file=sys.stderr)
        sys.exit(1)

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.table)
    s3 = boto3.client("s3")

    # Determine which novae to process
    if args.nova_id:
        novae = [{"nova_id": args.nova_id, "primary_name": args.nova_id}]
    else:
        novae = _scan_active_novae(table)
        if not novae:
            print("No ACTIVE novae found.")
            return

    print(f"Backfilling {len(novae)} novae")
    print(f"Table:  {args.table}")
    print(f"Bucket: {args.bucket}")
    if args.dry_run:
        print("Mode:   DRY RUN")
    print()

    totals = {"updated": 0, "would_update": 0, "skipped": 0, "products": 0}

    for nova in novae:
        nova_id = nova["nova_id"]
        name = nova.get("primary_name", nova_id)
        products = _query_valid_spectra(nova_id, table)

        if not products:
            continue

        print(f"── {name} ({len(products)} spectra)")
        totals["products"] += len(products)

        for product in products:
            stats = _backfill_item(table, s3, args.bucket, product, args.dry_run)
            for k, v in stats.items():
                totals[k] = totals.get(k, 0) + v

    print()
    print(f"Done. {totals['products']} products scanned.")
    if args.dry_run:
        print(f"  Would update: {totals.get('would_update', 0)}")
    else:
        print(f"  Updated: {totals.get('updated', 0)}")
    print(f"  Skipped (already populated): {totals.get('skipped', 0)}")


if __name__ == "__main__":
    main()
