#!/usr/bin/env python3
"""refresh_artifacts.py — Regenerate display artifacts in-place without a full sweep.

Runs specified generators locally and overwrites artifacts in the current
S3 release. No WorkItems, no coordinator, no Fargate, no bundle, no new
release prefix. Just remake the plots and go.

Usage:
    # Refresh spectra.json for all novae
    python refresh_artifacts.py --all --artifacts spectra

    # Refresh spectra + photometry for one nova
    python refresh_artifacts.py --name "V1369 Cen" --artifacts spectra photometry

    # Refresh everything except bundle for one nova
    python refresh_artifacts.py --name "V5668 Sgr" --artifacts all

    # Dry run — show what would be regenerated
    python refresh_artifacts.py --all --artifacts spectra --dry-run

    # Refresh only catalog.json (global, no per-nova work)
    python refresh_artifacts.py --catalog-only

Supported artifacts: spectra, photometry, sparkline, nova, references, catalog
"all" = all of the above (still no bundle)

Operator tooling — no CI requirements.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key

# ---------------------------------------------------------------------------
# We need to import the generators. Add the artifact_generator to sys.path.
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_AG_DIR = os.path.join(_REPO_ROOT, "services", "artifact_generator")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _AG_DIR not in sys.path:
    sys.path.insert(0, _AG_DIR)

# ---------------------------------------------------------------------------
# Env var aliasing — main.py uses CDK/Lambda-style names at module level.
# Map from local operator env vars to what main.py expects.
# ---------------------------------------------------------------------------
_ENV_ALIASES = {
    "NOVA_CAT_TABLE_NAME": "NOVACAT_TABLE_NAME",
    "NOVA_CAT_PHOTOMETRY_TABLE_NAME": "NOVACAT_PHOTOMETRY_TABLE_NAME",
    "NOVA_CAT_PRIVATE_BUCKET": "NOVACAT_PRIVATE_BUCKET",
    "NOVA_CAT_PUBLIC_SITE_BUCKET": "NOVACAT_PUBLIC_SITE_BUCKET",
    "PLAN_ID": None,  # main.py requires this; we provide a dummy
}

for target_key, source_key in _ENV_ALIASES.items():
    if target_key not in os.environ:
        if source_key and source_key in os.environ:
            os.environ[target_key] = os.environ[source_key]
        elif target_key == "PLAN_ID":
            os.environ[target_key] = "local-refresh"
        elif target_key == "BAND_REGISTRY_PATH":
            pass  # optional
        else:
            # Leave unset — will fail loudly if main.py needs it
            pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ALL_ARTIFACTS = ["references", "spectra", "photometry", "sparkline", "nova"]
_ARTIFACT_FILENAMES = {
    "references": "references.json",
    "spectra": "spectra.json",
    "photometry": "photometry.json",
    "sparkline": "sparkline.svg",
    "nova": "nova.json",
    "catalog": "catalog.json",
}


# ---------------------------------------------------------------------------
# AWS setup
# ---------------------------------------------------------------------------


def _get_aws_resources(table_name: str, phot_table_name: str) -> dict[str, Any]:
    dynamodb = boto3.resource("dynamodb")
    return {
        "dynamodb": dynamodb,
        "table": dynamodb.Table(table_name),
        "photometry_table": dynamodb.Table(phot_table_name) if phot_table_name else None,
        "s3": boto3.client("s3"),
    }


# ---------------------------------------------------------------------------
# Release discovery
# ---------------------------------------------------------------------------


def _get_current_release(s3, bucket: str) -> str | None:
    """Read the current release ID from current.json in the public bucket."""
    try:
        resp = s3.get_object(Bucket=bucket, Key="current.json")
        data = json.loads(resp["Body"].read().decode("utf-8"))
        return data.get("release_id")
    except Exception as exc:
        print(f"ERROR: Could not read current.json: {exc}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Nova helpers
# ---------------------------------------------------------------------------


def _scan_active_novae(table: Any) -> list[dict]:
    items: list[dict] = []
    kwargs: dict = {
        "FilterExpression": Attr("entity_type").eq("Nova") & Attr("status").eq("ACTIVE"),
    }
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    items.sort(key=lambda x: x.get("primary_name", ""))
    return items


def _resolve_name(table: Any, name: str) -> dict | None:
    normalized = name.strip().lower().replace("_", " ")
    pk = f"NAME#{normalized}"
    resp = table.query(KeyConditionExpression=Key("PK").eq(pk), Limit=1)
    items = resp.get("Items", [])
    if not items:
        return None
    nova_id = items[0]["nova_id"]
    nova_resp = table.get_item(Key={"PK": nova_id, "SK": "NOVA"})
    return nova_resp.get("Item")


def _get_nova_item(table: Any, nova_id: str) -> dict | None:
    resp = table.get_item(Key={"PK": nova_id, "SK": "NOVA"})
    return resp.get("Item")


# ---------------------------------------------------------------------------
# Context setup (mirrors main.py's _process_nova preamble)
# ---------------------------------------------------------------------------


def _build_nova_context(
    nova_item: dict,
    table: Any,
    photometry_table: Any | None,
) -> dict[str, Any]:
    """Build the nova_context dict that generators expect."""
    from main import _collect_observation_epochs, resolve_outburst_mjd

    nova_id = nova_item["nova_id"]

    ctx: dict[str, Any] = {
        "nova_item": nova_item,
        "spectra_count": int(nova_item.get("spectra_count", 0)),
        "photometry_count": int(nova_item.get("photometry_count", 0)),
        "references_count": int(nova_item.get("references_count", 0)),
        "has_sparkline": nova_item.get("has_sparkline", False),
        "spectral_visits": int(nova_item.get("spectral_visits", 0)),
    }

    # Outburst MJD resolution
    observation_epochs = _collect_observation_epochs(nova_id)
    outburst_mjd, is_estimated = resolve_outburst_mjd(
        nova_item.get("discovery_date"),
        nova_item.get("nova_type"),
        observation_epochs,
        outburst_date=nova_item.get("outburst_date"),
    )
    ctx["outburst_mjd"] = outburst_mjd
    ctx["outburst_mjd_is_estimated"] = is_estimated

    return ctx


# ---------------------------------------------------------------------------
# Per-nova artifact generation + upload
# ---------------------------------------------------------------------------


def _generate_and_upload(
    nova_item: dict,
    artifacts: list[str],
    aws: dict[str, Any],
    release_id: str,
    public_bucket: str,
    private_bucket: str,
    band_registry: dict[str, Any],
    dry_run: bool,
) -> dict[str, int]:
    nova_id = nova_item["nova_id"]
    table = aws["table"]
    s3 = aws["s3"]
    stats = {"generated": 0, "failed": 0}

    ctx = _build_nova_context(nova_item, table, aws["photometry_table"])

    prefix = f"releases/{release_id}/nova/{nova_id}/"

    for artifact_name in artifacts:
        filename = _ARTIFACT_FILENAMES[artifact_name]

        if dry_run:
            print(f"    [DRY RUN] Would regenerate {filename}")
            stats["generated"] += 1
            continue

        try:
            result = _run_generator(
                artifact_name, nova_id, ctx, aws, private_bucket, public_bucket, band_registry
            )
            if result is None:
                print(f"    {filename} — skipped (generator returned None)")
                continue

            # Upload
            s3_key = f"{prefix}{filename}"
            if isinstance(result, str):
                # SVG
                body = result.encode("utf-8")
                content_type = "image/svg+xml"
            else:
                # JSON
                body = json.dumps(result, default=_decimal_default).encode("utf-8")
                content_type = "application/json"

            s3.put_object(
                Bucket=public_bucket,
                Key=s3_key,
                Body=body,
                ContentType=content_type,
            )
            print(f"    {filename} → s3://{public_bucket}/{s3_key}")
            stats["generated"] += 1

        except Exception as exc:
            print(f"    {filename} — FAILED: {exc}")
            stats["failed"] += 1

    return stats


def _run_generator(
    artifact_name: str,
    nova_id: str,
    ctx: dict[str, Any],
    aws: dict[str, Any],
    private_bucket: str,
    public_bucket: str,
    band_registry: dict[str, Any],
) -> Any:
    """Run a single generator and return the artifact dict/string."""
    table = aws["table"]
    dynamodb = aws["dynamodb"]
    s3 = aws["s3"]

    if artifact_name == "references":
        from generators.references import generate_references_json

        return generate_references_json(nova_id, table, dynamodb, ctx)

    elif artifact_name == "spectra":
        from generators.spectra import generate_spectra_json

        return generate_spectra_json(nova_id, table, s3, private_bucket, ctx)

    elif artifact_name == "photometry":
        from generators.photometry import generate_photometry_json

        return generate_photometry_json(nova_id, aws["photometry_table"], band_registry, ctx)

    elif artifact_name == "sparkline":
        from generators.sparkline import generate_sparkline_svg

        return generate_sparkline_svg(nova_id, ctx)

    elif artifact_name == "nova":
        from generators.nova import generate_nova_json

        return generate_nova_json(nova_id, table, ctx)

    else:
        raise ValueError(f"Unknown artifact: {artifact_name}")


def _decimal_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


# ---------------------------------------------------------------------------
# Catalog-only mode
# ---------------------------------------------------------------------------


def _regenerate_catalog(
    aws: dict[str, Any],
    release_id: str,
    public_bucket: str,
    dry_run: bool,
) -> None:
    """Regenerate just catalog.json and upload it."""
    from generators.catalog import generate_catalog_json

    if dry_run:
        print("[DRY RUN] Would regenerate catalog.json")
        return

    table = aws["table"]
    artifact = generate_catalog_json(table)

    s3_key = f"releases/{release_id}/catalog.json"
    body = json.dumps(artifact, default=_decimal_default).encode("utf-8")
    aws["s3"].put_object(
        Bucket=public_bucket,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
    )
    print(f"catalog.json → s3://{public_bucket}/{s3_key}")


# ---------------------------------------------------------------------------
# Band registry loader
# ---------------------------------------------------------------------------


def _load_band_registry(registry_path: str) -> dict[str, Any]:
    try:
        with open(registry_path) as f:
            raw = json.load(f)
        return {entry["band_id"]: entry for entry in raw.get("bands", [])}
    except FileNotFoundError:
        print(f"WARNING: Band registry not found at {registry_path}", file=sys.stderr)
        return {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regenerate display artifacts in-place (no sweep, no bundle).",
    )

    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--name", help="Nova name")
    target.add_argument("--nova-id", help="Nova UUID")
    target.add_argument("--all", action="store_true", help="All ACTIVE novae")
    target.add_argument("--catalog-only", action="store_true", help="Only regenerate catalog.json")

    parser.add_argument(
        "--artifacts",
        nargs="+",
        choices=_ALL_ARTIFACTS + ["all"],
        help="Which artifacts to regenerate (default: all except bundle)",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("NOVACAT_TABLE_NAME", "NovaCat"),
    )
    parser.add_argument(
        "--phot-table",
        default=os.environ.get("NOVA_CAT_PHOTOMETRY_TABLE_NAME", ""),
    )
    parser.add_argument(
        "--public-bucket",
        default=os.environ.get("NOVACAT_PUBLIC_SITE_BUCKET", ""),
    )
    parser.add_argument(
        "--private-bucket",
        default=os.environ.get("NOVACAT_PRIVATE_BUCKET", ""),
    )
    parser.add_argument(
        "--registry",
        default=os.path.join(
            _REPO_ROOT, "services", "photometry_ingestor", "band_registry", "band_registry.json"
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--also-catalog",
        action="store_true",
        help="Also regenerate catalog.json after per-nova artifacts",
    )

    args = parser.parse_args()

    if not args.catalog_only and not args.artifacts:
        parser.error("--artifacts is required unless using --catalog-only")

    if not args.public_bucket:
        parser.error("--public-bucket or NOVACAT_PUBLIC_BUCKET required")

    aws = _get_aws_resources(args.table, args.phot_table)
    s3 = aws["s3"]

    # Find current release
    release_id = _get_current_release(s3, args.public_bucket)
    if not release_id:
        sys.exit(1)

    print(f"Release:  {release_id}")
    print(f"Table:    {args.table}")
    print(f"Bucket:   {args.public_bucket}")
    if args.dry_run:
        print("Mode:     DRY RUN")
    print()

    # Catalog-only mode
    if args.catalog_only:
        _regenerate_catalog(aws, release_id, args.public_bucket, args.dry_run)
        return

    # Resolve artifacts list
    artifact_list = _ALL_ARTIFACTS if "all" in args.artifacts else args.artifacts

    # Dependency order
    ordered = [a for a in _ALL_ARTIFACTS if a in artifact_list]

    # Load band registry if photometry is requested
    band_registry: dict[str, Any] = {}
    if "photometry" in ordered:
        band_registry = _load_band_registry(args.registry)
        if not band_registry:
            print("WARNING: No band registry loaded — photometry generation may fail")

    # Resolve nova targets
    if args.all:
        novae = _scan_active_novae(aws["table"])
    elif args.name:
        nova_item = _resolve_name(aws["table"], args.name)
        if not nova_item:
            print(f"ERROR: Could not resolve '{args.name}'", file=sys.stderr)
            sys.exit(1)
        novae = [nova_item]
    else:
        nova_item = _get_nova_item(aws["table"], args.nova_id)
        if not nova_item:
            print(f"ERROR: Nova {args.nova_id} not found", file=sys.stderr)
            sys.exit(1)
        novae = [nova_item]

    print(f"Novae:    {len(novae)}")
    print(f"Artifacts: {', '.join(ordered)}")
    print()

    totals = {"generated": 0, "failed": 0}
    for nova_item in novae:
        name = nova_item.get("primary_name", nova_item["nova_id"])
        print(f"── {name}")
        stats = _generate_and_upload(
            nova_item,
            ordered,
            aws,
            release_id,
            args.public_bucket,
            args.private_bucket or "",
            band_registry,
            args.dry_run,
        )
        totals["generated"] += stats["generated"]
        totals["failed"] += stats["failed"]
        print()

    # Optionally regenerate catalog.json
    if args.also_catalog:
        print("── catalog.json")
        _regenerate_catalog(aws, release_id, args.public_bucket, args.dry_run)
        print()

    print(f"Done. Generated: {totals['generated']}, Failed: {totals['failed']}")


if __name__ == "__main__":
    main()
