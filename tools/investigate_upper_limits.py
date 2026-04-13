#!/usr/bin/env python3
"""
Investigate missing upper limits for a nova.

Compares DynamoDB photometry rows (source of truth) against the published
photometry.json artifact in S3 to identify where upper limits are being
lost in the pipeline.

Usage:
    python investigate_upper_limits.py --nova "V5588 Sgr"
    python investigate_upper_limits.py --nova-id b8009ab4-9f35-486d-a071-ea4fc9490a85
    python investigate_upper_limits.py --nova "V5588 Sgr" --bucket my-bucket

Operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

# ── Config ──────────────────────────────────────────────────────────────────
MAIN_TABLE = "NovaCat"
PHOT_TABLE = "NovaCatPhotometry"
REGION = "us-east-1"

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


# ── Helpers ─────────────────────────────────────────────────────────────────


def _dec(v):
    """Convert Decimal to float for display."""
    if isinstance(v, Decimal):
        return float(v)
    return v


def _section(title: str) -> None:
    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}")


def _ok(msg: str) -> None:
    print(f"  {GREEN}✓{RESET} {msg}")


def _warn(msg: str) -> None:
    print(f"  {YELLOW}⚠{RESET} {msg}")


def _fail(msg: str) -> None:
    print(f"  {RED}✗{RESET} {msg}")


def _info(msg: str) -> None:
    print(f"  {CYAN}ℹ{RESET} {msg}")


# ── Name resolution ────────────────────────────────────────────────────────


def resolve_nova_id(main_table, name: str) -> str | None:
    normalized = re.sub(r"\s+", " ", name.strip().lower())
    resp = main_table.query(
        KeyConditionExpression=Key("PK").eq(f"NAME#{normalized}"),
        Limit=1,
    )
    items = resp.get("Items", [])
    if not items:
        return None
    return items[0].get("nova_id")


def get_nova_item(main_table, nova_id: str) -> dict | None:
    resp = main_table.get_item(Key={"PK": nova_id, "SK": "NOVA"})
    return resp.get("Item")


# ── DynamoDB photometry query ──────────────────────────────────────────────


def query_all_phot_rows(phot_table, nova_id: str) -> list[dict]:
    """Paginated query for all PHOT# rows."""
    rows = []
    kwargs = dict(
        KeyConditionExpression=Key("PK").eq(nova_id) & Key("SK").begins_with("PHOT#"),
    )
    while True:
        resp = phot_table.query(**kwargs)
        rows.extend(resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last
    return rows


# ── S3 artifact query ──────────────────────────────────────────────────────


def get_current_release(s3, bucket: str) -> str | None:
    try:
        resp = s3.get_object(Bucket=bucket, Key="current.json")
        pointer = json.loads(resp["Body"].read().decode("utf-8"))
        return pointer.get("release_id")
    except Exception as exc:
        print(f"  Could not read current.json: {exc}")
        return None


def get_photometry_json(s3, bucket: str, release_id: str, nova_id: str) -> dict | None:
    key = f"releases/{release_id}/nova/{nova_id}/photometry.json"
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        print(f"  Error reading photometry.json: {exc}")
        return None


def list_nova_artifacts(s3, bucket: str, release_id: str, nova_id: str) -> list[str]:
    prefix = f"releases/{release_id}/nova/{nova_id}/"
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"].split("/")[-1] for obj in resp.get("Contents", [])]
    except Exception:
        return []


# ── Analysis ───────────────────────────────────────────────────────────────


def analyze_ddb_rows(rows: list[dict]) -> None:
    _section("DynamoDB Photometry Rows")

    if not rows:
        _fail("No photometry rows found in DynamoDB.")
        return

    _ok(f"Total rows: {len(rows)}")

    # Split by upper limit
    detections = [r for r in rows if not r.get("is_upper_limit", False)]
    upper_limits = [r for r in rows if r.get("is_upper_limit", False)]

    _info(f"Detections: {len(detections)}")
    _info(f"Upper limits: {len(upper_limits)}")

    if not upper_limits:
        _warn("No upper limits in DynamoDB — nothing to investigate downstream.")
        return

    # Check upper limit data quality
    _section("Upper Limit Data Quality (DynamoDB)")

    ul_with_limiting = [r for r in upper_limits if r.get("limiting_value") is not None]
    ul_without_limiting = [r for r in upper_limits if r.get("limiting_value") is None]
    ul_with_flux = [r for r in upper_limits if r.get("flux_density") is not None]

    _info(f"Upper limits with limiting_value: {len(ul_with_limiting)}")
    if ul_without_limiting:
        _fail(f"Upper limits MISSING limiting_value: {len(ul_without_limiting)}")
        for r in ul_without_limiting[:5]:
            print(
                f"    MJD={_dec(r.get('time_mjd'))}, band={r.get('band_id')}, "
                f"flux_density={_dec(r.get('flux_density'))}"
            )
    else:
        _ok("All upper limits have limiting_value populated.")

    if ul_with_flux:
        _warn(
            f"Upper limits with non-null flux_density: {len(ul_with_flux)} "
            "(expected None for radio ULs)"
        )
    else:
        _ok("All upper limits have flux_density=None (correct for radio).")

    # Band breakdown
    _section("Band Breakdown (DynamoDB)")

    band_counts: Counter = Counter()
    band_ul_counts: Counter = Counter()
    for r in rows:
        band = r.get("band_id", "?")
        band_counts[band] += 1
        if r.get("is_upper_limit", False):
            band_ul_counts[band] += 1

    print(f"  {'Band':<25s} {'Total':>6s} {'ULs':>6s}")
    print(f"  {'─' * 40}")
    for band in sorted(band_counts.keys()):
        total = band_counts[band]
        uls = band_ul_counts.get(band, 0)
        marker = f" {YELLOW}← has ULs{RESET}" if uls > 0 else ""
        print(f"  {band:<25s} {total:>6d} {uls:>6d}{marker}")

    # Regime breakdown
    regimes = Counter(r.get("regime", "?") for r in rows)
    _info(f"Regimes: {dict(regimes)}")


def analyze_artifact(artifact: dict, ddb_upper_limits: list[dict]) -> None:
    _section("Published Artifact (photometry.json)")

    observations = artifact.get("observations", [])
    _info(f"Total observations in artifact: {len(observations)}")

    art_detections = [o for o in observations if not o.get("is_upper_limit", False)]
    art_uls = [o for o in observations if o.get("is_upper_limit", False)]

    _info(f"Detections in artifact: {len(art_detections)}")
    _info(f"Upper limits in artifact: {len(art_uls)}")

    if not art_uls and ddb_upper_limits:
        _fail(f"ZERO upper limits in artifact but {len(ddb_upper_limits)} in DynamoDB!")
    elif len(art_uls) < len(ddb_upper_limits):
        _warn(
            f"Fewer upper limits in artifact ({len(art_uls)}) than DynamoDB "
            f"({len(ddb_upper_limits)}) — some may have been suppressed or lost."
        )
    elif art_uls:
        _ok(f"Upper limits present in artifact: {len(art_uls)}")

    # Check UL values in artifact
    if art_uls:
        _section("Upper Limit Values in Artifact")

        # uls_with_value = [o for o in art_uls
        #                   if o.get("limiting_value") is not None
        #                   or o.get("flux_density") is not None
        #                   or o.get("magnitude") is not None]
        uls_empty = [
            o
            for o in art_uls
            if o.get("limiting_value") is None
            and o.get("flux_density") is None
            and o.get("magnitude") is None
        ]

        if uls_empty:
            _fail(
                f"Upper limits with NO value (no limiting_value, flux_density, "
                f"or magnitude): {len(uls_empty)}"
            )
            for o in uls_empty[:5]:
                print(f"    MJD={o.get('time_mjd')}, band={o.get('band')}")
        else:
            _ok("All artifact upper limits have at least one value field.")

        for o in art_uls[:5]:
            print(
                f"    MJD={o.get('time_mjd')}, band={o.get('band')}, "
                f"lim={o.get('limiting_value')}, "
                f"flux={o.get('flux_density')}, "
                f"mag={o.get('magnitude')}"
            )

    # Band breakdown in artifact
    _section("Band Breakdown (Artifact)")

    art_band_counts: Counter = Counter()
    art_band_ul_counts: Counter = Counter()
    for o in observations:
        band = o.get("band", "?")
        art_band_counts[band] += 1
        if o.get("is_upper_limit", False):
            art_band_ul_counts[band] += 1

    print(f"  {'Band':<25s} {'Total':>6s} {'ULs':>6s}")
    print(f"  {'─' * 40}")
    for band in sorted(art_band_counts.keys()):
        total = art_band_counts[band]
        uls = art_band_ul_counts.get(band, 0)
        marker = f" {YELLOW}← has ULs{RESET}" if uls > 0 else ""
        print(f"  {band:<25s} {total:>6d} {uls:>6d}{marker}")


def compare_ddb_to_artifact(ddb_rows: list[dict], artifact: dict) -> None:
    _section("DynamoDB ↔ Artifact Comparison")

    observations = artifact.get("observations", [])

    _info(f"DynamoDB rows: {len(ddb_rows)}")
    _info(f"Artifact observations: {len(observations)}")

    if len(observations) < len(ddb_rows):
        diff = len(ddb_rows) - len(observations)
        _warn(
            f"Artifact has {diff} fewer observations than DynamoDB. "
            f"Possible causes: subsampling, suppression, or stale artifact."
        )
    elif len(observations) == len(ddb_rows):
        _ok("Row counts match.")
    else:
        _warn(
            f"Artifact has MORE observations ({len(observations)}) than "
            f"DynamoDB ({len(ddb_rows)}) — unexpected."
        )

    # Compare upper limit counts by band
    ddb_ul_by_band: Counter = Counter()
    for r in ddb_rows:
        if r.get("is_upper_limit"):
            ddb_ul_by_band[r.get("band_id", "?")] += 1

    art_ul_by_band: Counter = Counter()
    for o in observations:
        if o.get("is_upper_limit"):
            art_ul_by_band[o.get("band", "?")] += 1

    all_bands = sorted(set(ddb_ul_by_band.keys()) | set(art_ul_by_band.keys()))
    if all_bands:
        print(f"\n  {'Band':<25s} {'DDB ULs':>8s} {'Art ULs':>8s} {'Status':>10s}")
        print(f"  {'─' * 55}")
        for band in all_bands:
            ddb_n = ddb_ul_by_band.get(band, 0)
            # The artifact uses display labels, DDB uses band_id — try both
            art_n = art_ul_by_band.get(band, 0)
            if art_n == 0:
                # Try matching by display name pattern (e.g., "34.8 GHz" vs "Radio_34.8_GHz")
                for art_band, count in art_ul_by_band.items():
                    if art_band in band or band in art_band:
                        art_n = count
                        break
            status = f"{GREEN}OK{RESET}" if ddb_n == art_n else f"{RED}MISMATCH{RESET}"
            print(f"  {band:<25s} {ddb_n:>8d} {art_n:>8d} {status:>10s}")


# ── Main ───────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Investigate missing upper limits for a nova.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--nova", help="Nova name (e.g., 'V5588 Sgr')")
    group.add_argument("--nova-id", help="Nova UUID directly")
    parser.add_argument(
        "--bucket",
        default=None,
        help="Public site S3 bucket (default: NOVACAT_PUBLIC_SITE_BUCKET env var)",
    )
    parser.add_argument("--region", default=REGION)
    parser.add_argument(
        "--skip-s3", action="store_true", help="Skip S3 artifact checks (DynamoDB only)"
    )
    args = parser.parse_args()

    import os

    bucket = args.bucket or os.environ.get("NOVACAT_PUBLIC_SITE_BUCKET")

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    main_table = dynamodb.Table(MAIN_TABLE)
    phot_table = dynamodb.Table(PHOT_TABLE)

    # ── Resolve nova ──────────────────────────────────────────────────
    _section("Nova Resolution")

    if args.nova:
        nova_id = resolve_nova_id(main_table, args.nova)
        if not nova_id:
            _fail(f"Could not resolve name: {args.nova!r}")
            sys.exit(1)
        _ok(f"Name: {args.nova} → nova_id: {nova_id}")
    else:
        nova_id = args.nova_id
        _info(f"Using nova_id directly: {nova_id}")

    nova_item = get_nova_item(main_table, nova_id)
    if nova_item:
        _ok(f"primary_name: {nova_item.get('primary_name')}")
        _info(f"status: {nova_item.get('status')}")
        _info(
            f"spectra_count: {nova_item.get('spectra_count', '?')}, "
            f"photometry_count: {nova_item.get('photometry_count', '?')}"
        )
    else:
        _warn("Nova item not found in main table.")

    # ── Query DynamoDB ────────────────────────────────────────────────
    ddb_rows = query_all_phot_rows(phot_table, nova_id)
    ddb_upper_limits = [r for r in ddb_rows if r.get("is_upper_limit", False)]

    analyze_ddb_rows(ddb_rows)

    if args.skip_s3:
        _info("\nSkipping S3 checks (--skip-s3).")
        return

    # ── Check S3 artifact ─────────────────────────────────────────────
    if not bucket:
        _warn("\nNo S3 bucket configured. Set NOVACAT_PUBLIC_SITE_BUCKET or pass --bucket.")
        _warn("Skipping artifact comparison.")
        return

    s3 = boto3.client("s3", region_name=args.region)

    _section("S3 Release Info")

    release_id = get_current_release(s3, bucket)
    if not release_id:
        _fail("Could not determine current release.")
        return
    _ok(f"Current release: {release_id}")

    artifacts = list_nova_artifacts(s3, bucket, release_id, nova_id)
    if artifacts:
        _ok(f"Artifacts for this nova: {', '.join(sorted(artifacts))}")
    else:
        _fail(f"No artifacts found for nova {nova_id} in release {release_id}.")
        _info("This nova may not have been included in the last artifact generation sweep.")
        return

    if "photometry.json" not in artifacts:
        _fail("photometry.json is MISSING from the release.")
        _info("Artifacts present: " + ", ".join(sorted(artifacts)))
        _info("The artifact generator may have skipped photometry for this nova.")
        return

    # ── Load and analyze artifact ─────────────────────────────────────
    artifact = get_photometry_json(s3, bucket, release_id, nova_id)
    if artifact is None:
        _fail("Could not load photometry.json.")
        return

    analyze_artifact(artifact, ddb_upper_limits)
    compare_ddb_to_artifact(ddb_rows, artifact)

    # ── Summary ───────────────────────────────────────────────────────
    _section("Summary")

    art_obs = artifact.get("observations", [])
    art_uls = [o for o in art_obs if o.get("is_upper_limit")]

    if len(ddb_upper_limits) > 0 and len(art_uls) == 0:
        _fail(
            "DIAGNOSIS: Upper limits exist in DynamoDB but are completely absent "
            "from the published artifact. The artifact generator is either not "
            "reading them or dropping them during generation."
        )
    elif len(ddb_upper_limits) > len(art_uls):
        _warn(
            "DIAGNOSIS: Some upper limits are missing from the artifact. "
            "Possible subsampling, suppression, or stale artifact."
        )
    elif len(ddb_upper_limits) == 0:
        _warn(
            "DIAGNOSIS: No upper limits in DynamoDB — nothing to publish. "
            "Check whether ingestion dropped the rows (band resolution failure?)."
        )
    else:
        _ok("Upper limits appear consistent between DynamoDB and the artifact.")

    print()


if __name__ == "__main__":
    main()
