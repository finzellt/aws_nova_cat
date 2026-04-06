#!/usr/bin/env python3
"""Pull published artifacts from S3 into frontend/public/data/ for local dev.

Reads the current release from the public site S3 bucket, downloads
catalog.json and per-nova artifacts, and writes them into the directory
structure expected by the frontend's dev-mode data client (DESIGN-003 §14.6).

S3 release layout (UUID-based):
    releases/<release_id>/catalog.json
    releases/<release_id>/nova/<nova_id>/nova.json
    releases/<release_id>/nova/<nova_id>/spectra.json
    ...

Local dev layout (name-based):
    frontend/public/data/catalog.json
    frontend/public/data/nova/<primary_name>/nova.json
    frontend/public/data/nova/<primary_name>/spectra.json
    ...

Usage
-----
  # Pull all novae from the current release:
  python tools/pull_dev_artifacts.py

  # Pull specific novae only:
  python tools/pull_dev_artifacts.py --nova "V1369 Cen" --nova "V1324 Sco"

  # Use a specific release ID instead of current:
  python tools/pull_dev_artifacts.py --release 20260401-143022

  # Dry run — show what would be downloaded:
  python tools/pull_dev_artifacts.py --dry-run

Prerequisites
-------------
  - AWS credentials configured (env vars, ~/.aws/credentials, or instance role)
  - NOVACAT_PUBLIC_SITE_BUCKET env var set (written by deploy.sh), or pass
    --bucket explicitly
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import boto3

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
_OUTPUT_DIR = _REPO_ROOT / "frontend" / "public" / "data"

# Per-nova artifacts to download. Order doesn't matter; all are optional
# (a nova might not have photometry yet, for instance).
_NOVA_ARTIFACTS = [
    "nova.json",
    "spectra.json",
    "photometry.json",
    "references.json",
    "sparkline.svg",
]


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _resolve_bucket(explicit: str | None) -> str:
    """Return the public site bucket name."""
    if explicit:
        return explicit

    import os

    env_val = os.environ.get("NOVACAT_PUBLIC_SITE_BUCKET")
    if env_val:
        return env_val

    print(
        "ERROR: No bucket specified. Either:\n"
        "  - Set NOVACAT_PUBLIC_SITE_BUCKET (written by deploy.sh), or\n"
        "  - Pass --bucket <name> explicitly.",
        file=sys.stderr,
    )
    sys.exit(1)


def _resolve_release_id(s3, bucket: str, explicit: str | None) -> str:
    """Return the active release ID from current.json, or the explicit override."""
    if explicit:
        print(f"Using explicit release ID: {explicit}")
        return explicit

    print("Reading current.json to resolve active release...")
    try:
        resp = s3.get_object(Bucket=bucket, Key="current.json")
        pointer = json.loads(resp["Body"].read().decode("utf-8"))
        release_id = pointer["release_id"]
        print(f"Active release: {release_id}")
        return release_id
    except Exception as exc:
        print(f"ERROR: Could not read current.json: {exc}", file=sys.stderr)
        sys.exit(1)


def _download_json(s3, bucket: str, key: str) -> dict | list | None:
    """Download and parse a JSON object from S3. Returns None on 404."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        print(f"  WARNING: Failed to read s3://{bucket}/{key}: {exc}")
        return None


def _download_bytes(s3, bucket: str, key: str) -> bytes | None:
    """Download raw bytes from S3. Returns None on 404."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        print(f"  WARNING: Failed to read s3://{bucket}/{key}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pull published artifacts from S3 for local frontend dev.",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Public site S3 bucket name (default: NOVACAT_PUBLIC_SITE_BUCKET env var).",
    )
    parser.add_argument(
        "--release",
        default=None,
        help="Release ID to pull (default: read from current.json).",
    )
    parser.add_argument(
        "--nova",
        action="append",
        default=None,
        dest="novae",
        help=(
            "Nova primary name to pull (e.g., 'V1369 Cen'). "
            "Can be specified multiple times. Default: all novae in the release."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without writing files.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing frontend/public/data/ before downloading.",
    )
    args = parser.parse_args()

    bucket = _resolve_bucket(args.bucket)
    s3 = boto3.client("s3")
    release_id = _resolve_release_id(s3, bucket, args.release)

    # ── Step 1: Download catalog.json ────────────────────────────────
    catalog_key = f"releases/{release_id}/catalog.json"
    print(f"Downloading catalog.json from {catalog_key}...")
    catalog = _download_json(s3, bucket, catalog_key)
    if catalog is None:
        print(f"ERROR: catalog.json not found at {catalog_key}", file=sys.stderr)
        sys.exit(1)

    # ── Step 2: Build nova_id → primary_name mapping ─────────────────
    novae = catalog.get("novae", [])
    id_to_name: dict[str, str] = {}
    name_to_id: dict[str, str] = {}
    for nova in novae:
        nova_id = nova["nova_id"]
        name = nova["primary_name"]
        id_to_name[nova_id] = name
        name_to_id[name] = nova_id

    print(f"Catalog contains {len(novae)} novae.")

    # ── Step 3: Determine which novae to pull ────────────────────────
    if args.novae:
        target_names = []
        for requested in args.novae:
            if requested in name_to_id:
                target_names.append(requested)
            else:
                print(
                    f"  WARNING: '{requested}' not found in catalog. "
                    f"Available: {', '.join(sorted(name_to_id.keys()))}",
                )
        if not target_names:
            print("ERROR: No valid novae to pull.", file=sys.stderr)
            sys.exit(1)
    else:
        target_names = sorted(name_to_id.keys())

    print(f"Pulling artifacts for {len(target_names)} novae: {', '.join(target_names)}")

    if args.dry_run:
        print("\n── Dry run ──")
        print(f"Would write catalog.json to {_OUTPUT_DIR / 'catalog.json'}")
        for name in target_names:
            nova_dir = _OUTPUT_DIR / "nova" / name
            for artifact in _NOVA_ARTIFACTS:
                print(f"Would write {nova_dir / artifact}")
        print("\nDry run complete — nothing written.")
        return

    # ── Step 4: Prepare output directory ─────────────────────────────
    if args.clean and _OUTPUT_DIR.exists():
        import shutil

        print(f"Cleaning {_OUTPUT_DIR}...")
        shutil.rmtree(_OUTPUT_DIR)

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Step 5: Write catalog.json ───────────────────────────────────
    catalog_path = _OUTPUT_DIR / "catalog.json"
    catalog_path.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
    print(f"  ✓ {catalog_path.relative_to(_REPO_ROOT)}")

    # ── Step 6: Download per-nova artifacts ──────────────────────────
    total_downloaded = 0
    total_skipped = 0

    for name in target_names:
        nova_id = name_to_id[name]
        nova_dir = _OUTPUT_DIR / "nova" / name
        nova_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n{name} ({nova_id}):")

        for artifact in _NOVA_ARTIFACTS:
            s3_key = f"releases/{release_id}/nova/{nova_id}/{artifact}"

            if artifact.endswith(".json"):
                data = _download_json(s3, bucket, s3_key)
                if data is not None:
                    out_path = nova_dir / artifact
                    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
                    print(f"  ✓ {artifact}")
                    total_downloaded += 1
                else:
                    print(f"  – {artifact} (not found)")
                    total_skipped += 1
            else:
                # Binary artifacts (SVG, etc.)
                raw = _download_bytes(s3, bucket, s3_key)
                if raw is not None:
                    out_path = nova_dir / artifact
                    out_path.write_bytes(raw)
                    print(f"  ✓ {artifact}")
                    total_downloaded += 1
                else:
                    print(f"  – {artifact} (not found)")
                    total_skipped += 1

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\nDone. {total_downloaded} artifacts downloaded, {total_skipped} not found.")
    print(f"Local dev data at: {_OUTPUT_DIR.relative_to(_REPO_ROOT)}/")
    print("\nRun 'cd frontend && npm run dev' — artifacts will be served from /data/.")


if __name__ == "__main__":
    main()
