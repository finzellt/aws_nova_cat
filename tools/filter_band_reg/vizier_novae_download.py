#!/usr/bin/env python3
"""
Search VizieR for catalogs matching keywords, then download their data
in formats that preserve the original metadata and structure.

Each catalog's tables are saved in two forms:
  - VOTable XML (.vot) — the native VizieR interchange format, preserving
    all column metadata (UCDs, utypes, units, descriptions, data types)
  - A JSON sidecar (.meta.json) — the raw Astropy table.meta dict, which
    captures everything astroquery was able to parse from the response

A catalog_manifest.csv is also written summarizing every downloaded table
(catalog name, table name, title, description, number of rows/columns).

Filename convention
-------------------
  Catalog slashes become underscores; all other characters (including '+')
  are preserved.  The 0-based table index follows a double-underscore:

    J/A+A/452/567  table 0  →  J_A+A_452_567__0.vot
    J/AJ/153/238   table 2  →  J_AJ_153_238__2.vot
    B/cb           table 1  →  B_cb__1.vot

  To reverse: split on "__" for the table index, then replace "_" with "/"
  in the catalog portion.  The "+" in journal names like "A+A" survives the
  round-trip because it is never replaced.

Usage:

python vizier_novae_download.py \
    --from-manifest ./vizier_data/catalog_manifest.csv \
    --outdir ./vizier_data

    # Search + download all matches (default keywords: Novae Optical)
    python vizier_novae_download.py

    # Search only (no download)
    python vizier_novae_download.py --search-only

    # Download specific catalogs by name (skip search)
    python vizier_novae_download.py --catalogs J/A+A/612/A37 J/MNRAS/789/012

    # Re-download from an existing manifest (fixes truncated tables)
    python vizier_novae_download.py --from-manifest ./vizier_data/catalog_manifest.csv

    # Verify existing downloads — report which tables look truncated
    python vizier_novae_download.py --verify ./vizier_data

    # Custom keywords
    python vizier_novae_download.py --keywords Novae photometry

    # Custom output directory
    python vizier_novae_download.py --outdir ./vizier_data

    # Cap rows per table (default: unlimited)
    python vizier_novae_download.py --row-limit 10000
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from astropy.io.votable import from_table, parse_single_table
from astropy.io.votable import writeto as write_votable
from astroquery.vizier import Vizier

# ============================================================
# Common row-count thresholds that indicate truncation.
# astroquery defaults to 50; VizieR web defaults to 200.
# ============================================================

SUSPECT_ROW_COUNTS = {50, 100, 200, 500, 1000}


def search_catalogs(
    keywords: list[str],
    max_catalogs: int = 500,
) -> dict[str, str]:
    """
    Search VizieR for catalogs matching all given keywords.

    Returns
    -------
    dict[str, str]
        Mapping of catalog name -> title.
    """
    print(f"Searching VizieR for catalogs matching: {keywords}")
    catalog_dict = Vizier.find_catalogs(keywords, max_catalogs=max_catalogs)

    results = {}
    for cat_name, cat_info in catalog_dict.items():
        title = getattr(cat_info, "title", str(cat_info))
        results[cat_name] = title

    return results


def print_catalog_list(catalogs: dict[str, str]) -> None:
    """Print a numbered summary of found catalogs."""
    print(f"\nFound {len(catalogs)} catalog(s):\n")
    print(f"{'#':<4} {'Catalog':<25} {'Title'}")
    print("-" * 90)
    for i, (name, title) in enumerate(catalogs.items(), 1):
        title_display = title[:58] + "..." if len(title) > 61 else title
        print(f"{i:<4} {name:<25} {title_display}")


def sanitize_filename(catalog_name: str) -> str:
    """
    Turn a VizieR catalog name into a safe filename base.

    ONLY replaces slashes with underscores.  All other characters
    (including '+' in journal names like 'A+A') are preserved so the
    mapping is reversible:

        J/A+A/452/567  →  J_A+A_452_567
        reverse:           J_A+A_452_567.replace("_", "/")  →  J/A+A/452/567

    This is critical for build_metadata_table.py's parse_votable_filename()
    to correctly reconstruct the catalog name from the filename.
    """
    return catalog_name.replace("/", "_")


def make_meta_serializable(meta: dict) -> dict:
    """
    Best-effort conversion of an Astropy table.meta dict to something
    JSON-serializable. Anything that can't be serialized is cast to str.
    """
    out = {}
    for k, v in meta.items():
        try:
            json.dumps(v)
            out[k] = v
        except (TypeError, ValueError):
            out[k] = str(v)
    return out


def download_catalogs(
    catalog_names: list[str],
    outdir: Path,
    row_limit: int = -1,
) -> list[dict]:
    """
    Download full table data for the given VizieR catalogs.

    Each table is saved as:
      - {name}__{idx}.vot       — VOTable XML (preserves all column metadata)
      - {name}__{idx}.meta.json — raw table.meta dict

    Parameters
    ----------
    catalog_names : list[str]
        VizieR catalog identifiers (e.g. "J/A+A/612/A37").
    outdir : Path
        Directory to write files into.
    row_limit : int
        Maximum rows per table. -1 for unlimited.

    Returns
    -------
    list[dict]
        Manifest entries for every downloaded table.
    """
    outdir.mkdir(parents=True, exist_ok=True)

    v = Vizier(row_limit=row_limit)

    manifest = []

    for cat_name in catalog_names:
        print(f"\nDownloading: {cat_name}")
        try:
            table_list = v.get_catalogs(cat_name)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        if not table_list:
            print("  No tables returned.")
            continue

        print(f"  Got {len(table_list)} table(s)")

        safe_name = sanitize_filename(cat_name)
        for idx, table in enumerate(table_list):
            base = f"{safe_name}__{idx}"
            vot_path = outdir / f"{base}.vot"
            meta_path = outdir / f"{base}.meta.json"

            # --- Write VOTable XML (preserves UCDs, units, descriptions) ---
            votable = from_table(table)
            write_votable(votable, str(vot_path))

            # --- Write raw table.meta as JSON sidecar ---
            meta_serializable = make_meta_serializable(table.meta)
            meta_path.write_text(json.dumps(meta_serializable, indent=2, ensure_ascii=False))

            nrows = len(table)
            ncols = len(table.colnames)
            table_name = table.meta.get("name", "")
            description = table.meta.get("description", "")

            manifest.append(
                {
                    "catalog": cat_name,
                    "table_index": idx,
                    "table_name": table_name,
                    "description": description,
                    "rows": nrows,
                    "columns": ncols,
                    "column_names": ";".join(table.colnames),
                    "vot_file": f"{base}.vot",
                    "meta_file": f"{base}.meta.json",
                }
            )

            print(f"  [{idx}] {table_name or '?'}: {nrows} rows × {ncols} cols → {base}.vot")

    return manifest


def write_manifest(manifest: list[dict], outdir: Path) -> None:
    """Write catalog_manifest.csv summarizing all downloaded tables."""
    if not manifest:
        return

    manifest_path = outdir / "catalog_manifest.csv"
    fieldnames = list(manifest[0].keys())

    with open(manifest_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(manifest)

    print(f"\nManifest written to {manifest_path}")


# ============================================================
# Re-download from existing manifest
# ============================================================


def load_manifest(manifest_path: Path) -> list[dict]:
    """Load an existing catalog_manifest.csv."""
    rows = []
    with open(manifest_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows


def redownload_from_manifest(
    manifest_path: Path,
    outdir: Path,
    row_limit: int = -1,
) -> list[dict]:
    """
    Re-download all catalogs listed in an existing manifest.

    Extracts the unique catalog names from the manifest and downloads
    them fresh with the specified row_limit (default: unlimited).
    Existing files in outdir are overwritten.
    """
    old_manifest = load_manifest(manifest_path)
    catalog_names = list(dict.fromkeys(row["catalog"] for row in old_manifest))

    print(f"Re-downloading {len(catalog_names)} catalog(s) from manifest ...")
    print(f"  (row_limit={row_limit})\n")

    # Show before/after comparison for each table
    old_rows_by_file: dict[str, int] = {}
    for row in old_manifest:
        vot_file = row.get("vot_file", "")
        try:
            old_rows_by_file[vot_file] = int(row.get("rows", 0))
        except (ValueError, TypeError):
            pass

    new_manifest = download_catalogs(catalog_names, outdir, row_limit)

    # Print comparison
    print("\n" + "=" * 70)
    print("Before/after row counts:")
    print(f"  {'File':<40} {'Old':>8} {'New':>8} {'Delta':>8}")
    print("  " + "-" * 66)

    changed = 0
    for entry in new_manifest:
        vot_file = entry["vot_file"]
        new_rows = entry["rows"]
        old_rows = old_rows_by_file.get(vot_file)

        if old_rows is not None:
            delta = new_rows - old_rows
            marker = " ***" if delta != 0 else ""
            if delta != 0:
                changed += 1
            print(f"  {vot_file:<40} {old_rows:>8} {new_rows:>8} {delta:>+8}{marker}")
        else:
            print(f"  {vot_file:<40} {'new':>8} {new_rows:>8}")

    print(f"\n  {changed} table(s) had different row counts.")

    return new_manifest


# ============================================================
# Verify existing downloads for truncation
# ============================================================


def verify_downloads(votable_dir: Path) -> None:
    """
    Scan a directory of .vot files and flag tables that may be truncated.

    A table is flagged as suspect if its row count exactly matches a common
    default limit (50, 100, 200, 500, 1000).
    """
    vot_files = sorted(votable_dir.glob("*.vot"))

    if not vot_files:
        print(f"No .vot files found in {votable_dir}")
        return

    print(f"Verifying {len(vot_files)} VOTable files in {votable_dir}\n")
    print(f"  {'File':<45} {'Rows':>8} {'Status'}")
    print("  " + "-" * 70)

    suspect = []
    ok = 0
    errors = 0

    for vot_path in vot_files:
        try:
            table = parse_single_table(vot_path)
            nrows = len(table.array) if table.array is not None else 0

            if nrows in SUSPECT_ROW_COUNTS:
                status = f"SUSPECT (exactly {nrows} — possible truncation)"
                suspect.append((vot_path.name, nrows))
            else:
                status = "ok"
                ok += 1

            print(f"  {vot_path.name:<45} {nrows:>8} {status}")

        except Exception as exc:
            errors += 1
            print(f"  {vot_path.name:<45} {'ERROR':>8} {exc}")

    print(f"\n  OK: {ok}  |  Suspect: {len(suspect)}  |  Errors: {errors}")

    if suspect:
        print("\n  Suspect tables (row count matches a common default limit):")
        for name, nrows in suspect:
            print(f"    {name}: {nrows} rows")
        print(
            f"\n  These tables may have been downloaded with a row limit."
            f"\n  Re-download with: python {sys.argv[0]} "
            f"--from-manifest {votable_dir / 'catalog_manifest.csv'}"
        )


# ============================================================
# CLI
# ============================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Search VizieR for catalogs by keyword and download them."
    )
    parser.add_argument(
        "--keywords",
        "-k",
        nargs="+",
        default=["Novae", "Optical"],
        help='Keywords to search (default: "Novae" "Optical")',
    )
    parser.add_argument(
        "--catalogs",
        "-c",
        nargs="+",
        default=None,
        help="Skip search; download these specific catalog names directly.",
    )
    parser.add_argument(
        "--from-manifest",
        type=Path,
        default=None,
        metavar="CSV",
        help=(
            "Re-download all catalogs listed in an existing manifest CSV. "
            "Use this to fix truncated tables by re-fetching with row_limit=-1."
        ),
    )
    parser.add_argument(
        "--verify",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "Verify existing .vot files in DIR for possible truncation. Does not download anything."
        ),
    )
    parser.add_argument(
        "--search-only",
        action="store_true",
        help="Search and list catalogs without downloading.",
    )
    parser.add_argument(
        "--outdir",
        "-o",
        type=Path,
        default=Path("./vizier_downloads"),
        help="Output directory for downloads (default: ./vizier_downloads)",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=-1,
        help="Max rows per table (-1 = unlimited, default: -1)",
    )
    parser.add_argument(
        "--max-catalogs",
        "--max",
        type=int,
        default=500,
        help="Max catalogs returned by search (default: 500)",
    )
    args = parser.parse_args()

    # --- Verify mode (no downloads) ---
    if args.verify:
        verify_downloads(args.verify)
        return

    # --- Re-download from manifest mode ---
    if args.from_manifest:
        if not args.from_manifest.exists():
            print(f"Error: manifest not found: {args.from_manifest}")
            sys.exit(1)
        manifest = redownload_from_manifest(args.from_manifest, args.outdir, args.row_limit)
        write_manifest(manifest, args.outdir)
        return

    # --- Direct download mode (skip search) ---
    if args.catalogs:
        print(f"Downloading {len(args.catalogs)} specified catalog(s)...")
        manifest = download_catalogs(args.catalogs, args.outdir, args.row_limit)
        write_manifest(manifest, args.outdir)
        return

    # --- Search mode ---
    catalogs = search_catalogs(args.keywords, max_catalogs=args.max_catalogs)

    if not catalogs:
        print("No catalogs found matching the given keywords.")
        sys.exit(0)

    print_catalog_list(catalogs)

    if args.search_only:
        return

    # --- Download all search results ---
    print(f"\nDownloading all {len(catalogs)} catalog(s)...")
    manifest = download_catalogs(list(catalogs.keys()), args.outdir, args.row_limit)
    write_manifest(manifest, args.outdir)


if __name__ == "__main__":
    main()
