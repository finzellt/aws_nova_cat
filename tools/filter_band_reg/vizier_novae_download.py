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

Usage:
    # Search + download all matches (default keywords: Novae Optical)
    python vizier_novae_download.py

    # Search only (no download)
    python vizier_novae_download.py --search-only

    # Download specific catalogs by name (skip search)
    python vizier_novae_download.py --catalogs J/A+A/612/A37 J/MNRAS/789/012

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
import re
import sys
from pathlib import Path

from astropy.io.votable import from_table
from astropy.io.votable import writeto as write_votable
from astroquery.vizier import Vizier


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


def sanitize_filename(name: str) -> str:
    """Turn a VizieR catalog/table name into a safe filename."""
    return re.sub(r"[^\w\-.]", "_", name)


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
