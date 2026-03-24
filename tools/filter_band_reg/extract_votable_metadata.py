#!/usr/bin/env python3
"""
Extract field-level metadata from VOTable files into a single CSV.

Usage:
    python extract_votable_metadata.py <vot_dir> [-o OUTPUT]

Where <vot_dir> is the directory containing .vot files.
Output defaults to field_metadata.csv in the current directory.

Requires: astropy
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from astropy.io.votable import parse


def extract_fields(vot_path: Path) -> list[dict[str, str]]:
    """Extract field metadata from every table in a VOTable file."""
    rows = []
    try:
        votable = parse(str(vot_path), verify="warn")
    except Exception as e:
        print(f"  WARNING: Could not parse {vot_path.name}: {e}", file=sys.stderr)
        return rows

    for resource in votable.resources:
        for table in resource.tables:
            table_name = table.name or ""
            n_rows = table.nrows

            for i, field in enumerate(table.fields):
                rows.append(
                    {
                        "vot_file": vot_path.name,
                        "table_name": table_name,
                        "table_rows": str(n_rows),
                        "col_index": str(i),
                        "field_name": field.name or "",
                        "datatype": field.datatype or "",
                        "unit": str(field.unit) if field.unit else "",
                        "ucd": field.ucd or "",
                        "utype": field.utype or "",
                        "description": (field.description or "").strip(),
                    }
                )

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract field-level metadata from VOTable files.")
    parser.add_argument("vot_dir", type=Path, help="Directory containing .vot files")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("field_metadata.csv"),
        help="Output CSV path (default: field_metadata.csv)",
    )
    args = parser.parse_args()

    if not args.vot_dir.is_dir():
        print(f"ERROR: {args.vot_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    vot_files = sorted(args.vot_dir.glob("*.vot"))
    if not vot_files:
        print(f"ERROR: No .vot files found in {args.vot_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(vot_files)} VOTable files in {args.vot_dir}")

    all_rows: list[dict[str, str]] = []
    for vot_path in vot_files:
        print(f"  Processing {vot_path.name}...")
        rows = extract_fields(vot_path)
        all_rows.extend(rows)

    fieldnames = [
        "vot_file",
        "table_name",
        "table_rows",
        "col_index",
        "field_name",
        "datatype",
        "unit",
        "ucd",
        "utype",
        "description",
    ]

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    # Print summary
    tables = {(r["vot_file"], r["table_name"]) for r in all_rows}
    print(f"\nDone. {len(all_rows)} fields across {len(tables)} tables.")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
