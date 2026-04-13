#!/usr/bin/env python3
"""
Convert Strope, Schaefer & Henden (2010) VOTable V-band light curves into
NovaCat ticket + CSV pairs for batch ingestion.

Source: J/AJ/140/34/table2 — "Binned light curves for 92 novae"
        Strope, R. J., Schaefer, B. E. & Henden, A. A. 2010, AJ, 140, 34

Input:  J_AJ_140_34__1.vot  (VOTable, 11843 rows across 92 novae)
Output: One directory per nova containing:
          {nova_slug}_Strope2010_optical_photometry.txt   (ticket)
          {nova_slug}_Strope2010_optical_photometry.csv   (headerless data)

VOTable columns used:
  Nova   — Nova name (e.g. "OS And")
  JD     — Julian Date of time bin middle (absolute epoch)
  Vmag   — Binned and averaged V band magnitude
  e_Vmag — 1σ error in Vmag

Columns NOT ingested:
  T      — Time relative to fiducial (redundant with JD)
  o_Vmag — Number of observations per bin (informational only)

CSV column layout (headerless, 0-indexed):
  0: JD
  1: Vmag
  2: e_Vmag
  3: Filter string ("V")
  4: Upper limit flag (always "0" — binned data has no non-detections)

Personal operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

from astropy.io.votable import parse as parse_votable


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BIBCODE = "2010AJ....140...34S"
REFERENCE = "Strope, Schaefer & Henden (2010)"

# Template with error column (col 2 = error, col 3 = filter, col 4 = upper limit)
TICKET_TEMPLATE_WITH_ERR = """\
OBJECT NAME: {object_name}
WAVELENGTH REGIME: optical
TIME SYSTEM: JD
TIME UNITS: days
FLUX UNITS: mags
FLUX ERROR UNITS: mags
FILTER SYSTEM: Johnson
MAGNITUDE SYSTEM: Vega
TELESCOPE: NA
OBSERVER: NA
REFERENCE: {reference}
BIBCODE: {bibcode}
ASSUMED DATE OF OUTBURST: NA
DATA FILENAME: {data_filename}
TIME COLUMN NUMBER: 0
FLUX COLUMN NUMBER: 1
FLUX ERROR COLUMN NUMBER: 2
FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER: 3
UPPER LIMIT FLAG COLUMN NUMBER: 4
TELESCOPE COLUMN NUMBER: NA
OBSERVER COLUMN NUMBER: NA
FILTER SYSTEM COLUMN NUMBER: NA
TICKET STATUS: Completed
"""

# Template without error column (col 2 = filter, col 3 = upper limit)
TICKET_TEMPLATE_NO_ERR = """\
OBJECT NAME: {object_name}
WAVELENGTH REGIME: optical
TIME SYSTEM: JD
TIME UNITS: days
FLUX UNITS: mags
FLUX ERROR UNITS: mags
FILTER SYSTEM: Johnson
MAGNITUDE SYSTEM: Vega
TELESCOPE: NA
OBSERVER: NA
REFERENCE: {reference}
BIBCODE: {bibcode}
ASSUMED DATE OF OUTBURST: NA
DATA FILENAME: {data_filename}
TIME COLUMN NUMBER: 0
FLUX COLUMN NUMBER: 1
FLUX ERROR COLUMN NUMBER: NA
FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER: 2
UPPER LIMIT FLAG COLUMN NUMBER: 3
TELESCOPE COLUMN NUMBER: NA
OBSERVER COLUMN NUMBER: NA
FILTER SYSTEM COLUMN NUMBER: NA
TICKET STATUS: Completed
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def nova_slug(name: str) -> str:
    """Convert a nova name to a filesystem-safe slug.

    'OS And' → 'OS_And'
    """
    return name.strip().replace(" ", "_")


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------


def group_rows(table):
    """Group VOTable rows by nova name.

    Returns dict mapping (slug, object_name) → list of row dicts.
    Masked/NaN e_Vmag values are stored as None.
    """
    import numpy as np

    groups = defaultdict(list)

    for row in table:
        name = str(row["Nova"]).strip()
        jd = float(row["JD"])
        vmag = float(row["Vmag"])

        # Handle masked/NaN errors (e.g. GK Per — 1901 visual estimates)
        e_vmag_raw = row["e_Vmag"]
        if np.ma.is_masked(e_vmag_raw) or np.isnan(float(e_vmag_raw)):
            e_vmag = None
        else:
            e_vmag = float(e_vmag_raw)

        slug = nova_slug(name)
        obj_name = name.replace(" ", "_")
        key = (slug, obj_name)

        groups[key].append(
            {
                "jd": jd,
                "vmag": vmag,
                "e_vmag": e_vmag,
            }
        )

    return groups


# ---------------------------------------------------------------------------
# Output generation
# ---------------------------------------------------------------------------


def write_ticket_and_csv(
    output_dir: Path,
    slug: str,
    object_name: str,
    rows: list[dict],
) -> tuple[Path, Path]:
    """Write a ticket .txt and headerless CSV for one nova.

    Returns (ticket_path, csv_path).
    """
    nova_dir = output_dir / slug
    nova_dir.mkdir(parents=True, exist_ok=True)

    base = f"{slug}_Strope2010_optical_photometry"
    csv_filename = f"{base}.csv"
    ticket_filename = f"{base}.txt"

    csv_path = nova_dir / csv_filename
    ticket_path = nova_dir / ticket_filename

    # --- Determine if this nova has error data ---
    has_errors = any(r["e_vmag"] is not None for r in rows)

    # --- Write CSV (headerless, sorted by JD) ---
    rows_sorted = sorted(rows, key=lambda r: r["jd"])

    with open(csv_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        for r in rows_sorted:
            if has_errors:
                # 5 columns: JD, Vmag, e_Vmag, filter, upper_limit
                writer.writerow(
                    [
                        f"{r['jd']:.6f}",
                        f"{r['vmag']:.4f}",
                        f"{r['e_vmag']:.4f}" if r["e_vmag"] is not None else "",
                        "V",
                        "0",
                    ]
                )
            else:
                # 4 columns: JD, Vmag, filter, upper_limit (no error column)
                writer.writerow(
                    [
                        f"{r['jd']:.6f}",
                        f"{r['vmag']:.4f}",
                        "V",
                        "0",
                    ]
                )

    # --- Write ticket ---
    template = TICKET_TEMPLATE_WITH_ERR if has_errors else TICKET_TEMPLATE_NO_ERR
    ticket_text = template.format(
        object_name=object_name,
        reference=REFERENCE,
        bibcode=BIBCODE,
        data_filename=csv_filename,
    )
    ticket_path.write_text(ticket_text)

    return ticket_path, csv_path


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def print_summary(groups: dict, output_dir: Path) -> None:
    total_rows = 0

    print(f"\n{'─' * 70}")
    print(f"  Output directory: {output_dir}")
    print(f"{'─' * 70}")
    print(f"  {'Nova':25s} {'Rows':>6s} {'V_peak':>7s} {'JD_min':>14s} {'JD_max':>14s}")
    print(f"  {'─' * 25} {'─' * 6} {'─' * 7} {'─' * 14} {'─' * 14}")

    for (slug, obj_name), rows in sorted(groups.items()):
        n = len(rows)
        total_rows += n
        v_peak = min(r["vmag"] for r in rows)
        jd_min = min(r["jd"] for r in rows)
        jd_max = max(r["jd"] for r in rows)
        print(f"  {slug:25s} {n:6d} {v_peak:7.1f} {jd_min:14.1f} {jd_max:14.1f}")

    print(f"  {'─' * 25} {'─' * 6}")
    print(f"  {'TOTAL':25s} {total_rows:6d}")
    print(f"\n  Tickets generated: {len(groups)}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert Strope+2010 VOTable to NovaCat ticket+CSV pairs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "votable",
        type=Path,
        help="Path to the J_AJ_140_34__1.vot file",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("data/optical_strope2010"),
        help="Output directory (default: data/optical_strope2010)",
    )
    parser.add_argument(
        "--nova",
        type=str,
        default=None,
        help="Generate only for this nova slug (e.g. V1500_Cyg)",
    )
    args = parser.parse_args()

    if not args.votable.exists():
        print(f"ERROR: VOTable not found: {args.votable}", file=sys.stderr)
        sys.exit(1)

    # --- Parse VOTable ---
    print(f"  Parsing {args.votable} ...")
    vot = parse_votable(str(args.votable))
    table = vot.get_first_table().to_table()
    names = set(str(n).strip() for n in table["Nova"])
    print(f"  {len(table)} rows, {len(names)} novae")

    # --- Group ---
    groups = group_rows(table)

    # --- Filter ---
    if args.nova:
        filtered = {k: v for k, v in groups.items() if k[0] == args.nova}
        if not filtered:
            available = sorted(k[0] for k in groups)
            print(f"ERROR: Nova '{args.nova}' not found. Available:", file=sys.stderr)
            for s in available:
                print(f"  {s}", file=sys.stderr)
            sys.exit(1)
        groups = filtered

    # --- Generate ---
    args.output.mkdir(parents=True, exist_ok=True)

    for (slug, obj_name), rows in sorted(groups.items()):
        ticket_path, csv_path = write_ticket_and_csv(args.output, slug, obj_name, rows)
        print(f"  ✓ {slug:25s}  {len(rows):5d} rows  → {ticket_path.name}")

    # --- Summary ---
    print_summary(groups, args.output)
    print(f"  Next step:")
    print(f"    python tools/batch_ingest.py try-parse {args.output}")
    print(f"    python tools/batch_ingest.py tickets {args.output} --dry-run")
    print()


if __name__ == "__main__":
    main()
