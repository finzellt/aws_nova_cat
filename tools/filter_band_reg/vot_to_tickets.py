#!/usr/bin/env python3
"""
Convert Chomiuk et al. (2021) VOTable radio observations into NovaCat
ticket + CSV pairs for batch ingestion.

Source: J/ApJS/257/49/table3 — "Radio observations of classical novae"
        Chomiuk, L. et al. 2021, ApJS, 257, 49

Input:  J_ApJS_257_49__2.vot  (VOTable, 1935 rows across 31 novae)
Output: One directory per nova containing:
          {nova_slug}_Chomiuk2021_radio_photometry.txt   (ticket)
          {nova_slug}_Chomiuk2021_radio_photometry.csv   (headerless data)

U Sco (recurrent nova) is split by outburst year (m_Name column):
  U_Sco_1987/, U_Sco_1999/, U_Sco_2010/

VOTable columns used:
  Name    — Nova identifier (e.g. "V1370 Aql")
  m_Name  — Outburst year for U Sco only (-32768 = sentinel for N/A)
  MJD     — Modified Julian Date
  Freq    — Observation frequency in GHz
  l_Flux  — Upper limit flag: "<" = 3σ upper limit, "" = detection
  Flux    — Flux density in mJy
  e_Flux  — 1σ uncertainty in mJy
  f_Flux  — Footnote flags from the paper (b,c,d,e,f,g,h,i)
  Config  — Telescope/array configuration (VLA/A, ATCA/6A, WSRT, etc.)

CSV column layout (headerless, 0-indexed):
  0: MJD
  1: Flux (mJy)
  2: e_Flux (mJy)
  3: Frequency string (e.g. "5.00 GHz") — for radio band resolution
  4: Upper limit flag (0 = detection, 1 = upper limit)
  5: Telescope/config string

Personal operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
from astropy.io.votable import parse as parse_votable


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BIBCODE = "2021ApJS..257...49C"
REFERENCE = "Chomiuk et al. (2021)"
USCO_SENTINEL = -32768  # VOTable null for m_Name (short)

# f_Flux footnote meanings from the paper (Table 3 notes).
# Stored as notes in a sidecar file for reference; not ingested directly.
FFLUX_NOTES = {
    "b": "Previously published data from the literature",
    "c": "Observation from a non-standard frequency setup",
    "d": "Possibly contaminated by a nearby source",
    "e": "Marginal detection",
    "f": "Likely non-thermal emission",
    "g": "Extended emission detected",
    "h": "Multiple components resolved",
    "i": "Time-variable within the observation",
}

# Ticket template.  Uses str.format() with named placeholders.
TICKET_TEMPLATE = """\
OBJECT NAME: {object_name}
WAVELENGTH REGIME: radio
TIME SYSTEM: MJD
TIME UNITS: days
FLUX UNITS: mJy
FLUX ERROR UNITS: mJy
FILTER SYSTEM: NA
MAGNITUDE SYSTEM: NA
TELESCOPE: NA
OBSERVER: NA
REFERENCE: {reference}
BIBCODE: {bibcode}
ASSUMED DATE OF OUTBURST: {assumed_outburst_date}
DATA FILENAME: {data_filename}
TIME COLUMN NUMBER: 0
FLUX COLUMN NUMBER: 1
FLUX ERROR COLUMN NUMBER: 2
FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER: 3
UPPER LIMIT FLAG COLUMN NUMBER: 4
TELESCOPE COLUMN NUMBER: 5
OBSERVER COLUMN NUMBER: NA
FILTER SYSTEM COLUMN NUMBER: NA
TICKET STATUS: Completed
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def nova_slug(name: str, outburst_year: int | None = None) -> str:
    """Convert a nova name to a filesystem-safe slug.

    'V1370 Aql' → 'V1370_Aql'
    'U Sco' with year 1999 → 'U_Sco_1999'
    """
    slug = name.strip().replace(" ", "_")
    if outburst_year is not None:
        slug = f"{slug}_{outburst_year}"
    return slug


def format_freq(freq_ghz: float) -> str:
    """Format a frequency value as a string for the filter column.

    The photometry reader's resolve_radio_frequency() expects strings
    like '5.00 GHz' and fuzzy-matches to the band registry within
    ±20% tolerance.
    """
    return f"{freq_ghz:.2f} GHz"


def upper_limit_flag(l_flux: str) -> str:
    """Convert VOTable l_Flux to ticket upper-limit flag.

    '<' → '1' (3σ upper limit)
    ''  → '0' (detection)
    """
    return "1" if l_flux.strip() == "<" else "0"


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------


def group_rows(table):
    """Group VOTable rows by (nova_slug, object_name, outburst_year).

    Returns dict mapping (slug, object_name, outburst_year|None) → list of row dicts.
    """
    groups = defaultdict(list)

    for row in table:
        name = str(row["Name"]).strip()
        m_name = int(row["m_Name"]) if row["m_Name"] != USCO_SENTINEL else None
        mjd = float(row["MJD"])
        freq = float(row["Freq"])
        flux = float(row["Flux"])
        e_flux = float(row["e_Flux"])
        l_flux = str(row["l_Flux"]).strip()
        f_flux = str(row["f_Flux"]).strip()
        config = str(row["Config"]).strip()

        # U Sco is the only nova with m_Name (outburst year)
        outburst_year = m_name if name == "U Sco" else None
        slug = nova_slug(name, outburst_year)

        # Object name for the ticket: use underscore-separated form,
        # matching the convention in existing tickets.
        # For recurrent novae (U Sco), the object name stays as the
        # base name — all outbursts resolve to the same nova_id.
        # Only the slug/filename includes the outburst year.
        obj_name = name.replace(" ", "_")

        key = (slug, obj_name, outburst_year)
        groups[key].append(
            {
                "mjd": mjd,
                "flux": flux,
                "e_flux": e_flux,
                "freq_ghz": freq,
                "l_flux": l_flux,
                "f_flux": f_flux,
                "config": config,
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
    """Write a ticket .txt and headerless CSV for one nova group.

    Returns (ticket_path, csv_path).
    """
    nova_dir = output_dir / slug
    nova_dir.mkdir(parents=True, exist_ok=True)

    base = f"{slug}_Chomiuk2021_radio_photometry"
    csv_filename = f"{base}.csv"
    ticket_filename = f"{base}.txt"

    csv_path = nova_dir / csv_filename
    ticket_path = nova_dir / ticket_filename

    # --- Write CSV (headerless) ---
    # Sort by MJD for readability
    rows_sorted = sorted(rows, key=lambda r: r["mjd"])

    with open(csv_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        for r in rows_sorted:
            writer.writerow(
                [
                    f"{r['mjd']:.4f}",           # col 0: MJD
                    f"{r['flux']:.4f}",           # col 1: flux (mJy)
                    f"{r['e_flux']:.4f}",         # col 2: flux error (mJy)
                    format_freq(r["freq_ghz"]),   # col 3: frequency string
                    upper_limit_flag(r["l_flux"]),  # col 4: upper limit (0/1)
                    r["config"],                  # col 5: telescope/config
                ]
            )

    # --- Write ticket ---
    ticket_text = TICKET_TEMPLATE.format(
        object_name=object_name,
        reference=REFERENCE,
        bibcode=BIBCODE,
        assumed_outburst_date="NA",
        data_filename=csv_filename,
    )
    ticket_path.write_text(ticket_text)

    return ticket_path, csv_path


def write_footnotes_sidecar(output_dir: Path, groups: dict) -> Path:
    """Write a sidecar file documenting f_Flux footnote flags per nova.

    This is informational only — not ingested. Helps the operator
    understand which rows have special notes from the original paper.
    """
    sidecar_path = output_dir / "_footnotes_reference.txt"
    lines = [
        "f_Flux footnote key (from Chomiuk et al. 2021, Table 3):",
        "=" * 60,
    ]
    for code, desc in sorted(FFLUX_NOTES.items()):
        lines.append(f"  {code} = {desc}")
    lines.append("")
    lines.append("Novae with flagged rows:")
    lines.append("-" * 60)

    for (slug, obj_name, _), rows in sorted(groups.items()):
        flagged = [r for r in rows if r["f_flux"]]
        if flagged:
            flag_counts = defaultdict(int)
            for r in flagged:
                for flag in r["f_flux"].split(","):
                    flag_counts[flag.strip()] += 1
            summary = ", ".join(f"{k}:{v}" for k, v in sorted(flag_counts.items()))
            lines.append(f"  {slug:25s}  {len(flagged):4d}/{len(rows):4d} rows  flags: {summary}")

    sidecar_path.write_text("\n".join(lines) + "\n")
    return sidecar_path


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def print_summary(groups: dict, output_dir: Path) -> None:
    """Print a human-readable summary of what was generated."""
    total_rows = 0
    total_upper = 0

    print(f"\n{'─' * 70}")
    print(f"  Output directory: {output_dir}")
    print(f"{'─' * 70}")
    print(f"  {'Nova':30s} {'Rows':>6s} {'Upper':>6s} {'Freqs':>6s}")
    print(f"  {'─' * 30} {'─' * 6} {'─' * 6} {'─' * 6}")

    for (slug, obj_name, _), rows in sorted(groups.items()):
        n_rows = len(rows)
        n_upper = sum(1 for r in rows if r["l_flux"] == "<")
        n_freqs = len(set(r["freq_ghz"] for r in rows))
        total_rows += n_rows
        total_upper += n_upper
        print(f"  {slug:30s} {n_rows:6d} {n_upper:6d} {n_freqs:6d}")

    print(f"  {'─' * 30} {'─' * 6} {'─' * 6}")
    print(f"  {'TOTAL':30s} {total_rows:6d} {total_upper:6d}")
    print(f"\n  Tickets generated: {len(groups)}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert Chomiuk+2021 VOTable to NovaCat ticket+CSV pairs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "votable",
        type=Path,
        help="Path to the J_ApJS_257_49__2.vot file",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("data/radio_chomiuk2021"),
        help="Output directory (default: data/radio_chomiuk2021)",
    )
    parser.add_argument(
        "--nova",
        type=str,
        default=None,
        help="Generate only for this nova slug (e.g. V1370_Aql)",
    )
    args = parser.parse_args()

    if not args.votable.exists():
        print(f"ERROR: VOTable not found: {args.votable}", file=sys.stderr)
        sys.exit(1)

    # --- Parse VOTable ---
    print(f"  Parsing {args.votable} ...")
    vot = parse_votable(str(args.votable))
    table = vot.get_first_table().to_table()
    print(f"  {len(table)} rows, {len(set(str(r['Name']).strip() for r in table))} novae")

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

    for (slug, obj_name, _), rows in sorted(groups.items()):
        ticket_path, csv_path = write_ticket_and_csv(args.output, slug, obj_name, rows)
        print(f"  ✓ {slug:30s}  {len(rows):4d} rows  → {ticket_path.name}")

    # --- Footnotes sidecar ---
    sidecar = write_footnotes_sidecar(args.output, groups)

    # --- Summary ---
    print_summary(groups, args.output)
    print(f"  Footnotes reference: {sidecar}")
    print(f"\n  Next step:")
    print(f"    python tools/batch_ingest.py try-parse {args.output}")
    print(f"    python tools/batch_ingest.py tickets {args.output} --dry-run")
    print()


if __name__ == "__main__":
    main()
