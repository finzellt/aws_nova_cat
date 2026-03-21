#!/usr/bin/env python3
"""
Quick diagnostic queries against the harvested SVO FPS database.
Run this and paste the output back to Claude.

Usage:
    python svo_diagnostic.py
    python svo_diagnostic.py --db /path/to/svo_fps.db
"""

import argparse
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB_PATH = "svo_fps.db"


def run(db_path: str):
    if not Path(db_path).exists():
        print(f"ERROR: Database not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print("=" * 70)
    print("SVO FPS Database Diagnostic Report")
    print(f"Database: {db_path} ({Path(db_path).stat().st_size / (1024*1024):.1f} MB)")
    print("=" * 70)

    # ----- 1. Basic stats & Band field coverage -----
    print("\n1. BASIC STATS & BAND COVERAGE")
    print("-" * 50)

    row = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN band IS NOT NULL AND band != '' THEN 1 ELSE 0 END) as has_band,
            COUNT(DISTINCT band) as unique_bands
        FROM filters
    """).fetchone()
    print(f"  Total filters:          {row['total']:,}")
    print(f"  With Band label:        {row['has_band']:,}  ({100*row['has_band']/row['total']:.1f}%)")
    print(f"  Without Band label:     {row['total'] - row['has_band']:,}")
    print(f"  Unique Band values:     {row['unique_bands']}")

    print("\n  Top 30 Band labels by frequency:")
    print(f"  {'Band':<20} {'Count':>8}")
    print(f"  {'-'*20} {'-'*8}")
    for r in conn.execute("""
        SELECT band, COUNT(*) as n
        FROM filters
        WHERE band IS NOT NULL AND band != ''
        GROUP BY band
        ORDER BY n DESC
        LIMIT 30
    """):
        print(f"  {r['band']:<20} {r['n']:>8}")

    # ----- 2. Commercial amateur filters -----
    print("\n\n2. COMMERCIAL AMATEUR FILTERS IN DATABASE")
    print("-" * 50)

    results = conn.execute("""
        SELECT filter_id, band, wavelength_eff, fwhm, transmission_count
        FROM filters
        WHERE LOWER(filter_id) LIKE '%astrodon%'
           OR LOWER(filter_id) LIKE '%baader%'
           OR LOWER(filter_id) LIKE '%chroma%'
           OR LOWER(filter_id) LIKE '%optec%'
           OR LOWER(filter_id) LIKE '%schuler%'
           OR LOWER(filter_id) LIKE '%omega%'
           OR LOWER(filter_id) LIKE '%bessell%'
        ORDER BY filter_id
    """).fetchall()

    if results:
        print(f"  Found {len(results)} filters:")
        print(f"  {'Filter ID':<50} {'Band':<8} {'λ_eff':>10} {'FWHM':>10} {'Pts':>5}")
        print(f"  {'-'*50} {'-'*8} {'-'*10} {'-'*10} {'-'*5}")
        for r in results:
            wl = f"{r['wavelength_eff']:.1f}" if r['wavelength_eff'] else "—"
            fw = f"{r['fwhm']:.1f}" if r['fwhm'] else "—"
            bd = r['band'] or "—"
            tc = str(r['transmission_count']) if r['transmission_count'] else "—"
            print(f"  {r['filter_id']:<50} {bd:<8} {wl:>10} {fw:>10} {tc:>5}")
    else:
        print("  None found by name. Trying broader search...")

    # Broader search: look for facility names that sound amateur/commercial
    print("\n  Facilities that might contain amateur/commercial filters:")
    for r in conn.execute("""
        SELECT facility, COUNT(*) as n
        FROM filters
        GROUP BY facility
        ORDER BY facility
    """):
        fac = r['facility'] or ''
        fac_lower = fac.lower()
        if any(kw in fac_lower for kw in [
            'generic', 'generic', 'bessell', 'johnson', 'cousins',
            'stromgren', 'sloan', 'standard', 'kron',
        ]):
            print(f"    {fac:<40} {r['n']:>5} filters")

    # ----- 3. Transmission curve completeness -----
    print("\n\n3. TRANSMISSION CURVE COMPLETENESS")
    print("-" * 50)

    row = conn.execute("""
        SELECT
            SUM(CASE WHEN transmission_count > 0 THEN 1 ELSE 0 END) as with_curves,
            SUM(CASE WHEN transmission_count = 0 OR transmission_count IS NULL THEN 1 ELSE 0 END) as without
        FROM filters
    """).fetchone()
    total = (row['with_curves'] or 0) + (row['without'] or 0)
    wc = row['with_curves'] or 0
    print(f"  With transmission curves:    {wc:,}  ({100*wc/total:.1f}%)")
    print(f"  Without transmission curves:  {row['without'] or 0:,}")

    # Curve point stats
    row2 = conn.execute("""
        SELECT
            MIN(transmission_count) as mn,
            MAX(transmission_count) as mx,
            AVG(transmission_count) as avg,
            SUM(transmission_count) as total_pts
        FROM filters
        WHERE transmission_count > 0
    """).fetchone()
    if row2['mn'] is not None:
        print(f"  Points per curve: min={row2['mn']}, max={row2['mx']}, avg={row2['avg']:.0f}")
        print(f"  Total transmission points:   {row2['total_pts']:,}")

    # ----- 4. Bonus: J/H/K band landscape -----
    print("\n\n4. NEAR-IR BAND LANDSCAPE (J, H, K variants)")
    print("-" * 50)
    print("  These are especially relevant for nova photometry.")
    print(f"\n  {'Band':<10} {'Count':>6}  Sample filter IDs")
    print(f"  {'-'*10} {'-'*6}  {'-'*40}")

    for band_pattern in ['J', 'H', 'K', 'Ks', "K'", 'Kp']:
        results = conn.execute("""
            SELECT band, filter_id
            FROM filters
            WHERE band = ?
            ORDER BY filter_id
        """, (band_pattern,)).fetchall()
        if results:
            samples = [r['filter_id'] for r in results[:3]]
            more = f" (+{len(results)-3} more)" if len(results) > 3 else ""
            print(f"  {band_pattern:<10} {len(results):>6}  {', '.join(samples)}{more}")

    # ----- 5. Bonus: UBVRI landscape -----
    print("\n\n5. OPTICAL BAND LANDSCAPE (UBVRI)")
    print("-" * 50)
    print(f"\n  {'Band':<10} {'Count':>6}  Sample filter IDs")
    print(f"  {'-'*10} {'-'*6}  {'-'*40}")

    for band_pattern in ['U', 'B', 'V', 'R', 'I', 'u', 'g', 'r', 'i', 'z']:
        results = conn.execute("""
            SELECT band, filter_id
            FROM filters
            WHERE band = ?
            ORDER BY filter_id
        """, (band_pattern,)).fetchall()
        if results:
            samples = [r['filter_id'] for r in results[:3]]
            more = f" (+{len(results)-3} more)" if len(results) > 3 else ""
            print(f"  {band_pattern:<10} {len(results):>6}  {', '.join(samples)}{more}")

    # ----- 6. Harvest run info -----
    print("\n\n6. HARVEST METADATA")
    print("-" * 50)
    row = conn.execute("""
        SELECT started_at, finished_at, endpoint_used,
               filters_total, filters_success, filters_failed
        FROM harvest_runs
        ORDER BY run_id DESC LIMIT 1
    """).fetchone()
    if row:
        print(f"  Started:    {row['started_at']}")
        print(f"  Finished:   {row['finished_at']}")
        print(f"  Endpoint:   {row['endpoint_used']}")
        print(f"  Total:      {row['filters_total']}")
        print(f"  Success:    {row['filters_success']}")
        print(f"  Failed:     {row['filters_failed']}")

    print("\n" + "=" * 70)
    print("End of diagnostic report")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="SVO FPS database diagnostic queries")
    p.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite database")
    args = p.parse_args()
    run(args.db)
