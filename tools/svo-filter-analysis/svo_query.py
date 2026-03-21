#!/usr/bin/env python3
"""
SVO FPS Local Database — Query Utility
=======================================

Companion to svo_harvest.py. Provides a Pythonic interface for querying
the harvested filter database, plus CLI tools for quick lookups.

Usage as a library
------------------
    from svo_query import SVODatabase

    db = SVODatabase("svo_fps.db")

    # Look up a specific filter
    f = db.get_filter("HST/ACS_WFC.F435W")
    print(f["wavelength_eff"], f["fwhm"])

    # Get transmission curve as numpy arrays
    wl, tr = db.get_transmission("HST/ACS_WFC.F435W")

    # Search by wavelength range (Angstroms)
    optical = db.search(wavelength_min=3000, wavelength_max=10000)

    # Search by facility / instrument
    jwst = db.search(facility="JWST")
    nircam = db.search(facility="JWST", instrument="NIRCam")

    # List all facilities
    db.list_facilities()

    # List instruments for a facility
    db.list_instruments("HST")

    # Export a filter to CSV
    db.export_csv("HST/ACS_WFC.F435W", "acs_f435w.csv")

Usage from CLI
--------------
    # List all facilities
    python svo_query.py facilities

    # List instruments for a facility
    python svo_query.py instruments HST

    # List filters matching criteria
    python svo_query.py search --facility JWST --instrument NIRCam

    # Show details for a specific filter
    python svo_query.py info "HST/ACS_WFC.F435W"

    # Export transmission curve to CSV
    python svo_query.py export "HST/ACS_WFC.F435W" -o curve.csv

    # Summary statistics
    python svo_query.py stats

Dependencies
------------
    - sqlite3 (built-in)
    - numpy (optional, for array returns)
    - matplotlib (optional, for plotting)
"""

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

# Optional imports
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


DEFAULT_DB_PATH = "svo_fps.db"


class SVODatabase:
    """Interface to a harvested SVO FPS SQLite database."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        if not Path(db_path).exists():
            raise FileNotFoundError(
                f"Database not found: {db_path}\n"
                f"Run svo_harvest.py first to create it."
            )
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  # Dict-like access

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ------------------------------------------------------------------
    # Single-filter lookups
    # ------------------------------------------------------------------

    def get_filter(self, filter_id: str) -> dict | None:
        """
        Get full metadata for a single filter.
        Returns None if not found.
        """
        cur = self.conn.execute(
            "SELECT * FROM filters WHERE filter_id = ?", (filter_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_transmission(self, filter_id: str):
        """
        Get the transmission curve for a filter.

        Returns:
            If numpy is available: (wavelength_array, transmission_array)
            Otherwise: list of (wavelength, transmission) tuples

        Wavelength is in Angstroms, transmission is dimensionless (0–1 typically).
        """
        cur = self.conn.execute(
            "SELECT wavelength, transmission FROM transmission_curves "
            "WHERE filter_id = ? ORDER BY wavelength",
            (filter_id,),
        )
        rows = cur.fetchall()

        if not rows:
            return (None, None) if HAS_NUMPY else []

        if HAS_NUMPY:
            data = [(r[0], r[1]) for r in rows]
            wl = np.array([d[0] for d in data])
            tr = np.array([d[1] for d in data])
            return wl, tr
        else:
            return [(r[0], r[1]) for r in rows]

    # ------------------------------------------------------------------
    # Search / listing
    # ------------------------------------------------------------------

    def search(
        self,
        facility: str | None = None,
        instrument: str | None = None,
        wavelength_min: float | None = None,
        wavelength_max: float | None = None,
        name_contains: str | None = None,
        mag_sys: str | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """
        Search filters by various criteria. All conditions are AND-ed.

        Args:
            facility:        Exact facility name (case-insensitive)
            instrument:      Exact instrument name (case-insensitive)
            wavelength_min:  Minimum effective wavelength (Angstroms)
            wavelength_max:  Maximum effective wavelength (Angstroms)
            name_contains:   Substring match on filter_id
            mag_sys:         Magnitude system (e.g. "Vega", "AB")
            limit:           Max results (default 500)

        Returns:
            List of filter metadata dicts.
        """
        conditions = []
        params = []

        if facility:
            conditions.append("LOWER(facility) = LOWER(?)")
            params.append(facility)
        if instrument:
            conditions.append("LOWER(instrument) = LOWER(?)")
            params.append(instrument)
        if wavelength_min is not None:
            conditions.append("wavelength_eff >= ?")
            params.append(wavelength_min)
        if wavelength_max is not None:
            conditions.append("wavelength_eff <= ?")
            params.append(wavelength_max)
        if name_contains:
            conditions.append("filter_id LIKE ?")
            params.append(f"%{name_contains}%")
        if mag_sys:
            conditions.append("LOWER(mag_sys) = LOWER(?)")
            params.append(mag_sys)

        where = " AND ".join(conditions) if conditions else "1=1"
        query = (
            f"SELECT * FROM filters WHERE {where} "
            f"ORDER BY wavelength_eff LIMIT ?"
        )
        params.append(limit)

        cur = self.conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

    def list_facilities(self) -> list[tuple[str, int]]:
        """Return list of (facility_name, filter_count) sorted by name."""
        cur = self.conn.execute(
            "SELECT facility, COUNT(*) as cnt FROM filters "
            "GROUP BY facility ORDER BY facility"
        )
        return [(row[0], row[1]) for row in cur.fetchall()]

    def list_instruments(self, facility: str) -> list[tuple[str, int]]:
        """Return list of (instrument_name, filter_count) for a facility."""
        cur = self.conn.execute(
            "SELECT instrument, COUNT(*) as cnt FROM filters "
            "WHERE LOWER(facility) = LOWER(?) "
            "GROUP BY instrument ORDER BY instrument",
            (facility,),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]

    def list_filters(
        self, facility: str | None = None, instrument: str | None = None
    ) -> list[str]:
        """Return list of filter_id strings, optionally filtered."""
        conditions = []
        params = []
        if facility:
            conditions.append("LOWER(facility) = LOWER(?)")
            params.append(facility)
        if instrument:
            conditions.append("LOWER(instrument) = LOWER(?)")
            params.append(instrument)

        where = " AND ".join(conditions) if conditions else "1=1"
        cur = self.conn.execute(
            f"SELECT filter_id FROM filters WHERE {where} ORDER BY filter_id",
            params,
        )
        return [row[0] for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return summary statistics about the database."""
        s = {}
        s["total_filters"] = self.conn.execute(
            "SELECT COUNT(*) FROM filters"
        ).fetchone()[0]
        s["total_facilities"] = self.conn.execute(
            "SELECT COUNT(DISTINCT facility) FROM filters"
        ).fetchone()[0]
        s["total_instruments"] = self.conn.execute(
            "SELECT COUNT(DISTINCT instrument) FROM filters"
        ).fetchone()[0]
        s["filters_with_transmission"] = self.conn.execute(
            "SELECT COUNT(*) FROM filters WHERE transmission_count > 0"
        ).fetchone()[0]
        s["total_transmission_points"] = self.conn.execute(
            "SELECT COUNT(*) FROM transmission_curves"
        ).fetchone()[0]

        row = self.conn.execute(
            "SELECT MIN(wavelength_eff), MAX(wavelength_eff) FROM filters"
        ).fetchone()
        s["wavelength_range_angstrom"] = (row[0], row[1])

        # Top 10 facilities by filter count
        s["top_facilities"] = self.list_facilities()[:10]

        # Harvest info
        row = self.conn.execute(
            "SELECT started_at, finished_at, endpoint_used, "
            "filters_total, filters_success, filters_failed "
            "FROM harvest_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        if row:
            s["last_harvest"] = {
                "started": row[0],
                "finished": row[1],
                "endpoint": row[2],
                "total": row[3],
                "success": row[4],
                "failed": row[5],
            }

        # Database file size
        s["db_size_mb"] = round(Path(self.db_path).stat().st_size / (1024 * 1024), 2)

        return s

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_csv(self, filter_id: str, output_path: str):
        """Export a filter's transmission curve to CSV."""
        cur = self.conn.execute(
            "SELECT wavelength, transmission FROM transmission_curves "
            "WHERE filter_id = ? ORDER BY wavelength",
            (filter_id,),
        )
        rows = cur.fetchall()
        if not rows:
            raise ValueError(f"No transmission data for {filter_id}")

        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["wavelength_angstrom", "transmission"])
            writer.writerows(rows)

    def export_all_metadata_csv(self, output_path: str):
        """Export all filter metadata to a single CSV."""
        cur = self.conn.execute("SELECT * FROM filters ORDER BY filter_id")
        rows = cur.fetchall()
        if not rows:
            raise ValueError("No filters in database")

        keys = [desc[0] for desc in cur.description]
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))

    # ------------------------------------------------------------------
    # Plotting (optional, requires matplotlib)
    # ------------------------------------------------------------------

    def plot_transmission(self, *filter_ids: str, ax=None, show: bool = True):
        """
        Plot transmission curves for one or more filters.
        Requires matplotlib.
        """
        if not HAS_MATPLOTLIB:
            raise ImportError("matplotlib is required for plotting")
        if not HAS_NUMPY:
            raise ImportError("numpy is required for plotting")

        if ax is None:
            fig, ax = plt.subplots(1, 1, figsize=(12, 6))

        for fid in filter_ids:
            wl, tr = self.get_transmission(fid)
            if wl is not None:
                ax.plot(wl, tr, label=fid, linewidth=1.2)

        ax.set_xlabel("Wavelength (Å)")
        ax.set_ylabel("Transmission")
        ax.set_title("Filter Transmission Curves")
        ax.legend(fontsize=8, loc="best")
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0)

        if show:
            plt.tight_layout()
            plt.show()

        return ax


# ============================================================================
# CLI
# ============================================================================


def cli_facilities(args):
    with SVODatabase(args.db) as db:
        facilities = db.list_facilities()
        print(f"\n{'Facility':<40} {'Filters':>8}")
        print("-" * 50)
        for name, count in facilities:
            print(f"{name:<40} {count:>8}")
        print(f"\nTotal: {len(facilities)} facilities")


def cli_instruments(args):
    with SVODatabase(args.db) as db:
        instruments = db.list_instruments(args.facility)
        print(f"\nInstruments for {args.facility}:")
        print(f"\n{'Instrument':<40} {'Filters':>8}")
        print("-" * 50)
        for name, count in instruments:
            print(f"{name:<40} {count:>8}")
        print(f"\nTotal: {len(instruments)} instruments")


def cli_search(args):
    with SVODatabase(args.db) as db:
        results = db.search(
            facility=args.facility,
            instrument=args.instrument,
            wavelength_min=args.wl_min,
            wavelength_max=args.wl_max,
            name_contains=args.name,
            limit=args.limit,
        )
        if not results:
            print("No filters found matching criteria.")
            return

        print(f"\n{'Filter ID':<40} {'λ_eff (Å)':>12} {'FWHM (Å)':>12} {'MagSys':>8} {'Points':>8}")
        print("-" * 85)
        for f in results:
            wl = f"{f['wavelength_eff']:.1f}" if f["wavelength_eff"] else "—"
            fw = f"{f['fwhm']:.1f}" if f["fwhm"] else "—"
            ms = f["mag_sys"] or "—"
            tc = str(f["transmission_count"]) if f["transmission_count"] else "—"
            print(f"{f['filter_id']:<40} {wl:>12} {fw:>12} {ms:>8} {tc:>8}")

        print(f"\n{len(results)} filters shown", end="")
        if len(results) >= args.limit:
            print(f" (limit={args.limit}, use --limit to see more)")
        else:
            print()


def cli_info(args):
    with SVODatabase(args.db) as db:
        f = db.get_filter(args.filter_id)
        if not f:
            print(f"Filter not found: {args.filter_id}")
            sys.exit(1)

        print(f"\n{'='*60}")
        print(f"  {f['filter_id']}")
        print(f"{'='*60}")
        skip_keys = {"raw_metadata"}
        for key, val in f.items():
            if key in skip_keys or val is None:
                continue
            print(f"  {key:<25} {val}")

        if args.raw:
            print("\n--- Raw Metadata ---")
            raw = json.loads(f["raw_metadata"]) if f["raw_metadata"] else {}
            print(json.dumps(raw, indent=2))


def cli_stats(args):
    with SVODatabase(args.db) as db:
        s = db.stats()
        print(f"\n{'='*50}")
        print("  SVO FPS Database Summary")
        print(f"{'='*50}")
        print(f"  Database file:           {db.db_path}")
        print(f"  Database size:           {s['db_size_mb']} MB")
        print(f"  Total filters:           {s['total_filters']:,}")
        print(f"  Total facilities:        {s['total_facilities']:,}")
        print(f"  Total instruments:       {s['total_instruments']:,}")
        print(f"  With transmission data:  {s['filters_with_transmission']:,}")
        print(f"  Total curve points:      {s['total_transmission_points']:,}")

        wl_range = s.get("wavelength_range_angstrom", (None, None))
        if wl_range[0]:
            print(f"  Wavelength range:        {wl_range[0]:.0f} – {wl_range[1]:.0f} Å")

        if s.get("last_harvest"):
            h = s["last_harvest"]
            print("\n  Last Harvest:")
            print(f"    Started:    {h['started']}")
            print(f"    Finished:   {h['finished']}")
            print(f"    Endpoint:   {h['endpoint']}")
            print(f"    Success:    {h['success']}")
            print(f"    Failed:     {h['failed']}")

        if s.get("top_facilities"):
            print("\n  Top Facilities:")
            for name, count in s["top_facilities"]:
                print(f"    {name:<35} {count:>6} filters")


def cli_export(args):
    with SVODatabase(args.db) as db:
        output = args.output or f"{args.filter_id.replace('/', '_')}.csv"
        db.export_csv(args.filter_id, output)
        print(f"Exported to {output}")


def cli_plot(args):
    with SVODatabase(args.db) as db:
        db.plot_transmission(*args.filter_ids)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Query a harvested SVO FPS database"
    )
    p.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )

    sub = p.add_subparsers(dest="command", help="Command")

    # facilities
    sub.add_parser("facilities", help="List all facilities")

    # instruments
    inst = sub.add_parser("instruments", help="List instruments for a facility")
    inst.add_argument("facility", help="Facility name")

    # search
    s = sub.add_parser("search", help="Search filters")
    s.add_argument("--facility", help="Facility name")
    s.add_argument("--instrument", help="Instrument name")
    s.add_argument("--wl-min", type=float, help="Min effective wavelength (Å)")
    s.add_argument("--wl-max", type=float, help="Max effective wavelength (Å)")
    s.add_argument("--name", help="Substring match on filter ID")
    s.add_argument("--limit", type=int, default=100, help="Max results (default: 100)")

    # info
    inf = sub.add_parser("info", help="Show details for a specific filter")
    inf.add_argument("filter_id", help="Filter ID (e.g. HST/ACS_WFC.F435W)")
    inf.add_argument("--raw", action="store_true", help="Show raw metadata JSON")

    # stats
    sub.add_parser("stats", help="Database summary statistics")

    # export
    exp = sub.add_parser("export", help="Export transmission curve to CSV")
    exp.add_argument("filter_id", help="Filter ID")
    exp.add_argument("-o", "--output", help="Output CSV path")

    # plot
    pl = sub.add_parser("plot", help="Plot transmission curves (needs matplotlib)")
    pl.add_argument("filter_ids", nargs="+", help="One or more filter IDs to plot")

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "facilities": cli_facilities,
        "instruments": cli_instruments,
        "search": cli_search,
        "info": cli_info,
        "stats": cli_stats,
        "export": cli_export,
        "plot": cli_plot,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
