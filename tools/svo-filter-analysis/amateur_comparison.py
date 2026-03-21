#!/usr/bin/env python3
"""
Amateur Filter Head-to-Head Comparison
=======================================

Compares specific amateur filter sets (e.g., Astrodon vs Baader) against
each other and against a reference professional filter (e.g., Bessell).

This answers the specific question: "If Observer A uses an Astrodon V
and Observer B uses a Baader V, how much do their magnitudes differ
for the same star?"

Usage
-----
    python amateur_comparison.py --custom-dir ./custom_filters/ --custom-unit nm
    python amateur_comparison.py --custom-dir ./custom_filters/ --custom-unit nm --db svo_fps.db

    If --db is provided, the script also pulls in Bessell/Johnson reference
    filters from the SVO database for comparison.

Custom Filter Naming
--------------------
The script groups filters by manufacturer using the filename prefix:
    Astrodon_B_B.csv   → manufacturer="Astrodon", filter_name="B", band="B"
    Baader_V_V.csv     → manufacturer="Baader", filter_name="V", band="V"

The convention is: Manufacturer_FilterName_Band.csv
"""

import argparse
import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import interp1d

# ============================================================================
# Configuration
# ============================================================================

OUTPUT_DIR = "analysis_output"
INTERP_GRID_STEP = 1.0  # Angstroms

# Blackbody grid — denser than the main analysis for smooth curves
TEMP_GRID = np.concatenate([
    np.arange(2500, 5000, 250),
    np.arange(5000, 10000, 500),
    np.arange(10000, 30001, 1000),
])

TEMP_LABELS = {3500: "3500 K", 5800: "5800 K", 10000: "10000 K", 25000: "25000 K"}

# Physical constants
H_PLANCK = 6.62607015e-34
C_LIGHT = 2.99792458e8
K_BOLTZ = 1.380649e-23

# Reference filters to pull from SVO (if --db provided)
REFERENCE_FILTERS = {
    "U": ["OAF/Bessell.U", "HCT/HFOSC.Bessell_U"],
    "B": ["OAF/Bessell.B", "HCT/HFOSC.Bessell_B"],
    "V": ["OAF/Bessell.V", "HCT/HFOSC.Bessell_V"],
    "R": ["OAF/Bessell.R", "HCT/HFOSC.Bessell_R"],
    "I": ["OAF/Bessell.I", "HCT/HFOSC.Bessell_I"],
}

# Plot colors per manufacturer
MANUFACTURER_COLORS = {
    "Astrodon": "#2166ac",
    "Baader": "#b2182b",
    "Chroma": "#1b7837",
    "Optec": "#762a83",
    "Bessell": "#333333",
    "Reference": "#333333",
}

MANUFACTURER_MARKERS = {
    "Astrodon": "o",
    "Baader": "s",
    "Chroma": "^",
    "Optec": "D",
    "Bessell": "x",
    "Reference": "x",
}


# ============================================================================
# Physics
# ============================================================================


def blackbody_flux(wavelength_angstrom: np.ndarray, T: float) -> np.ndarray:
    lam = wavelength_angstrom * 1e-10
    with np.errstate(over="ignore", divide="ignore"):
        exponent = np.clip(H_PLANCK * C_LIGHT / (lam * K_BOLTZ * T), 0, 500)
        B = (2 * H_PLANCK * C_LIGHT ** 2 / lam ** 5) / (np.exp(exponent) - 1)
    return B * lam  # Photon counting


def synthetic_mag(grid, transmission, sed):
    numerator = np.trapezoid(sed * transmission, grid)
    denominator = np.trapezoid(transmission, grid)
    if numerator <= 0 or denominator <= 0:
        return np.nan
    return -2.5 * np.log10(numerator / denominator)


# ============================================================================
# Filter loading
# ============================================================================


def load_custom_filters(custom_dir: str, wavelength_unit: str = "A") -> list[dict]:
    """
    Load custom filter CSVs. Returns list of filter dicts with
    'manufacturer', 'filter_name', 'band', 'wavelength', 'transmission'.
    """
    unit_mult = {"a": 1.0, "nm": 10.0, "um": 10000.0}[wavelength_unit.lower()]
    filters = []

    for csv_file in sorted(Path(custom_dir).glob("*.csv")):
        parts = csv_file.stem.split("_")
        if len(parts) < 3:
            print(f"  WARNING: Can't parse {csv_file.name} — expected Manufacturer_Name_Band.csv")
            continue

        manufacturer = parts[0]
        filter_name = "_".join(parts[1:-1])
        band = parts[-1]

        wl_list, tr_list = [], []
        with open(csv_file) as f:
            for row in csv.reader(f):
                if not row or row[0].startswith("#"):
                    continue
                try:
                    wl_list.append(float(row[0]) * unit_mult)
                    tr_list.append(float(row[1]))
                except (ValueError, IndexError):
                    continue

        if len(wl_list) < 3:
            continue

        wl = np.array(wl_list)
        tr = np.array(tr_list)
        order = np.argsort(wl)
        wl, tr = wl[order], tr[order]

        # Auto-detect percentage
        if tr.max() > 1.5:
            tr = tr / 100.0

        tr = np.clip(tr, 0.0, None)

        filters.append({
            "manufacturer": manufacturer,
            "filter_name": filter_name,
            "band": band,
            "label": f"{manufacturer} {filter_name}",
            "wavelength": wl,
            "transmission": tr,
        })
        print(f"  Loaded: {csv_file.name} -> {manufacturer} {filter_name} ({band}-band)")

    return filters


def load_reference_filters(db_path: str) -> list[dict]:
    """Load reference Bessell filters from the SVO database."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    filters = []

    for band, filter_ids in REFERENCE_FILTERS.items():
        for fid in filter_ids:
            row = conn.execute(
                "SELECT filter_id, transmission_count FROM filters WHERE filter_id = ?",
                (fid,),
            ).fetchone()
            if not row or row["transmission_count"] == 0:
                continue

            curve = conn.execute(
                "SELECT wavelength, transmission FROM transmission_curves "
                "WHERE filter_id = ? ORDER BY wavelength",
                (fid,),
            ).fetchall()

            wl = np.array([c[0] for c in curve])
            tr = np.clip(np.array([c[1] for c in curve]), 0.0, None)

            short_name = fid.split("/")[-1] if "/" in fid else fid
            filters.append({
                "manufacturer": "Reference",
                "filter_name": short_name,
                "band": band,
                "label": f"Ref: {short_name}",
                "wavelength": wl,
                "transmission": tr,
            })
            print(f"  Loaded reference: {fid} ({band}-band)")
            break  # Take the first available reference per band

    conn.close()
    return filters


# ============================================================================
# Analysis
# ============================================================================


def compute_magnitude_grid(
    filters: list[dict],
    temp_grid: np.ndarray,
) -> dict:
    """
    For each filter, compute synthetic magnitudes across a temperature grid.

    Returns dict with:
      'filters': list of filter dicts (with 'mags' array added)
      'temp_grid': temperature array
      'bands': set of bands present
    """
    for filt in filters:
        wl = filt["wavelength"]
        tr = filt["transmission"]

        # Interpolate onto a fine grid
        grid_min = wl.min() - 100
        grid_max = wl.max() + 100
        grid = np.arange(grid_min, grid_max, INTERP_GRID_STEP)
        interp_func = interp1d(wl, tr, kind="linear", bounds_error=False, fill_value=0.0)
        tr_interp = interp_func(grid)

        mags = []
        for T in temp_grid:
            sed = blackbody_flux(grid, T)
            mags.append(synthetic_mag(grid, tr_interp, sed))

        filt["mags"] = np.array(mags)
        filt["grid"] = grid
        filt["tr_interp"] = tr_interp

    return {
        "filters": filters,
        "temp_grid": temp_grid,
        "bands": sorted(set(f["band"] for f in filters)),
    }


def compute_pairwise_comparison(data: dict) -> list[dict]:
    """
    For each pair of filters in the same band from different manufacturers,
    compute the magnitude difference across the temperature grid.

    Returns list of comparison dicts.
    """
    filters = data["filters"]
    temp_grid = data["temp_grid"]
    comparisons = []

    for i in range(len(filters)):
        for j in range(i + 1, len(filters)):
            fi, fj = filters[i], filters[j]

            if fi["band"] != fj["band"]:
                continue
            if fi["manufacturer"] == fj["manufacturer"]:
                continue

            delta_mag = (fi["mags"] - fj["mags"]) * 1000  # millimag

            comparisons.append({
                "band": fi["band"],
                "filter_a": fi["label"],
                "filter_b": fj["label"],
                "manufacturer_a": fi["manufacturer"],
                "manufacturer_b": fj["manufacturer"],
                "delta_mmag": delta_mag,
                "temp_grid": temp_grid,
            })

    return comparisons


def compute_overlap_between(filt_a: dict, filt_b: dict) -> float:
    """Compute normalized overlap between two specific filters."""
    wl_min = min(filt_a["wavelength"].min(), filt_b["wavelength"].min()) - 100
    wl_max = max(filt_a["wavelength"].max(), filt_b["wavelength"].max()) + 100
    grid = np.arange(wl_min, wl_max, INTERP_GRID_STEP)

    interp_a = interp1d(filt_a["wavelength"], np.clip(filt_a["transmission"], 0, None),
                        kind="linear", bounds_error=False, fill_value=0.0)
    interp_b = interp1d(filt_b["wavelength"], np.clip(filt_b["transmission"], 0, None),
                        kind="linear", bounds_error=False, fill_value=0.0)

    tr_a = interp_a(grid)
    tr_b = interp_b(grid)

    # Normalize
    int_a = np.trapezoid(tr_a, grid)
    int_b = np.trapezoid(tr_b, grid)
    if int_a > 0:
        tr_a = tr_a / int_a
    if int_b > 0:
        tr_b = tr_b / int_b

    integrand = np.sqrt(tr_a * tr_b)
    return float(np.clip(np.trapezoid(integrand, grid), 0.0, 1.0))


# ============================================================================
# Visualization
# ============================================================================


def plot_transmission_comparison(data: dict, output_dir: str):
    """
    Plot raw transmission curves for each band, grouped by manufacturer.
    """
    bands = data["bands"]
    ncols = min(3, len(bands))
    nrows = (len(bands) + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows), squeeze=False)

    for idx, band in enumerate(bands):
        ax = axes[idx // ncols][idx % ncols]
        band_filters = [f for f in data["filters"] if f["band"] == band]

        for filt in band_filters:
            color = MANUFACTURER_COLORS.get(filt["manufacturer"], "gray")
            ax.plot(filt["wavelength"], filt["transmission"],
                    color=color, linewidth=1.5, alpha=0.8,
                    label=filt["label"])

        ax.set_xlabel("Wavelength (Å)")
        ax.set_ylabel("Transmission")
        ax.set_title(f"{band}-band")
        ax.legend(fontsize=7, loc="best")
        ax.grid(alpha=0.3)

    for idx in range(len(bands), nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    fig.suptitle("Amateur Filter Transmission Curves by Band", fontsize=13)
    plt.tight_layout()
    out_path = Path(output_dir) / "amateur_transmission_curves.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_magnitude_differences(comparisons: list[dict], output_dir: str):
    """
    Plot magnitude difference vs temperature for each pairwise comparison.
    One panel per band.
    """
    bands = sorted(set(c["band"] for c in comparisons))
    ncols = min(3, len(bands))
    nrows = (len(bands) + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows), squeeze=False)

    for idx, band in enumerate(bands):
        ax = axes[idx // ncols][idx % ncols]
        band_comps = [c for c in comparisons if c["band"] == band]

        for comp in band_comps:
            label = f"{comp['filter_a']} vs {comp['filter_b']}"
            ax.plot(comp["temp_grid"], comp["delta_mmag"],
                    linewidth=1.5, alpha=0.8, label=label)

        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhspan(-10, 10, color="green", alpha=0.08)
        ax.axhspan(-30, 30, color="yellow", alpha=0.05)

        ax.set_xlabel("Temperature (K)")
        ax.set_ylabel("Δmag (mmag)")
        ax.set_title(f"{band}-band")
        ax.set_xscale("log")
        ax.legend(fontsize=6, loc="best")
        ax.grid(alpha=0.3)

    for idx in range(len(bands), nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    fig.suptitle(
        "Amateur Filter Magnitude Differences vs Stellar Temperature\n"
        "(Green band: ±10 mmag, Yellow band: ±30 mmag)",
        fontsize=11,
    )
    plt.tight_layout()
    out_path = Path(output_dir) / "amateur_magnitude_differences.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def write_comparison_table(
    data: dict,
    comparisons: list[dict],
    output_dir: str,
):
    """
    Write a summary CSV with pairwise comparisons at key temperatures,
    plus overlap values.
    """
    filters = data["filters"]
    key_temps = [3500, 5800, 10000, 25000]

    out_path = Path(output_dir) / "amateur_comparison_table.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        header = ["band", "filter_a", "filter_b", "overlap"]
        for T in key_temps:
            header.append(f"delta_mmag_{T}K")
        writer.writerow(header)

        for comp in comparisons:
            # Find the filters to compute overlap
            fa = next((fi for fi in filters if fi["label"] == comp["filter_a"]), None)
            fb = next((fi for fi in filters if fi["label"] == comp["filter_b"]), None)
            ov = compute_overlap_between(fa, fb) if (fa and fb) else None

            row = [
                comp["band"],
                comp["filter_a"],
                comp["filter_b"],
                f"{ov:.4f}" if ov is not None else "",
            ]
            for T in key_temps:
                t_idx = np.argmin(np.abs(comp["temp_grid"] - T))
                row.append(f"{comp['delta_mmag'][t_idx]:.1f}")
            writer.writerow(row)

    print(f"  Saved: {out_path}")


def print_summary(data: dict, comparisons: list[dict]):
    """Print a human-readable summary to the terminal."""
    filters = data["filters"]
    key_temps = [3500, 5800, 10000, 25000]

    print(f"\n{'='*90}")
    print("AMATEUR FILTER COMPARISON SUMMARY")
    print(f"{'='*90}")

    bands = sorted(set(c["band"] for c in comparisons))
    for band in bands:
        band_comps = [c for c in comparisons if c["band"] == band]
        print(f"\n  {band}-band:")

        for comp in band_comps:
            fa = next((fi for fi in filters if fi["label"] == comp["filter_a"]), None)
            fb = next((fi for fi in filters if fi["label"] == comp["filter_b"]), None)
            ov = compute_overlap_between(fa, fb) if (fa and fb) else None

            print(f"    {comp['filter_a']} vs {comp['filter_b']}")
            if ov is not None:
                print(f"      Overlap: {ov:.3f}")
            print("      Δmag:  ", end="")
            for T in key_temps:
                t_idx = np.argmin(np.abs(comp["temp_grid"] - T))
                delta = comp["delta_mmag"][t_idx]
                print(f" {T}K: {delta:+.0f} mmag  ", end="")
            print()

            # Characterize
            max_delta = max(abs(comp["delta_mmag"][np.argmin(np.abs(comp["temp_grid"] - T))])
                           for T in key_temps)
            if max_delta < 10:
                verdict = "NEGLIGIBLE — safe to combine without correction"
            elif max_delta < 30:
                verdict = "SMALL — within typical CCD measurement noise"
            elif max_delta < 100:
                verdict = "MODERATE — detectable in precision work"
            else:
                verdict = "LARGE — color correction recommended"
            print(f"      Verdict: {verdict}")


# ============================================================================
# Main
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Head-to-head comparison of amateur filter sets",
    )
    parser.add_argument("--custom-dir", required=True,
                        help="Directory containing custom filter CSVs")
    parser.add_argument("--custom-unit", default="A", choices=["A", "nm", "um"],
                        help="Wavelength unit of CSVs (default: Angstrom)")
    parser.add_argument("--db", default=None,
                        help="SVO database for reference filters (optional)")
    parser.add_argument("-o", "--output-dir", default=OUTPUT_DIR, help="Output directory")
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Load custom filters
    print("Loading amateur filters ...")
    filters = load_custom_filters(args.custom_dir, args.custom_unit)
    if not filters:
        print("ERROR: No valid filter files found.")
        sys.exit(1)

    # Load reference filters from SVO
    if args.db:
        print("\nLoading reference filters from SVO ...")
        refs = load_reference_filters(args.db)
        filters.extend(refs)

    # Summarize what we have
    manufacturers = sorted(set(f["manufacturer"] for f in filters))
    bands = sorted(set(f["band"] for f in filters))
    print(f"\nManufacturers: {', '.join(manufacturers)}")
    print(f"Bands: {', '.join(bands)}")
    print(f"Total filters: {len(filters)}")
    for mfg in manufacturers:
        mfg_bands = sorted(set(f["band"] for f in filters if f["manufacturer"] == mfg))
        print(f"  {mfg}: {', '.join(mfg_bands)}")

    # Compute magnitudes
    print("\nComputing synthetic photometry across temperature grid ...")
    data = compute_magnitude_grid(filters, TEMP_GRID)

    # Pairwise comparisons
    print("Computing pairwise comparisons ...")
    comparisons = compute_pairwise_comparison(data)
    print(f"  {len(comparisons)} cross-manufacturer pairs")

    if not comparisons:
        print("\nNo cross-manufacturer pairs found!")
        print("Make sure you have filters from at least two manufacturers in the same band.")
        sys.exit(1)

    # Print summary
    print_summary(data, comparisons)

    # Generate plots
    print("\nGenerating plots ...")
    plot_transmission_comparison(data, args.output_dir)
    plot_magnitude_differences(comparisons, args.output_dir)

    # Write table
    write_comparison_table(data, comparisons, args.output_dir)

    print(f"\nDone! Results in {args.output_dir}/")


if __name__ == "__main__":
    main()
