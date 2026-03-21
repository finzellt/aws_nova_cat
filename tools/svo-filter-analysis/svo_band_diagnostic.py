#!/usr/bin/env python3
"""
SVO Band Outlier Diagnostic
============================

Identifies filters that don't belong in their assigned band by computing
each filter's mean overlap with the rest of its band. Filters with low
mean overlap are flagged as outliers — typically narrowband filters that
happen to sit within a broadband window.

Usage
-----
    # Run diagnostic — identify outliers
    python svo_band_diagnostic.py --db svo_fps.db

    # Adjust the outlier threshold (default: 0.6)
    python svo_band_diagnostic.py --db svo_fps.db --threshold 0.5

    # After reviewing, re-run svo_analysis.py with the exclusion list:
    python svo_analysis.py --db svo_fps.db --exclude outliers.json
"""

import argparse
import csv
import json
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import interp1d

# Import the core functions from svo_analysis
# (We duplicate the essentials here to keep this self-contained)

INTERP_GRID_STEP = 1.0

CORE_BANDS = ["B", "V", "R", "I", "J", "H", "K", "Ks"]
SLOAN_BANDS = ["u", "g", "r", "i", "z"]

DEFAULT_DB_PATH = "svo_fps.db"
OUTPUT_DIR = "analysis_output"


def load_band_filters(db_path: str, bands: list[str]) -> dict[str, list[dict]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    band_filters = {}
    for band in bands:
        rows = conn.execute(
            "SELECT filter_id, facility, instrument, filter_name, "
            "       wavelength_eff, fwhm, transmission_count "
            "FROM filters WHERE band = ? AND transmission_count > 0 "
            "ORDER BY filter_id",
            (band,),
        ).fetchall()
        filters_in_band = []
        for row in rows:
            curve = conn.execute(
                "SELECT wavelength, transmission FROM transmission_curves "
                "WHERE filter_id = ? ORDER BY wavelength",
                (row["filter_id"],),
            ).fetchall()
            if len(curve) < 3:
                continue
            wl = np.array([c[0] for c in curve])
            tr = np.array([c[1] for c in curve])
            filters_in_band.append({
                "filter_id": row["filter_id"],
                "facility": row["facility"],
                "instrument": row["instrument"],
                "filter_name": row["filter_name"],
                "wavelength_eff": row["wavelength_eff"],
                "fwhm": row["fwhm"],
                "wavelength": wl,
                "transmission": tr,
            })
        if filters_in_band:
            band_filters[band] = filters_in_band
    conn.close()
    return band_filters


def normalize_and_interpolate(filt, grid_min, grid_max, grid_step=INTERP_GRID_STEP):
    wl = filt["wavelength"]
    tr = np.clip(filt["transmission"], 0.0, None)
    grid = np.arange(grid_min, grid_max + grid_step, grid_step)
    interp_func = interp1d(wl, tr, kind="linear", bounds_error=False, fill_value=0.0)
    tr_interp = interp_func(grid)
    integral = np.trapz(tr_interp, grid)
    if integral > 0:
        tr_interp = tr_interp / integral
    return grid, tr_interp


def compute_overlap(grid, tr_a, tr_b):
    integrand = np.sqrt(tr_a * tr_b)
    return float(np.clip(np.trapz(integrand, grid), 0.0, 1.0))


def analyze_band(band: str, filters: list[dict]) -> list[dict]:
    """
    Compute per-filter mean overlap and return detailed diagnostics.

    Returns list of dicts sorted by mean_overlap (ascending = worst first):
      filter_id, facility, instrument, wavelength_eff, fwhm,
      mean_overlap, min_overlap, median_overlap, n_low_pairs
    """
    n = len(filters)
    if n < 2:
        return []

    # Common grid
    all_min = min(f["wavelength"].min() for f in filters)
    all_max = max(f["wavelength"].max() for f in filters)
    span = all_max - all_min
    grid_min = all_min - span * 0.05
    grid_max = all_max + span * 0.05

    # Normalize and interpolate
    grid = None
    normed = []
    for f in filters:
        g, tr = normalize_and_interpolate(f, grid_min, grid_max)
        if grid is None:
            grid = g
        normed.append(tr)

    # Compute full overlap matrix
    overlap = np.ones((n, n))
    for i in range(n):
        for j in range(i + 1, n):
            ov = compute_overlap(grid, normed[i], normed[j])
            overlap[i, j] = ov
            overlap[j, i] = ov

    # Per-filter statistics
    results = []
    for i in range(n):
        # All overlaps for this filter (excluding self)
        row = np.concatenate([overlap[i, :i], overlap[i, i+1:]])
        results.append({
            "filter_id": filters[i]["filter_id"],
            "facility": filters[i]["facility"],
            "instrument": filters[i]["instrument"],
            "wavelength_eff": filters[i]["wavelength_eff"],
            "fwhm": filters[i]["fwhm"],
            "mean_overlap": float(np.mean(row)),
            "min_overlap": float(np.min(row)),
            "median_overlap": float(np.median(row)),
            "n_low_pairs": int(np.sum(row < 0.5)),  # Pairs with <50% overlap
        })

    # Sort by mean overlap (worst first)
    results.sort(key=lambda x: x["mean_overlap"])
    return results


def classify_filters(
    diagnostics: list[dict],
    threshold: float,
) -> tuple[list[dict], list[dict]]:
    """Split into outliers and inliers based on mean overlap threshold."""
    outliers = [d for d in diagnostics if d["mean_overlap"] < threshold]
    inliers = [d for d in diagnostics if d["mean_overlap"] >= threshold]
    return outliers, inliers


def plot_mean_overlap_distribution(
    all_diagnostics: dict[str, list[dict]],
    threshold: float,
    output_dir: str,
):
    """
    Strip plot / dot plot of per-filter mean overlaps for each band.
    """
    bands = sorted(all_diagnostics.keys())
    n_bands = len(bands)

    fig, ax = plt.subplots(1, 1, figsize=(max(8, n_bands * 0.8), 6))

    for i, band in enumerate(bands):
        diags = all_diagnostics[band]
        means = [d["mean_overlap"] for d in diags]

        # Color by outlier status
        colors = ["red" if m < threshold else "steelblue" for m in means]

        # Jittered x positions
        x = np.full(len(means), i) + np.random.uniform(-0.2, 0.2, len(means))
        ax.scatter(x, means, c=colors, s=15, alpha=0.6, edgecolors="none")

    ax.set_xticks(range(n_bands))
    ax.set_xticklabels([f"{b}\n(n={len(all_diagnostics[b])})" for b in bands])
    ax.set_ylabel("Mean Intra-Band Overlap")
    ax.set_title("Per-Filter Mean Overlap Within Band\n(Red = outlier below threshold)")
    ax.axhline(y=threshold, color="red", linestyle="--", alpha=0.5,
               label=f"Threshold = {threshold}")
    ax.set_ylim(-0.05, 1.05)
    ax.legend(loc="lower left")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    out_path = Path(output_dir) / "mean_overlap_by_filter.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_outlier_curves(
    band: str,
    filters: list[dict],
    diagnostics: list[dict],
    threshold: float,
    output_dir: str,
):
    """
    Plot the actual transmission curves for a band, coloring outliers red.
    This makes it immediately obvious what the outlier filters look like.
    """
    # Build lookup for mean overlap
    overlap_lookup = {d["filter_id"]: d["mean_overlap"] for d in diagnostics}

    outlier_filters = [f for f in filters if overlap_lookup.get(f["filter_id"], 1.0) < threshold]
    inlier_filters = [f for f in filters if overlap_lookup.get(f["filter_id"], 1.0) >= threshold]

    if not outlier_filters:
        return  # Nothing to show

    fig, ax = plt.subplots(1, 1, figsize=(12, 6))

    # Plot inliers in gray
    for f in inlier_filters:
        ax.plot(f["wavelength"], f["transmission"], color="gray", alpha=0.2, linewidth=0.5)

    # Plot outliers in red with labels
    for f in outlier_filters:
        mean_ov = overlap_lookup[f["filter_id"]]
        label = f["filter_id"].split("/")[-1] if "/" in f["filter_id"] else f["filter_id"]
        ax.plot(f["wavelength"], f["transmission"], color="red", alpha=0.7, linewidth=1.2,
                label=f"{label} (ov={mean_ov:.2f})")

    ax.set_xlabel("Wavelength (Å)")
    ax.set_ylabel("Transmission")
    ax.set_title(f"{band}-band: Outlier Filters (red) vs Normal (gray)\n"
                 f"{len(outlier_filters)} outliers, {len(inlier_filters)} normal")

    if len(outlier_filters) <= 15:
        ax.legend(fontsize=7, loc="best")

    ax.grid(alpha=0.3)
    plt.tight_layout()
    out_path = Path(output_dir) / f"outlier_curves_{band}.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Identify outlier filters within SVO band groups",
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SVO database path")
    parser.add_argument("--threshold", type=float, default=0.6,
                        help="Mean overlap below this = outlier (default: 0.6)")
    parser.add_argument("-o", "--output-dir", default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--include-stromgren", action="store_true")
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    bands = CORE_BANDS + SLOAN_BANDS
    if args.include_stromgren:
        bands += ["b", "v", "y"]

    print(f"Loading filters for bands: {', '.join(bands)}")
    band_filters = load_band_filters(args.db, bands)
    total = sum(len(v) for v in band_filters.values())
    print(f"Loaded {total} filters across {len(band_filters)} bands\n")

    all_diagnostics = {}
    all_outliers = {}
    all_outlier_ids = {}

    print("=" * 80)
    print(f"{'Band':<6} {'Total':>6} {'Outliers':>9} {'Inliers':>8}  "
          f"{'Worst Filter':<40} {'Mean Ov':>8}")
    print("-" * 80)

    for band in bands:
        if band not in band_filters:
            continue

        filters = band_filters[band]
        diagnostics = analyze_band(band, filters)
        outliers, inliers = classify_filters(diagnostics, args.threshold)

        all_diagnostics[band] = diagnostics
        all_outliers[band] = outliers
        all_outlier_ids[band] = [o["filter_id"] for o in outliers]

        worst = diagnostics[0] if diagnostics else None
        worst_str = f"{worst['filter_id']}" if worst else "—"
        worst_ov = f"{worst['mean_overlap']:.3f}" if worst else "—"

        print(f"{band:<6} {len(filters):>6} {len(outliers):>9} {len(inliers):>8}  "
              f"{worst_str:<40} {worst_ov:>8}")

    print("=" * 80)

    # Detailed outlier report
    total_outliers = sum(len(v) for v in all_outliers.values())
    print(f"\nTotal outliers (threshold={args.threshold}): {total_outliers}")

    if total_outliers > 0:
        print(f"\n{'='*80}")
        print("DETAILED OUTLIER LIST")
        print(f"{'='*80}")
        print(f"{'Band':<5} {'Filter ID':<45} {'λ_eff':>9} {'FWHM':>9} "
              f"{'MeanOv':>7} {'MinOv':>7} {'MedOv':>7} {'LowPrs':>7}")
        print("-" * 100)

        for band in bands:
            if band not in all_outliers or not all_outliers[band]:
                continue
            for o in all_outliers[band]:
                wl = f"{o['wavelength_eff']:.0f}" if o['wavelength_eff'] else "—"
                fw = f"{o['fwhm']:.0f}" if o['fwhm'] else "—"
                print(f"{band:<5} {o['filter_id']:<45} {wl:>9} {fw:>9} "
                      f"{o['mean_overlap']:>7.3f} {o['min_overlap']:>7.3f} "
                      f"{o['median_overlap']:>7.3f} {o['n_low_pairs']:>7}")

    # Write exclusion list as JSON (for use by svo_analysis.py)
    exclusion_path = out / "outliers.json"
    exclusion_data = {
        "threshold": args.threshold,
        "total_outliers": total_outliers,
        "bands": {},
    }
    for band in bands:
        if band in all_outlier_ids and all_outlier_ids[band]:
            exclusion_data["bands"][band] = all_outlier_ids[band]

    exclusion_path.write_text(json.dumps(exclusion_data, indent=2))
    print(f"\nExclusion list saved to: {exclusion_path}")

    # Write full diagnostic CSV
    csv_path = out / "filter_diagnostics.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "band", "filter_id", "facility", "instrument",
            "wavelength_eff", "fwhm",
            "mean_overlap", "min_overlap", "median_overlap",
            "n_low_pairs", "is_outlier",
        ])
        for band in bands:
            if band not in all_diagnostics:
                continue
            outlier_set = set(all_outlier_ids.get(band, []))
            for d in all_diagnostics[band]:
                writer.writerow([
                    band, d["filter_id"], d["facility"], d["instrument"],
                    f"{d['wavelength_eff']:.1f}" if d["wavelength_eff"] else "",
                    f"{d['fwhm']:.1f}" if d["fwhm"] else "",
                    f"{d['mean_overlap']:.4f}",
                    f"{d['min_overlap']:.4f}",
                    f"{d['median_overlap']:.4f}",
                    d["n_low_pairs"],
                    d["filter_id"] in outlier_set,
                ])
    print(f"Full diagnostics saved to: {csv_path}")

    # Generate plots
    print("\nGenerating plots ...")
    plot_mean_overlap_distribution(all_diagnostics, args.threshold, args.output_dir)

    for band in bands:
        if band in all_outliers and all_outliers[band]:
            plot_outlier_curves(band, band_filters[band], all_diagnostics[band],
                               args.threshold, args.output_dir)

    # Summary recommendation
    print(f"\n{'='*80}")
    print("RECOMMENDATION")
    print(f"{'='*80}")
    print(f"With threshold={args.threshold}, {total_outliers} filters flagged as outliers")
    print(f"out of {total} total ({100*total_outliers/total:.1f}%).")
    print("\nTo re-run the main analysis excluding these:")
    print(f"  python svo_analysis.py --db {args.db} --exclude {exclusion_path}")
    print(f"\nReview the outlier curve plots in {args.output_dir}/ to verify")
    print("these are genuinely incompatible filters (narrowband, mislabeled, etc.)")


if __name__ == "__main__":
    main()
