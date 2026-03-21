#!/usr/bin/env python3
"""
SVO Filter Band Analysis & AAVSO Systematics
=============================================

Two-track analysis:
  Track 1: Pairwise overlap integrals within each band, with grouping
           threshold recommendation.
  Track 2: Synthetic photometry systematics for commercial amateur filters.

Usage
-----
    # Run full analysis
    python svo_analysis.py --db svo_fps.db

    # Add custom filters first (e.g., digitized Astrodon/Baader curves),
    # then run analysis including them
    python svo_analysis.py --db svo_fps.db --custom-dir ./custom_filters/

    # Run only Track 1 (overlap) or Track 2 (systematics)
    python svo_analysis.py --db svo_fps.db --track overlap
    python svo_analysis.py --db svo_fps.db --track systematics

    # Include Stromgren bands
    python svo_analysis.py --db svo_fps.db --include-stromgren

Custom Filter Directory
-----------------------
Place CSV files in the custom filter directory. Each CSV must have:
  - Two columns: wavelength, transmission
  - Wavelength in Angstroms (or use --custom-unit nm to auto-convert)
  - Filename convention: Facility_Instrument_FilterName_Band.csv
    e.g., Astrodon_Photometrics_V_V.csv, Baader_UBVRI_B_B.csv

The _Band suffix tells the script which band to assign the filter to.

Dependencies
------------
    pip install numpy scipy matplotlib
"""

import argparse
import csv
import json
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import Normalize as mplNormalize
from scipy.cluster.hierarchy import linkage
from scipy.interpolate import interp1d

# ============================================================================
# Configuration
# ============================================================================

DEFAULT_DB_PATH = "svo_fps.db"
OUTPUT_DIR = "analysis_output"

# Bands we care about
CORE_BANDS = ["B", "V", "R", "I", "J", "H", "K", "Ks"]
SLOAN_BANDS = ["u", "g", "r", "i", "z"]
STROMGREN_BANDS = ["b", "v", "y"]  # Stromgren u overlaps with Johnson U

# Common wavelength grid for interpolation (Angstroms)
# Fine enough for any filter, coarse enough to be fast
INTERP_GRID_STEP = 1.0  # 1 Angstrom steps
INTERP_MIN_WL = 500.0  # Far UV
INTERP_MAX_WL = 60000.0  # Mid-IR

# Blackbody temperatures for synthetic photometry (Kelvin)
SED_TEMPERATURES = [3500, 5800, 10000, 25000]
SED_LABELS = {
    3500: "Cool red (3500 K)",
    5800: "Solar (5800 K)",
    10000: "Hot blue (10000 K)",
    25000: "Very hot (25000 K)",
}

# Physical constants for blackbody
H_PLANCK = 6.62607015e-34  # J·s
C_LIGHT = 2.99792458e8  # m/s
K_BOLTZ = 1.380649e-23  # J/K

# Plot style
plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 150,
    "savefig.bbox": "tight",
    "font.size": 10,
})


# ============================================================================
# Data loading
# ============================================================================


def load_band_filters(
    db_path: str,
    bands: list[str],
) -> dict[str, list[dict]]:
    """
    Load filters and their transmission curves for the specified bands.

    Returns:
        Dict mapping band_name -> list of filter dicts, where each dict has:
          'filter_id', 'facility', 'instrument', 'filter_name',
          'wavelength_eff', 'fwhm', 'wavelength' (array), 'transmission' (array)
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    band_filters = {}

    for band in bands:
        filters_in_band = []

        # Get metadata for filters in this band
        rows = conn.execute(
            "SELECT filter_id, facility, instrument, filter_name, "
            "       wavelength_eff, fwhm, transmission_count "
            "FROM filters WHERE band = ? AND transmission_count > 0 "
            "ORDER BY filter_id",
            (band,),
        ).fetchall()

        for row in rows:
            fid = row["filter_id"]

            # Load transmission curve
            curve = conn.execute(
                "SELECT wavelength, transmission FROM transmission_curves "
                "WHERE filter_id = ? ORDER BY wavelength",
                (fid,),
            ).fetchall()

            if len(curve) < 3:
                continue

            wl = np.array([c[0] for c in curve])
            tr = np.array([c[1] for c in curve])

            filters_in_band.append({
                "filter_id": fid,
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


def load_custom_filters(
    custom_dir: str,
    wavelength_unit: str = "A",
) -> dict[str, list[dict]]:
    """
    Load custom filter CSVs from a directory.

    Filename convention: anything ending with _BAND.csv, where BAND is the
    band label (e.g., Astrodon_V_V.csv assigns to band "V").

    Returns dict in same format as load_band_filters.
    """
    custom_path = Path(custom_dir)
    if not custom_path.exists():
        return {}

    unit_multiplier = 1.0
    if wavelength_unit.lower() == "nm":
        unit_multiplier = 10.0  # nm -> Angstrom
    elif wavelength_unit.lower() == "um":
        unit_multiplier = 10000.0  # um -> Angstrom

    band_filters = {}

    for csv_file in sorted(custom_path.glob("*.csv")):
        # Parse band from filename: last segment before .csv after splitting on _
        stem = csv_file.stem
        parts = stem.rsplit("_", 1)
        if len(parts) == 2:
            band = parts[1]
        else:
            print(f"  WARNING: Cannot determine band from filename {csv_file.name}")
            print("  Expected format: Name_Band.csv (e.g., Astrodon_V_V.csv)")
            continue

        # Read CSV
        wl_list, tr_list = [], []
        with open(csv_file) as f:
            reader = csv.reader(f)
            for row_data in reader:
                # Skip comments and headers
                if not row_data or row_data[0].startswith("#"):
                    continue
                try:
                    wl = float(row_data[0]) * unit_multiplier
                    tr = float(row_data[1])
                    wl_list.append(wl)
                    tr_list.append(tr)
                except (ValueError, IndexError):
                    continue

        if len(wl_list) < 3:
            print(f"  WARNING: Too few points in {csv_file.name}, skipping")
            continue

        wl = np.array(wl_list)
        tr = np.array(tr_list)

        # Sort by wavelength
        order = np.argsort(wl)
        wl = wl[order]
        tr = tr[order]

        # Auto-detect percentage scale
        if tr.max() > 1.5:
            tr = tr / 100.0

        filt = {
            "filter_id": f"Custom/{stem}",
            "facility": "Custom",
            "instrument": stem.rsplit("_", 1)[0] if "_" in stem else stem,
            "filter_name": stem,
            "wavelength_eff": np.trapezoid(wl * tr, wl) / np.trapezoid(tr, wl) if np.trapezoid(tr, wl) > 0 else 0,
            "fwhm": None,
            "wavelength": wl,
            "transmission": tr,
        }

        if band not in band_filters:
            band_filters[band] = []
        band_filters[band].append(filt)
        print(f"  Loaded custom filter: {csv_file.name} -> band {band}")

    return band_filters


def merge_filter_dicts(
    svo: dict[str, list[dict]],
    custom: dict[str, list[dict]],
) -> dict[str, list[dict]]:
    """Merge SVO and custom filter dictionaries."""
    merged = dict(svo)
    for band, filters in custom.items():
        if band in merged:
            merged[band].extend(filters)
        else:
            merged[band] = list(filters)
    return merged


# ============================================================================
# Normalization and interpolation
# ============================================================================


def normalize_and_interpolate(
    filt: dict,
    grid_min: float | None = None,
    grid_max: float | None = None,
    grid_step: float = INTERP_GRID_STEP,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Normalize a filter's transmission curve to unit integral and interpolate
    onto a common wavelength grid.

    This handles:
      - Uneven sampling (digitized curves)
      - Different wavelength ranges across filters
      - Normalization so ∫T(λ)dλ = 1

    Args:
        filt: Filter dict with 'wavelength' and 'transmission' arrays.
        grid_min/max: Wavelength bounds for the common grid. If None, uses
                      the filter's own range padded by 10%.
        grid_step: Step size for the common grid (Angstroms).

    Returns:
        (wavelength_grid, normalized_transmission) — both 1D arrays on the
        common grid. Transmission is zero outside the filter's native range.
    """
    wl = filt["wavelength"]
    tr = filt["transmission"]

    # Clip any negative transmission values
    tr = np.clip(tr, 0.0, None)

    # Determine grid bounds
    if grid_min is None:
        grid_min = wl.min()
    if grid_max is None:
        grid_max = wl.max()

    # Create the common grid
    grid = np.arange(grid_min, grid_max + grid_step, grid_step)

    # Interpolate onto common grid (zero outside native range)
    interp_func = interp1d(
        wl, tr,
        kind="linear",
        bounds_error=False,
        fill_value=0.0,
    )
    tr_interp = interp_func(grid)

    # Normalize so integral = 1
    integral = np.trapezoid(tr_interp, grid)
    if integral > 0:
        tr_interp = tr_interp / integral

    return grid, tr_interp


def compute_band_grid_range(filters: list[dict], padding_frac: float = 0.05) -> tuple[float, float]:
    """
    Determine a common wavelength grid range that covers all filters in a band,
    with some padding.
    """
    all_min = min(f["wavelength"].min() for f in filters)
    all_max = max(f["wavelength"].max() for f in filters)
    span = all_max - all_min
    return all_min - span * padding_frac, all_max + span * padding_frac


# ============================================================================
# Track 1: Overlap computation
# ============================================================================


def compute_overlap(
    grid: np.ndarray,
    tr_a: np.ndarray,
    tr_b: np.ndarray,
) -> float:
    """
    Compute the overlap integral between two normalized transmission curves.

    Uses the Bhattacharyya coefficient: ∫ sqrt(T_a(λ) · T_b(λ)) dλ

    Since T_a and T_b are normalized to unit integral, this returns a value
    in [0, 1] where 1 = identical shapes and 0 = no overlap.
    """
    integrand = np.sqrt(tr_a * tr_b)
    return float(np.clip(np.trapezoid(integrand, grid), 0.0, 1.0))


def compute_band_overlap_matrix(
    filters: list[dict],
) -> tuple[np.ndarray, list[str], np.ndarray, list[np.ndarray]]:
    """
    Compute the full pairwise overlap matrix for a band.

    Returns:
        overlap_matrix: NxN array of overlap values
        filter_ids: list of filter_id strings
        grid: common wavelength grid
        normalized_curves: list of normalized transmission arrays
    """
    n = len(filters)

    # Determine common grid for this band
    grid_min, grid_max = compute_band_grid_range(filters)
    grid_step = INTERP_GRID_STEP

    # Normalize and interpolate all filters onto the same grid
    grid = None
    normalized = []
    for f in filters:
        g, tr = normalize_and_interpolate(f, grid_min, grid_max, grid_step)
        if grid is None:
            grid = g
        normalized.append(tr)

    # Compute pairwise overlaps
    overlap = np.ones((n, n))
    for i in range(n):
        for j in range(i + 1, n):
            ov = compute_overlap(grid, normalized[i], normalized[j])
            overlap[i, j] = ov
            overlap[j, i] = ov

    filter_ids = [f["filter_id"] for f in filters]
    return overlap, filter_ids, grid, normalized


def compute_all_overlaps(
    band_filters: dict[str, list[dict]],
) -> dict[str, dict]:
    """
    Run overlap computation for all bands.

    Returns dict mapping band -> {
        'overlap_matrix': ndarray,
        'filter_ids': list[str],
        'grid': ndarray,
        'normalized_curves': list[ndarray],
        'filters': list[dict],
    }
    """
    results = {}
    for band, filters in sorted(band_filters.items()):
        if len(filters) < 2:
            print(f"  {band}: only {len(filters)} filter(s), skipping overlap")
            continue

        print(f"  {band}: computing {len(filters)} filters "
              f"({len(filters) * (len(filters) - 1) // 2} pairs) ...")
        overlap, fids, grid, normed = compute_band_overlap_matrix(filters)

        results[band] = {
            "overlap_matrix": overlap,
            "filter_ids": fids,
            "grid": grid,
            "normalized_curves": normed,
            "filters": filters,
        }

    return results


# ============================================================================
# Track 1: Visualization
# ============================================================================


def plot_overlap_boxplots(results: dict, output_dir: str):
    """
    Box plot showing the distribution of pairwise overlaps for each band.
    """
    bands = []
    overlap_distributions = []

    for band in sorted(results.keys()):
        mat = results[band]["overlap_matrix"]
        n = mat.shape[0]
        # Extract upper triangle (excluding diagonal)
        vals = mat[np.triu_indices(n, k=1)]
        if len(vals) > 0:
            bands.append(f"{band}\n(n={n})")
            overlap_distributions.append(vals)

    if not bands:
        print("  No bands with sufficient data for box plots.")
        return

    fig, ax = plt.subplots(1, 1, figsize=(max(8, len(bands) * 0.8), 6))

    bp = ax.boxplot(
        overlap_distributions,
        labels=bands,
        patch_artist=True,
        showfliers=True,
        flierprops={"marker": ".", "markersize": 3, "alpha": 0.5},
        medianprops={"color": "black", "linewidth": 1.5},
    )

    # Color boxes by median
    medians = [np.median(d) for d in overlap_distributions]
    cmap = plt.cm.RdYlGn
    norm = mplNormalize(vmin=0.5, vmax=1.0)
    for patch, med in zip(bp["boxes"], medians, strict=False):
        patch.set_facecolor(cmap(norm(med)))
        patch.set_alpha(0.7)

    ax.set_ylabel("Pairwise Overlap (Bhattacharyya coefficient)")
    ax.set_title("Intra-Band Filter Overlap Distributions")
    ax.set_ylim(-0.05, 1.05)
    ax.axhline(y=0.9, color="gray", linestyle="--", alpha=0.5, label="0.9 threshold")
    ax.axhline(y=0.8, color="gray", linestyle=":", alpha=0.5, label="0.8 threshold")
    ax.legend(loc="lower left", fontsize=8)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    out_path = Path(output_dir) / "overlap_boxplots.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_overlap_heatmap(band: str, data: dict, output_dir: str):
    """
    Heatmap of the overlap matrix for a single band, with hierarchical
    clustering to reveal sub-groups.
    """
    mat = data["overlap_matrix"]
    fids = data["filter_ids"]
    n = mat.shape[0]

    if n < 3:
        return

    # Hierarchical clustering on the distance matrix
    # Clip to handle floating point: overlap can be 1.0000000000000002,
    # making dist slightly negative, which linkage rejects.
    dist = np.clip(1.0 - mat, 0.0, None)
    # Condensed distance matrix (upper triangle)
    condensed = dist[np.triu_indices(n, k=1)]
    Z = linkage(condensed, method="average")

    # Reorder by clustering
    from scipy.cluster.hierarchy import leaves_list
    order = leaves_list(Z)
    mat_ordered = mat[np.ix_(order, order)]
    fids_ordered = [fids[i] for i in order]

    # Shorten filter IDs for display
    short_ids = []
    for fid in fids_ordered:
        # "HST/ACS_WFC.F435W" -> "ACS_WFC.F435W"
        if "/" in fid:
            short_ids.append(fid.split("/", 1)[1])
        else:
            short_ids.append(fid)

    fig, ax = plt.subplots(1, 1, figsize=(max(8, n * 0.35), max(6, n * 0.3)))
    im = ax.imshow(mat_ordered, cmap="RdYlGn", vmin=0.5, vmax=1.0, aspect="auto")

    if n <= 40:
        ax.set_xticks(range(n))
        ax.set_xticklabels(short_ids, rotation=90, fontsize=max(4, 8 - n // 10))
        ax.set_yticks(range(n))
        ax.set_yticklabels(short_ids, fontsize=max(4, 8 - n // 10))
    else:
        ax.set_xticks([])
        ax.set_yticks([])

    ax.set_title(f"{band}-band Overlap Matrix (n={n}, clustered)")
    fig.colorbar(im, ax=ax, label="Overlap", shrink=0.8)

    plt.tight_layout()
    out_path = Path(output_dir) / f"heatmap_{band}.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_overlap_cdf(results: dict, output_dir: str):
    """
    Cumulative distribution of ALL pairwise overlap values across all bands.
    Used to find a natural threshold.
    """
    all_overlaps = []
    for band, data in results.items():
        mat = data["overlap_matrix"]
        n = mat.shape[0]
        vals = mat[np.triu_indices(n, k=1)]
        all_overlaps.extend(vals)

    all_overlaps = np.array(all_overlaps)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # CDF
    sorted_ov = np.sort(all_overlaps)
    cdf = np.arange(1, len(sorted_ov) + 1) / len(sorted_ov)
    ax1.plot(sorted_ov, cdf, "b-", linewidth=1.5)
    ax1.set_xlabel("Overlap Value")
    ax1.set_ylabel("Cumulative Fraction")
    ax1.set_title("CDF of All Pairwise Overlaps")
    ax1.axvline(x=0.9, color="red", linestyle="--", alpha=0.5, label="0.90")
    ax1.axvline(x=0.85, color="orange", linestyle="--", alpha=0.5, label="0.85")
    ax1.axvline(x=0.8, color="green", linestyle="--", alpha=0.5, label="0.80")
    ax1.legend(fontsize=8)
    ax1.grid(alpha=0.3)

    # Histogram
    ax2.hist(all_overlaps, bins=50, edgecolor="black", alpha=0.7, color="steelblue")
    ax2.set_xlabel("Overlap Value")
    ax2.set_ylabel("Count")
    ax2.set_title(f"Distribution of Pairwise Overlaps (n={len(all_overlaps):,})")
    ax2.axvline(x=0.9, color="red", linestyle="--", alpha=0.5, label="0.90")
    ax2.axvline(x=0.85, color="orange", linestyle="--", alpha=0.5, label="0.85")
    ax2.axvline(x=0.8, color="green", linestyle="--", alpha=0.5, label="0.80")
    ax2.legend(fontsize=8)
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    out_path = Path(output_dir) / "overlap_cdf_histogram.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")

    # Print summary stats
    print(f"\n  Overlap statistics ({len(all_overlaps):,} pairs across {len(results)} bands):")
    for threshold in [0.95, 0.90, 0.85, 0.80, 0.70]:
        frac = np.mean(all_overlaps >= threshold) * 100
        print(f"    >= {threshold:.2f}: {frac:.1f}% of pairs")


def write_overlap_summary(results: dict, output_dir: str):
    """Write a CSV summary table of overlap statistics per band."""
    out_path = Path(output_dir) / "overlap_summary.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "band", "n_filters", "n_pairs",
            "median_overlap", "min_overlap", "max_overlap",
            "std_overlap", "pct_above_090", "pct_above_085",
        ])
        for band in sorted(results.keys()):
            mat = results[band]["overlap_matrix"]
            n = mat.shape[0]
            vals = mat[np.triu_indices(n, k=1)]
            if len(vals) == 0:
                continue
            writer.writerow([
                band, n, len(vals),
                f"{np.median(vals):.4f}",
                f"{np.min(vals):.4f}",
                f"{np.max(vals):.4f}",
                f"{np.std(vals):.4f}",
                f"{100 * np.mean(vals >= 0.90):.1f}",
                f"{100 * np.mean(vals >= 0.85):.1f}",
            ])
    print(f"  Saved: {out_path}")


# ============================================================================
# Track 2: Synthetic photometry & AAVSO systematics
# ============================================================================


def blackbody_flux(wavelength_angstrom: np.ndarray, T: float) -> np.ndarray:
    """
    Planck function B_λ(T) in per-Angstrom units.
    Returns flux proportional to B_λ (arbitrary overall normalization is fine
    since we're computing magnitude *differences*).

    Args:
        wavelength_angstrom: Wavelength array in Angstroms
        T: Temperature in Kelvin

    Returns:
        Array of flux values (arbitrary units, consistent across calls).
    """
    # Convert Angstrom to meters
    lam = wavelength_angstrom * 1e-10

    # Planck function: B_λ = (2hc²/λ⁵) / (exp(hc/λkT) - 1)
    # We include a factor of λ for photon-counting detectors (CCD)
    with np.errstate(over="ignore", divide="ignore"):
        exponent = H_PLANCK * C_LIGHT / (lam * K_BOLTZ * T)
        # Clip to avoid overflow
        exponent = np.clip(exponent, 0, 500)
        B = (2 * H_PLANCK * C_LIGHT ** 2 / lam ** 5) / (np.exp(exponent) - 1)

    # Multiply by lambda for photon counting
    B_photon = B * lam

    return B_photon


def synthetic_mag(
    grid: np.ndarray,
    transmission: np.ndarray,
    sed: np.ndarray,
) -> float:
    """
    Compute synthetic magnitude through a filter for a given SED.

    mag = -2.5 * log10(∫ SED(λ) T(λ) dλ / ∫ T(λ) dλ)

    Note: since we're computing magnitude DIFFERENCES between filters,
    the zero point cancels out. We use unnormalized T here (raw transmission)
    to correctly weight by filter throughput shape.
    """
    numerator = np.trapezoid(sed * transmission, grid)
    denominator = np.trapezoid(transmission, grid)

    if numerator <= 0 or denominator <= 0:
        return np.nan

    return -2.5 * np.log10(numerator / denominator)


def compute_systematics(
    band_filters: dict[str, list[dict]],
    temperatures: list[float] = SED_TEMPERATURES,
) -> dict[str, dict]:
    """
    For each band, compute synthetic magnitudes through all filters
    at each blackbody temperature. Returns per-band results.

    Returns dict mapping band -> {
        'filter_ids': list[str],
        'temperatures': list[float],
        'magnitudes': ndarray of shape (n_filters, n_temps),
        'spreads': ndarray of shape (n_temps,),  # max - min per temp
    }
    """
    results = {}

    for band, filters in sorted(band_filters.items()):
        if len(filters) < 2:
            continue

        # Determine common grid
        grid_min, grid_max = compute_band_grid_range(filters)
        grid = np.arange(grid_min, grid_max + INTERP_GRID_STEP, INTERP_GRID_STEP)

        # Interpolate raw (unnormalized) transmission curves onto common grid
        raw_curves = []
        for f in filters:
            interp_func = interp1d(
                f["wavelength"], np.clip(f["transmission"], 0, None),
                kind="linear", bounds_error=False, fill_value=0.0,
            )
            raw_curves.append(interp_func(grid))

        # Compute magnitudes
        n_f = len(filters)
        n_t = len(temperatures)
        mags = np.zeros((n_f, n_t))

        for j, T in enumerate(temperatures):
            sed = blackbody_flux(grid, T)
            for i in range(n_f):
                mags[i, j] = synthetic_mag(grid, raw_curves[i], sed)

        # Center magnitudes per temperature (subtract mean) so we see
        # the *relative* offsets between filters
        for j in range(n_t):
            col = mags[:, j]
            valid = ~np.isnan(col)
            if valid.sum() > 0:
                mags[valid, j] -= np.mean(col[valid])

        # Compute spread (max - min) per temperature in millimag
        spreads = np.zeros(n_t)
        for j in range(n_t):
            col = mags[:, j]
            valid = ~np.isnan(col)
            if valid.sum() >= 2:
                spreads[j] = (np.nanmax(col) - np.nanmin(col)) * 1000  # millimag

        results[band] = {
            "filter_ids": [f["filter_id"] for f in filters],
            "temperatures": temperatures,
            "magnitudes": mags,
            "spreads": spreads,
        }

        print(f"  {band}: spread = " +
              ", ".join(f"{s:.0f} mmag @ {T}K" for s, T in zip(spreads, temperatures, strict=False)))

    return results


def plot_systematics(sys_results: dict, output_dir: str):
    """
    Plot the magnitude spread vs temperature for each band.
    """
    bands = sorted(sys_results.keys())
    n_bands = len(bands)
    if n_bands == 0:
        return

    ncols = min(4, n_bands)
    nrows = (n_bands + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(4 * ncols, 3.5 * nrows), squeeze=False)

    for idx, band in enumerate(bands):
        ax = axes[idx // ncols][idx % ncols]
        data = sys_results[band]
        temps = data["temperatures"]
        spreads = data["spreads"]
        n_f = len(data["filter_ids"])

        ax.plot(temps, spreads, "bo-", linewidth=1.5, markersize=5)

        # Shade typical uncertainty ranges
        ax.axhspan(0, 10, color="green", alpha=0.1, label="< 10 mmag")
        ax.axhspan(10, 30, color="yellow", alpha=0.1, label="10-30 mmag")
        ax.axhspan(30, 100, color="red", alpha=0.05, label="> 30 mmag")

        ax.set_xlabel("Temperature (K)")
        ax.set_ylabel("Spread (mmag)")
        ax.set_title(f"{band}-band (n={n_f})")
        ax.set_xscale("log")
        ax.grid(alpha=0.3)
        ax.set_xlim(2500, 35000)

    # Remove empty subplots
    for idx in range(n_bands, nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    fig.suptitle(
        "Filter-to-Filter Systematic Spread by Blackbody Temperature\n"
        "(Green: negligible, Yellow: comparable to CCD noise, Red: significant)",
        fontsize=11,
    )
    plt.tight_layout()
    out_path = Path(output_dir) / "systematics_spread.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_systematics_detail(band: str, data: dict, band_filters: list[dict], output_dir: str):
    """
    Detailed plot for a single band: individual filter magnitude offsets
    at each temperature.
    """
    mags = data["magnitudes"]  # (n_filters, n_temps), centered
    fids = data["filter_ids"]
    temps = data["temperatures"]
    n_f = len(fids)

    if n_f < 2 or n_f > 50:
        return

    # Short labels
    short_ids = []
    for fid in fids:
        if "/" in fid:
            short_ids.append(fid.split("/", 1)[1])
        else:
            short_ids.append(fid)

    fig, ax = plt.subplots(1, 1, figsize=(max(8, n_f * 0.4), 6))

    x = np.arange(n_f)
    width = 0.8 / len(temps)

    for j, T in enumerate(temps):
        offsets = mags[:, j] * 1000  # millimag
        ax.bar(x + j * width, offsets, width, label=f"{T} K", alpha=0.7)

    ax.set_xticks(x + width * (len(temps) - 1) / 2)
    ax.set_xticklabels(short_ids, rotation=90, fontsize=max(5, 9 - n_f // 10))
    ax.set_ylabel("Magnitude Offset (mmag)")
    ax.set_title(f"{band}-band: Per-Filter Offsets Relative to Mean")
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.axhline(y=0, color="black", linewidth=0.5)

    plt.tight_layout()
    out_path = Path(output_dir) / f"systematics_detail_{band}.png"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def write_systematics_summary(sys_results: dict, output_dir: str):
    """Write CSV summary of systematics."""
    out_path = Path(output_dir) / "systematics_summary.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        header = ["band", "n_filters"]
        for T in SED_TEMPERATURES:
            header.append(f"spread_mmag_{T}K")
        writer.writerow(header)

        for band in sorted(sys_results.keys()):
            data = sys_results[band]
            row = [band, len(data["filter_ids"])]
            for s in data["spreads"]:
                row.append(f"{s:.1f}")
            writer.writerow(row)

    print(f"  Saved: {out_path}")


# ============================================================================
# Main
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="SVO filter band overlap analysis & AAVSO systematics",
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SVO database path")
    parser.add_argument("--custom-dir", default=None,
                        help="Directory containing custom filter CSVs")
    parser.add_argument("--custom-unit", default="A", choices=["A", "nm", "um"],
                        help="Wavelength unit of custom CSV files (default: Angstrom)")
    parser.add_argument("-o", "--output-dir", default=OUTPUT_DIR,
                        help="Output directory for plots and tables")
    parser.add_argument("--track", choices=["overlap", "systematics", "both"],
                        default="both", help="Which analysis track to run")
    parser.add_argument("--include-stromgren", action="store_true",
                        help="Include Stromgren bands (b, v, y)")
    parser.add_argument("--exclude", default=None,
                        help="JSON file of filter IDs to exclude (from svo_band_diagnostic.py)")
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Determine bands
    bands = CORE_BANDS + SLOAN_BANDS
    if args.include_stromgren:
        bands += STROMGREN_BANDS
    print(f"Target bands: {', '.join(bands)}")

    # Load filters
    print(f"\nLoading filters from {args.db} ...")
    band_filters = load_band_filters(args.db, bands)
    total = sum(len(v) for v in band_filters.values())
    print(f"  Loaded {total} filters across {len(band_filters)} bands")

    # Load custom filters
    if args.custom_dir:
        print(f"\nLoading custom filters from {args.custom_dir} ...")
        custom = load_custom_filters(args.custom_dir, args.custom_unit)
        band_filters = merge_filter_dicts(band_filters, custom)
        new_total = sum(len(v) for v in band_filters.values())
        print(f"  Total filters after merge: {new_total}")

    # Apply exclusion list
    if args.exclude:
        print(f"\nApplying exclusion list from {args.exclude} ...")
        with open(args.exclude) as f:
            exclusion_data = json.load(f)
        excluded_count = 0
        for band, excluded_ids in exclusion_data.get("bands", {}).items():
            if band in band_filters:
                excluded_set = set(excluded_ids)
                before = len(band_filters[band])
                band_filters[band] = [
                    f for f in band_filters[band]
                    if f["filter_id"] not in excluded_set
                ]
                removed = before - len(band_filters[band])
                excluded_count += removed
                if removed > 0:
                    print(f"  {band}: excluded {removed} outliers")
        print(f"  Total excluded: {excluded_count}")

    # Print band summary
    print("\nBand summary:")
    for band in bands:
        if band in band_filters:
            n = len(band_filters[band])
            custom_count = sum(1 for f in band_filters[band] if f["facility"] == "Custom")
            suffix = f" ({custom_count} custom)" if custom_count > 0 else ""
            print(f"  {band}: {n} filters{suffix}")
        else:
            print(f"  {band}: no filters found")

    # === Track 1: Overlap ===
    if args.track in ("overlap", "both"):
        print("\n" + "=" * 60)
        print("TRACK 1: Intra-Band Overlap Computation")
        print("=" * 60)

        overlap_results = compute_all_overlaps(band_filters)

        print("\nGenerating visualizations ...")
        plot_overlap_boxplots(overlap_results, args.output_dir)

        for band, data in overlap_results.items():
            if data["overlap_matrix"].shape[0] >= 3:
                plot_overlap_heatmap(band, data, args.output_dir)

        plot_overlap_cdf(overlap_results, args.output_dir)
        write_overlap_summary(overlap_results, args.output_dir)

    # === Track 2: Systematics ===
    if args.track in ("systematics", "both"):
        print("\n" + "=" * 60)
        print("TRACK 2: Synthetic Photometry Systematics")
        print("=" * 60)

        sys_results = compute_systematics(band_filters)

        print("\nGenerating visualizations ...")
        plot_systematics(sys_results, args.output_dir)

        for band, data in sys_results.items():
            if band in band_filters:
                plot_systematics_detail(band, data, band_filters[band], args.output_dir)

        write_systematics_summary(sys_results, args.output_dir)

    print("\n" + "=" * 60)
    print(f"Analysis complete! Output in: {args.output_dir}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
