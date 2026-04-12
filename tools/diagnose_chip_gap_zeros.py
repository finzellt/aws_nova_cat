#!/usr/bin/env python3
"""Diagnose chip-gap zero-runs in V906 Car UVES spectra.

Pulls web-ready CSVs (what the artifact generator sees) and optionally
the raw FITS files (full resolution) for all V906 Car UVES spectra.
For each spectrum:

  1. Identifies all runs of zero or near-zero flux
  2. Reports run length, wavelength position, and local spacing
  3. Generates per-spectrum plots with zero-runs highlighted

Operator tooling — no CI requirements.

Usage:
    # Basic — pull web-ready CSVs and analyze
    python diagnose_chip_gap_zeros.py

    # Also pull raw FITS for full-resolution comparison
    python diagnose_chip_gap_zeros.py --fits

    # Limit to N spectra (useful for quick check)
    python diagnose_chip_gap_zeros.py --limit 3

    # Custom output directory
    python diagnose_chip_gap_zeros.py --outdir /tmp/chipgap

Environment variables:
    NOVACAT_TABLE_NAME     — DynamoDB table (default: reads from env)
    NOVACAT_PRIVATE_BUCKET — S3 bucket (default: reads from env)
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from pathlib import Path

import boto3
import matplotlib.pyplot as plt
import numpy as np
from boto3.dynamodb.conditions import Key

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NOVA_NAME = "V906 Car"
# Relative threshold: fraction of peak flux below which a value is "near-zero"
# (matches RELATIVE_ZERO_FRACTION in shared.py)
RELATIVE_ZERO_FRACTION = 1e-6


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------


def resolve_nova_id(table, name: str) -> str | None:
    """Resolve a nova name to nova_id via NameMapping."""
    normalized = name.strip().lower().replace("_", " ")
    pk = f"NAME#{normalized}"
    resp = table.query(KeyConditionExpression=Key("PK").eq(pk), Limit=1)
    items = resp.get("Items", [])
    return items[0]["nova_id"] if items else None


def query_uves_spectra(table, nova_id: str) -> list[dict]:
    """Query all VALID UVES DataProduct items for a nova."""
    items: list[dict] = []
    kwargs = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("DATAPRODUCT#SPECTRA#")
        ),
    }
    while True:
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            if item.get("instrument") == "UVES" and item.get("validation_status") == "VALID":
                items.append(item)
        last_key = resp.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def download_web_ready_csv(s3, bucket: str, nova_id: str, dp_id: str) -> str | None:
    """Download a web-ready CSV from S3, return contents or None."""
    key = f"derived/spectra/{nova_id}/{dp_id}/web_ready.csv"
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read().decode("utf-8")
    except Exception as e:
        print(f"  [WARN] Could not download {key}: {e}")
        return None


def download_raw_fits(s3, bucket: str, raw_s3_key: str) -> bytes | None:
    """Download a raw FITS file from S3, return bytes or None."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=raw_s3_key)
        return resp["Body"].read()
    except Exception as e:
        print(f"  [WARN] Could not download FITS {raw_s3_key}: {e}")
        return None


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def parse_web_ready_csv(body: str) -> tuple[np.ndarray, np.ndarray]:
    """Parse a web-ready CSV into wavelength and flux arrays."""
    reader = csv.reader(io.StringIO(body))
    header = next(reader, None)
    if not header:
        return np.array([]), np.array([])

    wl_list: list[float] = []
    fx_list: list[float] = []
    for row in reader:
        if len(row) < 2:
            continue
        try:
            wl_list.append(float(row[0]))
            fx_list.append(float(row[1]))
        except ValueError:
            continue
    return np.array(wl_list), np.array(fx_list)


# ---------------------------------------------------------------------------
# FITS extraction (optional, for full-resolution comparison)
# ---------------------------------------------------------------------------


def extract_fits_spectrum(fits_bytes: bytes) -> tuple[np.ndarray, np.ndarray] | None:
    """Extract wavelength/flux from a FITS file. Returns None on failure."""
    try:
        from astropy.io import fits

        with fits.open(io.BytesIO(fits_bytes), memmap=False) as hdul:
            primary = hdul[0]

            # Try primary image HDU with WCS
            if primary.data is not None and primary.data.ndim >= 1:
                flux = np.asarray(primary.data, dtype=np.float64).flatten()
                crval1 = float(primary.header.get("CRVAL1", 0.0))
                cdelt1 = float(primary.header.get("CDELT1", 1.0))
                crpix1 = float(primary.header.get("CRPIX1", 1.0))
                n_pix = len(flux)
                wavelength = crval1 + cdelt1 * (np.arange(n_pix) - (crpix1 - 1.0))
                # Convert Angstrom to nm if needed
                if crval1 > 1000:  # likely Angstrom
                    wavelength = wavelength / 10.0
                return wavelength, flux

            # Try binary table
            if len(hdul) > 1 and hasattr(hdul[1], "columns"):
                tbl = hdul[1].data
                col_names = [c.upper() for c in hdul[1].columns.names]
                wave_col = flux_col = None
                for cand in ("WAVE", "WAVELENGTH", "LAMBDA", "WAVE_AIR"):
                    if cand in col_names:
                        wave_col = hdul[1].columns.names[col_names.index(cand)]
                        break
                for cand in ("FLUX", "FLUX_REDUCED", "FLUX_OPT"):
                    if cand in col_names:
                        flux_col = hdul[1].columns.names[col_names.index(cand)]
                        break
                if wave_col and flux_col:
                    wl = np.asarray(tbl[wave_col], dtype=np.float64).flatten()
                    fx = np.asarray(tbl[flux_col], dtype=np.float64).flatten()
                    # Convert Angstrom to nm if needed
                    unit = hdul[1].header.get("TUNIT1", "") or primary.header.get("CUNIT1", "")
                    if "ang" in unit.lower() or (len(wl) > 0 and wl[0] > 1000):
                        wl = wl / 10.0
                    return wl, fx

        return None
    except Exception as e:
        print(f"  [WARN] FITS extraction failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Zero-run analysis
# ---------------------------------------------------------------------------


def find_zero_runs(
    wavelengths: np.ndarray,
    fluxes: np.ndarray,
) -> list[dict]:
    """Find all runs of zero/near-zero flux in the spectrum.

    Returns a list of dicts, each describing one run:
      - start_idx, end_idx: indices (inclusive start, exclusive end)
      - length: number of points in the run
      - wl_start, wl_end: wavelength range of the run
      - wl_center: center wavelength
      - is_edge: whether the run touches the array boundary
      - local_spacing: median wavelength step in the run's neighborhood
      - spacing_is_regular: whether the run's spacing matches the local median
      - max_flux_in_run: the largest |flux| value in the run
    """
    n = len(fluxes)
    if n == 0:
        return []

    peak = np.max(np.abs(fluxes))
    if peak == 0.0:
        return [{"note": "ALL_ZERO", "length": n}]

    threshold = peak * RELATIVE_ZERO_FRACTION

    # Also check for exact zeros separately
    is_dead = np.abs(fluxes) < threshold
    is_exact_zero = fluxes == 0.0

    runs: list[dict] = []

    # Find contiguous runs of dead pixels
    in_run = False
    run_start = 0
    for i in range(n):
        if is_dead[i] or is_exact_zero[i]:
            if not in_run:
                run_start = i
                in_run = True
        else:
            if in_run:
                _append_run(runs, wavelengths, fluxes, run_start, i, n, is_exact_zero)
                in_run = False
    if in_run:
        _append_run(runs, wavelengths, fluxes, run_start, n, n, is_exact_zero)

    return runs


def _append_run(
    runs: list[dict],
    wavelengths: np.ndarray,
    fluxes: np.ndarray,
    start: int,
    end: int,
    n: int,
    is_exact_zero: np.ndarray,
) -> None:
    """Append a run descriptor to the runs list."""
    length = end - start
    is_edge = start == 0 or end == n

    # Local spacing: look at 20 points around the run
    ctx_start = max(0, start - 10)
    ctx_end = min(n, end + 10)
    if ctx_end - ctx_start > 1:
        local_steps = np.diff(wavelengths[ctx_start:ctx_end])
        local_median_step = float(np.median(local_steps))
    else:
        local_median_step = float("nan")

    # Spacing within the run itself
    if length > 1:
        run_steps = np.diff(wavelengths[start:end])
        run_median_step = float(np.median(run_steps))
        # "Regular" if within 50% of local median
        spacing_is_regular = (
            abs(run_median_step - local_median_step) / max(local_median_step, 1e-12) < 0.5
        )
    else:
        run_median_step = float("nan")
        # For single-point runs, check gap to neighbors
        if start > 0 and end < n:
            gap_left = wavelengths[start] - wavelengths[start - 1]
            gap_right = wavelengths[end] - wavelengths[end - 1] if end < n else float("inf")
            spacing_is_regular = (
                abs(gap_left - local_median_step) / max(local_median_step, 1e-12) < 0.5
                and abs(gap_right - local_median_step) / max(local_median_step, 1e-12) < 0.5
            )
        else:
            spacing_is_regular = True  # edge — can't tell

    exact_zero_count = int(np.sum(is_exact_zero[start:end]))

    runs.append(
        {
            "start_idx": start,
            "end_idx": end,
            "length": length,
            "wl_start": float(wavelengths[start]),
            "wl_end": float(wavelengths[end - 1]),
            "wl_center": float(np.mean(wavelengths[start:end])),
            "is_edge": is_edge,
            "local_median_step_nm": round(local_median_step, 6),
            "run_median_step_nm": round(run_median_step, 6) if length > 1 else None,
            "spacing_is_regular": spacing_is_regular,
            "max_flux_in_run": float(np.max(np.abs(fluxes[start:end]))),
            "exact_zero_count": exact_zero_count,
            "all_exact_zeros": exact_zero_count == length,
        }
    )


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def plot_spectrum_with_zeros(
    wavelengths: np.ndarray,
    fluxes: np.ndarray,
    runs: list[dict],
    dp_id: str,
    label: str,
    outdir: Path,
) -> None:
    """Plot a spectrum with zero-runs highlighted in red."""
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), gridspec_kw={"height_ratios": [3, 1]})

    # Top: full spectrum
    ax_full = axes[0]
    ax_full.plot(wavelengths, fluxes, linewidth=0.5, color="steelblue", alpha=0.8)
    for run in runs:
        if "start_idx" not in run:
            continue
        s, e = run["start_idx"], run["end_idx"]
        ax_full.axvspan(
            wavelengths[s],
            wavelengths[min(e, len(wavelengths) - 1)],
            color="red",
            alpha=0.3,
            label=f"zero run (n={run['length']})" if s == runs[0].get("start_idx") else None,
        )
    ax_full.set_xlabel("Wavelength (nm)")
    ax_full.set_ylabel("Flux")
    ax_full.set_title(f"{label}\n{dp_id[:13]}  —  {len(runs)} zero-run(s) found")
    ax_full.legend(loc="upper right", fontsize=8)

    # Bottom: zoom into chip-gap region (~520–620 nm for UVES red arm)
    ax_zoom = axes[1]
    # Find runs near the chip gap (roughly 520-620 nm)
    interior_runs = [r for r in runs if "start_idx" in r and not r["is_edge"]]
    if interior_runs:
        # Zoom around the first interior run
        center = interior_runs[0]["wl_center"]
        zoom_half = 15.0  # ±15 nm
        mask = (wavelengths >= center - zoom_half) & (wavelengths <= center + zoom_half)
        if np.any(mask):
            ax_zoom.plot(
                wavelengths[mask],
                fluxes[mask],
                linewidth=1.0,
                color="steelblue",
                marker=".",
                markersize=3,
            )
            for run in interior_runs:
                s, e = run["start_idx"], run["end_idx"]
                wl_s = wavelengths[s]
                wl_e = wavelengths[min(e, len(wavelengths) - 1)]
                if wl_e >= center - zoom_half and wl_s <= center + zoom_half:
                    ax_zoom.axvspan(wl_s, wl_e, color="red", alpha=0.3)
            ax_zoom.set_xlabel("Wavelength (nm)")
            ax_zoom.set_ylabel("Flux")
            ax_zoom.set_title(
                f"Zoom: {center - zoom_half:.1f}–{center + zoom_half:.1f} nm (around first interior zero-run)"
            )
        else:
            ax_zoom.text(
                0.5, 0.5, "No data in chip-gap zoom range", transform=ax_zoom.transAxes, ha="center"
            )
    else:
        ax_zoom.text(
            0.5, 0.5, "No interior zero-runs found", transform=ax_zoom.transAxes, ha="center"
        )

    plt.tight_layout()
    safe_id = dp_id[:13].replace("-", "")
    plt.savefig(outdir / f"chipgap_{safe_id}_{label.lower().replace(' ', '_')}.png", dpi=150)
    plt.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose chip-gap zeros in V906 Car UVES spectra")
    parser.add_argument(
        "--fits", action="store_true", help="Also download raw FITS for full-res comparison"
    )
    parser.add_argument("--limit", type=int, default=0, help="Limit to N spectra (0 = all)")
    parser.add_argument(
        "--outdir", type=str, default="chipgap_diag", help="Output directory for plots"
    )
    args = parser.parse_args()

    table_name = os.environ.get("NOVACAT_TABLE_NAME")
    bucket_name = os.environ.get("NOVACAT_PRIVATE_BUCKET")
    if not table_name or not bucket_name:
        print("ERROR: Set NOVACAT_TABLE_NAME and NOVACAT_PRIVATE_BUCKET env vars.")
        print("       (These are set automatically after running deploy.sh)")
        sys.exit(1)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    ddb = boto3.resource("dynamodb")
    table = ddb.Table(table_name)
    s3 = boto3.client("s3")

    # Resolve nova
    nova_id = resolve_nova_id(table, NOVA_NAME)
    if not nova_id:
        print(f"ERROR: Could not resolve '{NOVA_NAME}' to a nova_id.")
        sys.exit(1)
    print(f"Nova: {NOVA_NAME}  →  {nova_id}")

    # Query UVES spectra
    products = query_uves_spectra(table, nova_id)
    print(f"Found {len(products)} VALID UVES spectra")

    if args.limit > 0:
        products = products[: args.limit]
        print(f"  (limited to {args.limit})")

    # Summary accumulators
    all_summaries: list[dict] = []

    for i, product in enumerate(products):
        dp_id = product["data_product_id"]
        obs_mjd = product.get("observation_date_mjd", "?")
        print(f"\n{'=' * 70}")
        print(f"[{i + 1}/{len(products)}] {dp_id[:13]}...  MJD={obs_mjd}")
        print(f"{'=' * 70}")

        # --- Web-ready CSV analysis ---
        csv_body = download_web_ready_csv(s3, bucket_name, nova_id, dp_id)
        if csv_body:
            wl_csv, fx_csv = parse_web_ready_csv(csv_body)
            print(f"  Web-ready CSV: {len(wl_csv)} points")
            print(f"    Flux range: [{fx_csv.min():.6e}, {fx_csv.max():.6e}]")
            print(f"    Exact zeros: {np.sum(fx_csv == 0.0)}")

            if len(wl_csv) > 1:
                steps = np.diff(wl_csv)
                print(
                    f"    WL step: median={np.median(steps):.4f} nm, "
                    f"min={steps.min():.4f} nm, max={steps.max():.4f} nm"
                )

            runs_csv = find_zero_runs(wl_csv, fx_csv)
            interior_runs = [r for r in runs_csv if "start_idx" in r and not r["is_edge"]]
            edge_runs = [r for r in runs_csv if "start_idx" in r and r["is_edge"]]

            print(
                f"    Zero-runs: {len(runs_csv)} total, "
                f"{len(interior_runs)} interior, {len(edge_runs)} edge"
            )

            for j, run in enumerate(interior_runs):
                print(f"    INTERIOR RUN {j + 1}:")
                print(
                    f"      Position: {run['wl_start']:.2f}–{run['wl_end']:.2f} nm "
                    f"(center: {run['wl_center']:.2f} nm)"
                )
                print(f"      Length: {run['length']} points")
                print(
                    f"      All exact zeros: {run['all_exact_zeros']} "
                    f"({run['exact_zero_count']}/{run['length']} exact)"
                )
                print(
                    f"      Spacing regular: {run['spacing_is_regular']} "
                    f"(local median step: {run['local_median_step_nm']:.4f} nm"
                    + (
                        f", run step: {run['run_median_step_nm']:.4f} nm"
                        if run["run_median_step_nm"]
                        else ""
                    )
                    + ")"
                )
                print(f"      Max |flux| in run: {run['max_flux_in_run']:.6e}")

            plot_spectrum_with_zeros(wl_csv, fx_csv, runs_csv, dp_id, "web_ready", outdir)

            all_summaries.append(
                {
                    "dp_id": dp_id[:13],
                    "mjd": str(obs_mjd),
                    "csv_points": len(wl_csv),
                    "exact_zeros": int(np.sum(fx_csv == 0.0)),
                    "interior_runs": len(interior_runs),
                    "interior_run_lengths": [r["length"] for r in interior_runs],
                    "interior_run_centers_nm": [round(r["wl_center"], 1) for r in interior_runs],
                    "interior_spacing_regular": [r["spacing_is_regular"] for r in interior_runs],
                }
            )
        else:
            print("  [SKIP] No web-ready CSV")
            continue

        # --- Optional: raw FITS analysis ---
        if args.fits:
            raw_key = product.get("raw_s3_key")
            if raw_key:
                fits_bytes = download_raw_fits(s3, bucket_name, raw_key)
                if fits_bytes:
                    result = extract_fits_spectrum(fits_bytes)
                    if result:
                        wl_fits, fx_fits = result
                        print(f"  Raw FITS: {len(wl_fits)} points")
                        print(f"    Flux range: [{fx_fits.min():.6e}, {fx_fits.max():.6e}]")
                        print(f"    Exact zeros: {np.sum(fx_fits == 0.0)}")

                        runs_fits = find_zero_runs(wl_fits, fx_fits)
                        interior_fits = [
                            r for r in runs_fits if "start_idx" in r and not r["is_edge"]
                        ]
                        print(
                            f"    Zero-runs: {len(runs_fits)} total, {len(interior_fits)} interior"
                        )

                        for j, run in enumerate(interior_fits):
                            print(
                                f"    FITS INTERIOR RUN {j + 1}: "
                                f"{run['wl_start']:.2f}–{run['wl_end']:.2f} nm, "
                                f"len={run['length']}, "
                                f"regular_spacing={run['spacing_is_regular']}, "
                                f"all_exact_zeros={run['all_exact_zeros']}"
                            )

                        plot_spectrum_with_zeros(
                            wl_fits, fx_fits, runs_fits, dp_id, "raw_fits", outdir
                        )

    # --- Summary table ---
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(
        f"{'dp_id':>15s} {'MJD':>12s} {'pts':>6s} {'zeros':>6s} {'int_runs':>9s} {'run_lens':>20s} {'centers_nm':>30s} {'regular?':>20s}"
    )
    print("-" * 120)
    for s in all_summaries:
        print(
            f"{s['dp_id']:>15s} {s['mjd']:>12s} {s['csv_points']:>6d} {s['exact_zeros']:>6d} "
            f"{s['interior_runs']:>9d} {str(s['interior_run_lengths']):>20s} "
            f"{str(s['interior_run_centers_nm']):>30s} {str(s['interior_spacing_regular']):>20s}"
        )

    print(f"\nPlots saved to: {outdir.resolve()}")
    print("Done.")


if __name__ == "__main__":
    main()
