#!/usr/bin/env python3
"""Diagnose X-Shooter spectra skipped during artifact generation.

Queries DynamoDB for VALID spectra DataProducts with instrument matching
'XSHOOTER' or 'X-Shooter', then for each (up to --limit):

  1. Downloads the web-ready CSV from S3
  2. Downloads the raw FITS file from S3
  3. Inspects the FITS HDU structure
  4. Reports flux statistics (min, max, mean, % near-zero)
  5. Runs the same _trim_dead_edges logic used by the generator
  6. Plots raw FITS flux + web-ready CSV flux side-by-side

Output:
  - Console summary table
  - Per-spectrum PNG plots in --output-dir

Usage:
  python tools/diagnose_xshooter.py
  python tools/diagnose_xshooter.py --limit 5 --output-dir ./xshooter_diag
  python tools/diagnose_xshooter.py --nova <nova_id>

Requires:
  pip install astropy matplotlib boto3

Environment variables (set by deploy.sh):
  NOVACAT_TABLE_NAME
  NOVACAT_PRIVATE_BUCKET
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
from astropy.io import fits
from boto3.dynamodb.conditions import Attr, Key

# ---------------------------------------------------------------------------
# Constants (mirrored from generators/spectra.py)
# ---------------------------------------------------------------------------

_ZERO_THRESHOLD = 1e-10


# ---------------------------------------------------------------------------
# Edge trimming (copied from generators/spectra.py for local replay)
# ---------------------------------------------------------------------------


def trim_dead_edges(
    wavelengths: list[float],
    fluxes: list[float],
) -> tuple[list[float], list[float], int, int]:
    """Replay the generator's edge trimming logic.

    Returns (trimmed_wl, trimmed_fx, blue_trim_count, red_trim_count).
    """
    n = len(fluxes)
    if n == 0:
        return wavelengths, fluxes, 0, 0

    blue_zeros = 0
    for f in fluxes:
        if abs(f) < _ZERO_THRESHOLD:
            blue_zeros += 1
        else:
            break
    blue_trim = blue_zeros if blue_zeros > 1 else 0

    red_zeros = 0
    for f in reversed(fluxes):
        if abs(f) < _ZERO_THRESHOLD:
            red_zeros += 1
        else:
            break
    red_trim = red_zeros if red_zeros > 1 else 0

    end = n - red_trim if red_trim else n
    return wavelengths[blue_trim:end], fluxes[blue_trim:end], blue_trim, red_trim


# ---------------------------------------------------------------------------
# S3 / DDB helpers
# ---------------------------------------------------------------------------


def query_xshooter_products(
    table_resource,
    nova_id: str | None = None,
) -> list[dict]:
    """Query VALID SPECTRA DataProducts, filter for X-Shooter instrument."""
    if nova_id:
        nova_ids = [nova_id]
    else:
        # Scan for all novae that have spectra (not ideal at scale, fine for diagnostics)
        nova_ids = _discover_nova_ids_with_spectra(table_resource)

    results: list[dict] = []
    for nid in nova_ids:
        kwargs = {
            "KeyConditionExpression": (
                Key("PK").eq(nid) & Key("SK").begins_with("PRODUCT#SPECTRA#")
            ),
            "FilterExpression": Attr("validation_status").eq("VALID"),
        }
        while True:
            response = table_resource.query(**kwargs)
            for item in response.get("Items", []):
                instrument = (item.get("instrument") or "").upper()
                if "XSHOO" in instrument or "X-SHOOTER" in instrument.replace(" ", "-"):
                    results.append(item)
            last_key = response.get("LastEvaluatedKey")
            if last_key is None:
                break
            kwargs["ExclusiveStartKey"] = last_key

    return results


def _discover_nova_ids_with_spectra(table_resource) -> list[str]:
    """Scan for distinct nova_ids that have SPECTRA products."""
    nova_ids: set[str] = set()
    # Query the GSI for all nova items, then check for spectra
    # Simpler: scan for items where SK begins with PRODUCT#SPECTRA
    # For diagnostic purposes, just scan for NOVA items and collect PKs
    kwargs = {
        "FilterExpression": Attr("SK").eq("NOVA") & Attr("status").eq("ACTIVE"),
        "ProjectionExpression": "PK",
    }
    while True:
        response = table_resource.scan(**kwargs)
        for item in response.get("Items", []):
            nova_ids.add(item["PK"])
        last_key = response.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key

    return sorted(nova_ids)


def download_web_ready_csv(
    s3_client, bucket: str, nova_id: str, dp_id: str
) -> tuple[list[float], list[float]] | None:
    """Download and parse the web-ready CSV. Returns (wavelengths, fluxes) or None."""
    key = f"derived/spectra/{nova_id}/{dp_id}/web_ready.csv"
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
    except Exception as exc:
        print(f"    [CSV] Missing: {key} — {exc}")
        return None

    reader = csv.reader(io.StringIO(body))
    next(reader, None)  # skip header

    wavelengths: list[float] = []
    fluxes: list[float] = []
    for row in reader:
        if len(row) >= 2:
            try:
                wavelengths.append(float(row[0]))
                fluxes.append(float(row[1]))
            except ValueError:
                continue
    return wavelengths, fluxes


def download_fits(
    s3_client,
    bucket: str,
    nova_id: str,
    dp_id: str,
    raw_s3_key: str | None = None,
    raw_s3_bucket: str | None = None,
) -> bytes | None:
    """Download the raw FITS file. Tries the DataProduct's stored key first,
    then falls back to the legacy path."""
    # Try stored path first
    attempts: list[tuple[str, str]] = []
    if raw_s3_key and raw_s3_bucket:
        attempts.append((raw_s3_bucket, raw_s3_key))
    # Legacy archive path
    attempts.append((bucket, f"raw/spectra/{nova_id}/{dp_id}/primary.fits"))
    # Ticket ingestion path (public bucket — skip, we don't know the name)

    for try_bucket, try_key in attempts:
        try:
            resp = s3_client.get_object(Bucket=try_bucket, Key=try_key)
            print(f"    [FITS] Found: s3://{try_bucket}/{try_key}")
            return resp["Body"].read()
        except Exception:
            continue

    print(f"    [FITS] Not found for {dp_id} (tried {len(attempts)} paths)")
    return None


# ---------------------------------------------------------------------------
# FITS inspection
# ---------------------------------------------------------------------------


def inspect_fits(fits_bytes: bytes) -> dict:
    """Extract HDU structure and flux data from FITS bytes."""
    info: dict = {"hdus": [], "flux_data": None, "wavelength_data": None}

    with io.BytesIO(fits_bytes) as buf:
        with fits.open(buf) as hdul:
            for i, hdu in enumerate(hdul):
                hdu_info = {
                    "index": i,
                    "type": type(hdu).__name__,
                    "name": hdu.name,
                    "shape": getattr(hdu, "data", None) is not None
                    and hasattr(hdu.data, "shape")
                    and hdu.data.shape
                    or None,
                    "columns": None,
                }

                # Check for table columns
                if hasattr(hdu, "columns") and hdu.columns is not None:
                    hdu_info["columns"] = [f"{c.name} ({c.format})" for c in hdu.columns]

                # Try to extract flux data
                if hdu.data is not None:
                    if hasattr(hdu, "columns") and hdu.columns is not None:
                        # Binary table — look for FLUX column
                        col_names = [c.name.upper() for c in hdu.columns]
                        for flux_col in ["FLUX", "FLUX_REDUCED", "FLUX_OPT", "DATA"]:
                            if flux_col in col_names:
                                idx = col_names.index(flux_col)
                                real_name = hdu.columns[idx].name
                                info["flux_data"] = np.array(
                                    hdu.data[real_name], dtype=float
                                ).flatten()
                                break
                        for wl_col in [
                            "WAVE",
                            "WAVELENGTH",
                            "LAMBDA",
                            "WAVE_AIR",
                            "WAVE_VAC",
                        ]:
                            if wl_col in col_names:
                                idx = col_names.index(wl_col)
                                real_name = hdu.columns[idx].name
                                info["wavelength_data"] = np.array(
                                    hdu.data[real_name], dtype=float
                                ).flatten()
                                break
                    elif hdu.data.ndim == 1 and info["flux_data"] is None:
                        # Image HDU with 1D data — likely flux
                        info["flux_data"] = np.array(hdu.data, dtype=float).flatten()
                    elif hdu.data.ndim == 2 and info["flux_data"] is None:
                        # 2D image — take first row as flux
                        info["flux_data"] = np.array(hdu.data[0], dtype=float).flatten()

                # Extract wavelength from header WCS if not found in table
                if (
                    info["wavelength_data"] is None
                    and hdu.data is not None
                    and info["flux_data"] is not None
                    and not hasattr(hdu, "columns")
                ):
                    header = hdu.header
                    crval = header.get("CRVAL1")
                    cdelt = header.get("CDELT1") or header.get("CD1_1")
                    crpix = header.get("CRPIX1", 1.0)
                    naxis1 = header.get("NAXIS1") or (len(info["flux_data"]))
                    if crval is not None and cdelt is not None:
                        info["wavelength_data"] = crval + (np.arange(naxis1) - (crpix - 1)) * cdelt

                info["hdus"].append(hdu_info)

                # Capture some header keywords
                if i == 0 or hdu.name == "PRIMARY":
                    header = hdu.header
                    info["instrument"] = header.get("INSTRUME", "?")
                    info["telescope"] = header.get("TELESCOP", "?")
                    info["date_obs"] = header.get("DATE-OBS", "?")
                    info["object"] = header.get("OBJECT", "?")

    return info


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def plot_spectrum_diagnostic(
    dp_id: str,
    nova_name: str,
    csv_wl: list[float] | None,
    csv_fx: list[float] | None,
    fits_wl: np.ndarray | None,
    fits_fx: np.ndarray | None,
    trim_result: dict | None,
    output_dir: Path,
) -> None:
    """Generate a diagnostic plot with up to 3 panels."""
    panels = 0
    if fits_fx is not None:
        panels += 1
    if csv_wl is not None:
        panels += 1
    if trim_result is not None:
        panels += 1

    if panels == 0:
        return

    fig, axes = plt.subplots(max(panels, 1), 1, figsize=(14, 4 * max(panels, 1)))
    if panels == 1:
        axes = [axes]

    ax_idx = 0

    # Panel 1: Raw FITS
    if fits_fx is not None:
        ax = axes[ax_idx]
        ax_idx += 1
        x = fits_wl if fits_wl is not None else np.arange(len(fits_fx))
        ax.plot(x, fits_fx, linewidth=0.5, color="#0072B2", alpha=0.8)
        ax.set_title(f"Raw FITS flux — {dp_id[:12]}…", fontsize=10)
        ax.set_xlabel("Wavelength" if fits_wl is not None else "Pixel index")
        ax.set_ylabel("Flux (raw units)")
        ax.ticklabel_format(axis="y", style="scientific", scilimits=(-3, 3))

        # Mark the zero threshold
        ax.axhline(
            y=_ZERO_THRESHOLD,
            color="red",
            linestyle="--",
            linewidth=0.8,
            label=f"ZERO_THRESHOLD = {_ZERO_THRESHOLD:.0e}",
        )
        ax.axhline(y=-_ZERO_THRESHOLD, color="red", linestyle="--", linewidth=0.8)
        ax.legend(fontsize=8)

    # Panel 2: Web-ready CSV
    if csv_wl is not None and csv_fx is not None:
        ax = axes[ax_idx]
        ax_idx += 1
        ax.plot(csv_wl, csv_fx, linewidth=0.5, color="#009E73", alpha=0.8)
        ax.set_title(f"Web-ready CSV flux — {dp_id[:12]}…", fontsize=10)
        ax.set_xlabel("Wavelength (nm)")
        ax.set_ylabel("Flux")
        ax.ticklabel_format(axis="y", style="scientific", scilimits=(-3, 3))

        ax.axhline(
            y=_ZERO_THRESHOLD,
            color="red",
            linestyle="--",
            linewidth=0.8,
            label=f"ZERO_THRESHOLD = {_ZERO_THRESHOLD:.0e}",
        )
        ax.legend(fontsize=8)

        # Highlight near-zero regions
        near_zero = [abs(f) < _ZERO_THRESHOLD for f in csv_fx]
        if any(near_zero):
            for i, is_zero in enumerate(near_zero):
                if is_zero:
                    ax.axvspan(
                        csv_wl[max(0, i - 1)],
                        csv_wl[min(len(csv_wl) - 1, i + 1)],
                        alpha=0.15,
                        color="red",
                    )

    # Panel 3: After trim
    if trim_result is not None and trim_result["trimmed_wl"]:
        ax = axes[ax_idx]
        ax_idx += 1
        ax.plot(
            trim_result["trimmed_wl"],
            trim_result["trimmed_fx"],
            linewidth=0.5,
            color="#D55E00",
            alpha=0.8,
        )
        ax.set_title(
            f"After edge trim — blue={trim_result['blue_trim']}, "
            f"red={trim_result['red_trim']}, "
            f"remaining={len(trim_result['trimmed_wl'])} pts",
            fontsize=10,
        )
        ax.set_xlabel("Wavelength (nm)")
        ax.set_ylabel("Flux")
        ax.ticklabel_format(axis="y", style="scientific", scilimits=(-3, 3))

    fig.suptitle(f"{nova_name} — X-Shooter diagnostic", fontsize=12, fontweight="bold")
    fig.tight_layout()

    output_path = output_dir / f"xshooter_diag_{dp_id[:16]}.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    [PLOT] Saved: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--limit", type=int, default=10, help="Max number of spectra to diagnose (default: 10)"
    )
    parser.add_argument("--nova", type=str, default=None, help="Restrict to a specific nova_id")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./xshooter_diag",
        help="Directory for output plots (default: ./xshooter_diag)",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=os.environ.get("NOVACAT_TABLE_NAME"),
        help="DynamoDB table name (default: $NOVACAT_TABLE_NAME)",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.environ.get("NOVACAT_PRIVATE_BUCKET"),
        help="Private S3 bucket (default: $NOVACAT_PRIVATE_BUCKET)",
    )
    args = parser.parse_args()

    if not args.table:
        print("ERROR: --table or NOVACAT_TABLE_NAME env var required.", file=sys.stderr)
        sys.exit(1)
    if not args.bucket:
        print("ERROR: --bucket or NOVACAT_PRIVATE_BUCKET env var required.", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.table)
    s3_client = boto3.client("s3")

    print(f"Querying for X-Shooter spectra (table={args.table})...")
    products = query_xshooter_products(table, nova_id=args.nova)
    print(f"Found {len(products)} X-Shooter DataProduct items.\n")

    if not products:
        print("No X-Shooter spectra found. Nothing to diagnose.")
        return

    # Summary table header
    print(
        f"{'#':<4} {'Nova ID':<38} {'DP ID':<38} {'CSV pts':>8} "
        f"{'CSV zeros%':>10} {'Blue trim':>10} {'Red trim':>10} {'Survives?':>10}"
    )
    print("-" * 160)

    diagnosed = 0
    skipped_count = 0
    survived_count = 0

    for product in products[: args.limit]:
        diagnosed += 1
        nova_id = product["PK"]
        dp_id = product["data_product_id"]
        nova_name = product.get("object_name", nova_id[:12])

        print(
            f"\n[{diagnosed}/{min(len(products), args.limit)}] "
            f"Nova={nova_id[:12]}… DP={dp_id[:12]}… "
            f"Instrument={product.get('instrument', '?')}"
        )

        # --- Web-ready CSV ---
        csv_result = download_web_ready_csv(s3_client, args.bucket, nova_id, dp_id)
        csv_wl = csv_fx = None
        csv_points = 0
        csv_zero_pct = 0.0
        if csv_result:
            csv_wl, csv_fx = csv_result
            csv_points = len(csv_wl)
            if csv_points > 0:
                near_zero = sum(1 for f in csv_fx if abs(f) < _ZERO_THRESHOLD)
                csv_zero_pct = 100.0 * near_zero / csv_points

        # --- Edge trimming replay ---
        trim_result = None
        blue_trim = red_trim = 0
        survives = "N/A"
        if csv_wl and csv_fx:
            trimmed_wl, trimmed_fx, blue_trim, red_trim = trim_dead_edges(csv_wl, csv_fx)
            survives = "YES" if trimmed_wl else "NO"
            if trimmed_wl:
                survived_count += 1
            else:
                skipped_count += 1
            trim_result = {
                "trimmed_wl": trimmed_wl,
                "trimmed_fx": trimmed_fx,
                "blue_trim": blue_trim,
                "red_trim": red_trim,
            }

        # --- Raw FITS ---
        fits_bytes = download_fits(
            s3_client,
            args.bucket,
            nova_id,
            dp_id,
            raw_s3_key=product.get("raw_s3_key"),
            raw_s3_bucket=product.get("raw_s3_bucket"),
        )
        fits_info = None
        fits_fx = fits_wl = None
        if fits_bytes:
            fits_info = inspect_fits(fits_bytes)
            fits_fx = fits_info.get("flux_data")
            fits_wl = fits_info.get("wavelength_data")

            print(f"    [FITS] HDUs: {len(fits_info['hdus'])}")
            for hdu in fits_info["hdus"]:
                cols = f"  columns={hdu['columns']}" if hdu["columns"] else ""
                print(
                    f"           [{hdu['index']}] {hdu['type']} "
                    f"name={hdu['name']} shape={hdu['shape']}{cols}"
                )

            if fits_fx is not None:
                fx_arr = fits_fx[np.isfinite(fits_fx)]
                if len(fx_arr) > 0:
                    print(
                        f"    [FITS] Flux stats: min={fx_arr.min():.4e}, "
                        f"max={fx_arr.max():.4e}, mean={fx_arr.mean():.4e}, "
                        f"points={len(fx_arr)}, "
                        f"near-zero(<{_ZERO_THRESHOLD:.0e})="
                        f"{np.sum(np.abs(fx_arr) < _ZERO_THRESHOLD)}"
                    )

        # Summary row
        print(
            f"{'':4} {nova_id:<38} {dp_id:<38} {csv_points:>8} "
            f"{csv_zero_pct:>9.1f}% {blue_trim:>10} {red_trim:>10} {survives:>10}"
        )

        # --- Plot ---
        plot_spectrum_diagnostic(
            dp_id=dp_id,
            nova_name=nova_name,
            csv_wl=csv_wl,
            csv_fx=csv_fx,
            fits_wl=fits_wl,
            fits_fx=fits_fx,
            trim_result=trim_result,
            output_dir=output_dir,
        )

    print(f"\n{'=' * 80}")
    print(
        f"SUMMARY: {diagnosed} diagnosed, {survived_count} survive trimming, "
        f"{skipped_count} would be skipped"
    )
    print(f"Plots saved to: {output_dir}/")


if __name__ == "__main__":
    main()
