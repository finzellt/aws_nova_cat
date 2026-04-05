#!/usr/bin/env python3
"""
Backfill web-ready CSVs for pre-existing validated spectra.

Scans DynamoDB for all ACTIVE novae, queries each for VALID SPECTRA
DataProduct items, then checks S3 for the corresponding web-ready CSV.
For any missing CSVs, downloads the raw FITS, extracts wavelength/flux,
converts to nanometres, and uploads a two-column CSV.

This is the backfill companion to the inline CSV generation added in
ADR-031 P-4 (spectra_validator + ticket_ingestor).  It targets ~52
spectra ingested before that step existed.

Usage:
    python tools/backfill_web_ready_csv.py --dry-run
    python tools/backfill_web_ready_csv.py --nova "V1324 Sco"
    python tools/backfill_web_ready_csv.py --nova "V1324 Sco" --dry-run
    python tools/backfill_web_ready_csv.py

Personal operator tooling -- not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time

import boto3
import numpy as np
from astropy import units as u
from astropy.io import fits
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
_CF_PREFIX = "NovaCat"

_CSV_HEADER = "wavelength_nm,flux"
_S3_KEY_TEMPLATE = "derived/spectra/{nova_id}/{data_product_id}/web_ready.csv"
_CSV_CONTENT_TYPE = "text/csv"

# FITS raw paths (tried in order)
_PRIVATE_RAW_KEY = "raw/spectra/{nova_id}/{data_product_id}/primary.fits"
_PUBLIC_RAW_KEY = "raw/{nova_id}/ticket_ingestion/{data_product_id}.fits"

# Terminal colours
_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"


# ---------------------------------------------------------------------------
# CloudFormation helpers
# ---------------------------------------------------------------------------


def _resolve_cf_exports() -> dict[str, str]:
    """Return a dict of export_name -> value from CloudFormation exports."""
    cf = boto3.client("cloudformation", region_name=_REGION)
    exports: dict[str, str] = {}
    paginator = cf.get_paginator("list_exports")
    for page in paginator.paginate():
        for export in page["Exports"]:
            exports[export["Name"]] = export["Value"]
    return exports


def _get_export(exports: dict[str, str], key: str) -> str:
    name = f"{_CF_PREFIX}-{key}"
    val = exports.get(name)
    if not val:
        print(f"{_RED}Missing CloudFormation export: {name}{_RESET}")
        sys.exit(1)
    return val


# ---------------------------------------------------------------------------
# DynamoDB queries
# ---------------------------------------------------------------------------


def _scan_active_novae(table) -> list[dict]:
    """Scan for all ACTIVE Nova items."""
    items = []
    kwargs = {
        "FilterExpression": Attr("status").eq("ACTIVE") & Attr("SK").eq("NOVA"),
    }
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _query_valid_spectra(nova_id: str, table) -> list[dict]:
    """Query all VALID spectra DataProduct items for a nova."""
    items = []
    kwargs = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
        ),
        "FilterExpression": Attr("validation_status").eq("VALID"),
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _csv_exists(s3, bucket: str, key: str) -> bool:
    """Check if an S3 object exists (HEAD)."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def _download_fits(
    s3, nova_id: str, data_product_id: str, private_bucket: str, public_bucket: str
) -> bytes | None:
    """Try to download the raw FITS from private bucket, then public bucket."""
    private_key = _PRIVATE_RAW_KEY.format(nova_id=nova_id, data_product_id=data_product_id)
    try:
        resp = s3.get_object(Bucket=private_bucket, Key=private_key)
        return resp["Body"].read()
    except ClientError:
        pass

    public_key = _PUBLIC_RAW_KEY.format(nova_id=nova_id, data_product_id=data_product_id)
    try:
        resp = s3.get_object(Bucket=public_bucket, Key=public_key)
        return resp["Body"].read()
    except ClientError:
        return None


# ---------------------------------------------------------------------------
# FITS extraction
# ---------------------------------------------------------------------------


def _extract_wavelength_flux(fits_bytes: bytes) -> tuple[np.ndarray, np.ndarray, str]:
    """Extract wavelength and flux arrays from a FITS file.

    Handles two layouts:
      1. Image HDU (hdu[0].data is a 1-D array) with WCS keywords for
         wavelength reconstruction.
      2. Binary table extension (hdu[1] with columns like 'WAVE'/'FLUX').

    Returns (wavelength, flux, spectral_units).
    """
    with fits.open(io.BytesIO(fits_bytes), memmap=False) as hdul:
        primary = hdul[0]

        # --- Try primary image HDU first ---
        if primary.data is not None and primary.data.ndim >= 1:
            flux = np.asarray(primary.data, dtype=np.float64).flatten()
            spectral_units = primary.header.get("CUNIT1", "Angstrom")
            crval1 = float(primary.header.get("CRVAL1", 0.0))
            cdelt1 = float(primary.header.get("CDELT1", 1.0))
            crpix1 = float(primary.header.get("CRPIX1", 1.0))
            n_pix = len(flux)
            wavelength = crval1 + cdelt1 * (np.arange(n_pix) - (crpix1 - 1.0))
            return wavelength, flux, spectral_units

        # --- Try binary table extension ---
        if len(hdul) > 1 and hasattr(hdul[1], "columns"):
            table = hdul[1].data
            col_names = [c.upper() for c in hdul[1].columns.names]

            # Find wavelength column
            wave_col = None
            for candidate in ("WAVE", "WAVELENGTH", "LAMBDA", "WAVE_AIR"):
                if candidate in col_names:
                    wave_col = hdul[1].columns.names[col_names.index(candidate)]
                    break

            # Find flux column
            flux_col = None
            for candidate in ("FLUX", "FLUX_REDUCED", "FLUX_OPT"):
                if candidate in col_names:
                    flux_col = hdul[1].columns.names[col_names.index(candidate)]
                    break

            if wave_col and flux_col:
                wavelength = np.asarray(table[wave_col], dtype=np.float64).flatten()
                flux = np.asarray(table[flux_col], dtype=np.float64).flatten()
                # Check table header for units, fall back to primary
                spectral_units = (
                    hdul[1].header.get("TUNIT1") or primary.header.get("CUNIT1") or "Angstrom"
                )
                return wavelength, flux, spectral_units

    raise ValueError("Could not extract wavelength/flux from FITS file")


# ---------------------------------------------------------------------------
# CSV generation (full-resolution, no LTTB)
# ---------------------------------------------------------------------------


def _build_csv(wavelength: np.ndarray, flux: np.ndarray, spectral_units: str) -> str:
    """Convert wavelengths to nm and build a two-column CSV string.

    Unlike the service-layer build_web_ready_csv, this does NOT downsample.
    The full-resolution data is written; LTTB is applied later by the
    spectra artifact generator at generation time.
    """
    source_unit = u.Unit(spectral_units)
    wavelength_nm = (wavelength * source_unit).to(u.nm).value

    buf = io.StringIO()
    buf.write(_CSV_HEADER)
    buf.write("\n")
    for w, f in zip(wavelength_nm, flux, strict=False):
        buf.write(f"{w},{f}\n")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill web-ready CSVs for validated spectra missing them.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be processed without writing to S3.",
    )
    parser.add_argument(
        "--nova",
        type=str,
        default=None,
        help='Process a single nova by name (e.g. --nova "V1324 Sco").',
    )
    args = parser.parse_args()

    dry_run: bool = args.dry_run
    nova_filter: str | None = args.nova

    # --- Resolve infrastructure ---
    print(f"{_BOLD}Resolving CloudFormation exports...{_RESET}")
    exports = _resolve_cf_exports()
    table_name = _get_export(exports, "TableName")
    private_bucket = _get_export(exports, "PrivateBucketName")
    public_bucket = _get_export(exports, "PublicSiteBucketName")
    print(f"  Table:          {table_name}")
    print(f"  Private bucket: {private_bucket}")
    print(f"  Public bucket:  {public_bucket}")

    dynamodb = boto3.resource("dynamodb", region_name=_REGION)
    table = dynamodb.Table(table_name)
    s3 = boto3.client("s3", region_name=_REGION)

    # --- Discover novae ---
    print(f"\n{_BOLD}Scanning for ACTIVE novae...{_RESET}")
    novae = _scan_active_novae(table)
    print(f"  Found {len(novae)} active novae.")

    if nova_filter:
        novae = [n for n in novae if n.get("nova_name") == nova_filter]
        if not novae:
            print(f"{_RED}No active nova found with name '{nova_filter}'.{_RESET}")
            sys.exit(1)
        print(f"  Filtered to: {novae[0]['nova_name']}")

    # --- Process each nova ---
    total_spectra = 0
    already_have_csv = 0
    processed = 0
    failures = 0
    start_time = time.time()

    for nova in novae:
        nova_id = nova["PK"]
        nova_name = nova.get("nova_name", nova_id)

        products = _query_valid_spectra(nova_id, table)
        if not products:
            continue

        print(f"\n{_BOLD}{nova_name}{_RESET} — {len(products)} valid spectra")
        total_spectra += len(products)

        for product in products:
            dp_id = product["data_product_id"]
            csv_key = _S3_KEY_TEMPLATE.format(nova_id=nova_id, data_product_id=dp_id)

            # Check if CSV already exists
            if _csv_exists(s3, private_bucket, csv_key):
                already_have_csv += 1
                print(f"  {_DIM}[skip] {dp_id[:12]}... CSV exists{_RESET}")
                continue

            if dry_run:
                print(f"  {_YELLOW}[dry-run] {dp_id[:12]}... would process{_RESET}")
                processed += 1
                continue

            # Download raw FITS
            fits_bytes = _download_fits(s3, nova_id, dp_id, private_bucket, public_bucket)
            if fits_bytes is None:
                print(f"  {_RED}[fail] {dp_id[:12]}... FITS not found in either bucket{_RESET}")
                failures += 1
                continue

            # Extract and convert
            try:
                wavelength, flux, spectral_units = _extract_wavelength_flux(fits_bytes)
                csv_content = _build_csv(wavelength, flux, spectral_units)
            except Exception as exc:
                print(f"  {_RED}[fail] {dp_id[:12]}... extraction error: {exc}{_RESET}")
                failures += 1
                continue

            # Upload
            try:
                s3.put_object(
                    Bucket=private_bucket,
                    Key=csv_key,
                    Body=csv_content.encode("utf-8"),
                    ContentType=_CSV_CONTENT_TYPE,
                )
                processed += 1
                print(f"  {_GREEN}[ok]   {dp_id[:12]}... wrote {len(csv_content)} bytes{_RESET}")
            except Exception as exc:
                print(f"  {_RED}[fail] {dp_id[:12]}... S3 upload error: {exc}{_RESET}")
                failures += 1

    # --- Summary ---
    elapsed = time.time() - start_time
    mode = f"{_YELLOW}DRY RUN{_RESET}" if dry_run else "LIVE"

    print(f"\n{'=' * 60}")
    print(f"{_BOLD}Backfill complete{_RESET} ({mode}) in {elapsed:.1f}s")
    print(f"  Total valid spectra found : {total_spectra}")
    print(f"  Already have web-ready CSV: {already_have_csv}")
    print(f"  Processed (new CSVs)      : {processed}")
    print(f"  Failures                  : {failures}")
    print(f"{'=' * 60}")

    if failures > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
