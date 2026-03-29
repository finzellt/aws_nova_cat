#!/usr/bin/env python3
"""Upload ingest_ticket smoke test fixtures to the NovaCat private S3 bucket.

Resolves the target bucket name from the NovaCatSmoke CloudFormation stack
(or a stack name you supply on the command line), then uploads three sets of
fixture files:

  1. V4739 Sgr photometry — synthetic ticket + synthetic photometry CSV.
  2. GQ Mus spectra       — real ticket + real data files when
                             tests/fixtures/spectra/gq_mus/ is present;
                             synthetic equivalents otherwise.
  3. Quarantine           — synthetic ticket with a deliberately
                             unresolvable object name; no data files needed
                             (the workflow quarantines before IngestPhotometry).

S3 key layout
-------------
  raw/tickets/<ticket_filename>
  raw/data/v4739_sgr/<data_filename>
  raw/data/gq_mus/<data_filename>
  raw/data/bogus/<data_filename>

These paths match the ticket_path / data_dir values hard-coded in
tests/smoke/test_ingest_ticket.py.

Usage
-----
  python tools/smoke/upload_ingest_ticket_fixtures.py
  python tools/smoke/upload_ingest_ticket_fixtures.py --stack NovaCat
  python tools/smoke/upload_ingest_ticket_fixtures.py --dry-run

Prerequisites
-------------
  AWS credentials configured (env vars, ~/.aws/credentials, or instance role).
  The target stack must be in CREATE_COMPLETE or UPDATE_COMPLETE.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from pathlib import Path

import boto3

# ---------------------------------------------------------------------------
# Repo root — used to locate real fixture files.
# ---------------------------------------------------------------------------

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
_GQ_MUS_FIXTURES_DIR = _REPO_ROOT / "tests" / "fixtures" / "spectra" / "gq_mus"
_GQ_MUS_REAL_TICKET = "GQ_Mus_Williams_Optical_Spectra.txt"
_GQ_MUS_METADATA_FILENAME = "GQ_Mus_Williams_Optical_Spectra_MetaData.csv"

# ---------------------------------------------------------------------------
# Fixture text constants (match integration test verbatim)
# ---------------------------------------------------------------------------

_V4739_SGR_TICKET_TEXT = """\
OBJECT NAME: V4739_Sgr
WAVELENGTH REGIME: Optical
TIME SYSTEM: JD
TIME UNITS: days
FLUX UNITS: mags
FLUX ERROR UNITS: mags
FILTER SYSTEM: Johnson-Cousins
MAGNITUDE SYSTEM: Vega
TELESCOPE: Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector
OBSERVER: Gilmore, A. C. & Kilmartin, P. M.
REFERENCE: Livingston et al. (2001)
BIBCODE: 2001IBVS.5172....1L
ASSUMED DATE OF OUTBURST: NA
DATA FILENAME: V4739_Sgr_Livingston_optical_Photometry.csv
TIME COLUMN NUMBER: 0
FLUX COLUMN NUMBER: 1
FLUX ERROR COLUMN NUMBER: 2
FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER: 3
UPPER LIMIT FLAG COLUMN NUMBER: 4
TELESCOPE COLUMN NUMBER: 5
OBSERVER COLUMN NUMBER: 6
FILTER SYSTEM COLUMN NUMBER: 7
TICKET STATUS: Completed
"""

_V4739_SGR_CSV_ROWS = [
    ["2452148.839", "7.46", "0.009", "V", "0", "Mt John Observatory", "Gilmore", "Johnson-Cousins"],
    ["2452148.853", "7.51", "0.009", "V", "0", "Mt John Observatory", "Gilmore", "Johnson-Cousins"],
    ["2452148.869", "7.58", "0.009", "V", "0", "Mt John Observatory", "Gilmore", "Johnson-Cousins"],
]

# Synthetic GQ Mus ticket — used only when the real fixture directory is absent.
# METADATA FILENAME is injected at runtime so it always matches what we upload.
_GQ_MUS_SYNTHETIC_TICKET_TEMPLATE = """\
OBJECT NAME: GQ_Mus
FLUX UNITS: NA
FLUX ERROR UNITS: NA
WAVELENGTH REGIME: Optical
TIME SYSTEM: JD
ASSUMED DATE OF OUTBURST: NA
REFERENCE: Williams et al. (1992)
BIBCODE: 1992AJ....104..725W
DEREDDENED FLAG: False
METADATA FILENAME: {metadata_filename}
FILENAME COLUMN: 0
WAVELENGTH COLUMN: 1
FLUX COLUMN: 2
FLUX ERROR COLUMN: 3
FLUX UNITS COLUMN: 4
DATE COLUMN: 5
TELESCOPE COLUMN: 7
INSTRUMENT COLUMN: 8
OBSERVER COLUMN: 6
SNR COLUMN: NA
DISPERSION COLUMN: 9
RESOLUTION COLUMN: NA
WAVELENGTH RANGE COLUMN: 10,11
TICKET STATUS: Completed
"""

_GQ_MUS_SYNTHETIC_METADATA_HEADER = (
    "#FILENAME,WAVELENGTH COL NUM,FLUX COL NUM,FLUX ERR COL NUM,"
    "FLUX UNITS,DATE,OBSERVER,TELESCOPE,INSTRUMENT,DISPERSION,"
    "WAVELENGTH RANGE 1,WAVELENGTH RANGE 2"
)
_GQ_MUS_SYNTHETIC_METADATA_ROW = (
    "gq_mus_smoke_spectrum.csv,0,1,NA,ergs/cm^2/sec,"
    "2.44732e+06,Williams,CTIO 1 m,2D-Frutti,3.0,3100.0,7450.0"
)
_GQ_MUS_SYNTHETIC_SPECTRUM_FILENAME = "gq_mus_smoke_spectrum.csv"
_GQ_MUS_SYNTHETIC_SPECTRUM_ROWS = [
    (3100.0, 1.52e-13),
    (3103.0, 1.61e-13),
    (3106.0, 1.74e-13),
    (3109.0, 1.68e-13),
    (3112.0, 1.55e-13),
]

# Quarantine ticket — object name is deliberately nonsensical so that
# initialize_nova returns NOT_FOUND and the workflow quarantines cleanly.
_QUARANTINE_TICKET_TEXT = """\
OBJECT NAME: ZZ_BOGUS_SMOKE_TEST_NOEXIST_9999
WAVELENGTH REGIME: Optical
TIME SYSTEM: JD
TIME UNITS: days
FLUX UNITS: mags
FLUX ERROR UNITS: mags
FILTER SYSTEM: Johnson-Cousins
MAGNITUDE SYSTEM: Vega
TELESCOPE: Synthetic Telescope
OBSERVER: Smoke Test
REFERENCE: Smoke Test (2026)
BIBCODE: 2026SMOKE.001....1T
ASSUMED DATE OF OUTBURST: NA
DATA FILENAME: bogus_photometry.csv
TIME COLUMN NUMBER: 0
FLUX COLUMN NUMBER: 1
FLUX ERROR COLUMN NUMBER: 2
FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER: 3
UPPER LIMIT FLAG COLUMN NUMBER: 4
TELESCOPE COLUMN NUMBER: 5
OBSERVER COLUMN NUMBER: 6
FILTER SYSTEM COLUMN NUMBER: 7
TICKET STATUS: Completed
"""

# Minimal placeholder CSV for the quarantine data_dir — never read by the
# workflow (quarantine fires at ResolveNova, before IngestPhotometry), but
# the prefix must exist so resolve_dir returns cleanly if it is ever called.
_BOGUS_CSV_ROW = ["2460000.0", "14.5", "0.1", "V", "0", "Telescope", "Observer", "Johnson-Cousins"]


# ---------------------------------------------------------------------------
# CloudFormation helpers
# ---------------------------------------------------------------------------


def _resolve_private_bucket(stack_name: str) -> str:
    """Return the private bucket name from the given CloudFormation stack."""
    export_name = f"{stack_name}-PrivateBucketName"
    cf = boto3.client("cloudformation")
    try:
        resp = cf.describe_stacks(StackName=stack_name)
    except cf.exceptions.ClientError as exc:
        print(f"ERROR: Could not describe stack '{stack_name}': {exc}", file=sys.stderr)
        sys.exit(1)

    stacks = resp.get("Stacks", [])
    if not stacks:
        print(f"ERROR: Stack '{stack_name}' returned no results.", file=sys.stderr)
        sys.exit(1)

    status = stacks[0].get("StackStatus", "")
    terminal_ok = {"CREATE_COMPLETE", "UPDATE_COMPLETE"}
    if status not in terminal_ok:
        print(
            f"ERROR: Stack '{stack_name}' is in status '{status}'. Expected one of {terminal_ok}.",
            file=sys.stderr,
        )
        sys.exit(1)

    for output in stacks[0].get("Outputs", []):
        if output.get("ExportName") == export_name:
            return output["OutputValue"]

    print(
        f"ERROR: Stack '{stack_name}' has no export named '{export_name}'. "
        "Re-deploy the stack and ensure all CfnOutputs are present.",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------


def _upload(
    s3_client,
    *,
    bucket: str,
    key: str,
    body: str | bytes,
    dry_run: bool,
) -> None:
    """Upload a single object, printing the key regardless of dry_run."""
    print(f"  {'[dry-run] ' if dry_run else ''}s3://{bucket}/{key}")
    if dry_run:
        return
    if isinstance(body, str):
        body = body.encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)


def _csv_bytes(rows: list[list[str]]) -> bytes:
    """Serialise a list of rows to CSV bytes."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


# ---------------------------------------------------------------------------
# Per-fixture upload routines
# ---------------------------------------------------------------------------


def _upload_v4739_sgr(s3_client, *, bucket: str, dry_run: bool) -> None:
    print("\n--- V4739 Sgr (photometry) ---")
    _upload(
        s3_client,
        bucket=bucket,
        key="raw/tickets/V4739_Sgr_Livingston_optical_Photometry.txt",
        body=_V4739_SGR_TICKET_TEXT,
        dry_run=dry_run,
    )
    _upload(
        s3_client,
        bucket=bucket,
        key="raw/data/v4739_sgr/V4739_Sgr_Livingston_optical_Photometry.csv",
        body=_csv_bytes(_V4739_SGR_CSV_ROWS),
        dry_run=dry_run,
    )


def _upload_gq_mus(s3_client, *, bucket: str, dry_run: bool) -> None:
    print("\n--- GQ Mus (spectra) ---")
    use_real = _GQ_MUS_FIXTURES_DIR.is_dir()
    if use_real:
        print(f"  (using real fixtures from {_GQ_MUS_FIXTURES_DIR})")
    else:
        print(f"  ({_GQ_MUS_FIXTURES_DIR} not found — using synthetic fixtures)")

    if use_real:
        _upload_gq_mus_real(s3_client, bucket=bucket, dry_run=dry_run)
    else:
        _upload_gq_mus_synthetic(s3_client, bucket=bucket, dry_run=dry_run)


def _upload_gq_mus_real(s3_client, *, bucket: str, dry_run: bool) -> None:
    """Upload the real GQ Mus ticket, metadata CSV, and all spectrum CSVs."""
    ticket_path = _GQ_MUS_FIXTURES_DIR / _GQ_MUS_REAL_TICKET
    if not ticket_path.exists():
        print(
            f"  WARNING: Expected real ticket at {ticket_path} — not found. "
            "Falling back to synthetic.",
            file=sys.stderr,
        )
        _upload_gq_mus_synthetic(s3_client, bucket=bucket, dry_run=dry_run)
        return

    _upload(
        s3_client,
        bucket=bucket,
        key=f"raw/tickets/{_GQ_MUS_REAL_TICKET}",
        body=ticket_path.read_bytes(),
        dry_run=dry_run,
    )

    metadata_path = _GQ_MUS_FIXTURES_DIR / _GQ_MUS_METADATA_FILENAME
    if not metadata_path.exists():
        print(
            f"  WARNING: Expected metadata CSV at {metadata_path} — not found.",
            file=sys.stderr,
        )
        sys.exit(1)

    _upload(
        s3_client,
        bucket=bucket,
        key=f"raw/data/gq_mus/{_GQ_MUS_METADATA_FILENAME}",
        body=metadata_path.read_bytes(),
        dry_run=dry_run,
    )

    # Upload every spectrum CSV referenced in the metadata CSV (col 0 of each
    # data row, skipping the header).
    spectrum_filenames: list[str] = []
    with metadata_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader)  # skip header
        for row in reader:
            if row:
                spectrum_filenames.append(row[0].strip())

    for fname in spectrum_filenames:
        spectrum_path = _GQ_MUS_FIXTURES_DIR / fname
        if not spectrum_path.exists():
            print(
                f"  WARNING: Spectrum CSV '{fname}' referenced in metadata but not found "
                f"at {spectrum_path} — skipping.",
                file=sys.stderr,
            )
            continue
        _upload(
            s3_client,
            bucket=bucket,
            key=f"raw/data/gq_mus/{fname}",
            body=spectrum_path.read_bytes(),
            dry_run=dry_run,
        )


def _upload_gq_mus_synthetic(s3_client, *, bucket: str, dry_run: bool) -> None:
    """Upload synthetic GQ Mus ticket, metadata CSV, and one spectrum CSV."""
    ticket_text = _GQ_MUS_SYNTHETIC_TICKET_TEMPLATE.format(
        metadata_filename=_GQ_MUS_METADATA_FILENAME,
    )
    _upload(
        s3_client,
        bucket=bucket,
        key=f"raw/tickets/{_GQ_MUS_REAL_TICKET}",
        body=ticket_text,
        dry_run=dry_run,
    )

    metadata_text = "\n".join([_GQ_MUS_SYNTHETIC_METADATA_HEADER, _GQ_MUS_SYNTHETIC_METADATA_ROW])
    _upload(
        s3_client,
        bucket=bucket,
        key=f"raw/data/gq_mus/{_GQ_MUS_METADATA_FILENAME}",
        body=metadata_text,
        dry_run=dry_run,
    )

    spectrum_rows = [[str(w), str(f)] for w, f in _GQ_MUS_SYNTHETIC_SPECTRUM_ROWS]
    _upload(
        s3_client,
        bucket=bucket,
        key=f"raw/data/gq_mus/{_GQ_MUS_SYNTHETIC_SPECTRUM_FILENAME}",
        body=_csv_bytes(spectrum_rows),
        dry_run=dry_run,
    )


def _upload_quarantine(s3_client, *, bucket: str, dry_run: bool) -> None:
    print("\n--- Quarantine (bogus object name) ---")
    _upload(
        s3_client,
        bucket=bucket,
        key="raw/tickets/BOGUS_OBJECT_smoke_test_quarantine.txt",
        body=_QUARANTINE_TICKET_TEXT,
        dry_run=dry_run,
    )
    # Minimal placeholder data file — never read by the workflow (the
    # execution quarantines at ResolveNova before reaching IngestPhotometry),
    # but the prefix should exist for completeness.
    _upload(
        s3_client,
        bucket=bucket,
        key="raw/data/bogus/bogus_photometry.csv",
        body=_csv_bytes([_BOGUS_CSV_ROW]),
        dry_run=dry_run,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stack",
        default="NovaCatSmoke",
        help="CloudFormation stack name (default: NovaCatSmoke)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be uploaded without actually uploading.",
    )
    args = parser.parse_args()

    print(f"Resolving private bucket from stack '{args.stack}'...")
    bucket = _resolve_private_bucket(args.stack)
    print(f"Target bucket: {bucket}")

    s3 = boto3.client("s3")

    _upload_v4739_sgr(s3, bucket=bucket, dry_run=args.dry_run)
    _upload_gq_mus(s3, bucket=bucket, dry_run=args.dry_run)
    _upload_quarantine(s3, bucket=bucket, dry_run=args.dry_run)

    print("\nDone." if not args.dry_run else "\nDry run complete — nothing uploaded.")


if __name__ == "__main__":
    main()
