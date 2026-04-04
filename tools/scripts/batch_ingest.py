#!/usr/bin/env python3
"""
Batch ingestion — upload tickets or discover novae by name.

Two modes:

  tickets  — Upload ticket .txt files + data to S3, then run ingest_ticket
             for each. For hand-curated photometry and spectra data.

  names    — Run initialize_nova for each name. Downstream workflows fire
             automatically: refresh_references, discover_spectra_products,
             acquire_and_validate_spectra. For pulling data from ESO/ADS.

Usage:
    # Ticket-based ingestion
    python tools/batch_ingest.py tickets data/
    python tools/batch_ingest.py tickets data/ --dry-run
    python tools/batch_ingest.py tickets data/ --upload-only
    python tools/batch_ingest.py tickets data/ --nova "V1324_Sco"

    # Name-based ingestion (ESO archive discovery)
    python tools/batch_ingest.py names "V1324 Sco" "RS Oph" "GK Per"
    python tools/batch_ingest.py names --file nova_list.txt
    python tools/batch_ingest.py names --file nova_list.txt --dry-run

    # Mixed: names from file + extra names on command line
    python tools/batch_ingest.py names --file nova_list.txt "Extra Nova"

Expected ticket directory structure (one directory per nova):

    data/
      V1324_Sco/
        V1324_Sco_photometry.txt          <- ticket
        V1324_Sco_photometry.csv          <- data file referenced by ticket
      RS_Oph/
        RS_Oph_Williams_Optical_Spectra.txt
        RS_Oph_Williams_Optical_Spectra_MetaData.csv
        RS_Oph_spectrum_001.csv

nova_list.txt format (one name per line, blank lines and # comments ignored):

    V1324 Sco
    RS Oph
    # GK Per  <- skipped
    T Pyx

Personal operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

import boto3

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_CYAN = "\033[96m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"


def _ok(msg: str) -> None:
    print(f"  {_GREEN}✓{_RESET} {msg}")


def _fail(msg: str) -> None:
    print(f"  {_RED}✗{_RESET} {msg}")


def _info(msg: str) -> None:
    print(f"  {_DIM}{msg}{_RESET}")


def _warn(msg: str) -> None:
    print(f"  {_YELLOW}⚠{_RESET} {msg}")


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {_BOLD}{title}{_RESET}")
    print(f"{'─' * 60}")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def _get_config(need_tickets: bool = False, need_names: bool = False) -> dict[str, str]:
    """Load required env vars based on which mode we're running."""
    config: dict[str, str] = {}
    missing = []

    if need_tickets:
        for key in ("NOVACAT_PRIVATE_BUCKET", "NOVACAT_INGEST_TICKET_ARN"):
            val = os.environ.get(key)
            if not val:
                missing.append(key)
            else:
                config[key] = val

    if need_names:
        val = os.environ.get("NOVACAT_INIT_ARN")
        if not val:
            missing.append("NOVACAT_INIT_ARN")
        else:
            config["NOVACAT_INIT_ARN"] = val

    if missing:
        print(
            f"{_RED}Missing env vars:{_RESET} {', '.join(missing)}\n"
            f"Run deploy.sh first, or source ~/.zshrc.",
            file=sys.stderr,
        )
        sys.exit(1)

    return config


# ===========================================================================
# Ticket mode
# ===========================================================================

# ---------------------------------------------------------------------------
# Ticket parsing
# ---------------------------------------------------------------------------


def _parse_ticket(ticket_path: Path) -> dict[str, str]:
    """Parse a ticket .txt file into a key-value dict."""
    fields = {}
    with ticket_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                continue
            key, _, value = line.partition(":")
            fields[key.strip().upper()] = value.strip()
    return fields


def _ticket_type(fields: dict[str, str]) -> str:
    if "METADATA FILENAME" in fields:
        return "spectra"
    if "DATA FILENAME" in fields:
        return "photometry"
    return "unknown"


def _data_files_for_ticket(ticket_path: Path, fields: dict[str, str], ttype: str) -> list[Path]:
    """Resolve all data files referenced by a ticket."""
    ticket_dir = ticket_path.parent
    files = []

    if ttype == "photometry":
        data_filename = fields.get("DATA FILENAME", "")
        if data_filename:
            p = ticket_dir / data_filename
            if p.exists():
                files.append(p)
            else:
                _warn(f"Data file not found: {p}")

    elif ttype == "spectra":
        meta_filename = fields.get("METADATA FILENAME", "")
        if meta_filename:
            meta_path = ticket_dir / meta_filename
            if meta_path.exists():
                files.append(meta_path)
                with meta_path.open(newline="", encoding="utf-8") as fh:
                    reader = csv.reader(fh)
                    next(reader, None)  # skip header
                    for row in reader:
                        if row:
                            spectrum_name = row[0].strip()
                            sp = ticket_dir / spectrum_name
                            if sp.exists():
                                files.append(sp)
                            else:
                                _warn(f"Spectrum file not found: {sp}")
            else:
                _warn(f"Metadata CSV not found: {meta_path}")

    return files


# ---------------------------------------------------------------------------
# Ticket discovery
# ---------------------------------------------------------------------------


def _discover_tickets(base_dir: Path, nova_filter: str | None = None) -> list[dict]:
    results = []

    for subdir in sorted(base_dir.iterdir()):
        if not subdir.is_dir():
            continue
        if nova_filter and subdir.name != nova_filter:
            continue

        tickets = sorted(subdir.glob("*.txt"))
        if not tickets:
            continue

        for ticket_path in tickets:
            fields = _parse_ticket(ticket_path)
            ttype = _ticket_type(fields)
            if ttype == "unknown":
                _warn(f"Skipping {ticket_path.name} — no DATA/METADATA FILENAME")
                continue

            data_files = _data_files_for_ticket(ticket_path, fields, ttype)
            nova_slug = subdir.name

            results.append(
                {
                    "ticket_path": ticket_path,
                    "ticket_fields": fields,
                    "ticket_type": ttype,
                    "data_files": data_files,
                    "nova_slug": nova_slug,
                    "object_name": fields.get("OBJECT NAME", nova_slug),
                }
            )

    return results


# ---------------------------------------------------------------------------
# Ticket upload
# ---------------------------------------------------------------------------


def _upload_ticket_set(s3_client, bucket: str, ticket_info: dict, dry_run: bool) -> dict[str, str]:
    ticket_path: Path = ticket_info["ticket_path"]
    nova_slug: str = ticket_info["nova_slug"]

    ticket_s3_key = f"raw/tickets/{ticket_path.name}"
    _info(f"{'[dry-run] ' if dry_run else ''}s3://{bucket}/{ticket_s3_key}")
    if not dry_run:
        s3_client.put_object(
            Bucket=bucket,
            Key=ticket_s3_key,
            Body=ticket_path.read_bytes(),
        )

    data_s3_prefix = f"raw/data/{nova_slug.lower()}"
    for data_file in ticket_info["data_files"]:
        data_s3_key = f"{data_s3_prefix}/{data_file.name}"
        _info(f"{'[dry-run] ' if dry_run else ''}s3://{bucket}/{data_s3_key}")
        if not dry_run:
            s3_client.put_object(
                Bucket=bucket,
                Key=data_s3_key,
                Body=data_file.read_bytes(),
            )

    return {"ticket_s3_key": ticket_s3_key, "data_dir": data_s3_prefix}


# ---------------------------------------------------------------------------
# Ticket ingestion
# ---------------------------------------------------------------------------


def _ingest_ticket(
    sfn_client,
    ingest_arn: str,
    ticket_s3_key: str,
    data_dir: str,
    object_name: str,
    index: int,
    total: int,
) -> dict:
    correlation_id = f"batch-ticket-{int(time.time())}-{index}"
    exec_name = f"batch-t-{int(time.time())}-{index}"

    resp = sfn_client.start_sync_execution(
        stateMachineArn=ingest_arn,
        name=exec_name,
        input=json.dumps(
            {
                "ticket_path": ticket_s3_key,
                "data_dir": data_dir,
                "correlation_id": correlation_id,
            }
        ),
    )

    status = resp["status"]
    outcome = "unknown"
    if status == "SUCCEEDED":
        try:
            output = json.loads(resp.get("output", "{}"))
            outcome = output.get("finalize", {}).get("outcome", "unknown")
        except (json.JSONDecodeError, KeyError):
            pass

    return {
        "object_name": object_name,
        "status": status,
        "outcome": outcome,
        "ticket": ticket_s3_key,
        "error": resp.get("error"),
        "cause": resp.get("cause"),
    }


# ---------------------------------------------------------------------------
# Ticket mode entry point
# ---------------------------------------------------------------------------


def _run_tickets(args) -> None:
    if not args.directory.is_dir():
        print(f"{_RED}Not a directory:{_RESET} {args.directory}", file=sys.stderr)
        sys.exit(1)

    config = _get_config(need_tickets=True)
    s3_client = boto3.client("s3", region_name=_REGION)
    sfn_client = boto3.client("stepfunctions", region_name=_REGION)

    # -- Discover --
    _section("Discovering tickets")
    tickets = _discover_tickets(args.directory, nova_filter=args.nova)

    if not tickets:
        _warn("No tickets found.")
        sys.exit(0)

    print(f"\n  Found {_BOLD}{len(tickets)}{_RESET} ticket(s):\n")
    for t in tickets:
        data_count = len(t["data_files"])
        print(
            f"    {t['object_name']:25s}  "
            f"{t['ticket_type']:12s}  "
            f"{data_count} data file(s)  "
            f"{_DIM}{t['ticket_path'].name}{_RESET}"
        )

    # -- Upload --
    _section("Uploading to S3")
    s3_paths = []
    for t in tickets:
        _info(f"\n{t['object_name']} ({t['ticket_type']})")
        paths = _upload_ticket_set(s3_client, config["NOVACAT_PRIVATE_BUCKET"], t, args.dry_run)
        s3_paths.append(paths)

    if args.dry_run:
        _info("\nDry run — nothing uploaded.")
        return

    _ok(f"Uploaded {len(tickets)} ticket(s) + data to S3")

    if args.upload_only:
        _info("\n--upload-only — skipping ingestion.")
        return

    # -- Ingest --
    _section("Ingesting tickets")
    results = []

    for i, (t, paths) in enumerate(zip(tickets, s3_paths, strict=False)):
        label = f"[{i + 1}/{len(tickets)}] {t['object_name']} ({t['ticket_type']})"
        print(f"\n  {_CYAN}▸{_RESET} {label}")

        result = _ingest_ticket(
            sfn_client,
            ingest_arn=config["NOVACAT_INGEST_TICKET_ARN"],
            ticket_s3_key=paths["ticket_s3_key"],
            data_dir=paths["data_dir"],
            object_name=t["object_name"],
            index=i,
            total=len(tickets),
        )
        results.append(result)

        if result["status"] == "SUCCEEDED":
            _ok(f"{result['outcome']}")
        else:
            _fail(f"{result['status']}: {result.get('error', 'unknown')}")

    _print_summary(results)


# ===========================================================================
# Names mode
# ===========================================================================


def _read_names_file(path: Path) -> list[str]:
    """Read nova names from a file (one per line, # comments, blank lines ok)."""
    names = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            names.append(line)
    return names


def _initialize_nova(sfn_client, init_arn: str, name: str, index: int, total: int) -> dict:
    """Fire initialize_nova for a single name. Downstream workflows launch automatically."""
    correlation_id = f"batch-name-{int(time.time())}-{index}"
    exec_name = f"batch-n-{int(time.time())}-{index}"

    resp = sfn_client.start_sync_execution(
        stateMachineArn=init_arn,
        name=exec_name,
        input=json.dumps(
            {
                "candidate_name": name,
                "correlation_id": correlation_id,
                "source": "batch_ingest",
            }
        ),
    )

    status = resp["status"]
    outcome = "unknown"
    nova_id = None

    if status == "SUCCEEDED":
        try:
            output = json.loads(resp.get("output", "{}"))
            finalize = output.get("finalize", {})
            outcome = finalize.get("outcome", "unknown")
            nova_id = output.get("upsert", {}).get("nova_id") or output.get("name_check", {}).get(
                "nova_id"
            )
        except (json.JSONDecodeError, KeyError):
            pass

    return {
        "object_name": name,
        "status": status,
        "outcome": outcome,
        "nova_id": nova_id,
        "error": resp.get("error"),
        "cause": resp.get("cause"),
    }


def _run_names(args) -> None:
    config = _get_config(need_names=True)
    sfn_client = boto3.client("stepfunctions", region_name=_REGION)

    # Collect names from args + optional file
    names: list[str] = list(args.names) if args.names else []

    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            print(f"{_RED}File not found:{_RESET} {args.file}", file=sys.stderr)
            sys.exit(1)
        names.extend(_read_names_file(file_path))

    if not names:
        print(
            f"{_RED}No nova names provided.{_RESET} Pass names as args or use --file.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Deduplicate while preserving order
    seen = set()
    unique_names = []
    for n in names:
        if n not in seen:
            seen.add(n)
            unique_names.append(n)
    names = unique_names

    _section("Name-based ingestion")
    print(f"\n  {_BOLD}{len(names)}{_RESET} nova(e) to process:\n")
    for name in names:
        print(f"    {name}")

    if args.dry_run:
        _info("\nDry run — nothing will be ingested.")
        return

    print()
    _info(
        "Each name fires initialize_nova → ingest_new_nova → "
        "refresh_references + discover_spectra_products automatically."
    )
    _info(
        "Downstream workflows run asynchronously — spectra acquisition "
        "may take several minutes per nova after this script finishes.\n"
    )

    # -- Ingest --
    _section("Initializing novae")
    results = []
    total = len(names)

    for i, name in enumerate(names):
        label = f"[{i + 1}/{total}] {name}"
        print(f"\n  {_CYAN}▸{_RESET} {label}")

        result = _initialize_nova(
            sfn_client,
            init_arn=config["NOVACAT_INIT_ARN"],
            name=name,
            index=i,
            total=total,
        )
        results.append(result)

        if result["status"] == "SUCCEEDED":
            nova_id_short = result["nova_id"][:12] if result["nova_id"] else "?"
            _ok(f"{result['outcome']}  nova_id={nova_id_short}...")
        else:
            _fail(f"{result['status']}: {result.get('error', 'unknown')}")

    _print_summary(results)


# ===========================================================================
# Shared summary
# ===========================================================================


def _print_summary(results: list[dict]) -> None:
    _section("Summary")
    succeeded = sum(1 for r in results if r["status"] == "SUCCEEDED")
    failed = sum(1 for r in results if r["status"] != "SUCCEEDED")
    quarantined = sum(1 for r in results if r.get("outcome") == "QUARANTINED")

    for r in results:
        icon = f"{_GREEN}✓{_RESET}" if r["status"] == "SUCCEEDED" else f"{_RED}✗{_RESET}"
        detail = r.get("outcome", "")
        if r.get("nova_id"):
            detail += f"  {_DIM}({r['nova_id'][:12]}...){_RESET}"
        print(f"  {icon} {r['object_name']:25s}  {detail}")

    print(
        f"\n  {_BOLD}{succeeded} succeeded, {failed} failed"
        f"{f', {quarantined} quarantined' if quarantined else ''}{_RESET}"
    )

    if failed > 0:
        print(f"\n  {_RED}Failed:{_RESET}")
        for r in results:
            if r["status"] != "SUCCEEDED":
                print(f"    {r['object_name']}: {r.get('error')} — {r.get('cause', '')}")

    print()
    sys.exit(1 if failed > 0 else 0)


# ===========================================================================
# Main — subcommand dispatch
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch ingestion into NovaCat — tickets or names.",
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    # -- tickets subcommand --
    tickets_parser = subparsers.add_parser(
        "tickets",
        help="Upload ticket .txt files + data, then run ingest_ticket for each",
    )
    tickets_parser.add_argument(
        "directory",
        type=Path,
        help="Directory containing per-nova subdirectories with tickets + data",
    )
    tickets_parser.add_argument("--dry-run", action="store_true")
    tickets_parser.add_argument(
        "--upload-only", action="store_true", help="Upload to S3 but don't trigger ingestion"
    )
    tickets_parser.add_argument("--nova", default=None, help="Process only this subdirectory")

    # -- names subcommand --
    names_parser = subparsers.add_parser(
        "names",
        help="Run initialize_nova for each name (triggers ESO/ADS discovery)",
    )
    names_parser.add_argument(
        "names",
        nargs="*",
        help="Nova names (e.g., 'V1324 Sco' 'RS Oph')",
    )
    names_parser.add_argument(
        "--file",
        "-f",
        default=None,
        help="Read nova names from a file (one per line, # comments ok)",
    )
    names_parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    print(f"\n{_BOLD}Nova Cat — Batch Ingestion{_RESET}\n")

    if args.mode == "tickets":
        _run_tickets(args)
    elif args.mode == "names":
        _run_names(args)


if __name__ == "__main__":
    main()
