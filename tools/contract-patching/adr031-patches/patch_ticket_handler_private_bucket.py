#!/usr/bin/env python3
"""
ADR-031 Decision 4 patch — ticket_ingestor/handler.py

Passes private_bucket=_PRIVATE_BUCKET to write_spectrum calls so the
spectra writer can generate web-ready CSVs.

Changes:
  1. Adds private_bucket=_PRIVATE_BUCKET to the write_spectrum call in
     _ingest_spectra.

Usage:
    python patch_ticket_handler_private_bucket.py path/to/services/ticket_ingestor/handler.py
"""

from __future__ import annotations

import sys


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _replace_once(content: str, old: str, new: str, label: str) -> str:
    if old not in content:
        print(f"REPLACE FAILED — anchor for {label!r} not found.")
        sys.exit(1)
    if content.count(old) > 1:
        print(f"REPLACE FAILED — anchor for {label!r} appears more than once.")
        sys.exit(1)
    return content.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/handler.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(src, "_PRIVATE_BUCKET = os.environ", "_PRIVATE_BUCKET env var")
    _require(
        src,
        "write_spectrum(\n                result=result,\n                nova_id=nova_id,\n"
        "                job_run_id=job_run_id,\n                bucket=_PUBLIC_BUCKET_NAME,\n"
        "                s3=_s3,\n                table=_TABLE,\n            )",
        "write_spectrum call in _ingest_spectra",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add private_bucket to write_spectrum call
    # =========================================================================
    OLD_CALL = (
        "write_spectrum(\n"
        "                result=result,\n"
        "                nova_id=nova_id,\n"
        "                job_run_id=job_run_id,\n"
        "                bucket=_PUBLIC_BUCKET_NAME,\n"
        "                s3=_s3,\n"
        "                table=_TABLE,\n"
        "            )"
    )
    NEW_CALL = (
        "write_spectrum(\n"
        "                result=result,\n"
        "                nova_id=nova_id,\n"
        "                job_run_id=job_run_id,\n"
        "                bucket=_PUBLIC_BUCKET_NAME,\n"
        "                s3=_s3,\n"
        "                table=_TABLE,\n"
        "                private_bucket=_PRIVATE_BUCKET,\n"
        "            )"
    )
    src = _replace_once(src, OLD_CALL, NEW_CALL, "private_bucket kwarg")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    if "private_bucket=_PRIVATE_BUCKET," not in src:
        print("POSTCONDITION FAILED — private_bucket kwarg not found after patch.")
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")


if __name__ == "__main__":
    main()
