#!/usr/bin/env python3
"""
ADR-031 Decision 7 patch — spectra_validator/handler.py

Adds best-effort WorkItem write after a VALID RecordValidationResult.

Changes:
  1. Adds import for write_work_item, DirtyType from nova_common.work_item.
  2. Inserts WorkItem write at the end of _handle_record_validation_result,
     only on the VALID path, before the return.

Usage:
    python patch_handler_work_item.py path/to/services/spectra_validator/handler.py
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
    _require(
        src,
        "from nova_common.web_ready_csv import",
        "web_ready_csv import (Decision 4 patch applied)",
    )
    _require(
        src,
        '    return {"persisted_outcome": validation_outcome}',
        "RecordValidationResult return statement",
    )
    _require(
        src,
        '"RecordValidationResult complete"',
        "RecordValidationResult log line",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add import
    # =========================================================================
    OLD_IMPORT = (
        "from nova_common.web_ready_csv import build_web_ready_csv, write_web_ready_csv_to_s3"
    )
    NEW_IMPORT = (
        "from nova_common.web_ready_csv import build_web_ready_csv, write_web_ready_csv_to_s3\n"
        "from nova_common.work_item import DirtyType, write_work_item"
    )
    src = _replace_once(src, OLD_IMPORT, NEW_IMPORT, "work_item import")

    # =========================================================================
    # Patch 2 — Insert WorkItem write before the return in RecordValidationResult
    # =========================================================================
    OLD_RETURN = '    return {"persisted_outcome": validation_outcome}'
    NEW_RETURN = (
        "    # --- ADR-031 Decision 7: WorkItem for the regeneration pipeline ---\n"
        "    # Best-effort; only on the VALID path (QUARANTINED/TERMINAL don't\n"
        "    # produce data that needs artifact regeneration).\n"
        '    if validation_outcome == "VALID":\n'
        "        write_work_item(\n"
        "            _table,\n"
        "            nova_id=nova_id,\n"
        "            dirty_type=DirtyType.spectra,\n"
        '            source_workflow="acquire_and_validate_spectra",\n'
        '            job_run_id=event.get("job_run_id", event.get("correlation_id", "unknown")),\n'
        '            correlation_id=event.get("correlation_id", "unknown"),\n'
        "        )\n"
        "\n"
        '    return {"persisted_outcome": validation_outcome}'
    )
    src = _replace_once(src, OLD_RETURN, NEW_RETURN, "WorkItem write in RecordValidationResult")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    checks = [
        ("from nova_common.work_item import DirtyType, write_work_item", "work_item import"),
        ("DirtyType.spectra", "DirtyType.spectra usage"),
        ('source_workflow="acquire_and_validate_spectra"', "source_workflow value"),
    ]

    failed = False
    for marker, label in checks:
        if marker not in src:
            print(f"POSTCONDITION FAILED — {label!r}")
            failed = True

    if failed:
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")


if __name__ == "__main__":
    main()
