#!/usr/bin/env python3
"""
ADR-031 Decision 7 patch — ticket_ingestor/handler.py

Adds best-effort WorkItem writes to both the photometry and spectra branches
of ingest_ticket.

Changes:
  1. Adds import for write_work_item, DirtyType from nova_common.work_item.
  2. Inserts WorkItem write at the end of _ingest_photometry, after
     upsert_envelope_item and before the return.
  3. Inserts WorkItem write at the end of _ingest_spectra, after the
     write loop and before the return.

Usage:
    python patch_ticket_handler_work_item.py path/to/services/ticket_ingestor/handler.py
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
        src, "from ticket_ingestor.spectra_writer import write_spectrum", "spectra_writer import"
    )
    _require(
        src,
        '    return {\n        "rows_produced": len(result.rows),\n        "failures": len(result.failures),\n    }',
        "_ingest_photometry return block",
    )
    _require(
        src,
        '    return {\n        "spectra_ingested": spectra_ingested,\n        "spectra_failed": write_failures,\n    }',
        "_ingest_spectra return block",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add import
    # =========================================================================
    OLD_IMPORT = "from ticket_ingestor.spectra_writer import write_spectrum"
    NEW_IMPORT = (
        "from ticket_ingestor.spectra_writer import write_spectrum\n"
        "from nova_common.work_item import DirtyType, write_work_item"
    )
    src = _replace_once(src, OLD_IMPORT, NEW_IMPORT, "work_item import")

    # =========================================================================
    # Patch 2 — WorkItem write in _ingest_photometry
    # =========================================================================
    OLD_PHOT_RETURN = (
        "    return {\n"
        '        "rows_produced": len(result.rows),\n'
        '        "failures": len(result.failures),\n'
        "    }"
    )
    NEW_PHOT_RETURN = (
        "    # --- ADR-031 Decision 7: WorkItem for the regeneration pipeline ---\n"
        "    if result.rows:\n"
        "        write_work_item(\n"
        "            _TABLE,\n"
        "            nova_id=str(nova_id),\n"
        "            dirty_type=DirtyType.photometry,\n"
        '            source_workflow="ingest_ticket",\n'
        '            job_run_id=str(event.get("job_run_id", "unknown")),\n'
        '            correlation_id=str(event.get("correlation_id", "unknown")),\n'
        "        )\n"
        "\n"
        "    return {\n"
        '        "rows_produced": len(result.rows),\n'
        '        "failures": len(result.failures),\n'
        "    }"
    )
    src = _replace_once(src, OLD_PHOT_RETURN, NEW_PHOT_RETURN, "photometry WorkItem write")

    # =========================================================================
    # Patch 3 — WorkItem write in _ingest_spectra
    # =========================================================================
    OLD_SPEC_RETURN = (
        "    return {\n"
        '        "spectra_ingested": spectra_ingested,\n'
        '        "spectra_failed": write_failures,\n'
        "    }"
    )
    NEW_SPEC_RETURN = (
        "    # --- ADR-031 Decision 7: WorkItem for the regeneration pipeline ---\n"
        "    if spectra_ingested > 0:\n"
        "        write_work_item(\n"
        "            _TABLE,\n"
        "            nova_id=str(nova_id),\n"
        "            dirty_type=DirtyType.spectra,\n"
        '            source_workflow="ingest_ticket",\n'
        '            job_run_id=str(event.get("job_run_id", "unknown")),\n'
        '            correlation_id=str(event.get("correlation_id", "unknown")),\n'
        "        )\n"
        "\n"
        "    return {\n"
        '        "spectra_ingested": spectra_ingested,\n'
        '        "spectra_failed": write_failures,\n'
        "    }"
    )
    src = _replace_once(src, OLD_SPEC_RETURN, NEW_SPEC_RETURN, "spectra WorkItem write")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    checks = [
        ("from nova_common.work_item import DirtyType, write_work_item", "work_item import"),
        ("DirtyType.photometry", "photometry dirty type"),
        ("DirtyType.spectra", "spectra dirty type"),
        ('source_workflow="ingest_ticket"', "source_workflow value"),
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
