#!/usr/bin/env python3
"""
ADR-031 Decision 7 patch — reference_manager/handler.py

Adds best-effort WorkItem write to UpsertDiscoveryDateMetadata, the final
scientific task in refresh_references before FinalizeJobRunSuccess.

By the time UpsertDiscoveryDateMetadata runs, all references have already
been linked by the ReconcileReferences Map state. The WorkItem signals
"this nova has new reference data" regardless of whether the discovery
date was updated.

Changes:
  1. Adds import for write_work_item, DirtyType from nova_common.work_item.
  2. Inserts WorkItem write at the top of _handle_upsertDiscoveryDateMetadata,
     after the nova_id validation check.

Usage:
    python patch_reference_manager_work_item.py path/to/services/reference_manager/handler.py
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
    _require(src, "from nova_common.tracing import tracer", "tracer import")
    _require(
        src,
        "def _handle_upsertDiscoveryDateMetadata(event: dict, context: object) -> dict:",
        "UpsertDiscoveryDateMetadata function signature",
    )
    _require(
        src,
        '    new_date: str | None = event.get("earliest_publication_date")',
        "new_date extraction line",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add import
    # =========================================================================
    OLD_IMPORT = "from nova_common.tracing import tracer"
    NEW_IMPORT = (
        "from nova_common.tracing import tracer\n"
        "from nova_common.work_item import DirtyType, write_work_item"
    )
    src = _replace_once(src, OLD_IMPORT, NEW_IMPORT, "work_item import")

    # =========================================================================
    # Patch 2 — Insert WorkItem write after nova_id check, before the
    #           discovery date logic.  Anchored on the new_date extraction
    #           line which immediately follows the nova_id TerminalError check.
    # =========================================================================
    OLD_NEW_DATE = '    new_date: str | None = event.get("earliest_publication_date")'
    NEW_NEW_DATE = (
        "    # --- ADR-031 Decision 7: WorkItem for the regeneration pipeline ---\n"
        "    # Written unconditionally: by the time UpsertDiscoveryDateMetadata runs,\n"
        "    # the ReconcileReferences Map state has already linked all references.\n"
        '    # The WorkItem signals "this nova has new reference data" regardless of\n'
        "    # whether the discovery date was updated.\n"
        "    write_work_item(\n"
        "        _table,\n"
        "        nova_id=nova_id,\n"
        "        dirty_type=DirtyType.references,\n"
        '        source_workflow="refresh_references",\n'
        '        job_run_id=str(event.get("job_run_id", event.get("correlation_id", "unknown"))),\n'
        '        correlation_id=str(event.get("correlation_id", "unknown")),\n'
        "    )\n"
        "\n"
        '    new_date: str | None = event.get("earliest_publication_date")'
    )
    src = _replace_once(
        src, OLD_NEW_DATE, NEW_NEW_DATE, "WorkItem write in UpsertDiscoveryDateMetadata"
    )

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    checks = [
        ("from nova_common.work_item import DirtyType, write_work_item", "work_item import"),
        ("DirtyType.references", "references dirty type"),
        ('source_workflow="refresh_references"', "source_workflow value"),
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
