#!/usr/bin/env python3
"""
ADR-031 Decision 7 doc patch — docs/storage/dynamodb-access-patterns.md

Adds the WORKQUEUE partition to the table overview and a new
"Artifact regeneration pipeline" access pattern section.

Changes:
  1. Adds PK = "WORKQUEUE" to the global identity partitions list.
  2. Adds WORKQUEUE SK prefix to the per-partition SK list.
  3. Adds a new workflow section for the artifact regeneration pipeline
     between the last workflow section and the Operational access patterns.

Usage:
    python patch_access_patterns_adr031.py path/to/docs/storage/dynamodb-access-patterns.md
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
        print(f"Usage: {sys.argv[0]} <path/to/dynamodb-access-patterns.md>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src,
        '- `PK = "WORKFLOW#<correlation_id>"`',
        "WORKFLOW partition in global identity list",
    )
    _require(
        src,
        "## Operational access patterns (debug/admin)",
        "Operational access patterns section",
    )
    _require(
        src,
        "- `ATTEMPT#...`",
        "ATTEMPT SK prefix in per-nova partitions list",
    )

    if "WORKQUEUE" in src:
        print("PRECONDITION FAILED — 'WORKQUEUE' already present in file.")
        sys.exit(1)

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add WORKQUEUE to global identity partitions
    # =========================================================================
    OLD_WORKFLOW_PK = '- `PK = "WORKFLOW#<correlation_id>"`'
    NEW_WORKFLOW_PK = (
        '- `PK = "WORKFLOW#<correlation_id>"`\n'
        '- `PK = "WORKQUEUE"` — artifact regeneration work orders (ADR-031 Decision 7,\n'
        "  DESIGN-003 §3)"
    )
    src = _replace_once(src, OLD_WORKFLOW_PK, NEW_WORKFLOW_PK, "WORKQUEUE global partition")

    # =========================================================================
    # Patch 2 — Add WORKQUEUE SK prefix note after ATTEMPT
    # =========================================================================
    OLD_ATTEMPT_PREFIX = "- `ATTEMPT#...`"
    NEW_ATTEMPT_PREFIX = (
        "- `ATTEMPT#...`\n"
        "\n"
        "The `WORKQUEUE` partition uses a different SK structure:\n"
        "- `<nova_id>#<dirty_type>#<created_at>` — ordered for per-nova grouping"
    )
    src = _replace_once(src, OLD_ATTEMPT_PREFIX, NEW_ATTEMPT_PREFIX, "WORKQUEUE SK prefix")

    # =========================================================================
    # Patch 3 — Add artifact regeneration pipeline section before Operational
    # =========================================================================
    OLD_OPERATIONAL = "## Operational access patterns (debug/admin)"
    NEW_OPERATIONAL = (
        "## Artifact regeneration pipeline (DESIGN-003 §3–§4)\n"
        "\n"
        "Purpose: Signal which novae have new data so the regeneration pipeline\n"
        "knows which artifacts to rebuild.\n"
        "\n"
        "### Write WorkItem (ingestion workflows → WORKQUEUE)\n"
        "\n"
        "After scientific data is persisted, each ingestion workflow writes a WorkItem:\n"
        "\n"
        "```\n"
        "PutItem:\n"
        '  PK = "WORKQUEUE"\n'
        '  SK = "<nova_id>#<dirty_type>#<created_at>"\n'
        "```\n"
        "\n"
        "| Workflow | dirty_type |\n"
        "|---|---|\n"
        "| `acquire_and_validate_spectra` (VALID outcome) | `spectra` |\n"
        "| `ingest_ticket` (spectra branch) | `spectra` |\n"
        "| `ingest_ticket` (photometry branch) | `photometry` |\n"
        "| `refresh_references` | `references` |\n"
        "\n"
        "Best-effort: a failed write logs a warning but does not fail the ingestion.\n"
        "\n"
        "### Read all pending WorkItems (coordinator sweep)\n"
        "\n"
        "```\n"
        "Query:\n"
        '  PK = "WORKQUEUE"\n'
        "```\n"
        "\n"
        "Returns all pending WorkItems across all novae. The coordinator groups\n"
        "by `nova_id` (extracted from the SK prefix) and derives per-nova\n"
        "regeneration manifests using the dirty_type → artifact dependency matrix\n"
        "(DESIGN-003 §3.4).\n"
        "\n"
        "### Read WorkItems for a specific nova\n"
        "\n"
        "```\n"
        "Query:\n"
        '  PK = "WORKQUEUE"\n'
        '  SK begins_with "<nova_id>#"\n'
        "```\n"
        "\n"
        "Useful for operator diagnosis: check what changes are pending for a\n"
        "specific nova.\n"
        "\n"
        "### Delete consumed WorkItems (after successful regeneration)\n"
        "\n"
        "```\n"
        "BatchWriteItem (DeleteRequest):\n"
        '  PK = "WORKQUEUE"\n'
        "  SK = <exact SK from the batch plan's workitem_sks list>\n"
        "```\n"
        "\n"
        "Only the WorkItems that were present when the coordinator built the\n"
        "batch plan are deleted — not any that arrived during execution.\n"
        "\n"
        "---\n"
        "\n"
        "## Operational access patterns (debug/admin)"
    )
    src = _replace_once(src, OLD_OPERATIONAL, NEW_OPERATIONAL, "artifact regeneration section")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    checks = [
        ("WORKQUEUE", "WORKQUEUE reference"),
        ("dirty_type", "dirty_type reference"),
        ("Artifact regeneration pipeline", "new section header"),
        ("Write WorkItem", "write pattern"),
        ("Read all pending WorkItems", "read-all pattern"),
        ("Delete consumed WorkItems", "delete pattern"),
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
