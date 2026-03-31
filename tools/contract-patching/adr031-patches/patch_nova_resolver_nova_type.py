#!/usr/bin/env python3
"""
ADR-031 Decision 6 patch — nova_resolver/handler.py

Adds ``nova_type: None`` to the Nova stub created by CreateNovaId.

Changes:
  1. Inserts ``"nova_type": None,`` into the CreateNovaId put_item dict,
     after the ``"status": "ACTIVE",`` line.

Usage:
    python patch_nova_resolver_nova_type.py path/to/services/nova_resolver/handler.py
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
        '            "status": "ACTIVE",\n            "created_by_job_run_id": job_run_id,',
        "CreateNovaId put_item dict — status + created_by_job_run_id",
    )
    # Ensure nova_type is not already present
    if '"nova_type"' in src:
        print("PRECONDITION FAILED — 'nova_type' already present in file.")
        sys.exit(1)

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Insert nova_type: None after status: ACTIVE
    # =========================================================================
    OLD_STATUS = '            "status": "ACTIVE",\n            "created_by_job_run_id": job_run_id,'
    NEW_STATUS = (
        '            "status": "ACTIVE",\n'
        '            "nova_type": None,\n'
        '            "created_by_job_run_id": job_run_id,'
    )
    src = _replace_once(src, OLD_STATUS, NEW_STATUS, "nova_type insertion")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    if '"nova_type": None,' not in src:
        print("POSTCONDITION FAILED — nova_type not found after patch.")
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")


if __name__ == "__main__":
    main()
