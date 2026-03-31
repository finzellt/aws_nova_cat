#!/usr/bin/env python3
"""
ADR-031 Decision 6 patch — contracts/models/entities.py

Adds ``nova_type: str | None`` field to the Nova Pydantic model.

Changes:
  1. Inserts the nova_type field between discovery_date and aliases on the
     Nova class.

Usage:
    python patch_entities_nova_type.py path/to/contracts/models/entities.py
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
        print(f"Usage: {sys.argv[0]} <path/to/entities.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src,
        "ComputeDiscoveryDate. Never use 01 as a proxy for unknown day.",
        "discovery_date field description",
    )
    _require(
        src,
        "    aliases: list[str] = Field(",
        "aliases field",
    )
    # Ensure nova_type is not already present
    if "nova_type:" in src:
        print("PRECONDITION FAILED — 'nova_type' field already present in file.")
        sys.exit(1)

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Insert nova_type field between discovery_date and aliases
    # =========================================================================
    OLD_ALIASES = "    aliases: list[str] = Field("
    NEW_ALIASES = (
        "    nova_type: str | None = Field(\n"
        "        default=None,\n"
        "        description=(\n"
        "            \"Nova classification — e.g. 'recurrent', 'symbiotic'. \"\n"
        '            "Initially null for all novae. Populated via manual operator "\n'
        '            "tagging; automated classification deferred post-MVP. "\n'
        '            "SIMBAD no longer provides a recurrent nova object type, so "\n'
        '            "this field cannot be derived from archive resolution."\n'
        "        ),\n"
        "        max_length=64,\n"
        "    )\n"
        "\n"
        "    aliases: list[str] = Field("
    )
    src = _replace_once(src, OLD_ALIASES, NEW_ALIASES, "nova_type field insertion")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    checks = [
        ("nova_type: str | None = Field(", "nova_type field declaration"),
        ("Initially null for all novae", "nova_type description"),
        ("max_length=64,", "nova_type max_length"),
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
