#!/usr/bin/env python3
"""
ADR-019 amendment patch — add ``band_name`` to ``PhotometryRow``.

Changes:
  1. Adds ``band_name: str`` field to ``PhotometryRow`` Section 3
     (Spectral / Bandpass Metadata), immediately after ``band_id``.

``band_name`` is the canonical short display label for the photometric band
(e.g., ``V``, ``B``, ``UVW1``, ``5 GHz``).  It is populated from the band
registry entry's ``band_name`` field at ingestion time and serves as the
default identifier for all public-facing outputs.

See: ADR-019 amendment (2026-04-03), "Add ``band_name`` to ``PhotometryRow``".

Usage:
    python patch_add_band_name.py path/to/contracts/models/entities.py

Precondition assertions abort with a clear message if the target text is not
found exactly as expected — safe to re-run after a failed partial application.
"""

from __future__ import annotations

import sys


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _require_absent(content: str, marker: str, label: str) -> None:
    if marker in content:
        print(f"PRECONDITION FAILED — {label!r} already present (patch may have been applied).")
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
        print(f"Usage: {sys.argv[0]} <path/to/contracts/models/entities.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src,
        "class PhotometryRow(BaseModel):",
        "PhotometryRow class definition",
    )
    _require(
        src,
        "    band_id: str = Field(\n"
        "        ...,\n"
        "        min_length=1,\n"
        "        max_length=256,\n"
        '        description="NovaCat canonical band ID resolved from the band registry (ADR-017).",\n'
        "    )",
        "band_id field definition (exact text)",
    )
    _require(
        src,
        "    regime: str = Field(",
        "regime field definition",
    )
    _require_absent(
        src,
        "    band_name: str = Field(",
        "band_name field (should not exist yet)",
    )

    print("All preconditions satisfied. Applying patch…")

    # =========================================================================
    # Patch 1 — Insert band_name field between band_id and regime.
    # =========================================================================
    OLD_BAND_ID_TO_REGIME = (
        "    band_id: str = Field(\n"
        "        ...,\n"
        "        min_length=1,\n"
        "        max_length=256,\n"
        '        description="NovaCat canonical band ID resolved from the band registry (ADR-017).",\n'
        "    )\n"
        "    regime: str = Field("
    )

    NEW_BAND_ID_TO_REGIME = (
        "    band_id: str = Field(\n"
        "        ...,\n"
        "        min_length=1,\n"
        "        max_length=256,\n"
        '        description="NovaCat canonical band ID resolved from the band registry (ADR-017).",\n'
        "    )\n"
        "    band_name: str = Field(\n"
        "        ...,\n"
        "        min_length=1,\n"
        "        max_length=256,\n"
        "        description=(\n"
        "            \"Canonical short display label for the band (e.g., 'V', 'B', 'UVW1'). \"\n"
        '            "Populated from the band registry entry\'s band_name field at ingestion "\n'
        '            "time. Default identifier for all public-facing outputs. "\n'
        '            "See ADR-019 amendment (2026-04-03)."\n'
        "        ),\n"
        "    )\n"
        "    regime: str = Field("
    )

    src = _replace_once(
        src,
        OLD_BAND_ID_TO_REGIME,
        NEW_BAND_ID_TO_REGIME,
        "Insert band_name between band_id and regime",
    )

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        ("    band_name: str = Field(", "band_name field present"),
        (
            "band registry entry's band_name field at ingestion",
            "band_name description text",
        ),
        ("    band_id: str = Field(", "band_id field still present"),
        ("    regime: str = Field(", "regime field still present"),
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
    print()
    print("Next steps:")
    print("  1. Run: mypy --strict contracts/ && ruff check contracts/")
    print("  2. Update photometry_reader.py to populate band_name from the registry entry.")
    print("  3. Update test fixtures to include band_name in PhotometryRow construction.")


if __name__ == "__main__":
    main()
