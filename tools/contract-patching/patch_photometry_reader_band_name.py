#!/usr/bin/env python3
"""
ADR-019 amendment patch — add ``band_name`` to ``photometry_reader.py``.

Changes:
  1. Adds ``band_name`` property to the ``RegistryEntryLike`` protocol so
     the reader can access it from registry entries.
  2. Populates ``band_name`` on the ``PhotometryRow`` constructor call in
     ``_transform_row()``, with a defensive fallback to ``band_id`` when
     the registry entry's ``band_name`` is ``None``.

See: ADR-019 amendment (2026-04-03), "Add ``band_name`` to ``PhotometryRow``".

Usage:
    python patch_photometry_reader_band_name.py \\
        path/to/services/ticket_ingestor/photometry_reader.py
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
        print(f"Usage: {sys.argv[0]} <path/to/photometry_reader.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src,
        "class RegistryEntryLike(Protocol):",
        "RegistryEntryLike protocol definition",
    )
    _require(
        src,
        "    @property\n"
        "    def band_id(self) -> str: ...\n"
        "\n"
        "    @property\n"
        "    def regime(self) -> str: ...",
        "band_id and regime properties in RegistryEntryLike (exact layout)",
    )
    _require(
        src,
        "            band_id=band_res.band_id,\n            regime=entry.regime,",
        "PhotometryRow constructor band_id → regime sequence",
    )
    _require_absent(
        src,
        "def band_name(self)",
        "band_name property on RegistryEntryLike (should not exist yet)",
    )
    _require_absent(
        src,
        "band_name=",
        "band_name kwarg in PhotometryRow constructor (should not exist yet)",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add band_name property to RegistryEntryLike protocol.
    #
    # Insert between band_id and regime.  The registry's BandRegistryEntry
    # has band_name: str | None, so the protocol mirrors that.
    # =========================================================================
    OLD_PROTOCOL_PROPS = (
        "    @property\n"
        "    def band_id(self) -> str: ...\n"
        "\n"
        "    @property\n"
        "    def regime(self) -> str: ..."
    )

    NEW_PROTOCOL_PROPS = (
        "    @property\n"
        "    def band_id(self) -> str: ...\n"
        "\n"
        "    @property\n"
        "    def band_name(self) -> str | None: ...\n"
        "\n"
        "    @property\n"
        "    def regime(self) -> str: ..."
    )

    src = _replace_once(
        src,
        OLD_PROTOCOL_PROPS,
        NEW_PROTOCOL_PROPS,
        "Add band_name to RegistryEntryLike",
    )

    # =========================================================================
    # Patch 2 — Populate band_name in the PhotometryRow constructor.
    #
    # Defensive fallback: if the registry entry's band_name is None (should
    # not happen for non-excluded entries, but defensive), use band_id.
    # =========================================================================
    OLD_CONSTRUCTOR = "            band_id=band_res.band_id,\n            regime=entry.regime,"

    NEW_CONSTRUCTOR = (
        "            band_id=band_res.band_id,\n"
        "            band_name=entry.band_name if entry.band_name else band_res.band_id,\n"
        "            regime=entry.regime,"
    )

    src = _replace_once(
        src,
        OLD_CONSTRUCTOR,
        NEW_CONSTRUCTOR,
        "Add band_name to PhotometryRow constructor",
    )

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        ("    def band_name(self) -> str | None: ...", "band_name in RegistryEntryLike"),
        (
            "band_name=entry.band_name if entry.band_name else band_res.band_id,",
            "band_name in constructor",
        ),
        ("band_id=band_res.band_id,", "band_id still in constructor"),
        ("regime=entry.regime,", "regime still in constructor"),
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
    print("  1. Update the test's FakeRegistryEntry dataclass to include band_name.")
    print(
        "  2. Run: mypy --strict services/ticket_ingestor/ && ruff check services/ticket_ingestor/"
    )


if __name__ == "__main__":
    main()
