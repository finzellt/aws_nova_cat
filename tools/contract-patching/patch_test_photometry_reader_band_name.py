#!/usr/bin/env python3
"""
ADR-019 amendment patch — add ``band_name`` to photometry reader tests.

Changes:
  1. Adds ``band_name: str | None = None`` to the ``_MockEntry`` dataclass.
  2. Adds ``band_name="V"`` to the ``_V_ENTRY`` fixture (canonical hit).
     ``_GENERIC_GG_ENTRY`` deliberately keeps ``band_name=None`` to exercise
     the fallback-to-band_id path in the reader.
  3. Adds ``band_name`` assertion in the happy-path test (``test_happy_path…``).
  4. Adds ``band_name`` assertion in the generic fallback test.

See: ADR-019 amendment (2026-04-03), "Add ``band_name`` to ``PhotometryRow``".

Usage:
    python patch_test_photometry_reader_band_name.py \\
        path/to/tests/services/test_ticket_ingestor_photometry_reader.py
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
        print(
            f"Usage: {sys.argv[0]} "
            "<path/to/tests/services/test_ticket_ingestor_photometry_reader.py>"
        )
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(src, "class _MockEntry:", "_MockEntry dataclass definition")
    _require(
        src,
        "    band_id: str\n    regime: str\n    svo_filter_id: str | None = None",
        "_MockEntry fields (exact layout)",
    )
    _require(
        src,
        '_V_ENTRY = _MockEntry(\n    band_id="Generic_V",\n    regime="optical",',
        "_V_ENTRY fixture (exact layout)",
    )
    _require(
        src,
        '        assert first.band_id == "Generic_V"\n        assert first.regime == "optical"',
        "happy-path band_id + regime assertions",
    )
    _require(
        src,
        '        assert row.band_id == "Generic_Gg"\n'
        "        assert row.band_resolution_type == BandResolutionType.generic_fallback",
        "generic fallback band_id assertion",
    )
    _require_absent(
        src,
        "band_name",
        "band_name (should not appear anywhere yet)",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add band_name field to _MockEntry dataclass.
    #
    # Insert between band_id and regime to mirror the RegistryEntryLike
    # protocol and BandRegistryEntry field order.
    # =========================================================================
    src = _replace_once(
        src,
        "    band_id: str\n    regime: str\n    svo_filter_id: str | None = None",
        "    band_id: str\n"
        "    band_name: str | None = None\n"
        "    regime: str\n"
        "    svo_filter_id: str | None = None",
        "Add band_name to _MockEntry",
    )

    # =========================================================================
    # Patch 2 — Add band_name="V" to _V_ENTRY.
    #
    # This is the canonical alias hit case.  The reader should stamp
    # band_name="V" onto the PhotometryRow.
    # =========================================================================
    src = _replace_once(
        src,
        '_V_ENTRY = _MockEntry(\n    band_id="Generic_V",\n    regime="optical",',
        "_V_ENTRY = _MockEntry(\n"
        '    band_id="Generic_V",\n'
        '    band_name="V",\n'
        '    regime="optical",',
        'Add band_name="V" to _V_ENTRY',
    )

    # =========================================================================
    # Patch 3 — Assert band_name in happy-path test.
    #
    # The V entry has band_name="V", so the PhotometryRow should carry it.
    # =========================================================================
    src = _replace_once(
        src,
        '        assert first.band_id == "Generic_V"\n        assert first.regime == "optical"',
        '        assert first.band_id == "Generic_V"\n'
        '        assert first.band_name == "V"\n'
        '        assert first.regime == "optical"',
        "Assert band_name in happy-path test",
    )

    # =========================================================================
    # Patch 4 — Assert band_name fallback in generic fallback test.
    #
    # _GENERIC_GG_ENTRY has band_name=None, so the reader's defensive
    # fallback should produce band_name == band_id == "Generic_Gg".
    # =========================================================================
    src = _replace_once(
        src,
        '        assert row.band_id == "Generic_Gg"\n'
        "        assert row.band_resolution_type == BandResolutionType.generic_fallback",
        '        assert row.band_id == "Generic_Gg"\n'
        '        assert row.band_name == "Generic_Gg"  # fallback: band_name was None\n'
        "        assert row.band_resolution_type == BandResolutionType.generic_fallback",
        "Assert band_name fallback in generic fallback test",
    )

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        ("    band_name: str | None = None\n    regime: str", "band_name in _MockEntry"),
        ('    band_name="V",', "band_name on _V_ENTRY"),
        ('assert first.band_name == "V"', "band_name assertion in happy-path"),
        (
            'assert row.band_name == "Generic_Gg"  # fallback: band_name was None',
            "band_name fallback assertion in generic fallback test",
        ),
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
    print("  1. Run: pytest tests/services/test_ticket_ingestor_photometry_reader.py -v")
    print("  2. Run: mypy --strict tests/ && ruff check tests/")


if __name__ == "__main__":
    main()
