#!/usr/bin/env python3
"""
Patch — test_ticket_ingestor_photometry_reader.py alias ownership fix.

Updates the mock band registry and all dependent assertions to reflect
the corrected alias ownership rule (ADR-017 amendment, 2026-04-01):
bare "V" resolves to Generic_V, not JohnsonCousins_V.

Changes:
  1. Mock _V_ENTRY: band_id, svo_filter_id, lambda_eff, bandpass_width
     updated to reflect Generic_V with Bessell reference data.
  2. Mock _ALIAS_INDEX: "V" → "Generic_V".
  3. Mock _ENTRIES dict: key "JohnsonCousins_V" → "Generic_V".
  4. TestResolveBand: band_id assertion updated.
  5. Happy-path assertions: band_id updated.
  6. TestDeriveRowId: all "JohnsonCousins_V" → "Generic_V" for consistency.

Usage:
    python patch_test_photometry_reader.py tests/services/test_ticket_ingestor_photometry_reader.py

Precondition assertions abort with a clear message if the target text is
not found exactly as expected — safe to re-run after a failed partial
application.
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


def _replace_all(content: str, old: str, new: str, label: str) -> str:
    count = content.count(old)
    if count == 0:
        print(f"REPLACE FAILED — anchor for {label!r} not found.")
        sys.exit(1)
    print(f"  {label}: {count} occurrence(s)")
    return content.replace(old, new)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/test_ticket_ingestor_photometry_reader.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # =====================================================================
    # Precondition checks
    # =====================================================================
    _require(src, '"V": "JohnsonCousins_V"', "alias index V → JohnsonCousins_V")
    _require(src, '"JohnsonCousins_V": _V_ENTRY', "entries dict key")
    _require(src, 'band_id="JohnsonCousins_V"', "_V_ENTRY band_id")
    _require(src, 'svo_filter_id="Generic/Bessell_V.dat"', "_V_ENTRY svo_filter_id")
    _require(src, 'assert res.band_id == "JohnsonCousins_V"', "TestResolveBand assertion")
    _require(src, 'assert first.band_id == "JohnsonCousins_V"', "happy path assertion")

    print("All preconditions satisfied. Applying patches…\n")

    # =====================================================================
    # Patch 1 — Mock _V_ENTRY definition
    # =====================================================================
    src = _replace_once(
        src,
        (
            "# Registry contents used across tests:\n"
            '#   "V"              → JohnsonCousins_V (canonical, not excluded)\n'
            '#   "EXCLUDED_BAND"  → Generic_EXCLUDED_BAND (alias exists, but excluded)\n'
            '#   "Gg"             → no alias, but Generic_Gg entry exists (generic fallback)\n'
            '#   "XYZ"            → no alias, no Generic_XYZ entry (unresolvable)\n'
            "_V_ENTRY = _MockEntry(\n"
            '    band_id="JohnsonCousins_V",\n'
            '    regime="optical",\n'
            '    svo_filter_id="Generic/Bessell_V.dat",\n'
            "    lambda_eff=5448.0,\n"
            "    spectral_coord_unit=SpectralCoordUnit.angstrom,\n"
            "    bandpass_width=840.0,\n"
            ")"
        ),
        (
            "# Registry contents used across tests:\n"
            '#   "V"              → Generic_V (alias on Generic entry per ADR-017\n'
            "#                      amendment § Alias Ownership Invariant)\n"
            '#   "EXCLUDED_BAND"  → Generic_EXCLUDED_BAND (alias exists, but excluded)\n'
            '#   "Gg"             → no alias, but Generic_Gg entry exists (generic fallback)\n'
            '#   "XYZ"            → no alias, no Generic_XYZ entry (unresolvable)\n'
            "_V_ENTRY = _MockEntry(\n"
            '    band_id="Generic_V",\n'
            '    regime="optical",\n'
            '    svo_filter_id="HCT/HFOSC.Bessell_V",\n'
            "    lambda_eff=5696.92,\n"
            "    spectral_coord_unit=SpectralCoordUnit.angstrom,\n"
            "    bandpass_width=1584.54,\n"
            ")"
        ),
        "mock _V_ENTRY definition",
    )

    # =====================================================================
    # Patch 2 — Mock alias index: "V" → "Generic_V"
    # =====================================================================
    src = _replace_once(
        src,
        '        "V": "JohnsonCousins_V",',
        '        "V": "Generic_V",',
        "alias index V entry",
    )

    # =====================================================================
    # Patch 3 — Mock entries dict: key "JohnsonCousins_V" → "Generic_V"
    # =====================================================================
    src = _replace_once(
        src,
        '        "JohnsonCousins_V": _V_ENTRY,',
        '        "Generic_V": _V_ENTRY,',
        "entries dict key",
    )

    # =====================================================================
    # Patch 4 — TestResolveBand assertion
    # =====================================================================
    src = _replace_once(
        src,
        (
            "    def test_alias_match_returns_canonical_high(self) -> None:\n"
            '        res = _resolve_band("V", _REGISTRY)\n'
            '        assert res.band_id == "JohnsonCousins_V"\n'
            "        assert res.resolution_type == BandResolutionType.canonical\n"
            "        assert res.confidence == BandResolutionConfidence.high"
        ),
        (
            "    def test_alias_match_returns_canonical_high(self) -> None:\n"
            '        # "V" is a direct alias on Generic_V per alias ownership rule.\n'
            "        # Step 1 (alias lookup) fires → canonical / high.\n"
            '        res = _resolve_band("V", _REGISTRY)\n'
            '        assert res.band_id == "Generic_V"\n'
            "        assert res.resolution_type == BandResolutionType.canonical\n"
            "        assert res.confidence == BandResolutionConfidence.high"
        ),
        "TestResolveBand alias match assertion",
    )

    # =====================================================================
    # Patch 5 — Happy-path band_id assertion
    # =====================================================================
    src = _replace_once(
        src,
        'assert first.band_id == "JohnsonCousins_V"',
        'assert first.band_id == "Generic_V"',
        "happy path band_id assertion",
    )

    # =====================================================================
    # Patch 6 — TestDeriveRowId: all "JohnsonCousins_V" → "Generic_V"
    # =====================================================================
    src = _replace_all(
        src,
        '"JohnsonCousins_V"',
        '"Generic_V"',
        "TestDeriveRowId band_id references",
    )

    # =====================================================================
    # Post-condition checks
    # =====================================================================
    checks = [
        ('"V": "Generic_V"', "alias index updated"),
        ('"Generic_V": _V_ENTRY', "entries dict key updated"),
        ('band_id="Generic_V"', "_V_ENTRY band_id updated"),
        ('svo_filter_id="HCT/HFOSC.Bessell_V"', "_V_ENTRY svo_filter_id updated"),
        ('assert res.band_id == "Generic_V"', "TestResolveBand assertion updated"),
        ('assert first.band_id == "Generic_V"', "happy path assertion updated"),
    ]

    failed = False
    for marker, label in checks:
        if marker not in src:
            print(f"POSTCONDITION FAILED — {label!r}")
            failed = True

    # Verify no stale JohnsonCousins_V references remain
    if "JohnsonCousins_V" in src:
        print("POSTCONDITION FAILED — stale 'JohnsonCousins_V' reference(s) remain")
        failed = True

    if failed:
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"\nPatched successfully: {path}")
    print()
    print("Follow-up needed:")
    print("  _resolve_band() assigns canonical/high for any Step 1 alias match,")
    print("  even when target is a Generic entry. Consider adding a post-Step-1")
    print("  check to downgrade to generic_fallback/low for Generic_ band_ids.")
    print("  See ADR-017 amendment Decision 4 note.")


if __name__ == "__main__":
    main()
