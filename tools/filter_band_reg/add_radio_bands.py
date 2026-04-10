#!/usr/bin/env python3
"""
Insert radio band entries into band_specs.json.

Radio bands have no SVO profiles, so they must be present in band_specs.json
as sparse entries to survive seed rebuilds. This script idempotently adds
any missing radio entries — if a band_id already exists, it is skipped.

Usage:
    python tools/filter_band_reg/add_radio_bands.py
    python tools/filter_band_reg/add_radio_bands.py --dry-run
    python tools/filter_band_reg/add_radio_bands.py --specs /alt/band_specs.json

Operator tooling — no CI requirements.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_SPECS_PATH = _SCRIPT_DIR / "band_specs.json"

# ---------------------------------------------------------------------------
# Radio band definitions (from band_registry.json, pre-seed-rebuild)
# ---------------------------------------------------------------------------

_RADIO_ENTRIES: list[dict] = [
    {
        "_comment": "── Radio bands (sparse — no SVO profiles) ──────────────────────────",
        "band_id": "Radio_1.4_GHz",
        "aliases": ["Radio_1.4_GHz", "1.4 GHz", "1.4GHz", "1.4_GHz", "L band"],
        "band_name": "1.4 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 1.4,
    },
    {
        "band_id": "Radio_4.9_GHz",
        "aliases": ["Radio_4.9_GHz", "4.9 GHz", "4.9GHz", "4.9_GHz", "C band"],
        "band_name": "4.9 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 4.9,
    },
    {
        "band_id": "Radio_8.5_GHz",
        "aliases": ["Radio_8.5_GHz", "8.5 GHz", "8.5GHz", "8.5_GHz", "X band"],
        "band_name": "8.5 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 8.5,
    },
    {
        "band_id": "Radio_15_GHz",
        "aliases": ["Radio_15_GHz", "15 GHz", "15GHz", "15_GHz", "Ku band"],
        "band_name": "15 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 15.0,
    },
    {
        "band_id": "Radio_22_GHz",
        "aliases": ["Radio_22_GHz", "22 GHz", "22GHz", "22_GHz", "K band"],
        "band_name": "22 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 22.0,
    },
    {
        "band_id": "Radio_34.8_GHz",
        "aliases": ["Radio_34.8_GHz", "34.8 GHz", "34.8GHz", "34.8_GHz", "Ka band"],
        "band_name": "34.8 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 34.8,
    },
    {
        "band_id": "Radio_44_GHz",
        "aliases": ["Radio_44_GHz", "44 GHz", "44GHz", "44_GHz", "Q band"],
        "band_name": "44 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 44.0,
    },
    {
        "band_id": "Radio_230_GHz",
        "aliases": ["Radio_230_GHz", "230 GHz", "230GHz", "230_GHz", "1.3 mm"],
        "band_name": "230 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 230.0,
    },
    {
        "band_id": "Radio_345_GHz",
        "aliases": ["Radio_345_GHz", "345 GHz", "345GHz", "345_GHz", "0.87 mm"],
        "band_name": "345 GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": 345.0,
    },
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Insert radio band entries into band_specs.json (idempotent).",
    )
    parser.add_argument(
        "--specs",
        default=None,
        help=f"Path to band_specs.json (default: {_DEFAULT_SPECS_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be added without writing",
    )
    args = parser.parse_args()

    specs_path = Path(args.specs) if args.specs else _DEFAULT_SPECS_PATH

    if not specs_path.exists():
        print(f"ERROR: {specs_path} not found", file=sys.stderr)
        sys.exit(1)

    raw = json.loads(specs_path.read_text(encoding="utf-8"))
    bands = raw["bands"]
    existing_ids = {b["band_id"] for b in bands}

    # Collect aliases already in use for collision detection
    existing_aliases: dict[str, str] = {}
    for b in bands:
        for alias in b.get("aliases", []):
            existing_aliases[alias] = b["band_id"]

    added = []
    skipped = []
    alias_conflicts = []

    for entry in _RADIO_ENTRIES:
        bid = entry["band_id"]

        if bid in existing_ids:
            skipped.append(bid)
            continue

        # Check for alias collisions
        conflicts = []
        for alias in entry["aliases"]:
            if alias in existing_aliases:
                conflicts.append(f"{alias!r} (owned by {existing_aliases[alias]})")
        if conflicts:
            alias_conflicts.append((bid, conflicts))
            continue

        bands.append(entry)
        # Register aliases so subsequent entries in this batch can detect collisions
        existing_ids.add(bid)
        for alias in entry["aliases"]:
            existing_aliases[alias] = bid
        added.append(bid)

    # Report
    print(f"\nRadio bands in band_specs.json: {specs_path}\n")

    if added:
        print(f"  Added ({len(added)}):")
        for bid in added:
            print(f"    ✓ {bid}")

    if skipped:
        print(f"\n  Already present ({len(skipped)}):")
        for bid in skipped:
            print(f"    · {bid}")

    if alias_conflicts:
        print(f"\n  ✗ Alias conflicts ({len(alias_conflicts)}):")
        for bid, conflicts in alias_conflicts:
            print(f"    {bid}:")
            for c in conflicts:
                print(f"      {c}")

    if not added:
        print("\n  Nothing to add.")
        return

    if args.dry_run:
        print(f"\n  Dry run — {len(added)} entries would be added.\n")
        return

    # Write
    output = json.dumps(raw, indent=2, ensure_ascii=False)
    specs_path.write_text(output + "\n", encoding="utf-8")
    print(f"\n  Wrote {specs_path} ({len(added)} entries added)\n")


if __name__ == "__main__":
    main()
