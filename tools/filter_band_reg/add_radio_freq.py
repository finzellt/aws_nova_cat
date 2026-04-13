#!/usr/bin/env python3
"""
Add radio bands to the filter band registry (end-to-end).

Takes one or more radio frequency strings, generates sparse band_specs.json
entries, inserts them, reseeds band_registry.json, and copies the result to
services/.  Idempotent — existing band_ids are skipped.

Usage:
    # Preview what would be added (no changes)
    python tools/filter_band_reg/add_radio_freq.py "1.74 GHz" "2.40 GHz" "28.20 GHz"

    # Apply: insert → reseed → copy to services/
    python tools/filter_band_reg/add_radio_freq.py "1.74 GHz" "2.40 GHz" --apply

    # Also accepts bare numbers (assumed GHz)
    python tools/filter_band_reg/add_radio_freq.py 1.74 2.40 0.61 --apply

Operator tooling — no CI requirements (mypy / ruff not enforced).
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths (relative to this script's location in tools/filter_band_reg/)
# ---------------------------------------------------------------------------

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_SPECS_PATH = _SCRIPT_DIR / "band_specs.json"
_DEFAULT_SEED_OUTPUT = _SCRIPT_DIR / "band_registry.json"
_DEFAULT_REGISTRY_PATH = (
    _SCRIPT_DIR.parent.parent
    / "services"
    / "photometry_ingestor"
    / "band_registry"
    / "band_registry.json"
)

# ---------------------------------------------------------------------------
# Display helpers (matching project style)
# ---------------------------------------------------------------------------

_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_CYAN = "\033[96m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"


def _ok(msg: str) -> None:
    print(f"  {_GREEN}✓{_RESET} {msg}")


def _fail(msg: str) -> None:
    print(f"  {_RED}✗{_RESET} {msg}")


def _warn(msg: str) -> None:
    print(f"  {_YELLOW}⚠{_RESET} {msg}")


def _info(msg: str) -> None:
    print(f"  {_DIM}{msg}{_RESET}")


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {_BOLD}{title}{_RESET}")
    print(f"{'─' * 60}")


# ---------------------------------------------------------------------------
# Frequency parsing
# ---------------------------------------------------------------------------

_FREQ_RE = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*(?:GHz|ghz|MHz|mhz)?\s*$",
    re.IGNORECASE,
)


def parse_freq_ghz(raw: str) -> float | None:
    """Parse a frequency string into GHz.

    Accepts: "1.74 GHz", "1.74GHz", "1.74", "1400 MHz".
    Returns None if unparseable.
    """
    m = _FREQ_RE.match(raw.strip())
    if m is None:
        return None

    freq = float(m.group(1))
    # Detect MHz by keyword presence
    if re.search(r"MHz", raw, re.IGNORECASE):
        freq /= 1000.0

    if freq <= 0:
        return None
    return freq


# ---------------------------------------------------------------------------
# Entry generation
# ---------------------------------------------------------------------------


def _format_freq_str(freq_ghz: float) -> str:
    """Format frequency for display/alias.

    Drops trailing zeros: 1.40 → '1.4', 28.20 → '28.2', 0.61 → '0.61'.
    Integers get no decimal: 28.0 → '28'.
    """
    if freq_ghz == int(freq_ghz):
        return str(int(freq_ghz))
    # Strip trailing zeros but keep at least one decimal digit
    return f"{freq_ghz:.10f}".rstrip("0").rstrip(".")


def make_radio_entry(freq_ghz: float) -> dict:
    """Build a band_specs.json entry for a radio band at the given frequency."""
    fs = _format_freq_str(freq_ghz)
    band_id = f"Radio_{fs}_GHz"

    return {
        "band_id": band_id,
        "aliases": [
            band_id,           # Radio_1.74_GHz
            f"{fs} GHz",       # 1.74 GHz
            f"{fs}GHz",        # 1.74GHz
            f"{fs}_GHz",       # 1.74_GHz
        ],
        "band_name": f"{fs} GHz",
        "regime": "radio",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": freq_ghz,
    }


# ---------------------------------------------------------------------------
# Insertion into band_specs.json
# ---------------------------------------------------------------------------


def insert_entries(
    specs_path: Path,
    entries: list[dict],
) -> tuple[list[str], list[str], list[tuple[str, list[str]]]]:
    """Insert entries into band_specs.json.  Returns (added, skipped, conflicts)."""
    raw = json.loads(specs_path.read_text(encoding="utf-8"))
    bands = raw["bands"]

    existing_ids = {b["band_id"] for b in bands}
    existing_aliases: dict[str, str] = {}
    for b in bands:
        for alias in b.get("aliases", []):
            existing_aliases[alias] = b["band_id"]

    added: list[str] = []
    skipped: list[str] = []
    conflicts: list[tuple[str, list[str]]] = []

    for entry in entries:
        bid = entry["band_id"]

        if bid in existing_ids:
            skipped.append(bid)
            continue

        # Check alias collisions
        alias_conflicts = []
        for alias in entry["aliases"]:
            if alias in existing_aliases:
                alias_conflicts.append(
                    f"{alias!r} (owned by {existing_aliases[alias]})"
                )
        if alias_conflicts:
            conflicts.append((bid, alias_conflicts))
            continue

        bands.append(entry)
        existing_ids.add(bid)
        for alias in entry["aliases"]:
            existing_aliases[alias] = bid
        added.append(bid)

    # Write back
    if added:
        output = json.dumps(raw, indent=2, ensure_ascii=False)
        specs_path.write_text(output + "\n", encoding="utf-8")

    return added, skipped, conflicts


# ---------------------------------------------------------------------------
# Reseed + copy pipeline (from propose_filters.py)
# ---------------------------------------------------------------------------


def reseed_and_copy(
    specs_path: Path,
    seed_output: Path,
    registry_path: Path,
) -> bool:
    """Run seed_band_registry.py and copy result to services/.

    Returns True if all steps succeed.
    """
    _section("Reseeding band_registry.json")

    # seed_band_registry.py is in the same directory
    sys.path.insert(0, str(_SCRIPT_DIR))
    try:
        from seed_band_registry import main as seed_main

        rc = seed_main(["--specs", str(specs_path), "--output", str(seed_output)])
        if rc != 0:
            _fail(f"seed_band_registry.py exited with code {rc}")
            return False
        _ok(f"Seed complete: {seed_output}")
    except Exception as exc:
        _fail(f"Seed failed: {type(exc).__name__}: {exc}")
        return False

    _section("Copying to services/")
    try:
        shutil.copy2(seed_output, registry_path)
        _ok(f"Copied to {registry_path}")
    except Exception as exc:
        _fail(f"Copy failed: {exc}")
        return False

    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Add radio bands to the filter band registry. "
            "Takes frequency strings, inserts into band_specs.json, "
            "reseeds band_registry.json, and copies to services/."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "freqs",
        nargs="+",
        help=(
            "Radio frequencies to add. "
            "Accepts '1.74 GHz', '1.74GHz', '1.74' (assumed GHz), '1400 MHz'."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write to band_specs.json, reseed, and copy to services/",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview entries without writing anything",
    )
    parser.add_argument(
        "--specs",
        default=None,
        help=f"Path to band_specs.json (default: {_DEFAULT_SPECS_PATH})",
    )
    parser.add_argument(
        "--registry",
        default=None,
        help=f"Path to band_registry.json in services/ (default: {_DEFAULT_REGISTRY_PATH})",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=f"Seed output path (default: {_DEFAULT_SEED_OUTPUT})",
    )
    args = parser.parse_args()

    specs_path = Path(args.specs) if args.specs else _DEFAULT_SPECS_PATH
    registry_path = Path(args.registry) if args.registry else _DEFAULT_REGISTRY_PATH
    seed_output = Path(args.output) if args.output else _DEFAULT_SEED_OUTPUT

    if not specs_path.exists():
        print(f"{_RED}band_specs.json not found:{_RESET} {specs_path}", file=sys.stderr)
        sys.exit(1)

    # --- Parse frequencies ---
    _section(f"Parsing {len(args.freqs)} frequency argument(s)")

    entries: list[dict] = []
    for raw in args.freqs:
        freq = parse_freq_ghz(raw)
        if freq is None:
            _fail(f"Cannot parse frequency: {raw!r}")
            sys.exit(1)
        entry = make_radio_entry(freq)
        entries.append(entry)
        _ok(f"{raw!r:15s} → {entry['band_id']:25s}  (center={freq} GHz)")

    # --- Preview entries ---
    _section("Proposed band_specs.json entries")
    for entry in entries:
        print(json.dumps(entry, indent=2))
        print()

    # --- Check against existing ---
    _section("Checking against existing registry")
    raw_specs = json.loads(specs_path.read_text(encoding="utf-8"))
    existing_ids = {b["band_id"] for b in raw_specs["bands"]}
    existing_aliases: dict[str, str] = {}
    for b in raw_specs["bands"]:
        for alias in b.get("aliases", []):
            existing_aliases[alias] = b["band_id"]

    will_add = 0
    will_skip = 0
    has_conflicts = False

    for entry in entries:
        bid = entry["band_id"]
        if bid in existing_ids:
            _info(f"{bid} — already exists (will skip)")
            will_skip += 1
            continue

        alias_hits = [
            f"{a!r} → {existing_aliases[a]}"
            for a in entry["aliases"]
            if a in existing_aliases
        ]
        if alias_hits:
            _warn(f"{bid} — alias conflicts: {', '.join(alias_hits)}")
            has_conflicts = True
        else:
            _ok(f"{bid} — new, no conflicts")
            will_add += 1

    # --- Summary ---
    _section("Summary")
    print(f"  New bands to add:   {will_add}")
    print(f"  Already present:    {will_skip}")
    if has_conflicts:
        print(f"  {_RED}Alias conflicts found — resolve before applying{_RESET}")

    if has_conflicts:
        sys.exit(1)

    if will_add == 0:
        _info("\nNothing to add.")
        return

    # --- Apply or stop ---
    if args.apply and not args.dry_run:
        _section("Inserting into band_specs.json")
        added, skipped, conflicts = insert_entries(specs_path, entries)

        for bid in added:
            _ok(f"Added {bid}")
        for bid in skipped:
            _info(f"Skipped {bid} (already present)")
        for bid, c in conflicts:
            _fail(f"Conflict for {bid}: {c}")

        if not added:
            _info("No entries added.")
            return

        _ok(f"Wrote {specs_path} ({len(added)} entries added)")

        # Reseed + copy
        ok = reseed_and_copy(specs_path, seed_output, registry_path)
        if ok:
            print(f"\n  {_GREEN}{_BOLD}Done.{_RESET} {len(added)} radio band(s) added end-to-end.")
            print(f"  Run try-parse to verify resolution.\n")
        else:
            print(f"\n  {_RED}Reseed/copy failed — review output above.{_RESET}")
            print(f"  band_specs.json was updated; reseed manually if needed.\n")
            sys.exit(1)
    else:
        if args.dry_run:
            _info("\nDry run — no changes written.")
        else:
            _info("\nRun with --apply to insert, reseed, and copy to services/.")
        print()


if __name__ == "__main__":
    main()
