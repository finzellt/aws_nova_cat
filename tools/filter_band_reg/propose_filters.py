#!/usr/bin/env python3
"""
Propose new band registry entries for unresolvable filter strings.

Takes filter strings (typically surfaced by ``batch_ingest.py try-parse``)
and generates proposed ``band_specs.json`` entries — either as new aliases
on existing entries or as entirely new entries with SVO data.

Usage::

    # Preview proposals (no changes written)
    python tools/filter_band_reg/propose_filters.py "SG" "SR" "SI"

    # Apply: update band_specs.json, reseed band_registry.json, copy to services/
    python tools/filter_band_reg/propose_filters.py "SG" "SR" --apply

    # Dry run (same as default, with explicit messaging)
    python tools/filter_band_reg/propose_filters.py "SG" "SR" --dry-run

Workflow::

    try-parse data/                         # surfaces "Band Registry Gaps"
    propose_filters "SG" "SR" --apply       # adds aliases / entries, reseeds
    try-parse data/                         # confirms everything resolves
    git commit                              # band_specs.json + band_registry.json

Operator tooling — no CI requirements (mypy / ruff not enforced).
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_SPECS_PATH = _SCRIPT_DIR / "band_specs.json"
_DEFAULT_REGISTRY_PATH = (
    _SCRIPT_DIR.parent.parent
    / "services"
    / "photometry_ingestor"
    / "band_registry"
    / "band_registry.json"
)
_DEFAULT_SEED_OUTPUT = _SCRIPT_DIR / "band_registry.json"

# ---------------------------------------------------------------------------
# Display helpers (same style as batch_ingest.py)
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
# AAVSO code mapping (band_grouping_rules.md Rule S4)
# ---------------------------------------------------------------------------

# Maps well-known AAVSO shorthand codes to the band_id of the existing entry
# they should become aliases on.  If the target entry doesn't exist yet, we
# propose creating it.
_AAVSO_ALIAS_MAP: dict[str, str] = {
    # Sloan unprimed
    "SU": "SLOAN_SDSS_u",
    "SG": "SLOAN_SDSS_g",
    "SR": "SLOAN_SDSS_r",
    "SI": "SLOAN_SDSS_i",
    "SZ": "SLOAN_SDSS_z",
    # Cousins — distinct from Johnson/Bessell R/I per band_grouping_rules.md Rule S3
    "Rc": "Generic_Rc",
    "Ic": "Generic_Ic",
    # Excluded AAVSO modes
    "CV": "AAVSO_CV",
    "CR": "AAVSO_CR",
    "TG": "AAVSO_TG",
    "Vis.": "AAVSO_Vis",
}

# New Generic entries for Cousins Rc/Ic (SVO profiles added to local svo_fps.db)
_NEW_GENERIC_ENTRIES: dict[str, dict[str, Any]] = {
    "Generic_Rc": {
        "band_id": "Generic_Rc",
        "aliases": ["Generic_Rc", "Cousins_Rc", "Rc"],
        "band_name": "Rc",
        "regime": "optical",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [
            {"filter_id": "Generic/Cousins.R", "facility": "Generic", "instrument": "Cousins"}
        ],
        "sparse": False,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": None,
    },
    "Generic_Ic": {
        "band_id": "Generic_Ic",
        "aliases": ["Generic_Ic", "Cousins_Ic", "Ic"],
        "band_name": "Ic",
        "regime": "optical",
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [
            {"filter_id": "Generic/Cousins.I", "facility": "Generic", "instrument": "Cousins"}
        ],
        "sparse": False,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": None,
    },
}

# New excluded entries to create when the target band_id doesn't already exist
_NEW_EXCLUDED_ENTRIES: dict[str, dict[str, Any]] = {
    "AAVSO_CV": {
        "band_id": "AAVSO_CV",
        "aliases": ["AAVSO_CV", "CV"],
        "band_name": None,
        "regime": None,
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": True,
        "exclusion_reason": "unfiltered CCD observation (AAVSO CV band code)",
        "lambda_eff_hint": None,
    },
    "AAVSO_CR": {
        "band_id": "AAVSO_CR",
        "aliases": ["AAVSO_CR", "CR"],
        "band_name": None,
        "regime": None,
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": True,
        "exclusion_reason": "unfiltered CCD observation, red zero-point (AAVSO CR band code)",
        "lambda_eff_hint": None,
    },
    "AAVSO_TG": {
        "band_id": "AAVSO_TG",
        "aliases": ["AAVSO_TG", "TG"],
        "band_name": None,
        "regime": None,
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": [],
        "sparse": True,
        "excluded": True,
        "exclusion_reason": "unfiltered CCD observation, green/tri-colour (AAVSO TG band code)",
        "lambda_eff_hint": None,
    },
}

# SVO candidate patterns to try for unknown filter strings
_SVO_CANDIDATE_PATTERNS = [
    ("SLOAN/SDSS.{fs}", "SLOAN", None),
    ("HCT/HFOSC.Bessell_{fs}", "HCT", "HFOSC"),
    ("Generic/Johnson.{fs}", "Generic", "Johnson"),
    ("2MASS/2MASS.{fs}", "2MASS", None),
]


# ---------------------------------------------------------------------------
# Proposal types
# ---------------------------------------------------------------------------


class _AddAlias:
    """Proposal: add a filter string as a new alias on an existing entry."""

    def __init__(self, filter_string: str, target_band_id: str, reason: str):
        self.filter_string = filter_string
        self.target_band_id = target_band_id
        self.reason = reason


class _NewEntry:
    """Proposal: add an entirely new band_specs.json entry."""

    def __init__(self, filter_string: str, spec_dict: dict[str, Any], svo_ok: bool):
        self.filter_string = filter_string
        self.spec_dict = spec_dict
        self.svo_ok = svo_ok


class _AlreadyExists:
    """The filter string is already an alias in the registry."""

    def __init__(self, filter_string: str, existing_band_id: str):
        self.filter_string = filter_string
        self.existing_band_id = existing_band_id


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def _load_existing_alias_index(specs_path: Path, registry_path: Path) -> dict[str, str]:
    """Build a combined alias→band_id index from both band_specs.json and band_registry.json."""
    index: dict[str, str] = {}

    # From band_specs.json
    if specs_path.exists():
        raw = json.loads(specs_path.read_text(encoding="utf-8"))
        for entry in raw.get("bands", []):
            for alias in entry.get("aliases", []):
                index[alias] = entry["band_id"]

    # From band_registry.json (may have additional aliases from manual edits)
    if registry_path.exists():
        raw = json.loads(registry_path.read_text(encoding="utf-8"))
        for entry in raw.get("bands", []):
            for alias in entry.get("aliases", []):
                if alias not in index:
                    index[alias] = entry["band_id"]

    return index


def _load_specs_band_ids(specs_path: Path) -> set[str]:
    """Return the set of band_ids in band_specs.json."""
    if not specs_path.exists():
        return set()
    raw = json.loads(specs_path.read_text(encoding="utf-8"))
    return {entry["band_id"] for entry in raw.get("bands", [])}


def _propose_generic_entry(
    filter_string: str,
) -> dict[str, Any]:
    """Build a Generic_<filter_string> band_specs entry with SVO candidates to try."""
    band_id = f"Generic_{filter_string}"
    candidates = []
    for pattern, facility, instrument in _SVO_CANDIDATE_PATTERNS:
        candidates.append({
            "filter_id": pattern.format(fs=filter_string),
            "facility": facility,
            "instrument": instrument,
        })

    return {
        "band_id": band_id,
        "aliases": [band_id, filter_string],
        "band_name": filter_string,
        "regime": "optical",  # default guess — operator should review
        "observatory_facility": None,
        "instrument": None,
        "svo_candidates": candidates,
        "sparse": False,
        "excluded": False,
        "exclusion_reason": None,
        "lambda_eff_hint": None,
    }


def _propose_for_filter(
    filter_string: str,
    alias_index: dict[str, str],
    existing_band_ids: set[str],
) -> _AddAlias | _NewEntry | _AlreadyExists:
    """Determine the right proposal for a single filter string."""

    # 1. Already exists?
    if filter_string in alias_index:
        return _AlreadyExists(filter_string, alias_index[filter_string])

    # 2. AAVSO code mapping?
    if filter_string in _AAVSO_ALIAS_MAP:
        target = _AAVSO_ALIAS_MAP[filter_string]
        if target in existing_band_ids:
            return _AddAlias(filter_string, target, "AAVSO code")
        elif target in _NEW_EXCLUDED_ENTRIES:
            # Need to create the excluded entry
            return _NewEntry(
                filter_string,
                _NEW_EXCLUDED_ENTRIES[target],
                svo_ok=True,  # excluded entries don't need SVO
            )
        elif target in _NEW_GENERIC_ENTRIES:
            # Need to create a new Generic entry (e.g. Generic_Rc, Generic_Ic)
            return _NewEntry(
                filter_string,
                _NEW_GENERIC_ENTRIES[target],
                svo_ok=False,  # SVO lookup happens at seed time
            )
        else:
            # Target should exist but doesn't — propose as alias anyway,
            # operator will need to create the target first
            return _AddAlias(filter_string, target, f"AAVSO code (target {target} not yet in specs)")

    # 3. Generic fallback — propose a new Generic entry
    spec_dict = _propose_generic_entry(filter_string)
    return _NewEntry(filter_string, spec_dict, svo_ok=False)


# ---------------------------------------------------------------------------
# Apply logic
# ---------------------------------------------------------------------------


def _apply_proposals(
    proposals: list[_AddAlias | _NewEntry | _AlreadyExists],
    specs_path: Path,
    registry_path: Path,
    seed_output: Path,
) -> bool:
    """Apply proposals: modify band_specs.json, reseed, copy to services/.

    Returns True if all steps succeed.
    """
    # Load current band_specs.json
    raw = json.loads(specs_path.read_text(encoding="utf-8"))
    bands = raw["bands"]
    modified = False

    # Build band_id → index in the bands list for fast lookup
    id_to_idx = {b["band_id"]: i for i, b in enumerate(bands)}

    for p in proposals:
        if isinstance(p, _AlreadyExists):
            continue

        if isinstance(p, _AddAlias):
            idx = id_to_idx.get(p.target_band_id)
            if idx is None:
                _fail(f"Cannot add alias {p.filter_string!r}: target {p.target_band_id} not in specs")
                continue
            if p.filter_string not in bands[idx]["aliases"]:
                bands[idx]["aliases"].append(p.filter_string)
                _ok(f"Added alias {p.filter_string!r} to {p.target_band_id}")
                modified = True
            else:
                _info(f"Alias {p.filter_string!r} already on {p.target_band_id}")

        elif isinstance(p, _NewEntry):
            if p.spec_dict["band_id"] in id_to_idx:
                _info(f"Entry {p.spec_dict['band_id']} already in specs")
                continue
            bands.append(p.spec_dict)
            id_to_idx[p.spec_dict["band_id"]] = len(bands) - 1
            _ok(f"Added new entry: {p.spec_dict['band_id']}")
            modified = True

    if not modified:
        _info("No changes to apply.")
        return True

    # Write updated band_specs.json
    output_json = json.dumps(raw, indent=2, ensure_ascii=False)
    specs_path.write_text(output_json + "\n", encoding="utf-8")
    _ok(f"Wrote {specs_path}")

    # Reseed band_registry.json
    _section("Reseeding band_registry.json")
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

    # Copy to services/
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
            "Propose band registry entries for unresolvable filter strings. "
            "Generates new band_specs.json entries or alias additions."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "filters",
        nargs="+",
        help="Filter strings to propose entries for (e.g. 'SG' 'SR' 'XYZ')",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to band_specs.json, reseed, and copy to services/",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview proposals without writing anything (same as default, with explicit messaging)",
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

    # Load existing state
    alias_index = _load_existing_alias_index(specs_path, registry_path)
    existing_band_ids = _load_specs_band_ids(specs_path)

    # Generate proposals
    _section(f"Proposing band registry entries for {len(args.filters)} filter(s)")

    proposals: list[_AddAlias | _NewEntry | _AlreadyExists] = []
    n_alias = 0
    n_new = 0
    n_exists = 0
    n_sparse = 0

    for fs in args.filters:
        print(f"\n  {_CYAN}▸{_RESET} {fs!r}")

        proposal = _propose_for_filter(fs, alias_index, existing_band_ids)
        proposals.append(proposal)

        if isinstance(proposal, _AlreadyExists):
            _info(f"Already in registry as alias on {proposal.existing_band_id}")
            n_exists += 1

        elif isinstance(proposal, _AddAlias):
            _ok(f"{proposal.reason} → add alias {fs!r} to existing entry {proposal.target_band_id}")
            n_alias += 1

        elif isinstance(proposal, _NewEntry):
            if proposal.spec_dict.get("excluded"):
                _ok(f"New excluded entry: {proposal.spec_dict['band_id']}")
            elif proposal.svo_ok:
                _ok(f"New entry: {proposal.spec_dict['band_id']}")
            else:
                _warn(f"New entry (SVO lookup needed at seed time): {proposal.spec_dict['band_id']}")
                _info("SVO candidates to try:")
                for c in proposal.spec_dict.get("svo_candidates", []):
                    _info(f"  {c['filter_id']}")
                n_sparse += 1

            # Print the proposed JSON
            print(f"\n{json.dumps(proposal.spec_dict, indent=2)}\n")
            n_new += 1

    # Summary
    _section("Summary")
    if n_alias:
        print(f"  {n_alias} alias addition{'s' if n_alias != 1 else ''} to existing entries")
    if n_new:
        print(f"  {n_new} new entr{'ies' if n_new != 1 else 'y'}", end="")
        if n_sparse:
            print(f" ({n_sparse} need SVO lookup at seed time)")
        else:
            print()
    if n_exists:
        print(f"  {n_exists} already in registry (no action needed)")
    if not n_alias and not n_new:
        print("  Nothing to do.")
        return

    # Apply or instruct
    if args.apply and not args.dry_run:
        _section("Applying changes")
        ok = _apply_proposals(proposals, specs_path, registry_path, seed_output)
        if ok:
            print(f"\n  {_GREEN}{_BOLD}Done.{_RESET} Run try-parse again to verify.\n")
        else:
            print(f"\n  {_RED}Some steps failed — review output above.{_RESET}\n")
            sys.exit(1)
    elif args.dry_run:
        _info("\nDry run — no changes written.")
        _info(f"Run with --apply to write to {specs_path} and reseed.\n")
    else:
        _info(f"\nRun with --apply to write changes to {specs_path} and reseed.")
        _info("Or copy the JSON above into band_specs.json manually.\n")


if __name__ == "__main__":
    main()
