#!/usr/bin/env python3
"""Validate the committed nova priors bundle (ADR-036 Decision 4).

Runs in CI as a belt-and-suspenders check against hand-edits to the
bundled JSON and against CSV-JSON drift.  Catches:

  1. Schema version violations (``_schema_version`` major mismatch or
     unparseable string)
  2. Structural violations (``entries`` is not a mapping; entry value
     is not an object; entry key does not equal normalize(primary_name))
  3. Pydantic contract violations — every entry is re-parsed through
     ``NovaPriorsEntry``, so any hand-edit that breaks a co-field
     invariant (peak_mag / peak_mag_band / peak_mag_uncertain, stricter
     discovery_date, etc.) fails here
  4. Alias collisions between entries
  5. CSV-JSON drift — the ``_source_sha256`` in the JSON must match the
     current SHA-256 of the canonical CSV.  Hard fail: this is the only
     place in the pipeline that can detect "someone edited the CSV
     without regenerating the JSON" or vice versa.

The validator does NOT follow ``_source_csv`` in the JSON to locate the
CSV — it uses the canonical path.  If a hand-edit changed
``_source_csv``, following it would let the edit evade the drift check.

Usage
-----
    python tools/catalog-expansion/validate_nova_priors.py
    python tools/catalog-expansion/validate_nova_priors.py --json PATH --csv PATH

Exits 0 on success, 1 on any validation failure.  All errors are
reported in one pass before the non-zero exit so the operator sees the
full scope of the problem in a single CI run.

Personal operator tooling — subject to ruff but not mypy strict.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# sys.path setup — must precede contracts import.
# ---------------------------------------------------------------------------

# tools/catalog-expansion/validate_nova_priors.py → repo root is three up.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pydantic import ValidationError  # noqa: E402

from contracts.models.priors import NovaPriorsEntry  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SUPPORTED_MAJOR_VERSION = 1

_DEFAULT_JSON_PATH = _REPO_ROOT / "services" / "nova_resolver" / "nova_priors" / "nova_priors.json"
_DEFAULT_CSV_PATH = (
    _REPO_ROOT / "tools" / "catalog-expansion" / "nova_candidates_final_full_year.csv"
)

# ANSI colors — match build_nova_priors.py.
_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
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
# Normalization — must match reader._normalize_name and build script
# ---------------------------------------------------------------------------


def _normalize_name(name: str) -> str:
    """Canonical nova-name normalization (ADR-036 Decision 2)."""
    return re.sub(r"\s+", " ", name.replace("_", " ").strip().lower())


# ---------------------------------------------------------------------------
# Validation core
# ---------------------------------------------------------------------------


def _validate_bundle(json_path: Path, csv_path: Path) -> list[str]:
    """Validate the committed bundle against its source CSV.

    Returns a list of error messages.  Empty list means success.
    """
    errors: list[str] = []

    # --- Pre-flight: both files exist ---
    if not json_path.exists():
        errors.append(f"JSON not found: {json_path}")
    if not csv_path.exists():
        errors.append(f"CSV not found: {csv_path}")
    if errors:
        return errors

    # --- Parse JSON ---
    try:
        raw: dict[str, Any] = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"JSON parse error in {json_path}: {exc}"]

    # --- Schema version guard ---
    schema_version = raw.get("_schema_version", "0.0.0")
    major: int | None = None
    try:
        major = int(schema_version.split(".")[0])
    except (ValueError, IndexError):
        errors.append(f"cannot parse _schema_version {schema_version!r}")

    if major is not None and major != _SUPPORTED_MAJOR_VERSION:
        errors.append(
            f"_schema_version major {major} is not supported by this "
            f"validator (expected {_SUPPORTED_MAJOR_VERSION}).  Update "
            "validate_nova_priors.py to handle the new schema."
        )

    # --- Top-level shape ---
    entries_raw = raw.get("entries")
    if not isinstance(entries_raw, dict):
        errors.append("'entries' must be a mapping of normalized_name → entry")
        return errors  # can't continue without entries

    # --- Per-entry validation + alias index construction ---
    alias_index: dict[str, str] = {}

    for key, entry_data in entries_raw.items():
        if not isinstance(entry_data, dict):
            errors.append(f"entry {key!r} must be an object, got {type(entry_data).__name__}")
            continue

        # Pydantic re-validation — catches hand-edits to field values.
        try:
            entry = NovaPriorsEntry(**entry_data)
        except ValidationError as exc:
            for pyd_err in exc.errors():
                loc = ".".join(str(p) for p in pyd_err["loc"]) or "(model)"
                errors.append(f"entry {key!r} / {loc}: {pyd_err['msg']}")
            continue

        # Key-to-primary-name agreement — catches hand-edits to keys.
        expected_key = _normalize_name(entry.primary_name)
        if key != expected_key:
            errors.append(
                f"entry {key!r}: key does not match normalize(primary_name)={expected_key!r}"
            )
            continue

        # Alias index: self-alias + curated aliases (ADR-036 Decision 5).
        for alias in (entry.primary_name, *entry.aliases):
            normalized = _normalize_name(alias)
            if not normalized:
                continue
            existing = alias_index.get(normalized)
            if existing is None:
                alias_index[normalized] = key
            elif existing != key:
                errors.append(
                    f"alias collision: {normalized!r} maps to both {existing!r} and {key!r}"
                )

    # --- CSV-JSON drift check (the check unique to this script) ---
    csv_sha = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    json_sha = raw.get("_source_sha256", "")
    if csv_sha != json_sha:
        errors.append(
            "CSV-JSON drift detected — the committed JSON was not "
            "generated from the current CSV.\n"
            f"           CSV SHA-256:  {csv_sha}\n"
            f"           JSON claims:  {json_sha}\n"
            "    Either the CSV was modified after the JSON was last built, "
            "or the committed JSON is stale.  Run\n"
            "      python tools/catalog-expansion/build_nova_priors.py\n"
            "    to regenerate, and commit the regenerated JSON alongside "
            "the CSV change."
        )

    return errors


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the committed nova priors JSON against its source "
            "CSV (ADR-036 Decision 4).  CI-runnable."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--json",
        type=Path,
        default=_DEFAULT_JSON_PATH,
        help=(f"Bundled JSON path (default: {_DEFAULT_JSON_PATH.relative_to(_REPO_ROOT)})"),
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=_DEFAULT_CSV_PATH,
        help=(
            "Source CSV path for the SHA-256 drift check (default: "
            f"{_DEFAULT_CSV_PATH.relative_to(_REPO_ROOT)})"
        ),
    )
    args = parser.parse_args(argv)

    _section("Validating nova priors bundle")
    try:
        json_display = args.json.resolve().relative_to(_REPO_ROOT)
    except ValueError:
        json_display = args.json
    try:
        csv_display = args.csv.resolve().relative_to(_REPO_ROOT)
    except ValueError:
        csv_display = args.csv
    _info(f"JSON: {json_display}")
    _info(f"CSV:  {csv_display}")

    errors = _validate_bundle(args.json, args.csv)

    if not errors:
        _section("Result")
        _ok("Bundle is valid and in sync with the CSV.")
        return 0

    _section(f"Validation failures ({len(errors)})")
    for err in errors:
        _fail(err)
    print()
    _fail(
        f"Aborting — {len(errors)} error(s).  No action was taken; fix the "
        "CSV and/or JSON and rerun."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
