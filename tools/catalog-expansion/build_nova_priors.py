#!/usr/bin/env python3
"""Build script for the NovaCat nova priors artifact (ADR-036).

Transforms the operator-curated CSV at
``tools/catalog-expansion/nova_candidates_final_full_year.csv`` into the
bundled JSON artifact at
``services/nova_resolver/nova_priors/nova_priors.json``.

Per ADR-036 Decision 6, the script:

  1. Reads the CSV with ``utf-8-sig`` encoding (strips BOM).
  2. Normalizes ``Discovery_Date`` from M/D/YYYY to YYYY-MM-DD.  Requires a
     four-digit year AND a specific day (day=0 / day-00 is rejected because
     priors exist to ship precise dates — see ADR-036 Note on §3 and the
     _PRIORS_DISCOVERY_DATE_RE in contracts/models/priors.py).
  3. Filters blank rows (no Nova_Name).
  4. Dedupes by normalized primary name, keeping first occurrence and
     warning about duplicates.
  5. Validates TRUE/FALSE cells for is_nova, is_recurrent, Uncertainty.
  6. Validates each row through NovaPriorsEntry (Pydantic).  On any
     failure, aborts with a named diagnostic per row.
  7. Builds the alias index in memory and fails on cross-entry collisions.
  8. Emits JSON with ``_schema_version``, ``_generated_at``,
     ``_source_csv``, ``_source_sha256``, ``_note``, and ``entries``.

Usage::

    python tools/catalog-expansion/build_nova_priors.py
    python tools/catalog-expansion/build_nova_priors.py --dry-run
    python tools/catalog-expansion/build_nova_priors.py --csv path/to/other.csv
    python tools/catalog-expansion/build_nova_priors.py --output /tmp/priors.json
    python tools/catalog-expansion/build_nova_priors.py --verbose

Personal operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# sys.path setup — must precede contracts import.
# ---------------------------------------------------------------------------

# tools/catalog-expansion/build_nova_priors.py → repo root is three up.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pydantic import ValidationError  # noqa: E402

from contracts.models.priors import NovaPriorsEntry  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = "1.0.0"

_DEFAULT_CSV_PATH = (
    _REPO_ROOT / "tools" / "catalog-expansion" / "nova_candidates_final_full_year.csv"
)
_DEFAULT_OUTPUT_PATH = (
    _REPO_ROOT / "services" / "nova_resolver" / "nova_priors" / "nova_priors.json"
)

_EXPECTED_COLUMNS = {
    "Nova_Name",
    "SIMBAD_Name",
    "Input_Name",  # read and dropped per ADR-036 Decision 3
    "Nova_Aliases",
    "Discovery_Date",
    "Nova_Otypes",
    "is_nova",
    "is_recurrent",
    "Peak_Mag",
    "Filter",
    "Uncertainty",
}

# ANSI colors — same palette as other operator tooling.
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
# Primitive transforms
# ---------------------------------------------------------------------------


def _normalize_name(name: str) -> str:
    """Canonical nova-name normalization per ADR-036 Decision 2.

    Mirrors ``nova_resolver._normalize_candidate_name`` exactly:
      strip → replace underscores with spaces → lowercase → collapse whitespace.
    """
    return re.sub(r"\s+", " ", name.replace("_", " ").strip().lower())


def _normalize_date(raw: str) -> str | None:
    """Convert ``M/D/YYYY`` to ``YYYY-MM-DD``.

    Returns ``None`` for empty input.  Raises ``ValueError`` with an operator-
    readable message for every other failure mode.  Day=0 is rejected
    explicitly so the build-time message can reference the raw CSV value;
    the Pydantic validator would otherwise catch the normalized form later.
    """
    stripped = raw.strip()
    if not stripped:
        return None

    parts = stripped.split("/")
    if len(parts) != 3:
        raise ValueError(f"Discovery_Date={raw!r}: expected M/D/YYYY, got {len(parts)} part(s).")

    m_raw, d_raw, y_raw = parts
    if len(y_raw.strip()) != 4:
        raise ValueError(f"Discovery_Date={raw!r}: year must be 4 digits, got {y_raw!r}.")

    try:
        month = int(m_raw)
        day = int(d_raw)
        year = int(y_raw)
    except ValueError:
        raise ValueError(f"Discovery_Date={raw!r}: non-integer component.") from None

    if not (1 <= month <= 12):
        raise ValueError(f"Discovery_Date={raw!r}: month {month} out of range 1-12.")
    if day == 0:
        raise ValueError(
            f"Discovery_Date={raw!r}: day=0 (month-only precision) is not "
            "accepted in priors.  Either resolve the day and update the CSV, "
            "or clear the Discovery_Date cell so this entry serializes with "
            "discovery_date=None (refresh_references will fill it in later)."
        )
    if not (1 <= day <= 31):
        raise ValueError(f"Discovery_Date={raw!r}: day {day} out of range 1-31.")

    return f"{year:04d}-{month:02d}-{day:02d}"


def _parse_bool(raw: str, *, field: str) -> bool:
    """Strict TRUE/FALSE per ADR-036 Decision 6 step 5 (whitespace tolerant)."""
    stripped = raw.strip()
    if stripped == "TRUE":
        return True
    if stripped == "FALSE":
        return False
    raise ValueError(f"{field}={raw!r}: must be exactly 'TRUE' or 'FALSE' (case-sensitive).")


def _parse_float_or_none(raw: str, *, field: str) -> float | None:
    stripped = raw.strip()
    if not stripped:
        return None
    try:
        return float(stripped)
    except ValueError:
        raise ValueError(f"{field}={raw!r}: must be a decimal number or empty.") from None


def _parse_str_or_none(raw: str) -> str | None:
    stripped = raw.strip()
    return stripped or None


def _split_pipe(raw: str) -> list[str]:
    """Pipe-split, strip each token, drop empties, dedupe preserving order."""
    out: list[str] = []
    seen: set[str] = set()
    for tok in raw.split("|"):
        stripped = tok.strip()
        if not stripped or stripped in seen:
            continue
        seen.add(stripped)
        out.append(stripped)
    return out


# ---------------------------------------------------------------------------
# Row transform
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _RowError:
    row_index: int  # 1-based row number including header (row 1 = header)
    nova_name: str
    message: str


def _transform_row(
    row: dict[str, str], row_index: int
) -> tuple[NovaPriorsEntry | None, list[_RowError]]:
    """Transform one CSV row into a validated NovaPriorsEntry.

    Returns ``(entry, errors)``.  If ``errors`` is non-empty, ``entry`` is
    ``None``.  An entirely-blank row (no Nova_Name) returns ``(None, [])`` so
    the caller can count it as skipped rather than erroring.
    """
    nova_name = (row.get("Nova_Name") or "").strip()
    if not nova_name:
        return None, []

    errors: list[_RowError] = []

    def _record(exc: ValueError) -> None:
        errors.append(_RowError(row_index, nova_name, str(exc)))

    # --- Primitive transforms ---
    discovery_date: str | None = None
    try:
        discovery_date = _normalize_date(row.get("Discovery_Date") or "")
    except ValueError as exc:
        _record(exc)

    is_nova: bool | None = None
    try:
        is_nova = _parse_bool(row.get("is_nova") or "", field="is_nova")
    except ValueError as exc:
        _record(exc)

    is_recurrent: bool | None = None
    try:
        is_recurrent = _parse_bool(row.get("is_recurrent") or "", field="is_recurrent")
    except ValueError as exc:
        _record(exc)

    peak_mag: float | None = None
    peak_mag_ok = True
    try:
        peak_mag = _parse_float_or_none(row.get("Peak_Mag") or "", field="Peak_Mag")
    except ValueError as exc:
        _record(exc)
        peak_mag_ok = False

    peak_mag_uncertain: bool | None = None
    try:
        peak_mag_uncertain = _parse_bool(row.get("Uncertainty") or "", field="Uncertainty")
    except ValueError as exc:
        _record(exc)

    if errors:
        return None, errors

    # --- Pydantic validation ---
    # All four primitive-transformed fields are definitely non-None here
    # because any None assignment would have recorded an error above.
    assert is_nova is not None
    assert is_recurrent is not None
    assert peak_mag_ok  # satisfied by empty errors
    assert peak_mag_uncertain is not None

    try:
        entry = NovaPriorsEntry(
            primary_name=nova_name,
            simbad_main_id=_parse_str_or_none(row.get("SIMBAD_Name") or ""),
            aliases=_split_pipe(row.get("Nova_Aliases") or ""),
            discovery_date=discovery_date,
            otypes=_split_pipe(row.get("Nova_Otypes") or ""),
            is_nova=is_nova,
            is_recurrent=is_recurrent,
            peak_mag=peak_mag,
            peak_mag_band=_parse_str_or_none(row.get("Filter") or ""),
            peak_mag_uncertain=peak_mag_uncertain,
        )
    except ValidationError as exc:
        for pyd_err in exc.errors():
            loc = ".".join(str(part) for part in pyd_err["loc"]) or "(model)"
            errors.append(_RowError(row_index, nova_name, f"{loc}: {pyd_err['msg']}"))
        return None, errors

    return entry, []


# ---------------------------------------------------------------------------
# Build pipeline
# ---------------------------------------------------------------------------


def _read_csv(csv_path: Path) -> tuple[list[dict[str, str]], str]:
    """Read the CSV and compute its SHA-256.  Returns (rows, sha256_hex)."""
    raw_bytes = csv_path.read_bytes()
    sha256_hex = hashlib.sha256(raw_bytes).hexdigest()

    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = _EXPECTED_COLUMNS - fieldnames
        if missing:
            raise ValueError(
                f"CSV is missing required columns: {sorted(missing)}. Got: {sorted(fieldnames)}"
            )
        extra = fieldnames - _EXPECTED_COLUMNS
        if extra:
            _warn(f"CSV has unexpected columns (will be ignored): {sorted(extra)}")
        rows = list(reader)

    return rows, sha256_hex


def _build_alias_index(
    entries_by_key: dict[str, NovaPriorsEntry],
) -> tuple[dict[str, str], list[tuple[str, str, str]]]:
    """Build alias → normalized-primary-name map; return (index, collisions).

    Collisions are returned as a list of (alias, existing_target, new_target)
    tuples.  An empty collision list is the success signal.  Per ADR-036
    Decision 5, each entry's primary name self-aliases to its own key.
    """
    alias_to_key: dict[str, str] = {}
    collisions: list[tuple[str, str, str]] = []

    # Self-aliases first.  These cannot collide with each other because the
    # caller has already deduped by normalized primary name.
    for key in entries_by_key:
        alias_to_key[key] = key

    # Operator-curated aliases.
    for key, entry in entries_by_key.items():
        for alias in entry.aliases:
            normalized = _normalize_name(alias)
            if not normalized:
                continue  # defensive; _split_pipe already filtered blanks
            existing = alias_to_key.get(normalized)
            if existing is None:
                alias_to_key[normalized] = key
                continue
            if existing != key:
                collisions.append((normalized, existing, key))
            # else: same entry re-declaring its own primary name as an alias
            # (e.g. CK Vul listing "CK Vul" in Nova_Aliases) — no-op.

    return alias_to_key, collisions


def _build(csv_path: Path, output_path: Path, *, dry_run: bool, verbose: bool) -> int:
    # ── Step 1: Read CSV ──
    _section(
        f"Reading {csv_path.relative_to(_REPO_ROOT) if csv_path.is_relative_to(_REPO_ROOT) else csv_path}"
    )
    if not csv_path.exists():
        _fail(f"CSV not found: {csv_path}")
        return 1

    try:
        rows, sha256_hex = _read_csv(csv_path)
    except ValueError as exc:
        _fail(str(exc))
        return 1

    _info(f"SHA-256: {sha256_hex}")
    _info(f"{len(rows)} data row(s) including blanks")

    # ── Step 2: Transform and validate each row ──
    _section("Transforming and validating rows")
    all_errors: list[_RowError] = []
    entries_by_key: dict[str, NovaPriorsEntry] = {}
    duplicates: list[tuple[int, str, str]] = []  # (row_index, raw_name, normalized_key)
    blank_rows = 0

    for idx, row in enumerate(rows, start=2):  # row 1 is header → first data row is 2
        entry, errors = _transform_row(row, idx)
        if errors:
            all_errors.extend(errors)
            continue
        if entry is None:
            blank_rows += 1
            continue

        key = _normalize_name(entry.primary_name)
        existing = entries_by_key.get(key)
        if existing is not None:
            duplicates.append((idx, entry.primary_name, key))
            continue

        entries_by_key[key] = entry
        if verbose:
            _info(
                f"row {idx:>4}: {entry.primary_name} → {key}"
                f"{' [non-nova]' if not entry.is_nova else ''}"
                f"{' [recurrent]' if entry.is_recurrent else ''}"
            )

    if blank_rows:
        _info(f"{blank_rows} blank row(s) skipped")

    if duplicates:
        _section(f"Duplicate warnings ({len(duplicates)})")
        for idx, name, key in duplicates:
            _warn(f"row {idx}: {name!r} (normalized: {key!r}) — kept first occurrence")

    if all_errors:
        _section(f"Row validation errors ({len(all_errors)})")
        affected_rows: set[tuple[int, str]] = set()
        for err in all_errors:
            _fail(f"row {err.row_index} ({err.nova_name}): {err.message}")
            affected_rows.add((err.row_index, err.nova_name))
        print()
        _fail(
            f"Aborting — {len(all_errors)} error(s) across "
            f"{len(affected_rows)} row(s).  No JSON emitted.  Fix the CSV "
            "and rerun."
        )
        return 1

    # ── Step 3: Alias index + collision check ──
    _section("Building alias index")
    alias_to_key, collisions = _build_alias_index(entries_by_key)

    if collisions:
        _section(f"Alias collisions ({len(collisions)})")
        for alias, target_a, target_b in collisions:
            entry_a = entries_by_key[target_a]
            entry_b = entries_by_key[target_b]
            _fail(
                f"alias {alias!r} maps to both {entry_a.primary_name!r} "
                f"and {entry_b.primary_name!r}"
            )
        print()
        _fail(f"Aborting — {len(collisions)} alias collision(s).  Resolve in the CSV and rerun.")
        return 1

    _info(f"{len(alias_to_key)} alias index entries (incl. {len(entries_by_key)} self-aliases)")

    # ── Step 4: Build output ──
    _section("Serializing")
    # Sort entries by key for deterministic diffs.
    sorted_entries = {key: entries_by_key[key].model_dump() for key in sorted(entries_by_key)}

    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    try:
        source_csv_label = str(csv_path.resolve().relative_to(_REPO_ROOT))
    except ValueError:
        source_csv_label = str(csv_path.resolve())

    output_doc: dict[str, Any] = {
        "_schema_version": _SCHEMA_VERSION,
        "_generated_at": generated_at,
        "_source_csv": source_csv_label,
        "_source_sha256": sha256_hex,
        "_note": (
            "AUTO-GENERATED by tools/catalog-expansion/build_nova_priors.py — "
            "do not hand-edit.  Regenerate from "
            "nova_candidates_final_full_year.csv and commit both files "
            "together (ADR-036 Decision 6)."
        ),
        "entries": sorted_entries,
    }
    output_json = json.dumps(output_doc, indent=2, ensure_ascii=False)

    # ── Step 5: Summary ──
    _section("Summary")
    n_entries = len(entries_by_key)
    n_aliases = sum(len(e.aliases) for e in entries_by_key.values())
    n_with_date = sum(1 for e in entries_by_key.values() if e.discovery_date)
    n_with_peak = sum(1 for e in entries_by_key.values() if e.peak_mag is not None)
    n_recurrent = sum(1 for e in entries_by_key.values() if e.is_recurrent)
    n_non_nova = sum(1 for e in entries_by_key.values() if not e.is_nova)

    print(f"  {n_entries} entries")
    print(f"  {n_aliases} curated aliases across {n_entries} entries (pre-dedup)")
    print(f"  {len(alias_to_key)} unique entries in the alias index")
    print(f"  {n_with_date} with discovery_date")
    print(f"  {n_with_peak} with peak_mag")
    print(f"  {n_recurrent} recurrent")
    print(f"  {n_non_nova} non-nova (→ rejection path in item-3 consumer)")

    # ── Step 6: Write or dry-run ──
    if dry_run:
        _section("Dry run")
        _info(f"Would write {len(output_json)} bytes to {output_path}")
        _info("No file written.")
        return 0

    _section("Writing output")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_json + "\n", encoding="utf-8")
    _ok(f"Wrote {output_path} ({len(output_json)} bytes)")
    _info("Review the diff, then commit both the CSV and the JSON together.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build the NovaCat nova priors JSON artifact from the operator-"
            "curated CSV.  See ADR-036."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=_DEFAULT_CSV_PATH,
        help=f"Source CSV path (default: {_DEFAULT_CSV_PATH.relative_to(_REPO_ROOT)})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT_PATH,
        help=f"Output JSON path (default: {_DEFAULT_OUTPUT_PATH.relative_to(_REPO_ROOT)})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full pipeline but do not write the output file.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print one line per accepted entry.",
    )
    args = parser.parse_args(argv)

    return _build(args.csv, args.output, dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
