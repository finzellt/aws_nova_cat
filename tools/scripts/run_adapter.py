#!/usr/bin/env python3
"""
run_adapter.py — Manual test harness for CanonicalCsvAdapter.

Edit the CONFIG block below, then run:
    python run_adapter.py
"""

import csv
import json
import sys
from pathlib import Path
from uuid import UUID

# ---------------------------------------------------------------------------
# *** CONFIGURE HERE ***
# ---------------------------------------------------------------------------

# Error substrings to suppress from failure output (for exploratory runs).
# Matched case-insensitively against the full error string.
SUPPRESS_ERRORS: list[str] = [
    "spectral_coord_value",
    "excluded filter type",
]

CSV_PATH = Path(
    "/Users/tfinzell/Downloads/observations_20260317_212608/observations_20260317_212608_test.csv"
)

NOVA_ID = UUID("00000000-0000-0000-0000-000000000001")
NOVA_NAME = "V7994 Sgr"
RA_DEG = 270.9698333
DEC_DEG = -31.4573889

SHOW_VALID = False  # Print a sample of valid rows
SHOW_VALID_N = 5  # How many valid rows to print
SHOW_RAW = False  # Include raw source row in each failure printout

# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from photometry_ingestor.adapters.base import AdaptationFailure  # noqa: E402
from photometry_ingestor.adapters.canonical_csv import (  # noqa: E402
    CanonicalCsvAdapter,
    MissingRequiredColumnsError,
)

from contracts.models.entities import PhotometryRow  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    return rows


def _print_section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _print_failure(index: int, failure: AdaptationFailure, show_raw: bool) -> None:
    print(f"\n  [{index}] row_index={failure.row_index}")
    print(f"      error : {failure.error}")
    if show_raw:
        raw_str = json.dumps(failure.raw_row, default=str, indent=6)
        print(f"      raw   : {raw_str}")


def _print_valid_row(index: int, row: PhotometryRow) -> None:
    data = {k: v for k, v in row.model_dump().items() if v is not None}
    print(f"\n  [{index}]")
    for k, v in data.items():
        print(f"      {k:<28} {v}")


def _categorise_failures(failures: list[AdaptationFailure]) -> dict[str, list[AdaptationFailure]]:
    categories: dict[str, list] = {
        "excluded_filter": [],
        "unrecognized_filter": [],
        "ambiguous_filter": [],
        "missing_filter_name": [],
        "missing_time": [],
        "time_conversion": [],
        "pydantic_validation": [],
    }
    for f in failures:
        e = f.error.lower()
        if e.startswith("excluded filter"):
            categories["excluded_filter"].append(f)
        elif e.startswith("unrecognized filter"):
            categories["unrecognized_filter"].append(f)
        elif e.startswith("ambiguous filter"):
            categories["ambiguous_filter"].append(f)
        elif e.startswith("missing filter_name"):
            categories["missing_filter_name"].append(f)
        elif e.startswith("missing time"):
            categories["missing_time"].append(f)
        elif e.startswith("time conversion"):
            categories["time_conversion"].append(f)
        else:
            categories["pydantic_validation"].append(f)
    return categories


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    if not CSV_PATH.exists():
        print(f"ERROR: file not found: {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"\nLoading: {CSV_PATH}")
    rows = _load_csv(CSV_PATH)
    if rows:
        print(f"Headers  : {list(rows[0].keys())}")
    print(f"Row count: {len(rows)}")

    adapter = CanonicalCsvAdapter()

    try:
        result = adapter.adapt(
            raw_rows=iter(rows),
            nova_id=NOVA_ID,
            primary_name=NOVA_NAME,
            ra_deg=RA_DEG,
            dec_deg=DEC_DEG,
        )
    except MissingRequiredColumnsError as exc:
        _print_section("WARNING — MISSING REQUIRED COLUMNS (continuing anyway)")
        print(f"\n  Missing fields: {exc.missing_fields}")
        print("\n  In production this would quarantine the file.")
        print("  Re-running with an empty adapter to bypass the check...\n")
        # Re-run without the required-column guard by passing an empty
        # synonym dict so _check_required_columns sees no required fields.
        _adapter = CanonicalCsvAdapter.__new__(CanonicalCsvAdapter)
        _adapter._canonical_fields = adapter._canonical_fields
        _adapter._synonyms = adapter._synonyms
        _adapter._excluded_filters = adapter._excluded_filters
        # Monkey-patch the check to a no-op for this run
        _adapter._check_required_columns = lambda resolved: None  # type: ignore[method-assign]
        result = _adapter.adapt(
            raw_rows=iter(rows),
            nova_id=NOVA_ID,
            primary_name=NOVA_NAME,
            ra_deg=RA_DEG,
            dec_deg=DEC_DEG,
        )

    _print_section("SUMMARY")
    print(f"\n  Total rows      : {result.total_row_count}")
    print(f"  Valid rows      : {len(result.valid_rows)}")
    print(f"  Failed rows     : {len(result.failures)}")
    print(f"  Failure rate    : {result.failure_rate:.1%}")

    if result.total_row_count > 0:
        if result.failure_rate > 0.50:
            verdict = "LIKELY QUARANTINE  (>50% failure rate)"
        elif result.failure_rate > 0.20:
            verdict = "BORDERLINE  (>20% failure rate — check threshold config)"
        else:
            verdict = "LIKELY PROCEEDING  (<= 20% failure rate)"
        print(f"\n  Verdict         : {verdict}")

    # Apply suppression filter
    suppressed_count = 0
    visible_failures = result.failures
    if SUPPRESS_ERRORS:
        visible_failures = []
        for f in result.failures:
            if any(s.lower() in f.error.lower() for s in SUPPRESS_ERRORS):
                suppressed_count += 1
            else:
                visible_failures.append(f)
        if suppressed_count:
            print(f"\n  Suppressed      : {suppressed_count} failures matching {SUPPRESS_ERRORS}")

    if visible_failures:
        _print_section("FAILURE BREAKDOWN")
        categories = _categorise_failures(visible_failures)
        for category, items in categories.items():
            if items:
                label = category.replace("_", " ").title()
                print(f"\n  {label}: {len(items)}")
                seen: set[str] = set()
                for f in items:
                    key = f.error[:120]
                    if key not in seen:
                        print(f"    • {f.error[:120]}")
                        seen.add(key)
                    if len(seen) >= 3:
                        remaining = len(items) - 3
                        if remaining > 0:
                            print(f"    … and {remaining} more with similar errors")
                        break

    if visible_failures:
        _print_section(
            f"ALL FAILURES  ({len(visible_failures)} shown, {suppressed_count} suppressed)"
        )
        for i, failure in enumerate(visible_failures):
            _print_failure(i, failure, show_raw=SHOW_RAW)

    if SHOW_VALID and result.valid_rows:
        n = min(SHOW_VALID_N, len(result.valid_rows))
        _print_section(f"VALID ROWS — first {n} of {len(result.valid_rows)}")
        for i, row in enumerate(result.valid_rows[:n]):
            _print_valid_row(i, row)

    print()


if __name__ == "__main__":
    main()
