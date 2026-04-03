#!/usr/bin/env python3
"""
Patch ASL Retry blocks — add Lambda.TooManyRequestsException + Lambda.ServiceException.

Five ASL files have Retry blocks that only catch "RetryableError" (the application-
level exception). When Lambda itself throttles the invocation (concurrent nova
ingestion, burst traffic), Step Functions raises Lambda.TooManyRequestsException —
which falls through to the Catch → TerminalFailHandler path and permanently fails
the job run. Lambda.ServiceException (transient Lambda 500s) has the same problem.

The newer workflows (acquire_and_validate_spectra, regenerate_artifacts) already
include these error codes. This patch brings the five older workflows up to parity.

Target files:
  infra/workflows/initialize_nova.asl.json
  infra/workflows/ingest_new_nova.asl.json
  infra/workflows/refresh_references.asl.json
  infra/workflows/discover_spectra_products.asl.json
  infra/workflows/ingest_ticket.asl.json

The patch is JSON-aware: it parses each ASL file, walks all states (including
nested Map/Parallel iterators), finds Retry blocks with only ["RetryableError"],
and appends the two missing error codes. Text-based search/replace is not used
because JSON key ordering and whitespace vary across files.

Usage:
    python patch_asl_lambda_retry.py infra/workflows/

    Accepts a directory containing the ASL files. Patches are applied in-place.
    Re-running is safe — files that already have the error codes are skipped.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# ASL files to patch (must exist in the target directory).
_TARGET_FILES = [
    "initialize_nova.asl.json",
    "ingest_new_nova.asl.json",
    "refresh_references.asl.json",
    "discover_spectra_products.asl.json",
    "ingest_ticket.asl.json",
]

# Error codes to add alongside RetryableError.
_ADDITIONAL_ERRORS = [
    "Lambda.TooManyRequestsException",
    "Lambda.ServiceException",
]


def _patch_retry_blocks(states: dict[str, object], file_label: str) -> int:
    """Walk all states (recursively into Map/Parallel) and patch Retry blocks.

    Returns the number of Retry entries patched.
    """
    patched = 0

    for state_name, state in states.items():
        if not isinstance(state, dict):
            continue

        # Patch Retry blocks on this state
        retry_list = state.get("Retry")
        if isinstance(retry_list, list):
            for retry_entry in retry_list:
                if not isinstance(retry_entry, dict):
                    continue
                error_equals = retry_entry.get("ErrorEquals")
                if not isinstance(error_equals, list):
                    continue

                # Only patch blocks that have RetryableError but are missing
                # the Lambda error codes.
                has_retryable = "RetryableError" in error_equals
                has_throttle = "Lambda.TooManyRequestsException" in error_equals

                if has_retryable and not has_throttle:
                    for err_code in _ADDITIONAL_ERRORS:
                        if err_code not in error_equals:
                            error_equals.append(err_code)
                    patched += 1
                    print(f"    Patched: {file_label} → {state_name}")

        # Recurse into Map Iterator
        iterator = state.get("Iterator")
        if isinstance(iterator, dict):
            nested_states = iterator.get("States")
            if isinstance(nested_states, dict):
                patched += _patch_retry_blocks(nested_states, f"{file_label}/{state_name}/Iterator")

        # Recurse into Parallel Branches
        branches = state.get("Branches")
        if isinstance(branches, list):
            for i, branch in enumerate(branches):
                if isinstance(branch, dict):
                    nested_states = branch.get("States")
                    if isinstance(nested_states, dict):
                        patched += _patch_retry_blocks(
                            nested_states, f"{file_label}/{state_name}/Branch[{i}]"
                        )

    return patched


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/infra/workflows/>")
        sys.exit(1)

    workflows_dir = Path(sys.argv[1])
    if not workflows_dir.is_dir():
        print(f"ERROR: {workflows_dir} is not a directory.")
        sys.exit(1)

    # ── Precondition: all target files must exist ────────────────────────
    missing = [f for f in _TARGET_FILES if not (workflows_dir / f).exists()]
    if missing:
        print("PRECONDITION FAILED — missing ASL files:")
        for f in missing:
            print(f"  {workflows_dir / f}")
        sys.exit(1)

    print("All target files found. Scanning Retry blocks…\n")

    total_patched = 0
    files_modified = 0

    for filename in _TARGET_FILES:
        path = workflows_dir / filename
        with open(path) as fh:
            asl = json.load(fh)

        states = asl.get("States")
        if not isinstance(states, dict):
            print(f"  SKIP: {filename} — no States dict found")
            continue

        count = _patch_retry_blocks(states, filename)

        if count > 0:
            # Write back with consistent formatting (2-space indent, trailing newline)
            with open(path, "w") as fh:
                json.dump(asl, fh, indent=4)
                fh.write("\n")
            total_patched += count
            files_modified += 1
            print(f"  ✓ {filename}: {count} Retry block(s) patched\n")
        else:
            print(f"  ○ {filename}: already up to date\n")

    # ── Post-condition: verify all files now have the error codes ─────────
    print("─" * 60)
    print("Post-condition checks…\n")

    failed = False
    for filename in _TARGET_FILES:
        path = workflows_dir / filename
        content = path.read_text()

        for err_code in _ADDITIONAL_ERRORS:
            if err_code not in content:
                print(f"  POSTCONDITION FAILED: {filename} missing {err_code}")
                failed = True

        # Also verify the two already-correct files weren't broken
        if "RetryableError" not in content:
            print(f"  POSTCONDITION FAILED: {filename} lost RetryableError")
            failed = True

    # Also verify the already-correct files are still correct
    for already_ok in ["acquire_and_validate_spectra.asl.json", "regenerate_artifacts.asl.json"]:
        ok_path = workflows_dir / already_ok
        if ok_path.exists():
            ok_content = ok_path.read_text()
            for err_code in _ADDITIONAL_ERRORS:
                if err_code not in ok_content:
                    print(
                        f"  WARNING: {already_ok} is missing {err_code} (not patched by this script)"
                    )

    if failed:
        print("\nPatch FAILED — see errors above.")
        sys.exit(1)

    print("\n✓ All post-conditions satisfied.")
    print(f"  Files modified: {files_modified}")
    print(f"  Retry blocks patched: {total_patched}")
    print()
    print("Next steps:")
    print("  1. Review the diffs: git diff infra/workflows/")
    print("  2. Run synth tests: pytest tests/infra/test_synth.py -v")
    print("  3. Deploy: ./deploy.sh")
    print("  4. Re-run smoke tests: pytest tests/smoke/ -v")


if __name__ == "__main__":
    main()
