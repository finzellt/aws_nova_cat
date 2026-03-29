#!/usr/bin/env python3
"""
Patch infra/workflows/ingest_ticket.asl.json to fix two broken state
Parameter blocks that prevent the terminal-failure path from functioning.

Bug 1 — TerminalFailHandler Parameters missing "job_run.$"
  _terminal_fail_handler does event["job_run"] unconditionally.  The current
  Parameters block omits this field, causing a KeyError whenever any task
  fails and routes to TerminalFailHandler.

Bug 2 — FinalizeJobRunFailed Parameters wrong shape
  The current block passes "terminal_fail.$" and individual correlation_id /
  job_run_id fields.  _finalize_job_run_failed does event["job_run"], so the
  block must pass "job_run.$": "$.job_run" and "error.$": "$.error" — the
  same pattern used by every other workflow's FinalizeJobRunFailed state.

Usage:
    python tools/contract-patching/patch_ingest_ticket_asl.py \\
        path/to/infra/workflows/ingest_ticket.asl.json
"""

from __future__ import annotations

import sys


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found in file.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _replace_once(content: str, old: str, new: str, label: str) -> str:
    if old not in content:
        print(f"REPLACE FAILED — anchor for {label!r} not found.")
        sys.exit(1)
    if content.count(old) > 1:
        print(f"REPLACE FAILED — anchor for {label!r} appears more than once.")
        sys.exit(1)
    return content.replace(old, new)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path-to-ingest_ticket.asl.json>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # =========================================================================
    # Pre-conditions — abort if file doesn't look like what we expect
    # =========================================================================
    _require(src, '"task_name": "TerminalFailHandler"', "TerminalFailHandler state present")
    _require(src, '"task_name": "FinalizeJobRunFailed"', "FinalizeJobRunFailed state present")

    print("Applying patches…")

    # =========================================================================
    # Patch 1 — TerminalFailHandler Parameters: add "job_run.$"
    #
    # Current (buggy):
    #   "Parameters": {
    #       "task_name": "TerminalFailHandler",
    #       "workflow_name": "ingest_ticket",
    #       "correlation_id.$": "$.job_run.correlation_id",
    #       "job_run_id.$": "$.job_run.job_run_id",
    #       "error.$": "$.error"
    #   },
    #
    # Fixed:
    #   "Parameters": {
    #       "task_name": "TerminalFailHandler",
    #       "workflow_name": "ingest_ticket",
    #       "error.$": "$.error",
    #       "correlation_id.$": "$.job_run.correlation_id",
    #       "job_run_id.$": "$.job_run.job_run_id",
    #       "job_run.$": "$.job_run"
    #   },
    # =========================================================================
    OLD_TERMINAL_FAIL_PARAMS = """\
            "Parameters": {
                "task_name": "TerminalFailHandler",
                "workflow_name": "ingest_ticket",
                "correlation_id.$": "$.job_run.correlation_id",
                "job_run_id.$": "$.job_run.job_run_id",
                "error.$": "$.error"
            },"""

    NEW_TERMINAL_FAIL_PARAMS = """\
            "Parameters": {
                "task_name": "TerminalFailHandler",
                "workflow_name": "ingest_ticket",
                "error.$": "$.error",
                "correlation_id.$": "$.job_run.correlation_id",
                "job_run_id.$": "$.job_run.job_run_id",
                "job_run.$": "$.job_run"
            },"""

    src = _replace_once(
        src,
        OLD_TERMINAL_FAIL_PARAMS,
        NEW_TERMINAL_FAIL_PARAMS,
        "TerminalFailHandler Parameters",
    )

    # =========================================================================
    # Patch 2 — FinalizeJobRunFailed Parameters: replace with correct shape
    #
    # Current (buggy):
    #   "Parameters": {
    #       "task_name": "FinalizeJobRunFailed",
    #       "workflow_name": "ingest_ticket",
    #       "correlation_id.$": "$.job_run.correlation_id",
    #       "job_run_id.$": "$.job_run.job_run_id",
    #       "terminal_fail.$": "$.terminal_fail"
    #   },
    #
    # Fixed (matches pattern of all other workflows):
    #   "Parameters": {
    #       "task_name": "FinalizeJobRunFailed",
    #       "workflow_name": "ingest_ticket",
    #       "error.$": "$.error",
    #       "job_run.$": "$.job_run"
    #   },
    # =========================================================================
    OLD_FINALIZE_FAILED_PARAMS = """\
            "Parameters": {
                "task_name": "FinalizeJobRunFailed",
                "workflow_name": "ingest_ticket",
                "correlation_id.$": "$.job_run.correlation_id",
                "job_run_id.$": "$.job_run.job_run_id",
                "terminal_fail.$": "$.terminal_fail"
            },"""

    NEW_FINALIZE_FAILED_PARAMS = """\
            "Parameters": {
                "task_name": "FinalizeJobRunFailed",
                "workflow_name": "ingest_ticket",
                "error.$": "$.error",
                "job_run.$": "$.job_run"
            },"""

    src = _replace_once(
        src,
        OLD_FINALIZE_FAILED_PARAMS,
        NEW_FINALIZE_FAILED_PARAMS,
        "FinalizeJobRunFailed Parameters",
    )

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        ('"job_run.$": "$.job_run"', "job_run.$ present in TerminalFailHandler"),
        ('"error.$": "$.error"', "error.$ present in FinalizeJobRunFailed"),
    ]
    failed = False
    for marker, label in checks:
        if src.count(marker) < 1:
            print(f"POSTCONDITION FAILED — {label!r}")
            failed = True
    if failed:
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")
    print()
    print("Next steps:")
    print("  1. cdk deploy NovaCatSmoke   (redeploy to push the corrected ASL)")
    print("  2. Re-run smoke tests: pytest tests/smoke/test_ingest_ticket.py -v")


if __name__ == "__main__":
    main()
