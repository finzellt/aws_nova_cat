#!/usr/bin/env python3
"""
Patch: CLEANUP-2 — Remove dead poll_execution function from tests/smoke/conftest.py.

poll_execution used describe_execution to poll Standard Workflow executions.
All workflows are now Express (StartSyncExecution), so no caller remains.
The function and its section comment are dead code.

Usage:
    python patch_smoke_conftest_cleanup2.py tests/smoke/conftest.py
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
    count = content.count(old)
    if count > 1:
        print(f"REPLACE FAILED — anchor for {label!r} appears {count} times (expected 1).")
        sys.exit(1)
    return content.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/conftest.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # ── Precondition checks ───────────────────────────────────────────────
    _require(src, "def poll_execution(", "poll_execution function definition")
    _require(
        src,
        "# Execution polling helper (used by test_workflows.py and test_e2e.py)",
        "poll_execution section comment",
    )
    _require(
        src,
        "# DynamoDB wipe helpers",
        "next section header (ensures we capture the full block)",
    )

    print("All preconditions satisfied. Applying patches…")

    # ── Remove the entire poll_execution block ────────────────────────────
    # From the section comment through the end of the function, up to (but
    # not including) the next section comment.
    src = _replace_once(
        src,
        "# ---------------------------------------------------------------------------\n"
        "# Execution polling helper (used by test_workflows.py and test_e2e.py)\n"
        "# ---------------------------------------------------------------------------\n"
        "\n"
        "\n"
        "def poll_execution(\n"
        "    sfn_client: Any,\n"
        "    execution_arn: str,\n"
        "    timeout_seconds: int = 60,\n"
        "    poll_interval: int = _POLL_INTERVAL_SECONDS,\n"
        ") -> dict[str, Any]:\n"
        '    """\n'
        "    Poll a Step Functions execution until it reaches a terminal status\n"
        "    (SUCCEEDED, FAILED, TIMED_OUT, ABORTED) or the timeout is exceeded.\n"
        "\n"
        "    Returns the describe_execution response dict on terminal status.\n"
        "    Raises TimeoutError if the execution has not completed within timeout_seconds.\n"
        '    """\n'
        "    deadline = time.monotonic() + timeout_seconds\n"
        "    while time.monotonic() < deadline:\n"
        "        resp = sfn_client.describe_execution(executionArn=execution_arn)\n"
        '        status = resp["status"]\n'
        '        if status != "RUNNING":\n'
        "            return cast(dict[str, Any], resp)\n"
        "        time.sleep(poll_interval)\n"
        "\n"
        "    raise TimeoutError(\n"
        '        f"Execution {execution_arn} did not complete within {timeout_seconds}s. "\n'
        '        "Increase timeout or check CloudWatch Logs for the state machine."\n'
        "    )\n"
        "\n"
        "\n",
        "\n",
        "remove poll_execution function and section comment",
    )
    print("  ✓ Removed poll_execution function and section comment")

    # ── Post-condition checks ─────────────────────────────────────────────
    assert "poll_execution" not in src, "poll_execution still present"
    assert "Execution polling helper" not in src, "section comment still present"
    assert "describe_execution" not in src, "describe_execution reference still present"
    # Verify surrounding context survived
    assert "def sns_client" in src, "sns_client fixture was lost"
    assert "DynamoDB wipe helpers" in src, "next section header was lost"
    assert "def dynamodb_resource" in src, "dynamodb_resource fixture was lost"

    with open(path, "w") as fh:
        fh.write(src)

    print(f"\nDone. Wrote {path}")


if __name__ == "__main__":
    main()
