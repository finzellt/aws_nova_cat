#!/usr/bin/env python3
"""
Patch: DOC-5 — ADR-021 §6.3 stale SFN polling language.

Steps 4–5 describe the old start_execution() + describe_execution() polling
pattern for initialize_nova. The actual implementation uses StartSyncExecution
(Express Workflow). This patch rewrites steps 4–5 accordingly.

ADR-021 is Draft status, so this is low-friction.

Usage:
    python patch_adr021_doc5.py docs/adr/ADR-021-layer-0-pre-ingestion-normalization.md
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
        print(f"Usage: {sys.argv[0]} <path/to/ADR-021-layer-0-pre-ingestion-normalization.md>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # ── Precondition checks ───────────────────────────────────────────────
    _require(
        src,
        "fire one\n   `initialize_nova` execution (Express SFn) via `sfn.start_execution()`",
        "DOC-5: step 4 stale start_execution language",
    )
    _require(
        src,
        "5. **Poll** `describe_execution()` until all `initialize_nova` executions complete.\n"
        "   Executions are fired in parallel; polling continues until all reach a terminal state.",
        "DOC-5: step 5 stale polling language",
    )

    print("All preconditions satisfied. Applying patches…")

    # ── Step 4: start_execution → start_sync_execution ────────────────────
    src = _replace_once(
        src,
        "4. **Unknown names:** for each name with no DynamoDB match, fire one\n"
        "   `initialize_nova` execution (Express SFn) via `sfn.start_execution()`",
        "4. **Unknown names:** for each name with no DynamoDB match, invoke\n"
        "   `initialize_nova` (Express SFn) synchronously via `sfn.start_sync_execution()`.\n"
        "   Each call blocks until the execution completes and returns the result inline",
        "DOC-5: step 4 — start_execution → start_sync_execution",
    )
    print("  ✓ Step 4: start_execution → start_sync_execution")

    # ── Step 5: polling → collect results ─────────────────────────────────
    src = _replace_once(
        src,
        "5. **Poll** `describe_execution()` until all `initialize_nova` executions complete.\n"
        "   Executions are fired in parallel; polling continues until all reach a terminal state.",
        "5. **Collect results** from each `start_sync_execution` response.\n"
        "   Invocations are issued in sequence; each returns synchronously upon completion.",
        "DOC-5: step 5 — polling → collect results",
    )
    print("  ✓ Step 5: polling → collect results")

    # ── Post-condition checks ─────────────────────────────────────────────
    assert "sfn.start_execution()" not in src, "Step 4 post-condition failed"
    assert "**Poll** `describe_execution()`" not in src, "Step 5 post-condition failed"
    # Verify surrounding context is intact
    assert "**Batch DynamoDB check:**" in src, "Step 3 context lost"
    assert "**`CREATED_AND_LAUNCHED`**" in src, "Step 6 context lost"

    with open(path, "w") as fh:
        fh.write(src)

    print(f"\nDone. Wrote {path}")


if __name__ == "__main__":
    main()
