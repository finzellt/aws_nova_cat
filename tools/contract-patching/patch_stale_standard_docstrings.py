#!/usr/bin/env python3
"""
Patch: Fix stale "Standard Workflow" references in docstrings and comments.

The CDK code creates all workflows as EXPRESS (state_machine_type="EXPRESS"),
but several docstrings and comments incorrectly say "Standard". This patch
corrects them to match the actual runtime behavior.

Targets:
  1. infra/nova_constructs/workflows.py — module docstring + method docstring
  2. infra/workflows/discover_spectra_products.asl.json — ASL comment

Note: Run this BEFORE the discover_spectra Standard switch prompt (bug 3),
which will further update these files. This patch fixes what is currently
wrong; the bug 3 prompt builds on top.

Usage:
    python patch_stale_standard_docstrings.py <repo_root>

Example:
    python tools/contract-patching/patch_stale_standard_docstrings.py .
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _require_absent(content: str, marker: str, label: str) -> None:
    if marker in content:
        print(f"PRECONDITION FAILED — {label!r} already present (patch may have been applied).")
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


# ───────────────────────────────────────────────────────────────────────────
# Patch 1: infra/nova_constructs/workflows.py
# ───────────────────────────────────────────────────────────────────────────


def _patch_workflows_py(repo_root: Path) -> None:
    path = repo_root / "infra" / "nova_constructs" / "workflows.py"
    print(f"\n── Patching {path.relative_to(repo_root)} ──")

    src = path.read_text()

    # ── Preconditions ─────────────────────────────────────────────────────
    _require(
        src,
        "Standard Workflows (not Express): Nova Cat operates at low throughput",
        "module docstring — stale Standard claim",
    )
    _require(
        src,
        "Create a Standard Workflow state machine from an ASL file.",
        "_create_state_machine docstring — stale Standard claim",
    )
    _require(
        src,
        "# CloudWatch log group for Express Workflow execution logging",
        "log group comment (already correct — confirms EXPRESS is in code)",
    )
    _require(
        src,
        'state_machine_type="EXPRESS"',
        "actual CDK code creates EXPRESS (confirms mismatch)",
    )

    print("  Preconditions satisfied.")

    # ── Patch 1a — Module docstring ───────────────────────────────────────
    src = _replace_once(
        src,
        (
            "  - Standard Workflows (not Express): Nova Cat operates at low throughput\n"
            "    with operator-triggered executions. Standard Workflows provide exact-once\n"
            "    semantics, unlimited duration, and full execution history — appropriate\n"
            "    for a scientific data pipeline where auditability matters."
        ),
        (
            "  - Express Workflows: All workflows use Express by default to minimize\n"
            "    state transition costs. Express provides duration-based billing\n"
            "    (no per-transition charge) appropriate for high-fan-out workflows\n"
            "    like acquire_and_validate_spectra. Individual workflows may be\n"
            "    switched to Standard via the workflow_type parameter when they\n"
            "    need unlimited execution duration (e.g. discover_spectra_products,\n"
            "    regenerate_artifacts)."
        ),
        "fix module docstring Standard → Express",
    )
    print("  ✓ Fixed module docstring")

    # ── Patch 1b — _create_state_machine docstring ────────────────────────
    src = _replace_once(
        src,
        "Create a Standard Workflow state machine from an ASL file.",
        "Create a Step Functions state machine from an ASL file.",
        "fix _create_state_machine docstring",
    )
    print("  ✓ Fixed _create_state_machine docstring")

    # ── Post-conditions ───────────────────────────────────────────────────
    _require_absent(
        src,
        "Standard Workflows (not Express)",
        "stale module docstring should be gone",
    )
    _require_absent(
        src,
        "Create a Standard Workflow state machine",
        "stale method docstring should be gone",
    )

    path.write_text(src)
    print(f"  ✓ Wrote {path.relative_to(repo_root)}")


# ───────────────────────────────────────────────────────────────────────────
# Patch 2: infra/workflows/discover_spectra_products.asl.json
# ───────────────────────────────────────────────────────────────────────────


def _patch_discover_spectra_asl(repo_root: Path) -> None:
    path = repo_root / "infra" / "workflows" / "discover_spectra_products.asl.json"
    print(f"\n── Patching {path.relative_to(repo_root)} ──")

    src = path.read_text()

    # ── Preconditions ─────────────────────────────────────────────────────
    _require(
        src,
        "independent Standard Workflow state machines",
        "ASL comment — stale Standard reference",
    )

    print("  Preconditions satisfied.")

    # ── Patch ─────────────────────────────────────────────────────────────
    src = _replace_once(
        src,
        "independent Standard Workflow state machines",
        "independent Express Workflow state machines",
        "fix ASL comment Standard → Express",
    )
    print("  ✓ Fixed ASL comment")

    # ── Validate JSON still parses ────────────────────────────────────────
    try:
        json.loads(src)
    except json.JSONDecodeError as exc:
        print(f"  JSON VALIDATION FAILED after patch: {exc}")
        sys.exit(1)

    # ── Post-conditions ───────────────────────────────────────────────────
    _require_absent(
        src,
        "independent Standard Workflow state machines",
        "stale ASL comment should be gone",
    )

    path.write_text(src)
    print(f"  ✓ Wrote {path.relative_to(repo_root)}")


# ───────────────────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────────────────


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <repo_root>")
        print(f"Example: python {sys.argv[0]} .")
        sys.exit(1)

    repo_root = Path(sys.argv[1]).resolve()

    # Sanity check — make sure we're in the right repo
    if not (repo_root / "infra" / "nova_constructs" / "workflows.py").exists():
        print(f"ERROR: {repo_root} does not look like the Nova Cat repo root.")
        sys.exit(1)

    _patch_workflows_py(repo_root)
    _patch_discover_spectra_asl(repo_root)

    print("\n══ All patches applied successfully. ══")
    print("\nNext steps:")
    print("  1. Run: cd infra && cdk synth")
    print("  2. Run: python -m ruff check infra/nova_constructs/workflows.py")
    print("  3. Commit with the bug 3 CC prompt changes (if running together)")


if __name__ == "__main__":
    main()
