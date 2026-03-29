#!/usr/bin/env python3
"""
Patch: DOC-4 + CLEANUP-3 in test_ingest_ticket_integration.py

DOC-4: Remove stale "_sleep is also patched to a no-op" line from module
       docstring. The _sleep patch was removed during the Express migration
       (no polling loop = no sleep).

CLEANUP-3: Remove dead "PUBLIC_BUCKET_NAME" env var from the aws_env fixture.
           The old var is no longer read by any handler; only
           NOVA_CAT_PUBLIC_SITE_BUCKET is used.

Usage:
    python patch_integ_test_doc4_cleanup3.py tests/integration/test_ingest_ticket_integration.py
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
        print(f"Usage: {sys.argv[0]} <path/to/test_ingest_ticket_integration.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # ── Precondition checks ───────────────────────────────────────────────
    _require(
        src,
        "_sleep is also patched to a no-op for the quarantine path to avoid delays.",
        "DOC-4: stale _sleep docstring line",
    )
    _require(
        src,
        '    monkeypatch.setenv("PUBLIC_BUCKET_NAME", _PUBLIC_BUCKET)',
        "CLEANUP-3: dead PUBLIC_BUCKET_NAME env var",
    )

    print("All preconditions satisfied. Applying patches…")

    # ── DOC-4: Remove stale _sleep line from module docstring ─────────────
    src = _replace_once(
        src,
        "For the quarantine path _sfn is configured to return a SUCCEEDED execution\n"
        'whose output encodes outcome="NOT_FOUND".\n'
        "_sleep is also patched to a no-op for the quarantine path to avoid delays.\n",
        "For the quarantine path _sfn is configured to return a SUCCEEDED execution\n"
        'whose output encodes outcome="NOT_FOUND".\n',
        "DOC-4: remove stale _sleep docstring line",
    )
    print("  ✓ DOC-4: Removed stale _sleep docstring line")

    # ── CLEANUP-3: Remove dead PUBLIC_BUCKET_NAME env var ─────────────────
    src = _replace_once(
        src,
        '    monkeypatch.setenv("PUBLIC_BUCKET_NAME", _PUBLIC_BUCKET)\n',
        "",
        "CLEANUP-3: remove dead PUBLIC_BUCKET_NAME line",
    )
    print("  ✓ CLEANUP-3: Removed dead PUBLIC_BUCKET_NAME env var")

    # ── Post-condition checks ─────────────────────────────────────────────
    assert "_sleep is also patched" not in src, "DOC-4 post-condition failed"
    assert '"PUBLIC_BUCKET_NAME"' not in src, "CLEANUP-3 post-condition failed"
    # Verify the env vars that SHOULD remain are still present
    assert '"NOVA_CAT_PUBLIC_SITE_BUCKET"' in src, "NOVA_CAT_PUBLIC_SITE_BUCKET was lost"
    assert '"PHOTOMETRY_TABLE_NAME"' in src, "PHOTOMETRY_TABLE_NAME was lost"

    with open(path, "w") as fh:
        fh.write(src)

    print(f"\nDone. Wrote {path}")


if __name__ == "__main__":
    main()
