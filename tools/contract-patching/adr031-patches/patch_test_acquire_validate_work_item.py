#!/usr/bin/env python3
"""
ADR-031 Decision 7 test patch — test_acquire_and_validate_spectra_integration.py

Adds a WorkItem assertion to the happy-path test in TestHappyPathValid.

Usage:
    python patch_test_acquire_validate_work_item.py path/to/tests/integration/test_acquire_and_validate_spectra_integration.py
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
    if content.count(old) > 1:
        print(f"REPLACE FAILED — anchor for {label!r} appears more than once.")
        sys.exit(1)
    return content.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/test_file.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src,
        'assert dp["fits_profile_id"] == "ESO_UVES"',
        "fits_profile_id assertion in happy path test",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add WorkItem assertion after fits_profile_id assertion
    # =========================================================================
    OLD_ASSERT = '        assert dp["fits_profile_id"] == "ESO_UVES"'
    NEW_ASSERT = (
        '        assert dp["fits_profile_id"] == "ESO_UVES"\n'
        "\n"
        "        # ADR-031 Decision 7: WorkItem written for the regeneration pipeline\n"
        "        wq_resp = table.query(\n"
        "            KeyConditionExpression=(\n"
        '                Key("PK").eq("WORKQUEUE") & Key("SK").begins_with(f"{_NOVA_ID}#spectra#")\n'
        "            ),\n"
        "        )\n"
        '        assert len(wq_resp["Items"]) >= 1, (\n'
        '            "No WorkItem found in WORKQUEUE for spectra after VALID validation"\n'
        "        )\n"
        '        wi = wq_resp["Items"][0]\n'
        '        assert wi["dirty_type"] == "spectra"\n'
        '        assert wi["source_workflow"] == "acquire_and_validate_spectra"\n'
        '        assert wi["nova_id"] == _NOVA_ID'
    )
    src = _replace_once(src, OLD_ASSERT, NEW_ASSERT, "WorkItem assertion")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    if "WORKQUEUE" not in src:
        print("POSTCONDITION FAILED — WORKQUEUE not found after patch.")
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")


if __name__ == "__main__":
    main()
