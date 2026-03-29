#!/usr/bin/env python3
"""
Patch: STALE-1 — Update test_synth.py for ticket ingestion additions.

Changes:
  1. Add ticket_parser and nova_resolver_ticket to _ZIP_FUNCTIONS
  2. Add ticket_ingestor to _DOCKER_FUNCTIONS
  3. Add "nova-cat-ingest-ticket" to _EXPECTED_STATE_MACHINES
  4. Rename test_all_twelve_functions_exist → test_all_fifteen_functions_exist
  5. Add test_ingest_ticket_output_exists to TestCfnOutputs

Usage:
    python patch_test_synth_stale1.py tests/infra/test_synth.py
"""

from __future__ import annotations

import sys


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _require_absent(content: str, marker: str, label: str) -> None:
    if marker in content:
        print(f"PRECONDITION FAILED — {label!r} already present (patch already applied?).")
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
        print(f"Usage: {sys.argv[0]} <path/to/test_synth.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # ── Precondition checks ───────────────────────────────────────────────
    _require(src, '"nova-cat-name-reconciler"', "_ZIP_FUNCTIONS last entry")
    _require_absent(src, '"nova-cat-ticket-parser"', "ticket_parser already in _ZIP_FUNCTIONS")
    _require(
        src,
        '"nova-cat-spectra-validator": {"memory": 512, "timeout": 300},\n}',
        "_DOCKER_FUNCTIONS closing brace",
    )
    _require_absent(
        src, '"nova-cat-ticket-ingestor"', "ticket_ingestor already in _DOCKER_FUNCTIONS"
    )
    _require(
        src,
        '"nova-cat-acquire-and-validate-spectra",\n]',
        "_EXPECTED_STATE_MACHINES closing bracket",
    )
    _require_absent(
        src, '"nova-cat-ingest-ticket"', "ingest_ticket already in _EXPECTED_STATE_MACHINES"
    )
    _require(src, "def test_all_twelve_functions_exist", "old test method name")
    _require_absent(
        src, "test_ingest_ticket_output_exists", "ingest_ticket CfnOutput test already present"
    )

    print("All preconditions satisfied. Applying patches…")

    # ── 1. Add ticket_parser and nova_resolver_ticket to _ZIP_FUNCTIONS ───
    src = _replace_once(
        src,
        '    "nova-cat-name-reconciler": {"memory": 256, "timeout": 90},\n}',
        '    "nova-cat-name-reconciler": {"memory": 256, "timeout": 90},\n'
        '    "nova-cat-ticket-parser": {"memory": 256, "timeout": 30},\n'
        '    "nova-cat-nova-resolver-ticket": {"memory": 256, "timeout": 120},\n}',
        "add ticket_parser + nova_resolver_ticket to _ZIP_FUNCTIONS",
    )
    print("  ✓ Added ticket_parser and nova_resolver_ticket to _ZIP_FUNCTIONS")

    # ── 2. Add ticket_ingestor to _DOCKER_FUNCTIONS ───────────────────────
    src = _replace_once(
        src,
        '    "nova-cat-spectra-validator": {"memory": 512, "timeout": 300},\n}',
        '    "nova-cat-spectra-validator": {"memory": 512, "timeout": 300},\n'
        '    "nova-cat-ticket-ingestor": {"memory": 512, "timeout": 600},\n}',
        "add ticket_ingestor to _DOCKER_FUNCTIONS",
    )
    print("  ✓ Added ticket_ingestor to _DOCKER_FUNCTIONS")

    # ── 3. Add ingest_ticket to _EXPECTED_STATE_MACHINES ──────────────────
    src = _replace_once(
        src,
        '    "nova-cat-acquire-and-validate-spectra",\n]',
        '    "nova-cat-acquire-and-validate-spectra",\n    "nova-cat-ingest-ticket",\n]',
        "add ingest_ticket to _EXPECTED_STATE_MACHINES",
    )
    print("  ✓ Added ingest_ticket to _EXPECTED_STATE_MACHINES")

    # ── 4. Rename test method ─────────────────────────────────────────────
    src = _replace_once(
        src,
        "def test_all_twelve_functions_exist",
        "def test_all_fifteen_functions_exist",
        "rename test_all_twelve → test_all_fifteen",
    )
    print("  ✓ Renamed test_all_twelve_functions_exist → test_all_fifteen_functions_exist")

    # ── 5. Add CfnOutput test for ingest_ticket ──────────────────────────
    # Insert after the acquire_and_validate_spectra output test
    src = _replace_once(
        src,
        "    def test_acquire_and_validate_spectra_output_exists(\n"
        "        self, template: assertions.Template\n"
        "    ) -> None:\n"
        "        template.has_output(\n"
        '            "*", {"Export": {"Name": "NovaCat-AcquireAndValidateSpectraStateMachineArn"}}\n'
        "        )",
        "    def test_acquire_and_validate_spectra_output_exists(\n"
        "        self, template: assertions.Template\n"
        "    ) -> None:\n"
        "        template.has_output(\n"
        '            "*", {"Export": {"Name": "NovaCat-AcquireAndValidateSpectraStateMachineArn"}}\n'
        "        )\n"
        "\n"
        "    def test_ingest_ticket_output_exists(self, template: assertions.Template) -> None:\n"
        "        template.has_output(\n"
        '            "*", {"Export": {"Name": "NovaCat-IngestTicketStateMachineArn"}}\n'
        "        )",
        "add test_ingest_ticket_output_exists",
    )
    print("  ✓ Added test_ingest_ticket_output_exists")

    # ── Post-condition checks ─────────────────────────────────────────────
    assert '"nova-cat-ticket-parser"' in src, "ticket_parser post-condition failed"
    assert '"nova-cat-nova-resolver-ticket"' in src, "nova_resolver_ticket post-condition failed"
    assert '"nova-cat-ticket-ingestor"' in src, "ticket_ingestor post-condition failed"
    assert '"nova-cat-ingest-ticket"' in src, "ingest_ticket SM post-condition failed"
    assert "test_all_fifteen_functions_exist" in src, "rename post-condition failed"
    assert "test_all_twelve_functions_exist" not in src, "old name still present"
    assert "test_ingest_ticket_output_exists" in src, "CfnOutput test post-condition failed"

    with open(path, "w") as fh:
        fh.write(src)

    print(f"\nDone. Wrote {path}")


if __name__ == "__main__":
    main()
