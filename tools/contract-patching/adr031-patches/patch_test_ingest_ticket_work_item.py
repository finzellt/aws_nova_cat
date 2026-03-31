#!/usr/bin/env python3
"""
ADR-031 Decision 7 test patch — test_ingest_ticket_integration.py

Adds WorkItem assertions to both the photometry and spectra happy-path tests.

Usage:
    python patch_test_ingest_ticket_work_item.py path/to/tests/integration/test_ingest_ticket_integration.py
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
        '        assert len(phot_resp["Items"]) == ingest_result["rows_produced"]',
        "photometry row count assertion",
    )
    _require(
        src,
        '        assert row["band_id"] == "HCT_HFOSC_Bessell_V"',
        "photometry band_id spot check",
    )
    # For spectra: look for the JobRun assertion that comes after the spectra ingest
    _require(
        src,
        '        assert job_run_item["outcome"] == "INGESTED_SPECTRA"',
        "spectra JobRun outcome assertion",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Photometry WorkItem assertion
    #           Anchored after the band_id spot check (last photometry data assertion)
    # =========================================================================
    OLD_PHOT = '        assert row["band_id"] == "HCT_HFOSC_Bessell_V"'
    NEW_PHOT = (
        '        assert row["band_id"] == "HCT_HFOSC_Bessell_V"\n'
        "\n"
        "        # ADR-031 Decision 7: WorkItem written for the regeneration pipeline\n"
        '        wq_resp = aws_resources["main_table"].query(\n'
        "            KeyConditionExpression=(\n"
        '                Key("PK").eq("WORKQUEUE")\n'
        '                & Key("SK").begins_with(f"{_V4739_SGR_NOVA_ID}#photometry#")\n'
        "            ),\n"
        "        )\n"
        '        assert len(wq_resp["Items"]) >= 1, (\n'
        '            "No WorkItem found in WORKQUEUE for photometry after ticket ingestion"\n'
        "        )"
    )
    src = _replace_once(src, OLD_PHOT, NEW_PHOT, "photometry WorkItem assertion")

    # =========================================================================
    # Patch 2 — Spectra WorkItem assertion
    #           Anchored before the spectra JobRun outcome assertion
    # =========================================================================
    OLD_SPEC = '        assert job_run_item["outcome"] == "INGESTED_SPECTRA"'
    NEW_SPEC = (
        "        # ADR-031 Decision 7: WorkItem written for the regeneration pipeline\n"
        '        wq_resp = aws_resources["main_table"].query(\n'
        "            KeyConditionExpression=(\n"
        '                Key("PK").eq("WORKQUEUE")\n'
        '                & Key("SK").begins_with(f"{_GQ_MUS_NOVA_ID}#spectra#")\n'
        "            ),\n"
        "        )\n"
        '        assert len(wq_resp["Items"]) >= 1, (\n'
        '            "No WorkItem found in WORKQUEUE for spectra after ticket ingestion"\n'
        "        )\n"
        "\n"
        '        assert job_run_item["outcome"] == "INGESTED_SPECTRA"'
    )
    src = _replace_once(src, OLD_SPEC, NEW_SPEC, "spectra WorkItem assertion")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    if src.count("WORKQUEUE") < 2:
        print(
            f"POSTCONDITION FAILED — expected 2+ WORKQUEUE references, found {src.count('WORKQUEUE')}"
        )
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")


if __name__ == "__main__":
    main()
