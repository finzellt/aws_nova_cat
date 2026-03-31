#!/usr/bin/env python3
"""
ADR-031 smoke test patch — tests/smoke/test_e2e.py

Adds assertion blocks to the full-pipeline smoke test covering all
ADR-031 decisions:

  1. Decision 6: nova_type field present on the Nova item (Stage 1–2)
  2. Decisions 2/3/5: enrichment fields on VALID stubs (new Stage 5b)
  3. Decision 4: web-ready CSV exists in S3 for each VALID stub (Stage 5b)
  4. Decision 7: WorkItem in WORKQUEUE for spectra (Stage 5b)

Usage:
    python patch_test_e2e_adr031.py path/to/tests/smoke/test_e2e.py
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
        print(f"Usage: {sys.argv[0]} <path/to/test_e2e.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src,
        '_ok("Nova item exists with ACTIVE status and coordinates")',
        "Stage 1-2 success marker",
    )
    _require(
        src,
        '_ok(f"acquire_and_validate_spectra — {valid_count} VALID product(s)")',
        "Stage 5 success marker",
    )
    _require(
        src,
        "from boto3.dynamodb.conditions import Key",
        "Key import",
    )
    _require(
        src,
        "        # ══════════════════════════════════════════════════════════════════\n"
        "        # Summary",
        "Summary section header",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 0 — Ensure boto3 is imported (for inline S3 client creation)
    # =========================================================================
    if "import boto3" not in src:
        src = _replace_once(
            src,
            "from boto3.dynamodb.conditions import Key",
            "import boto3\nfrom boto3.dynamodb.conditions import Key",
            "boto3 import",
        )

    # =========================================================================
    # Patch 1 — Decision 6: nova_type present on Nova item (Stage 1–2)
    #           Inserted just before the existing success marker.
    # =========================================================================
    OLD_NOVA_OK = '        _ok("Nova item exists with ACTIVE status and coordinates")'
    NEW_NOVA_OK = (
        "        # ADR-031 Decision 6: nova_type field must exist (value is null/None)\n"
        '        assert "nova_type" in nova_item, (\n'
        "            f\"Nova item missing 'nova_type' field — ADR-031 Decision 6 \"\n"
        '            f"forward-write not applied by initialize_nova. "\n'
        '            f"nova_id={nova_id}"\n'
        "        )\n"
        "        print(f\"  nova_type: {nova_item.get('nova_type')!r}\")\n"
        "\n"
        '        _ok("Nova item exists with ACTIVE status and coordinates")'
    )
    src = _replace_once(src, OLD_NOVA_OK, NEW_NOVA_OK, "nova_type assertion")

    # =========================================================================
    # Patch 2 — Decisions 2/3/5, Decision 4, Decision 7
    #           Inserted as a new "Stage 5b" between Stage 5's _ok line and
    #           the Summary block.
    # =========================================================================
    OLD_SUMMARY = (
        '        _ok(f"acquire_and_validate_spectra — {valid_count} VALID product(s)")\n'
        "\n"
        "        # ══════════════════════════════════════════════════════════════════\n"
        "        # Summary"
    )
    NEW_SUMMARY = (
        '        _ok(f"acquire_and_validate_spectra — {valid_count} VALID product(s)")\n'
        "\n"
        "        # ══════════════════════════════════════════════════════════════════\n"
        "        # Stage 5b: ADR-031 data layer readiness checks\n"
        "        # Confirms: enrichment fields, web-ready CSVs, and WorkItems are\n"
        "        # present for VALID spectra products.\n"
        "        # ══════════════════════════════════════════════════════════════════\n"
        '        _section("Stage 5b — ADR-031 data layer readiness checks")\n'
        "\n"
        '        _s3_check = boto3.client("s3")\n'
        "        for stub in final_stubs:\n"
        '            vs = stub.get("validation_status")\n'
        '            dp_id = stub.get("data_product_id", "?")\n'
        '            if vs == "VALID":\n'
        "                # Decisions 2/3/5: enrichment fields present on DDB item.\n"
        "                # Values depend on real FITS headers — assert presence only.\n"
        '                for field in ("instrument", "telescope", "observation_date_mjd"):\n'
        "                    assert field in stub, (\n"
        "                        f\"Product {dp_id} is VALID but missing '{field}' — \"\n"
        '                        f"ADR-031 enrichment fields not written by "\n'
        '                        f"RecordValidationResult"\n'
        "                    )\n"
        "\n"
        "                # Decision 4: web-ready CSV exists in the private S3 bucket.\n"
        '                csv_key = f"derived/spectra/{nova_id}/{dp_id}/web_ready.csv"\n'
        "                try:\n"
        "                    _s3_check.head_object(\n"
        "                        Bucket=stack.private_bucket_name,\n"
        "                        Key=csv_key,\n"
        "                    )\n"
        "                except Exception:\n"
        "                    pytest.fail(\n"
        '                        f"Product {dp_id} is VALID but web-ready CSV "\n'
        '                        f"not found at s3://{stack.private_bucket_name}/{csv_key}"\n'
        "                    )\n"
        "\n"
        '        print(f"  enrichment fields + web-ready CSVs verified for {valid_count} VALID product(s)")\n'
        "\n"
        "        # Decision 7: at least one WorkItem in WORKQUEUE for spectra\n"
        '        wq_items = _query_prefix(table, "WORKQUEUE", f"{nova_id}#spectra#")\n'
        "        assert len(wq_items) >= 1, (\n"
        '            f"No WorkItem found in WORKQUEUE for spectra after VALID validation. "\n'
        '            f"nova_id={nova_id}"\n'
        "        )\n"
        '        print(f"  WorkItems in WORKQUEUE: {len(wq_items)}")\n'
        "\n"
        '        _ok("ADR-031 data layer readiness checks passed")\n'
        "\n"
        "        # ══════════════════════════════════════════════════════════════════\n"
        "        # Summary"
    )
    src = _replace_once(src, OLD_SUMMARY, NEW_SUMMARY, "ADR-031 Stage 5b checks")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    checks = [
        ('"nova_type" in nova_item', "nova_type assertion"),
        ("Stage 5b — ADR-031 data layer readiness checks", "Stage 5b section title"),
        ('"instrument", "telescope", "observation_date_mjd"', "enrichment field list"),
        ("derived/spectra/{nova_id}/{dp_id}/web_ready.csv", "web-ready CSV key"),
        ('_query_prefix(table, "WORKQUEUE"', "WorkItem query"),
        ("ADR-031 data layer readiness checks passed", "Stage 5b success marker"),
    ]

    failed = False
    for marker, label in checks:
        if marker not in src:
            print(f"POSTCONDITION FAILED — {label!r}")
            failed = True

    if failed:
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")
    print()
    print("Assertions added:")
    print("  Stage 1-2: nova_type field present on Nova item (Decision 6)")
    print("  Stage 5b:  enrichment fields on VALID stubs (Decisions 2/3/5)")
    print("  Stage 5b:  web-ready CSV in S3 for VALID stubs (Decision 4)")
    print("  Stage 5b:  WorkItem in WORKQUEUE for spectra (Decision 7)")


if __name__ == "__main__":
    main()
