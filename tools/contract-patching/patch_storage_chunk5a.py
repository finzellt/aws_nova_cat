#!/usr/bin/env python3
"""
Chunk 5a patch — storage.py additions for ingest_ticket workflow.

Changes:
  1. Updates the module docstring to list the photometry table.
  2. Updates the NovaCatStorage class docstring Exposes list.
  3. Provisions the dedicated photometry DynamoDB table (PhotometryRows)
     immediately after the main NovaCat table + EligibilityIndex GSI.
  4. Adds a CfnOutput for the photometry table name.

Design notes baked into comments:
  - Separate table (not a separate SK prefix in the main table) because
    PhotometryRow items have a different schema, different IAM grant scope,
    and different throughput profile from all other NovaCat entities (ADR-020).
  - No GSI at provisioning time — post-MVP cross-nova queries will be enabled
    by a future GSI on band + epoch fields without any storage migration.
  - Same billing mode (PAY_PER_REQUEST) and removal policy as the main table.
  - PITR follows the same enable_pitr parameter as the main table (prod only).

Usage:
    python patch_storage_chunk5a.py path/to/infra/nova_constructs/storage.py

Precondition assertions abort with a clear message if the target text is not
found exactly as expected — safe to re-run after a failed partial application.
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
        print(f"Usage: {sys.argv[0]} <path/to/storage.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src,
        "  - Single DynamoDB table (NovaCat) with EligibilityIndex GSI",
        "module docstring sentinel",
    )
    _require(
        src,
        "      quarantine_topic    — SNS topic for quarantine notifications (all workflows)",
        "class docstring Exposes list tail",
    )
    _require(
        src,
        "        self.table.add_global_secondary_index(",
        "EligibilityIndex GSI call",
    )
    _require(
        src,
        "        )\n\n        # ------------------------------------------------------------------\n        # S3 — Private data bucket",
        "GSI closing paren → S3 private bucket comment transition",
    )
    _require(
        src,
        '"QuarantineTopicArn",',
        "QuarantineTopicArn CfnOutput logical ID",
    )
    _require(
        src,
        '            export_name=f"{cf_prefix}-QuarantineTopicArn",\n        )',
        "QuarantineTopicArn CfnOutput closing paren",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Module docstring: add photometry table to the Provisions list.
    # =========================================================================
    OLD_MODULE_DOC = "  - Single DynamoDB table (NovaCat) with EligibilityIndex GSI"
    NEW_MODULE_DOC = (
        "  - Single DynamoDB table (NovaCat) with EligibilityIndex GSI\n"
        "  - Dedicated DynamoDB table (NovaCatPhotometry) for PhotometryRow items (ADR-020)"
    )
    src = _replace_once(src, OLD_MODULE_DOC, NEW_MODULE_DOC, "module docstring Provisions list")

    # =========================================================================
    # Patch 2 — Class docstring: add photometry_table to the Exposes list.
    # =========================================================================
    OLD_CLASS_DOC = (
        "      quarantine_topic    — SNS topic for quarantine notifications (all workflows)"
    )
    NEW_CLASS_DOC = (
        "      quarantine_topic    — SNS topic for quarantine notifications (all workflows)\n"
        "      photometry_table    — dedicated DynamoDB table for PhotometryRow items (ADR-020)"
    )
    src = _replace_once(src, OLD_CLASS_DOC, NEW_CLASS_DOC, "class docstring Exposes list")

    # =========================================================================
    # Patch 3 — Provision the photometry table immediately after the
    #           EligibilityIndex GSI block and before the S3 private bucket.
    #
    # Key design points encoded in the comment block:
    #   - Separate table per ADR-020 Decision 1 rationale
    #   - PK = nova_id, SK = "PHOT#<row_id>" (ADR-020 Decision 2)
    #   - No GSI at provisioning time (post-MVP)
    #   - Same billing mode + removal policy as main table
    # =========================================================================
    OLD_GSI_TO_S3 = (
        "        )\n\n"
        "        # ------------------------------------------------------------------\n"
        "        # S3 — Private data bucket"
    )
    NEW_GSI_TO_S3 = """\
        )

        # ------------------------------------------------------------------
        # DynamoDB — Dedicated photometry table (PhotometryRows)
        #
        # Stores individual PhotometryRow items for all novae. Kept separate
        # from the main NovaCat table (ADR-020 Decision 1) because:
        #   - PhotometryRow has a distinct schema and independent lifecycle
        #   - ticket_ingestor and ingest_photometry need a narrowly scoped
        #     IAM grant that does not extend to all NovaCat entities
        #   - Separate table simplifies future GSI design for cross-nova
        #     photometry queries without touching the main table
        #
        # Primary key (ADR-020 Decision 2):
        #   PK (String) = "<nova_id>"
        #   SK (String) = "PHOT#<row_id>"
        #
        # No GSI provisioned at this time. A future GSI on band + epoch
        # fields will enable cross-nova queries (ADR-020 OQ-5); it can be
        # added without any storage migration.
        #
        # Billing mode and removal policy match the main table.
        # PITR follows the same enable_pitr parameter (prod-only by default).
        # ------------------------------------------------------------------
        self.photometry_table = dynamodb.Table(
            self,
            "PhotometryTable",
            table_name=f"{cf_prefix}Photometry",
            partition_key=dynamodb.Attribute(
                name="PK",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="SK",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=enable_pitr,
            removal_policy=removal_policy,
        )

        # ------------------------------------------------------------------
        # S3 — Private data bucket"""

    src = _replace_once(src, OLD_GSI_TO_S3, NEW_GSI_TO_S3, "photometry table insertion point")

    # =========================================================================
    # Patch 4 — CfnOutput: add PhotometryTableName after QuarantineTopicArn.
    # =========================================================================
    OLD_LAST_OUTPUT = '            export_name=f"{cf_prefix}-QuarantineTopicArn",\n        )'
    NEW_LAST_OUTPUT = (
        '            export_name=f"{cf_prefix}-QuarantineTopicArn",\n'
        "        )\n"
        "        cdk.CfnOutput(\n"
        "            self,\n"
        '            "PhotometryTableName",\n'
        "            value=self.photometry_table.table_name,\n"
        '            description="NovaCat dedicated photometry DynamoDB table name",\n'
        '            export_name=f"{cf_prefix}-PhotometryTableName",\n'
        "        )"
    )
    src = _replace_once(src, OLD_LAST_OUTPUT, NEW_LAST_OUTPUT, "PhotometryTableName CfnOutput")

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        (
            "  - Dedicated DynamoDB table (NovaCatPhotometry) for PhotometryRow items (ADR-020)",
            "module docstring updated",
        ),
        (
            "      photometry_table    — dedicated DynamoDB table for PhotometryRow items (ADR-020)",
            "class docstring updated",
        ),
        ("self.photometry_table = dynamodb.Table(", "photometry_table provisioned"),
        ('"PhotometryTable",', "PhotometryTable construct ID"),
        ('table_name=f"{cf_prefix}Photometry",', "photometry table name"),
        ("point_in_time_recovery=enable_pitr,", "PITR on photometry table"),
        ('"PhotometryTableName",', "PhotometryTableName CfnOutput logical ID"),
        ('export_name=f"{cf_prefix}-PhotometryTableName"', "PhotometryTableName export name"),
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
    print("Next steps:")
    print("  1. Update nova_cat_stack.py: pass photometry_table=self.storage.photometry_table")
    print("     to the NovaCatCompute constructor (one-line change).")
    print("  2. Run: mypy --strict infra/ && ruff check infra/")
    print("  3. Run: cdk diff NovaCatSmoke to verify two new resources:")
    print("     - AWS::DynamoDB::Table (NovaCatPhotometry)")
    print("     - AWS::CloudFormation::Export (PhotometryTableName)")


if __name__ == "__main__":
    main()
