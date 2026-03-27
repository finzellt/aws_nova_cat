#!/usr/bin/env python3
"""
Chunk 5a patch — compute.py additions for ingest_ticket workflow.

Adds three Lambda constructs:
  ticket_parser          — zip-bundled  (parses .txt ticket file)
  nova_resolver_ticket   — zip-bundled  (NameMapping lookup + initialize_nova polling)
  ticket_ingestor        — container    (photometry + spectra ingestion; astropy required)

Also:
  - Adds `photometry_table: dynamodb.Table` parameter to __init__ and
    _grant_permissions (caller: nova_cat_stack.py must be updated to pass
    storage.photometry_table once storage.py is patched in Chunk 5a file 4).
  - Injects PHOTOMETRY_TABLE_NAME and DIAGNOSTICS_BUCKET env vars into
    ticket_ingestor after construction (not in shared_env; table-specific).
  - Adds least-privilege IAM grants for all three new functions.

Usage:
    python patch_compute_chunk5a.py path/to/infra/nova_constructs/compute.py

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
        print(f"Usage: {sys.argv[0]} <path/to/compute.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks — verify the file is the expected version before
    # making any modifications.
    # -------------------------------------------------------------------------
    _require(src, '"name_reconciler": _FunctionSpec(', "name_reconciler in _FUNCTION_SPECS")
    _require(src, "name_reconciler: lambda_.Function", "name_reconciler mypy attribute declaration")
    _require(
        src,
        "ads_secret: secretsmanager.ISecret,\n        services_root",
        "__init__ signature anchor",
    )
    _require(
        src,
        "self._grant_permissions(\n            table, private_bucket, public_site_bucket, quarantine_topic, ads_secret\n        )",
        "_grant_permissions call site",
    )
    _require(
        src,
        'self._functions["reference_manager"].add_environment(',
        "reference_manager add_environment anchor",
    )
    _require(
        src,
        'table.grant_read_write_data(self._functions["name_reconciler"])',
        "name_reconciler grant (end of _grant_permissions)",
    )
    _require(
        src,
        'self._functions["spectra_discoverer"] = spectra_discoverer\n\n        # ------------------------------------------------------------------\n        # IAM grants',
        "spectra_discoverer block → IAM grants transition",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — _FUNCTION_SPECS: append ticket_parser and nova_resolver_ticket
    #           after the name_reconciler entry.
    # =========================================================================
    OLD_SPECS_TAIL = """\
    "name_reconciler": _FunctionSpec(
        service_dir="name_reconciler",
        description=(
            "Queries naming authorities, reconciles official designations and aliases, "
            "and applies name updates to the nova record. "
            "Handles FetchCurrentNamingState, QueryAuthorityA/B, ReconcileNaming, "
            "ApplyNameUpdates. Used by: name_check_and_reconcile."
        ),
        timeout=cdk.Duration.seconds(90),  # External authority queries
    ),
}"""

    NEW_SPECS_TAIL = """\
    "name_reconciler": _FunctionSpec(
        service_dir="name_reconciler",
        description=(
            "Queries naming authorities, reconciles official designations and aliases, "
            "and applies name updates to the nova record. "
            "Handles FetchCurrentNamingState, QueryAuthorityA/B, ReconcileNaming, "
            "ApplyNameUpdates. Used by: name_check_and_reconcile."
        ),
        timeout=cdk.Duration.seconds(90),  # External authority queries
    ),
    # ------------------------------------------------------------------
    # ingest_ticket workflow functions (Chunk 5a)
    # ------------------------------------------------------------------
    "ticket_parser": _FunctionSpec(
        service_dir="ticket_parser",
        description=(
            "Reads a .txt ticket file from S3, parses key-value pairs, discriminates "
            "ticket type (photometry: DATA FILENAME / spectra: METADATA FILENAME), and "
            "validates with Pydantic. Handles ParseTicket. Used by: ingest_ticket."
        ),
        timeout=cdk.Duration.seconds(30),
    ),
    "nova_resolver_ticket": _FunctionSpec(
        service_dir="nova_resolver_ticket",
        description=(
            "Resolves OBJECT NAME to nova_id via NameMapping lookup. If absent, "
            "invokes initialize_nova (StartExecution + DescribeExecution poll until "
            "terminal). Raises UNRESOLVABLE_OBJECT_NAME or IDENTITY_AMBIGUITY for "
            "quarantine-eligible outcomes. Handles ResolveNova. Used by: ingest_ticket."
        ),
        timeout=cdk.Duration.seconds(120),  # Accounts for initialize_nova execution + polling
    ),
}"""

    src = _replace_once(src, OLD_SPECS_TAIL, NEW_SPECS_TAIL, "_FUNCTION_SPECS tail")

    # =========================================================================
    # Patch 2 — NovaCatCompute class: add mypy attribute declarations for the
    #           three new functions after name_reconciler.
    # =========================================================================
    OLD_ATTRS = "    name_reconciler: lambda_.Function"
    NEW_ATTRS = """\
    name_reconciler: lambda_.Function
    # ingest_ticket workflow (Chunk 5a)
    ticket_parser: lambda_.Function
    nova_resolver_ticket: lambda_.Function
    ticket_ingestor: lambda_.DockerImageFunction"""

    src = _replace_once(src, OLD_ATTRS, NEW_ATTRS, "mypy attribute declarations")

    # =========================================================================
    # Patch 3 — __init__ signature: add photometry_table parameter after table.
    #           nova_cat_stack.py must be updated once storage.py is patched.
    # =========================================================================
    OLD_INIT_SIG = """\
        table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
        ads_secret: secretsmanager.ISecret,
        services_root: str = "../../services","""

    NEW_INIT_SIG = """\
        table: dynamodb.Table,
        photometry_table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
        ads_secret: secretsmanager.ISecret,
        services_root: str = "../../services","""

    src = _replace_once(src, OLD_INIT_SIG, NEW_INIT_SIG, "__init__ signature")

    # =========================================================================
    # Patch 4 — Construct body: insert ticket_ingestor DockerImageFunction block
    #           immediately before the "IAM grants" comment that follows
    #           spectra_discoverer.
    # =========================================================================
    OLD_DOCKER_TAIL = """\
        self._functions["spectra_discoverer"] = spectra_discoverer

        # ------------------------------------------------------------------
        # IAM grants — least-privilege
        # ------------------------------------------------------------------
        self._grant_permissions(
            table, private_bucket, public_site_bucket, quarantine_topic, ads_secret
        )"""

    NEW_DOCKER_TAIL = """\
        self._functions["spectra_discoverer"] = spectra_discoverer

        # ------------------------------------------------------------------
        # ticket_ingestor — DockerImageFunction
        #
        # Handles both the photometry and spectra branches of ingest_ticket.
        # astropy is required for FITS construction (spectra branch), so
        # container deployment is mandatory — same reason as spectra_validator.
        # Memory sized for FITS assembly + per-row DDB writes across a full
        # ticket; timeout covers the spectra branch (multiple FITS + S3 uploads).
        # ------------------------------------------------------------------
        ticket_ingestor = lambda_.DockerImageFunction(
            self,
            "TicketIngestor",
            function_name=f"{env_prefix}-ticket-ingestor",
            architecture=lambda_.Architecture.ARM_64,
            code=lambda_.DockerImageCode.from_image_asset(
                self._services_root,
                file="ticket_ingestor/Dockerfile",
            ),
            description=(
                "Executes photometry and spectra ingestion for ticket-driven workflows. "
                "Photometry branch: reads headerless CSV, resolves band registry, writes "
                "PhotometryRow items to dedicated DDB table. "
                "Spectra branch: reads metadata CSV, converts per-spectrum CSVs to FITS, "
                "uploads to public S3 bucket, writes DDB DataProduct + FileObject items. "
                "Handles IngestPhotometry, IngestSpectra. Used by: ingest_ticket. "
                "Container-bundled: astropy required for FITS I/O."
            ),
            memory_size=512,
            timeout=cdk.Duration.minutes(10),
            environment=shared_env,
            log_retention=_LOG_RETENTION,
            tracing=lambda_.Tracing.ACTIVE,
        )
        self._functions["ticket_ingestor"] = ticket_ingestor

        # ------------------------------------------------------------------
        # IAM grants — least-privilege
        # ------------------------------------------------------------------
        self._grant_permissions(
            table, photometry_table, private_bucket, public_site_bucket, quarantine_topic, ads_secret
        )"""

    src = _replace_once(src, OLD_DOCKER_TAIL, NEW_DOCKER_TAIL, "ticket_ingestor block + grant call")

    # =========================================================================
    # Patch 5 — Post-construction env var injection: add PHOTOMETRY_TABLE_NAME
    #           and DIAGNOSTICS_BUCKET for ticket_ingestor after the existing
    #           reference_manager.add_environment block.
    # =========================================================================
    OLD_ENV_INJECTION = """\
        # ADS secret name injected only into reference_manager (the sole consumer)
        self._functions["reference_manager"].add_environment(
            "ADS_SECRET_NAME", ads_secret.secret_name
        )"""

    NEW_ENV_INJECTION = """\
        # ADS secret name injected only into reference_manager (the sole consumer)
        self._functions["reference_manager"].add_environment(
            "ADS_SECRET_NAME", ads_secret.secret_name
        )

        # Photometry table name and diagnostics bucket injected only into
        # ticket_ingestor — these are not in shared_env because no other
        # Lambda currently requires them.
        self._functions["ticket_ingestor"].add_environment(
            "PHOTOMETRY_TABLE_NAME", photometry_table.table_name
        )
        self._functions["ticket_ingestor"].add_environment(
            # Row-failure diagnostics land in the private bucket under
            # diagnostics/photometry/<nova_id>/row_failures/<sha256>.json.
            # DIAGNOSTICS_BUCKET is a separate env var (not NOVA_CAT_PRIVATE_BUCKET)
            # so the handler can be tested with a distinct moto bucket.
            "DIAGNOSTICS_BUCKET", private_bucket.bucket_name
        )"""

    src = _replace_once(src, OLD_ENV_INJECTION, NEW_ENV_INJECTION, "env var injection block")

    # =========================================================================
    # Patch 6 — _grant_permissions signature: add photometry_table parameter.
    # =========================================================================
    OLD_GRANT_SIG = '''\
    def _grant_permissions(
        self,
        table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
        ads_secret: secretsmanager.ISecret,
    ) -> None:
        """
        Grants least-privilege IAM permissions to each Lambda function.
        """'''

    NEW_GRANT_SIG = '''\
    def _grant_permissions(
        self,
        table: dynamodb.Table,
        photometry_table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
        ads_secret: secretsmanager.ISecret,
    ) -> None:
        """
        Grants least-privilege IAM permissions to each Lambda function.
        """'''

    src = _replace_once(src, OLD_GRANT_SIG, NEW_GRANT_SIG, "_grant_permissions signature")

    # =========================================================================
    # Patch 7 — _grant_permissions body: append grants for the three new
    #           functions after the name_reconciler grant (end of method).
    # =========================================================================
    OLD_GRANTS_TAIL = '        table.grant_read_write_data(self._functions["name_reconciler"])'

    NEW_GRANTS_TAIL = """\
        table.grant_read_write_data(self._functions["name_reconciler"])

        # ------------------------------------------------------------------
        # ingest_ticket workflow grants (Chunk 5a)
        # ------------------------------------------------------------------

        # ticket_parser: reads .txt ticket file from private bucket (S3 key
        # supplied as ticket_path in the workflow event).
        private_bucket.grant_read(
            self._functions["ticket_parser"],
            "raw/*",
        )

        # nova_resolver_ticket: reads NameMapping items (name → nova_id lookup);
        # may write NameMapping on alias upsert path within initialize_nova.
        # sfn:StartExecution + sfn:DescribeExecution on initialize_nova are
        # granted in workflows.py (NovaCatWorkflows owns the SFN ARNs).
        table.grant_read_write_data(self._functions["nova_resolver_ticket"])

        # ticket_ingestor (photometry branch):
        #   - Read data CSV from private bucket (raw/*)
        #   - Write PhotometryRow items to dedicated photometry table
        #   - Read/write PRODUCT#PHOTOMETRY_TABLE envelope in main table
        #   - Write row-failure diagnostics to private bucket (diagnostics/*)
        # ticket_ingestor (spectra branch):
        #   - Read metadata CSV + spectrum CSVs from private bucket (raw/*)
        #   - Write FITS files to public site bucket (raw/<nova_id>/ticket_ingestion/*)
        #   - Write DataProduct + FileObject items to main table
        private_bucket.grant_read(
            self._functions["ticket_ingestor"],
            "raw/*",
        )
        private_bucket.grant_write(
            self._functions["ticket_ingestor"],
            "diagnostics/*",
        )
        photometry_table.grant_write_data(self._functions["ticket_ingestor"])
        table.grant_read_write_data(self._functions["ticket_ingestor"])
        public_site_bucket.grant_write(
            self._functions["ticket_ingestor"],
            "raw/*",
        )"""

    src = _replace_once(
        src, OLD_GRANTS_TAIL, NEW_GRANTS_TAIL, "grants tail (name_reconciler → new grants)"
    )

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        ('"ticket_parser": _FunctionSpec(', "ticket_parser in _FUNCTION_SPECS"),
        ('"nova_resolver_ticket": _FunctionSpec(', "nova_resolver_ticket in _FUNCTION_SPECS"),
        ("ticket_parser: lambda_.Function", "ticket_parser mypy attribute"),
        ("nova_resolver_ticket: lambda_.Function", "nova_resolver_ticket mypy attribute"),
        ("ticket_ingestor: lambda_.DockerImageFunction", "ticket_ingestor mypy attribute"),
        ("photometry_table: dynamodb.Table,", "photometry_table parameter"),
        ('"ticket_ingestor/Dockerfile"', "ticket_ingestor Dockerfile path"),
        ('self._functions["ticket_ingestor"] = ticket_ingestor', "ticket_ingestor registered"),
        (
            '"PHOTOMETRY_TABLE_NAME", photometry_table.table_name',
            "PHOTOMETRY_TABLE_NAME env injection",
        ),
        ('"DIAGNOSTICS_BUCKET", private_bucket.bucket_name', "DIAGNOSTICS_BUCKET env injection"),
        (
            'photometry_table.grant_write_data(self._functions["ticket_ingestor"])',
            "photometry_table grant",
        ),
        (
            'public_site_bucket.grant_write(\n            self._functions["ticket_ingestor"]',
            "public_site_bucket grant for ticket_ingestor",
        ),
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
    print("  1. Apply storage.py patch (Chunk 5a file 4) to add photometry_table.")
    print("  2. Update nova_cat_stack.py: pass photometry_table=self.storage.photometry_table")
    print("     to the NovaCatCompute constructor.")
    print("  3. Run: mypy --strict infra/ && ruff check infra/")


if __name__ == "__main__":
    main()
