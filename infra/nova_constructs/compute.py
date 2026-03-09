"""
Nova Cat Compute Construct

Provisions all Lambda functions with:
  - Consistent runtime (Python 3.11)
  - Shared environment variables (table name, bucket names, SNS topic ARN)
  - Least-privilege IAM grants derived from each function's actual access patterns
    (see dynamodb-access-patterns.md for the source of truth)
  - Structured logging enabled via LOG_LEVEL and POWERTOOLS_SERVICE_NAME
  - Reserved concurrency not set at this layer (Step Functions controls
    invocation rate; reserved concurrency added per-function if throttling
    becomes an issue in production)

Lambda function inventory (12 functions):
  nova_resolver          — name resolution, coordinate dedup, nova upsert (initialize_nova)
  job_run_manager        — JobRun/Attempt record writes (all workflows, shared)
  idempotency_guard      — idempotency lock acquisition (all workflows, shared)
  archive_resolver       — external archive queries: SIMBAD, TNS (initialize_nova)
  workflow_launcher      — SFN start-execution + SNS publish for downstream workflows
  reference_manager      — ADS reference fetch, upsert, link, discovery_date compute
  spectra_discoverer     — provider adapter dispatch, data_product_id assignment, stub persist
  spectra_acquirer       — bytes download, fingerprint, ZIP unpack (acquire_and_validate_spectra)
  spectra_validator      — FITS profile selection, normalization, validation result record
                           (DockerImageFunction — astropy + numpy require container bundling)
  photometry_ingestor    — photometry file validation, parquet rebuild, metadata persist
  quarantine_handler     — quarantine persistence + SNS best-effort notification (all workflows)
  name_reconciler        — naming authority queries, reconciliation, alias updates
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import aws_cdk as cdk
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_secretsmanager as secretsmanager
import aws_cdk.aws_sns as sns
from constructs import Construct

# Default Lambda settings — tuned for Nova Cat's low-throughput, cost-aware profile.
_DEFAULT_MEMORY_MB = 256
_DEFAULT_TIMEOUT = cdk.Duration.seconds(30)
_PYTHON_RUNTIME = lambda_.Runtime.PYTHON_3_11
_LOG_RETENTION = logs.RetentionDays.THREE_MONTHS


@dataclass
class _FunctionSpec:
    """Internal spec for each zip-bundled Lambda function."""

    service_dir: str  # directory under services/
    description: str
    memory_mb: int = _DEFAULT_MEMORY_MB
    timeout: cdk.Duration = _DEFAULT_TIMEOUT


# spectra_validator, archive_resolver, and spectra_discoverer are intentionally
# absent — all three are DockerImageFunctions (astropy/astroquery have compiled
# C extensions that exceed the Lambda zip limit) and are constructed separately below.
_FUNCTION_SPECS: dict[str, _FunctionSpec] = {
    "nova_resolver": _FunctionSpec(
        service_dir="nova_resolver",
        description=(
            "Resolves candidate names and coordinates to stable nova_id. "
            "Handles CheckExistingNovaByName, CheckExistingNovaByCoordinates, "
            "CreateNovaId, UpsertMinimalNovaMetadata, UpsertAliasForExistingNova. "
            "Used by: initialize_nova."
        ),
    ),
    "job_run_manager": _FunctionSpec(
        service_dir="job_run_manager",
        description=(
            "Writes JobRun and Attempt operational records. "
            "Handles BeginJobRun, TerminalFailHandler (error classification + fingerprint), "
            "FinalizeJobRunSuccess, FinalizeJobRunFailed, "
            "FinalizeJobRunQuarantined. Used by: all workflows (shared)."
        ),
    ),
    "idempotency_guard": _FunctionSpec(
        service_dir="idempotency_guard",
        description=(
            "Acquires and checks workflow-level idempotency locks via conditional writes. "
            "Handles AcquireIdempotencyLock. Used by: all workflows (shared)."
        ),
    ),
    "workflow_launcher": _FunctionSpec(
        service_dir="workflow_launcher",
        description=(
            "Starts downstream Step Functions executions and publishes SNS continuation events. "
            "Used by: initialize_nova, ingest_new_nova, discover_spectra_products."
        ),
    ),
    "reference_manager": _FunctionSpec(
        service_dir="reference_manager",
        description=(
            "Fetches ADS references, upserts Reference entities, links NovaReference "
            "records, and computes discovery_date. Used by: refresh_references."
        ),
        timeout=cdk.Duration.seconds(90),  # ADS API calls
    ),
    "spectra_acquirer": _FunctionSpec(
        service_dir="spectra_acquirer",
        description=(
            "Downloads spectra bytes from provider URLs, handles ZIP unpacking, "
            "and computes content fingerprints (SHA-256). Persists raw bytes to S3. "
            "Handles AcquireArtifact. Used by: acquire_and_validate_spectra."
        ),
        memory_mb=512,  # Larger for in-memory FITS/ZIP handling
        timeout=cdk.Duration.minutes(15),  # Per execution-governance.md acquisition timeout
    ),
    "photometry_ingestor": _FunctionSpec(
        service_dir="photometry_ingestor",
        description=(
            "Validates photometry files, rebuilds the canonical per-nova parquet table, "
            "and persists ingestion metadata. Used by: ingest_photometry."
        ),
        memory_mb=512,  # Parquet rebuild may be memory-intensive
        timeout=cdk.Duration.minutes(5),
    ),
    "quarantine_handler": _FunctionSpec(
        service_dir="quarantine_handler",
        description=(
            "Persists quarantine status and diagnostic metadata, then publishes a "
            "best-effort SNS notification for operator review. "
            "Handles QuarantineHandler across all workflows (shared). "
            "SNS publish errors are swallowed — quarantine persistence is authoritative."
        ),
    ),
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
}


class NovaCatCompute(Construct):
    """
    Compute layer for Nova Cat.

    Exposes each Lambda function as a named attribute (e.g. self.nova_resolver)
    for use by the Step Functions construct.
    """

    # Explicit attribute declarations so mypy can see dynamically assigned
    # attributes (set via setattr in __init__). One entry per Lambda function.
    nova_resolver: lambda_.Function
    job_run_manager: lambda_.Function
    idempotency_guard: lambda_.Function
    archive_resolver: lambda_.DockerImageFunction
    workflow_launcher: lambda_.Function
    reference_manager: lambda_.Function
    spectra_discoverer: lambda_.DockerImageFunction
    spectra_acquirer: lambda_.Function
    spectra_validator: lambda_.DockerImageFunction
    photometry_ingestor: lambda_.Function
    quarantine_handler: lambda_.Function
    name_reconciler: lambda_.Function

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
        ads_secret: secretsmanager.ISecret,
        services_root: str = "../../services",
    ) -> None:
        super().__init__(scope, construct_id)

        self._services_root = os.path.join(os.path.dirname(__file__), services_root)

        # ------------------------------------------------------------------
        # Shared environment variables injected into every Lambda.
        # ------------------------------------------------------------------
        shared_env = {
            "NOVA_CAT_TABLE_NAME": table.table_name,
            "NOVA_CAT_PRIVATE_BUCKET": private_bucket.bucket_name,
            "NOVA_CAT_PUBLIC_SITE_BUCKET": public_site_bucket.bucket_name,
            "NOVA_CAT_QUARANTINE_TOPIC_ARN": quarantine_topic.topic_arn,
            "LOG_LEVEL": "INFO",
            "POWERTOOLS_SERVICE_NAME": "nova-cat",
        }

        # ------------------------------------------------------------------
        # nova_common Lambda Layer
        # ------------------------------------------------------------------
        nova_common_layer = lambda_.LayerVersion(
            self,
            "NovaCommonLayer",
            layer_version_name="nova-cat-nova-common",
            code=lambda_.Code.from_asset(os.path.join(self._services_root, "nova_common_layer")),
            compatible_runtimes=[_PYTHON_RUNTIME],
            description="Nova Cat shared utilities: Powertools Logger, Tracer, configure_logging",
        )

        # ------------------------------------------------------------------
        # Build all zip-bundled Lambda functions from specs
        # ------------------------------------------------------------------
        self._functions: dict[str, lambda_.Function | lambda_.DockerImageFunction] = {}

        for name, spec in _FUNCTION_SPECS.items():
            fn = lambda_.Function(
                self,
                _to_pascal(name),
                function_name=f"nova-cat-{name.replace('_', '-')}",
                runtime=_PYTHON_RUNTIME,
                handler="handler.handle",
                code=lambda_.Code.from_asset(os.path.join(self._services_root, spec.service_dir)),
                description=spec.description,
                memory_size=spec.memory_mb,
                timeout=spec.timeout,
                environment=shared_env,
                log_retention=_LOG_RETENTION,
                tracing=lambda_.Tracing.ACTIVE,
                layers=[nova_common_layer],
            )
            self._functions[name] = fn

        # ------------------------------------------------------------------
        # spectra_validator — DockerImageFunction
        #
        # astropy and numpy include compiled C extensions that exceed the
        # Lambda zip size limit and cannot be deployed as a layer. The service
        # directory contains a Dockerfile that installs these dependencies into
        # the Lambda container image. The nova_common layer is NOT used here —
        # its contents are instead installed directly via requirements.txt.
        # ------------------------------------------------------------------
        spectra_validator = lambda_.DockerImageFunction(
            self,
            "SpectraValidator",
            function_name="nova-cat-spectra-validator",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(self._services_root, "spectra_validator")
            ),
            description=(
                "Selects FITS profile, normalizes spectral arrays to IVOA-aligned model, "
                "and records validation outcomes. Used by: acquire_and_validate_spectra. "
                "Container-bundled: astropy + numpy require Docker deployment."
            ),
            memory_size=512,
            timeout=cdk.Duration.minutes(5),
            environment=shared_env,
            log_retention=_LOG_RETENTION,
            tracing=lambda_.Tracing.ACTIVE,
        )
        self._functions["spectra_validator"] = spectra_validator

        # ------------------------------------------------------------------
        # archive_resolver — DockerImageFunction
        #
        # astroquery (which wraps astropy) includes compiled C extensions.
        # Container deployment is required for the same reason as spectra_validator.
        # ------------------------------------------------------------------
        archive_resolver = lambda_.DockerImageFunction(
            self,
            "ArchiveResolver",
            function_name="nova-cat-archive-resolver",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(self._services_root, "archive_resolver")
            ),
            description=(
                "Queries external public archives (SIMBAD, TNS) to resolve a candidate name "
                "to coordinates and nova classification. "
                "Handles ResolveCandidateAgainstPublicArchives. Used by: initialize_nova. "
                "Container-bundled: astropy/astroquery require Docker deployment."
            ),
            memory_size=_DEFAULT_MEMORY_MB,
            timeout=cdk.Duration.seconds(90),
            environment=shared_env,
            log_retention=_LOG_RETENTION,
            tracing=lambda_.Tracing.ACTIVE,
        )
        self._functions["archive_resolver"] = archive_resolver

        # ------------------------------------------------------------------
        # spectra_discoverer — DockerImageFunction
        #
        # Provider adapters (e.g. eso.py) use astropy for coordinate handling.
        # ------------------------------------------------------------------
        spectra_discoverer = lambda_.DockerImageFunction(
            self,
            "SpectraDiscoverer",
            function_name="nova-cat-spectra-discoverer",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(self._services_root, "spectra_discoverer")
            ),
            description=(
                "Dispatches provider discovery adapters, assigns stable data_product_id values, "
                "persists DataProduct stubs. Used by: discover_spectra_products. "
                "Container-bundled: astropy/astroquery require Docker deployment."
            ),
            memory_size=_DEFAULT_MEMORY_MB,
            timeout=cdk.Duration.seconds(60),
            environment=shared_env,
            log_retention=_LOG_RETENTION,
            tracing=lambda_.Tracing.ACTIVE,
        )
        self._functions["spectra_discoverer"] = spectra_discoverer

        # ------------------------------------------------------------------
        # IAM grants — least-privilege
        # ------------------------------------------------------------------
        self._grant_permissions(
            table, private_bucket, public_site_bucket, quarantine_topic, ads_secret
        )

        # ------------------------------------------------------------------
        # Expose each function as a named attribute for Step Functions wiring
        # ------------------------------------------------------------------
        for name, fn in self._functions.items():
            setattr(self, name, fn)

        # ADS secret name injected only into reference_manager (the sole consumer)
        self._functions["reference_manager"].add_environment(
            "ADS_SECRET_NAME", ads_secret.secret_name
        )

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
        """
        table.grant_read_write_data(self._functions["nova_resolver"])

        table.grant_write_data(self._functions["job_run_manager"])

        table.grant_read_write_data(self._functions["idempotency_guard"])

        table.grant_read_data(self._functions["workflow_launcher"])
        quarantine_topic.grant_publish(self._functions["workflow_launcher"])

        table.grant_read_write_data(self._functions["reference_manager"])
        ads_secret.grant_read(self._functions["reference_manager"])

        table.grant_read_write_data(self._functions["spectra_discoverer"])
        quarantine_topic.grant_publish(self._functions["spectra_discoverer"])

        table.grant_read_write_data(self._functions["spectra_acquirer"])
        private_bucket.grant_write(
            self._functions["spectra_acquirer"],
            "raw/spectra/*",
        )

        table.grant_read_write_data(self._functions["spectra_validator"])
        private_bucket.grant_read(
            self._functions["spectra_validator"],
            "raw/spectra/*",
        )
        private_bucket.grant_write(
            self._functions["spectra_validator"],
            "derived/spectra/*",
        )
        private_bucket.grant_write(
            self._functions["spectra_validator"],
            "quarantine/spectra/*",
        )

        table.grant_read_write_data(self._functions["photometry_ingestor"])
        private_bucket.grant_read(
            self._functions["photometry_ingestor"],
            "raw/photometry/*",
        )
        private_bucket.grant_read_write(
            self._functions["photometry_ingestor"],
            "derived/photometry/*",
        )

        table.grant_write_data(self._functions["quarantine_handler"])
        private_bucket.grant_write(
            self._functions["quarantine_handler"],
            "quarantine/*",
        )
        quarantine_topic.grant_publish(self._functions["quarantine_handler"])

        table.grant_read_write_data(self._functions["name_reconciler"])


def _to_pascal(snake: str) -> str:
    """Convert snake_case to PascalCase for CloudFormation logical IDs."""
    return "".join(word.capitalize() for word in snake.split("_"))
