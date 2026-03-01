"""
Nova Cat Compute Construct

Provisions all Lambda functions with:
  - Consistent runtime (Python 3.12)
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
  photometry_ingestor    — photometry file validation, parquet rebuild, metadata persist
  quarantine_handler     — quarantine persistence + SNS best-effort notification (all workflows)
  name_reconciler        — naming authority queries, reconciliation, alias updates
"""

from __future__ import annotations

from dataclasses import dataclass

import aws_cdk as cdk
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sns as sns
from constructs import Construct

# Default Lambda settings — tuned for Nova Cat's low-throughput, cost-aware profile.
# Acquisition Lambda gets a higher memory and longer timeout; others are conservative.
_DEFAULT_MEMORY_MB = 256
_DEFAULT_TIMEOUT = cdk.Duration.seconds(30)
_PYTHON_RUNTIME = lambda_.Runtime.PYTHON_3_12
_LOG_RETENTION = logs.RetentionDays.THREE_MONTHS


@dataclass
class _FunctionSpec:
    """Internal spec for each Lambda function."""

    service_dir: str  # directory under services/
    description: str
    memory_mb: int = _DEFAULT_MEMORY_MB
    timeout: cdk.Duration = _DEFAULT_TIMEOUT


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
            "Handles BeginJobRun, FinalizeJobRunSuccess, FinalizeJobRunFailed, "
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
    "archive_resolver": _FunctionSpec(
        service_dir="archive_resolver",
        description=(
            "Queries external public archives (SIMBAD, TNS) to resolve a candidate name "
            "to coordinates and nova classification. "
            "Handles ResolveCandidateAgainstPublicArchives. Used by: initialize_nova."
        ),
        timeout=cdk.Duration.seconds(90),  # External network calls; generous timeout
    ),
    "workflow_launcher": _FunctionSpec(
        service_dir="workflow_launcher",
        description=(
            "Starts downstream Step Functions executions and publishes SNS continuation events. "
            "Handles PublishIngestNewNova, LaunchRefreshReferences, "
            "LaunchDiscoverSpectraProducts, PublishAcquireAndValidateSpectraRequests. "
            "Used by: initialize_nova, ingest_new_nova, discover_spectra_products."
        ),
    ),
    "reference_manager": _FunctionSpec(
        service_dir="reference_manager",
        description=(
            "Fetches references from ADS, upserts Reference entities, links NovaReference "
            "records, and computes discovery_date. "
            "Handles FetchReferenceCandidates, NormalizeReference, UpsertReferenceEntity, "
            "LinkNovaReference, ComputeDiscoveryDate, UpsertDiscoveryDateMetadata. "
            "Used by: refresh_references."
        ),
        timeout=cdk.Duration.seconds(90),  # ADS API calls
    ),
    "spectra_discoverer": _FunctionSpec(
        service_dir="spectra_discoverer",
        description=(
            "Dispatches provider-specific discovery adapters, normalizes results, assigns "
            "stable data_product_id values, persists DataProduct stubs, and publishes "
            "AcquireAndValidateSpectra continuation events. "
            "Handles QueryProviderForProducts, NormalizeProviderProducts, "
            "DeduplicateAndAssignDataProductIds, PersistDataProductMetadata. "
            "Used by: discover_spectra_products."
        ),
        timeout=cdk.Duration.seconds(60),
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
    "spectra_validator": _FunctionSpec(
        service_dir="spectra_validator",
        description=(
            "Selects FITS profile, normalizes spectral arrays to canonical IVOA-aligned model, "
            "runs domain sanity checks, and records validation outcomes. "
            "Handles ValidateBytes, RecordValidationResult, RecordDuplicateLinkage, "
            "CheckOperationalStatus. Used by: acquire_and_validate_spectra."
        ),
        memory_mb=512,  # FITS parsing is memory-intensive
        timeout=cdk.Duration.minutes(5),  # Per execution-governance.md validation timeout
    ),
    "photometry_ingestor": _FunctionSpec(
        service_dir="photometry_ingestor",
        description=(
            "Validates an uploaded photometry file, rebuilds the canonical per-nova "
            "photometry_table.parquet, and persists ingestion summary metadata. "
            "Handles ValidatePhotometry, RebuildPhotometryTable, PersistPhotometryMetadata, "
            "CheckOperationalStatus. Used by: ingest_photometry."
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
    for use by the Step Functions construct in the next epic.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
        services_root: str = "../../services",
    ) -> None:
        super().__init__(scope, construct_id)

        # ------------------------------------------------------------------
        # Shared environment variables injected into every Lambda.
        # Functions read these rather than hardcoding resource names.
        # ------------------------------------------------------------------
        shared_env = {
            "NOVA_CAT_TABLE_NAME": table.table_name,
            "NOVA_CAT_PRIVATE_BUCKET": private_bucket.bucket_name,
            "NOVA_CAT_PUBLIC_SITE_BUCKET": public_site_bucket.bucket_name,
            "NOVA_CAT_QUARANTINE_TOPIC_ARN": quarantine_topic.topic_arn,
            "LOG_LEVEL": "INFO",
            # AWS Lambda Powertools service name for structured logging
            "POWERTOOLS_SERVICE_NAME": "nova-cat",
        }

        # ------------------------------------------------------------------
        # Build all Lambda functions from specs
        # ------------------------------------------------------------------
        self._functions: dict[str, lambda_.Function] = {}

        for name, spec in _FUNCTION_SPECS.items():
            fn = lambda_.Function(
                self,
                # Logical ID: PascalCase for CloudFormation readability
                _to_pascal(name),
                function_name=f"nova-cat-{name.replace('_', '-')}",
                runtime=_PYTHON_RUNTIME,
                handler="handler.handle",
                code=lambda_.Code.from_asset(f"{services_root}/{spec.service_dir}"),
                description=spec.description,
                memory_size=spec.memory_mb,
                timeout=spec.timeout,
                environment=shared_env,
                log_retention=_LOG_RETENTION,
                tracing=lambda_.Tracing.ACTIVE,  # X-Ray tracing; low cost at this scale
            )
            self._functions[name] = fn

        # ------------------------------------------------------------------
        # IAM grants — least-privilege, derived from dynamodb-access-patterns.md
        # ------------------------------------------------------------------
        self._grant_permissions(table, private_bucket, public_site_bucket, quarantine_topic)

        # ------------------------------------------------------------------
        # Expose each function as a named attribute for Step Functions wiring
        # ------------------------------------------------------------------
        for name, fn in self._functions.items():
            setattr(self, name, fn)

    def _grant_permissions(
        self,
        table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
    ) -> None:
        """
        Grants least-privilege IAM permissions to each Lambda function.

        Grants are derived directly from the access patterns in
        dynamodb-access-patterns.md and the S3 layout in s3-layout.md.
        """

        # nova_resolver: reads NameMapping + coordinate scan, writes Nova + NameMapping
        table.grant_read_write_data(self._functions["nova_resolver"])

        # job_run_manager: writes JobRun and Attempt items (per-nova partition)
        table.grant_write_data(self._functions["job_run_manager"])

        # idempotency_guard: conditional put/get on idempotency lock items
        # (stored as per-nova JOBRUN# items or a dedicated LOCK# prefix)
        table.grant_read_write_data(self._functions["idempotency_guard"])

        # archive_resolver: no DynamoDB access — only external HTTP calls
        # (no grants needed beyond execution role basics)

        # workflow_launcher: reads Nova status, writes product stubs, publishes SNS/SFN
        # SFN start-execution permissions are added by the Step Functions construct
        # when state machines are defined; grant SNS here.
        table.grant_read_data(self._functions["workflow_launcher"])
        quarantine_topic.grant_publish(self._functions["workflow_launcher"])

        # reference_manager: reads + writes REF# and NOVAREF# items, updates Nova.discovery_date
        table.grant_read_write_data(self._functions["reference_manager"])

        # spectra_discoverer: reads LocatorAlias, writes DataProduct stubs + LocatorAlias
        table.grant_read_write_data(self._functions["spectra_discoverer"])
        # Publishes AcquireAndValidateSpectra events (via SNS or SFN start-execution)
        quarantine_topic.grant_publish(self._functions["spectra_discoverer"])

        # spectra_acquirer: reads DataProduct metadata, writes raw bytes to S3,
        # updates DataProduct cooldown fields
        table.grant_read_write_data(self._functions["spectra_acquirer"])
        private_bucket.grant_write(
            self._functions["spectra_acquirer"],
            "raw/spectra/*",
        )

        # spectra_validator: reads DataProduct, writes validation results + derived artifacts
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

        # photometry_ingestor: reads/writes DataProduct (PHOTOMETRY_TABLE),
        # reads raw upload, writes canonical parquet + snapshots
        table.grant_read_write_data(self._functions["photometry_ingestor"])
        private_bucket.grant_read(
            self._functions["photometry_ingestor"],
            "raw/photometry/*",
        )
        private_bucket.grant_read_write(
            self._functions["photometry_ingestor"],
            "derived/photometry/*",
        )

        # quarantine_handler: writes quarantine status to DynamoDB,
        # writes quarantine context to S3, publishes to SNS
        table.grant_write_data(self._functions["quarantine_handler"])
        private_bucket.grant_write(
            self._functions["quarantine_handler"],
            "quarantine/*",
        )
        quarantine_topic.grant_publish(self._functions["quarantine_handler"])

        # name_reconciler: reads Nova naming state, writes NameMapping updates
        table.grant_read_write_data(self._functions["name_reconciler"])


def _to_pascal(snake: str) -> str:
    """Convert snake_case to PascalCase for CloudFormation logical IDs."""
    return "".join(word.capitalize() for word in snake.split("_"))
