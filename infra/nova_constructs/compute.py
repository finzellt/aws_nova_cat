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
import shutil
import subprocess
from dataclasses import dataclass

import aws_cdk as cdk
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_secretsmanager as secretsmanager
import aws_cdk.aws_sns as sns
import jsii
from constructs import Construct


@jsii.implements(cdk.ILocalBundling)
class _LocalPipBundler:
    """
    Local bundling implementation for zip-bundled Lambda functions.

    CDK tries local bundling before Docker. If this returns True, Docker is
    never invoked — which is essential in CI environments (act, GitHub Actions)
    where Docker-in-Docker is unavailable.

    Strategy:
      1. Copy service source files to output_dir
      2. Copy contracts/ package from repo root (shared Pydantic models)
      3. Run pip install -r requirements.txt into output_dir
         (targeting manylinux2014_x86_64 / cp311 to match the Lambda runtime)
      4. Return True on success, False to fall back to Docker
    """

    def __init__(self, service_path: str) -> None:
        self._service_path = service_path

    @staticmethod
    def _ignore_junk(directory: str, contents: list[str]) -> set[str]:
        """Exclude __pycache__, .DS_Store, and fixtures from Lambda bundles."""
        return {c for c in contents if c in {"__pycache__", ".DS_Store", "fixtures"}}

    def try_bundle(self, output_dir: str, *, image: object = None, **kwargs: object) -> bool:
        try:
            # Copy all source files first
            for item in os.listdir(self._service_path):
                if item in {"__pycache__", ".DS_Store"}:
                    continue
                src = os.path.join(self._service_path, item)
                dst = os.path.join(output_dir, item)
                if os.path.isdir(src):
                    shutil.copytree(src, dst, dirs_exist_ok=True, ignore=self._ignore_junk)
                else:
                    shutil.copy2(src, dst)

            # Copy contracts/ package (shared Pydantic models — repo root
            # sibling of services/).  Required by artifact_coordinator and
            # artifact_finalizer which import from contracts.models at runtime.
            contracts_dir = os.path.normpath(os.path.join(self._service_path, "../../contracts"))
            if os.path.exists(contracts_dir):
                shutil.copytree(
                    contracts_dir,
                    os.path.join(output_dir, "contracts"),
                    dirs_exist_ok=True,
                    ignore=self._ignore_junk,
                )

            # Install dependencies if requirements.txt exists
            req_file = os.path.join(self._service_path, "requirements.txt")
            if os.path.exists(req_file):
                subprocess.run(
                    [
                        "pip",
                        "install",
                        "-r",
                        req_file,
                        "-t",
                        output_dir,
                        "--quiet",
                        "--platform",
                        "manylinux2014_x86_64",
                        "--only-binary",
                        ":all:",
                        "--implementation",
                        "cp",
                        "--python-version",
                        "311",
                    ],
                    check=True,
                )
            return True
        except Exception:  # noqa: BLE001
            return False


@jsii.implements(cdk.ILocalBundling)
class _PackagedLocalBundler:
    """
    Local bundling for services whose handler imports from a same-named package.

    Some services (e.g. ticket_parser) have the structure:
      services/<name>/handler.py        — Lambda entry point
      services/<name>/<other>.py        — importable as <name>.<other> in tests
                                          (because services/ is in sys.path)

    When deployed flat to /var/task/, the sub-module import fails because there
    is no <name>/ directory.  This bundler creates the correct layout:

      /asset-output/handler.py          — Lambda entry point at root
      /asset-output/<package_name>/     — Python package directory
        <all non-handler .py files>

    So "from <package_name>.parser import ..." resolves to
    /var/task/<package_name>/parser.py in the Lambda runtime.
    """

    def __init__(self, service_path: str, package_name: str) -> None:
        self._service_path = service_path
        self._package_name = package_name

    def try_bundle(self, output_dir: str, *, image: object = None, **kwargs: object) -> bool:
        try:
            pkg_dir = os.path.join(output_dir, self._package_name)
            os.makedirs(pkg_dir, exist_ok=True)

            for item in os.listdir(self._service_path):
                if item in {"__pycache__", ".DS_Store", "requirements.txt"}:
                    continue
                src = os.path.join(self._service_path, item)
                if item == "handler.py":
                    shutil.copy2(src, output_dir)
                elif os.path.isdir(src):
                    shutil.copytree(
                        src,
                        os.path.join(pkg_dir, item),
                        dirs_exist_ok=True,
                        ignore=_LocalPipBundler._ignore_junk,
                    )
                else:
                    shutil.copy2(src, pkg_dir)

            # Copy contracts/ package (shared models, repo root sibling of services/)
            contracts_dir = os.path.normpath(os.path.join(self._service_path, "../../contracts"))
            if os.path.exists(contracts_dir):
                shutil.copytree(
                    contracts_dir,
                    os.path.join(output_dir, "contracts"),
                    dirs_exist_ok=True,
                    ignore=_LocalPipBundler._ignore_junk,
                )

            req_file = os.path.join(self._service_path, "requirements.txt")
            if os.path.exists(req_file):
                subprocess.run(
                    [
                        "pip",
                        "install",
                        "-r",
                        req_file,
                        "-t",
                        output_dir,
                        "--quiet",
                        "--platform",
                        "manylinux2014_x86_64",
                        "--only-binary",
                        ":all:",
                        "--implementation",
                        "cp",
                        "--python-version",
                        "311",
                    ],
                    check=True,
                )
            return True
        except Exception:  # noqa: BLE001
            return False


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
    package_name: str | None = None  # if set, use _PackagedLocalBundler


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
            "Writes JobRun operational records. "
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
            "Starts downstream Step Functions executions. "
            "Used by: initialize_nova, ingest_new_nova, discover_spectra_products."
        ),
        timeout=cdk.Duration.seconds(300),  # fan-out: 8s delay × up to ~30 batches
    ),
    "reference_manager": _FunctionSpec(
        service_dir="reference_manager",
        description=(
            "Fetches ADS references, upserts Reference entities, links NovaReference "
            "records, and computes discovery_date. Used by: refresh_references."
        ),
        timeout=cdk.Duration.seconds(180),  # ADS API + sequential per-candidate reconcile
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
        package_name="ticket_parser",
    ),
    "nova_resolver_ticket": _FunctionSpec(
        service_dir="nova_resolver_ticket",
        description=(
            "Resolves OBJECT NAME to nova_id via NameMapping. Invokes initialize_nova "
            "if absent (StartSyncExecution — Express workflow). Raises UNRESOLVABLE_OBJECT_NAME "
            "or IDENTITY_AMBIGUITY for quarantine outcomes. "
            "Handles ResolveNova. Used by: ingest_ticket."
        ),
        timeout=cdk.Duration.seconds(120),  # initialize_nova may take up to ~60s
    ),
    # ------------------------------------------------------------------
    # regenerate_artifacts workflow functions (Epic 2)
    # ------------------------------------------------------------------
    "artifact_coordinator": _FunctionSpec(
        service_dir="artifact_coordinator",
        description=(
            "Sweep coordinator: queries WORKQUEUE, builds per-nova manifests, "
            "persists RegenBatchPlan, launches regenerate_artifacts workflow. "
            "Invoked by EventBridge (6h cron) or manually."
        ),
        timeout=cdk.Duration.seconds(60),  # paginated WORKQUEUE query + plan write + SFn start
    ),
    "artifact_finalizer": _FunctionSpec(
        service_dir="artifact_finalizer",
        description=(
            "Commits succeeded novae: deletes consumed WorkItems, writes observation "
            "counts to Nova items, updates RegenBatchPlan status. "
            "Handles UpdatePlanInProgress, Finalize, FailHandler. "
            "Used by: regenerate_artifacts."
        ),
        timeout=cdk.Duration.seconds(300),  # batch WorkItem deletes for large sweeps
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
    # ingest_ticket workflow (Chunk 5a)
    ticket_parser: lambda_.Function
    nova_resolver_ticket: lambda_.Function
    ticket_ingestor: lambda_.DockerImageFunction
    # regenerate_artifacts workflow (Epic 2)
    artifact_coordinator: lambda_.Function
    artifact_finalizer: lambda_.Function

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        table: dynamodb.Table,
        photometry_table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
        ads_secret: secretsmanager.ISecret,
        services_root: str = "../../services",
        env_prefix: str = "nova-cat",
    ) -> None:
        super().__init__(scope, construct_id)

        self._env_prefix = env_prefix

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
            layer_version_name=f"{env_prefix}-nova-common",
            code=lambda_.Code.from_asset(os.path.join(self._services_root, "nova_common_layer")),
            compatible_runtimes=[_PYTHON_RUNTIME],
            description="Nova Cat shared utilities: Powertools Logger, Tracer, configure_logging",
        )

        # ------------------------------------------------------------------
        # Build all zip-bundled Lambda functions from specs
        # ------------------------------------------------------------------
        self._functions: dict[str, lambda_.Function | lambda_.DockerImageFunction] = {}

        # Absolute path to contracts/ — bundled into every Lambda so that
        # services importing from contracts.models resolve at runtime.
        contracts_abs = os.path.normpath(os.path.join(self._services_root, "..", "contracts"))
        # Docker copy snippet shared by both bundler branches.
        _docker_contracts = (
            "if [ -d /contracts ]; then cp -r /contracts /asset-output/contracts; fi"
        )

        for name, spec in _FUNCTION_SPECS.items():
            service_path = os.path.join(self._services_root, spec.service_dir)
            if spec.package_name:
                # Services whose handler.py imports from a same-named package
                # (e.g. ticket_parser.parser) need a nested directory layout in
                # the Lambda deployment.  _PackagedLocalBundler copies handler.py
                # to the root and all other module files into <package_name>/.
                pkg = spec.package_name
                docker_cmd = (
                    f"mkdir -p /asset-output/{pkg} && "
                    f"cp handler.py /asset-output/ && "
                    f'for f in *.py; do [ "$f" != handler.py ] && '
                    f'cp "$f" /asset-output/{pkg}/; done && '
                    f"{_docker_contracts} && "
                    f"if [ -f requirements.txt ]; then "
                    f"pip install -r requirements.txt -t /asset-output --quiet; fi"
                )
                local_bundler: cdk.ILocalBundling = _PackagedLocalBundler(service_path, pkg)
            else:
                docker_cmd = (
                    "pip install -r requirements.txt -t /asset-output && "
                    "cp -r . /asset-output && "
                    f"{_docker_contracts}"
                )
                local_bundler = _LocalPipBundler(service_path)

            fn = lambda_.Function(
                self,
                _to_pascal(name),
                function_name=f"{env_prefix}-{name.replace('_', '-')}",
                runtime=_PYTHON_RUNTIME,
                handler="handler.handle",
                code=lambda_.Code.from_asset(
                    service_path,
                    asset_hash_type=cdk.AssetHashType.SOURCE,
                    bundling=cdk.BundlingOptions(
                        image=_PYTHON_RUNTIME.bundling_image,
                        command=["bash", "-c", docker_cmd],
                        local=local_bundler,
                        volumes=[
                            cdk.DockerVolume(
                                host_path=contracts_abs,
                                container_path="/contracts",
                            ),
                        ],
                    ),
                ),
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
            function_name=f"{env_prefix}-spectra-validator",
            architecture=lambda_.Architecture.ARM_64,
            code=lambda_.DockerImageCode.from_image_asset(
                self._services_root,
                file="spectra_validator/Dockerfile",
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
        # ADR-031 Decision 4: web-ready CSV uploads to derived/spectra/
        private_bucket.grant_write(
            spectra_validator,
            "derived/*",
        )

        # ------------------------------------------------------------------
        # archive_resolver — DockerImageFunction
        #
        # astroquery (which wraps astropy) includes compiled C extensions.
        # Container deployment is required for the same reason as spectra_validator.
        # ------------------------------------------------------------------
        archive_resolver = lambda_.DockerImageFunction(
            self,
            "ArchiveResolver",
            function_name=f"{env_prefix}-archive-resolver",
            architecture=lambda_.Architecture.ARM_64,
            code=lambda_.DockerImageCode.from_image_asset(
                self._services_root,  # context = services/
                file="archive_resolver/Dockerfile",  # path relative to context
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
            function_name=f"{env_prefix}-spectra-discoverer",
            architecture=lambda_.Architecture.ARM_64,
            code=lambda_.DockerImageCode.from_image_asset(
                self._services_root,
                file="spectra_discoverer/Dockerfile",
            ),
            description=(
                "Dispatches provider discovery adapters, assigns stable data_product_id values, "
                "persists DataProduct stubs. Used by: discover_spectra_products. "
                "Container-bundled: astropy/astroquery require Docker deployment."
            ),
            memory_size=_DEFAULT_MEMORY_MB,
            timeout=cdk.Duration.seconds(120),
            environment=shared_env,
            log_retention=_LOG_RETENTION,
            tracing=lambda_.Tracing.ACTIVE,
        )
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
                "Ingests photometry (CSV → PhotometryRow DDB items) and spectra "
                "(CSV → FITS → S3 + DDB refs) for ticket-driven workflows. "
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
            table,
            photometry_table,
            private_bucket,
            public_site_bucket,
            quarantine_topic,
            ads_secret,
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
            "DIAGNOSTICS_BUCKET",
            private_bucket.bucket_name,
        )

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
        """
        table.grant_read_write_data(self._functions["nova_resolver"])

        table.grant_write_data(self._functions["job_run_manager"])

        table.grant_read_write_data(self._functions["idempotency_guard"])

        table.grant_read_write_data(self._functions["workflow_launcher"])
        quarantine_topic.grant_publish(self._functions["workflow_launcher"])

        table.grant_read_write_data(self._functions["reference_manager"])
        ads_secret.grant_read(self._functions["reference_manager"])

        table.grant_read_write_data(self._functions["spectra_discoverer"])
        quarantine_topic.grant_publish(self._functions["spectra_discoverer"])

        table.grant_read_write_data(self._functions["spectra_acquirer"])
        private_bucket.grant_write(
            self._functions["spectra_acquirer"],
            "raw/*",
        )

        table.grant_read_write_data(self._functions["spectra_validator"])
        private_bucket.grant_read(
            self._functions["spectra_validator"],
            "raw/*",
        )
        private_bucket.grant_write(
            self._functions["spectra_validator"],
            "derived/*",
        )
        private_bucket.grant_write(
            self._functions["spectra_validator"],
            "quarantine/spectra/*",
        )

        table.grant_read_write_data(self._functions["photometry_ingestor"])
        private_bucket.grant_read(
            self._functions["photometry_ingestor"],
            "raw/*",
        )
        private_bucket.grant_read_write(
            self._functions["photometry_ingestor"],
            "derived/*",
        )

        table.grant_write_data(self._functions["quarantine_handler"])
        private_bucket.grant_write(
            self._functions["quarantine_handler"],
            "quarantine/*",
        )
        quarantine_topic.grant_publish(self._functions["quarantine_handler"])

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
        # ADR-031 Decision 4: web-ready CSV uploads to derived/spectra/
        private_bucket.grant_write(
            self._functions["ticket_ingestor"],
            "derived/*",
        )
        photometry_table.grant_write_data(self._functions["ticket_ingestor"])
        table.grant_read_write_data(self._functions["ticket_ingestor"])
        public_site_bucket.grant_write(
            self._functions["ticket_ingestor"],
            "raw/*",
        )

        # ------------------------------------------------------------------
        # regenerate_artifacts workflow grants (Epic 2)
        # ------------------------------------------------------------------

        # artifact_coordinator: reads WORKQUEUE + REGEN_PLAN partitions,
        # writes REGEN_PLAN items.  sfn:StartExecution on the
        # regenerate_artifacts state machine is granted in workflows.py.
        table.grant_read_write_data(self._functions["artifact_coordinator"])

        # artifact_finalizer: reads REGEN_PLAN (plan loading), deletes
        # WORKQUEUE items (batch_write_item), writes observation counts
        # to Nova items (PK=<nova_id>, SK=NOVA), updates REGEN_PLAN status.
        table.grant_read_write_data(self._functions["artifact_finalizer"])


def _to_pascal(snake: str) -> str:
    """Convert snake_case to PascalCase for CloudFormation logical IDs."""
    return "".join(word.capitalize() for word in snake.split("_"))
