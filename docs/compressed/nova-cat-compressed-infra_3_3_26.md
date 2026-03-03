# Nova Cat — Compressed CDK Infrastructure

---

## STACK STRUCTURE

```
app.py
  └── NovaCatStack ("NovaCat", region=us-east-1, account from context)
        ├── NovaCatStorage ("Storage")
        ├── NovaCatCompute ("Compute", receives storage resources)
        └── NovaCatWorkflows ("Workflows", receives compute)

Tags on all resources: Project=NovaCat, ManagedBy=CDK, Environment=prod|dev
```

**Env context keys**: `account` (required), `env` (optional; `"prod"` enables RETAIN + PITR)
- `env=prod` → `RemovalPolicy.RETAIN`, `enable_pitr=True`
- default → `RemovalPolicy.DESTROY`, PITR off

**Deploy**: `cdk deploy -c account=<ID>` / `cdk deploy -c account=<ID> -c env=prod`

---

## NovaCatStorage

**DynamoDB table** `NovaCat`:
- `PK` (String) + `SK` (String); PAY_PER_REQUEST; PITR env-dependent; removal_policy env-dependent
- **GSI1 `EligibilityIndex`**: `GSI1PK` (String) + `GSI1SK` (String); projection=ALL
  - ALL projection because eligibility queries need cooldown fields; avoids extra reads on hot acquisition path
  - Items no longer eligible have GSI1PK/SK set to null → removed from index automatically

**S3 private bucket** (`bucket_name=None` = CDK-generated):
- versioned=True (safety net; not relied on for app semantics), S3_MANAGED encryption, BLOCK_ALL public access, enforce_ssl=True
- Lifecycle: `quarantine/` → expire 365d (+ noncurrent 30d); `workflow-payloads/` → expire 30d

**S3 public site bucket** (`bucket_name=None`):
- versioned=False (releases are immutable by convention), S3_MANAGED, BLOCK_ALL, enforce_ssl=True
- Lifecycle: `releases/` → expire 730d
- Public access via future CloudFront OAC (bucket policy); not wired yet

**SNS quarantine topic** `nova-cat-quarantine-notifications`:
- One shared topic for all workflows; workflow name + reason code in message body for subscriber filtering

**CfnOutputs** (with export names): `NovaCat-TableName`, `NovaCat-PrivateBucketName`, `NovaCat-PublicSiteBucketName`, `NovaCat-QuarantineTopicArn`

---

## NovaCatCompute

**Runtime**: Python 3.11. **Log retention**: 3 months. **Tracing**: ACTIVE (X-Ray). **Handler**: `handler.handle`.
**Function name pattern**: `nova-cat-{name.replace('_', '-')}`. **CloudFormation logical ID**: PascalCase of name.

**Shared env vars** (all Lambdas):
`NOVA_CAT_TABLE_NAME`, `NOVA_CAT_PRIVATE_BUCKET`, `NOVA_CAT_PUBLIC_SITE_BUCKET`, `NOVA_CAT_QUARANTINE_TOPIC_ARN`, `LOG_LEVEL=INFO`, `POWERTOOLS_SERVICE_NAME=nova-cat`

**`nova_common` Lambda Layer** (`nova-cat-nova-common`):
- Asset: `services/nova_common_layer/` (structure: `python/nova_common/...` → unpacked to `/opt/python/nova_common/`)
- Attached to all functions

### Lambda Function Inventory

| Name | memory | timeout | Notes |
|---|---|---|---|
| `nova_resolver` | 256 MB | 30s | |
| `job_run_manager` | 256 MB | 30s | |
| `idempotency_guard` | 256 MB | 30s | |
| `archive_resolver` | 256 MB | **90s** | External network (SIMBAD, TNS) |
| `workflow_launcher` | 256 MB | 30s | |
| `reference_manager` | 256 MB | **90s** | ADS API calls |
| `spectra_discoverer` | 256 MB | **60s** | |
| `spectra_acquirer` | **512 MB** | **15 min** | In-memory FITS/ZIP; per execution-governance.md |
| `spectra_validator` | **512 MB** | **5 min** | FITS parsing memory-intensive |
| `photometry_ingestor` | **512 MB** | **5 min** | Parquet rebuild |
| `quarantine_handler` | 256 MB | 30s | |
| `name_reconciler` | 256 MB | **90s** | External authority queries |

### IAM Grants (least-privilege)

| Lambda | DynamoDB | S3 | SNS |
|---|---|---|---|
| `nova_resolver` | read_write | — | — |
| `job_run_manager` | **write only** | — | — |
| `idempotency_guard` | read_write | — | — |
| `archive_resolver` | — | — | — |
| `workflow_launcher` | **read only** | — | quarantine publish |
| `reference_manager` | read_write | — | — |
| `spectra_discoverer` | read_write | — | quarantine publish |
| `spectra_acquirer` | read_write | write `raw/spectra/*` | — |
| `spectra_validator` | read_write | read `raw/spectra/*`, write `derived/spectra/*` + `quarantine/spectra/*` | — |
| `photometry_ingestor` | read_write | read `raw/photometry/*`, read_write `derived/photometry/*` | — |
| `quarantine_handler` | **write only** | write `quarantine/*` | quarantine publish |
| `name_reconciler` | read_write | — | — |

> `workflow_launcher` SFN `states:StartExecution` grant is added by `NovaCatWorkflows` (not here), because NovaCatCompute has no knowledge of SFN.

Functions exposed as named attributes (`self.nova_resolver`, etc.) for SFN wiring.

**Pre-synth linter** (`lint_stack.py`): validates Lambda description ≤256 chars and function name ≤64 chars against `_FUNCTION_SPECS`. Exit 0 = pass, 1 = fail. Run before mypy + cdk synth in CI.

---

## NovaCatWorkflows

**Pattern**: ASL JSON files under `infra/workflows/` with `${Token}` placeholders. CDK uses `CfnStateMachine` (L1, not L2) + `definition_string=Fn.sub(asl, substitutions)` to resolve Lambda ARNs at deploy time. All state machines: `STANDARD` type (exact-once, unlimited duration, full execution history).

**IAM execution role per state machine**: `states.amazonaws.com` principal; `lambda:InvokeFunction` scoped to only the Lambdas that workflow actually calls (not wildcard); CloudWatch Logs delivery permissions (on `*`).

**`workflow_launcher` SFN grant**: `states:StartExecution` on `nova-cat-*` (wildcard on prefix, not individual ARNs — breaks CDK dependency cycle while remaining meaningfully scoped).

**ARN construction** for `workflow_launcher` env vars uses `stack.format_arn(service="states", resource="stateMachine", resource_name="nova-cat-{name}", arn_format=COLON_RESOURCE_NAME)` — avoids cycle between state machine role and Lambda env var.

### State Machines

**Provisioning order**: placeholder stubs first (so ARNs exist for `workflow_launcher` env vars), then `ingest_new_nova`, then `initialize_nova`.

#### `refresh_references` + `discover_spectra_products` (placeholder stubs)
- Single Fail state ASL; no substitutions; no invokable functions
- Provisioned early so their ARNs can be injected into `workflow_launcher`

#### `ingest_new_nova` (`nova-cat-ingest-new-nova`)
ASL tokens → Lambda:
- `BeginJobRunFunctionArn`, `FinalizeJobRunSuccessFunctionArn`, `FinalizeJobRunFailedFunctionArn` → `job_run_manager`
- `AcquireIdempotencyLockFunctionArn` → `idempotency_guard`
- `LaunchRefreshReferencesFunctionArn`, `LaunchDiscoverSpectraProductsFunctionArn` → `workflow_launcher`

Invokable: `job_run_manager`, `idempotency_guard`, `workflow_launcher`

#### `initialize_nova` (`nova-cat-initialize-nova`)
ASL tokens → Lambda:
- `BeginJobRunFunctionArn`, `FinalizeJobRunSuccess/Failed/QuarantinedFunctionArn` → `job_run_manager`
- `AcquireIdempotencyLockFunctionArn` → `idempotency_guard`
- `NormalizeCandidateNameFunctionArn`, `CheckExistingNovaByNameFunctionArn`, `CheckExistingNovaByCoordinatesFunctionArn`, `CreateNovaIdFunctionArn`, `UpsertMinimalNovaMetadataFunctionArn`, `UpsertAliasForExistingNovaFunctionArn` → `nova_resolver`
- `ResolveCandidateAgainstPublicArchivesFunctionArn` → `archive_resolver`
- `PublishIngestNewNovaFunctionArn` → `workflow_launcher`
- `QuarantineHandlerFunctionArn`, `TerminalFailHandlerFunctionArn` → `quarantine_handler`

Invokable: `job_run_manager`, `idempotency_guard`, `nova_resolver`, `archive_resolver`, `workflow_launcher`, `quarantine_handler`

**`workflow_launcher` env vars** (added after state machines exist):
- `INGEST_NEW_NOVA_STATE_MACHINE_ARN` → `nova-cat-ingest-new-nova`
- `REFRESH_REFERENCES_STATE_MACHINE_ARN` → `nova-cat-refresh-references`
- `DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN` → `nova-cat-discover-spectra-products`

**CfnOutputs**: `NovaCat-InitializeNovaStateMachineArn`, `NovaCat-IngestNewNovaStateMachineArn`, `NovaCat-RefreshReferencesStateMachineArn`, `NovaCat-DiscoverSpectraProductsStateMachineArn`
