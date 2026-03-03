## 14. Infrastructure (CDK)

> 📄 Source files live at `infra/` (entry point, stack, lint) and `infra/nova_constructs/` (constructs).
> Single-stack MVP deployment. Stack designed to split into storage/compute/workflows stacks in future.
> Deploy: `cdk deploy -c account=<ID>` (dev) or `cdk deploy -c account=<ID> -c env=prod` (prod).

### 14.1 Entry Point & Stack
**`infra/app.py`** — CDK app; instantiates `NovaCatStack` in `us-east-1`; account from context.

**`infra/nova_cat/nova_cat_stack.py`** — `NovaCatStack`
- Composes `NovaCatStorage` → `NovaCatCompute` → `NovaCatWorkflows` in that order.
- `removal_policy`: `DESTROY` (dev) / `RETAIN` (prod) based on `-c env=prod` context flag.
- `enable_pitr`: True in prod only.
- Tags all resources: `Project=NovaCat`, `ManagedBy=CDK`, `Environment=dev|prod`.

**`infra/lint_stack.py`** — Pre-synth linter (run before mypy + cdk synth).
- Validates Lambda description lengths (≤256 chars) and function name lengths (≤64 chars).
- Reads `_FUNCTION_SPECS` directly from `nova_constructs/compute.py`.
- Exit 0 = all passed; exit 1 = failures.

---

### 14.2 `NovaCatStorage` ✅ Implemented
**`infra/nova_constructs/storage.py`**

| Resource | Physical name | Notes |
|---|---|---|
| DynamoDB table | `NovaCat` | PAY_PER_REQUEST; PK=String, SK=String |
| GSI | `EligibilityIndex` | GSI1PK/GSI1SK; projection=ALL |
| Private S3 bucket | CDK-generated | Versioned; S3-managed encryption; SSL enforced |
| Public site S3 bucket | CDK-generated | Not versioned; all public access blocked |
| SNS topic | `nova-cat-quarantine-notifications` | Shared across all workflows |

**DynamoDB design notes:**
- PITR disabled by default (cost); enabled in prod via `enable_pitr` param.
- GSI items removed from index by nulling `GSI1PK`/`GSI1SK` (no explicit delete needed).

**Private bucket lifecycle rules:**
- `quarantine/` → expire after 365 days (+ noncurrent versions after 30 days)
- `workflow-payloads/` → expire after 30 days

**Public bucket lifecycle rules:**
- `releases/` → expire after 730 days

**CFN outputs (export names):** `NovaCat-TableName`, `NovaCat-PrivateBucketName`,
`NovaCat-PublicSiteBucketName`, `NovaCat-QuarantineTopicArn`

---

### 14.3 `NovaCatCompute` ✅ Implemented
**`infra/nova_constructs/compute.py`**

Provisions all Lambda functions from `_FUNCTION_SPECS` dict (dataclass-driven).
All functions receive standard env vars: `NOVA_CAT_TABLE_NAME`, `NOVA_CAT_PRIVATE_BUCKET`,
`NOVA_CAT_PUBLIC_SITE_BUCKET`, `NOVA_CAT_QUARANTINE_TOPIC_ARN`, `POWERTOOLS_SERVICE_NAME=nova-cat`.

Function naming convention: `nova-cat-<fn_name>` (underscores → hyphens).

| Attribute | Lambda | Runtime / notes |
|---|---|---|
| `archive_resolver` | `nova-cat-archive_resolver` | **Container image** (astropy/astroquery); not zip+layer |
| `idempotency_guard` | `nova-cat-idempotency_guard` | Zip + `nova_common` layer |
| `job_run_manager` | `nova-cat-job_run_manager` | Zip + `nova_common` layer |
| `nova_resolver` | `nova-cat-nova_resolver` | Zip + `nova_common` layer |
| `workflow_launcher` | `nova-cat-workflow_launcher` | Zip + `nova_common` layer |
| `quarantine_handler` | `nova-cat-quarantine_handler` | Zip + `nova_common` layer |
| `reference_manager` | `nova-cat-reference_manager` | Zip + `nova_common` layer |
| `spectra_acquirer` | `nova-cat-spectra_acquirer` | Zip + `nova_common` layer |
| `spectra_discoverer` | `nova-cat-spectra_discoverer` | Zip + `nova_common` layer |
| `spectra_validator` | `nova-cat-spectra_validator` | Zip + `nova_common` layer |
| `photometry_ingestor` | `nova-cat-photometry_ingestor` | Zip + `nova_common` layer |
| `name_reconciler` | `nova-cat-name_reconciler` | Zip + `nova_common` layer |

IAM grants (per function, least-privilege):
- DynamoDB table: read/write
- Private S3 bucket: read/write
- SNS quarantine topic: publish

---

### 14.4 `NovaCatWorkflows` ⚙️ Partially implemented
**`infra/nova_constructs/workflows.py`**

Provisions Step Functions state machines from ASL JSON files under `infra/workflows/`.
Uses `CfnStateMachine` (L1) + `Fn::Sub` for Lambda ARN token substitution at deploy time
(not synth time). ASL files kept as plain JSON (spec artifact; not CDK L2 constructs).
All state machines: **Standard Workflows** (not Express) — exact-once semantics, full history.

IAM execution role per state machine: `lambda:InvokeFunction` scoped to only the Lambdas
that state machine actually calls + CloudWatch Logs delivery permissions.

| State machine | Status | ASL file | CFN export |
|---|---|---|---|
| `nova-cat-initialize-nova` | ✅ | `infra/workflows/initialize_nova.asl.json` | `NovaCat-InitializeNovaStateMachineArn` |
| all others | ⚙️ not yet provisioned | — | — |

**Token → Lambda mapping for `initialize_nova`:**

| ASL token | Lambda |
|---|---|
| `BeginJobRunFunctionArn` | `job_run_manager` |
| `FinalizeJobRunSuccess/Failed/QuarantinedFunctionArn` | `job_run_manager` |
| `AcquireIdempotencyLockFunctionArn` | `idempotency_guard` |
| `NormalizeCandidateName/CheckExistingNovaByName/ByCoordinates/CreateNovaId/UpsertMinimalNovaMetadata/UpsertAliasForExistingNovaFunctionArn` | `nova_resolver` |
| `ResolveCandidateAgainstPublicArchivesFunctionArn` | `archive_resolver` |
| `PublishIngestNewNovaFunctionArn` | `workflow_launcher` |
| `QuarantineHandlerFunctionArn` / `TerminalFailHandlerFunctionArn` | `quarantine_handler` |
