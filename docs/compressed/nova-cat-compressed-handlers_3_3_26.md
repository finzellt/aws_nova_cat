# Nova Cat — Compressed Lambda Handlers

---

## SHARED PATTERNS

### Lambda dispatch pattern (all handlers)
Every handler uses the same entry point pattern:
```python
def handle(event, context):
    configure_logging(event)          # inject structured log fields
    task_name = event["task_name"]    # Step Functions state name
    return _TASK_HANDLERS[task_name](event, context)
```
`@tracer.capture_lambda_handler` on `handle`; `@tracer.capture_method` on each task fn.
Unknown `task_name` → `ValueError`.

### Standard env vars (all Lambdas, injected by CDK)
`NOVA_CAT_TABLE_NAME`, `NOVA_CAT_PRIVATE_BUCKET`, `NOVA_CAT_PUBLIC_SITE_BUCKET`, `NOVA_CAT_QUARANTINE_TOPIC_ARN`, `LOG_LEVEL` (default INFO), `POWERTOOLS_SERVICE_NAME` ("nova-cat")

### `nova_common` layer

**`nova_common.errors`** — three exception types; Step Functions matches on class `__name__`:
```python
class RetryableError(Exception): pass   # transient; SFN retries with backoff
class TerminalError(Exception): pass    # unrecoverable; → TerminalFailHandler
class QuarantineError(Exception): pass  # ambiguous/suspicious; → QuarantineHandler
```

**`nova_common.logging`** — single `Logger` instance (AWS Powertools). Call `configure_logging(event)` once per handler before any logging. Injects persistent fields: `correlation_id` (from `event["correlation_id"]` or `event["job_run"]["correlation_id"]`), `job_run_id`, `workflow_name`, `state_name` (from `task_name`), `candidate_name`, `nova_id`.

**`nova_common.tracing`** — single `Tracer()` instance. Service name from `POWERTOOLS_SERVICE_NAME`. Use `@tracer.capture_method` or `tracer.provider.in_subsegment(...)`.

---

## STUB-ONLY HANDLERS
The following handlers are fully scaffolded (dispatch table + env vars) but all tasks are `raise NotImplementedError`. No logic to compress.

| Lambda | Workflow | Tasks (all stubs) |
|---|---|---|
| `spectra_acquirer` | acquire_and_validate_spectra | AcquireArtifact |
| `spectra_discoverer` | discover_spectra_products | QueryProviderForProducts, NormalizeProviderProducts, DeduplicateAndAssignDataProductIds, PersistDataProductMetadata |
| `spectra_validator` | acquire_and_validate_spectra | CheckOperationalStatus, ValidateBytes, RecordValidationResult, RecordDuplicateLinkage |
| `reference_manager` | refresh_references | FetchReferenceCandidates, NormalizeReference, UpsertReferenceEntity, LinkNovaReference, ComputeDiscoveryDate, UpsertDiscoveryDateMetadata |
| `photometry_ingestor` | ingest_photometry | CheckOperationalStatus, ValidatePhotometry, RebuildPhotometryTable, PersistPhotometryMetadata |
| `name_reconciler` | name_check_and_reconcile | FetchCurrentNamingState, QueryAuthorityA, QueryAuthorityB, ReconcileNaming, ApplyNameUpdates, PublishNameReconciled |

---

## IMPLEMENTED HANDLERS

### `idempotency_guard`
**Task**: `AcquireIdempotencyLock`

**DDB item**: `PK="IDEMPOTENCY#<key>"`, `SK="LOCK"`. Fields: `idempotency_key`, `job_run_id`, `workflow_name`, `primary_id`, `acquired_at`, `ttl` (Unix epoch).

**Key format**: `{workflow_name}:{primary_id}:{schema_version}:{time_bucket}` where `time_bucket = YYYY-MM-DDTHH` (1-hour granularity).
- `initialize_nova` → `primary_id = normalized_candidate_name`
- all other workflows → `primary_id = nova_id`

**Logic**:
```python
# conditional put; attribute_not_exists(PK) — first writer wins
# ConditionalCheckFailedException → raise RetryableError (SFN retries)
# TTL = 24h from now
# Returns: {idempotency_key, acquired_at}
```

**Manual override** (stale lock): `aws dynamodb delete-item --table-name NovaCat --key '{"PK":{"S":"IDEMPOTENCY#<key>"},"SK":{"S":"LOCK"}}'`

---

### `job_run_manager`
**Tasks**: `BeginJobRun`, `FinalizeJobRunSuccess`, `FinalizeJobRunFailed`, `FinalizeJobRunQuarantined`

**DDB key**: `PK="WORKFLOW#<correlation_id>"` (pre-nova) or `"<nova_id>"` (post-creation); `SK="JOBRUN#<workflow_name>#<started_at>#<job_run_id>"`

> Pre-nova note: `initialize_nova` calls `BeginJobRun` before `nova_id` exists → `PK=WORKFLOW#<correlation_id>`. Workflows that end as `NOT_FOUND`/`NOT_A_CLASSICAL_NOVA` keep this partition permanently.

**`BeginJobRun`**:
- Generates `job_run_id = uuid4()`, echoes or generates `correlation_id`
- Writes JobRun with `status=RUNNING` via `put_item(ConditionExpression=Attr("PK").not_exists())`
- Stores `candidate_name` or `nova_id` (whichever present) for traceability
- Returns: `{job_run_id, correlation_id, started_at, pk, sk}`

**`FinalizeJobRunSuccess`**: `update_item` → `status=SUCCEEDED`, sets `outcome` (e.g. `CREATED_AND_LAUNCHED`, `EXISTS_AND_LAUNCHED`, `NOT_FOUND`, `NOT_A_CLASSICAL_NOVA`, `LAUNCHED`)

**`FinalizeJobRunFailed`**: `update_item` → `status=FAILED`, sets `error_type` (`error["Error"]`), `error_message` (truncated to 500 chars)

**`FinalizeJobRunQuarantined`**: `update_item` → `status=QUARANTINED`

---

### `nova_resolver`
**Tasks**: `NormalizeCandidateName`, `CheckExistingNovaByName`, `CheckExistingNovaByCoordinates`, `CreateNovaId`, `UpsertMinimalNovaMetadata`, `UpsertAliasForExistingNova`

**Angular separation thresholds**: `< 2"` = DUPLICATE, `2"–10"` = AMBIGUOUS, `> 10"` = NONE. Computed via **haversine formula** (no astropy).

```python
def _angular_separation_arcsec(ra1, dec1, ra2, dec2) -> float:
    # inputs in degrees; haversine for numerical stability at small separations
    a = sin(Δdec/2)² + cos(dec1)·cos(dec2)·sin(Δra/2)²
    return degrees(2·asin(√a)) * 3600
```

**`NormalizeCandidateName`**: lowercase + collapse whitespace (`re.sub(r"\s+", " ", s.strip().lower())`). Blank input → `TerminalError`. Returns `{normalized_candidate_name}`.

**`CheckExistingNovaByName`**: `query(PK="NAME#<normalized>", Limit=1)`. Returns `{exists: bool, nova_id?: str}`.

**`CheckExistingNovaByCoordinates`**: Full scan of all `SK="NOVA"` items projected to `{nova_id, ra_deg, dec_deg}`. At MVP scale (<1000 novae) scan is acceptable; future GSI on coord fields needed at larger scale. Computes min separation across all; returns `{match_outcome: DUPLICATE|AMBIGUOUS|NONE, min_sep_arcsec, matched_nova_id?}`.

**`CreateNovaId`**: `uuid4()` → `put_item` Nova stub with `status="PENDING"`. Returns `{nova_id}`.

**`UpsertMinimalNovaMetadata`**:
1. `update_item` Nova: sets `ra_deg`, `dec_deg` (as `Decimal`), `coord_epoch`, `coord_frame="ICRS"`, `resolver_source`, `status="ACTIVE"`, `aliases` list
2. `put_item` PRIMARY NameMapping (`name_kind="PRIMARY"`, `source="INGESTION"`)
3. For each SIMBAD alias: normalize → skip if blank or matches `normalized_candidate_name` → `put_item` ALIAS NameMapping (`name_kind="ALIAS"`, `source="SIMBAD"`)

> `aliases` is denormalized onto the Nova item so `refresh_references` can get all known names in one `get_item` call.

**`UpsertAliasForExistingNova`**: `put_item` ALIAS NameMapping for `matched_nova_id`. Called on DUPLICATE coordinate path. Returns `{nova_id}`.

---

### `quarantine_handler`
**Task**: `QuarantineHandler`
**Shared across all workflows** — updates existing JobRun record, then best-effort SNS.

**Logic**:
1. Compute `error_fingerprint = sha256(f"{reason_code}:{workflow_name}:{candidate_name}")[:12]`
2. `update_item` existing JobRun (`pk`/`sk` from `event["job_run"]`): sets `quarantine_reason_code`, `classification_reason`, `error_fingerprint`, `quarantined_at`, and optionally `extra_context` (e.g. `min_sep_arcsec`, float→Decimal)
3. Publish SNS (best-effort — exceptions caught+logged, MUST NOT fail workflow):
   - Topic: `NOVA_CAT_QUARANTINE_TOPIC_ARN`
   - Subject: `"[NovaCat] Quarantine: {workflow_name} — {reason_code}"`
   - Payload: `{workflow_name, primary_id, correlation_id, error_fingerprint, quarantine_reason_code, classification_reason}`
   - `primary_id = nova_id or candidate_name`

**Classification reasons** (human-readable strings keyed by reason code):
- `COORDINATE_AMBIGUITY`: "Candidate coordinates fall in the ambiguous 2"–10" separation band..."
- `OTHER`: "Nova classification is ambiguous — conflicting or inconclusive resolver results..."
- Fallback: "Quarantine triggered — see error_fingerprint for details."

Returns `{quarantine_reason_code, error_fingerprint, quarantined_at}`.

---

### `workflow_launcher`
**Tasks**: `PublishIngestNewNova`, `LaunchRefreshReferences`, `LaunchDiscoverSpectraProducts`, `PublishAcquireAndValidateSpectraRequests` (stub — NotImplementedError)

**Additional env vars**: `INGEST_NEW_NOVA_STATE_MACHINE_ARN`, `REFRESH_REFERENCES_STATE_MACHINE_ARN`, `DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN`

**All three implemented tasks** call shared `_start_execution(state_machine_arn, workflow_label, nova_id, correlation_id, job_run_id)`:

```python
def _start_execution(...):
    # Execution name: f"{nova_id}-{job_run_id[:8]}"  → 45 chars (limit=80)
    # Input payload: {nova_id, correlation_id}  — downstream reads rest from DDB
    # ExecutionAlreadyExists → idempotent success (handles both typed exception and ClientError for moto compat)
    # ThrottlingException | ServiceUnavailable | InternalServerError → raise RetryableError
    # Returns: {nova_id, execution_arn, execution_name} or {nova_id, execution_name, already_existed: True}
```

**Which task starts which workflow**:
- `PublishIngestNewNova` → `INGEST_NEW_NOVA_STATE_MACHINE_ARN` (called from 3 paths in initialize_nova)
- `LaunchRefreshReferences` → `REFRESH_REFERENCES_STATE_MACHINE_ARN` (from ingest_new_nova Parallel)
- `LaunchDiscoverSpectraProducts` → `DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN` (from ingest_new_nova Parallel)
- Downstream Parallel branch failures do NOT retroactively fail `ingest_new_nova`
