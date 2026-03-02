# Nova Cat — Compressed Architecture Reference

> Synthesized from all architecture docs, ADRs, workflow specs, contracts, and observability plan.
> **NOTE: `dynamodb-item-model.md`, `entities.py`, and `ADR-003` (Persistence Model for DynamoDB/S3)
> could not be read during synthesis — re-add their compressed forms separately.**

---

## 1. System Identity

**Nova Cat**: Serverless AWS (Python, Lambda, Step Functions, DynamoDB, S3) for aggregating,
validating, and publishing classical nova data. Low-throughput, cost-aware, MVP-scoped.

**Authoritative sources (in order):** DynamoDB item model → Workflow specs → Pydantic contracts
(`contracts/models`) → Generated JSON schemas. Docs conform to these; not vice versa.

**Stale/obsolete:** Any reference to `dataset_id` or Dataset abstraction (superseded by ADR-004).

> 📄 See `ADR-004-architecture-baseline-and-alignment-policy.md` for full alignment policy and
> authoritative source hierarchy.
> 📄 See `current-architecture.md` for the full authoritative architecture snapshot.

---

## 2. Identity Model

| Identifier | Scope | Notes |
|---|---|---|
| `nova_id` | Nova | UUID; stable forever |
| `data_product_id` | Spectra product | UUID; derived from provider identity hash |
| `reference_id` | Bibliography | UUID |

- Names never used downstream of `initialize_nova`.
- `data_product_id = UUID(hash(provider + provider_product_key + canonical_locator))`
- Name resolution via `NAME#<normalized_name>` DDB partition.
- Coordinate dup detection: `< 2"` → alias existing; `2–10"` → quarantine; `> 10"` → new nova.
- Locator dedup via `LOCATOR#<provider>#<locator_identity>` partition.

---

## 3. Domain Entities

> 📄 See `entities.py` for full Pydantic model definitions.
> 📄 See `dynamodb-item-model.md` for DynamoDB item shapes and field-level detail.

### Nova
`nova_id`, `primary_name`, `primary_name_normalized`, `status`, `ra_deg`, `dec_deg`, `frame`,
`epoch`, `discovery_date`. Derived astro metadata not persisted.

### DataProduct (Spectra)
Atomic ingestion/validation unit. Per-product: `acquisition_status` (`STUB|ACQUIRED|FAILED`),
`validation_status` (`UNVALIDATED|VALID|QUARANTINED`), `eligibility`, cooldown fields,
`content_fingerprint` (SHA-256), `header_signature_hash`, `selected_profile`,
`quarantine_reason_code`. No dataset abstraction.

### DataProduct (Photometry Table)
- One `PHOTOMETRY_TABLE` product per nova.
- Canonical S3 key: `derived/photometry/<nova_id>/photometry_table.parquet`
- Routine ingestion: rebuild + overwrite in place (no snapshot).
- Snapshot **only** on `photometry_schema_version` change → copies old to
  `derived/photometry/<nova_id>/snapshots/schema=<old_version>/`.
- DDB stores: canonical S3 key, `photometry_schema_version`, ingestion summary.

### Reference
Global bibliographic entity. Linked to nova via `NOVAREF` items (many-to-many, no duplication).

### JobRun / Attempt
One JobRun per workflow execution; one Attempt per task invocation (incl. retries). Operational
state separate from scientific state. Scientific enums MUST NOT encode retryability.

---

## 4. Contracts & Schema Governance

> 📄 See `0001-contracts-and-schema.md` for full ADR.
> 📄 See `events.py` for all event model definitions.
> 📄 See `entities.py` for all entity model definitions.

- Pydantic models = source of truth for all contracts (entities + events).
- JSON schemas generated from models, committed to `/schemas`, treated as stable.
- Every entity: `schema_version` (SemVer); every event: `event_version` (SemVer).
- Breaking = remove/rename/retype field, tighten validation. Non-breaking = add optional field,
  relax validation, expand enums (if consumers tolerate unknowns).
- CI enforces: schema regen match, no drift, fixture validation.
- **Idempotency keys internal-only; NEVER in boundary schemas.**

### Event Models (`events.py`)

All events extend `EventBase`: `event_version="1.0.0"`, `correlation_id` (UUID, auto-generated),
`initiated_at` (tz-aware UTC). `extra="forbid"`.

| Event | Required | Notes |
|---|---|---|
| `InitializeNovaEvent` | `candidate_name` | `source`, `attributes` optional |
| `IngestNewNovaEvent` | `nova_id` | Behavioral knobs (e.g., `force_refresh`) via `attributes` |
| `RefreshReferencesEvent` | `nova_id` | ADS query hints via `attributes` |
| `DiscoverSpectraProductsEvent` | `nova_id` | Provider constraints via `attributes` e.g. `{"sources":["ESO"]}` |
| `AcquireAndValidateSpectraEvent` | `nova_id`, `provider`, `data_product_id` | `provider` included to avoid extra DDB read for key construction |
| `IngestPhotometryEvent` | `candidate_name` OR `nova_id` | `photometry_schema_version` optional; validator enforces ≥1 identifier |
| `NameCheckAndReconcileEvent` | `nova_id` | Naming hints via `attributes` |

---

## 5. DynamoDB Table Layout

> 📄 See `dynamodb-item-model.md` for full item shapes, field types, and TTL/GSI details.
> 📄 See `dynamodb-access-patterns.md` for per-workflow read/write patterns.
> 📄 See `ADR-003-Persistence-Model-for-DynamoDB_S3` for persistence model ADR.

Single heterogeneous table.

| PK | SK | Item type |
|---|---|---|
| `<nova_id>` | `NOVA` | Nova |
| `<nova_id>` | `PRODUCT#PHOTOMETRY_TABLE` | Photometry product |
| `<nova_id>` | `PRODUCT#SPECTRA#<provider>#<data_product_id>` | Spectra product |
| `<nova_id>` | `FILE#...` | FileObject |
| `<nova_id>` | `REF#<reference_id>` | Reference |
| `<nova_id>` | `NOVAREF#<reference_id>` | Nova↔Reference link |
| `<nova_id>` | `JOBRUN#...` | JobRun |
| `<nova_id>` | `ATTEMPT#<job_run_id>#...` | Attempt |
| `NAME#<normalized_name>` | `NOVA#<nova_id>` | Name mapping |
| `LOCATOR#<provider>#<locator_identity>` | `DATA_PRODUCT#<data_product_id>` | Locator alias |

**GSI (eligibility):** `GSI1PK=<nova_id>`, `GSI1SK begins_with "ELIG#ACQUIRE#SPECTRA#"`. Index
attrs removed immediately after validation.

**Common queries:** `SK begins_with "PRODUCT#"`, `"PRODUCT#SPECTRA#"`, `"JOBRUN#"`, `"REF#"`,
`"NOVAREF#"`, `"ATTEMPT#<job_run_id>#"`.

---

## 6. S3 Layout

> 📄 See `s3-layout.md` for full prefix conventions, rules, and design rationale.
> 📄 See `ADR-003-Persistence-Model-for-DynamoDB_S3` for persistence model ADR.

Two buckets: `nova-cat-private-data` (never exposed) and `nova-cat-public-site`.

### Private prefixes
```
raw/spectra/<nova_id>/<data_product_id>/primary.fits
raw/spectra/<nova_id>/<data_product_id>/source.json        # provenance snapshot
raw/spectra/<nova_id>/<data_product_id>/archive.zip        # if ZIP
raw/spectra/<nova_id>/<data_product_id>/unzipped/<path>

quarantine/spectra/<nova_id>/<data_product_id>/<timestamp>/primary.fits
quarantine/spectra/<nova_id>/<data_product_id>/<timestamp>/context.json
  # fields: validation_status, quarantine_reason_code, header_sig_hash,
  #         fits_profile_id, locator_identity, CW log refs

derived/spectra/<nova_id>/<data_product_id>/normalized/spectrum.parquet
derived/spectra/<nova_id>/<data_product_id>/normalized/metadata.json
derived/spectra/<nova_id>/<data_product_id>/plots/preview.png

derived/photometry/<nova_id>/photometry_table.parquet      # canonical (overwritten in place)
derived/photometry/<nova_id>/metadata.json
derived/photometry/<nova_id>/snapshots/schema=<old_ver>/photometry_table.parquet

raw/photometry/uploads/<ingest_file_id>/original/<filename>
raw/photometry/uploads/<ingest_file_id>/split/<nova_id>/<filename>

bundles/<nova_id>/full.zip
bundles/<nova_id>/manifest.json   # data_product_ids, photometry key, checksums

workflow-payloads/<workflow_name>/<job_run_id>/input.json  # optional; if payload > SFN limit
```

### Public prefixes
```
releases/<release_id>/...
current/...   # optional pointer
```

**Principles:** raw bytes immutable (re-acquire overwrites only if not yet validated); derived
artifacts reproducible; quarantine preserves full diagnostic context; FITS profiles stay in code
repo by default.

---

## 7. Workflow Architecture

> 📄 See `ADR-002-workflow-orchestration-and-execution-model.md` for orchestration ADR.
> 📄 See individual workflow spec files for full state machine details.

### Chain
```
initialize_nova(candidate_name)
  └─► ingest_new_nova(nova_id)
        ├─► refresh_references
        └─► discover_spectra_products
              └─► acquire_and_validate_spectra (per data_product_id)

ingest_photometry           (API-triggered, independent)
name_check_and_reconcile    (scheduled post-establishment)
```

### Common state machine prefix (all workflows)
1. ValidateInput (Pass)
2. EnsureCorrelationId (Choice+Pass — generate UUID if absent)
3. BeginJobRun (Task)
4. AcquireIdempotencyLock (Task)
5. [workflow-specific states]
6. FinalizeJobRunSuccess / FinalizeJobRunQuarantined / FinalizeJobRunFailed

### Common error handling (all workflows)
**QuarantineHandler:** persists status + diagnostics, emits `QUARANTINED` outcome, publishes SNS
(best-effort — MUST NOT fail workflow if SNS fails). SNS payload: `workflow_name`, primary_id,
`correlation_id`, `error_fingerprint`, classification_reason.

---

## 8. Workflow Specs

### 8.1 `initialize_nova`
> 📄 See `initialize-nova.md` for full state machine, retry policy, and log fields.

Input: `candidate_name` | Idempotency: `InitializeNova:{normalized_candidate_name}:{schema_version}:{time_bucket}`

Post-prefix states:
1. NormalizeCandidateName
2. CheckExistingNovaByName → found: PublishIngestNewNova → **`EXISTS_AND_LAUNCHED`**
3. ResolveCandidateAgainstPublicArchives
4. CheckExistingNovaByCoordinates → CoordinateMatchClassification:
   - `< 2"` → UpsertAlias → PublishIngestNewNova → **`EXISTS_AND_LAUNCHED`**
   - `2–10"` → **Quarantine**
   - `> 10"` → continue
5. CandidateIsNova? No → **`NOT_FOUND`**
6. CandidateIsClassicalNova? No → **`NOT_A_CLASSICAL_NOVA`** | Ambiguous → **Quarantine** | Yes → continue
7. CreateNovaId → UpsertMinimalNovaMetadata → PublishIngestNewNova → **`CREATED_AND_LAUNCHED`**

`NOT_FOUND` and `NOT_A_CLASSICAL_NOVA` are terminal-success; no downstream launch.

Timeouts: NormalizeName 10s, CheckByName 20s, ResolveArchives 60s, CheckByCoords 20s,
UpsertAlias 20s, CreateNovaId 10s, UpsertMetadata 30s, Publish 10s.
All retry 3× (2× for short publish tasks) with 2/10/30s backoff.

---

### 8.2 `ingest_new_nova`
> 📄 See `ingest-new-nova.md` for full state machine, retry policy, and log fields.

Input: `nova_id` | Idempotency: `IngestNewNova:{nova_id}:{schema_version}` | Target: ≤10 min

Post-prefix states:
1. LaunchDownstream (Parallel): LaunchRefreshReferences + LaunchDiscoverSpectraProducts
   (each 10s, retry 2×)
2. SummarizeLaunch → FinalizeJobRunSuccess

Downstream failures do NOT retroactively fail this coordinator.

---

### 8.3 `discover_spectra_products`
> 📄 See `discover-spectra-products.md` for full state machine, identity ladder, and retry policy.

Input: `nova_id` | Idempotency: `DiscoverSpectraProducts:{nova_id}:{schema_version}:{time_bucket}`

Post-prefix states:
1. DiscoverAcrossProviders (Map, MaxConcurrency=1 MVP):
   - QueryProviderForProducts (60s, retry 3×)
   - NormalizeProviderProducts (30s, retry 2× transient only)
   - DeduplicateAndAssignDataProductIds (30s, retry 3×)
   - PersistDataProductMetadata (30s, retry 3×)
   - PublishAcquireAndValidateSpectraRequests (10s, retry 2×)
2. SummarizeDiscovery → FinalizeJobRunSuccess

**Identity ladder:** (1) NATIVE_ID — provider-native ID; (2) METADATA_KEY — provider+instrument+obs_time;
(3) WEAK → defer to byte-level dedup. Persist `identity_strategy` on record.

**Rules:**
- Existing VALIDATED `data_product_id` → do NOT republish acquire event.
- New locator for existing product → persist as alias.
- Item-level quarantine (malformed/ambiguous) → don't fail whole workflow; count in summary.

**Stub fields set on new products:** `acquisition_status=STUB`, `validation_status=UNVALIDATED`,
`eligibility=ACQUIRE`, cooldown initialized, eligibility index attrs present.

---

### 8.4 `acquire_and_validate_spectra`
> 📄 See `acquire-and-validate-spectra.md` for full state machine, validation profile logic,
> duplicate detection details, and retry policy.

Input: `nova_id`, `provider`, `data_product_id` | Idempotency: `AcquireAndValidateSpectra:{data_product_id}:{schema_version}` | One `data_product_id` per execution.

Post-prefix states:
1. LoadDataProductMetadata (10s, retry 3×)
2. CheckOperationalStatus (10s, retry 3×)
3. AlreadyValidated? (`validation_status==VALID`) → **`SKIPPED_DUPLICATE`**
4. CooldownActive? (`now < next_eligible_attempt_at`) → **`SKIPPED_BACKOFF`**
5. AcquireArtifact (15m, retry 3×, backoff 10/60/180s; HTTP 429 → retryable + longer cooldown).
   ZIP → unpack + select primary FITS. Provider dispatch internal to Lambda (no SFN branching).
6. ValidateBytes/Profile-Driven (5m, retry 2× transient only):
   - Open FITS → select profile (provider + header: `INSTRUME`, `TELESCOP`) → extract arrays →
     normalize headers → sanity checks (axis monotonicity, units, finiteness) → fingerprint
   - No profile / missing required metadata → **QUARANTINE**
7. DuplicateByFingerprint? (SHA-256 vs existing VALIDATED):
   - Yes → RecordDuplicateLinkage → **`DUPLICATE_OF_EXISTING`** (current NOT marked VALIDATED)
   - No → continue
8. RecordValidationResult (20s, retry 3×) → FinalizeJobRunSuccess **`VALIDATED`**
   - Sets `eligibility=NONE`, removes eligibility index attrs

**On retryable failure:** increment `attempt_count_total`, set `last_attempt_at`,
`last_error_fingerprint`; compute `next_eligible_attempt_at = now + capped_exponential_backoff(attempt_count)`.

**Quarantine triggers:** checksum mismatch, corrupt/unreadable FITS, unknown profile, missing
required metadata, invalid units, failed sanity checks.

---

### 8.5 `refresh_references`
> 📄 See `refresh-references.md` for full state machine, dedupe keys, and log fields.

Input: `nova_id` | Idempotency: `RefreshReferences:{nova_id}:{schema_version}:{time_bucket}`

Post-prefix states:
1. FetchReferenceCandidates (60s, retry 3×)
2. ReconcileReferences (Map, MaxConcurrency=5):
   NormalizeReference → UpsertReferenceEntity (→ `reference_id`) → LinkNovaReference.
   Item failures → quarantine item + continue (don't fail Map).
3. ComputeDiscoveryDate (20s, retry 2× transient; lives here post-reconciliation by design)
4. UpsertDiscoveryDateMetadata (no-op if unchanged; monotonically earlier)
5. FinalizeJobRunSuccess

Dedupe keys: `ReferenceUpsert:{source}:{source_key}:{schema_version}`,
`NovaReferenceLink:{nova_id}:{reference_id}`,
`DiscoveryDate:{nova_id}:{earliest_reference_id}:{rule_version}`.

---

### 8.6 `ingest_photometry`
> 📄 See `ingest-photometry.md` for full state machine, schema-change snapshot semantics,
> and quarantine details.

Input: `candidate_name` OR `nova_id` | Idempotency: `IngestPhotometry:{nova_id}:{photometry_schema_version}`

Post-prefix states:
1. ResolveNovaId (if `candidate_name`)
2. CheckOperationalStatus → AlreadyIngested? → **`SKIPPED_DUPLICATE`**
3. ValidatePhotometry (5m; no retry for deterministic errors; 2× transient)
4. RebuildPhotometryTable (5m, retry 2×) — overwrites canonical parquet
5. PersistPhotometryMetadata (30s, retry 3×)
6. FinalizeJobRunSuccess **`INGESTED`**

Schema-change snapshot: if `photometry_schema_version` changes → copy old canonical to snapshot
path → write new canonical.

**Quarantine:** invalid columns, invalid units, malformed timestamps, inconsistent data.

---

### 8.7 `name_check_and_reconcile`
> 📄 See `name-check-and-reconcile.md` for full state machine and quarantine details.

Input: `nova_id` | Idempotency: `NameCheckAndReconcile:{nova_id}:{schema_version}:{time_bucket}`
NOT a name front door. Runs post-establishment (~first 6 weeks); stops when naming stabilizes.

Post-prefix states:
1. FetchCurrentNamingState → QueryNamingAuthorities (Parallel) → ReconcileNaming
2. NamingChanged? No → **`NO_CHANGE`** | Yes → ApplyNameUpdates → (optional) PublishNameReconciled
   → **`UPDATED`**

**Quarantine:** conflicting authorities with ambiguous canonical designation, non-monotonic changes
implying identity merge/split.

---

## 9. Execution Governance

> 📄 See `execution-governance.md` for full retry conventions, cooldown policy, and throttling
> principles.

### Retry conventions
| Class | MaxAttempts | Backoff |
|---|---|---|
| Default | 3 | 2s, 10s, 30s |
| Acquisition tasks | 3 | 10s, 60s, 180s |
| Short publish tasks | 2 | 2s, 10s |

Only RETRYABLE errors retried. No unbounded retries.

### Error taxonomy
- **RETRYABLE:** throttling, transient network, timeouts, 5xx
- **TERMINAL:** unsupported schema_version, missing/invalid UUIDs, missing required fields,
  logical impossibility
- **QUARANTINE:** ambiguous resolution, spectra integrity/format/domain failures, malformed
  provider records, unknown FITS profile

### Cooldown (spectra acquisition)
Before each run: `validation_status==VALID` → skip | `now < next_eligible_attempt_at` → skip |
else proceed. On retryable failure: `next_eligible_attempt_at = now + capped_exponential_backoff(attempt_count)`.

### Concurrency (MVP)
- DiscoverSpectraProducts Map: MaxConcurrency=1
- RefreshReferences Map: MaxConcurrency=5
- AcquireAndValidateSpectra: 1 per execution

### Correlation ID
Optional in boundary events; workflow generates if absent. Propagated in all downstream events,
JobRun/Attempt records, structured logs.

---

## 10. Observability

> 📄 See `observability-plan.md` for full metrics list, alarm definitions, and traceability model.

**Structured log fields (all workflows):** `workflow_name`, `execution_arn`, `job_run_id`,
`state_name`, `attempt_number`, `correlation_id`, primary UUID(s), `error_classification`,
`error_fingerprint`, `duration_ms`. No `dataset_id`.

**Join keys:** `job_run_id` (within execution), `correlation_id` (cross-workflow chains),
`nova_id`/`data_product_id` (domain).

**Metrics (per workflow):** `executions_started/succeeded/failed_terminal/quarantined/skipped_duplicate`,
`execution_duration_ms` p50/p95. Cross-cutting: `task_attempts_total`, `retry_count`,
`quarantine_count` (by provider), `spectra_validation_status_counts`, `photometry_ingestion_counts`.

**Alarms:** elevated terminal failure rate; elevated quarantine rate (especially per-provider);
provider health (RETRYABLE spikes); latency regression (p95 > baseline).

---

## 11. Architectural Invariants

1. UUID-first downstream execution; no name-based ops beyond `initialize_nova`.
2. No Dataset abstraction; `DataProduct` is atomic scientific unit.
3. One `data_product_id` per `acquire_and_validate_spectra` execution (Mode 1 MVP).
4. Photometry: one per nova, canonical overwrite, snapshot only on schema version change.
5. Explicit QUARANTINE semantics; operational review loop required.
6. Operational state never encoded in scientific enums.
7. Idempotency keys internal-only; never in boundary schemas.
8. Provider-specific logic in adapters; no SFN branching by provider.
9. `ComputeDiscoveryDate` lives in `refresh_references` (post-reconciliation by design).
10. Continuation payload event model for workflow chaining.

---

## 12. Non-MVP / Deferred

- Mode 2 batch acquisition (multi-`data_product_id` per execution)
- Global multi-nova sweeps / spatial indexing
- Full VO compliance enforcement
- Advanced photometry version diffing
- Schema migration workflow implementation (structure documented)
- Runtime-managed FITS profile definitions in S3
