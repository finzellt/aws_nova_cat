# Nova Cat — Compressed Architecture Reference 2

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
2. FinalizeJobRunSuccess

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
