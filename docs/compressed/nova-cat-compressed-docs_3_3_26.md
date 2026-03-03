# Nova Cat — Compressed Reference Docs

---

## 1. CROSS-CUTTING CONVENTIONS

### Retry Defaults
| Profile | MaxAttempts | Backoff |
|---|---|---|
| Default | 3 | 2s, 10s, 30s |
| Long-running acquisition | 3 | 10s, 60s, 180s |

### Error Taxonomy
- **RETRYABLE**: throttling, transient network, timeouts, 5xx
- **TERMINAL**: unsupported schema_version, missing/invalid UUIDs (nova_id / data_product_id / bibcode), invalid required fields, logical impossibility (e.g. object explicitly not a classical nova)
- **QUARANTINE**: ambiguous name resolution, content fails integrity/format/sanity checks, malformed provider records, unknown/incompatible FITS profile

> Scientific state (`validation_status`, `acquisition_status`) MUST NOT encode retryability. Retryability is operational; lives in JobRun/Attempt records and `last_attempt_outcome`.

### Correlation & Idempotency
- Every workflow input SHOULD include `correlation_id`; if absent, workflow MUST generate UUID and propagate it.
- `correlation_id` MUST appear in all downstream events, all JobRun/Attempt records, and structured logs.
- Every workflow defines a deterministic idempotency key (time-bucketed for poll/refresh workflows).
- Idempotency keys are **internal only** — MUST NOT appear in event payload schemas.
- Step-level dedupe keys defined for: identity assignment, AcquireArtifact, validation result recording, link relationships.

### Concurrency Defaults (MVP)
- DiscoverAcrossProviders Map: MaxConcurrency 1
- RefreshReferences Map: MaxConcurrency 5 (tunable)
- AcquireAndValidateSpectra: one `data_product_id` per execution (Mode 1)

### Quarantine Handling (ALL workflows — standard pattern)
1. Persist quarantine status + diagnostic metadata.
2. Emit JobRun outcome = `QUARANTINED`.
3. Publish SNS notification (best-effort; MUST NOT fail workflow): include `workflow_name`, primary identifier, `correlation_id`, `error_fingerprint`, brief reason.

### Observability
All logs: structured JSON.

**Required log fields**: `workflow_name`, `execution_arn`, `job_run_id`, `state_name`, `attempt_number`, `correlation_id`, `idempotency_key` (internal), primary identifiers (`nova_id` / `data_product_id` / `bibcode`), `error_classification`, `error_fingerprint`, `duration_ms`.

No `dataset_id` exists in the system.

**Key metrics**: `executions_started/succeeded/failed_terminal/quarantined/skipped_duplicate`, `execution_duration_ms` (p50/p95), `task_attempts_total`, `retry_count`, `quarantine_count` (by provider), `spectra_validation_status_counts`, `photometry_ingestion_counts`.

**Traceability**: `job_run_id` joins execution history + Attempt records + logs. `correlation_id` joins cross-workflow chains. `nova_id` / `data_product_id` are canonical domain join keys.

**Alarms**: elevated terminal failure rate; elevated quarantine rate (especially provider-specific); provider health spikes; p95 latency regressions.

---

## 2. DYNAMODB

### Table: `NovaCat` — Single table, heterogeneous. PK (string) + SK (string).

**Partition namespaces:**
- Per-nova: `PK = "<nova_id>"` (raw UUID)
- Global identity: `PK = "NAME#<normalized_name>"`, `PK = "LOCATOR#<provider>#<locator_identity>"`, `PK = "REFERENCE#<bibcode>"`, `PK = "WORKFLOW#<correlation_id>"` (pre-nova)

**SK prefixes within nova partition**: `NOVA`, `PRODUCT#...`, `FILE#...`, `NOVAREF#...`, `JOBRUN#...`, `ATTEMPT#...`

**GSI1 (EligibilityIndex)**:
- `GSI1PK = "<nova_id>"`
- `GSI1SK = "ELIG#<eligibility>#<product_type>#<provider>#<data_product_id>"`
- Set `GSI1PK/SK = null` once product is no longer eligible (validated/quarantined).

---

### Item Types

#### 1. Nova
**Key**: `PK=<nova_id>`, `SK="NOVA"`

**Fields**: `entity_type="Nova"`, `schema_version`, `nova_id`, `primary_name`, `primary_name_normalized`, `ra_deg`, `dec_deg`, `coord_frame` (e.g. ICRS), `coord_epoch` (e.g. J2000), `discovery_date` (optional, derived), `aliases` (string list), `status` (ACTIVE|MERGED|DEPRECATED), `created_at`, `updated_at`

---

#### 2. NameMapping
**Key**: `PK="NAME#<normalized_name>"`, `SK="NOVA#<nova_id>"`

**Fields**: `entity_type="NameMapping"`, `schema_version`, `name_raw`, `name_normalized`, `name_kind` (PRIMARY|ALIAS), `nova_id`, `source` (USER_INPUT|INGESTION|SIMBAD|TNS|OTHER), `created_at`, `updated_at`

---

#### 3a. DataProduct — Photometry Table (one per nova)
**Key**: `PK=<nova_id>`, `SK="PRODUCT#PHOTOMETRY_TABLE"`

**Fields**: `entity_type="DataProduct"`, `schema_version`, `data_product_id`, `product_type="PHOTOMETRY_TABLE"`, `s3_bucket`, `s3_key` (current parquet), `last_ingestion_at`, `last_ingestion_source`, `ingestion_count`, `photometry_schema_version`, `created_at`, `updated_at`

---

#### 3b. DataProduct — Spectra (one per atomic product)
**Key**: `PK=<nova_id>`, `SK="PRODUCT#SPECTRA#<provider>#<data_product_id>"`

**Fields**:
- Identity: `entity_type="DataProduct"`, `schema_version`, `data_product_id`, `product_type="SPECTRA"`, `provider`, `nova_id`
- Locators: `locators` (list of `{kind: URL|S3|OTHER, value, role: PRIMARY|MIRROR}`), `locator_identity` (normalized; `provider_product_id:<id>` preferred, else `url:<normalized_url>`)
- Lifecycle: `validation_status` (UNVALIDATED|VALID|QUARANTINED|TERMINAL_INVALID), `acquisition_status` (STUB|ACQUIRED|FAILED_RETRYABLE|SKIPPED_DUPLICATE|SKIPPED_BACKOFF), `eligibility` (ACQUIRE|NONE)
- Cooldown: `attempt_count`, `last_attempt_at`, `next_eligible_attempt_at`, `last_error_fingerprint`
- Fingerprints: `byte_length`, `etag`, `sha256`, `header_signature_hash`
- Profile: `fits_profile_id`, `profile_selection_inputs` (provider+hints+sig_hash), `normalization_notes`
- Quarantine: `quarantine_reason_code` (UNKNOWN_PROFILE|MISSING_CRITICAL_METADATA|CHECKSUM_MISMATCH|COORDINATE_PROXIMITY|OTHER), `manual_review_status` (PENDING|CLEARED_RETRY_APPROVED|CLEARED_TERMINAL)
- S3: `raw_s3_bucket`, `raw_s3_key`, `derived_s3_prefix`
- Other: `hints` (instrument/telescope/pipeline), `provenance` (required for donated data), `identity_strategy` (NATIVE_ID|METADATA_KEY|WEAK)
- GSI: `GSI1PK=<nova_id>`, `GSI1SK="ELIG#ACQUIRE#SPECTRA#<provider>#<data_product_id>"` (null when not eligible)

---

#### 4. LocatorAlias
**Key**: `PK="LOCATOR#<provider>#<locator_identity>"`, `SK="DATA_PRODUCT#<data_product_id>"`

**Fields**: `entity_type="LocatorAlias"`, `schema_version`, `provider`, `locator_identity`, `data_product_id`, `nova_id`, `created_at`, `updated_at`

Deduplication authority during discovery. Locator normalization: prefer `provider_product_id:<id>`, else `url:<normalized_url>`.

---

#### 5. FileObject (optional S3 registry)
**Key**: Role-scoped SK, always ends in `#ID#<file_id>`.

| Role | PK | SK |
|---|---|---|
| WORKFLOW_QUARANTINE_CONTEXT | `WORKFLOW#<correlation_id>` | `FILE#WORKFLOW_QUARANTINE_CONTEXT#ID#<file_id>` |
| SPECTRA_RAW_FITS | `<nova_id>` | `FILE#SPECTRA_RAW_FITS#NOVA#<nova_id>#ID#<file_id>` |
| SPECTRA_QUARANTINE_CONTEXT | `<nova_id>` | `FILE#SPECTRA_QUARANTINE_CONTEXT#NOVA#<nova_id>#PRODUCT#<data_product_id>#ID#<file_id>` |
| SPECTRA_NORMALIZED | `<nova_id>` | `FILE#SPECTRA_NORMALIZED#NOVA#<nova_id>#PRODUCT#<data_product_id>#ID#<file_id>` |
| SPECTRA_PLOT | `<nova_id>` | `FILE#SPECTRA_PLOT#NOVA#<nova_id>#PRODUCT#<data_product_id>#ID#<file_id>` |
| PHOTOMETRY_TABLE | `<nova_id>` | `FILE#PHOTOMETRY_TABLE#NOVA#<nova_id>#ID#<file_id>` |
| PHOTOMETRY_SNAPSHOT | `<nova_id>` | `FILE#PHOTOMETRY_SNAPSHOT#NOVA#<nova_id>#ID#<file_id>` |
| BUNDLE_MANIFEST/ZIP/OTHER | `<nova_id>` | `FILE#<ROLE>#NOVA#<nova_id>#ID#<file_id>` |

> `WORKFLOW_QUARANTINE_CONTEXT` uses `WORKFLOW#<correlation_id>` PK because no `nova_id` exists yet.

**Fields**: `entity_type="FileObject"`, `schema_version`, `file_id`, `nova_id` (nullable), `data_product_id` (nullable), `role`, `bucket`, `key`, `content_type`, `byte_length`, `etag`, `sha256`, `created_by` (workflow + job_run_id), `created_at`, `updated_at`

---

#### 6. Reference (global, shared)
**Key**: `PK="REFERENCE#<bibcode>"`, `SK="METADATA"`

**Fields**: `entity_type="Reference"`, `schema_version`, `bibcode`, `reference_type` (journal_article|conference_abstract|poster|catalog|software|atel|cbat_circular|arxiv_preprint|other), `title`, `year`, `publication_date` (YYYY-MM-DD; day `00` = month-only precision), `authors`, `doi`, `arxiv_id` (bare, no prefix), `provenance`, `created_at`, `updated_at`. No TTL.

ADS URL always derivable: `https://ui.adsabs.harvard.edu/abs/<bibcode>` — not stored.

**Upsert**: GetItem on bibcode → if not found: write new; if found: update mutable fields only.

**ADS doctype → reference_type**: `article→journal_article`, `eprint→arxiv_preprint`, `inproceedings|abstract→conference_abstract`, `circular→cbat_circular`, `telegram→atel`, `catalog→catalog`, `software→software`, else `other`.

---

#### 7. NovaReference (per-nova link)
**Key**: `PK=<nova_id>`, `SK="NOVAREF#<bibcode>"`

**Fields**: `entity_type="NovaReference"`, `schema_version`, `nova_id`, `bibcode`, `role` (DISCOVERY|SPECTRA_SOURCE|PHOTOMETRY_SOURCE|OTHER), `added_by_workflow`, `notes`, `provenance`, `created_at`, `updated_at`

Default role = OTHER. Idempotent upsert: `condition_expression=attribute_not_exists(PK)`.

---

#### 8. JobRun
**Key**: `PK=<nova_id>`, `SK="JOBRUN#<workflow_name>#<started_at>#<job_run_id>"`

**Fields**: `entity_type="JobRun"`, `schema_version`, `job_run_id`, `workflow_name`, `execution_arn`, `status` (RUNNING|SUCCEEDED|FAILED), `started_at`, `ended_at`, `created_at`, `updated_at`

---

#### 9. Attempt
**Key**: `PK=<nova_id>`, `SK="ATTEMPT#<job_run_id>#<task_name>#<attempt_no>#<timestamp>"`

**Fields**: `entity_type="Attempt"`, `schema_version`, `job_run_id`, `task_name`, `attempt_no`, `status` (STARTED|SUCCEEDED|FAILED), `error_type`, `error_message`, `duration_ms`

---

### Key Access Patterns
| Pattern | Query |
|---|---|
| All products for nova | `PK=<nova_id>`, SK begins_with `PRODUCT#` |
| Spectra products | `PK=<nova_id>`, SK begins_with `PRODUCT#SPECTRA#` |
| Eligible spectra (GSI1) | `GSI1PK=<nova_id>`, GSI1SK begins_with `ELIG#ACQUIRE#SPECTRA#` |
| JobRuns for nova | `PK=<nova_id>`, SK begins_with `JOBRUN#` |
| Attempts for JobRun | `PK=<nova_id>`, SK begins_with `ATTEMPT#<job_run_id>#` |
| Reference lookup | GetItem `PK=REFERENCE#<bibcode>`, `SK=METADATA` |
| Nova by name | GetItem `PK=NAME#<normalized_name>` |
| Locator → product | GetItem `PK=LOCATOR#<provider>#<locator_identity>` |

---

### DynamoDB Invariants
- QUARANTINE is human-gated: no auto-retries; re-entry requires `manual_review_status` update.
- `next_eligible_attempt_at` is authoritative for `SKIPPED_BACKOFF`.
- LocatorAlias is the dedupe authority during discovery; normalization must be deterministic.
- `eligibility=ACQUIRE` until validated or quarantined/terminal; GSI attributes nulled out after.
- Provider-level cooldown deferred post-MVP (per-product only in MVP).

---

## 3. S3 LAYOUT

### Buckets
- `nova-cat-private-data` (private): raw bytes, uploads, quarantine, derived artifacts, bundles, optional workflow payloads
- `nova-cat-public-site` (public): static site releases, curated public assets

### Private Bucket Keys

```
# Raw Spectra (immutable; re-acquire only if not yet validated)
raw/spectra/<nova_id>/<data_product_id>/primary.fits
raw/spectra/<nova_id>/<data_product_id>/source.json
raw/spectra/<nova_id>/<data_product_id>/archive.zip          # if acquired as archive
raw/spectra/<nova_id>/<data_product_id>/unzipped/<rel_path>

# Spectra Quarantine (bytes + context; multiple attempts OK)
quarantine/spectra/<nova_id>/<data_product_id>/<timestamp>/primary.fits
quarantine/spectra/<nova_id>/<data_product_id>/<timestamp>/context.json
# context.json: validation_status, quarantine_reason_code, header_signature_hash, fits_profile_id, locator_identity, CloudWatch log refs

# Derived Spectra
derived/spectra/<nova_id>/<data_product_id>/normalized/spectrum.parquet
derived/spectra/<nova_id>/<data_product_id>/normalized/metadata.json
derived/spectra/<nova_id>/<data_product_id>/plots/preview.png

# Photometry Uploads (transient inputs)
raw/photometry/uploads/<ingest_file_id>/original/<filename>
raw/photometry/uploads/<ingest_file_id>/split/<nova_id>/<filename>  # if multi-nova
raw/photometry/uploads/<ingest_file_id>/manifest.json

# Canonical Photometry Table (one per nova; overwritten on routine ingestion)
derived/photometry/<nova_id>/photometry_table.parquet
derived/photometry/<nova_id>/metadata.json
derived/photometry/<nova_id>/plots/lightcurve.png

# Photometry Schema-Change Snapshots (ONLY on schema version change)
derived/photometry/<nova_id>/snapshots/schema=<old_version>/photometry_table.parquet
derived/photometry/<nova_id>/snapshots/schema=<old_version>/at=<timestamp>/photometry_table.parquet

# Bundles (rebuilt when new data arrives)
bundles/<nova_id>/full.zip
bundles/<nova_id>/manifest.json
# manifest: bundle_build_id, created_at, data_product_ids, photometry_table_key, checksums

# Optional
workflow-payloads/<workflow_name>/<job_run_id>/input.json
workflow-payloads/<workflow_name>/<job_run_id>/output.json

# FITS Profile assets (if runtime-managed; default is in-repo code)
profiles/fits/<profile_id>/<version>/profile.yaml
```

### Public Bucket Keys
```
releases/<release_id>/index.html
releases/<release_id>/assets/...
releases/<release_id>/nova/<nova_id>/...
current/...   # optional redirect/copy pointer
```

### S3 Invariants
- Raw bytes are immutable.
- Derived artifacts are reproducible from raw.
- Snapshots represent schema boundaries only (not ingestion history).
- UUID-first layout. Spectra are atomic (`data_product_id`-scoped). Photometry is singleton-per-nova.

---

## 4. FITS PROFILES

IVOA-aligned (Spectrum Data Model, ObsCore, common FITS header conventions). Provider-specific profiles normalize deviations into canonical internal representation.

### Profile Selection (deterministic, in order)
1. Match by `provider`
2. If multiple: match by header signature fields (`INSTRUME`, `TELESCOP`, `ORIGIN`, VO markers)
3. No match → QUARANTINE

Applied during `ValidateBytes` state of `acquire_and_validate_spectra`.

### Profile Structure (each profile defines)
- **Identification**: provider, optional header signature rules, optional instrument/telescope IDs
- **Data Location**: HDU name/index, table vs image, column name aliases
  - Wavelength aliases: WAVE, WAVELENGTH, LAMBDA, VO spectral axis columns
  - Flux aliases: FLUX, F_LAMBDA, SPEC
- **Units**: acceptable input units, canonical output units, conversion rules; unknown/missing → QUARANTINE. Follow IVOA/VOUnits conventions.
- **Header Normalization**: provider keyword → canonical field (e.g. DATE-OBS→observation_time, MJD-OBS→observation_mjd, RA/DEC→target_coordinates, EXPTIME→exposure_time, INSTRUME→instrument); missing required fields → QUARANTINE
- **Validation Rules**: spectral axis monotonic, flux non-empty, acceptable NaN/Inf fraction, plausible wavelength range, required metadata present; structural corruption or schema violation → QUARANTINE; transient → RETRYABLE

### Canonical Internal Fields (IVOA-aligned)
`spectral_axis`, `flux_axis`, `flux_units`, `spectral_units`, `observation_time`, `target_coordinates`, `instrument`, `exposure_time`, `provider`, `dataset_id`, `nova_id`

### Failure Summary
| Cause | Classification |
|---|---|
| Unknown profile | QUARANTINE |
| Missing required IVOA metadata | QUARANTINE |
| Checksum mismatch | QUARANTINE |
| Invalid identifiers | TERMINAL |
| Transient download failure | RETRYABLE |

### Packaging
ZIP handling (unpack → identify FITS → select via profile) is acquisition logic, not profile logic.

### Invariants
- UUID identity never modified by normalization.
- `correlation_id` propagated but profile-independent.
- Profile evolution must not invalidate previously validated datasets.
- Persist on product record: `header_signature_hash`, `fits_profile_id`, `profile_selection_inputs`, `normalization_notes`, `derived_s3_prefix`.

---

## 5. WORKFLOWS

### Common State Machine Prefix (all workflows)
1. ValidateInput (Pass)
2. EnsureCorrelationId (Choice+Pass) — generate UUID if missing
3. BeginJobRun (Task)
4. AcquireIdempotencyLock (Task)
... workflow-specific states ...
N-2. TerminalFailHandler (Task)
N-1. FinalizeJobRunFailed (Task)

Common terminal states: `FinalizeJobRunSuccess`, `FinalizeJobRunQuarantined`, `FinalizeJobRunFailed`.

---

### initialize_nova

**Purpose**: Name-only front door. Resolve `candidate_name` → `nova_id` or create new Nova. Only entry point when system has only a name.

**Input**: `candidate_name` (required), `correlation_id` (optional)
**Output** (on CREATED_AND_LAUNCHED or EXISTS_AND_LAUNCHED): publishes `ingest_new_nova` event with `nova_id`, `correlation_id`

**State machine** (after common prefix):
5. NormalizeCandidateName
6. CheckExistingNovaByName
7. ExistsInDB? → YES: PublishIngestNewNova → FinalizeSuccess(`EXISTS_AND_LAUNCHED`)
8. ResolveCandidateAgainstPublicArchives (queries SIMBAD/TNS; returns coords + aliases list)
9. CheckExistingNovaByCoordinates (compute angular separation to all existing novas)
10. CoordinateMatchClassification?
    - <2" → UpsertAliasForExistingNova → PublishIngestNewNova → FinalizeSuccess(`EXISTS_AND_LAUNCHED`)
    - 2"–10" → QuarantineHandler(`COORDINATE_AMBIGUITY`)
    - >10" → continue
11. CandidateIsNova? → NO: FinalizeSuccess(`NOT_FOUND`)
12. CandidateIsClassicalNova?
    - NO → FinalizeSuccess(`NOT_A_CLASSICAL_NOVA`)
    - Ambiguous → QuarantineHandler
    - YES → continue
13. CreateNovaId
14. UpsertMinimalNovaMetadata — writes Nova + PRIMARY NameMapping + ALIAS NameMappings (from SIMBAD `ids`; strip `V* ` prefix; skip if normalized form == normalized_candidate_name; `name_kind=ALIAS`, `source=SIMBAD`)
15. PublishIngestNewNova
16. FinalizeSuccess(`CREATED_AND_LAUNCHED`)

**Retry policy**:
| State | Timeout | MaxAttempts | Backoff |
|---|---|---|---|
| BeginJobRun / AcquireIdempotencyLock | 10s | 3 | 2s,10s,30s |
| NormalizeCandidateName | 10s | 2 | 2s,10s |
| CheckExistingNovaByName | 20s | 3 | 2s,10s,30s |
| ResolveCandidateAgainstPublicArchives | 60s | 3 | 2s,10s,30s |
| CheckExistingNovaByCoordinates | 20s | 3 | 2s,10s,30s |
| UpsertAliasForExistingNova | 20s | 3 | 2s,10s,30s |
| CreateNovaId / UpsertMinimalNovaMetadata | 10s/30s | 3 | 2s,10s,30s |
| PublishIngestNewNova | 10s | 2 | 2s,10s |

**Idempotency key**: `InitializeNova:{normalized_candidate_name}:{schema_version}:{time_bucket}`

**Terminal success outcomes** (no downstream launch): `NOT_FOUND`, `NOT_A_CLASSICAL_NOVA`

---

### ingest_new_nova

**Purpose**: Coordinator — bootstraps ingestion for existing `nova_id`. Short-lived, cost-aware. Does NOT perform "ensure initialized."

**Input**: `nova_id` (required), `correlation_id` (optional)
**Output**: publishes `refresh_references` and `discover_spectra_products` events

**State machine** (after common prefix):
5. LaunchDownstream (Parallel)
   - LaunchRefreshReferences → publishes `refresh_references` event
   - LaunchDiscoverSpectraProducts → publishes `discover_spectra_products` event
6. FinalizeJobRunSuccess

**Retry policy**: BeginJobRun/Lock: 10s, 3 attempts, 2s/10s/30s. Launch tasks: 10s, 2 attempts, 2s/10s.
Target duration ≤ 10 min. Downstream failures do NOT retroactively fail this coordinator.

**Idempotency key**: `IngestNewNova:{nova_id}:{schema_version}`

---

### refresh_references

**Purpose**: Fetch ADS references for `nova_id`, upsert global Reference entities, link via NovaReference, compute `discovery_date`.

**Input**: `nova_id` (required), `correlation_id` (optional), `attributes.ads_name_hints: list[str]` (optional — extra aliases for ADS query)

**ADS Query**: single name-based query using all aliases from NameMapping partition + `ads_name_hints`, individually quoted and OR'd. No coordinate search (ADS positional search not reliably documented). Auth: Bearer token from Secrets Manager; HTTP 429 → RETRYABLE. Unauthenticated: 5 req/day; Authenticated: 5000 req/day.

**Normalization rules**:
- `publication_date`: ADS `date` → `YYYY-MM-00` (day 00 = month-only precision; always discard day from ADS)
- `arxiv_id`: strip `arXiv:` prefix
- `authors`, `title`, `doi`: store as-is

**State machine** (after common prefix):
5. FetchReferenceCandidates
6. ReconcileReferences (Map, MaxConcurrency 5)
   - NormalizeReference
   - UpsertReferenceEntity → yields bibcode
   - LinkNovaReference (idempotent: condition_expression=attribute_not_exists(PK))
   - ItemFailureHandler (Catch → QuarantineItem + continue)
7. ComputeDiscoveryDate
8. UpsertDiscoveryDateMetadata (no-op if unchanged)
9. FinalizeJobRunSuccess

**Retry policy**:
| State | Timeout | MaxAttempts | Backoff |
|---|---|---|---|
| BeginJobRun/Lock | 10s | 3 | 2s,10s,30s |
| FetchReferenceCandidates | 60s | 3 | 2s,10s,30s |
| Map item tasks | 20s ea | 2 | 2s,10s |
| ComputeDiscoveryDate | 20s | 2 | internal transient only |
| UpsertDiscoveryDateMetadata | 20s | 3 | 2s,10s,30s |

**Idempotency keys**: workflow: `RefreshReferences:{nova_id}:{schema_version}:{time_bucket}`; upsert: `ReferenceUpsert:ADS:{bibcode}:{schema_version}`; link: `NovaReferenceLink:{nova_id}:{bibcode}`; discovery date: `DiscoveryDate:{nova_id}:{earliest_bibcode}:{rule_version}`

**Invariant**: `discovery_date` update is monotonically earlier. Stored as `YYYY-MM-DD`; day `00` = month-only. Lexicographic comparison is correct.

**Extra log fields**: `reference_source`, `candidate_count`, `upsert_count`, `link_count`, `quarantined_count`, `discovery_date_old`, `discovery_date_new`

---

### discover_spectra_products

**Purpose**: Discover spectra across providers for a `nova_id`; assign stable `data_product_id`s; persist stubs; publish acquisition events. Does NOT download bytes.

**Input**: `nova_id` (required), `correlation_id` (optional)
**Output**: publishes `acquire_and_validate_spectra` event per product (`nova_id`, `data_product_id`, `correlation_id`)

**State machine** (after common prefix):
5. DiscoverAcrossProviders (Map, MaxConcurrency 1 MVP)
   - QueryProviderForProducts
   - NormalizeProviderProducts
   - DeduplicateAndAssignDataProductIds
   - PersistDataProductMetadata
   - PublishAcquireAndValidateSpectraRequests
6. SummarizeDiscovery
7. FinalizeJobRunSuccess

**Identity ladder** (per discovered record):
1. **NATIVE_ID** (strong): use provider-native product ID
2. **METADATA_KEY** (strong fallback): provider + instrument + observation_time [+ telescope, program_id, pipeline_tag]
3. **WEAK**: defer definitive dedupe to acquire_and_validate_spectra (byte fingerprint)
Persist `identity_strategy` on product record.

**Locator Alias Rule**: if discovered record resolves to existing `data_product_id` and new locator is not yet recorded, persist it as additional alias.

**Publication Rule**: if existing `data_product_id` is already VALID → do NOT publish new acquire request.

**Persisted stub fields**: `data_product_id`, `nova_id`, `provider`, `locators`, `locator_identity`, `acquisition_status=STUB`, `validation_status=UNVALIDATED`, `eligibility=ACQUIRE`, `attempt_count=0`, cooldown fields initialized, GSI1 attributes set, provenance hints (instrument, telescope, pipeline_tag).

**Retry policy**:
| State | Timeout | MaxAttempts | Backoff |
|---|---|---|---|
| QueryProviderForProducts | 60s | 3 | 2s,10s,30s |
| NormalizeProviderProducts | 30s | 2 | internal transient |
| DeduplicateAndAssignDataProductIds | 30s | 3 | 2s,10s,30s |
| PersistDataProductMetadata | 30s | 3 | 2s,10s,30s |
| PublishAcquireAndValidateSpectraRequests | 10s | 2 | 2s,10s |

**Idempotency key**: `DiscoverSpectraProducts:{nova_id}:{schema_version}:{time_bucket}`

**Quarantine** (item-level): malformed provider record; ambiguous/unsafe identity mapping. Item-level quarantines MUST NOT fail entire workflow; count and surface in summary.

---

### acquire_and_validate_spectra

**Purpose**: Atomic acquisition + validation of a single spectra `data_product_id`. Source-agnostic. Profile-driven, IVOA-aligned.

**Input**: `nova_id` (required), `data_product_id` (required), `correlation_id` (optional)

**State machine** (after common prefix):
5. LoadDataProductMetadata
6. CheckOperationalStatus
7. AlreadyValidated? (validation_status==VALID) → FinalizeSuccess(`SKIPPED_DUPLICATE`)
8. CooldownActive? (now < next_eligible_attempt_at) → FinalizeSuccess(`SKIPPED_BACKOFF`)
9. AcquireArtifact — downloads bytes; if ZIP: unpack + select primary FITS via hints/heuristics
10. ValidateBytes (Profile-Driven):
    1. Open FITS
    2. Select profile (provider → header signature → no match=QUARANTINE)
    3. Extract spectral arrays via profile mapping
    4. Normalize headers to canonical fields
    5. Lightweight sanity checks (axis monotonicity, units, finiteness, wavelength range)
    6. Produce validation summary + fingerprint
11. DuplicateByFingerprint? (SHA-256 matches existing VALID product)
    → YES: RecordDuplicateLinkage (`duplicate_of_data_product_id=<canonical>`) → FinalizeSuccess(`DUPLICATE_OF_EXISTING`)
    → NO: continue
12. RecordValidationResult — persist lifecycle state, set `eligibility=NONE`, null GSI attributes
13. FinalizeSuccess(`VALIDATED`)
    ↕ errors → QuarantineHandler or TerminalFailHandler

**Cooldown enforcement** (before AcquireArtifact, also checked at steps 7-8):
- On RETRYABLE failure: increment `attempt_count`, compute `next_eligible_attempt_at = now + backoff(attempt_count)` (capped exponential; provider-tunable post-MVP)

**Retry policy**:
| State | Timeout | MaxAttempts | Backoff |
|---|---|---|---|
| LoadDataProductMetadata / CheckOperationalStatus | 10s | 3 | 2s,10s,30s |
| AcquireArtifact | 15m | 3 | 10s,60s,180s |
| ValidateBytes | 5m | 2 | internal transient only |
| RecordValidationResult | 20s | 3 | 2s,10s,30s |

**Idempotency key**: `AcquireAndValidateSpectra:{data_product_id}:{schema_version}`
Step dedupe keys: `Acquire:{data_product_id}:{locator_identity}`, `Validate:{data_product_id}:{content_fingerprint}`

**Persisted operational fields on DataProduct**: `validation_status`, `acquisition_status`, `attempt_count_total`, `last_attempt_at`, `last_attempt_outcome` (SUCCESS|RETRYABLE_FAILURE|TERMINAL_FAILURE|QUARANTINE), `last_error_fingerprint`, `next_eligible_attempt_at`, `last_successful_fingerprint`, `content_fingerprint`, `duplicate_of_data_product_id`

---

### ingest_photometry

**Purpose**: Ingest photometry source file via API; rebuild+overwrite canonical photometry table; persist provenance. No dataset abstraction.

**Input**: `candidate_name` OR `nova_id` (one required), `correlation_id` (optional). No `dataset_id`.

**State machine** (after common prefix):
3. ResolveNovaId (Task) — if input is `candidate_name`, via NameMapping
4. BeginJobRun / AcquireIdempotencyLock
5. CheckOperationalStatus
6. AlreadyIngested? → FinalizeSuccess(`SKIPPED_DUPLICATE`)
7. ValidatePhotometry
8. RebuildPhotometryTable
9. PersistPhotometryMetadata — updates: `current_s3_key`, `photometry_schema_version`, `last_ingestion_at`, `last_ingestion_source`, `ingestion_count`
10. FinalizeSuccess(`INGESTED`)

**Canonical overwrite**: routine ingestion always overwrites `derived/photometry/<nova_id>/photometry_table.parquet`. No snapshot.

**Schema-change snapshot**: if `photometry_schema_version` changes → copy existing canonical to `derived/photometry/<nova_id>/snapshots/schema=<old_version>/...` → write new canonical. Snapshots are schema-boundary markers only.

**Retry policy**:
| State | Timeout | MaxAttempts | Backoff |
|---|---|---|---|
| BeginJobRun/Lock/CheckStatus | 10s | 3 | 2s,10s,30s |
| ValidatePhotometry / RebuildPhotometryTable | 5m | 2 | transient only |
| PersistPhotometryMetadata | 30s | 3 | 2s,10s,30s |

**Quarantine**: data readable but invalid columns, invalid units, malformed timestamps, ambiguous/inconsistent data.
**Terminal**: missing required identifiers, malformed file, unsupported schema version.

**Idempotency key**: `IngestPhotometry:{nova_id}:{photometry_schema_version}`

---

### name_check_and_reconcile

**Purpose**: Post-establishment naming enrichment for existing `nova_id`. Checks for new official designations or aliases. Runs on limited schedule (~first 6 weeks after eruption). NOT a name-only front door.

**Input**: `nova_id` (required), `correlation_id` (optional)
**Output**: publishes `name_reconciled` event if updates occur

**State machine** (after common prefix):
5. FetchCurrentNamingState
6. QueryNamingAuthorities (Parallel: QueryAuthorityA, QueryAuthorityB)
7. ReconcileNaming
8. NamingChanged? → NO: FinalizeSuccess(`NO_CHANGE`)
9. ApplyNameUpdates
10. PublishNameReconciled (optional)
11. FinalizeSuccess(`UPDATED`)

**Retry policy**: BeginJobRun/Lock: 10s/3/2s,10s,30s. FetchCurrentNamingState: 20s/3/same. QueryAuthority*: 60s/3/same. ReconcileNaming: 20s/2 internal. ApplyNameUpdates: 20s/3/same. PublishNameReconciled: 10s/2.

**Quarantine**: conflicting authorities with ambiguous canonical designation; non-monotonic changes implying identity merge/split ambiguity.

**Idempotency key**: `NameCheckAndReconcile:{nova_id}:{schema_version}:{time_bucket}`
**Invariant**: names updated as enrichment only; UUID identity remains stable.

---

## 6. ACQUISITION COOLDOWN POLICY

Before each acquisition attempt on a `data_product_id`:
1. `validation_status == VALID` → short-circuit (`SKIPPED_DUPLICATE`)
2. `now < next_eligible_attempt_at` → short-circuit (`SKIPPED_BACKOFF`)
3. Else → proceed

On RETRYABLE failure:
- Increment `attempt_count_total`
- `next_eligible_attempt_at = now + backoff(attempt_count_total)` (capped exponential)

Checksum mismatch → QUARANTINE (not retryable). HTTP 429 → RETRYABLE.

QUARANTINE → human-gated; no auto-retry; requires `manual_review_status` update.
