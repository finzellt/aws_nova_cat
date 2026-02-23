# DynamoDB Access Patterns (Epic 3)

This document lists the concrete DynamoDB read/write patterns that Nova Cat workflows must execute.
Scope: primarily **single-nova** workflows (partitioned by `nova_id`), with small global lookups for name resolution and S3-triggered ingestion.

## Table and Indexes

Primary table: `NovaCat`

Primary key:
- `PK` (string)
- `SK` (string)

Global secondary indexes (minimal set):
- **GSI1 (DatasetEligibilityIndex)**: query download-eligible datasets per nova
  - `GSI1PK` = `NOVA#<nova_id>`
  - `GSI1SK` = `ELIG#<eligibility>#<dataset_id>`
- **GSI2 (NameResolutionIndex)**: resolve candidate name/alias to `nova_id`
  - `GSI2PK` = `NAME#<normalized_name>`
  - `GSI2SK` = `NOVA#<nova_id>`
- **GSI3 (S3ObjectIndex)**: map an S3 object to its owning dataset/nova (for S3-triggered ingestion)
  - `GSI3PK` = `S3#<bucket>`
  - `GSI3SK` = `<key>`

Notes:
- Per-nova items live under `PK = NOVA#<nova_id>`.
- Name and S3 lookup items use separate PK namespaces (`NAME#...`) but remain in the same table for simplicity/cost.

---

## Access Patterns by Workflow

### 1) initialize_nova

**AP-INIT-1: Resolve candidate_name/alias -> nova_id**
- Query: `GSI2` where `GSI2PK = NAME#<normalized_name>`
- Return: one or more `nova_id` candidates
- Determinism rule: if multiple matches exist, resolution requires explicit reconciliation (human or deterministic rule); otherwise pick the single match.

**AP-INIT-2: Create new nova when not found**
- Write: `PutItem` `Nova` item under `PK=NOVA#<nova_id>`, `SK=META`
- Condition: `attribute_not_exists(PK)` AND `attribute_not_exists(SK)` (or a dedicated `nova_id` uniqueness check)

**AP-INIT-3: Attach/confirm alias mapping**
- Write: `PutItem` alias mapping item:
  - `PK = NAME#<normalized_name>`
  - `SK = NOVA#<nova_id>`
  - `GSI2PK/GSI2SK` populated
- Condition:
  - If you want strict uniqueness: `attribute_not_exists(PK)` AND `attribute_not_exists(SK)`
  - If you want “many-to-one allowed”: allow multiple aliases mapping to the same nova, but prevent alias mapping to multiple novae unless explicitly allowed.

---

### 2) ingest_new_nova (coordinator over nova_id)

**AP-INGEST-1: Read Nova**
- Get: `PK=NOVA#<nova_id>`, `SK=META`

**AP-INGEST-2: Write JobRun**
- Put: `JobRun` item under nova partition (see item model)
- Condition: none (job_run_id is UUID)

**AP-INGEST-3: Update coordinator state**
- Update: `Nova` item fields (e.g., last_ingest_at, status flags) via `UpdateItem`

---

### 3) refresh_references

**AP-REF-1: Upsert Reference entity (reference_id)**
- Put/Update: global-ish `Reference` item stored under nova partition OR reference partition.
  - Recommended (minimal per-nova scoping): store references under nova partition:
    - `PK=NOVA#<nova_id>`, `SK=REF#<reference_id>`
  - If references are reused across novae later, introduce `PK=REF#<reference_id>`.

**AP-REF-2: Link reference to nova (NovaReference)**
- Put: `PK=NOVA#<nova_id>`, `SK=NOVAREF#<reference_id>`
- Condition: optional `attribute_not_exists` to keep it idempotent

**AP-REF-3: Compute discovery_date**
- Read: query all `NOVAREF#...` (or `REF#...`) for the nova
- Write: update `Nova` with derived `discovery_date` (earliest relevant ref date)

---

### 4) discover_spectra_products

**AP-DISC-1: List existing datasets for a provider (dedupe)**
- Query: `PK=NOVA#<nova_id>`, `begins_with(SK, "DATASET#SPECTRA#<provider>#")`
- Used to avoid re-creating dataset_id for the same stable provider identity.

**AP-DISC-2: Create/Upsert spectra Dataset**
- Put: `Dataset` item
- Condition: `attribute_not_exists(PK)` AND `attribute_not_exists(SK)` when creating new; otherwise update locators/hints if provider re-publishes metadata.

**AP-DISC-3: Persist provider identifiers and locators**
- Update: on dataset item: provider identity, product locators (URLs/IDs), optional hints

**AP-DISC-4: Make dataset download-eligible**
- Update: dataset state fields AND set `GSI1PK/GSI1SK` accordingly (eligibility derived from state)

---

### 5) download_and_validate_spectra

**AP-DL-1: Query download-eligible datasets for nova**
- Query: `GSI1` where:
  - `GSI1PK = NOVA#<nova_id>`
  - `begins_with(GSI1SK, "ELIG#DOWNLOAD#")` (or exact `ELIG#DOWNLOAD#` prefix)
- Returns: dataset items already known to be eligible.

**AP-DL-2: Acquire a dataset “lease” / in-progress marker**
- Update: dataset item with:
  - `processing_status = IN_PROGRESS`
  - `lease_owner = <job_run_id>`
  - `lease_expires_at = now + N minutes`
- Condition:
  - Either `processing_status IN (NULL, READY, FAILED_RETRYABLE)` OR `lease_expires_at < now`
  - Prevents duplicate work under retries / concurrent executions.

**AP-DL-3: Write Attempt records per task invocation**
- Put: `Attempt` item per state machine task (download, validate, normalize, store, etc.)

**AP-DL-4: Persist file fingerprints/checksums**
- Update: dataset item (and/or FileObject items) with:
  - byte_length, etag, sha256, fits_checksum (if available), header_signature hash
- Used for short-circuiting and quarantine classification determinism.

**AP-DL-5: Persist validation results + quarantine reason**
- Update: dataset item with:
  - `validation_status` ∈ {VALID, QUARANTINED, TERMINAL_INVALID, RETRYABLE_ERROR}
  - `quarantine_reason_code` and details
  - `profile_selection` inputs/outputs

**AP-DL-6: Store S3 FileObject mappings**
- Put/Update: FileObject items under dataset (and GSI3 fields for reverse lookup by S3 key)

---

### 6) ingest_photometry_dataset (triggered by S3 object existence)

**AP-PHOT-1: Map S3 object -> owning dataset/nova**
- Query: `GSI3` with:
  - `GSI3PK = S3#<bucket>`
  - `GSI3SK = <key>` (exact match via Query or Get by a dedicated PK)
- Return: `nova_id`, `dataset_id`, dataset type

**AP-PHOT-2: Read Dataset + acquire lease**
- Get: dataset item, then same lease acquisition pattern as AP-DL-2

**AP-PHOT-3: Update dataset ingestion status**
- Update: dataset state, derived artifacts pointers, etc.

---

## Operational Traceability

**AP-OPS-1: List JobRuns for a nova**
- Query: `PK=NOVA#<nova_id>`, `begins_with(SK, "JOBRUN#")`

**AP-OPS-2: List Attempts for a JobRun**
- Query: `PK=NOVA#<nova_id>`, `begins_with(SK, "ATTEMPT#<job_run_id>#")`

**AP-OPS-3: Find a specific JobRun by job_run_id (optional)**
- If desired, add a small lookup item:
  - `PK=JOBRUN#<job_run_id>`, `SK=META` (or a GSI)
- This is optional; per-nova listing is usually enough.

---

## Determinism Notes (Why these access patterns exist)

- **Eligibility is queryable, not computed ad hoc**: GSI1 makes “download-eligible datasets” a direct query.
- **Retries can re-enter safely**: leases + fingerprints prevent double work and allow expired leases to be reclaimed.
- **Profile selection is reproducible**: header signature + hints + chosen profile are persisted and can be replayed.
