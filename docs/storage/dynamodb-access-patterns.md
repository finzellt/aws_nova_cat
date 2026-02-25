# DynamoDB Access Patterns

This document defines the conceptual read/write patterns that Nova Cat workflows rely on.

It is a workflow-facing contract.
Field definitions and item structure are defined in `dynamodb-item-model.md`.

---

## Table overview

The table is heterogeneous and uses namespaced `PK` values.

Two primary partition types exist:

1) Per-nova partitions
- `PK = "<nova_id>"`

2) Global identity partitions
- `PK = "NAME#<normalized_name>"`
- `PK = "LOCATOR#<provider>#<locator_identity>"`

Within per-nova partitions, item types are distinguished by `SK` prefixes such as:
- `NOVA`
- `PRODUCT#...`
- `FILE#...`
- `REF#...`
- `NOVAREF#...`
- `JOBRUN#...`
- `ATTEMPT#...`

---

## initialize_nova

Purpose: Resolve a candidate name to a stable `nova_id`, or create a new `Nova`.

### A) CheckExistingNovaByName

**Read**
- Query `NameMapping` partition:
  `PK = "NAME#<normalized_name>"`

**Outcome**
- If mapping exists → resolve to `nova_id`, publish `ingest_new_nova`, end (`EXISTS_AND_LAUNCHED`)

---

### B) CheckExistingNovaByCoordinates (if name not found)

**Read**
- Retrieve existing `Nova` coordinates (project-only): `nova_id`, `ra_deg`, `dec_deg`
- Compute angular separation in code
- Determine minimum separation

**Classification**
- `< 2"` → duplicate
- `2"–10"` → quarantine
- `> 10"` → new nova

---

### C) Duplicate (< 2")

**Write**
- Insert `NameMapping` (new alias → existing nova):
  `PK = "NAME#<normalized_name>"`
  `SK = "NOVA#<nova_id>"`

**Side effects**
- Publish `ingest_new_nova`
- Finalize (`EXISTS_AND_LAUNCHED`)

---

### D) Identity quarantine (2"–10")

**Write**
- Insert new Nova item with:
    - status = QUARANTINED
    - quarantine_reason_code = COORDINATE_AMBIGUITY
    - `PK = "<new_nova_id>"`, `SK = "NOVA"`

**Side effects**
- Finalize (`QUARANTINED`)
  *(Notification side effects are specified in workflow docs.)*

---

### E) Create new nova (> 10")

**Write**
- Insert `Nova` item:
  `PK = "<nova_id>"`, `SK = "NOVA"`

- Insert primary `NameMapping`:
  `PK = "NAME#<normalized_primary_name>"`, `SK = "NOVA#<nova_id>"`

**Side effects**
- Publish `ingest_new_nova`
- Finalize (`CREATED_AND_LAUNCHED`)

---

## ingest_new_nova

Purpose: Coordinate per-nova ingestion steps for an existing `nova_id`.

### Reads
- Read nova metadata:
  `PK = "<nova_id>"`, `SK = "NOVA"`

### Writes
- Insert photometry table data product if missing:
  `PK = "<nova_id>"`, `SK = "PRODUCT#PHOTOMETRY_TABLE"`

### Notes
- If `Nova.status != ACTIVE`, the workflow should short-circuit and finalize without launching downstream ingestion steps.

---

## refresh_references

Purpose: Upsert references and link them to the nova; optionally derive `discovery_date`.

### Reads (optional; for reconciliation/dedupe)
- Query references:
  `PK = "<nova_id>"`, `SK begins_with "REF#"`

- Query links:
  `PK = "<nova_id>"`, `SK begins_with "NOVAREF#"`

### Writes
- Upsert `Reference` items:
  `PK = "<nova_id>"`, `SK = "REF#<reference_id>"`

- Upsert `NovaReference` items:
  `PK = "<nova_id>"`, `SK = "NOVAREF#<reference_id>"`

- Update `Nova.discovery_date` when derivable

---

## discover_spectra_products

Purpose: Discover spectra products across providers, assign stable `data_product_id`s, persist stubs, and publish acquisition requests.

### Reads (per discovered product)
- Query locator alias mapping:
  `PK = "LOCATOR#<provider>#<locator_identity>"`
  → determines whether a stable `data_product_id` already exists

### Writes (per discovered product)
- Generate deterministic `data_product_id` from provider identity fields; insert `LocatorAlias` if not present.
  - Insert `LocatorAlias`:
    `PK = "LOCATOR#<provider>#<locator_identity>"`
    `SK = "DATA_PRODUCT#<data_product_id>"`

- Upsert spectra `DataProduct` stub:
  `PK = "<nova_id>"`
  `SK = "PRODUCT#SPECTRA#<provider>#<data_product_id>"`

  Stub state includes:
  - `acquisition_status = STUB`
  - `validation_status = UNVALIDATED`
  - `eligibility = ACQUIRE`
  - cooldown fields initialized (attempt_count = 0, etc.)
  - eligibility index attributes present (so it can appear in the eligibility index)

### Side effects
- Publish one `acquire_and_validate_spectra` request per `data_product_id`

---

## acquire_and_validate_spectra

Purpose: Acquire bytes for a single spectra product, validate via profile selection, persist outcomes, and enforce cooldown/backoff.

### Reads
- Read spectra `DataProduct`:
  `PK = "<nova_id>"`, `SK = "PRODUCT#SPECTRA#<provider>#<data_product_id>"`

### Decision inputs (from the product record)
- Already validated? (`validation_status == VALID`)
- Cooldown active? (`now < next_eligible_attempt_at`)
- Quarantine gate? (`validation_status == QUARANTINED`)

### Writes
- Write JobRun and Attempt operational records for traceability:
  - Insert/Update `JobRun` (one per workflow execution)
  - Insert `Attempt` records (one per task invocation, including retries)

- Update per-product cooldown fields during acquisition attempts:
  - increment `attempt_count`
  - set `last_attempt_at`
  - set `last_error_fingerprint` on failure
  - set `next_eligible_attempt_at` on retryable failure

- After `ValidateBytes` (i.e., once a definitive classification is known):
  - Persist fingerprints/checksums and profile selection outputs as applicable
  - Update lifecycle state (`validation_status`, `acquisition_status`, etc.)
  - Set `eligibility = NONE`
  - Remove eligibility index attributes so the product no longer appears in the eligibility index

### Optional query pattern (admin/repair)
- List eligible spectra products for a nova via eligibility index:
  - Query `GSI1PK = "<nova_id>"`
  - `GSI1SK begins_with "ELIG#ACQUIRE#SPECTRA#"`

Note: In MVP, executions are normally launched from discovery output; this query supports repair/sweeps.

---

## ingest_photometry

Purpose: Ingest a new photometry source file via API, update the per-nova photometry table product, and write derived artifacts to S3.

There is no dataset abstraction.

### Reads

Resolve input name → `nova_id` via `NameMapping`:

PK = "NAME#<normalized_name>"

Read photometry table `DataProduct`:

PK = "<nova_id>"
SK = "PRODUCT#PHOTOMETRY_TABLE"

### Writes

Update photometry table `DataProduct` in place:

- `current_s3_key`
- `photometry_schema_version`
- `last_ingestion_at`
- `last_ingestion_source`
- `ingestion_count`

### Canonical Overwrite Behavior

For routine ingestion under the same schema version:

- Rebuild and overwrite the canonical photometry table key:

derived/photometry/<nova_id>/photometry_table.parquet

### Schema-Change Snapshot Behavior (Forward-Compatible)

If the photometry schema version changes:

1. Copy the existing canonical table to an immutable snapshot key:

derived/photometry/<nova_id>/snapshots/schema=<old_schema_version>/...

2. Write the new canonical table using the new schema version.

Snapshots are created only for schema migrations, not for normal ingestion.

### Optional

Insert `FileObject` entries for:

- Raw uploaded file
- Split per-nova file (if applicable)
- Derived artifacts

---

## Operational access patterns (debug/admin)

### List all data products for a nova (photometry + spectra)
- `PK = "<nova_id>"`, `SK begins_with "PRODUCT#"`

### List spectra products for a nova
- `PK = "<nova_id>"`, `SK begins_with "PRODUCT#SPECTRA#"`

### List JobRuns for a nova
- `PK = "<nova_id>"`, `SK begins_with "JOBRUN#"`

### List Attempts (all for a nova)
- `PK = "<nova_id>"`, `SK begins_with "ATTEMPT#"`

### List Attempts for a specific JobRun
- `PK = "<nova_id>"`, `SK begins_with "ATTEMPT#<job_run_id>#"`

### List references and reference links
- `PK = "<nova_id>"`, `SK begins_with "REF#"`
- `PK = "<nova_id>"`, `SK begins_with "NOVAREF#"`
