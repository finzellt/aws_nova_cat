# DynamoDB Access Patterns

This document defines the conceptual read/write patterns that Nova Cat workflows rely on.

It is a workflow-facing contract.
Field definitions and item structure are defined in `dynamodb-item-model.md`.

---

## Table overview

The table is heterogeneous and uses namespaced `PK` values.

Three primary partition types exist:

1) Per-nova partitions
- `PK = "<nova_id>"`

2) Global identity partitions
- `PK = "NAME#<normalized_name>"`
- `PK = "LOCATOR#<provider>#<locator_identity>"`
- `PK = "REFERENCE#<bibcode>"`
- `PK = "WORKFLOW#<correlation_id>"` — pre-nova workflow artifacts written before a
  `nova_id` exists (e.g. `FileObject` records during `initialize_nova` quarantine)
- `PK = "WORKQUEUE"` — artifact regeneration work orders (ADR-031 Decision 7,
  DESIGN-003 §3)

Within per-nova partitions, item types are distinguished by `SK` prefixes such as:
- `NOVA`
- `PRODUCT#...`
- `FILE#...`
- `NOVAREF#...`
- `JOBRUN#...`
- `ATTEMPT#...`

The `WORKQUEUE` partition uses a different SK structure:
- `<nova_id>#<dirty_type>#<created_at>` — ordered for per-nova grouping

### Global Secondary Index: EligibilityIndex (GSI1)

The table has one GSI used to identify spectra products that are ready to acquire:

- **GSI1PK** = `<nova_id>` — scopes the query to a single nova
- **GSI1SK** = `ELIG#<eligibility>#SPECTRA#<provider>#<data_product_id>`

Key values:
- `eligibility = ACQUIRE` → GSI1 attributes are **present** on the item; product appears in the index
- `eligibility = NONE` → GSI1 attributes are **absent** (removed); product drops off the index

This is the core signal that controls whether a spectra product is eligible for
`acquire_and_validate_spectra`. It is set by `discover_spectra_products` on stub creation
and cleared by `acquire_and_validate_spectra` on any terminal outcome (VALID, QUARANTINED,
TERMINAL_INVALID, or SKIPPED_*). See dynamodb-item-model.md §3.2 for the full field spec.

> **`data_product_id` — stable, deterministic UUID (SPECTRA products)**
>
> Minted during `discover_spectra_products`. Derived as follows:
> - **Preferred:** `UUID(hash(provider + provider_product_key))` — when a provider-native product ID is available.
> - **Fallback:** `UUID(hash(provider + normalized_canonical_locator))` — when no native ID exists.
>
> Immutable once assigned; never reused across distinct products.
> Appears in spectra `SK` patterns as `PRODUCT#SPECTRA#<provider>#<data_product_id>`.
> See ADR-003 for full specification.

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

- Write `FileObject` quarantine context record (before `nova_id` is confirmed):
  `PK = "WORKFLOW#<correlation_id>"`
  `SK = "FILE#WORKFLOW_QUARANTINE_CONTEXT#ID#<file_id>"`
  *(Uses `correlation_id` as partition key because no stable `nova_id` exists at write time.
  See dynamodb-item-model.md §5 for the full FileObject key table.)*

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

- Update `Nova.aliases` with the raw alias list from archive resolution:
  `PK = "<nova_id>"`, `SK = "NOVA"` (same item, SET aliases = :aliases)

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
- Query existing nova-reference links:
  `PK = "<nova_id>"`, `SK begins_with "NOVAREF#"`

### Writes
- Upsert global `Reference` items (one per bibcode; shared across all novas):
  `PK = "REFERENCE#<bibcode>"`, `SK = "METADATA"`

- Upsert `NovaReference` link items (nova-scoped):
  `PK = "<nova_id>"`, `SK = "NOVAREF#<bibcode>"`

- Update `Nova.discovery_date` when derivable

---

## discover_spectra_products

Purpose: Discover spectra products across providers, assign stable `data_product_id`s, persist stubs, and publish acquisition requests.

`data_product_id` is minted here via deterministic derivation:
`UUID(hash(provider + provider_product_key))` (preferred) or
`UUID(hash(provider + normalized_canonical_locator))` (fallback). See ADR-003.

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
  - `eligibility = ACQUIRE` ← marks this product as ready to acquire
  - cooldown fields initialized (`attempt_count = 0`, `next_eligible_attempt_at = null`, etc.)
  - GSI1 attributes written so the product appears in the EligibilityIndex:
    - `GSI1PK = "<nova_id>"`
    - `GSI1SK = "ELIG#ACQUIRE#SPECTRA#<provider>#<data_product_id>"`

  A product that resolves to an existing `data_product_id` that is already
  `VALID` must **not** have a new acquisition request published for it, and its
  `eligibility` must remain `NONE` (GSI1 attributes absent).

### Side effects
- Publish one `acquire_and_validate_spectra` event per newly eligible `data_product_id`

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
  - **Remove GSI1 attributes** (`GSI1PK`, `GSI1SK`) so the product drops off the
    EligibilityIndex and cannot be re-acquired by a future sweep

### EligibilityIndex query (core acquisition trigger pattern)

In MVP, `acquire_and_validate_spectra` executions are launched directly from
`discover_spectra_products` output events — the GSI is not used for normal dispatch.

The GSI exists to support:
- **Repair / retry sweeps**: re-queue all eligible products for a nova after an outage
- **Operator-triggered re-ingestion**: list what still needs acquiring without scanning all products

```
Query GSI1PK = "<nova_id>"
      GSI1SK begins_with "ELIG#ACQUIRE#SPECTRA#"
```

Returns all spectra products for the nova with `eligibility = ACQUIRE`, ordered by
`<provider>#<data_product_id>`. An empty result means all products are either
validated, quarantined, or terminal — nothing left to acquire.

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

- `s3_bucket`
- `s3_key`
- `last_ingestion_at`
- `last_ingestion_source`
- `ingestion_count`

Note: `photometry_schema_version` is not a persisted field on `DataProduct`. If schema
versioning is ever required, a typed field must be added to the contract first.

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

## name_check_and_reconcile

Purpose: Validate and reconcile the canonical name and aliases for an existing nova.

### Reads
- Read nova metadata:
  `PK = "<nova_id>"`, `SK = "NOVA"`

- Query existing `NameMapping` entries for this nova:
  *(via GSI or scan of known aliases on the Nova item)*

### Writes
- Upsert `NameMapping` items for any new or corrected aliases:
  `PK = "NAME#<normalized_alias>"`, `SK = "NOVA#<nova_id>"`

- Update `Nova.primary_name` / `Nova.primary_name_normalized` if a canonical name
  change is approved

### Notes
- `proposed_public_name` and `proposed_aliases` are passed via `attributes` on the
  boundary event, not as typed fields. See `NameCheckAndReconcileEvent` in `events.py`.
- This workflow operates entirely downstream of `initialize_nova`; it does not perform
  coordinate-based identity resolution.

---

## Artifact regeneration pipeline (DESIGN-003 §3–§4)

Purpose: Signal which novae have new data so the regeneration pipeline
knows which artifacts to rebuild.

### Write WorkItem (ingestion workflows → WORKQUEUE)

After scientific data is persisted, each ingestion workflow writes a WorkItem:
```
PutItem:
  PK = "WORKQUEUE"
  SK = "<nova_id>#<dirty_type>#<created_at>"
```

| Workflow | dirty_type |
|---|---|
| `acquire_and_validate_spectra` (VALID outcome) | `spectra` |
| `ingest_ticket` (spectra branch) | `spectra` |
| `ingest_ticket` (photometry branch) | `photometry` |
| `refresh_references` | `references` |

Best-effort: a failed write logs a warning but does not fail the ingestion.

### Read all pending WorkItems (coordinator sweep)
```
Query:
  PK = "WORKQUEUE"
```

Returns all pending WorkItems across all novae. The coordinator groups
by `nova_id` (extracted from the SK prefix) and derives per-nova
regeneration manifests using the dirty_type → artifact dependency matrix
(DESIGN-003 §3.4).

### Read WorkItems for a specific nova
```
Query:
  PK = "WORKQUEUE"
  SK begins_with "<nova_id>#"
```

Useful for operator diagnosis: check what changes are pending for a
specific nova.

### Delete consumed WorkItems (after successful regeneration)
```
BatchWriteItem (DeleteRequest):
  PK = "WORKQUEUE"
  SK = <exact SK from the batch plan's workitem_sks list>
```

Only the WorkItems that were present when the coordinator built the
batch plan are deleted — not any that arrived during execution.

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

### List reference links for a nova
- `PK = "<nova_id>"`, `SK begins_with "NOVAREF#"`
- SK suffix is the ADS bibcode (e.g. `NOVAREF#2013ATel.5297....1W`)

### Look up a specific reference by bibcode
- `PK = "REFERENCE#<bibcode>"`, `SK = "METADATA"`
- Direct `GetItem` — no scan or query required
