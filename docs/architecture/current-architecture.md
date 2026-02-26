# Nova Cat — Current Architecture Snapshot

_Last Updated: 2026-02-26_

This document captures the **authoritative architectural baseline** of Nova Cat at this point in time.

It defines:
- System boundaries
- Core domain model
- Workflow architecture
- Persistence model
- Identity rules
- Validation strategy
- Observability model

This document supersedes informal descriptions elsewhere.
If drift is detected between artifacts, this file represents intended reality.

---

# 1. System Overview

Nova Cat is a serverless AWS application for aggregating, validating, and publishing classical nova data.

Core characteristics:

- Python-based
- AWS serverless (Lambda, Step Functions, DynamoDB, S3)
- UUID-first identity
- Contract-first design (Pydantic → JSON Schema)
- Atomic spectra data products
- Profile-driven validation
- IVOA-aligned canonical representation
- Low throughput, cost-aware architecture

The system is designed for **singular nova ingestion** in MVP.

---

# 2. Identity Model

## 2.1 Stable Identifiers

All persistent entities use UUIDs:

- `nova_id`
- `data_product_id`
- `reference_id`

Names are never used as identifiers downstream of `initialize_nova`.

---

## 2.2 Name Resolution

Name-based resolution occurs only in `initialize_nova`.

Mechanism:
- `NAME#<normalized_name>` global partition
- Maps alias → `nova_id`
- Deterministic normalization applied

Duplicate detection via coordinate thresholds:

| Separation | Action |
|------------|--------|
| < 2"       | Attach alias to existing nova |
| 2–10"      | Identity quarantine |
| > 10"      | Create new nova |

Identity quarantine persists as:

`Nova.status = QUARANTINED`


---

## 2.3 Locator Identity

Each spectra product identity is deterministic:

```
identity_key = hash(provider + provider_product_key + canonical_locator)
data_product_id = UUID(identity_key)
```


`LOCATOR#<provider>#<locator_identity>` ensures stable deduplication.

---

# 3. Domain Model (Persistent Entities)

## 3.1 Nova

Fields include:

Fields include:

- nova_id
- primary_name
- primary_name_normalized
- status
- position (ra_deg, dec_deg, frame, epoch)
- discovery_date

Derived astronomical metadata (e.g., constellation) is not persisted.

---

## 3.2 DataProduct (Spectra)

Atomic unit of ingestion and validation.

Each product has independent:

- acquisition_state
- validation_state
- cooldown metadata
- fingerprint (sha256)
- header signature hash
- selected_profile
- quarantine_reason_code (if applicable)

No dataset abstraction exists.

---

## 3.3 Photometry Table

One logical photometry table exists per nova.

The photometry table is modeled as a `DataProduct` of type `PHOTOMETRY_TABLE` and is stored at a stable canonical S3 key.

### Canonical Behavior (MVP)

- The photometry table is rebuilt and overwritten **in place** on each ingestion.
- There is exactly one authoritative current table per nova.
- No snapshotting occurs during routine ingestion under the same schema version.

### Schema Versioning Policy

Photometry versioning is triggered **only when the photometry schema version changes**.

When a schema change occurs:

1. The existing canonical table is copied to an immutable snapshot location.
2. A new canonical table is written using the new schema version.

Snapshots are therefore:
- Schema-boundary artifacts
- Immutable
- Not created during normal ingestion

In MVP:
- `photometry_schema_version` is fixed.
- Schema migration workflows are documented but may not yet be implemented.

DynamoDB stores:
- The canonical S3 key
- The current `photometry_schema_version`
- Ingestion summary metadata

---

## 3.4 Reference

Global bibliographic entity.

Linked to nova via `NOVAREF` items.

---

## 3.5 Operational Records

### JobRun
One per workflow execution.

### Attempt
One per task invocation (including retries).

Operational state is separate from scientific state.

---

# 4. Workflow Architecture

All workflows operate on UUIDs only.

## 4.1 initialize_nova
- Accepts candidate_name
- Performs name resolution
- Creates nova if necessary
- Launches ingest_new_nova

Terminal outcomes:
- CREATED_AND_LAUNCHED
- EXISTS_AND_LAUNCHED
- NOT_FOUND
- NOT_A_CLASSICAL_NOVA

---

## 4.2 ingest_new_nova
Coordinator:
- refresh_references
- discover_spectra_products

---

## 4.3 discover_spectra_products
- Map across providers
- Adapter-based discovery
- Assign data_product_id
- Publish AcquireAndValidateSpectra continuation event

---

## 4.4 acquire_and_validate_spectra
- Download bytes
- Unzip if necessary
- Compute fingerprint
- Select FITS profile
- Normalize to canonical model
- Classify outcome (RETRYABLE / TERMINAL / QUARANTINE)
- Persist results

Eligibility index removed immediately after validation.

---

## 4.5 refresh_references
- Upsert references
- Link nova to references
- Compute discovery_date

---

## 4.6 ingest_photometry

- Accepts API-driven photometry upload
- Resolves name → `nova_id`
- Rebuilds and overwrites the canonical photometry table
- Updates ingestion summary fields
- If schema version changes (future capability), snapshots prior table before overwrite

No dataset abstraction exists.

---

# 5. Validation Architecture

Spectra validation is profile-driven:

Layered model:

1. Provider Adapter (discovery + acquisition)
2. FITS Profile (header normalization rules)
3. Generic Validator (IVOA-aligned canonicalization)

Unknown profile or missing critical metadata → QUARANTINE.

IVOA Spectrum DM / ObsCore conventions used where possible.

---

# 6. Persistence Model

Single DynamoDB table:

## Per-nova partition

```
PK = <nova_id>
```

Item types:
- NOVA
- PRODUCT#...
- FILE#...
- REF#...
- NOVAREF#...
- JOBRUN#...
- ATTEMPT#...

## Global identity partitions
- NAME#<normalized_name>
- LOCATOR#<provider>#<locator_identity>

S3 layout:
- raw/
- derived/
- quarantine/
- bundles/
- site/releases/

Photometry canonical key:
- derived/photometry/<nova_id>/photometry_table.parquet

Photometry snapshots (schema change only):
- derived/photometry/<nova_id>/snapshots/schema=<old_schema_version>/...

---

# 7. Execution Governance

Failure taxonomy:

- RETRYABLE
- TERMINAL
- QUARANTINE

Idempotency:
- Internal only
- Enforced via JobRun + Attempt records

Correlation ID:
- Optional in boundary schemas
- Propagated across workflows

No idempotency keys exposed in boundary schemas.

---

# 8. Observability Model

Structured logs include:

- workflow_name
- execution_arn
- job_run_id
- state_name
- attempt_number
- correlation_id
- primary UUID(s)
- error_classification
- error_fingerprint

Metrics:
- Success / failure / quarantine counts
- Retry rate
- Provider health
- Latency

---

# 9. Architectural Invariants

The following must remain true:

- UUID-first downstream execution
- No name-based operations beyond resolution boundary
- Atomic spectra products
- Explicit quarantine semantics
- Deterministic locator identity
- Operational state separate from scientific state
- Continuation payload event model
- Minimal Step Functions branching
- Profile-driven validation

---

# 10. Deferred / Non-MVP

- Global multi-nova sweeps
- Spatial indexing
- Full VO compliance enforcement
- Advanced photometry version diffing
- Provider auto-discovery scaling

---

# End of Snapshot
