# Nova Cat — Compressed Architecture Reference 1

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
