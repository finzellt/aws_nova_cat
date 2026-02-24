# DynamoDB Item Model (Epic 3)

This document defines the DynamoDB item types, keys, fields, and examples for Nova Cat persistence **after the spectra refactor**:
- **Spectra** are atomic **data products** identified by `data_product_id` (one per spectra file/product).
- **Photometry** has exactly **one** main `PHOTOMETRY_TABLE` data product per nova (one DynamoDB item), updated in place over time.
- “Dataset” is not a persisted concept for MVP.
- Ordering requirement: within a nova, items should be sortable by **product type first**, then provider (for spectra).

### Design goals:
- Minimal, cost-conscious, access-pattern driven
- Primarily **per-nova partitioned** by `nova_id`
- Deterministic retries via **cooldown/backoff fields** on each spectra data product
- QUARANTINE is **human-gated** (no auto-retries)
- Large bytes/artifacts live in S3; DynamoDB stores structured metadata and pointers

---

## Table and Keys

### Table: `NovaCat`

### Primary key:
- `PK` (string)
- `SK` (string)

---

### Conventions:
- Per-nova partition uses raw UUID:
  - `PK = "<nova_id>"`
- Global mapping items use non-UUID `PK` namespaces (still in the same table).

---

### Minimal GSIs:
- **GSI1 (EligibilityIndex)** for *per-nova* “eligible spectra products” queries
  - `GSI1PK = "<nova_id>"`
  - `GSI1SK = "ELIG#<eligibility>#<product_type>#<provider>#<data_product_id>"`

---

### Notes:
- We intentionally **do not** add a dedicated “human name” GSI. Humans search by name through `NameMapping` items (primary name and aliases).

---

## Item Types

### 1) Nova

Canonical nova record.

#### Key:
- `PK = "<nova_id>"`
- `SK = "NOVA"`

---

#### Fields (suggested):
- `entity_type = "Nova"`
- `schema_version` (internal item evolution)
- `nova_id` (UUID string)
- `primary_name` (mutable)
- `primary_name_normalized` (deterministic normalization for lookups)
- `ra_deg` (float; ICRS right ascension in degrees)
- `dec_deg` (float; ICRS declination in degrees)
- `coord_frame` (string; e.g., `"ICRS"`; optional but recommended for explicitness)
- `coord_epoch` (string; e.g., `"J2000"`; optional but recommended for explicitness)
- `discovery_date` (optional, derived from references)
- `status` (ACTIVE | MERGED | DEPRECATED)
- `created_at`, `updated_at` (ISO-8601 UTC)

#### Example:
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "NOVA",
  "entity_type": "Nova",
  "schema_version": "1",
  "nova_id": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "primary_name": "V1324 Sco",
  "primary_name_normalized": "v1324 sco",
  "status": "ACTIVE",
  "discovery_date": "2012-06-01",
  "created_at": "2026-02-21T20:00:00Z",
  "updated_at": "2026-02-23T18:10:00Z"
}
```




### 2) NameMapping (primary name and aliases → nova_id)

Used for:

- API inputs that arrive as `primary_name` or alias
- `initialize_nova` resolution
- Human lookup

#### Key

- `PK = "NAME#<normalized_name>"`
- `SK = "NOVA#<nova_id>"`

---

#### Fields

- `entity_type = "NameMapping"`
- `schema_version` (internal item evolution)
- `name_raw` (original display string)
- `name_normalized`
- `name_kind` (`PRIMARY` | `ALIAS`)
- `nova_id`
- `source` (`USER_INPUT` | `INGESTION` | `SIMBAD` | `TNS` | `OTHER`)
- `created_at`
- `updated_at`

#### Example (primary):

```json
{
  "PK": "NAME#nova sco 2012 #2",
  "SK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "entity_type": "NameMapping",
  "schema_version": "1",
  "name_raw": "V1324 Sco",
  "name_normalized": "v1324 sco",
  "name_kind": "PRIMARY",
  "nova_id": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "source": "INGESTION",
  "created_at": "2026-02-21T20:00:00Z",
  "updated_at": "2026-02-21T20:00:00Z"
}
```


#### Normalization note

We store both `primary_name` and `primary_name_normalized` to keep lookups deterministic while preserving display formatting.

---

### 3) DataProduct (core unit of work)

There are two product types:

- `PHOTOMETRY_TABLE` (exactly one per nova; single DynamoDB item)
- `SPECTRA` (many per nova; one DynamoDB item per atomic spectra product)

---

#### 3.1 Photometry table data product (one per nova)

##### Key

- `PK = "<nova_id>"`
- `SK = "PRODUCT#PHOTOMETRY_TABLE"`

---

##### Fields (suggested)

- `entity_type = "DataProduct"`
- `schema_version` (internal item evolution)
- `data_product_id` (stable UUID)
- `product_type = "PHOTOMETRY_TABLE"`

###### S3 pointers

- `s3_bucket`
- `s3_key` (current parquet object)

###### Ingestion summary

- `last_ingestion_at`
- `last_ingestion_source` (small descriptor or pointer)
- `ingestion_count` (optional)

###### Optional “pseudo-versioning”

- `last_ingested_file_id`
  **or**
- `last_ingested_object_etag` (low-cardinality)

#### Example:
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "PRODUCT#PHOTOMETRY_TABLE",
  "entity_type": "DataProduct",
  "schema_version": "1",
  "data_product_id": "111f1a33-8f2d-4a72-a9c5-2ec8a39edb9e",
  "nova_id": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "product_type": "PHOTOMETRY_TABLE",
  "s3_bucket": "nova-cat-private-data",
  "s3_key": "derived/photometry/4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1/photometry_table.parquet",
  "last_ingestion_at": "2026-02-23T18:00:00Z",
  "last_ingestion_source": "api_upload:amateur_batch_2026_02",
  "ingestion_count": 3,
  "created_at": "2026-02-21T20:00:05Z",
  "updated_at": "2026-02-23T18:00:00Z"
}
```


#### 3.2 Spectra data product (atomic; many per nova)

##### Key (product type first, then provider)

- `PK = "<nova_id>"`
- `SK = "PRODUCT#SPECTRA#<provider>#<data_product_id>"`

---

##### Fields (suggested)

###### Identity

- `schema_version` (internal item evolution)
- `data_product_id` (stable, deterministically generated UUID; see ADR-003)
- `product_type = "SPECTRA"`
- `provider` (string; may be `DONATION` or similar for donated sources)

###### Locators (source-agnostic)

- `locators` (list; typically one entry MVP)

Example locator object:

```json
{ "kind": "URL" | "S3" | "OTHER", "value": "...", "role": "PRIMARY" | "MIRROR" }
```

- `locator_identity`
  The normalized identity used for alias mapping (see `LocatorAlias`).

---

#### Acquisition / validation lifecycle

- `validation_status`
  (`UNVALIDATED` | `VALID` | `QUARANTINED` | `TERMINAL_INVALID`)

- `acquisition_status`
  (`STUB` | `ACQUIRED` | `FAILED_RETRYABLE` | `SKIPPED_DUPLICATE` | `SKIPPED_BACKOFF`)

- `eligibility`
  (`ACQUIRE` | `NONE`)
  → Drives `GSI1`

---

#### Cooldown / backoff fields
*(per-product; provider-level deferred post-MVP)*

- `attempt_count` (int)
- `last_attempt_at` (ISO-8601 UTC)
- `next_eligible_attempt_at` (ISO-8601 UTC)
- `last_error_fingerprint` (low-cardinality identifier)

---

#### Fingerprints / checksums
*(populated after acquisition)*

- `byte_length`
- `etag` (if applicable)
- `sha256` (preferred)
- `header_signature_hash` (stable hash of selected header cards)

---

#### Profile-driven validation

- `fits_profile_id`
  (string; exact selected profile id/version)

- `profile_selection_inputs`
  (small object: provider + hints + signature hash)

- `normalization_notes`
  (small list; large logs stored in CloudWatch/S3)

---

#### QUARANTINE gating

- `quarantine_reason_code`
  (`UNKNOWN_PROFILE` | `MISSING_CRITICAL_METADATA` | `CHECKSUM_MISMATCH` | `OTHER`)

- `manual_review_status`
  (`PENDING` | `CLEARED_RETRY_APPROVED` | `CLEARED_TERMINAL`)
  *(optional but recommended)*

---

#### S3 pointers (raw and derived)

- `raw_s3_bucket`
- `raw_s3_key`
- `derived_s3_prefix` (optional)

---

#### Format / provenance hints

- `hints`
  (instrument / telescope / pipeline tag / etc.; optional)

- `provenance`
  (small object; donated data requires this)

---

#### Eligibility index fields

- `GSI1PK = "<nova_id>"`
- `GSI1SK = "ELIG#<eligibility>#SPECTRA#<provider>#<data_product_id>"`

---

#### Example (discovered stub):
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "PRODUCT#SPECTRA#ESO#2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9",
  "entity_type": "DataProduct",
  "schema_version": "1",
  "data_product_id": "2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9",
  "nova_id": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "product_type": "SPECTRA",
  "provider": "ESO",
  "locator_identity": "provider_product_id:SPEC12345",
  "locators": [
    { "kind": "URL", "role": "PRIMARY", "value": "https://example.provider/spectra/SPEC12345.fits" }
  ],
  "hints": { "instrument": "UVES", "pipeline_tag": "v3" },
  "acquisition_status": "STUB",
  "validation_status": "UNVALIDATED",
  "eligibility": "ACQUIRE",
  "attempt_count": 0,
  "created_at": "2026-02-23T18:05:00Z",
  "updated_at": "2026-02-23T18:05:00Z",
  "GSI1PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "GSI1SK": "ELIG#ACQUIRE#SPECTRA#ESO#2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9"
}
```


#### Example (validated):
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "PRODUCT#SPECTRA#ESO#2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9",
  "entity_type": "DataProduct",
  "schema_version": "1",
  "data_product_id": "2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9",
  "product_type": "SPECTRA",
  "provider": "ESO",
  "locator_identity": "provider_product_id:SPEC12345",
  "locators": [
    { "kind": "URL", "role": "PRIMARY", "value": "https://example.provider/spectra/SPEC12345.fits" }
  ],
  "acquisition_status": "ACQUIRED",
  "validation_status": "VALID",
  "eligibility": "NONE",
  "attempt_count": 1,
  "last_attempt_at": "2026-02-23T18:10:00Z",
  "next_eligible_attempt_at": null,
  "last_error_fingerprint": null,
  "byte_length": 184233,
  "etag": "\"9b2cf535f27731c974343645a3985328\"",
  "sha256": "0c3b...a91f",
  "header_signature_hash": "hsig:2f91...aa1c",
  "fits_profile_id": "eso_uves_v3@1.0.0",
  "profile_selection_inputs": {
    "provider": "ESO",
    "hints": { "instrument": "UVES", "pipeline_tag": "v3" },
    "header_signature_hash": "hsig:2f91...aa1c"
  },
  "normalization_notes": ["Mapped WAVE axis from ESO keyword set; units Å -> m."],
  "raw_s3_bucket": "nova-cat-private-data",
  "raw_s3_key": "raw/spectra/4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1/2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9/primary.fits",
  "updated_at": "2026-02-23T18:12:00Z",
  "GSI1PK": null,
  "GSI1SK": null
}
```

#### Example (quarantined; human gated):
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "PRODUCT#SPECTRA#AMATEUR#0a77c1f5-5b9a-44d0-9f30-1b3c4f6ad111",
  "entity_type": "DataProduct",
  "schema_version": "1",
  "data_product_id": "0a77c1f5-5b9a-44d0-9f30-1b3c4f6ad111",
  "product_type": "SPECTRA",
  "provider": "AMATEUR",
  "locator_identity": "url:https://example.org/uploads/spectrum_001.fits",
  "locators": [
    { "kind": "URL", "role": "PRIMARY", "value": "https://example.org/uploads/spectrum_001.fits" }
  ],
  "acquisition_status": "ACQUIRED",
  "validation_status": "QUARANTINED",
  "eligibility": "NONE",
  "attempt_count": 1,
  "last_attempt_at": "2026-02-23T18:20:00Z",
  "last_error_fingerprint": "VALIDATION_UNKNOWN_PROFILE",
  "quarantine_reason_code": "UNKNOWN_PROFILE",
  "manual_review_status": "PENDING",
  "updated_at": "2026-02-23T18:25:00Z"
}
```

---

### 4) LocatorAlias (provider + locator_identity → data_product_id)

Used during `discover_spectra_products` to:

- Detect “we already know this spectra product”
- Enforce stable identity even if multiple equivalent locators exist

---

#### Key

- `PK = "LOCATOR#<provider>#<locator_identity>"`
- `SK = "DATA_PRODUCT#<data_product_id>"`

---

#### Fields

- `schema_version` (internal item evolution)
- `entity_type = "LocatorAlias"`
- `provider`
- `locator_identity` (normalized)

Locator identity normalization rules:

- Preferred: `provider_product_id:<id>`
- Else: `url:<normalized_url>`

- `data_product_id`
- `nova_id` (stored as an attribute for traceability)
- `created_at`
- `updated_at`

#### Example:
```json
{
  "PK": "LOCATOR#ESO#provider_product_id:SPEC12345",
  "SK": "DATA_PRODUCT#2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9",
  "entity_type": "LocatorAlias",
  "schema_version": "1",
  "provider": "ESO",
  "locator_identity": "provider_product_id:SPEC12345",
  "data_product_id": "2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9",
  "nova_id": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "created_at": "2026-02-23T18:05:00Z",
  "updated_at": "2026-02-23T18:05:00Z"
}
```

---

### 5) FileObject (optional registry for S3 objects)

Recommended if you want strong provenance of stored objects and easy listing of raw/derived artifacts per product.

#### Key

- `PK = "<nova_id>"`
- `SK = "FILE#<product_type>#<data_product_id>#<role>#<name_or_id>"`

---

#### Fields

- `schema_version` (internal item evolution)
- `entity_type = "FileObject"`
- `data_product_id`
- `product_type`
- `role`
  (`RAW_FITS` | `QUARANTINE_CONTEXT` | `NORMALIZED` | `PLOT` | `MANIFEST` | `DATA_BUNDLE` | `OTHER`)
- `bucket`
- `key`
- `content_type`
- `byte_length`
- `etag`
- `sha256`
- `created_by` (workflow + `job_run_id`)
- `created_at`
- `updated_at`

#### Example:
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "FILE#SPECTRA#2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9#RAW_FITS#primary",
  "entity_type": "FileObject",
  "schema_version": "1",
  "data_product_id": "2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9",
  "product_type": "SPECTRA",
  "role": "RAW_FITS",
  "bucket": "nova-cat-private-data",
  "key": "raw/spectra/4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1/2c7d1f4d-5b7a-4a4d-9f31-8d3b4fd0d4d9/primary.fits",
  "content_type": "application/fits",
  "byte_length": 184233,
  "etag": "\"9b2cf535f27731c974343645a3985328\"",
  "sha256": "0c3b...a91f",
  "created_by": {
    "workflow": "acquire_and_validate_spectra",
    "job_run_id": "5a4fce02-3b02-4b5c-8d06-541d9f2d4f60"
  },
  "created_at": "2026-02-23T18:10:10Z",
  "updated_at": "2026-02-23T18:10:10Z"
}
```

---

### 6) Reference (per-nova scoped)

#### Key

- `PK = "<nova_id>"`
- `SK = "REF#<reference_id>"`

---

#### Fields

- `schema_version` (internal item evolution)
- `entity_type = "Reference"`
- `reference_id` (UUID)
- `source`
- `source_identifier`
- `published_at` (ISO-8601)
- Small metadata (e.g., `title`, `authors`) — optional

#### Example:
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "REF#7d5e1f5c-2a7c-4e0c-b8b9-3d5a4f4c0b2a",
  "entity_type": "Reference",
  "schema_version": "1",
  "reference_id": "7d5e1f5c-2a7c-4e0c-b8b9-3d5a4f4c0b2a",
  "source": "ADS",
  "source_identifier": "2012ATel.XXXX....1A",
  "title": "Discovery of Nova Sco 2012",
  "published_at": "2012-06-01T00:00:00Z",
  "created_at": "2026-02-23T18:30:00Z",
  "updated_at": "2026-02-23T18:30:00Z"
}
```

---

### 7) NovaReference (link + role)

#### Key

- `PK = "<nova_id>"`
- `SK = "NOVAREF#<reference_id>"`

---

#### Fields

- `schema_version` (internal item evolution)
- `entity_type = "NovaReference"`
- `reference_id`
- `role`
  (`DISCOVERY` | `SPECTRA_SOURCE` | `PHOTOMETRY_SOURCE` | `OTHER`)
- `added_by_workflow`



#### Example:
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "NOVAREF#7d5e1f5c-2a7c-4e0c-b8b9-3d5a4f4c0b2a",
  "entity_type": "NovaReference",
  "schema_version": "1",
  "reference_id": "7d5e1f5c-2a7c-4e0c-b8b9-3d5a4f4c0b2a",
  "role": "DISCOVERY",
  "added_by_workflow": "refresh_references",
  "created_at": "2026-02-23T18:30:05Z",
  "updated_at": "2026-02-23T18:30:05Z"
}
```

---

### 8) JobRun (one per workflow execution)

#### Key

- `PK = "<nova_id>"`
- `SK = "JOBRUN#<workflow_name>#<started_at>#<job_run_id>"`

---

#### Fields

- `schema_version` (internal item evolution)
- `entity_type = "JobRun"`
- `job_run_id` (UUID)
- `workflow_name`
- `execution_arn`
- `status`
  (`RUNNING` | `SUCCEEDED` | `FAILED`)
- `started_at`
- `ended_at`

#### Example:
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "JOBRUN#acquire_and_validate_spectra#2026-02-23T18:10:00Z#5a4fce02-3b02-4b5c-8d06-541d9f2d4f60",
  "entity_type": "JobRun",
  "schema_version": "1",
  "job_run_id": "5a4fce02-3b02-4b5c-8d06-541d9f2d4f60",
  "workflow_name": "acquire_and_validate_spectra",
  "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:AcquireAndValidateSpectra:...",
  "status": "SUCCEEDED",
  "started_at": "2026-02-23T18:10:00Z",
  "ended_at": "2026-02-23T18:12:00Z",
  "created_at": "2026-02-23T18:10:00Z",
  "updated_at": "2026-02-23T18:12:00Z"
}
```

---

### 9) Attempt (one per task invocation, including retries)

#### Key

- `PK = "<nova_id>"`
- `SK = "ATTEMPT#<job_run_id>#<task_name>#<attempt_no>#<timestamp>"`

---

#### Fields

- `schema_version` (internal item evolution)
- `entity_type = "Attempt"`
- `job_run_id`
- `task_name` (Step Functions state name)
- `attempt_no`
- `status` (`STARTED` | `SUCCEEDED` | `FAILED`)
- `error_type`
- `error_message` (short)
- `duration_ms`

#### Example:
```json
{
  "PK": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "ATTEMPT#5a4fce02-3b02-4b5c-8d06-541d9f2d4f60#download_bytes#1#2026-02-23T18:10:10Z",
  "entity_type": "Attempt",
  "schema_version": "1",
  "job_run_id": "5a4fce02-3b02-4b5c-8d06-541d9f2d4f60",
  "task_name": "download_bytes",
  "attempt_no": 1,
  "status": "SUCCEEDED",
  "duration_ms": 8423,
  "created_at": "2026-02-23T18:10:10Z",
  "updated_at": "2026-02-23T18:10:18Z"
}
```

---

## Invariants (minimal, deterministic)

### Product type first ordering

- Photometry table is always addressable at
  `SK = "PRODUCT#PHOTOMETRY_TABLE"`.

- Spectra products sort under
  `PRODUCT#SPECTRA#<provider>#<data_product_id>`.

---

### Eligibility is explicit

- Spectra use `eligibility = ACQUIRE` until validated or quarantined/terminal.
- Photometry table does not use eligibility for acquisition (API-driven ingestion).

---

### Cooldown is enforced per spectra product

- `next_eligible_attempt_at` is authoritative for `SKIPPED_BACKOFF`.
- Provider-level cooldown is deferred post-MVP.

---

### QUARANTINE is human-gated

- No auto-retries from `QUARANTINED`.
- Re-entry requires explicit manual action
  (modeled as `manual_review_status` or a future control plane).

---

### Locator alias mapping is the dedupe authority

- Discovery checks `LocatorAlias` before assigning a new `data_product_id`.
- Locator normalization rules must be deterministic.

---

## Mapping Notes: FITS profiles + IVOA-aligned normalization (minimal persistence)

Persist on spectra products:

- `header_signature_hash`
- `fits_profile_id` (exact chosen profile id/version)
- `profile_selection_inputs` (provider + hints + signature hash)
- `normalization_notes` (small)
- Pointers to normalized artifacts in S3
  (e.g., `derived_s3_prefix`)

This supports reproducibility and debugging without embedding the full canonical model in DynamoDB.
