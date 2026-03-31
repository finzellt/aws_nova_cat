# ADR-020: Photometry and Color Row Storage

Status: Draft
Date: 2026-03-19

> **⚠ Amended by ADR-031** (2026-03-31)
> ADR-031 (Data Layer Readiness for Artifact Generation, Decision 12) notes that
> Decision 7's execution model — a standalone `generate_nova_bundle` Fargate task
> with per-nova idempotency guards — is superseded by DESIGN-003 §4.4 and §10.
> Bundle generation is now step 6 in the unified artifact regeneration Fargate task,
> coordinated via WorkItem/RegenBatchPlan. The DDB-read logic described in Decision 7
> remains valid; the standalone task and `idempotency_guard` scoping do not.
>
> See: `docs/adr/ADR-031-data-layer-readiness-for-artifact-generation.md`

---

## Context

DESIGN-001 (§6, Question 5) explicitly deferred the canonical storage format for
`PhotometryRow` records to this ADR. DESIGN-002 introduced `ColorRow` as a parallel
data type, and §5.6 Question B deferred its canonical storage target to a future ADR.
ADR-021 (Layer 0 spec) established that `ingest_color` is a separate workflow from
`ingest_photometry`, creating a concrete concurrency constraint on the storage design.

This ADR resolves both deferred questions together. The color storage decision cannot be
made independently of the photometry storage decision: both workflows may execute
concurrently against the same nova, and their storage model must handle this cleanly.

---

## Constraints

- **Concurrent workflow execution.** `ingest_photometry` and `ingest_color` are separate
  Step Functions workflows (ADR-021) that may execute concurrently against the same nova.
  The storage model must not require cross-workflow coordination or locking.

- **Idempotency is a first-class requirement.** Multiple ingestion events for the same
  nova may arrive from different source files covering overlapping epochs. The storage
  model must support row-level deduplication — detecting and suppressing duplicate rows
  across successive ingest events without requiring a full-table rebuild.

- **Bundle delivery as the primary data product.** ADR-009 establishes pre-generated
  static artifacts as the primary data delivery mechanism for the website. Per-nova
  downloadable zip bundles are the vehicle for bulk data delivery to researchers.

- **Cost-conscious architecture.** The project operates under minimal funding.

---

## Alternatives Considered

### S3 Parquet (per-nova canonical file, rebuild-in-place)

The initial design in this ADR proposed a single Parquet file per nova per data type,
owned exclusively by one workflow and rebuilt in full on each ingestion event.

**Why rejected:** The rebuild-in-place model solves the concurrent-write race condition
(each workflow owns its own file) but does not solve row-level idempotency. Detecting
whether a given `PhotometryRow` has already been ingested requires either reading the
entire Parquet file and scanning for duplicates on every ingest, or maintaining a
separate row-identity index — which reintroduces DynamoDB anyway. At NovaCat's scale,
the per-file overhead (Lambda rebuild, S3 GET + PUT per modification, two independent
file lifecycles for photometry and color) adds operational complexity without meaningful
cost advantage over DynamoDB. Cost modelling confirmed that DynamoDB is competitive with
S3 Parquet at the row counts expected across the catalog, and cheaper once the Fargate
DDB-read cost for zip generation is compared against the equivalent S3 Parquet read cost
at scale.

### Single combined DynamoDB table (photometry + color rows co-mingled)

A single item type with a `row_type` discriminator field, co-located in the main NovaCat
table under a shared SK prefix.

**Why rejected:** Photometry and color rows have different schemas, different provenance
models, and different ingestion lifecycles. Co-mingling them under a single SK namespace
would require a discriminator in every query, complicate GSI design, and couple two
independently evolving schemas. Separate SK prefixes are the correct model.

---

## Decision 1 — Individual DynamoDB Items Per Row

`PhotometryRow` and `ColorRow` records are stored as individual DynamoDB items in the
existing `NovaCat` table, co-located with all other per-nova entities under the nova's
partition (`PK = "<nova_id>"`).

This is the canonical storage target for photometric and color data. No S3 Parquet file
is maintained as a live queryable store for these data types. S3 is used only for:
- Staged source files (input to ingestion workflows)
- Bundle zip artifacts (output of `generate_nova_bundle`)
- Schema-boundary snapshots (see Decision 5)

### Rationale

Storing rows as individual DDB items gives the ingestion workflows native, atomic
row-level operations:

- **Idempotency** is enforced via conditional `PutItem` — if an item with the same
  `row_id` already exists, the write is a no-op. No full-table scan required.
- **Concurrent safety** is native — DDB's item-level writes are atomic. Two workflows
  writing different rows into the same nova partition cannot corrupt each other.
- **Insertions are true insertions** — adding new rows from a new source file does not
  require reading, rebuilding, or rewriting any existing data.
- **Partial ingestion is recoverable** — if a workflow fails mid-batch, successfully
  written rows are persisted. Retry resumes cleanly; already-written rows are
  idempotent no-ops on re-execution.

---

## Decision 2 — DynamoDB Item Schema for PhotometryRow

### Key

```
PK = "<nova_id>"
SK = "PHOT#<row_id>"
```

`row_id` is a stable UUID derived deterministically from the row's natural identity
(source bibcode + observation epoch + band + telescope). Deterministic derivation is
essential for idempotency: the same logical observation ingested from two different
source files must produce the same `row_id` and therefore collide cleanly on write.

> **Open question (OQ-1):** The exact deterministic derivation function for `row_id`
> — which fields participate, how ties are broken, and how missing fields are handled —
> is a required deliverable of the `ingest_photometry` handler spec. This must be
> resolved before implementation.

### Fields

All fields defined in `photometry_table_model.md` are persisted on the item. Additional
DynamoDB-specific envelope fields:

- `entity_type = "PhotometryRow"`
- `schema_version` — `PhotometryRow` schema version at time of ingestion
- `ingested_at` — ISO 8601 UTC timestamp of this item's creation
- `ingestion_source` — descriptor of the source file (e.g. `upload:aavso_batch_2026_03`)

### Example key

```
PK = "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1"
SK = "PHOT#a3f1c2d4-7e8b-4a9f-b0c1-2d3e4f5a6b7c"
```

---

## Decision 3 — DynamoDB Item Schema for ColorRow

### Key

```
PK = "<nova_id>"
SK = "COLOR#<row_id>"
```

`row_id` derivation follows the same deterministic pattern as `PhotometryRow`, adapted
for the color identity: source bibcode + observation epoch + color string + telescope.

> **Open question (OQ-2):** The exact deterministic derivation function for color
> `row_id` is a required deliverable of the `ingest_color` handler spec.

### Fields

All fields defined in DESIGN-002 §5.3 are persisted on the item, plus the same
DynamoDB-specific envelope fields as `PhotometryRow`:

- `entity_type = "ColorRow"`
- `schema_version` — `ColorRow` schema version at time of ingestion
- `ingested_at`
- `ingestion_source`

---

## Decision 4 — Two DataProduct Envelope Items Per Nova

Two DynamoDB items serve as operational envelopes for the photometry and color row
collections respectively. These items carry ingestion metadata and serve as anchors for
idempotency guards and schema version tracking. They do not store row data.

### PRODUCT#PHOTOMETRY_TABLE

```
PK = "<nova_id>"
SK = "PRODUCT#PHOTOMETRY_TABLE"
```

Fields:
- `entity_type = "DataProduct"`
- `schema_version` (internal item evolution)
- `data_product_id` — stable UUID
- `product_type = "PHOTOMETRY_TABLE"`
- `photometry_schema_version` — current `PhotometryRow` schema version
- `last_ingestion_at`, `last_ingestion_source`, `ingestion_count`
- `last_ingested_file_sha256` — source-level idempotency anchor
- `row_count` — current count of `PHOT#` items in this partition (updated on each
  successful ingest)
- `created_at`, `updated_at`

### PRODUCT#COLOR_TABLE (new)

```
PK = "<nova_id>"
SK = "PRODUCT#COLOR_TABLE"
```

Fields (mirrors PHOTOMETRY_TABLE):
- `entity_type = "DataProduct"`
- `schema_version` (internal item evolution)
- `data_product_id` — stable UUID
- `product_type = "COLOR_TABLE"`
- `color_schema_version` — independent of `photometry_schema_version`
- `last_ingestion_at`, `last_ingestion_source`, `ingestion_count`
- `last_ingested_file_sha256` — source-level idempotency anchor
- `row_count` — current count of `COLOR#` items in this partition
- `created_at`, `updated_at`

Schema versioning is independent: a change to the `PhotometryRow` schema has no effect
on `COLOR_TABLE` items, and vice versa.

---

## Decision 5 — Schema-Boundary Snapshots to S3

When a schema version change occurs (i.e. `photometry_schema_version` or
`color_schema_version` increments), a point-in-time snapshot of the affected rows is
exported to S3 as a Parquet file before migration begins. This snapshot is immutable
and serves as a rollback artifact.

```
derived/photometry/<nova_id>/snapshots/schema=<old_version>/photometry_rows.parquet
derived/photometry/<nova_id>/snapshots/schema=<old_version>/color_rows.parquet
```

Snapshots are schema-boundary artifacts only. They are not created during normal
ingestion and are not queryable by the ingestion workflows.

In MVP, `photometry_schema_version` and `color_schema_version` are fixed. Schema
migration workflows are documented but not implemented.

---

## Decision 6 — initialize_nova Creates Envelope Items Only

When `initialize_nova` creates a new nova, it creates the two DataProduct envelope items
(`PRODUCT#PHOTOMETRY_TABLE` and `PRODUCT#COLOR_TABLE`) with `row_count = 0` and no
ingestion metadata. No S3 files are created at nova initialization time.

This replaces the prior plan to write empty Parquet files during `initialize_nova`.
Downstream consumers check `row_count` on the envelope item to determine whether any
data exists, rather than attempting to read a file that may or may not be present.

---

## Decision 7 — Bundle Generation Reads from DynamoDB

> **⚠ Execution model superseded by DESIGN-003** (2026-03-31)
> The standalone `generate_nova_bundle` Fargate task and its `idempotency_guard`
> scoping described below are superseded by DESIGN-003 §4.4 and §10. Bundle
> generation is now step 6 in the unified artifact regeneration Fargate task's
> per-nova dependency chain, coordinated via WorkItem/RegenBatchPlan rather than
> per-nova idempotency guards. The DDB-read pattern (paginated `Query` against the
> nova partition for `PHOT#` and `COLOR#` items) remains valid and is adopted by the
> bundle generator specified in DESIGN-003 §10.

The `generate_nova_bundle` Fargate task reads `PHOT#` and `COLOR#` items directly from
DynamoDB via paginated `Query` calls against the nova partition, converts them to the
appropriate output format (CSV or FITS per ADR-014), and writes the zip artifact to S3.

~~Bundle regeneration fan-in (both ingest workflows completing near-simultaneously)
is handled via `idempotency_guard`, scoped to `bundle_generation#{nova_id}#{time_bucket}`.
The first invocation acquires the lock; the second exits cleanly.~~

> **Open question (OQ-3):** ~~The bundle structure ADR must specify the output format
> (CSV vs. FITS per ADR-014) and filename convention for photometry and color artifacts
> within the zip.~~ **Resolved by DESIGN-003 §10.** The bundle uses consolidated FITS
> for photometry (§10.6) and original FITS files for spectra (§10.5). Filename
> conventions are specified in §10.3. OQ-3 is closed.

---

## S3 Key Summary

| File | S3 key | Owned by |
|---|---|---|
| Photometry snapshot (schema boundary only) | `derived/photometry/<nova_id>/snapshots/schema=<version>/photometry_rows.parquet` | schema migration workflow |
| Color snapshot (schema boundary only) | `derived/photometry/<nova_id>/snapshots/schema=<version>/color_rows.parquet` | schema migration workflow |
| Bundle photometry artifact | `<release>/nova/<nova_id>/<name>_bundle_<YYYYMMDD>.zip` | artifact regeneration Fargate task (DESIGN-003 §10) |
| Bundle color artifact | (included in bundle photometry FITS table) | artifact regeneration Fargate task (DESIGN-003 §10) |

---

## Required Updates to Existing Artifacts

- **current-architecture.md §3.3** — replace the Photometry Table section to describe
  row-level DDB storage; remove canonical Parquet file references; add `COLOR_TABLE`
  product type; update S3 key listing to remove canonical Parquet keys and add snapshot
  paths only.
- **dynamodb-item-model.md** — add `PHOT#<row_id>` and `COLOR#<row_id>` item shapes;
  add `PRODUCT#COLOR_TABLE` envelope item shape; update `PRODUCT#PHOTOMETRY_TABLE` to
  add `row_count` and remove `s3_key`/`s3_bucket` fields.
- **initialize_nova workflow spec** — replace empty Parquet file creation with
  `PRODUCT#PHOTOMETRY_TABLE` and `PRODUCT#COLOR_TABLE` envelope item creation (both
  with `row_count = 0`).
- **s3-layout.md** — remove canonical photometry/color Parquet keys; add snapshot paths.
- **contracts/models/entities.py** — add `COLOR_TABLE` to `ProductType` enum; add
  `color_table_snapshot` and `photometry_table_snapshot` to `FileRole` enum; remove
  `photometry_table` from `FileRole` (no longer a live S3 object).
- **ingest_photometry workflow spec** — rename `RebuildPhotometryTable` state to
  `PersistPhotometryRows`; update to describe paginated batch conditional `PutItem`;
  update idempotency logic to describe both source-level (file SHA-256) and row-level
  (conditional write) deduplication.
- **photometry_ingestor Lambda** — update memory/timeout profile; Parquet rebuild is
  replaced by paginated DDB batch writes (BatchWriteItem with condition expressions).

---

## Open Questions

| # | Question | Blocking? |
|---|---|---|
| OQ-1 | Deterministic `row_id` derivation function for `PhotometryRow` — participating fields and tie-breaking rules for missing fields | Blocks `ingest_photometry` implementation |
| OQ-2 | Deterministic `row_id` derivation function for `ColorRow` | Blocks `ingest_color` implementation |
| OQ-3 | Bundle output format and filename convention for photometry and color artifacts within the zip | Blocks bundle structure ADR |
| OQ-4 | Does `color_schema_version` follow the same snapshot-on-change policy as `photometry_schema_version`? (Assumed yes.) | Blocks `ingest_color` implementation |
| OQ-5 | GSI design for cross-nova photometry queries (post-MVP). Row-level DDB storage enables this naturally; GSI design is deferred. | Post-MVP only |

---

## Consequences

- Row-level idempotency is native and requires no additional infrastructure. Duplicate
  rows from overlapping source files are suppressed by conditional `PutItem` at write
  time.
- `ingest_photometry` and `ingest_color` are fully decoupled. Neither workflow reads or
  writes data owned by the other.
- Partial ingestion failures are safe and recoverable. Successfully written rows are not
  re-written on retry; the workflow resumes from the failed batch position.
- `initialize_nova` is simplified — no S3 writes are required at nova creation time.
- Bundle generation (Fargate) reads directly from DynamoDB, eliminating the need to
  maintain a separate queryable Parquet file as an intermediate artifact.
- Post-MVP cross-nova queries (e.g. "all V-band observations across the catalog") are
  enabled by a future GSI on band + epoch fields, without any storage migration.
- DynamoDB storage cost scales linearly with row count. At the expected catalog scale
  (~2M photometry rows across all novae), storage cost is under $0.50/month.
