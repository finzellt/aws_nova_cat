# ADR-015: Photometry Ingestion Mechanism and Column Mapping Strategy

Status: Proposed
Date: 2026-03-17

---

## Context

The photometry ingestion pipeline (`ingest_photometry` workflow) is specified at the
workflow level — state machine, retry policy, quarantine handling, and DynamoDB access
patterns are all documented. However, several foundational architectural questions were
left open that block implementation:

1. **How does a photometry source file physically reach the workflow?**
   The workflow spec describes an "API upload of a photometry file" but does not define
   the staging mechanism, the upload surface, or what the `IngestPhotometryEvent` carries
   to identify the file.

2. **How does the system map columns from heterogeneous source files to the canonical
   `PhotometryRow` schema?**
   The canonical photometry table schema (`photometry_table_model.md`) defines ~30 columns
   spanning optical through X-ray regimes. Real-world source files — literature tables,
   VizieR exports, AAVSO downloads — use inconsistent column names, units, and formats.
   No mapping strategy has been specified.

3. **What is the correct idempotency key for photometry ingestion?**
   The workflow spec defines the key as `IngestPhotometry:{nova_id}:{photometry_schema_version}`.
   Because the schema version is fixed in MVP, this key never varies per nova — a second
   upload of genuinely new data for the same nova would be incorrectly classified as a
   duplicate and skipped.

4. **How does the system prevent row-level duplicates when accumulating photometry from
   multiple source files over time?**
   File-level idempotency (Decision 3) handles exact re-uploads correctly. It does not
   handle the case where two distinct source files both contain measurements for the same
   nova at the same epoch in the same band from the same paper. No row-level deduplication
   strategy has been specified.

5. **What file size constraints apply given the Express workflow execution limit?**
   The `ingest_photometry` workflow runs on Step Functions Express, which imposes a 5-minute
   maximum execution lifetime. Large source files (e.g., AAVSO archival tables spanning
   decades of observations) could plausibly exhaust this budget. No size guard has been
   defined.

This ADR resolves all five questions and establishes the governing principles for
photometry ingestion through MVP and into the post-MVP donation pathway.

---

## Scope

This ADR covers:

- MVP ingestion trigger mechanism and file staging model
- Post-MVP ingestion pathway (VizieR fetching, donation API) at a planning level
- Column mapping strategy and its three-tier architecture
- Data type enforcement and coercion behavior
- AI-assisted adapter registration: scope, workflow, and explicit runtime exclusion
- Row-level deduplication strategy
- File size constraints and Lambda budget protection
- Idempotency key design
- Required additions to `IngestPhotometryEvent`

This ADR does **not** cover:

- The internal schema of `PhotometryRow` (implementation detail of the contracts layer)
- The canonical storage file format (Parquet vs. alternatives); see Open Questions
- Lambda handler implementation or table build logic
- Step Functions ASL changes
- CDK infrastructure changes

---

## Decision 1 — MVP Ingestion Mechanism: Operator-Prepared S3 Staging

### Decision

In MVP, photometry ingestion is triggered by the operator (TF) placing a prepared source
file in a designated S3 staging prefix and manually publishing an `IngestPhotometryEvent`
pointing to that key. There is no upload API surface in MVP.

### Staging convention

Photometry source files are staged at:

```
uploads/photometry/<nova_id>/<filename>
```

within the existing private data bucket, which is already designated as the canonical
home for raw staged data in the NovaCat architecture. The `IngestPhotometryEvent` carries
the S3 key (and optionally the bucket) identifying the staged file. The workflow reads
from S3; file content is never inlined in the event payload.

### Rationale

- Lambda execution context imposes practical limits (~6 MB synchronous payload, ~256 KB
  Step Functions event) that make inline file delivery unworkable for real photometry
  tables.
- S3-staging is durable, auditable, and consistent with how raw spectra bytes are handled
  elsewhere in the system.
- In MVP, the operator fully controls the input files. A dedicated upload API surface adds
  infrastructure complexity that yields no benefit until the donation pathway is active.
- Deferring the API surface keeps the ingestion contract simple and avoids prematurely
  encoding assumptions about the donation UX into the backend.
- The private data bucket already exists for this purpose. No new bucket is required.

### Required contract change

`IngestPhotometryEvent` must be amended to add:

- `raw_s3_key: str` — required; the staged upload location
- `raw_s3_bucket: str | None` — optional; defaults to the private data bucket

### Post-MVP pathway

The post-MVP donation feature will introduce an API upload surface. At that point:

- A thin API handler uploads the file to the same `uploads/photometry/<nova_id>/` prefix
  and publishes an `IngestPhotometryEvent` with the resulting key.
- The ingestion workflow is unchanged; only the event publisher changes.
- The `IngestPhotometryEvent` contract is already forward-compatible with this model.

---

## Decision 2 — Column Mapping: Three-Tier Architecture

### Decision

Column mapping from heterogeneous source files to the canonical `PhotometryRow` schema is
handled by a three-tier strategy. The tiers are complementary, not competing — each tier
handles a different class of source file, and multiple tiers apply simultaneously.

### Tier 1 — Canonical CSV (MVP)

For MVP, the operator prepares input files whose column headers directly match the
canonical field names defined in `photometry_table_model.md` (e.g., `magnitude`,
`time_mjd`, `filter_name`). The CSV parser performs no column name translation; it reads
headers directly and validates each row as a `PhotometryRow`.

This is the correct approach for operator-controlled ingestion. It imposes zero mapping
complexity while establishing the canonical schema as the authoritative reference.

### Tier 2 — Synonym Normalization with Type Enforcement (MVP; applies to all tiers)

A curated synonym registry (`adapters/photometry/synonyms.json`) maps common
non-canonical column name variants to canonical field names. Examples:

```json
{
  "MAG":        "magnitude",
  "MAGNITUDE":  "magnitude",
  "ERR":        "mag_err",
  "MAGERR":     "mag_err",
  "MJD":        "time_mjd",
  "FILTER":     "filter_name",
  "TELE":       "telescope",
  "TELESCOPE":  "telescope",
  "INST":       "instrument",
  "INS":        "instrument",
  "BIBCODE":    "bibcode"
}
```

Synonym matching is case-insensitive and applied before Pydantic validation. The registry
is a versioned file in the codebase and expands over time as new source formats are
encountered. Additions are non-breaking; removals or renames are breaking changes
requiring a registry version increment.

**Type enforcement and coercion** are applied immediately after column name resolution.
Once a source column is mapped to a canonical field, the adapter attempts to coerce its
value to the expected type (e.g., `"12.43"` -> `float`, `"FALSE"` -> `bool`,
`"51234.5"` -> `float`). Coercion failures are collected across all rows before any
quarantine decision is made — the adapter does not fail fast on the first bad row.
Whether to quarantine the file is then a threshold decision: if the proportion of rows
with coercion or validation failures exceeds a configurable threshold, the file is
quarantined with reason code `invalid_columns`. If below the threshold, failing rows are
dropped and logged, and the file proceeds with the clean subset.

### Tier 3 — UCD-Based Mapping (Post-MVP; VizieR and VO-compliant sources)

This tier applies exclusively at *ingestion time* and is concerned entirely with
*translating columns from a VizieR VOTable onto fields in our `PhotometryRow` schema*.
It has no bearing on how data is presented to the frontend or in published artifacts;
downstream presentation is governed by ADR-013 and ADR-014.

VizieR and other VO-compliant archives annotate table columns with IVOA UCD1+ controlled
vocabulary entries (e.g., `phot.mag`, `time.epoch`, `instr.filter.id`). The canonical
`photometry_table_model.md` already defines the canonical UCD for every `PhotometryRow`
field. A VizieR adapter can therefore perform fully deterministic, standard-based column
mapping by reading UCDs from the VOTable metadata and looking them up in a canonical
UCD->field map — with no reliance on column names at all.

This tier is the intended strategy for VizieR-sourced photometry post-MVP. It is robust,
standards-compliant, and requires no synonym curation for well-annotated tables.

### Tier 4 — AI-Assisted Adapter Registration (Post-MVP; novel donated formats)

For novel source formats that are neither canonical CSV nor VO-compliant (e.g., data
donated by researchers in bespoke formats), Claude is used to generate a proposed column
mapping.

**Critical constraint: AI-assisted mapping is a development-time tool, not a runtime
component.** It operates at adapter *registration* time, not at ingestion time.

The workflow is:

1. A new source format arrives (donation, novel VizieR table structure, etc.).
2. The operator runs an offline tool, feeding the table header, first few rows, and any
   available README or documentation to Claude.
3. Claude produces a proposed `mapping.json` for that source format.
4. The operator reviews the mapping. This is a required human gate — a quality control
   step, not an optional check.
5. The reviewed `mapping.json` is committed to the adapter registry in the codebase.
6. At ingestion time, the adapter looks up the stored mapping and applies it
   deterministically. No AI call occurs at runtime.

**Rationale for the development-time constraint:**

- **Correctness:** Non-determinism at a data boundary is a catalog integrity risk. The
  same file ingested twice could produce different column assignments if model output
  varies. Scientific data requires deterministic, auditable provenance.
- **Trust:** A professional research catalog cannot have misidentified columns entering
  the canonical table without a human review step. Tier 4 is a development accelerator,
  not a bypass of human judgment.
- **Cost and reliability:** An API call to an external model on every novel ingestion is
  an operational cost and a failure mode in the hot path. The ingestion workflow has no
  external AI dependency at runtime.

### Tier applicability summary

| Source type | Tiers applied |
|---|---|
| Operator-prepared canonical CSV (MVP) | 1, 2 |
| VizieR / VO-compliant VOTable (post-MVP) | 2, 3 |
| Donated bespoke format (post-MVP) | 2, 4 |

---

## Decision 3 — Idempotency Key: File SHA-256

### Decision

The idempotency key for `ingest_photometry` is:

```
IngestPhotometry:{nova_id}:{file_sha256}
```

where `file_sha256` is the SHA-256 digest of the raw source file bytes.

### Rationale

The previously specified key `IngestPhotometry:{nova_id}:{photometry_schema_version}`
does not work correctly: because `photometry_schema_version` is fixed in MVP, the key
reduces to `IngestPhotometry:{nova_id}:1.0` for every ingestion. A second upload of
genuinely new photometry data for the same nova would be classified as a duplicate and
skipped without processing.

Keying on file content solves this:

- Exact re-upload of an already-ingested file -> correctly classified as duplicate, skipped
- New photometry file for an existing nova -> different SHA-256, correctly processed
- The idempotency guarantee (exactly-once processing per unique file) is preserved

`file_sha256` may be supplied by the caller in `IngestPhotometryEvent` if the caller has
already computed it (e.g., as part of a staging upload script). If absent, the workflow
computes it from the staged S3 object before the idempotency check.

### Required contract change

`IngestPhotometryEvent` must be amended to add:

- `file_sha256: str | None` — optional; if provided, used directly; if absent, computed
  by the workflow from `raw_s3_key`

---

## Decision 4 — Row-Level Deduplication

### Decision

File-level idempotency (Decision 3) handles exact re-uploads but does not protect against
row-level duplicates arising from overlapping source files. The canonical deduplication
key for a photometry row is:

```
(nova_id, time_mjd, filter_name, bibcode)
```

This key identifies a unique measurement: same object, same epoch, same band, same
literature source. Before the canonical table is written, any rows in the incoming
validated set that share a deduplication key with a row already present in the canonical
table are dropped. Dropped rows are counted and logged; a high drop rate is surfaced as a
structured log warning to alert the operator to likely source file overlap.

### Rationale

- `nova_id` and `time_mjd` alone are insufficient: the same nova can have multiple
  measurements at the same epoch in different bands.
- Adding `filter_name` covers the multi-band case.
- Adding `bibcode` prevents cross-paper measurements at the same epoch and band from
  being incorrectly treated as duplicates — two independent observers reporting the same
  measurement is scientifically meaningful, not a data error.
- When `bibcode` is NULL (permitted by the schema for some source types), deduplication
  falls back to `(nova_id, time_mjd, filter_name, telescope, instrument)` as the best
  available proxy.

### Open question

The behavior when `bibcode` is NULL and the fallback key is also ambiguous (e.g., same
telescope and instrument but genuinely distinct observations) is not fully specified here.
This edge case should be addressed during handler implementation and captured in a note
on the `ValidatePhotometry` handler spec.

---

## Decision 5 — File Size Constraint

### Decision

The `ValidateInput` stage of the `ingest_photometry` workflow enforces a maximum file
size limit before any parsing or processing occurs. Files exceeding the limit are
quarantined immediately with reason code `file_too_large`. The initial limit is **50 MB**.

This value is a conservative starting point and should be tuned empirically as real
source files are ingested. The limit is configurable via Lambda environment variable
(`PHOTOMETRY_MAX_FILE_BYTES`) to allow adjustment without a code deployment.

### Rationale

The `ingest_photometry` workflow runs on Step Functions Express, which imposes a hard
5-minute execution lifetime. While typical literature photometry tables (hundreds to low
thousands of rows) process well within this budget, archival datasets can be substantially
larger — AAVSO visual estimate archives for well-observed novae like GK Per span tens of
thousands of rows accumulated over decades. Without a size guard, an oversized file will
cause a workflow timeout, consuming the full 5-minute budget and producing no useful
output or diagnostic information.

Enforcing the limit at `ValidateInput`, before S3 read of the full object body, requires
only a `HeadObject` call to retrieve `Content-Length`. This is cheap and fails fast.

### Consequence for large datasets

Files exceeding the limit are not silently dropped — they are quarantined with full
diagnostic metadata and an SNS notification per the standard quarantine handling policy.
The operator is therefore alerted and can decide whether to split the file or raise the
limit.

---

## Summary of Contract Changes

`IngestPhotometryEvent` requires the following additions:

| Field | Type | Required | Notes |
|---|---|---|---|
| `raw_s3_key` | `str` | Yes | Staged S3 object key for the source file |
| `raw_s3_bucket` | `str / None` | No | Defaults to private data bucket if absent |
| `file_sha256` | `str / None` | No | SHA-256 of source file bytes; computed by workflow if absent |

The existing `photometry_schema_version` field is retained as a forward-compatible hook
for future schema migration support, consistent with its current role. It is not used in
the idempotency key.

---

## Consequences

- The `ingest_photometry` workflow now has a well-defined, implementable contract for how
  source files are delivered, identified, and protected against both file-level and
  row-level duplication.
- The idempotency key correctly distinguishes between a duplicate upload and a genuine
  new data upload for an existing nova.
- The column mapping strategy provides a clear, implementable path from MVP through the
  full donation pathway without any architectural rework between tiers.
- AI assistance in the mapping pipeline is explicitly scoped to a development-time,
  human-reviewed registration workflow. The catalog's scientific integrity does not depend
  on runtime model correctness.
- The `synonyms.json` registry is a new versioned artifact in the codebase. Additions are
  non-breaking; removals or renames require a registry version increment.
- Type coercion failures are collected across all rows and resolved via a threshold
  policy, consistent with the row-level validation pattern used elsewhere in the pipeline.
- The 50 MB file size limit is a starting constraint, not a permanent ceiling. It should
  be revisited once real ingestion workloads are observed.
- VizieR fetching (Tier 3) is explicitly planned but not scoped to MVP. It should be
  addressed in a dedicated ADR when the VizieR adapter is implemented.
- The donation API surface (Tier 4 trigger) is explicitly planned but not scoped to MVP.
  Its introduction will require a new ADR covering the upload API design, authentication,
  and abuse prevention.

---

## Open Questions

1. **Canonical storage format (Parquet vs. alternatives):** There is an unresolved design
   question about whether Parquet is the right storage format for the canonical photometry
   table given NovaCat's specific access patterns, data volumes, and serverless
   constraints. This question is explicitly deferred and should be resolved in a dedicated
   ADR before the `RebuildPhotometryTable` handler is implemented.

2. **Synonym registry versioning:** Should `synonyms.json` carry an explicit schema
   version field, or is git history sufficient for auditability? To be resolved during
   contracts implementation.

3. **VizieR fetch trigger:** When the VizieR adapter is introduced post-MVP, what triggers
   it — a new workflow, an extension of `ingest_photometry`, or a new event type? To be
   resolved in a dedicated ADR.

4. **Row deduplication with NULL bibcode:** The fallback deduplication key when `bibcode`
   is NULL is partially specified. The edge case of ambiguous fallback keys should be
   fully specified during `ValidatePhotometry` handler implementation.
