# ADR-021: Layer 0 — Pre-Ingestion Normalization (`prep_photometry_file` workflow)

**Status:** Draft
**Date:** 2026-03-19
**Author:** TF
**Supersedes:** —
**Superseded by:** —
**Amends:** ADR-015 (adds `sidecar_s3_key`, `sidecar_s3_bucket` to `IngestPhotometryEvent`;
revises `IngestPhotometryEvent` to v2.0.0 for post-Layer-0 dispatch)
**Relates to:**
- `DESIGN-001` §5 Layer 0 — authoritative design basis
- `DESIGN-002` §3, §3.4, §5.5 — sidecar contract, inline header extraction, ColorRow
  routing (adopted/amended here)
- `ADR-015` — `IngestPhotometryEvent` contract (amended by §4 and §8.2)
- `ADR-017` — Band Registry (Accepted 2026-03-24; Layer 0 depends on its interface)
- `ADR-018` — Band Disambiguation Algorithm (not yet adopted)
- `ADR-019` — Photometry Table Model Revision (not yet adopted)
- `ADR-020` — Canonical Persistence Format (not yet adopted)
- `ADR-022` — ColorRow Design (not yet adopted)

---

## 1. Context

The adapter pipeline (Layer 4 / `CanonicalCsvAdapter`) assumes tidy, per-nova,
long-format input: one measurement row per band per epoch, with a canonical `band`
column identifying the passband. Real-world photometry files do not arrive this way.
They arrive as wide-format tables with one column per band, as multi-nova survey tables
mixing measurements from dozens of objects, and in compressed archives containing one
or more data files. They arrive with metadata buried in comment-line headers rather
than data columns, or in companion sidecar files rather than the data file itself.

Layer 0 is the normalization stage that bridges this gap. Its sole responsibility is
transformation: it takes a raw, heterogeneous input file and produces a normalized,
per-nova, long-format table and an enriched context object that the adapter pipeline
can consume without knowledge of the original file's shape.

**Key invariant (from DESIGN-001 §5 Layer 0):** Layer 0 is a *transformation* stage,
not a *validation* stage. It does not make quarantine decisions. Normalization failures
are propagated downstream as structured errors; the `ValidatePhotometry` handler (Layer 5)
decides what to quarantine.

### 1.1 Architecture Overview

The full pre-ingestion pipeline consists of two components upstream of `ingest_photometry`:

```
┌─────────────────────────────┐
│  unpack_source              │  Standard SFn workflow
│  Zip detection, format      │  Fire-and-forget fan-out
│  filtering, per-file fan-out│  to prep_photometry_file
└────────────┬────────────────┘
             │  one IngestPhotometryEvent per data file
             ▼
┌─────────────────────────────┐
│  prep_photometry_file       │  Standard SFn workflow (this ADR)
│  Sidecar loading, header    │  Fire-and-forget fan-out
│  extraction, format detect, │  to ingest_photometry /
│  pivot, nova split, routing │  ingest_color
└────────────┬────────────────┘
             │  one event per nova per data type
             ▼
┌──────────────┐  ┌──────────────┐
│ingest_phot.  │  │ingest_color  │
│(Express SFn) │  │(Express SFn) │
└──────────────┘  └──────────────┘
```

`unpack_source` is specified in DESIGN-001 §5 UnpackSource and is not in scope for
this ADR. This ADR specifies `prep_photometry_file` exclusively.

---

## 2. Decision Surface

The open questions this ADR resolves, drawn from DESIGN-001 §6 and DESIGN-002 §7:

| Ref     | Question                                                        | §Decided |
|---------|-----------------------------------------------------------------|----------|
| D1-Q13  | Multi-nova resolution vs. Step Functions Express budget         | §6       |
| D2-Q1   | Sidecar association mechanism (Option A / B / C)                | §4       |
| D2-Q5   | `filename_convention` controlled vocabulary                     | §5.2     |
| D2-Q7   | ColorRow workflow architecture (Option A / B / C)               | §8       |
| D2-Q10  | Inline header keyword vocabulary and synonym registry scope     | §5.1     |

---

## 3. Decisions (Summary)

| # | Decision | Detail |
|---|----------|--------|
| 1 | `prep_photometry_file` is a standalone Standard SFn workflow | §6 |
| 2 | Sidecar association: Option B — explicit `sidecar_s3_key` on event | §4 |
| 3 | Inline header keyword registry extends `synonyms.json` | §5.1 |
| 4 | Wide-format detection: long-first, then wide, single column scan pass | §7.3 |
| 5 | Multi-nova split: per-nova fan-out; DynamoDB batch check + `initialize_nova` polling | §6 |
| 6 | ColorRow workflow: Option B — separate `ingest_color` workflow | §8 |
| 7 | Persistence target: DynamoDB (decided in ADR-020); resolves write concurrency | §9.3 |
| 8 | `prep_photometry_file` Lambda is a `DockerImageFunction` (requires `astropy`) | §9.1 |
| 9 | Diagnostic residuals consolidated into single `HeaderResidual` model | §7.5 |
| 10 | Conflict detection follows trust-but-verify principle | §7.7 |
| 11 | `IngestPhotometryEvent` v2.0.0 and new `IngestColorEvent` | §8.2 |

---

## 4. Sidecar Association Mechanism

**Decision: Option B** — explicit `sidecar_s3_key` field on `IngestPhotometryEvent`.

Option A (filename convention) requires parsing logic at the wrong layer and is fragile.
Option C (hybrid) adds complexity without benefit for the operator-only MVP path. Option
B is explicit, auditable, and trivially extended for the donation API.

### 4.1 Amendment to ADR-015

`IngestPhotometryEvent` gains two new optional fields:

```python
sidecar_s3_key: Optional[str] = None
sidecar_s3_bucket: Optional[str] = None  # defaults to private data bucket if absent
```

`sidecar_s3_key = None` signals sidecar-absent ingestion. The pipeline must function
correctly in the sidecar's absence — it is an optional enrichment mechanism.

### 4.2 Sidecar Schema

The sidecar is a JSON object. Its normative schema is adopted from DESIGN-002 §3.5 with
the amendments below. All fields are optional unless marked **Required**.

> **Schema status:** Field names and types are adopted from DESIGN-002 §3.5. They are
> binding for ADR-021 purposes. Further amendments may be made in ADR-019 (for
> provenance fields) and ADR-022 (for ColorRow-specific fields).

> **Sidecar creation interface:** The design of the operator-facing sidecar creation
> tool (template + validation script for MVP) and the donor-facing form (post-MVP) are
> out of scope for this ADR. See §9.4.

#### Group 1 — Object Context

| Field | Type | Description |
|-------|------|-------------|
| `nova_name` | `string` | Human-readable object name (e.g. `"RS Oph"`). Used as input to nova resolution if `nova_id` absent. |
| `nova_id` | `string` (UUID) | NovaCat internal UUID. If present, bypasses nova resolution. Operator use only; ignored in donor sidecars. |

#### Group 2 — Band and Spectral Context

| Field | Type | Description |
|-------|------|-------------|
| `phot_system` | `string` | Asserted photometric system (canonical `PhotSystem` value). Optical/UV/NIR only; `null` for other regimes. |
| `mag_system` | `string` | Asserted magnitude zero-point system (`"Vega"`, `"AB"`, `"ST"`). Optical/UV/NIR only. |
| `band_assertions` | `object` | Map from file-internal band string to NovaCat canonical band registry ID. Regime-agnostic. |

#### Group 3 — Measurement Context

| Field | Type | Description |
|-------|------|-------------|
| `telescope` | `string` | File-level telescope default. Overridden by row-level column values. |
| `instrument` | `string` | File-level instrument default. |
| `observer` | `string` | Observer name or identifier. |

#### Group 4 — Provenance

| Field | Type | Description |
|-------|------|-------------|
| `bibcode` | `string` | 19-character ADS bibcode of the source publication. |
| `doi` | `string` | DOI of the source publication or dataset. |
| `data_url` | `string` | URL of the upstream data source. |
| `orig_catalog` | `string` | Name of the originating catalog or survey. |
| `orig_table_ref` | `string` | Table identifier within the source publication. |

#### Group 5 — Data Rights

| Field | Type | Description |
|-------|------|-------------|
| `data_rights` | `string` | Licence: `"public"`, `"CC-BY"`, `"CC-BY-SA"`, `"proprietary"`, `"other"`. Defaults to `"public"`. |
| `embargo_end_date` | `string` (ISO 8601) | Present only for `proprietary` data. |

#### Group 6 — Structural Hints

| Field | Type | Description |
|-------|------|-------------|
| `file_format` | `string` | Declared format: `"long"`, `"wide"`, `"color_only"`. Trusted over heuristic. |
| `wide_band_columns` | `array of string` | For wide-format files: column names that represent band measurements. |
| `color_columns` | `array of string` | Column names representing color or flux-ratio measurements. |
| `time_system` | `string` | Time system of epoch column (e.g. `"MJD_UTC"`, `"HJD_TT"`). |
| `column_map` | `object` | Explicit mapping from file column names or 0-based indices to canonical field names. |
| `filename_convention` | `string` | Declares the filename follows a known parseable pattern. See §5.2. |

#### Group 7 — Sidecar Metadata

| Field | Type | Description |
|-------|------|-------------|
| `sidecar_version` | `string` | **Required.** Semver schema version (e.g. `"1.0.0"`). |
| `created_by` | `string` | Free-text identifier of sidecar creator. |
| `notes` | `string` | Free-text notes. Not machine-interpreted; preserved in audit record. |

#### Group 8 — Donor Identity (post-MVP only)

| Field | Type | Description |
|-------|------|-------------|
| `donor_id` | `string` (UUID) | Opaque reference to donor account. Populated by donation API only. |
| `donor_trust_level` | `string` | Trust tier: `"standard"`, `"verified"`, `"trusted"`. Populated by API; ignored in operator sidecars. |

#### Pydantic Model Implementation Notes

The following validation constraints cannot be fully specified until upstream ADRs are
adopted. Implementers should reference the indicated ADR for the binding constraint:

- `phot_system`: validate against the `PhotSystem` enum finalized in ADR-019.
- `mag_system`: validate against the magnitude system vocabulary finalized in ADR-019.
- `time_system`: validate against the `time_orig_sys` allowed-values list finalized in
  ADR-019.
- `column_map`: value vocabulary is the canonical field name set from ADR-019 and
  `synonyms.json` (Epic D). Key format (string column names, string-encoded 0-based
  indices, or both) is an implementation decision for the `prep_photometry_file` handler;
  document in handler spec.
- `bibcode`: validate per the ADS 19-character bibcode format specification.
- `sidecar_version`: the known version set is self-bootstrapping; `"1.0.0"` is the
  initial version at first deployment.

---

## 5. Controlled Vocabularies

### 5.1 Inline Header Keyword Registry

**Decision:** The inline header keyword registry is an extension of `synonyms.json`.
No separate registry file is introduced.

> **Pre-implementation prerequisite:** `synonyms.json` currently maps column headers
> to `PhotometryRow` fields only. It must be extended to cover all ingestion-relevant
> metadata fields (`telescope`, `instrument`, `observer`, `bibcode`, etc.) before the
> inline header extraction can use it as its registry. This is a tracked pre-implementation
> task for Epic D (Adapter Revision).

**Comment prefixes recognised:** `#`, `%`, `/`
Strip rule: `^[#%/]+\s*` — all leading prefix characters are stripped before parsing,
regardless of count. `####` and `///` are handled identically to `#` and `/`.

**Parse tiers (applied in order to each stripped line):**
1. `KEY: value` (case-insensitive, whitespace-tolerant)
2. `KEY = value` (case-insensitive, whitespace-tolerant)
3. File-native delimiter split (e.g. comma for CSV), then attempt tiers 1–2 on each token

Lines that fail all three tiers are not treated as metadata. They are stored as
`HeaderResidual` records with `category="unparseable"` on `IngestionContext` for operator
review and parser improvement. This correctly handles the AAVSO header row collision:
the `#`-prefixed column header row fails all parse tiers and is stored, not silently
dropped.

**In-scope canonical fields for header extraction:**

| Canonical field | Representative synonyms (illustrative; authoritative set in `synonyms.json`) |
|-----------------|------------------------------------------------------------------------------|
| `telescope` | `Telescope`, `Tel`, `Observatory` |
| `instrument` | `Instrument`, `Instr` |
| `observer` | `Observer`, `Obs` |
| `bibcode` | `Bibcode`, `ADS` |
| `doi` | `DOI` |
| `orig_catalog` | `Catalog`, `Survey`, `Source` |
| `phot_system` | `PhotSystem`, `Photometric_System` |
| `mag_system` | `MagSystem`, `ZeroPoint` |
| `object` / `nova_name` | `Object`, `Target`, `Name` |
| `time_system` | `TimeSystem`, `Epoch_System` |

**Explicitly out-of-scope for header extraction:**
- Structural hints (`file_format`, `wide_band_columns`, `column_map`) — sidecar only
- `nova_id` — UUID assertions in comment lines are fragile; sidecar only
- Donor fields (`donor_id`, `donor_trust_level`) — programmatically populated only
- `embargo_end_date`, `data_rights` — too consequential for unvalidated comment lines

**Stop condition:** Delegated to `astropy` format detection *(pending successful
testing to confirm behaviour across all target formats)*. The comment scan applies to
plain-text formats only (CSV, TSV, ECSV, AAVSO text exports). For structured formats
that `astropy` recognises natively (VOTable, FITS, HDF5, ECSV with `astropy` header
block), metadata is extracted from `astropy`'s parsed table metadata instead of the
comment scan.

### 5.2 `filename_convention` Vocabulary

The `filename_convention` sidecar field declares that a file's name follows a known
parseable pattern, allowing Layer 0 to extract additional metadata tokens from the
filename. Absent this declaration, filenames are treated as opaque. Unrecognised values
are logged and ignored without error.

**MVP controlled vocabulary:**

| Value | Source | Extractable fields | Regex |
|-------|--------|--------------------|-------|
| `novacat_v1` | TF operator convention | `nova_name`, `date` | `^(?P<nova_name>.+?)_(?P<month>\d{1,2})_(?P<day>\d{1,2})_(?P<year>\d{2,4})\.(?P<ext>\w+)$` |
| `eso_raw` | ESO VLT raw frames | `instrument`, `exposure_start_time` | `^(?P<olas>[A-Z0-9]{4,8})\.(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})\.fits$` |
| `hlsp` | MAST HLSP standard | `telescope`, `instrument`, `target`, `filter`, `version`, `product_type` | `^hlsp_(?P<proj>[a-z0-9-]{1,20})_(?P<obs>[a-z0-9-]{1,20})_(?P<inst>[a-z0-9-]{1,20})_(?P<target>[a-z0-9.+-]{1,30})_(?P<optelem>[a-z0-9-]{0,20})_(?P<ver>v[0-9]{1,2}(?:\.[0-9]{1,2})?)_(?P<ptype>[a-z0-9-]{1,16})\.(?P<ext>[a-z0-9.]{1,8})$` |
| `sdss_fpc` | SDSS imaging corrected frames | `filter_band`, `run_id`, `camcol`, `field_id` | `^fpC-(?P<run>\d{6})-(?P<band>[ugriz])(?P<camcol>[1-6])-(?P<field>\d{4})\.fit(?:\.gz)?$` |
| `chandra_arc3` | Chandra CXC archive | `instrument`, `content_type`, `processing_level` | `^(?P<instr>[a-z]{3,6})(?P<src>[a-z])(?P<t>\d+)(?P<ver>N\d{3})(?:_(?P<f>[^_]+)_)?(?P<cont>[a-z]+)(?P<level>\d)(?P<sub>[a-z0-9]+)?\.fits$` |

**Behaviour when declared convention does not match filename:** Logged at `DEBUG`;
no `NormalizationError` raised; filename treated as opaque for that ingestion.

**`novacat_v1` format definition:**
`<nova_primary_name>_<MM>_<DD>_<YY>.<ext>` — e.g. `V1324_Sco_3_19_26.csv`.
Month, day, and year tokens are parsed as integers. Two-digit years are interpreted
as 2000+YY. Filenames that do not match this pattern are treated as pattern 2
(opaque) and no metadata is extracted.

---

## 6. Multi-Nova Split Protocol and Step Functions Budget

### 6.1 Workflow Execution Model

**Decision:** `prep_photometry_file` is a **standalone Standard Step Functions
workflow**. It is not embedded inside `ingest_photometry`.

Rationale: the per-nova fan-out model (§6.4) requires Standard workflow semantics —
there is no hard execution time ceiling, and the workflow may spend significant time
polling `initialize_nova` executions for unknown nova names. Express workflow semantics
(5-minute hard ceiling) are inappropriate for this workload.

`ingest_photometry` and `ingest_color` remain Express workflows; they always receive
a pre-resolved, single-nova, normalised input from `prep_photometry_file`.

> **Architectural evolution note:** Lifting nova resolution into an expanded
> `unpack_source` workflow with SFn-native Map state orchestration (`.sync` integration)
> is the preferred long-term direction. This would eliminate Lambda-internal polling,
> enable batch name deduplication across archive contents, and allow
> `prep_photometry_file` to become an Express workflow. This restructuring is deferred
> to a follow-up design session. See §9.4.

### 6.2 Preflight Phase

The preflight phase runs first inside the `prep_photometry_file` Lambda, before any
sidecar fetch, header extraction, or column parsing. It is cheap: no network calls,
no full file parse.

**Preflight operations:**
1. File byte count check against `MAX_FILE_BYTES` guard
2. Row count estimation (line count for plain-text files; row count for structured
   formats via `astropy` metadata)
3. Distinct object name count (requires reading the object name column values only)

**Guards (operator-configurable via Lambda environment variables):**

| Guard | Env var | Proposed value | Semantics |
|-------|---------|----------------|-----------|
| `MAX_FILE_BYTES` | `PREP_MAX_FILE_BYTES` | 50 MB | Consistent with ADR-015 Decision 5 |
| `MAX_ROWS` | `PREP_MAX_ROWS` | 100,000 | Safety valve; reject-not-split |

Either guard exceeded → structured `NormalizationError` returned to SFn;
ingestion halts. The operator must split the file or raise the guard. Files are not
silently split by the pipeline.

`MAX_DISTINCT_OBJECTS` guard is **not applied**. The per-nova fan-out model (§6.4)
makes it unnecessary — `prep_photometry_file` can handle arbitrary numbers of distinct
nova names without a concurrency problem.

### 6.3 Nova Name Resolution Procedure

Resolution applies only when no `nova_id` is pre-supplied via the sidecar (`nova_id`
field, Group 1). When `nova_id` is present in the sidecar, resolution is skipped
entirely for that file.

**Resolution procedure for multi-nova files:**

1. Identify the object name column (see §7.3 detection logic)
2. Extract all distinct name values from that column
3. **Batch DynamoDB check:** query the existing nova table for all distinct names in
   one batch. Names that match existing records → `nova_id` resolved immediately.
4. **Unknown names:** for each name with no DynamoDB match, invoke
   `initialize_nova` (Express SFn) synchronously via `sfn.start_sync_execution()`.
   Each call blocks until the execution completes and returns the result inline
5. **Collect results** from each `start_sync_execution` response.
   Invocations are issued in sequence; each returns synchronously upon completion.
6. **`CREATED_AND_LAUNCHED`** outcome → fetch `nova_id` from DynamoDB; proceed.
7. **`NOT_FOUND`** outcome → rows for that name are quarantined with reason code
   `UNRESOLVABLE_OBJECT_NAME`. Other names' rows are unaffected.
8. **Terminal failure** of an `initialize_nova` execution → treated as `NOT_FOUND`
   for that name; logged with execution ARN for operator review.

**Task token pattern** (DESIGN-002 §3.2 Option callback) is noted as the preferred
upgrade path post-MVP. It is not adopted here because it requires modifying
`initialize_nova`'s terminal handlers, which is out of scope for Epic A-0.

### 6.4 Per-Nova Fan-Out

After resolution and normalisation, `prep_photometry_file` splits the normalised table
into per-nova subsets and writes each subset to S3:

```
uploads/photometry/prepped/<correlation_id>/<nova_id>/<filename>
```

For each per-nova subset, `prep_photometry_file` fires one or more downstream
executions (see §8 for routing):
- `ingest_photometry` execution if photometry rows are present
- `ingest_color` execution if color rows are present

All downstream executions are **fire-and-forget**. `prep_photometry_file` does not
wait for their completion. Each child execution ARN is recorded in the
`prep_photometry_file` JobRun record for traceability.

---

## 7. `prep_photometry_file` Specification

This section is the normative implementation contract.

### 7.1 Input Contract

```python
@dataclass(frozen=True)
class PrepPhotometryFileInput:
    # The raw file, already staged to S3 by unpack_source
    raw_s3_bucket: str
    raw_s3_key: str
    file_sha256: str

    # Optional sidecar (Option B — see §4)
    sidecar_s3_bucket: Optional[str] = None
    sidecar_s3_key: Optional[str] = None

    # Workflow correlation
    correlation_id: str  # generated by unpack_source if absent from original event
    trust_level: Literal["operator", "donor"] = "operator"
```

Note: `nova_id` is no longer optionally pre-supplied at this level. Nova resolution
is always owned by `prep_photometry_file` (§6.3), with the exception that a sidecar
`nova_id` assertion short-circuits resolution for single-nova files.

### 7.2 Output Contract

```python
@dataclass(frozen=True)
class PrepPhotometryFileOutput:
    # One entry per nova found in the file. Length >= 1 on success.
    per_nova_outputs: list[PerNovaOutput]

    # Structured errors propagated to downstream workflows as quarantine
    # candidates. Layer 0 does not quarantine directly.
    normalization_errors: list[NormalizationError]

    # Execution ARNs of all fired downstream executions, for traceability
    child_execution_arns: list[str]

@dataclass(frozen=True)
class PerNovaOutput:
    nova_id: UUID
    # S3 key of the per-nova normalised CSV subset written by prep_photometry_file
    normalised_s3_key: str
    normalised_s3_bucket: str
    context: IngestionContext       # see §7.6
    routing: RowRouting             # see §8.1

@dataclass(frozen=True)
class NormalizationError:
    error_code: NormalizationErrorCode
    detail: str
    affected_rows: Optional[list[int]]  # 0-indexed raw file row numbers; None = file-level

class NormalizationErrorCode(str, Enum):
    UNRESOLVABLE_OBJECT_NAME   = "UNRESOLVABLE_OBJECT_NAME"
    FILE_TOO_LARGE             = "FILE_TOO_LARGE"
    ROW_COUNT_EXCEEDED         = "ROW_COUNT_EXCEEDED"
    SIDECAR_FETCH_FAILED       = "SIDECAR_FETCH_FAILED"
    SIDECAR_SCHEMA_INVALID     = "SIDECAR_SCHEMA_INVALID"
    SIDECAR_CONFLICT           = "SIDECAR_CONFLICT"
    FORMAT_UNDETECTABLE        = "FORMAT_UNDETECTABLE"
    PIVOT_FAILED               = "PIVOT_FAILED"
    HEADER_SCAN_FAILED         = "HEADER_SCAN_FAILED"   # non-fatal; logged only
    OBJECT_COLUMN_NOT_FOUND    = "OBJECT_COLUMN_NOT_FOUND"
```

### 7.3 Wide-Format Detection and Pivot Algorithm

#### Detection — ordered procedure

**Tier 1 — Sidecar override (highest precedence):**
- Sidecar `file_format = "long"` → treat as long; skip heuristic
- Sidecar `file_format = "wide"` → treat as wide; skip heuristic
- Sidecar `file_format = "color_only"` → treat as color-only; skip heuristic
- Sidecar `wide_band_columns` present → wide by declaration; use specified columns as
  band columns; skip heuristic

**Tier 2 — Column header scan (single pass; all classifications run simultaneously):**

Each column header is synonym-resolved against the extended `synonyms.json`. Each
column is classified as one of: epoch/metadata, band measurement, uncertainty, color,
object name, or unrecognised.

**Long-format detection:**
- Synonym-resolved `band` column exists **AND** singular flux/magnitude column exists
  → **long format confirmed**

**Long color-format detection:**
- Synonym-resolved `color` or `index` column exists **AND** singular value column
  exists → **long color format confirmed**

**Wide-format detection (only if long test fails):**
- ≥ 2 columns resolve to band aliases → **wide format confirmed**
- No epoch check is applied at this stage; epoch validation is `ingest_photometry`'s
  responsibility

**Multi-nova detection (runs in same pass regardless of format):**
- A column synonym-resolves to `object_name` / `nova_name` / `target` / `object`
  → multi-nova flag set; this column is used in §6.3 resolution

**Neither long nor wide confirmed:**
- `FORMAT_UNDETECTABLE` `NormalizationError` emitted; raw header line stored as a
  `HeaderResidual`; ingestion halts for this file

**ADR-017 seam:** Band alias lookup in the column header scan uses `synonyms.json`
only until ADR-017 (Band Registry) is adopted. Detection improves automatically once
the registry is available and its interface is exposed to Layer 0.

**Trust-but-verify (§7.7):** Even when sidecar Tier 1 overrides skip the heuristic
for routing purposes, the Tier 2 column header scan always runs in parallel. Any
disagreement between the sidecar declaration and the automated detection is recorded
as a conflict (see §7.7 `FORMAT_OVERRIDE_DISAGREEMENT`).

#### Pivot Algorithm (wide-format files only)

1. **Partition columns** into: epoch/metadata columns, band measurement columns,
   uncertainty columns, color columns, unrecognised columns.

2. **Pair uncertainty columns with band columns.** For each band column `B`, attempt
   to find a paired uncertainty column by testing the following suffix patterns
   (case-insensitive, first match wins):
   `{B}_err`, `{B}err`, `e_{B}`, `e{B}`, `{B}_unc`, `{B}_error`, `σ_{B}`, `err_{B}`.
   Suffix patterns live in `synonyms.json` as a named list; they are not hardcoded
   in Layer 0. No match → uncertainty is `null` for that band; not an error.

3. **Melt.** For each band measurement column, produce one output row per
   (epoch × band), carrying all epoch/metadata column values on every output row.

4. **Drop null-measurement rows.** After the melt, drop any output row whose band
   measurement value is null. This is **row-level**, not epoch-level: a null `i`-band
   value at epoch T does not affect the `g`-band and `r`-band rows at epoch T.

5. **Attach uncertainty.** Attach the paired uncertainty value to each output row
   (`null` if no pairing was found).

6. **Unrecognised columns** pass through unchanged to the output table. Layer 0 does
   not discard columns it cannot classify; that is a Layer 4 / Layer 5 concern.

7. **Mixed files (DESIGN-002 §5.5 Cases 3 and 4):** if both band measurement columns
   and color columns are identified, split the output into two subsets:
   - Photometry subset: pivot band columns as above
   - Color subset: extract color columns; metadata columns duplicated to both subsets
   Case 4 (single band + dependent colors) is treated as Case 3: no algebraic
   reconstruction of missing component magnitudes at ingestion time.

**Long-format files:** No pivot is performed. Column synonym resolution and
uncertainty column identification still run (via `synonyms.json`) to populate the
`IngestionContext` classification map for downstream use.

### 7.4 Sidecar Loading and Validation

1. If `sidecar_s3_key` is `null` → return empty sidecar context; no error.
2. Fetch sidecar JSON from S3. On fetch failure → emit `SIDECAR_FETCH_FAILED`;
   continue with empty sidecar context (non-fatal for ingestion).
3. Parse as JSON. On parse failure → emit `SIDECAR_SCHEMA_INVALID`; continue with
   empty sidecar context.
4. Validate against the sidecar Pydantic model (§4.2). Fields that fail schema
   validation are dropped and recorded as `HeaderResidual` entries with
   `source="sidecar"` and `category="schema_validation_failed"` on `IngestionContext`.
   Schema validation failure does not abort ingestion.
5. Check `sidecar_version`. If version is unrecognised → log `WARNING`; best-effort
   parse of recognised fields only.
6. Return validated sidecar context dict.

**Trust semantics (DESIGN-002 §3.8):**
- `operator` trust level: sidecar assertions are accepted as authoritative.
- `donor` trust level: sidecar assertions are high-quality hints, not overrides.
  Band registry disambiguation runs independently; conflicts are recorded.

### 7.5 Inline Header Extraction

The header scan produces a `header_context: dict[str, str]` (canonical key → raw string
value) from file metadata. Two extraction paths exist, with identical output contracts.

#### Path A — Plain-text formats (CSV, TSV, ECSV without astropy header, AAVSO text)

1. Read the raw file line-by-line.
2. Collect leading lines matching `^[#%/]+`. Stop at the first line that is neither
   empty nor prefix-bearing. Maximum `MAX_HEADER_LINES` lines collected (default 200;
   operator-configurable via `PREP_MAX_HEADER_LINES` Lambda environment variable).
   Guard exceeded → stop collecting, log at `WARNING`, proceed with lines collected.
3. For each collected line:
   a. Strip all leading prefix characters: `^[#%/]+\s*`
   b. Attempt parse tiers in order (§5.1): `KEY: value` → `KEY = value` →
      file-native delimiter split then key-value parse on each token
   c. On successful parse: attempt synonym resolution of KEY against the extended
      `synonyms.json`. On match → add `(canonical_key, raw_string_value)` to header
      context. On no match → emit `HeaderResidual` with `source="inline_header"`,
      `category="unrecognised_key"`, preserving the parsed key and value.
   d. On failed parse (all tiers): emit `HeaderResidual` with
      `source="inline_header"`, `category="unparseable"`, preserving the raw line.
      Do not abort.
4. Return `header_context: dict[str, str]` (canonical key → raw string value).
   Values are raw strings; type coercion is the downstream consumer's responsibility.

#### Path B — Structured formats (VOTable, FITS, HDF5, ECSV with astropy header)

1. Read the table via `astropy.table.Table.read()`.
2. Extract `table.meta` as `dict[str, Any]`.
3. For each key-value pair: synonym-resolve the key against `synonyms.json`. On
   match → add `(canonical_key, str(value))` to `header_context`. On no match →
   emit `HeaderResidual` with `source="astropy_meta"`,
   `category="unrecognised_key"`.
4. For FITS specifically: additionally process `COMMENT` and `HISTORY` list entries
   through the same parse tiers as Path A (these commonly contain embedded
   `KEY = value` pairs). Residuals from this step use `source="fits_comment"`.
5. Return `header_context: dict[str, str]` — identical output shape to Path A.

`HEADER_SCAN_FAILED` is logged at `WARNING` but is non-fatal and does not propagate
as a `NormalizationError` that downstream workflows act on.

#### Design Rationale — Consolidated Diagnostic Residuals

All extraction paths emit residuals into a single `header_residuals: list[HeaderResidual]`
on `IngestionContext`. This consolidation — covering unparseable lines, unrecognised
keys, dropped sidecar fields, and any future residual categories — serves an explicit
iterative-improvement function: unrecognised keys are the primary signal for expanding
`synonyms.json` coverage, and unparseable lines guide parser tier improvements. A single
typed list gives operator review tooling one artifact to inspect per ingestion event,
with `source` and `category` fields enabling targeted filtering. The `HeaderResidual`
model is defined in `entities.py`.

```python
class HeaderResidual(BaseModel, frozen=True):
    source: Literal["inline_header", "astropy_meta", "fits_comment", "sidecar"]
    category: Literal["unparseable", "unrecognised_key", "schema_validation_failed"]
    raw_line: Optional[str] = None        # for unparseable lines
    raw_key: Optional[str] = None         # for unrecognised or failed keys
    raw_value: Optional[str] = None       # for unrecognised or failed values
    detail: Optional[str] = None          # e.g. validation error message
```

### 7.6 Context Object Construction

The `IngestionContext` is assembled from three sources and is immutable after
construction. Precedence rules follow DESIGN-002 §3.7 exactly (reproduced below for
the five resolution chains):

**Band identity resolution precedence:**
1. Sidecar `band_assertions` map
2. File-internal row-level `filter_name` column value → band registry (ADR-018)
3. Inline file header band value → band registry
4. Band registry disambiguation using contextual signals

**Photometric system (`phot_system`) precedence:**
1. File-internal row-level `phot_system` column value
2. Sidecar `phot_system`
3. Inline file header `phot_system` value
4. Band registry inference from resolved band entry

**Magnitude system (`mag_system`) precedence:**
1. File-internal row-level `mag_system` column value
2. Sidecar `mag_system`
3. Inline file header `mag_system` value
4. Band registry inference

**Measurement context (`telescope`, `instrument`, `observer`) precedence:**
1. File-internal row-level column value
2. Sidecar value
3. Inline file header value

**Provenance fields (`bibcode`, `doi`, `orig_catalog`, etc.) precedence:**
1. File-internal column value
2. Sidecar value
3. Inline file header value

**Structural hints (`file_format`, `wide_band_columns`, `column_map`):**
Sidecar only. Inline headers do not carry structural hints.

The context object is immutable after `prep_photometry_file` produces it.
Downstream components read from it but do not modify it.

#### AAVSO Provenance Detection

Layer 0 is responsible for detecting and propagating a definitive AAVSO provenance
signal in `IngestionContext.aavso_provenance`. This signal is consumed by the band
disambiguation algorithm (ADR-018 Decision 5) to apply the AAVSO Generic fallback
exception on `AMBIGUOUS_BAND_UNRESOLVABLE`.

Detection uses the `aavso_signals` block in `detection_registry.json`. Criteria are
evaluated in priority order:

1. A `known_strings` match (`"AAVSO"` or `"aavso"`) found in observer column values
2. Observer column name matches `aavso_signals.column_names` (`Obs`, `Observer`,
   `ObsCode`) AND sampled values match the pattern `^[A-Z]{2,6}$`
3. Observer column name match alone — weak signal; logged at `DEBUG` but does **not**
   set `aavso_provenance = True`

Only criteria 1 and 2 constitute a definitive signal. `aavso_provenance` defaults to
`False` and is set to `True` only when criterion 1 or 2 is satisfied.

#### `IngestionContext` Pydantic Model

All types defined in `entities.py`.

```python
class ContextSource(str, Enum):
    SIDECAR        = "sidecar"
    INLINE_HEADER  = "inline_header"
    FILE_COLUMN    = "file_column"

class ColumnClassification(str, Enum):
    EPOCH_METADATA     = "epoch_metadata"
    BAND_MEASUREMENT   = "band_measurement"
    UNCERTAINTY        = "uncertainty"
    COLOR              = "color"
    OBJECT_NAME        = "object_name"
    UNRECOGNISED       = "unrecognised"

class IngestionContext(BaseModel, frozen=True):
    # --- Group 1: Precedence-resolved metadata fields ---
    # Individual None values are normal; an all-None aggregate state is a
    # Layer 5 quarantine trigger, not a Layer 0 concern.
    phot_system: Optional[str] = None
    mag_system: Optional[str] = None
    telescope: Optional[str] = None
    instrument: Optional[str] = None
    observer: Optional[str] = None
    bibcode: Optional[str] = None
    doi: Optional[str] = None
    data_url: Optional[str] = None
    orig_catalog: Optional[str] = None
    orig_table_ref: Optional[str] = None
    data_rights: Optional[str] = None
    embargo_end_date: Optional[str] = None
    time_system: Optional[str] = None

    # --- Group 2: Band assertions (sidecar Group 2, passed through) ---
    band_assertions: Optional[dict[str, str]] = None

    # --- Group 3: Structural hints (sidecar-only) ---
    file_format: Optional[str] = None
    wide_band_columns: Optional[list[str]] = None
    color_columns: Optional[list[str]] = None
    column_map: Optional[dict[str, str]] = None
    filename_convention: Optional[str] = None

    # --- Group 4: Trust and correlation (from PrepPhotometryFileInput) ---
    trust_level: Literal["operator", "donor"]
    correlation_id: str

    # --- Group 5: Provenance tracking ---
    # Records which source won the precedence race for each field.
    context_sources: dict[str, ContextSource]

    # --- Group 6: Column classification (from §7.3 detection pass) ---
    # Raw column name → classification
    column_classifications: dict[str, ColumnClassification]
    # Band column name → paired uncertainty column name (None if unpaired).
    # All-None pairings (no uncertainty data anywhere) is a Layer 5
    # quarantine signal; Layer 0 records faithfully without judging.
    uncertainty_pairings: dict[str, Optional[str]]

    # --- Group 7: AAVSO provenance signal ---
    # True when a definitive AAVSO provenance signal is detected (criteria 1 or 2
    # below). Consumed by ADR-018 Decision 5 to apply the AAVSO Generic fallback
    # exception on AMBIGUOUS_BAND_UNRESOLVABLE.
    aavso_provenance: bool = False

    # --- Group 8: Header extraction artifacts ---
    # Canonical key → raw string value (from inline header or astropy meta)
    header_context: dict[str, str]
    # Consolidated diagnostic residuals (see §7.5)
    header_residuals: list[HeaderResidual] = []

    # --- Group 9: Conflict records (populated by §7.7) ---
    conflicts: list[ConflictRecord] = []

    # --- Group 10: Sidecar passthrough metadata ---
    sidecar_version: Optional[str] = None
    sidecar_notes: Optional[str] = None
```

### 7.7 Conflict Detection

#### Trust-but-Verify Principle

Sidecar overrides that skip automated detection (§7.3 Tier 1 format overrides,
`band_assertions`, `nova_id` short-circuit) create blind spots where sidecar errors
would go undetected. Layer 0 therefore **always runs automated detection in parallel
with sidecar overrides**. The sidecar value wins per the precedence rules and flows
downstream; any disagreement with the automated result is recorded as a conflict in
`IngestionContext.conflicts` and emitted as a `SIDECAR_CONFLICT` `NormalizationError`.

Layer 0 records conflicts but does not quarantine rows. Quarantine is
`ValidatePhotometry`'s (Layer 5) responsibility.

#### Conflict Classes

```python
class ConflictClass(str, Enum):
    NOVA_NAME_MISMATCH             = "NOVA_NAME_MISMATCH"
    MULTI_OBJECT_IN_SINGLE_NOVA    = "MULTI_OBJECT_IN_SINGLE_NOVA"
    FORMAT_OVERRIDE_DISAGREEMENT   = "FORMAT_OVERRIDE_DISAGREEMENT"
    BAND_ASSERTION_DISAGREEMENT    = "BAND_ASSERTION_DISAGREEMENT"
    PHOT_SYSTEM_MISMATCH           = "PHOT_SYSTEM_MISMATCH"
    MAG_SYSTEM_MISMATCH            = "MAG_SYSTEM_MISMATCH"
    TIME_SYSTEM_MISMATCH           = "TIME_SYSTEM_MISMATCH"
    PROVENANCE_MISMATCH            = "PROVENANCE_MISMATCH"
```

| Class | Trigger | What's compared |
|-------|---------|-----------------|
| `NOVA_NAME_MISMATCH` | Sidecar `nova_name` differs from the single object name value in the file | Sidecar Group 1 vs. file column |
| `MULTI_OBJECT_IN_SINGLE_NOVA` | Sidecar asserts single-nova (via `nova_id` or `nova_name`) but object name column has ≥2 distinct values | Sidecar Group 1 vs. §7.3 multi-nova detection |
| `FORMAT_OVERRIDE_DISAGREEMENT` | Sidecar `file_format` or `wide_band_columns` contradicts what §7.3 heuristic detection would have concluded | Sidecar Group 6 vs. automated detection |
| `BAND_ASSERTION_DISAGREEMENT` | Sidecar `band_assertions` maps a column to a different identity than synonym resolution produces for that column header | Sidecar Group 2 vs. `synonyms.json` resolution |
| `PHOT_SYSTEM_MISMATCH` | Sidecar `phot_system` contradicts file-internal column or inline header `phot_system` value | Sidecar Group 2 vs. file column / inline header |
| `MAG_SYSTEM_MISMATCH` | Sidecar `mag_system` contradicts file-internal column or inline header value | Sidecar Group 2 vs. file column / inline header |
| `TIME_SYSTEM_MISMATCH` | Sidecar `time_system` contradicts file-internal or inline header time system | Sidecar Group 6 vs. file column / inline header |
| `PROVENANCE_MISMATCH` | Sidecar `bibcode`, `doi`, or other provenance field contradicts file-internal or inline header value | Sidecar Group 4 vs. file column / inline header |

All conflict classes emit `SIDECAR_CONFLICT` as a `NormalizationError`. There are no
severity tiers; Layer 5 decides disposition for all classes uniformly.

#### `ConflictRecord` Model

```python
class ConflictRecord(BaseModel, frozen=True):
    conflict_class: ConflictClass
    field: str                             # canonical field name or column name
    sidecar_value: Optional[str] = None
    automated_value: Optional[str] = None  # what the automated path produced
    detail: str                            # human-readable explanation
```

`ConflictClass`, `ConflictRecord` are defined in `entities.py`.

#### Known Blind Spot

Sidecar `nova_id` (UUID) bypasses resolution entirely per §6.3. If the sidecar UUID
is incorrect (points to the wrong nova), Layer 0 cannot detect this without running
resolution on the object name column — which may not even be present in a single-nova
file. This is accepted as an operator-trust tradeoff and documented here for awareness.

#### Forward Reference — Layer 4 Conflicts

Band-registry-level conflict detection (e.g. sidecar `phot_system = "Sloan"` vs.
file-internal band strings that are unambiguously Johnson) requires the band registry,
which is a Layer 4 concern. Layer 4 should apply the same trust-but-verify principle
when it detects such conflicts and emit `ConflictRecord` entries into a downstream
equivalent. This is an ADR-017/ADR-018 design input.

---

## 8. ColorRow Routing

**Decision: Option B** — separate `ingest_color` workflow.

Rationale: Layer 0's per-nova fan-out model already fires independent executions per
nova. Dispatching to `ingest_color` as a peer of `ingest_photometry` — rather than as
a branch inside it — is the natural extension of that model. Both workflows reuse the
same shared Lambda layer (`job_run_manager`, `idempotency_guard`, `quarantine_handler`)
and neither owns nova resolution. The processing logic for `PhotometryRow` and
`ColorRow` is fundamentally different; coupling them in a single workflow would produce
a branching state machine with no shared processing states.

Options A and C are rejected:
- Option A (ColorRow branch inside `ingest_photometry`) couples two distinct data type
  lifecycles and places the routing decision in the wrong layer
- Option C (post-processing sweep) offers no benefit if NovaCat ingests only directly
  reported colors (algebraic reconstruction deferred to ADR-022)

### 8.1 Routing Logic at the Pivot

The `RowRouting` object is produced by `prep_photometry_file` for each `PerNovaOutput`:

```python
@dataclass(frozen=True)
class RowRouting:
    # Indices into the normalised per-nova table
    photometry_row_indices: list[int]   # rows → ingest_photometry
    color_row_indices: list[int]        # rows → ingest_color
    # For Case 3/4 mixed files, both lists are non-empty.
```

**Four routing cases (DESIGN-002 §5.5):**

- **Case 1 — Single-band file:** all measurement columns map to `PhotometryRow` fields.
  `photometry_row_indices` = all rows; `color_row_indices` = empty.

- **Case 2 — Color-only file:** all measurement columns map to `ColorRow` fields.
  `color_row_indices` = all rows; `photometry_row_indices` = empty.

- **Case 3 — Mixed file:** both single-band and color measurement columns present.
  Both index lists are non-empty. Metadata columns are duplicated to both subsets.

- **Case 4 — Single band + dependent colors:** treated as Case 3. No algebraic
  reconstruction of missing component magnitudes. The single-band column dispatches
  to the photometry path; color columns dispatch to the color path.

### 8.2 Downstream Dispatch

`prep_photometry_file` fires downstream executions as follows:

- If `photometry_row_indices` is non-empty → fire `ingest_photometry` with an
  `IngestPhotometryEvent` (v2.0.0)
- If `color_row_indices` is non-empty → fire `ingest_color` with an
  `IngestColorEvent`

Both executions are fire-and-forget. Their ARNs are recorded in the
`prep_photometry_file` JobRun (`child_execution_arns`).

**SFn payload size:** Per-nova normalised subsets are always passed by S3 reference,
never inlined in the SFn event payload. This avoids the 256 KB SFn event size limit.
`IngestionContext` is inlined in the event payload (metadata-only object, typically
a few KB; well within the 256 KB limit).

#### `IngestPhotometryEvent` v2.0.0

This replaces the v1.0.0 schema from ADR-015 when Layer 0 is deployed. The
`candidate_name` resolution path is removed; `nova_id` is always pre-resolved by
`prep_photometry_file`.

```python
class IngestPhotometryEvent(EventBase):
    event_version: Literal["2.0.0"] = "2.0.0"
    job_type: Literal[JobType.ingest_photometry] = JobType.ingest_photometry

    # Always pre-resolved by prep_photometry_file
    nova_id: UUID

    # Per-nova normalised photometry subset
    normalised_s3_key: str
    normalised_s3_bucket: str

    context: IngestionContext
    correlation_id: str
    file_sha256: str               # of the original raw file
    parent_execution_arn: str      # prep_photometry_file ARN

    photometry_schema_version: Optional[str] = None
```

#### `IngestColorEvent`

New event type. Internals of the `ingest_color` workflow are deferred to ADR-022;
this schema is the interface contract only.

```python
class IngestColorEvent(EventBase):
    event_version: Literal["1.0.0"] = "1.0.0"
    job_type: Literal[JobType.ingest_color] = JobType.ingest_color

    nova_id: UUID

    normalised_s3_key: str
    normalised_s3_bucket: str

    context: IngestionContext
    correlation_id: str
    file_sha256: str
    parent_execution_arn: str

    color_schema_version: Optional[str] = None
```

`JobType.ingest_color` is a new enum member in `entities.py`.

---

## 9. Consequences

### 9.1 Immediate

- **ADR-015 amended:** `IngestPhotometryEvent` gains `sidecar_s3_key: Optional[str]`
  and `sidecar_s3_bucket: Optional[str]`. Additionally, `IngestPhotometryEvent` is
  revised to v2.0.0 for post-Layer-0 dispatch (§8.2). The v1.0.0 shape remains
  operative until `prep_photometry_file` is deployed.
- **New Standard SFn workflow:** `prep_photometry_file` is a new CDK stack entry.
- **New Express SFn workflow:** `ingest_color` is a new CDK stack entry (internals
  deferred to ADR-022).
- **New `DockerImageFunction`:** `prep_photometry_file` Lambda requires `astropy` +
  `numpy` and must be containerised, consistent with `spectra_validator` precedent.
- **`synonyms.json` scope extension:** must be extended to cover all ingestion-relevant
  metadata fields before inline header extraction can be implemented. Tracked as a
  pre-implementation task for Epic D (Adapter Revision).
- **`PhotometryQuarantineReasonCode` extended:** `UNRESOLVABLE_OBJECT_NAME`,
  `FORMAT_UNDETECTABLE`, `FILE_TOO_LARGE`, `ROW_COUNT_EXCEEDED` are new entries.
- **`entities.py` additions:** `HeaderResidual`, `IngestionContext`, `ContextSource`,
  `ColumnClassification`, `ConflictRecord`, `ConflictClass` models;
  `JobType.ingest_color` enum member.
- **DESIGN-001 §5 annotated:** Structured diagnostic residuals added as a cross-cutting
  pipeline concern in the §5 preamble, with forward reference to ADR-021 §7.5.

### 9.2 Forward Dependencies

- **ADR-017 (Band Registry)** must define the interface Layer 0 calls for wide-format
  column detection. Until adopted, detection uses `synonyms.json` lookups only.
- **ADR-018 (Disambiguation Algorithm)** is not called by Layer 0. Layer 0 passes band
  strings to the adapter (Layer 4); disambiguation is Layer 4's responsibility.
- **ADR-019 (Photometry Table Model Revision)** governs `PhotometryRow` schema changes.
  In particular, `spectral_coord_value` should be demoted from a required source field
  to a registry-derived field: once the band registry exists, a resolved band entry
  carries its spectral coordinate value. Layer 0's structural validity check must not
  require `spectral_coord_value` from the source file. ADR-019 also pins the
  `phot_system`, `mag_system`, and `time_system` enum vocabularies that the sidecar
  Pydantic model (§4.2) validates against.
- **ADR-022 (ColorRow Design)** governs everything downstream of the `color_row_indices`
  routing output. ADR-021 is intentionally silent on ColorRow persistence and schema.

### 9.3 Constraints on ADR-020

The following constraints on the persistence format decision (ADR-020) are established
by the architecture decided in this ADR:

1. **Write concurrency from fan-out.** The `prep_photometry_file` fan-out model can
   produce concurrent `ingest_photometry` and `ingest_color` executions for the same
   `nova_id` from a single mixed input file. Additionally, multiple files containing
   data for the same nova may be processed concurrently.

2. **Resolution: DynamoDB.** The persistence target for both `PhotometryRow` and
   `ColorRow` records is DynamoDB (decided in the ADR-020 design conversation, recorded
   here as a binding constraint). DynamoDB row-level writes are atomic and independent;
   concurrent executions writing to the same `nova_id` partition write to independent
   items and require no serialisation mechanism at the ingestion layer. This resolves
   all write concurrency concerns introduced by the fan-out model.

3. **PK / SK shape.** The proposed DynamoDB key structure is:
   `PK = <nova_id>#<PHOT|COLOR>`, `SK = #<wavelength_regime>#<band>#<datetime>`.
   The exact SK composition and serialisation is an ADR-020 decision, but must satisfy
   the access patterns in `photometry_table_model.md`.

### 9.4 Open Items Not Resolved in This ADR

| Item | Target |
|------|--------|
| `UnpackSource` workflow specification | DESIGN-001 §5 / separate ADR or Epic |
| `ColorRow` schema, persistence, deduplication key | ADR-022 |
| Band registry interface for column detection | ADR-017 |
| `spectral_coord_value` demotion from required field | ADR-019 / Epic C |
| `synonyms.json` scope extension | Epic D pre-implementation task |
| `filename_convention` parsing rules for conventions beyond `novacat_v1` | Epic A-0 follow-up |
| Sidecar creation interface (operator template + validation script for MVP; donor-facing form for post-MVP) | Epic D pre-implementation task (operator path) / DESIGN-003 (donor path) |
| `IngestPhotometryEvent` v1.0.0 → v2.0.0 transition plan (coexistence vs. flag day) | Epic E implementation concern |
| Lifting nova resolution into expanded `unpack_source` with SFn-native Map state orchestration (eliminates polling, enables batch name dedup, allows `prep_photometry_file` to become Express) | Documentation consistency epic / follow-up design session |
