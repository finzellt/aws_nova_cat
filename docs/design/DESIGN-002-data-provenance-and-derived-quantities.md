# DESIGN-002: Data Provenance and Derived Quantities

**Status:** Draft
**Date:** 2026-03-18
**Author:** TF
**Document class:** Design / Scoping (feeds future ADRs; does not itself constitute decisions)

**Relates to:**
- `DESIGN-001` — Photometry Ingestion System: Full Redesign *(direct predecessor; this document completes the open contracts left by DESIGN-001)*
- `ADR-019` — Photometry Table Model Revision *(resolution provenance field set defined here feeds ADR-019)*
- `ADR-020` — Canonical Persistence Format *(ingestion provenance storage deferred to ADR-020)*
- `ADR-021` — Layer 0 Pre-Ingestion Normalization *(sidecar contract and inline header extraction defined here feed ADR-021)*

---

## 1. Executive Summary

DESIGN-001 laid out the full seven-layer photometry ingestion architecture and identified
four areas requiring a dedicated design document before implementation could proceed:
the sidecar metadata contract (left as a stub in Layer 0), the provenance framework
(flagged as an unresolved gap in Layer 3 and the `FinalizeIngestion` handler), the
`ColorRow` data type (deferred from §4.2), and the donation workflow context (deferred
to post-MVP). This document addresses all four. It is the direct successor to DESIGN-001
and must be read in conjunction with it.

The four topics belong in a single document because they are not independent. The sidecar
is a provenance *input mechanism*: it supplies measurement provenance fields that the
framework (§4) defines. `ColorRow` is a derived quantity whose stored records carry the
same provenance tiers as `PhotometryRow`, and whose band components are resolved through
the same registry that DESIGN-001 specifies. The donation workflow is, at its core, a
trust-and-provenance problem: the key question it raises is how to handle provenance
assertions from an external source whose reliability cannot be assumed.

Like DESIGN-001, this document is exploratory. It maps the design space, proposes
structures, and identifies the questions that must be resolved in dedicated ADRs. It does
not make binding architectural decisions.

---

## 2. Scope and Relationship to DESIGN-001

### 2.1 Forward References This Document Satisfies

| DESIGN-001 reference | Location | This document's response |
|---|---|---|
| Sidecar contract schema deferred to DESIGN-002 | §5 Layer 0, Epic A-0 | §3 (full sidecar contract) |
| Open Question 11: mixed `PhotometryRow`/`ColorRow` files | §6 | §5.5 |
| Open Question 12: sidecar contract schema and association mechanism | §6 | §3.2, §3.5 |
| `ColorRow` design deferred to DESIGN-002 | §4.2 | §5 |
| Band-resolution provenance fields gap (Layer 3 / ADR-019) | §5 Layer 3 | §4.3 |
| `FinalizeIngestion` audit record design not specified | §5 Layer 5 | §4.4 |
| Donation workflow context deferred to DESIGN-002 | §4.2 | §6 |

### 2.2 What This Document Deliberately Defers

This document defines *what* the sidecar contains, *what* the provenance model captures,
and *what* `ColorRow` must represent. It does not decide:

- The physical storage format or DynamoDB schema for ingestion audit records (ADR-020)
- The Layer 0 component implementation and Step Functions integration (ADR-021)
- The complete `PhotometryRow` schema revision, including the exact resolution provenance
  field set (ADR-019)
- The `ColorRow` storage target and persistence format (a future ADR, TBD as ADR-022)
- The band registry schema (ADR-017) and disambiguation algorithm (ADR-018), which
  `ColorRow` band resolution depends on
- The full donation workflow design (proposed DESIGN-003; see §6)

### 2.3 Document Class and Schema Disclaimer

This is a scoping document, not an ADR. The structures proposed here — sidecar field
schema, provenance tier model, `ColorRow` field set — are the recommended designs, not
adopted decisions. They will be adopted, amended, or rejected as the relevant ADRs are
written. **The field sets defined in §3.5 and §5.3 are proposals, not final schemas.**
In particular, field names, types, and the division of concerns between fields are all
expected to evolve as ADR-019, ADR-021, and ADR-022 are authored. Readers should treat
those tables as a starting point for ADR discussions, not as implementation contracts.

---

## 3. Sidecar Contract

### 3.1 Purpose and Use Cases

A sidecar is a structured metadata file associated with a primary photometry data file.
Its purpose is to supply context that enriches the ingestion pipeline when that context
is absent from or ambiguous within the primary file. The sidecar is not a required
companion — it is an optional enrichment mechanism. The ingestion pipeline must function
correctly in its absence.

Two distinct use cases drive the sidecar design, and they have different trust
characteristics:

**Use case A — Operator-prepared sidecar (MVP).** The operator (TF) controls both the
primary file and any sidecar. The sidecar is a deliberate, authoritative annotation: the
operator knows the photometric system, has verified the bibcode, and is asserting context
that the pipeline should accept without further challenge. Trust is unconditional within
the constraints of schema validation.

**Use case B — Donor-supplied sidecar (post-MVP).** A data donor supplies both a
photometry file and a sidecar populated via a future web form. The donor may have
accurate knowledge of their own data (telescope, instrument, data rights), but their
assertions about external context (bibcode, photometric system) cannot be assumed
correct. Trust is conditional: donor sidecar fields are treated as hints subject to
downstream validation rather than as authoritative assertions, except for fields where
the donor is the uniquely authoritative source (data rights, embargo status, observer
identity).

The sidecar schema must accommodate both use cases. The trust level is a property of the
ingestion context, not of the sidecar schema itself — the same field set is used in both
cases, but its interpretation and downstream treatment differ.

### 3.2 Association Mechanism

A sidecar must be unambiguously associated with its primary file at the point when Layer
0 processes that file. Three association mechanisms are worth evaluating:

**Option A — Filename convention.** The sidecar is expected at the same S3 prefix as the
primary file, with the same base name and a `.sidecar.json` suffix (e.g.
`uploads/photometry/<nova_id>/rs_oph_aavso.csv` → `rs_oph_aavso.sidecar.json`). Layer 0
performs a conditional S3 `GetObject` on the derived sidecar key; if no object exists at
that key, no sidecar is present.

*Assessment:* Simple for the operator path. Problematic for the donation path, where the
donor uploads files through an API that assigns S3 keys programmatically — the donor
cannot control the key name and therefore cannot satisfy a filename convention without
explicit coordination with the API.

**Option B — Explicit sidecar key in `IngestPhotometryEvent`.** An optional
`sidecar_s3_key` field is added to `IngestPhotometryEvent`. If present, Layer 0 fetches
the sidecar from that key. If absent, no sidecar is used.

*Assessment:* Maximally explicit and unambiguous. Works identically on the operator path
(operator sets both keys manually) and the donation path (the upload API populates both
keys). The operator is never required to name files in a particular way. The slight cost
is that the operator must explicitly set `sidecar_s3_key` when staging a sidecar,
rather than relying on a naming convention that works automatically.

**Option C — Hybrid.** `sidecar_s3_key` in the event if present; fallback to filename
convention lookup if absent.

*Assessment:* Preserves ergonomic convenience for the operator (who can rely on naming
convention without modifying the event) while remaining forward-compatible with the
donation path (which will always use the explicit key). The cost is a conditional S3
lookup on every ingestion even when no sidecar is expected, and two code paths for
association resolution.

The recommended approach is **Option B** for its unambiguous, single-code-path design.
The operator ergonomics cost is marginal: staging a sidecar is already a deliberate act,
and populating one field in the event is a trivial additional step. The explicit key also
surfaces the sidecar association in the Step Functions execution history, which aids
debugging. This is an open question deferred to ADR-021; see §7, Question 1.

### 3.3 Filename as a Metadata Source

Filenames are in principle a metadata-bearing surface: a file named
`rs_oph_BVRI_LCO_2006.csv` contains object identity, band set, telescope, and year.
In practice, extracting this metadata reliably without contextual clues is infeasible.
Filename conventions in the wild are unstructured, inconsistent, and frequently
ambiguous — the same tokens can appear in different positions across different sources.
Ad-hoc filename parsing in the absence of a declared convention is therefore explicitly
out of scope for Layer 0.

Filename metadata is actionable only when a naming convention is *declared*. The sidecar
is the natural vehicle for this declaration: a `filename_convention` field (see §3.5)
allows an operator or donor to assert that the filename follows a specific pattern and
to specify the parsing rules. Absent that declaration, the filename is treated as
opaque. This boundary is intentional: it keeps Layer 0 deterministic and prevents a
class of silent misidentification errors that ad-hoc filename parsing would introduce.

### 3.4 Inline File Header Extraction

Many real-world photometry files embed structured key-value metadata in comment lines
above the data. Examples from common sources:

```
# Telescope: Las Cumbres Observatory 1m
# Filter: V
# Object: RS Oph
# Bibcode: 2023ApJ...945..100S
```

AAVSO export files, observatory pipeline outputs, and some ATel machine-readable tables
follow this pattern. The information these headers carry — particularly telescope,
instrument, and filter — is exactly the information most likely to be absent from the
data columns themselves.

Layer 0 should perform a lightweight header scan before invoking the `CanonicalCsvAdapter`
(the Layer 4 column-mapping and band-resolution component described in DESIGN-001 §5
Layer 4; not to be confused with the `PhotometryAdapter` Protocol or other adapter
objects in the codebase). This scan reads the file's leading comment lines (lines
beginning with `#` or `%`, up to the first non-comment line), attempts to parse them as
`KEY: value` or `KEY = value` pairs, and extracts any recognized keys into the ingestion
context. Recognized keys are a controlled vocabulary (see §3.5 for the full field set;
the header scan uses the same canonical field names and their synonyms from the synonym
registry). The existing `synonyms.json` is scoped to `PhotometryRow` field names; the
inline header scan requires coverage of sidecar/context fields (e.g. `nova_name`,
`file_format`) that fall outside that scope. The resolution adopted in ADR-021 is to
expand `synonyms.json` into a registry for all ingestion metadata, not just
`PhotometryRow` fields.

This is not a sidecar — it is a second, distinct source of file-internal context that
Layer 0 harvests before passing the file to the adapter. The extracted key-value pairs
feed the same context object as the sidecar, and the same precedence rules apply
(§3.7). The header scan is best-effort: unrecognized keys are ignored without error;
malformed values are logged but do not abort ingestion.

**Key constraint:** The header scan must not attempt to parse the *data* section of the
file. It operates only on leading comment lines and stops at the first non-comment,
non-empty line. This constraint keeps the scan cheap and prevents it from accidentally
consuming data rows that begin with `#` (a legitimate occurrence in some formats).

### 3.5 Field Schema

The sidecar is a JSON object. All fields are optional unless otherwise noted. Fields are
organized by the problem they solve.

> **Schema status:** The field set below is a proposed starting point, not a final
> schema. Field names, types, and groupings are expected to be refined during the
> authoring of ADR-021. Implementers must use the ADR-021 schema as the authoritative
> reference.

#### Group 1 — Object Context

These fields assert or clarify the identity of the nova to which the primary file's
measurements belong. Useful when the primary file lacks an object name column or when
the name column contains a non-canonical alias.

Note that a single source file may contain measurements for multiple novae. The
multi-nova split in Layer 0 (DESIGN-001 §5 Layer 0) handles this case: the file is
split by object identity before any per-nova processing occurs. The `nova_name` and
`nova_id` fields in the sidecar apply at the file level and are most useful for
single-nova files where the object identity is absent from the file itself.

| Field | Type | Description |
|---|---|---|
| `nova_name` | `string` | Human-readable object name (e.g. `"RS Oph"`, `"Nova Sco 2023"`). Used as input to the nova resolution machinery if `nova_id` is absent. |
| `nova_id` | `string` (UUID) | NovaCat internal UUID for the nova. If present, bypasses nova resolution entirely. Operator use only; ignored in donor sidecars. |

#### Group 2 — Band and Spectral Context

These fields resolve band identity and spectral measurement context that the adapter
cannot resolve from file-internal signals alone. They operate at file level (applying
uniformly to all rows) unless overridden by row-level context within the file.

Note that not all fields in this group are meaningful for all wavelength regimes.
`phot_system` and `mag_system` are optical/UV/NIR concepts and should be `null` for
X-ray, radio, and gamma-ray data. `band_assertions` is regime-agnostic: it maps any
band string (including energy-band strings like `"0.3-10 keV"` or radio sub-band labels)
to a NovaCat canonical band registry ID, regardless of regime.

| Field | Type | Description |
|---|---|---|
| `phot_system` | `string` | Asserted photometric system (canonical `PhotSystem` value, e.g. `"Johnson-Cousins"`, `"Sloan"`). Applicable to optical/UV/NIR data only; `null` for other regimes. Applied when the file contains no `phot_system` column or when that column's values are ambiguous. |
| `mag_system` | `string` | Asserted magnitude zero-point system (`"Vega"`, `"AB"`, `"ST"`). Applicable to optical/UV/NIR data only; `null` for other regimes. |
| `band_assertions` | `object` | Map from file-internal band string to NovaCat canonical band registry ID. Regime-agnostic: applicable to any band string from any wavelength regime. E.g. `{"V": "Johnson_V", "B": "Johnson_B"}` for optical; `{"soft": "Chandra_soft", "hard": "Chandra_hard"}` for X-ray. Takes precedence over the band registry disambiguation algorithm for the named strings. |
| `filename_convention` | `string` | Named filename convention (from a controlled vocabulary defined in ADR-021) that Layer 0 should apply when parsing the filename for metadata. |

#### Group 3 — Measurement Context

These fields supply measurement-level context that is absent from the primary file's
columns. They are applied as file-level *defaults* — they fill gaps where the file lacks
columns, but do not override row-level values where those are present. A file containing
measurements from multiple telescopes or instruments should carry `telescope` and
`instrument` columns rather than relying on the sidecar's file-level default, which
would apply the same value to all rows regardless of origin.

| Field | Type | Description |
|---|---|---|
| `telescope` | `string` | Telescope name or facility identifier. Applied as a file-level default only; row-level `telescope` column values take precedence. |
| `instrument` | `string` | Instrument identifier. Applied as a file-level default only. |
| `observer` | `string` | Observer name or identifier. Applied as a file-level default only. |

#### Group 4 — Measurement Provenance

These fields supply the scientific attribution for the data. They are the sidecar analog
of `PhotometryRow`'s Section 5 provenance fields (§4.2).

For aggregated data files — where a single paper compiles measurements from multiple
upstream sources — the appropriate value for `bibcode` is the bibcode of the aggregating
paper, since that is the citable source NovaCat is drawing from. Upstream source
attributions (the original observing campaigns or surveys) are best captured in
`orig_catalog` or `notes`, as row-level tracking of upstream bibcodes for each
measurement in a compiled table is generally not feasible from the file alone.

| Field | Type | Description |
|---|---|---|
| `bibcode` | `string` | 19-character ADS bibcode of the source publication. For compiled/aggregated tables, this is the bibcode of the aggregating paper. |
| `doi` | `string` | DOI of the source publication or dataset. |
| `data_url` | `string` | URL of the upstream data source. |
| `orig_catalog` | `string` | Name of the originating catalog or survey (e.g. `"AAVSO"`, `"ZTF DR3"`). For aggregated files, this may be a comma-separated list of upstream sources. |
| `orig_table_ref` | `string` | Table identifier within the source publication (e.g. `"Table 2"`, `"online_table_1"`). |

#### Group 5 — Data Rights

| Field | Type | Description |
|---|---|---|
| `data_rights` | `string` | Licence governing the data (`"public"`, `"CC-BY"`, `"CC-BY-SA"`, `"proprietary"`, `"other"`). Defaults to `"public"` if absent. |
| `embargo_end_date` | `string` (ISO 8601 date) | Date after which the data may be published. Present only for `proprietary` data. Null or absent otherwise. |

#### Group 6 — Structural Hints

These fields help Layer 0 interpret the primary file's structure before the adapter
processes it.

| Field | Type | Description |
|---|---|---|
| `file_format` | `string` | Declared format: `"long"` (tidy, one measurement per row), `"wide"` (multi-band columns), `"color_only"` (contains only color/flux-ratio columns, no single-band measurements). Used to guide the wide-to-long pivot and `ColorRow` routing. |
| `wide_band_columns` | `array of string` | For wide-format files: the column names that represent band measurements (as opposed to metadata columns). Provides the pivot specification when automatic detection is insufficient. |
| `color_columns` | `array of string` | Column names that represent color or flux-ratio measurements. Used to trigger `ColorRow` routing for those columns when the file mixes single-band and color data. |
| `time_system` | `string` | Time system of the file's epoch column (canonical `TimeOrigSys` value, e.g. `"MJD_UTC"`, `"HJD_TT"`). Applied when the file contains no explicit time system indicator. |
| `column_map` | `object` | Explicit mapping from file column names or 0-based column indices (as strings, e.g. `"0"`, `"1"`) to canonical `PhotometryRow` or `ColorRow` field names. Used to resolve ambiguous column headers or to handle headerless files. E.g. `{"col_0": "time_mjd", "col_1": "magnitude", "col_2": "mag_err"}`. This is a general-purpose column identity override; `wide_band_columns` and `color_columns` remain the preferred mechanism for band and color column identification specifically. |

#### Group 7 — Sidecar Metadata

| Field | Type | Description |
|---|---|---|
| `sidecar_version` | `string` | Schema version of the sidecar format (semver, e.g. `"1.0.0"`). **Required.** Enables forward-compatible schema evolution. |
| `created_by` | `string` | Free-text identifier of who created the sidecar (e.g. `"operator:tf"`, or opaque donor ID post-MVP). |
| `notes` | `string` | Free-text operator/donor notes on the file or its context. Not machine-interpreted; preserved in the ingestion audit record. |

#### Group 8 — Donor Identity (post-MVP only)

These fields are populated by the donation API and are not present in operator-prepared
sidecars. They are defined here for forward-compatibility planning; the donation API will
populate them programmatically. See §6 and the proposed DESIGN-003 for full context.

| Field | Type | Description |
|---|---|---|
| `donor_id` | `string` (opaque UUID) | Opaque reference to the donor account. PII is not stored in the sidecar; this is a reference to a separate donor registry. |
| `donor_trust_level` | `string` | Trust tier assigned to this donor by the platform (`"standard"`, `"verified"`, `"trusted"`). Populated by the API; ignored if present in operator sidecars. |

### 3.6 Injection into the Layer 0 Context Object

Layer 0 produces an enriched context object alongside the normalized data table. This
context object is the vehicle by which sidecar fields (and inline header values) are
propagated downstream to the `CanonicalCsvAdapter` (Layer 4) and the workflow handlers.
Its schema is part of the Layer 0 output contract defined in ADR-021; this section
describes the semantics.

The context object carries:

- All sidecar fields that were successfully parsed and validated, keyed by field name
- All inline header key-value pairs that were successfully extracted, keyed by canonical
  field name (synonym-resolved)
- A `context_sources` map recording which value for each field came from which source
  (`sidecar`, `inline_header`, or `file_column`), for resolution provenance (§4.3)
- The trust level applicable to this ingestion (`operator` or `donor`), derived from the
  ingestion event type

The context object is immutable after Layer 0 produces it. Downstream components read
from it but do not modify it.

### 3.7 Precedence Rules

When the same concept is supplied by multiple sources (sidecar, inline file header, and
file-internal column), a defined precedence order determines which value is used.

Band identity resolution, photometric system resolution, and magnitude system resolution
are three separate processes with separate precedence chains, not a single unified
lookup. The sidecar fields that appear in each chain (`band_assertions`, `phot_system`,
`mag_system`) operate on different dimensions and are not interchangeable:
`band_assertions` resolves *which band* a string refers to; `phot_system` asserts *which
photometric system* the file uses; `mag_system` asserts *which magnitude zero-point
system* applies. These are orthogonal concerns.

**For band identity resolution (resolving a band string to a canonical band registry ID):**

1. Sidecar `band_assertions` map (highest precedence — explicit override by name)
2. File-internal row-level `filter_name` or equivalent column value, passed through the
   band registry disambiguation algorithm (ADR-018)
3. Inline file header band value, passed through the band registry disambiguation
   algorithm
4. Band registry disambiguation result using contextual signals (photometric system,
   regime, file-level context) without any explicit assertion

**For photometric system resolution (`phot_system`):**

1. File-internal row-level `phot_system` column value
2. Sidecar `phot_system` (file-level assertion)
3. Inline file header `phot_system` value
4. Band registry inference (the resolved band's registry entry carries its photometric
   system membership; this can be used as a fallback)

**For magnitude zero-point system resolution (`mag_system`):**

1. File-internal row-level `mag_system` column value
2. Sidecar `mag_system` (file-level assertion)
3. Inline file header `mag_system` value
4. Band registry inference (the resolved band's `mag_system` field in the registry entry)

**For measurement context fields (`telescope`, `instrument`, `observer`):**

1. File-internal row-level column value (row-level specificity wins)
2. Sidecar value (file-level default)
3. Inline file header value

**For provenance fields (`bibcode`, `doi`, `orig_catalog`, etc.):**

1. File-internal column value (a file that carries its own `bibcode` column is the
   authoritative source for that information)
2. Sidecar value
3. Inline file header value

**For structural hints (`file_format`, `wide_band_columns`, `column_map`, etc.):**

Sidecar only. Inline headers do not carry structural hints. File-internal signals (e.g.
actual column structure) are used as a cross-check, not an override.

These precedence rules are invariant with respect to trust level. The trust level governs
*how confidently* downstream components treat the resolved value (§3.8), not which value
wins when multiple sources supply a value.

### 3.8 Validation and Trust

Sidecar fields are validated at schema level (type and format) by Layer 0 before
injection into the context object. Fields that fail schema validation are logged and
excluded from the context object; they do not abort ingestion.

Beyond schema validation, the downstream treatment of sidecar-supplied values depends on
the trust level:

**Operator trust level:** Sidecar assertions are accepted as authoritative within the
downstream pipeline. A sidecar-asserted `phot_system` overrides the band registry's
disambiguation result without generating a confidence penalty. The `quality_flag` of
affected rows is not degraded solely because a sidecar assertion was used.

**Donor trust level:** Sidecar assertions are treated as high-quality hints, not
authoritative overrides. The band registry disambiguation algorithm is still run
independently; if the sidecar's `band_assertions` agree with the registry result, the
combined confidence is higher than either alone. If they disagree, the discrepancy is
recorded in the resolution provenance (§4.3) and the `quality_flag` of affected rows is
degraded to `uncertain` (1). Fields where the donor is the uniquely authoritative source
— `data_rights`, `embargo_end_date`, `donor_id`, `observer` — are accepted without
downstream cross-check regardless of trust level.

**Quarantine on irreconcilable conflict.** When sidecar assertions and file-internal
values disagree in a way that the precedence rules cannot resolve and that is materially
significant — for example, the sidecar asserts `phot_system = "Sloan"` but the
file-internal band string is unambiguously `"B"` with no Sloan equivalent, or the sidecar
asserts a `nova_id` that does not match the object name column's resolved identity —
the affected rows must be quarantined rather than processed with either competing value.
A dedicated quarantine reason code (`sidecar_conflict`) is used, and the conflict details
are written to the diagnostics record. This ensures that irreconcilable conflicts produce
a visible, auditable failure rather than a silently incorrect stored row. The
`ValidatePhotometry` handler (Layer 5) is responsible for applying the quarantine
decision; Layer 0 records the conflict in the context object but does not itself
quarantine rows.

---

## 4. Provenance Framework

### 4.1 Three-Tier Model

NovaCat's provenance model for photometric data has three distinct tiers. They are
orthogonal: each captures a different kind of information, and each has a different
consumer.

```
┌─────────────────────────────────────────────────────────────────────┐
│  Tier 1 — Measurement Provenance                                    │
│  Who observed it. What published it. Where the data came from.     │
│  Consumer: researchers using NovaCat data.                          │
│  Lives on: every PhotometryRow and ColorRow record.                 │
├─────────────────────────────────────────────────────────────────────┤
│  Tier 2 — Resolution Provenance                                     │
│  How NovaCat processed the data. What confidence level applies.    │
│  Consumer: NovaCat operator; future data quality audit tools.       │
│  Lives on: every PhotometryRow and ColorRow record.                 │
├─────────────────────────────────────────────────────────────────────┤
│  Tier 3 — Ingestion Provenance                                      │
│  That the ingestion happened, when, and from what file.             │
│  Consumer: NovaCat operator; reproducibility / audit trail.         │
│  Lives on: one record per ingestion event (file × nova_id pair).   │
└─────────────────────────────────────────────────────────────────────┘
```

The tiers are not a hierarchy of importance — they are a separation of concerns. A
researcher tracing the scientific origin of a measurement needs Tier 1. An operator
debugging an anomalous light curve point needs Tier 2. An operator re-running ingestion
after a pipeline fix needs Tier 3.

### 4.2 Measurement Provenance (Tier 1)

Tier 1 formalizes and extends the existing `PhotometryRow` Section 5 provenance fields.
The current fields — `bibcode`, `doi`, `data_url`, `orig_catalog`, `orig_table_ref`,
`telescope`, `instrument`, `observer`, `data_rights` — are well-specified for literature-
compiled data and should be preserved. They become the baseline for `ColorRow` Section 5
as well (§5.3).

**Multiple provenances within a single file.** A single source file frequently contains
measurements with heterogeneous provenance — different telescopes, instruments, or even
different originating publications represented within the same table. This is handled
naturally by the existing row-level field structure: `telescope`, `instrument`,
`observer`, and `bibcode` are all per-row fields on `PhotometryRow`, so rows from
different observing campaigns within the same file carry different values. No special
handling is required as long as the source file carries these values in its own columns;
the sidecar's Group 3 and Group 4 fields are file-level *defaults* that fill gaps, not
mandatory overrides of row-level values.

**Bibliographic provenance for aggregated papers.** A compiled paper that aggregates data
from multiple upstream sources — multiple observatories, multiple surveys — is a common
pattern in nova research. For such a file, the appropriate `bibcode` (in the sidecar and
in the ingested rows, if the file lacks a `bibcode` column) is the bibcode of the
aggregating paper. That paper is the citable source NovaCat is drawing from. Upstream
source attributions are captured in `orig_catalog`, `notes`, or (where the file
explicitly records them) in per-row fields. Row-level tracking of upstream bibcodes for
each individual measurement in a compiled table is generally not feasible from the file
alone; this limitation is noted but not resolved here.

**Measurement provenance additions for donated data.** The current Section 5 fields are
underspecified for donated data. Two additional fields are proposed:

| Field | Type | Description |
|---|---|---|
| `donor_attribution` | `string \| None` | Display attribution for donated data (e.g. `"J. Smith, private communication"`). Not a donor account reference — this is the public-facing credit string that appears in the catalog. |
| `data_origin` | `string` enum | How the data entered NovaCat: `"literature"` (operator-staged from a published source), `"donated"` (donor upload), `"operator_derived"` (operator-computed, e.g. calibrated from raw frames). Defaults to `"literature"`. |

The `data_origin` field enables downstream filtering — a researcher who wants only
published, peer-reviewed data can filter on `data_origin = "literature"`. It also has
implications for the publication gate design, which is a separate concern noted here for
future design work.

### 4.3 Resolution Provenance (Tier 2)

**Scope clarification.** The band resolution *algorithm* — which tier fires, how
disambiguation proceeds, what escalation policy applies — is entirely within the purview
of DESIGN-001 (Layer 4 / Layer 2) and ADR-018. That algorithm is not defined here.
DESIGN-002's contribution is the *vocabulary and schema* for recording what the algorithm
produced: the set of fields that the `CanonicalCsvAdapter` (Layer 4) populates on each
output row, standardized here so that ADR-019 can adopt them as part of the
`PhotometryRow` schema revision. The values in the Tier 2 fields are produced during
adapter execution (DESIGN-001 domain); DESIGN-002 defines what gets recorded and what
the allowed values mean.

Every stored `PhotometryRow` and `ColorRow` record should carry the following resolution
provenance fields:

| Field | Type | Description |
|---|---|---|
| `band_resolution_tier` | `string` enum | Which mapping tier resolved the band: `"tier1_canonical"`, `"tier2_synonym"`, `"tier3_ucd"`, `"tier4_ai"`, `"sidecar_assertion"`, `"operator_confirmed"`. Populated by the adapter. |
| `band_resolution_confidence` | `string` enum | Confidence level of the band resolution: `"high"` (unambiguous registry match), `"medium"` (disambiguation required but resolved), `"low"` (resolved via sidecar hint with no independent corroboration). `"unresolved"` should not appear in persisted rows — those rows are quarantined before persistence. |
| `phot_system_source` | `string` enum | How the photometric system was determined: `"file_column"`, `"sidecar"`, `"inline_header"`, `"band_registry"`, `"inferred"`. |
| `sidecar_contributed` | `bool` | Whether any sidecar field influenced the band or photometric system resolution for this row. |
| `context_sources` | `object \| None` | Snapshot of the `context_sources` map from the Layer 0 context object (§3.6), recording which source supplied each field value. Nullable; may be omitted if storage constraints require it. |

The exact field set is a key input to ADR-019, which will decide whether these fields
belong as columns in the photometry table, as a nested JSON document in a
`resolution_meta` column, or in a separate resolution provenance table joined by
`row_id`. That decision depends on the storage format chosen in ADR-020.

**UCD alignment.** The closest IVOA UCD for resolution confidence metadata is
`meta.code.qual`, which applies broadly to data quality indicators. The `quality_flag`
field already uses this UCD. Resolution confidence is a distinct concept — it describes
the pipeline's certainty about *what* the data is, not the data's intrinsic quality.
No precise IVOA UCD exists for this concept; NovaCat's controlled vocabulary is the
authoritative definition.

### 4.4 Ingestion Provenance (Tier 3)

An ingestion provenance record is written once per ingestion event by `FinalizeIngestion`.
An *ingestion event* is defined as the processing of one (primary file, `nova_id`) pair.
A multi-nova file that is split by Layer 0 into three per-nova subsets produces three
ingestion events and three ingestion provenance records — one per resulting nova table,
not one per source file.

#### 4.4.1 Ingestion Provenance Record

The ingestion provenance record captures the audit trail of the ingestion event:

| Field | Type | Description |
|---|---|---|
| `ingestion_id` | UUID | Stable identifier for this ingestion event. Generated at workflow start and carried through all handler invocations. |
| `nova_id` | UUID | The nova this ingestion event produced data for. |
| `source_file_sha256` | `string` | SHA-256 of the raw primary file as staged in S3. The idempotency key (per ADR-015 Decision 3). |
| `source_file_s3_key` | `string` | S3 key of the raw primary file. |
| `sidecar_s3_key` | `string \| None` | S3 key of the sidecar, if one was present. |
| `sidecar_sha256` | `string \| None` | SHA-256 of the sidecar file. |
| `workflow_run_id` | `string` | Step Functions execution ARN. |
| `ingestion_timestamp` | `string` (ISO 8601) | UTC timestamp when `FinalizeIngestion` ran. |
| `row_count_valid` | `int` | Number of rows successfully persisted. |
| `row_count_quarantined` | `int` | Number of rows quarantined. |
| `schema_version` | `string` | The `PhotometryRow` (or `ColorRow`) schema version against which rows were validated. |
| `pipeline_version` | `string` | The Lambda deployment version / Git SHA of the ingestion pipeline. |
| `notes` | `string \| None` | Operator-supplied notes from the sidecar, if present. Carried through for audit trail completeness. |

#### 4.4.2 Column Mapping Manifest

Alongside the ingestion provenance record, `FinalizeIngestion` should produce a **column
mapping manifest** — a structured document recording in detail how the source file was
parsed and interpreted. This provides a machine-readable record of the exact decisions
applied to the file: which columns mapped to which fields, how each band string was
resolved, and what the sidecar or inline headers contributed. It is the primary tool for
debugging unexpected ingestion results and for verifying reproducibility.

The column mapping manifest captures:

| Field | Type | Description |
|---|---|---|
| `ingestion_id` | UUID | Foreign key to the ingestion provenance record (§4.4.1). |
| `source_columns` | `array of object` | One entry per column in the source file. Each entry records: the original column name or index, the canonical field name it was mapped to (or `null` if unmapped), the mapping tier used (`tier1_canonical`, `tier2_synonym`, `tier3_ucd`, `sidecar_column_map`, `inline_header`, `unmapped`), and any synonym or UCD that triggered the mapping. |
| `band_resolutions` | `array of object` | One entry per distinct band string encountered in the file. Each entry records: the original band string, the resolved canonical band registry ID (or `null` if unresolved), the resolution tier, the confidence level, and the contextual signals used (sidecar assertion, photometric system context, etc.). |
| `wide_to_long_pivot` | `object \| None` | If a wide-to-long pivot was performed: the list of band columns that were pivoted, the list of metadata columns that were preserved across rows, and the number of output rows produced. `null` if the file was already in long format. |
| `context_object_snapshot` | `object` | A full snapshot of the Layer 0 context object at the point it was passed to the adapter. Enables exact reproduction of the ingestion context. |

Where the column mapping manifest is stored — as part of the ingestion provenance
DynamoDB record, as a separate S3 JSON blob, or both — is deferred to ADR-020 alongside
the ingestion provenance record storage decision.

### 4.5 Confidence Semantics and Quality Flag Propagation

The Tier 2 resolution confidence interacts with the `quality_flag` field on stored rows.
The propagation rule is intentionally conservative:

| Resolution confidence | Sidecar trust level | Quality flag effect |
|---|---|---|
| `high` | any | No effect. `quality_flag` reflects intrinsic data quality only. |
| `medium` | operator | No effect. Operator-assisted disambiguation is trusted. |
| `medium` | donor | Degrade by at most 1 level (e.g. `good` → `uncertain`). |
| `low` | operator | Degrade by 1 level. |
| `low` | donor | Degrade by 1 level, and set `band_resolution_confidence = "low"` explicitly. |
| `unresolved` | any | Row is quarantined; not persisted. |

"Degrade by 1 level" means: if the row's intrinsic `quality_flag` is `good` (0), it
becomes `uncertain` (1). If it is already `uncertain` or worse, no further degradation
is applied — resolution confidence does not stack on top of already-poor data quality.

The `quality_flag` propagation logic lives in `ValidatePhotometry` (Layer 5). The
resolution confidence assessment lives in the adapter (Layer 4). The two stages are
separate: the adapter reports confidence; `ValidatePhotometry` applies the propagation
rule.

### 4.6 Lineage: Reconstructing the Source Chain

A NovaCat operator or researcher should be able to trace any persisted measurement back
to its ultimate source. The complete lineage chain is:

```
Persisted row (PhotometryRow / ColorRow)
  └─ row_id
       └─ ingestion_id (Tier 3 record, §4.4.1)
            ├─ source_file_sha256
            │    └─ S3 staged file (the exact bytes ingested)
            ├─ sidecar_s3_key (if present)
            │    └─ S3 sidecar file (the exact context supplied)
            ├─ workflow_run_id
            │    └─ Step Functions execution history
            └─ column_mapping_manifest (§4.4.2)
                 └─ exact interpretation applied to every column and band string
```

From the Tier 1 provenance fields on the row itself:

```
Persisted row
  ├─ bibcode → ADS record → full publication
  ├─ data_url → upstream source table / archive
  └─ orig_catalog → originating survey or catalog
```

The lineage chain is complete when both the Tier 1 and Tier 3 chains are preserved. The
Tier 3 chain enables the operator to reconstruct *exactly what was ingested and how*;
the Tier 1 chain enables a researcher to locate the *original scientific source*
independently of NovaCat's infrastructure.

**Key invariant.** The SHA-256 of the staged source file is the anchor of the Tier 3
chain. As long as the staged file is retained in S3, the ingestion is fully
reproducible: re-run the workflow with the same `source_file_s3_key` and
`source_file_sha256` and the result must be identical given the same pipeline version.

**Lineage in public-facing data products.** The lineage chain defined here has direct
implications for what NovaCat provides when data is downloaded. A bulk download should
be accompanied by a provenance manifest enabling a researcher to trace each measurement
back through the Tier 1 chain and credit the correct sources. The design of that
manifest — its content, format, and generation — is a frontend and publication pipeline
concern, noted here for future design work.

---

## 5. ColorRow: Colors and Flux Ratios as a First-Class Data Type

### 5.1 Motivation and Distinction from `PhotometryRow`

A color or flux ratio is not a single-band measurement. It is a relationship between two
bands — either observed directly (a color measured from simultaneous or near-simultaneous
observations in two bands) or derived (a color computed from separately measured single-
band magnitudes). This structural difference makes a shared base type with `PhotometryRow`
insufficient:

- A `PhotometryRow` references one band; a color references two. A band field on
  `PhotometryRow` is unambiguous; on a color row it requires disambiguation: is
  `band_id` the bluer band? The redder band? The numerator?
- The epoch semantics differ. A single-band measurement has a well-defined observation
  time. A color computed from two measurements at different times has *two* observation
  times, neither of which is "the" epoch in any physically unambiguous sense.
- Upper-limit semantics differ. An upper limit on a color is a bound on a *difference*,
  not a bound on a flux — it does not mean either individual band was a non-detection.
- Not all colors have an associated photometric system in the same sense as single-band
  measurements. The color `B-V` is defined with respect to a specific photometric system
  (Johnson-Cousins), but a flux ratio across two widely separated regimes is not a
  magnitude-system-dependent quantity at all.

A dedicated `ColorRow` type, distinct from `PhotometryRow`, is therefore the correct
model. They share the Tier 1 measurement provenance field set (§4.2) and the Tier 2
resolution provenance field set (§4.3), but their measurement fields and band-reference
fields differ.

### 5.2 Taxonomy of Derived Quantity Types

`ColorRow` must accommodate four distinct subtypes. The field schema (§5.3) must handle
all four cleanly.

**Type A — Magnitude difference (optical/UV/NIR).** The canonical color index: `B-V`,
`g-r`, `U-B`, `J-H`, etc. The value is `magnitude(band_1) - magnitude(band_2)` in a
defined photometric and magnitude system. Both bands must be defined in the band
registry. The photometric system is meaningful and must be recorded. The value is
inherently logarithmic (magnitudes are defined on a log scale), but this is definitional
and does not need to be flagged explicitly — see `value_is_log` in §5.3.

**Type B — Flux ratio.** A ratio of flux densities across two bands. The two bands may
span widely separated regimes with no shared photometric or magnitude system. The value
may be expressed as a linear ratio or as a base-10 logarithm of that ratio (e.g., a
logarithmic flux ratio between UV and optical bands); the `value_is_log` flag (§5.3)
distinguishes these two representations.

**Type C — Hardness ratio (X-ray).** A specialization of the flux ratio concept for
X-ray data: `HR = (H - S) / (H + S)` where H and S are hard and soft band count rates.
The bands are instrument-specific energy ranges. The value is bounded in [-1, 1]. The
formula is specific enough to warrant explicit representation rather than being collapsed
into the generic flux ratio type.

**Type D — Color index without individual-band measurements.** This is the real-world
occurrence noted in DESIGN-001 §4.2: a source file that reports only colors, with no
individual-band measurements present. `V-Ic = 1.23 ± 0.05` is a complete, valid data
product even if the individual `V` and `Ic` magnitudes are not reported. NovaCat must
ingest these gracefully. The `ColorRow` schema must not require that the component band
measurements exist elsewhere in the system.

### 5.3 Schema Design

The `ColorRow` schema is organized in parallel with `PhotometryRow` for consistency and
to simplify the shared provenance framework implementation.

> **Schema status:** The field set below is a proposed starting point, not a final
> schema. Field names, types, cross-field invariants, and the division between
> `PhotometryRow` and `ColorRow` concerns are all expected to be refined during the
> authoring of ADR-019 and ADR-022. Implementers must use those ADRs as the
> authoritative reference.

#### Section 1 — Identity

| Field | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `row_id` | UUID | `meta.id` | NO | Stable unique identifier for this color row. |
| `nova_id` | UUID | `meta.id;src` | NO | NovaCat UUID of the nova. |
| `schema_version` | string | `meta.version` | NO | `ColorRow` schema version. |

#### Section 2 — Temporal

| Field | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `time_mjd` | float | `time.epoch` | YES | Epoch of the color measurement in MJD (UTC). NULL for time-averaged or undated colors. |
| `time_mjd_band1` | float | `time.epoch` | YES | Epoch of the band-1 measurement, if the color was derived from asynchronous observations. NULL if simultaneous or not decomposable. |
| `time_mjd_band2` | float | `time.epoch` | YES | Epoch of the band-2 measurement. NULL if simultaneous or not decomposable. |
| `time_orig` | float | `time.epoch` | YES | Original reported epoch value (before conversion to MJD UTC). |
| `time_orig_sys` | `TimeOrigSys` | `time.scale` | YES | Time system of `time_orig`. |

When `time_mjd_band1` and `time_mjd_band2` are both non-NULL and differ, `time_mjd`
should be the midpoint of the two epochs. When both are NULL and the color was observed
simultaneously, `time_mjd` is the single observation epoch.

#### Section 3 — Band Identity

| Field | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `band1_id` | string | `instr.bandpass` | NO | NovaCat canonical band ID for the first band (numerator for ratios; minuend for magnitude differences; bluer band by convention for optical colors). |
| `band2_id` | string | `instr.bandpass` | NO | NovaCat canonical band ID for the second band. |
| `color_string_orig` | string | `meta.note` | YES | Original string representation of the color from the source file (e.g. `"B-V"`, `"V-Ic"`). Preserved for traceability; the canonical representation is derived from `band1_id` and `band2_id`. |
| `color_type` | `ColorType` enum | `meta.code` | NO | Subtype: `magnitude_difference`, `flux_ratio`, `hardness_ratio`. |
| `phot_system` | string | `meta.code` | YES | Photometric system (canonical `PhotSystem` value). Non-NULL for `magnitude_difference` only; NULL for `flux_ratio` and `hardness_ratio`, where the photometric system concept does not apply. |
| `mag_system` | `MagSystem` | `meta.code` | YES | Magnitude zero-point system. Non-NULL for `magnitude_difference` only; NULL for `flux_ratio` and `hardness_ratio`. |

#### Section 4 — Measurement

| Field | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `value` | float | `phot.color` (Type A/D), `phot.flux;arith.ratio` (Type B/C) | NO | The color or ratio value. For magnitude differences: `mag(band1) − mag(band2)`. For flux ratios: `flux(band1) / flux(band2)`, or `log10` of same if `value_is_log = True`. For hardness ratios: `(H−S)/(H+S)`. |
| `value_err` | float | `stat.error;phot.color` | YES | 1-sigma uncertainty on `value`. |
| `value_is_log` | bool | `meta.code` | NO | Applicable to `color_type = flux_ratio` only. `True` if `value` is `log10(flux(band1)/flux(band2))` rather than the linear ratio. `False` otherwise. For `magnitude_difference`, the log nature of the quantity is definitional (magnitudes are a log scale) and this flag is always `False` — no disambiguation is needed. For `hardness_ratio`, the formula is fixed and this flag is always `False`. Defaults to `False`. |
| `is_upper_limit` | bool | `meta.code.qual` | NO | True if this is an upper limit on the color value (i.e. a bound on the magnitude difference or ratio). Defaults to `False`. |
| `limiting_value` | float | `phot.color;stat.max` | YES | The limiting color value for upper-limit rows. NULL when `is_upper_limit = False`. |
| `quality_flag` | `QualityFlag` | `meta.code.qual` | NO | Data quality flag (same enum as `PhotometryRow`). |
| `notes` | string | `meta.note` | YES | Free-text notes on this measurement. Max 2048 characters. |

#### Section 5 — Provenance (Tier 1)

Identical field set to `PhotometryRow` Section 5, plus the Tier 1 additions from §4.2
(`data_origin`, `donor_attribution`).

#### Section 6 — Resolution Provenance (Tier 2)

The Tier 2 field set from §4.3, applied to both `band1_id` and `band2_id` independently:
`band1_resolution_tier`, `band1_resolution_confidence`, `band2_resolution_tier`,
`band2_resolution_confidence`, plus the shared `phot_system_source`,
`sidecar_contributed`, and `context_sources` fields. The exact schema is an input to
ADR-019 and ADR-022.

#### Cross-field invariants

1. `value` must be non-NULL for all non-upper-limit rows.
2. `limiting_value` must be non-NULL when `is_upper_limit = True`.
3. `limiting_value` must be NULL when `is_upper_limit = False`.
4. `phot_system` and `mag_system` must both be non-NULL when
   `color_type = magnitude_difference`, and both NULL when `color_type = flux_ratio`
   or `color_type = hardness_ratio`.
5. `value_is_log` may only be `True` when `color_type = flux_ratio`.
6. If both `time_mjd_band1` and `time_mjd_band2` are non-NULL, `time_mjd` must be
   their midpoint (within floating-point tolerance).
7. `band1_id` and `band2_id` must be distinct.

### 5.4 Relationship to the Band Registry and Resolution Model

Colors are not exempt from the band identity problem that drives DESIGN-001's band
registry design. `B-V` reported by a source is not self-defining: `B` in the
Johnson-Cousins system and `b` in the Strömgren system are distinct passbands with
different effective wavelengths, and treating the color string as an opaque identifier
would silently corrupt the scientific meaning of the stored record.

`ColorRow`'s `band1_id` and `band2_id` must therefore be resolved through the same band
registry (ADR-017) and disambiguation algorithm (ADR-018) as `PhotometryRow`'s `band_id`.
The resolution inputs differ slightly:

- The `color_string_orig` (e.g. `"B-V"`) must be parsed to extract the two component
  band strings before registry lookup.
- The component band strings are then resolved independently through the same
  disambiguation algorithm.
- The color type context often resolves photometric system ambiguity that would be
  ambiguous for a standalone band string. `B-V` combined with the nova community context
  strongly implies Johnson-Cousins; `g-r` in a ZTF-sourced file strongly implies Sloan.
  These contextual signals should inform the disambiguation algorithm's confidence
  assessment.
- For hardness ratios, the band registry entries are instrument-specific energy ranges
  (e.g. `Chandra_soft`, `Chandra_hard`). These are valid band registry entries under
  the DESIGN-001 model; no special handling is required.

The practical consequence is that `ColorRow` ingestion depends on Epics A (band registry)
and B (disambiguation algorithm) being complete before the color adapter can be
implemented — the same dependency chain as `PhotometryRow`.

**Key question.** Should NovaCat *compute* colors from individually ingested
`PhotometryRow` records, or should it ingest only colors that are *directly reported* in
source files? This is a significant design question with implications for catalog
completeness, reproducibility, and pipeline complexity. It is explicitly deferred to a
future ADR (see §7, Question 6).

### 5.5 Routing from Layer 0 and Workflow Architecture

#### Routing at the wide-to-long pivot

The wide-to-long pivot in Layer 0 (DESIGN-001 §5 Layer 0) is the decision point for
routing. After the pivot, each output column maps to either a `PhotometryRow` field, a
`ColorRow` field, or neither (metadata / unrecognized). The routing logic at this layer
must handle four cases:

**Case 1 — Single-band file.** All measurement columns map to `PhotometryRow` fields.
Dispatch to the photometry adapter. Standard path.

**Case 2 — Color-only file.** All measurement columns map to `ColorRow` fields (e.g.
columns named `B-V`, `V-Ic`, `g-r`). No single-band measurement columns are present.
Dispatch to the color ingestion path. This is a valid, non-malformed file; it must be
handled gracefully, not quarantined.

**Case 3 — Mixed file.** The file contains both single-band measurement columns and color
columns. This is DESIGN-001 Open Question 11. The recommended approach is to split the
file at this stage: pivot the single-band columns to produce `PhotometryRow` inputs (one
row per band per epoch), and extract the color columns separately to produce `ColorRow`
inputs. The two sets of inputs are then dispatched to their respective ingestion paths.
Metadata columns (epoch, telescope, observer, etc.) are duplicated to both sets.

**Case 4 — Single band plus dependent colors.** A specialized variant of the mixed case
that warrants explicit treatment: a file where one column contains single-band
measurements in band X, and the remaining measurement columns are colors that reference X
as one of their components (e.g. a `V` column alongside `B-V`, `V-R`, `V-Ic`). This
file type was the original real-world impetus for the `ColorRow` design. In principle,
the missing component magnitudes could be algebraically derived from the single-band
measurements and the colors. The recommended treatment is to route this file as Case 3 —
the `V` column dispatches to the photometry path and the color columns dispatch to the
color path — without attempting to derive the missing component magnitudes at ingestion
time. Automated algebraic reconstruction from mixed-format files introduces correctness
risks that are not justified by the benefit at this stage; derivation of colors from
individual-band measurements is deferred to the ADR that resolves §7 Question 6.

#### Workflow architecture for ColorRow ingestion

The workflow architecture for `ColorRow` downstream of Layer 0 is an open question. Three
structural options merit evaluation:

**Option A — Extend `ingest_photometry` with a ColorRow branch.** The existing workflow
gains a post-pivot branch: if the normalized output contains `ColorRow` inputs, the
workflow routes them through a `ValidateColor` → `PersistColor` state chain in parallel
with (or after) the `ValidatePhotometry` → `PersistPhotometry` chain.

*Assessment:* Minimizes the number of distinct workflow definitions. Couples the
photometry and color lifecycles in a single execution, which simplifies the
`FinalizeIngestion` record (one record captures both). May become unwieldy if the two
paths diverge significantly.

**Option B — Separate `ingest_color` workflow.** Layer 0 dispatches `ColorRow` inputs
to a dedicated `ingest_color` Step Functions workflow that mirrors the structure of
`ingest_photometry`.

*Assessment:* Clean separation of concerns. Each workflow is independently deployable
and testable. Produces separate ingestion provenance records, which may be desirable or
not depending on whether the two ingestions are considered one logical event. Adds
infrastructure overhead.

**Option C — Delegate ColorRow ingestion to a post-processing step.** `ingest_photometry`
handles only `PhotometryRow` records. `ColorRow` ingestion is triggered separately,
either by a downstream process or as a scheduled sweep. This option is most relevant if
NovaCat computes colors from individual-band measurements (§7 Question 6); if it only
ingests directly reported colors, this option offers little benefit.

The choice between these options is deferred to ADR-021 (Layer 0 spec) or a dedicated
ADR-022. The design here does not prescribe a choice; it defines the routing semantics
that any workflow architecture must implement.

### 5.6 Open Questions Deferred to a Future ADR

| # | Question |
|---|---|
| A | Should NovaCat compute colors from individual `PhotometryRow` records, or ingest only directly reported colors? If both, what governs which approach applies? |
| B | What is the canonical storage target for `ColorRow` records? Same S3 Parquet target as `PhotometryRow` (possibly with a `row_type` partition column), or a separate file? |
| C | What is the deduplication key for `ColorRow` records? The `bibcode` + band pair + epoch composite used for `PhotometryRow` is not directly applicable when colors are derived quantities. |
| D | What is the workflow architecture for `ColorRow` ingestion (Options A/B/C above)? |

---

## 6. Donation Workflow Context

> **Scope note.** The donation workflow is sufficiently complex and important to warrant
> its own dedicated design document, proposed as DESIGN-003. This section is intentionally
> high-level: it establishes the forward-compatibility constraints that decisions made now
> must satisfy, and identifies the key provenance-related design questions that DESIGN-003
> will need to resolve. It is not a complete donation workflow design.

### 6.1 Relationship to the MVP Operator Path

The MVP ingestion mechanism (DESIGN-001 Decision 1) is the baseline: the operator places
a prepared file in `uploads/photometry/<nova_id>/` and manually publishes an
`IngestPhotometryEvent`. The entire ingestion pipeline — Layer 0 through
`FinalizeIngestion` — is defined around this model.

The donation pathway extends this model without replacing it. The post-MVP donation
feature introduces an upload API surface. At the boundary, this API does the following:

1. Accepts a donor-uploaded file and (optionally) a donor-supplied sidecar
2. Assigns S3 keys to both
3. Augments the donor's sidecar with `donor_id`, `donor_trust_level`, and the platform-
   assigned identity fields
4. Publishes an `IngestPhotometryEvent` with `sidecar_s3_key` set to the assigned
   sidecar key

From that point forward, the ingestion pipeline sees a standard event and processes it
identically to an operator-staged ingestion — the only difference is the trust level in
the context object, which governs validation behavior per §3.8. The donation feature is
fully additive: it is a new event publisher, not a new pipeline.

### 6.2 Donor as a Provenance Source

Donated data differs from operator-staged data in two provenance-relevant ways:

**Attribution.** A donor is the original observer or the custodian of the data. Their
attribution must be recorded and eventually displayed in NovaCat. The `donor_attribution`
field (§4.2) carries the public-facing credit string; the `donor_id` in the sidecar
(§3.5 Group 8) carries the reference to the donor account. These are distinct:
`donor_id` is an internal reference that enables operator follow-up; `donor_attribution`
is the display string that appears in the catalog.

**Data rights.** Donated data may carry license conditions or embargoes that published
literature data does not. The `data_rights` and `embargo_end_date` fields (§3.5 Group 5)
are the mechanism for this. The ingestion pipeline must respect embargo status: a row
with a future `embargo_end_date` should be ingested and stored, but excluded from the
public catalog publication until the embargo lapses. The full design of embargo
enforcement — which layer checks it, how the publication gate implements it — is deferred
to DESIGN-003.

### 6.3 The Sidecar as the Donor's Voice

The sidecar is the primary mechanism by which a donor supplies context to the ingestion
pipeline. In the post-MVP donation UX, a donor completing an upload form would supply:

- Object name (→ `nova_name`)
- Telescope and instrument (→ `telescope`, `instrument`)
- Filter or photometric system (→ `phot_system`, or `band_assertions` if they know
  the specific bands)
- Time system (→ `time_system`)
- Attribution and data rights (→ `donor_attribution`, `data_rights`,
  `embargo_end_date`)
- Bibcode or DOI if the data accompanies a publication (→ `bibcode`, `doi`)
- Any structural hints if the file is wide-format (→ `file_format`,
  `wide_band_columns`)

The upload API translates the form submission into the sidecar JSON schema defined in
§3.5. The donor never sees the sidecar format directly; the form is the UX surface. This
separation is deliberate: the sidecar schema can evolve independently of the donation UX.

### 6.4 Forward Compatibility Requirements

The design decisions made in this document must not require backend rework when the
donation API is introduced. The specific forward-compatibility constraints are:

1. **`IngestPhotometryEvent` must include `sidecar_s3_key`** (Option B from §3.2) before
   the donation pathway is built. Adding it post-MVP would require amending the event
   contract while the workflow is in production — a non-trivial change. This is the
   strongest argument for adopting Option B now even though MVP does not require it for
   the operator path.

2. **The sidecar schema must include `sidecar_version`** from the first deployment.
   The donation API will produce sidecars at whatever schema version is current; the
   ingestion pipeline must be able to parse them. Version-aware parsing requires a
   `sidecar_version` field from day one.

3. **The trust level model must be built into the context object** even though only
   `operator` trust will be exercised in MVP. Adding trust-level-conditional behavior
   post-MVP to a pipeline that was built assuming operator trust only would require
   changes throughout the adapter and validation logic.

4. **The `data_origin` field** on `PhotometryRow` and `ColorRow` must be present in the
   schema from the first production deployment. Backfilling `data_origin = "literature"`
   on all previously ingested rows after the donation feature launches is feasible but
   operationally unpleasant. Better to default it at ingestion time from the start.

---

## 7. Key Design Questions

| # | Question | Target ADR | Notes |
|---|---|---|---|
| 1 | What is the sidecar association mechanism: explicit `sidecar_s3_key` in `IngestPhotometryEvent` (Option B), filename convention (Option A), or hybrid (Option C)? | ADR-021 | Option B recommended; see §3.2. |
| 2 | Where is the ingestion provenance record stored? DynamoDB item, S3 JSON blob, or both? | ADR-020 | The logical field set is defined in §4.4.1; storage is deferred. |
| 3 | Where is the column mapping manifest stored? Same decision as Question 2, or separate? | ADR-020 | The logical content is defined in §4.4.2. |
| 4 | Are Tier 2 resolution provenance fields stored as columns in the photometry table, as a nested JSON column, or in a separate joined table? | ADR-019 | Answer depends on ADR-020's storage format choice. |
| 5 | What is the controlled vocabulary and parsing rules for the `filename_convention` sidecar field? | ADR-021 | Closely coupled to the Layer 0 spec. |
| 6 | Should NovaCat compute colors from individual `PhotometryRow` records, or ingest only directly reported colors? | TBD (ADR-022 or ADR-021) | Has major implications for catalog completeness and pipeline complexity. |
| 7 | What is the `ColorRow` workflow architecture: extended `ingest_photometry` branch (Option A), separate `ingest_color` workflow (Option B), or post-processing sweep (Option C)? | ADR-021 or ADR-022 | See §5.5. |
| 8 | What is the canonical storage target and persistence format for `ColorRow` records? | TBD (likely ADR-020 extension or ADR-022) | May share the photometry Parquet target with a `row_type` partition. |
| 9 | What is the deduplication key for `ColorRow` records? | ADR-022 | The `PhotometryRow` bibcode + band + epoch key is not directly applicable. |
| 10 | ~~What is the inline header keyword controlled vocabulary and synonym registry for Layer 0 header extraction?~~ **Resolved:** `synonyms.json` is expanded in scope to cover all ingestion metadata fields, not just `PhotometryRow` fields. Inline header extraction reuses this expanded registry. | ADR-021 | Decision reached in ADR-021 conversation. |
| 11 | How does embargo status interact with the publication gate? Which layer enforces embargo exclusion? | DESIGN-003 / publication gate ADR (TBD) | Not in scope for the ingestion pipeline; flagged for DESIGN-003. |
| 12 | What is the full donation workflow design, including the upload API, donor trust tiers, donor registry, abuse prevention, and embargo enforcement? | DESIGN-003 | §6 establishes forward-compatibility constraints only. |

---

## 8. Work Decomposition

This document's outputs feed into existing epics and open a small number of new ones.
No new full epic sequence is proposed here; the work is additive to the DESIGN-001 epic
structure.

**Feeds into Epic A-0 (Pre-Ingestion Normalization Design):**

- The complete sidecar contract (§3) replaces the stub that Epic A-0 was to produce
- The inline header extraction spec (§3.4) is a new Layer 0 input to ADR-021
- The `ColorRow` routing logic at the pivot (§5.5) is a Layer 0 concern for ADR-021
- The column mapping manifest (§4.4.2) is a `FinalizeIngestion` output to be specified
  in ADR-021
- Open Questions 1, 5, 7, and 10 are directly in scope for ADR-021

**Feeds into Epic C (Photometry Table Model Revision / ADR-019):**

- The Tier 2 resolution provenance field set (§4.3) is the primary new input to ADR-019
- The Tier 1 measurement provenance additions (`data_origin`, `donor_attribution`) are
  additive schema changes to `PhotometryRow` in ADR-019

**Feeds into Epic E (Ingestion Workflow Implementation / ADR-020):**

- The ingestion provenance record (§4.4.1) specifies the `FinalizeIngestion` output
- The column mapping manifest (§4.4.2) is a companion `FinalizeIngestion` artifact
- Open Questions 2, 3, and 4 are in scope for ADR-020

**New work item — ColorRow design (ADR-022, proposed):**

The `ColorRow` schema defined in §5.3, the storage/persistence format, the deduplication
key, and the workflow architecture questions (Open Questions 6–9) are sufficiently
self-contained to warrant a dedicated ADR. The dependency chain for `ColorRow`
implementation mirrors the `PhotometryRow` chain: band registry (ADR-017) and
disambiguation algorithm (ADR-018) must be complete before a `ColorRow` adapter can be
implemented.

**New design document — Donation Workflow (DESIGN-003, proposed):**

The full donation workflow — upload API design, donor trust tiers, donor registry, abuse
prevention, embargo enforcement, donation UX — warrants a dedicated design document.
§6 of this document establishes the forward-compatibility constraints and the provenance
framework that DESIGN-003 must build on; it is not itself a complete donation design.

**Not a new epic.** The sidecar and provenance framework do not require implementation
of new standalone components. They manifest as:
- An updated `IngestPhotometryEvent` schema (adding `sidecar_s3_key`)
- Layer 0 logic additions (sidecar fetch + inline header scan + context object
  construction), specified in ADR-021
- Schema additions to `PhotometryRow` (Tier 1 and Tier 2 provenance fields), adopted
  in ADR-019
- The `FinalizeIngestion` handler output, specified in ADR-021 / ADR-020

---

## 9. Relationship to Existing Documents

| Document | Relationship | Action |
|---|---|---|
| `DESIGN-001` | Direct predecessor. This document completes the open contracts DESIGN-001 left as stubs or forward references. | No changes to DESIGN-001 required; annotate §8 table row for DESIGN-002 with `Status: Authored`. |
| `ADR-015` | The `IngestPhotometryEvent` contract must be amended to add `sidecar_s3_key` (§3.2 Option B). This is an additive change to an existing decision. | Annotate ADR-015 with a forward reference to ADR-021 for the `sidecar_s3_key` amendment. |
| `ADR-019` (forthcoming) | The Tier 2 resolution provenance field set (§4.3) and Tier 1 additions (§4.2) are direct inputs to the `PhotometryRow` schema revision. | Author ADR-019 using §4.2 and §4.3 of this document as input. |
| `ADR-020` (forthcoming) | The ingestion provenance record (§4.4.1) and column mapping manifest (§4.4.2) define the logical content of the `FinalizeIngestion` outputs. Storage format and location are decisions for ADR-020. | Author ADR-020 using §4.4 of this document as input. |
| `ADR-021` (forthcoming) | The full sidecar contract (§3), inline header extraction (§3.4), `ColorRow` routing (§5.5), and associated open questions are the primary inputs to ADR-021. | Author ADR-021 using §3, §3.4, and §5.5 of this document as input. |
| `ADR-022` (proposed, forthcoming) | The `ColorRow` schema (§5.3), band registry relationship (§5.4), workflow architecture (§5.5), and open questions (§5.6, §7 Questions 6–9) are inputs. | Propose ADR-022 once ADR-017 and ADR-018 are adopted. |
| `DESIGN-003` (proposed, forthcoming) | Full donation workflow design. §6 of this document establishes the forward-compatibility constraints and provenance framework that DESIGN-003 builds on. | Propose DESIGN-003 when the donation pathway becomes an active development target. |
| `contracts/models/entities.py` | Current `PhotometryRow` Section 5 fields are the baseline for this document. The Tier 1 and Tier 2 additions will be implemented in Epic C. | No action now; Epic C is the vehicle. |
| `photometry_table_model.md` | Under revision in Epic C. The provenance framework additions here should be incorporated into v2.0. | No action now; input to Epic C. |
