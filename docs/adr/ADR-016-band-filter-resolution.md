# ADR-016: Band and Filter Resolution Strategy for the CanonicalCsvAdapter

Status: Proposed
Date: 2026-03-17

---

## Context

ADR-015 defines the three-tier column mapping strategy for the `CanonicalCsvAdapter` and
identifies `filter_name` and `phot_system` as required non-nullable fields in
`PhotometryRow`. However, real-world photometry source files — particularly AAVSO exports
and literature tables — do not reliably provide these as separate, cleanly-valued columns.
Several distinct problems arise:

1. **Combined values.** A source file may encode both the photometric system and filter
   name in a single column value, e.g. `"Johnson V"`, `"Sloan g'"`, `"2MASS Ks"`. The
   column correctly resolves to `filter_name` via column name resolution, but the value
   must be split before `PhotometryRow` can be constructed.

2. **Ambiguous short names.** A source column containing `"V"` is almost certainly
   Johnson-Cousins in an optical survey, but `"V"` also designates a Swift/UVOT filter.
   `"K"` is 2MASS K-band (~2.2 μm) in an NIR survey but K-band (~22 GHz) in a radio
   context. The correct interpretation cannot be determined from the filter string alone.

3. **Physically meaningless or excluded filter types.** AAVSO archives include observation
   modes that are not standard photometric bands: visual estimates (`"Vis."`), tri-color
   channel exports (`"TG"`, `"TR"`, `"TB"`), unfiltered observations (`"CV"`, `"CR"`),
   and similar. These are not usable as calibrated photometry in the NovaCat context and
   must be rejected.

4. **Unrecognized filter strings.** A source file may contain a filter string not covered
   by any of the above cases — a novel instrument code, a typo, or a format not yet
   encountered.

None of these problems are addressed by the column name synonym registry defined in
ADR-015, which governs only column *name* translation. This ADR defines the value-level
band resolution strategy that runs after column name resolution and before Pydantic
validation.

---

## Decision 1 — Band Resolution as a Distinct Pipeline Stage

### Decision

Band and filter value resolution is implemented as a discrete stage in the per-row
processing pipeline, executed **after** column name resolution and **before** Pydantic
validation. It is encapsulated in a `_resolve_band()` helper method on the
`CanonicalCsvAdapter` class.

`_resolve_band()` accepts the full resolved kwargs dict (all columns, post-synonym
resolution) and returns either a `(filter_name, phot_system)` pair or a failure reason
string. On failure, the row is added to `AdaptationResult.failures` and excluded from
`valid_rows`. This is a row-level failure; file-level quarantine consequences are governed
by the failure-rate threshold policy established in ADR-015, Decision 2.

### Complete per-row processing pipeline

```
raw row dict
  → sentinel normalisation ("", "N/A", "nan", "--" etc. → None)
  → column name resolution (Tier 1 canonical check, then Tier 2 synonym lookup)
  → _resolve_band(kwargs) → writes filter_name + phot_system into kwargs, or row failure
  → filter kwargs to CANONICAL_FIELDS only (drop unresolved columns)
  → suppress source identity columns; stamp injected nova_id, primary_name, ra_deg, dec_deg
  → PhotometryRow(**kwargs)   ← Pydantic validation: final gate
      success   → appended to valid_rows
      ValidationError → appended to failures
```

### Rationale

Separating band resolution from column name resolution keeps each stage's responsibility
narrow and testable. `_resolve_band()` has no awareness of the CSV structure; it operates
only on the resolved kwargs dict. Pydantic remains the sole type-enforcement and
cross-field invariant gate; `_resolve_band()` does not duplicate any Pydantic logic.

---

## Decision 2 — Case-Sensitive Filter Value Matching

### Decision

Filter value matching within `_resolve_band()` is **case-sensitive**. This is an
intentional and explicit divergence from the case-insensitive matching used for column
*names* in the synonym registry.

### Rationale

In photometric filter nomenclature, letter case carries semantic meaning that
case-folding would destroy:

| String | Correct system |
|---|---|
| `V`, `B`, `U`, `R`, `I` | Johnson-Cousins (uppercase = Bessel/Johnson convention) |
| `u`, `g`, `r`, `i`, `z` | Sloan / SDSS (lowercase = SDSS convention) |
| `Ks` | 2MASS (distinguished from `K` by the trailing `s`) |

Silently case-folding `"i"` to `"I"` or vice versa would produce incorrect photometric
system assignments. Source files must be ingested with their original filter string
casing preserved.

---

## Decision 3 — Band Resolution Lookup and Conservative Defaults

### Decision

`_resolve_band()` applies a four-step resolution procedure in order:

**Step 1 — Both fields already present.**
If both `filter_name` and `phot_system` are present in the resolved kwargs (i.e., the
source file provided them as separate columns), `_resolve_band()` passes them through
unchanged. No lookup is performed.

**Step 2 — Combined value splitting.**
If `filter_name` is present but `phot_system` is absent, the filter string is checked
against a module-level lookup dict of known combined-value forms. Matching is exact and
case-sensitive (per Decision 2). Recognized combined forms include (non-exhaustively):

| Source string | Resolved filter_name | Resolved phot_system |
|---|---|---|
| `"Johnson V"` | `"V"` | `Johnson-Cousins` |
| `"Johnson B"`, `"Johnson U"`, `"Johnson R"`, `"Johnson I"` | bare letter | `Johnson-Cousins` |
| `"Sloan g'"`, `"Sloan r'"`, `"Sloan i'"`, `"Sloan z'"` | bare `g'` etc. | `Sloan` |
| `"SDSS g"`, `"SDSS r"`, `"SDSS i"`, `"SDSS z"` | bare letter | `Sloan` |
| `"2MASS J"`, `"2MASS H"`, `"2MASS K"`, `"2MASS Ks"` | bare letter/`Ks` | `2MASS` |
| `"Swift/UVOT UVW2"`, `"UVOT UVW2"`, etc. | `"UVW2"` etc. | `Swift-UVOT` |

**Step 3 — Unambiguous short names with conservative defaults.**
If the filter string is not a combined form, it is checked against a lookup dict of
unambiguous short names. A short name is "unambiguous" if there is only one reasonable
photometric system interpretation for it across all credible source file contexts. The
conservative default applies when the short name is the dominant convention:

| Filter string | Default phot_system | Rationale |
|---|---|---|
| `"V"`, `"B"`, `"U"`, `"R"`, `"I"` | `Johnson-Cousins` | Dominant convention; AAVSO primary filter set |
| `"u"`, `"g"`, `"r"`, `"i"`, `"z"` | `Sloan` | Lowercase = SDSS convention (case-sensitive per Decision 2) |
| `"J"`, `"H"` | `2MASS` | No significant ambiguity for these NIR bands |
| `"Ks"` | `2MASS` | Specific 2MASS designation |
| `"UVW2"`, `"UVM2"`, `"UVW1"` | `Swift-UVOT` | Unique to UVOT instrument; no ambiguity with other systems |
| `"Ku"`, `"Ka"` | `Radio` | Unambiguous radio bands; no optical/NIR equivalent |

These defaults are conservative in the sense that they reflect the most probable correct
interpretation for an operator-prepared file ingested through the MVP canonical CSV
pathway. They are not guaranteed to be correct for all possible source files; operators
providing data from non-dominant systems should include an explicit `phot_system` column.

**Step 4 — Genuinely ambiguous or unrecognized.**
If the filter string matches neither a combined form nor an unambiguous short name, the
row fails. See Decision 4 (context-aware disambiguation) and Decision 5 (excluded and
unrecognized filters) for how specific cases are handled at this step.

---

## Decision 4 — Context-Aware Disambiguation for Ambiguous Filters

### Decision

Certain filter strings are genuinely ambiguous between two physically distinct regimes and
cannot be resolved by a conservative default. For these cases, `_resolve_band()` consults
context fields already present in the resolved kwargs dict before concluding that a row
has failed.

The canonical ambiguous case is `"K"`:

- 2MASS K-band: near-infrared, ~2.2 μm
- Radio K-band: ~22 GHz

Context resolution rules for `"K"` (evaluated in order, first match wins):

| Context signal | Resolution |
|---|---|
| `phot_system` present and equals `"2MASS"` | 2MASS K |
| `phot_system` present and equals `"Radio"` | Radio K |
| `telescope` in known radio telescope set (`"VLA"`, `"ATCA"`, `"MeerKAT"`, `"JVLA"`, `"WSRT"`, `"AMI"`, ...) | Radio K |
| `telescope` in known NIR telescope set (`"CTIO"`, `"2MASS"`, `"UKIRT"`, `"VISTA"`, ...) | 2MASS K |
| `spectral_coord_type == "frequency"` | Radio K |
| `spectral_coord_type == "wavelength"` | 2MASS K |
| `spectral_coord_unit` in `{"GHz", "MHz"}` | Radio K |
| `spectral_coord_unit` in `{"Angstrom", "nm"}` | 2MASS K |
| No context available | Row failure: `"ambiguous filter 'K': cannot distinguish 2MASS K (~2.2 μm) from radio K-band (~22 GHz) — provide phot_system, spectral_coord_type, telescope, or spectral_coord_unit"` |

Additional ambiguous cases may be identified as real source files are ingested and should
be handled by extending this mechanism rather than applying a conservative default.

### Order of operations

Context fields (`telescope`, `spectral_coord_type`, `spectral_coord_unit`) are read from
the *already-resolved kwargs dict* — column name resolution has already run across all
source columns before `_resolve_band()` is called. There is no ordering dependency
problem; `_resolve_band()` is a pure consumer of the resolved dict.

---

## Decision 5 — Excluded and Unrecognized Filter Codes

### Decision

Filters that are not valid calibrated photometric bands are handled in two distinct
categories, each producing a distinct row-level failure message:

**Category 1 — Excluded (known, deliberately rejected).**
Certain filter strings are recognized but explicitly excluded because they represent
non-standard observation modes that are not usable as calibrated photometry:

- Visual estimates: `"Vis."`, `"Visual"`
- Tri-color DSLR channels: `"TG"`, `"TR"`, `"TB"`
- Unfiltered: `"CV"`, `"CR"`, `"CBB"`, `"Clear"`
- Other: `"STD"` (standard star observation marker)

Failure message: `"excluded filter type: '{value}' ({reason}) — row dropped"`, where
`{reason}` is a brief human-readable note (e.g. `"visual estimate"`, `"unfiltered DSLR"`).

**Category 2 — Unrecognized (not in any lookup).**
Filter strings that pass through all resolution steps without a match are unrecognized.

Failure message: `"unrecognized filter: '{value}' — not in combined-value lookup, short-name defaults, or excluded set; add to synonym registry or excluded_filters.json if encountered repeatedly"`

The distinction matters operationally: an excluded filter failure is expected and
informational; an unrecognized filter failure may signal a synonym gap that should be
addressed.

### `excluded_filters.json` as a living document

The excluded filter set is maintained in a versioned sibling file,
`services/photometry_ingestor/adapters/excluded_filters.json`, with the same versioning
conventions as `synonyms.json`. Each entry carries a `reason` field for diagnostic
clarity:

```json
{
  "_comment": "Filter codes excluded from NovaCat photometry ingestion. These represent non-standard observation modes that are not usable as calibrated photometry. See ADR-016, Decision 5.",
  "_registry_version": "1.0",
  "excluded": {
    "Vis.":    "visual estimate",
    "Visual":  "visual estimate",
    "TG":      "unfiltered DSLR tri-color green channel",
    "TR":      "unfiltered DSLR tri-color red channel",
    "TB":      "unfiltered DSLR tri-color blue channel",
    "CV":      "unfiltered (V-band corrected)",
    "CR":      "unfiltered (R-band corrected)",
    "CBB":     "unfiltered broadband",
    "Clear":   "unfiltered",
    "STD":     "standard star observation marker"
  }
}
```

**Maintenance obligation:** When adding entries to `excluded_filters.json`, the operator
should cross-check the filter string against the SVO Filter Profile Service
(https://svo2.cab.inta-csic.es/theory/fps/) to confirm the string does not correspond to
a legitimate registered filter. This is a manual obligation for MVP; it is not an
automated gate.

---

## Decision 6 — Scope Boundary: `CanonicalCsvAdapter` is Not a Filter Database

### Decision

The `CanonicalCsvAdapter` does not infer or supply values for `spectral_coord_value`,
`spectral_coord_type`, or `spectral_coord_unit` from filter identity alone. These fields
are required non-nullable fields in `PhotometryRow` and must be present in the source
file. If they are absent, the row fails Pydantic validation in the normal way.

### Rationale

Filling in `spectral_coord_value` from a filter name (e.g. Johnson V → 5500 Å) would
require embedding a filter property database in the adapter. This is a meaningful scope
expansion — filter central wavelengths vary between photometric system implementations,
and "correct" values require reference to the SVO Filter Profile Service or IVOA PhotDM.
A `FilterLibrary` component that fetches and caches SVO data is the appropriate future
home for this logic. It can be composed with the adapter when implemented.

For MVP, the canonical CSV contract is that the operator provides complete data.
`spectral_coord_*` columns should be included in every source file. The `synonyms.json`
registry already covers common column name variants for these fields (e.g. `WAVELENGTH`,
`WAVE`, `LAMBDA` → `spectral_coord_value`).

---

## Decision 7 — Row-Level Failure Persistence and Operator Review

### Decision

Row-level failures accumulated in `AdaptationResult.failures` are **always written to S3**
as a structured JSON file, regardless of whether the file as a whole proceeds to
persistence or is quarantined. The failure record is written by the ValidatePhotometry
handler immediately after `adapt()` returns, before the threshold decision is applied.

**S3 key convention:**
```
diagnostics/photometry/<nova_id>/row_failures/<file_sha256>.json
```

**Payload shape:**
```json
{
  "nova_id": "<uuid>",
  "file_sha256": "<hex>",
  "total_row_count": 500,
  "failure_count": 12,
  "failure_rate": 0.024,
  "failures": [
    {
      "row_index": 3,
      "raw_row": { "Band": "Vis.", "Magnitude": "12.4", ... },
      "error": "excluded filter type: 'Vis.' (visual estimate) — row dropped"
    },
    ...
  ]
}
```

### Rationale

Row-level failures that fall below the quarantine threshold are not silently discarded.
They are operationally valuable:

- **Resolution logic improvement.** Unrecognized filter strings and ambiguous-filter
  failures are signals that the band resolution lookup tables have gaps. Persisting them
  makes those gaps visible and auditable over time, without requiring the operator to
  re-ingest files.
- **Source file quality tracking.** A file that consistently produces a 5% failure rate
  from excluded AAVSO visual estimates is behaving correctly; a file that produces a 15%
  failure rate from unrecognized filters is a signal to investigate. The operator cannot
  distinguish these cases without access to the failures.
- **Consistency with quarantine philosophy.** The system already persists quarantined files
  with full diagnostic metadata. Row failures deserve the same treatment: they are the
  row-level analogue of a quarantined file.

Writing failures unconditionally — not only when the file is quarantined — ensures that
even a largely clean file's edge-case failures are retained. This is the mechanism by
which the band resolution logic improves over successive real-data ingestion runs.

### Threshold-decision semantics (for completeness)

- A **row-level failure** adds an `AdaptationFailure` to `AdaptationResult.failures` and
  excludes the row from `valid_rows`.
- The `AdaptationResult.failure_rate` property reflects all accumulated row failures.
- The **caller** (ValidatePhotometry handler) applies a configurable threshold **after**
  writing the failure record:
  - Below threshold: clean subset proceeds; failure record is written and logged.
  - Above threshold: file is quarantined with `COERCION_FAILURE_THRESHOLD_EXCEEDED`;
    failure record is written and included in the quarantine diagnostic payload.
- A file dominated by excluded-filter rows (e.g. a mostly-visual AAVSO export) will
  likely breach threshold and be quarantined. This is the correct outcome — the operator
  should pre-filter the source file before ingestion.
- `MISSING_REQUIRED_COLUMNS` is reserved for **file-level** structural failures, raised
  as a `MissingRequiredColumnsError` before any rows are processed (see ADR-015,
  Decision 2). Band resolution failures are always row-level.

---

## Open Questions

1. **Radio K-band telescope registry.** The known radio telescope set used for `"K"`
   disambiguation is currently a module-level constant. As more source files are ingested,
   this list will need expansion. The mechanism for keeping it current is not specified;
   it may eventually warrant the same living-document treatment as `excluded_filters.json`.

2. **Additional genuinely ambiguous filters.** `"K"` is the only identified case at time
   of writing. Others may emerge as real source files are ingested. The disambiguation
   mechanism in Decision 4 is designed to be extended per-filter without structural
   changes.

3. **`FilterLibrary` for `spectral_coord_*` auto-fill.** Decision 6 defers this to a
   post-MVP component. Timing and design are not specified here.

---

## Consequences

- `_resolve_band()` is the single responsible location for all filter value normalization.
  Column name resolution (synonym registry) and filter value resolution (this ADR) are
  cleanly separated concerns.
- The case-sensitive matching policy for filter values is explicitly documented and
  distinguishable from the case-insensitive policy for column names.
- `excluded_filters.json` provides an auditable, operator-maintainable record of
  deliberately rejected observation modes, with cross-SVO checking as a maintenance
  obligation.
- Operators preparing source files for canonical CSV ingestion have a clear contract:
  provide `spectral_coord_type`, `spectral_coord_value`, and `spectral_coord_unit`
  explicitly; provide `phot_system` explicitly for any filter that is ambiguous or
  non-standard.
- The conservative defaults documented in Decision 3 are the system's stated best-effort
  interpretation for common short filter names. They are not assertions of correctness
  for all possible source files.
- Row-level filter failures contribute to the failure-rate threshold. Files dominated by
  excluded or unrecognized filter types will be quarantined, prompting the operator to
  pre-filter source data before ingestion.
- Row failures are always persisted to S3 at a predictable key, regardless of whether the
  file proceeds or is quarantined. This creates a durable, reviewable record that drives
  iterative improvement of the band resolution lookup tables over time.
