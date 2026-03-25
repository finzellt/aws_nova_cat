# ADR-018: Band Disambiguation Algorithm

**Status:** Draft
**Date:** 2026-03-24
**Author:** TF
**Supersedes:** —
**Superseded by:** —
**Amends:** —
**Relates to:**
- `DESIGN-001` §5 Layer 4 — authoritative design basis; disambiguation is a Layer 4
  (`CanonicalCsvAdapter`) concern
- `DESIGN-002` §4.3 — resolution provenance field vocabulary adopted here
- `ADR-017` — Band Registry Design; alias index interface and `disambiguation_hints`
  reservation consumed here
- `ADR-019` — Photometry Table Model Revision; `band_resolution_type`, and
  `band_resolution_confidence` fields defined here feed
  ADR-019's `PhotometryRow` schema revision
- `ADR-021` — Layer 0 Pre-Ingestion Normalization; `IngestionContext` and
  `ConflictRecord` / `ConflictClass` machinery consumed here; see §6 for ADR-021
  amendment requirements

---

## 1. Context

ADR-017 establishes the band registry as the single authoritative source of truth for
photometric band identity in NovaCat. It defers to this ADR the question of how an
ambiguous band alias — one that maps to more than one registry entry — is resolved to
a single canonical `band_id` at ingestion time.

The problem is real and unavoidable. Filter string `"V"` almost always means
Johnson-Cousins V in an optical survey, but it is also a valid Swift/UVOT designation.
Filter string `"K"` is 2MASS K-band (~2.2 μm) in a near-infrared context but K-band
(~22 GHz) in a radio context. Filter string `"i"` is unambiguous; `"I"` is not (case
sensitivity is preserved per ADR-017 Decision 4, but an author's typo is not impossible).
As the registry grows to cover additional regimes and instruments, the surface area for
ambiguity grows with it.

ADR-016, which this document supersedes in spirit (ADR-016 itself is superseded by
ADR-017), addressed disambiguation as an adapter-internal concern using hardcoded Python
dicts and a table of context resolution rules for the `"K"` case specifically. DESIGN-001
§2.1 identifies the structural failure of that approach. This ADR replaces it with a
fully specified, testable algorithm expressed as a decision procedure, not prose.

**Scope.** This ADR covers:

- The input contract for the disambiguation algorithm
- Candidate set construction from the alias index
- The three-stage resolution funnel (instrument context narrowing → post-MVP band name
  matching → Generic fallback)
- The conflict detection and recording policy
- The AAVSO provenance exception
- Resolution provenance field population for all exit paths

This ADR does **not** cover:

- The `PhotometryRow` schema changes required to store provenance fields — ADR-019
- The `ColorRow` band component resolution — ADR-022 (the same algorithm applies, but
  ColorRow's parsing of color strings into component band strings is ADR-022's concern)
- The band registry schema and alias index interface — ADR-017
- Layer 0 normalization — ADR-021
- Any implementation work — Epic D (Adapter Revision)

---

## 2. Alternatives Considered

### Single-pass greedy resolution

Evaluate context signals in priority order and stop at the first signal that reduces the
candidate set to one. Simple to implement; fails to detect intra-file conflicts between
signals that would have been caught by a full evaluation pass.

**Rejected.** Conflict detection is a first-class requirement (see §3, Decision 3).
Greedy resolution would silently resolve ambiguous files that should be quarantined.

### Probabilistic scoring

Assign confidence weights to each context signal and pick the highest-scoring candidate.
More nuanced handling of partial or weak signals.

**Rejected for MVP.** Requires empirical calibration of weights from ingested data that
does not yet exist. Introduces non-determinism that complicates auditability. Can be
revisited post-MVP once the catalog has sufficient volume to calibrate weights.

### Operator-review queue instead of quarantine

Route unresolvable rows to an operator review queue rather than quarantining them.

**Rejected for MVP.** Adds operational infrastructure complexity. Quarantine with
structured diagnostic output achieves the same result — operator visibility — without
requiring a separate workflow. The quarantine record contains the full candidate set and
conflict details, which is sufficient for operator action.

---

## 3. Decisions

### Decision 1 — Input Contract and Preconditions

The disambiguation algorithm is called by `CanonicalCsvAdapter` (Layer 4) as part of
the per-row processing pipeline, after column name resolution and before Pydantic
validation. It receives:

- **`band_string`** — the raw filter string from the source row (e.g. `"V"`, `"K"`,
  `"g"`), post-sentinel normalisation
- **`measurement_class`** — whether the measurement column was classified as
  `magnitude_like` or `flux_like` by the synonym registry during column resolution.
  This classification is guaranteed present by the Layer 0 precondition (see below)
- **`row_context`** — the resolved kwargs dict (post-synonym resolution), carrying
  optional fields: `instrument`, `telescope`, `spectral_coord_type`,
  `spectral_coord_unit`.
- **`file_context`** — the `IngestionContext` object assembled by Layer 0, carrying
  optional file-level and sidecar-contributed fields including `orig_catalog`,
  `observatory`, `instrument`, `telescope`

The algorithm is a **pure function**: it performs no I/O. The band registry alias
index is injected at `CanonicalCsvAdapter` initialisation time, not per-call.

#### Layer 0 Precondition Guarantee

By the time the algorithm is called, Layer 0 (`prep_photometry_file`) has already
verified that the following fields are present in the normalised row:

- `epoch_mjd`
- `band_string` (non-empty)
- A classified measurement column (`magnitude_like` or `flux_like`)

Rows or files failing this check are rejected upstream with an appropriate
`NormalizationError`. The algorithm never sees them.

**Rationale:** It makes no sense to attempt band resolution for a row that will be
rejected for missing required fields. Failing fast at Layer 0 eliminates an entire
class of pointless disambiguation work and produces cleaner quarantine diagnostics.

#### Known Deferred Gaps

- **Radio vs. X-ray flux disambiguation** — `measurement_class = flux_like` covers
  the optical/NIR vs. radio/X-ray split cleanly, but does not distinguish radio from
  X-ray (both report flux). This is a known gap; left as a future extension when
  radio and X-ray regimes are fully supported
- **X-ray band column requirement** — the current requirement for ≥1 detectable band
  column in Layer 0 is incompatible with X-ray photometry tables, which typically
  report counts/second without a spectral band column. This structural check must be
  revisited before X-ray ingestion is supported

---

### Decision 2 — Candidate Set Construction

The alias index (ADR-017 Decision 8) is a case-sensitive dict keyed by alias string,
mapping each alias to a list of `band_id` values. Alias matching is case-sensitive
throughout (ADR-017 Decision 4).

The candidate set is constructed by a single O(1) lookup of `band_string` in the alias
index. Four outcomes are possible:

#### Outcome A — Zero matches

`band_string` appears in no registry entry's alias list. The string is unrecognised.

**Exit → QUARANTINE** with reason code `UNRECOGNIZED_BAND_STRING`. No disambiguation
is attempted. The raw `band_string` is recorded in the quarantine diagnostics to enable
iterative registry improvement.

#### Outcome B — All matches are excluded

All entries in the candidate set have `excluded: true`. The string is recognised and
deliberately rejected.

**Exit → EXCLUDED.** The `exclusion_reason` from the registry entry is recorded in the
row failure record. This is a distinct terminal from QUARANTINE: the string is known
and handled correctly; no operator action is required.

This outcome applies whether there is one excluded candidate or multiple (all excluded).

#### Outcome C — One or more non-excluded candidates

Proceed to Decision 3 (Stage 1). Excluded candidates are dropped from the set before
Stage 1 runs.

Note: a **single** non-excluded candidate does not produce an immediate high-confidence
resolution. The registry is not exhaustive — a single alias match may be correct or may
be a false positive against an incomplete registry. Stage 1 corroboration is always
performed.

#### Maintenance Note

Any newly added excluded filter must be cross-checked against the SVO Filter Profile
Service to verify it does not collide with a legitimate band alias in another
photometric system. Excluded filter names are generally distinctive, but SVO should be
consulted before committing any new excluded entry.

---

### Decision 3 — Stage 1: Instrument Context Narrowing

Stage 1 is both a **disambiguation stage** and a **file consistency validator**. All
available context signals are evaluated before any resolution decision is made.

#### Signal Priority Order

Signals are evaluated in the following priority order. Higher-priority signals are more
decisive; lower-priority signals are coarser but more commonly available.

| Priority | Signal | Source | Rationale |
|---|---|---|---|
| 1 | `instrument` | row or file context | Instrument names are globally unique; instruments do not share filter names with rare exception (see note below). Most decisive signal available |
| 2 | `telescope` / `observatory_facility` | row or file context | Strong narrowing power — reduces candidate set from thousands to at most a few hundred — but not unambiguously decisive. Subject to alias/nickname problem; mitigated by synonyms list |
| 3 | `measurement_class` | guaranteed present | Separates optical/NIR (magnitude-like) from radio/X-ray (flux-like). Coarse but always available |
| 4 | `spectral_coord_unit` / `spectral_coord_type` | row context | Similar coarseness to `measurement_class`; corroborating rather than primary |

**`phot_system` is not a disambiguation signal.** It has been dropped from the signal
list because the information it was originally intended to convey is more precisely
captured by `instrument`, `measurement_class`, and spectral coordinate fields. Its
presence in the resolved kwargs dict is irrelevant to the disambiguation algorithm.

**Note on instrument decisiveness.** In rare cases, a single physical instrument has
multiple calibrated filter profiles for physically indistinguishable chip variants (e.g.
HST WFC3/UVIS has separate SVO profiles for UVIS1 and UVIS2 chips). Per ADR-017's
registry seeding policy, filters that are physically indistinguishable within a
specified spectral threshold (identical `lambda_eff`, `lambda_pivot`, and `W_eff` to
within the threshold) are collapsed to a single registry entry at seed time. Instrument
match is therefore effectively decisive at runtime, contingent on correct registry
seeding.

#### Algorithm

1. **Collect** all available signals from `row_context` and `file_context`
2. **Evaluate all signals** against the non-excluded candidate set independently,
   noting which candidates each signal supports
3. **Check for intra-file conflicts.** If two signals that both originate from the
   ingested file (not the sidecar) point to incompatible subsets of the candidate
   set:
   - Emit a `ConflictRecord` with the appropriate `ConflictClass` (see §5)
   - **Exit → QUARANTINE** with reason code `CONFLICTING_BAND_CONTEXT`
   - Record the specific conflicting signals and their candidate subsets in the
     quarantine diagnostics
4. **Apply sidecar vs. file conflict policy** (if applicable). If a sidecar signal
   and a file signal conflict, the sidecar value wins (per ADR-021 §7.7 trust-but-
   verify principle). Emit a `ConflictRecord` and continue with the sidecar-preferred
   candidate subset
5. **Take the intersection** of all non-conflicting signal results to narrow the
   candidate set
6. **Apply excluded filter check.** If all remaining candidates are excluded after
   narrowing: **Exit → EXCLUDED**. Drop any excluded candidates before proceeding
7. **Evaluate narrowed set size:**
   - Narrowed set = 1 → proceed toward resolution; confidence determined by which
     signals fired (see Decision 6)
   - Narrowed set > 1 → proceed to Stage 2 (Decision 4)
   - Narrowed set = 0 → all signals collectively ruled out every candidate.
     **Exit → QUARANTINE** with reason code `BAND_CONTEXT_EXCLUDES_ALL_CANDIDATES`.
     This strongly indicates a missing registry entry; record the original full
     candidate set and all signal values in the quarantine diagnostics

#### Conflict Policy

| Conflict type | Policy |
|---|---|
| Intra-file signals conflict | QUARANTINE with `CONFLICTING_BAND_CONTEXT`; same-origin signals have equal epistemic weight, neither can be trusted over the other |
| Sidecar signal vs. file signal conflict | Trust sidecar; emit `ConflictRecord`; continue (inherited from ADR-021 §7.7) |

#### Conflict System Promotion Note

The `ConflictRecord` / `ConflictClass` machinery is currently defined in ADR-021 as a
Layer 0 concern. Band disambiguation in Layer 4 requires the same infrastructure.
ADR-021 §7.7 explicitly anticipates this: "Layer 4 should apply the same trust-but-
verify principle when it detects such conflicts and emit `ConflictRecord` entries into
a downstream equivalent."

**This ADR formalises that forward reference.** The conflict recording system is a
pipeline-wide concern, not a Layer 0-specific one. DESIGN-001 and DESIGN-002 should be
annotated accordingly. New `ConflictClass` values introduced by this ADR (see §5) must
be added to `entities.py`.

---

### Decision 4 — Stage 2: Post-MVP Band Name Matching (Reserved)

Stage 2 is a **reserved extension point**. At MVP scale (25 registry entries), if Stage
1 has not resolved the candidate set to a single entry, no additional signals are
available that Stage 1 has not already evaluated. No Stage 2 logic is implemented at
MVP.

The `disambiguation_hints` field (reserved as `{}` on all registry entries per ADR-017
Decision 3) is the designated mechanism for Stage 2 extensions. It is explicitly
designated a post-MVP extension point. No application code reads `disambiguation_hints`
until a concrete use case is identified and this ADR is revised.

**Proceed directly to Stage 3.**

---

### Decision 5 — Stage 3: Generic Fallback

We arrive here with a candidate set still > 1 after Stages 1 and 2.

#### Standard Fallback

1. Search the candidate set for an entry whose `band_id` matches `Generic_{band_string}`
   (e.g. `"Generic_V"`, `"Generic_K"`)
2. If found:
   - **Exit → RESOLVED** with the Generic entry's `band_id`
   - `band_resolution_type: "generic_fallback"`
   - `band_resolution_confidence: "low"`
   - The `Generic_*` prefix in `band_id` is self-documenting — any downstream consumer
     can identify a Generic fallback without additional metadata
3. If not found:
   - **Exit → QUARANTINE** with reason code `AMBIGUOUS_BAND_UNRESOLVABLE`
   - Record the full remaining candidate set in the quarantine diagnostics; this is
     actionable information for the operator to determine which registry entry is needed

#### AAVSO Provenance Exception

AAVSO photometry archives present a known, documented limitation: filter ambiguity is
endemic to the dataset, and sufficient context to resolve it is rarely present in the
file. Quarantining AAVSO data on `AMBIGUOUS_BAND_UNRESOLVABLE` would discard a large
body of scientifically useful (if imprecise) observations.

**Policy:** If a definitive AAVSO provenance signal is present in `IngestionContext`
(see ADR-021 amendment note below), the algorithm skips quarantine on
`AMBIGUOUS_BAND_UNRESOLVABLE` and resolves unconditionally to the Generic fallback:
- `band_resolution_type: "generic_fallback"`
- `band_resolution_confidence: "low"`

AAVSO-resolved rows are indistinguishable from standard Generic fallback rows by
provenance type and confidence level alone. Downstream data bundles and user-facing
documentation should note that AAVSO-sourced data carries inherently low band resolution
confidence. This is a known trade-off, not a pipeline deficiency.

**Post-MVP:** AAVSO is the first designated use case for `disambiguation_hints`. As
patterns in AAVSO observer filter usage accumulate, hints may eventually allow certain
AAVSO-sourced Generic resolutions to be elevated to `"medium"` confidence.

#### ADR-021 Amendment Required

Layer 0 (`prep_photometry_file`) must detect and propagate a definitive AAVSO
provenance signal in `IngestionContext`. This may derive from `orig_catalog`,
observatory identity, or other file-level signals that Layer 0 is already processing.
ADR-021 is not currently aware of this responsibility. This gap must be closed when
ADR-021 is next revised.

#### Note on Generic Entries for Ambiguous Bands

Generic entries should exist in the registry even for bands where ambiguity is
scientifically significant (e.g. `Generic_K`, where NIR vs. radio is a meaningful
distinction). Stage 1 signals — particularly `measurement_class` and
`spectral_coord_unit` — are the primary defense against dangerous Generic fallbacks.
The absence of a Generic entry is not a substitute for proper context signal coverage.

---

### Decision 6 — Resolution Provenance

Every resolved row carries three provenance fields defined in DESIGN-002 §4.3. This
decision specifies how the algorithm populates them for each exit path.

**Field vocabulary:**

- `band_resolution_type` — the mechanism by which resolution was achieved
- `band_resolution_confidence` — trustworthiness of the result

**Note:** DESIGN-002 §4.3 uses `band_resolution_tier` with a different vocabulary. This
ADR supersedes that vocabulary. `band_resolution_tier` is renamed to
`band_resolution_type` with the following allowed values: `canonical`, `synonym`,
`generic_fallback`, `sidecar_assertion`. DESIGN-002 §4.3 should be updated accordingly.

#### Provenance Table

| Exit path | `band_resolution_type` | `band_resolution_confidence` |
|---|---|---|
| Unambiguous alias + instrument or telescope corroboration | `canonical` | `high` |
| Unambiguous alias + coarse signals only (measurement_class, spectral_coord) | `canonical` | `medium` |
| Unambiguous alias + no corroborating context | `canonical` | `medium` |
| Stage 1 narrowed to 1 via instrument or telescope | `synonym` | `high` |
| Stage 1 narrowed to 1 via coarse signals only | `synonym` | `medium` |
| Generic fallback (standard) | `generic_fallback` | `low` |
| Generic fallback (AAVSO exception) | `generic_fallback` | `low` |
| Sidecar assertion, independently validated | `sidecar_assertion` | `medium` |
| Sidecar assertion, not independently validated | `sidecar_assertion` | `low` |

Non-resolution exits (EXCLUDED, all QUARANTINE variants) do not populate provenance
fields. No `PhotometryRow` is persisted for these exits.

#### Confidence Rationale

- **`high`**: the resolution is corroborated by a strong, specific context signal
  (instrument or telescope) that independently confirms the candidate identity
- **`medium`**: the resolution is unambiguous or corroborated, but only by coarse
  signals, or a sidecar assertion has been independently validated against the file
- **`low`**: the resolution relies on a fallback or unvalidated assertion; data
  should not be used for precision photometric work without independent verification

The `confidence: "low"` assignment for Generic fallback is intentional and permanent —
these rows should be visibly untrustworthy for precision work.

#### Sidecar Confidence Condition

`sidecar_assertion` / `medium` requires that the sidecar's band assertion has been
independently validated against the file (e.g. the declared column resolves correctly,
the declared unit parses as the declared type). If validation is not implemented,
`sidecar_assertion` defaults to `"low"`. This is a known gap to be addressed in the
`prep_photometry_file` handler specification.

---

### Decision 7 — File-Level Context

File-level context signals (`orig_catalog`, `observatory`, and file-level `instrument`
and `telescope` values from `IngestionContext`) are first-class inputs to Stage 1.
They are not handled separately; this decision establishes their relationship to
row-level signals.

**File-level signals are lower priority than row-level signals.** A row-level
`instrument` value is more specific than a file-level `orig_catalog` inference, because
it pertains to the specific measurement rather than the file as a whole.
When both are present and agree, they corroborate each other. When they conflict, the
row-level value takes precedence — applying the same intra-file conflict policy as
Decision 3.

**File-level signals participate in conflict detection.** A file-level signal that
conflicts with a row-level signal from the same ingested file (not the sidecar) triggers
the intra-file conflict policy: QUARANTINE with `CONFLICTING_BAND_CONTEXT`.

**The AAVSO exception is the canonical example** of a file-level signal producing a
named policy outcome. `orig_catalog` (or equivalent `IngestionContext` signal) is the
mechanism by which AAVSO provenance is detected. Future catalog-specific exceptions
follow this template.

---

## 4. Algorithm Summary

The complete algorithm as a decision procedure:

```
INPUT: band_string, measurement_class, row_context, file_context, alias_index

1. LOOKUP alias_index[band_string]
   → zero matches          → EXIT QUARANTINE(UNRECOGNIZED_BAND_STRING)
   → all matches excluded  → EXIT EXCLUDED
   → else                  → candidate_set = non-excluded matches

2. STAGE 1: Collect all available signals from row_context + file_context
   Evaluate each signal against candidate_set independently
   Check for intra-file conflicts across all signal results
   → intra-file conflict detected  → EXIT QUARANTINE(CONFLICTING_BAND_CONTEXT)
   → sidecar vs file conflict      → trust sidecar, emit ConflictRecord, continue
   Take intersection of non-conflicting signal results → narrowed_set
   Drop excluded candidates from narrowed_set
   → all narrowed candidates excluded → EXIT EXCLUDED
   → narrowed_set = 0  → EXIT QUARANTINE(BAND_CONTEXT_EXCLUDES_ALL_CANDIDATES)
   → narrowed_set = 1  → GOTO RESOLVE(narrowed_set[0], signals_that_fired)
   → narrowed_set > 1  → GOTO STAGE 2

3. STAGE 2: (Post-MVP reserved — pass through)
   GOTO STAGE 3

4. STAGE 3: Generic fallback
   → Generic_{band_string} in candidate_set  → GOTO RESOLVE(Generic entry, [])
   → AAVSO provenance signal present         → GOTO RESOLVE(Generic entry, [AAVSO])
   → else                                    → EXIT QUARANTINE(AMBIGUOUS_BAND_UNRESOLVABLE)

RESOLVE(entry, signals):
   Determine band_resolution_type and band_resolution_confidence from Decision 6 table
   EXIT RESOLVED(band_id=entry.band_id, type=..., confidence=...)
```

---

## 5. New ConflictClass Values

The following `ConflictClass` enum members must be added to `entities.py` to support
the intra-file conflict detection introduced by this ADR:

| Value | Trigger |
|---|---|
| `BAND_SIGNAL_CONFLICT` | Two file-sourced context signals (e.g. `instrument` and `spectral_coord_unit`) point to incompatible band candidates |

Existing ADR-021 conflict classes (`BAND_ASSERTION_DISAGREEMENT`, etc.) cover sidecar-vs-file conflicts. `BAND_SIGNAL_CONFLICT`
covers the new intra-file case introduced by this ADR.

---

## 6. Consequences

### 6.1 Immediate

- **`CanonicalCsvAdapter._resolve_band()`** is redesigned to implement the algorithm
  in §4. The existing ADR-016-era context resolution table for `"K"` is replaced by
  the generalised Stage 1 signal evaluation
- **`entities.py` additions:** `ConflictClass.BAND_SIGNAL_CONFLICT` enum member; new
  quarantine reason codes: `UNRECOGNIZED_BAND_STRING`, `CONFLICTING_BAND_CONTEXT`,
  `BAND_CONTEXT_EXCLUDES_ALL_CANDIDATES`, `AMBIGUOUS_BAND_UNRESOLVABLE`
- **`PhotometryRow` schema:** gains `band_resolution_type`, `band_resolution_confidence`,
  fields (ADR-019 governs the exact schema; this ADR defines the
  vocabulary)
- **DESIGN-002 §4.3 amendment required:** `band_resolution_tier` renamed to
  `band_resolution_type`; tier vocabulary replaced with type vocabulary defined in
  Decision 6

### 6.2 Required Amendments to Upstream Documents

| Document | Required change |
|---|---|
| **ADR-021** | Layer 0 must detect and propagate a definitive AAVSO provenance signal in `IngestionContext` (Decision 5). Layer 0's `ConflictClass` scope should be noted as pipeline-wide (Decision 3) |
| **DESIGN-001** | §5 Layer 4 and Layer 0 should be annotated to reflect that `ConflictRecord` emission is a pipeline-wide concern, not Layer 0-specific |
| **DESIGN-002** | §4.3 `band_resolution_tier` renamed to `band_resolution_type`; vocabulary updated to match Decision 6 |
| **`photometry_table_model.md`** | `phot_system` field definition requires update; its role as a disambiguation signal has been formally removed |

### 6.3 Forward Dependencies

| Downstream artifact | Dependency on this ADR |
|---|---|
| **ADR-019** (Photometry Table Model Revision) | Consumes `band_resolution_type`, `band_resolution_confidence`, vocabulary defined in Decision 6 |
| **ADR-022** (ColorRow Design) | `ColorRow.band1_id` and `band2_id` are resolved through this algorithm after color string parsing; same algorithm, different calling context |
| **Epic D** (Adapter Revision) | Implements `CanonicalCsvAdapter._resolve_band()` against this specification |

---

## 4. Open Items

None. This ADR is complete and ready for acceptance.

---
