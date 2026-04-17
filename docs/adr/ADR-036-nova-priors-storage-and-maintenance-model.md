# ADR-036: Nova Priors Storage and Maintenance Model

**Status:** Draft
**Date:** 2026-04-17
**Author:** TF
**Supersedes:** —
**Superseded by:** —
**Amends:** —
**Relates to:**
- `docs/worklog/bugs_list_4_17_26.md` — items 3 and 7 originate this work
- `docs/worklog/master-tasks.md` — `PI3` initial ingestion enrichment
- `ADR-005-Amendment-discovery-date` — `discovery_date` `YYYY-MM-DD` format and `00`-convention for unknown day/month
- `ADR-014` — Artifact Schemas; `nova.json` and `catalog.json` peak-magnitude fields are the downstream destination for `peak_mag*` at artifact generation time
- `ADR-017` — Band Registry Design; this ADR mirrors its physical form, seed pipeline, versioning, and Python interface patterns for a structurally identical problem
- `docs/workflows/initialize-nova.md` — primary consumer workflow
- `docs/storage/dynamodb-item-model.md` — Nova item schema; amended under §7
- `contracts/models/entities.py` — `Nova` Pydantic model; amended under §7
- `services/nova_resolver/handler.py` — `UpsertMinimalNovaMetadata` task (future consumer)
- `tools/novacat-tools/tools/set_nova_dates.py` — existing operator tool for post-hoc date edits

---

## 1. Context

NovaCat's `initialize_nova` workflow creates a Nova DDB item from a candidate name by
(1) normalizing the name, (2) checking whether it already exists, (3) resolving it
against public archives (SIMBAD + TNS via `archive_resolver`), (4) creating a new
`nova_id`, and (5) persisting coordinates and aliases via `UpsertMinimalNovaMetadata`.
This workflow never populates `discovery_date` or peak magnitude on the new Nova item.

Both fields are populated — or not — by other mechanisms:

- `discovery_date` is populated by `refresh_references` via the `ComputeDiscoveryDate`
  and `UpsertDiscoveryDateMetadata` tasks, derived from the earliest ADS reference
  `publication_date`. This is month-precision at best and wrong or absent for many
  historical novae where the ADS corpus is thin or where the earliest references are
  not about the nova itself (see the V5668 Sgr incident where "1886 Princeton
  Scientific Expedition" was linked as an ADS reference).
- `peak_mag` is not populated anywhere and does not exist in the data model.

An operator-curated catalog of ~500+ novae with known discovery dates, AAVSO peak
magnitudes, SIMBAD main IDs, object types, recurrent flags, and rich alias sets has
been accumulated at `tools/catalog-expansion/nova_candidates_final_full_year.csv`,
produced by the nova candidate processor notebook (OT15). This ADR establishes where
and how that curated data lives in the system and how it is made available to
`initialize_nova` at creation time.

### 1.1 Why Not Rely on `refresh_references` Alone

1. Historical novae (pre-~1960) have ADS references named for their discoverers or
   for the observatory reports that announced them, not always the nova itself. The
   derived discovery date is unreliable for this population — see the V5668 Sgr bug
   and the pre-1960 alias-suppression logic planned under item 8.
2. ADS `publication_date` is month-precision at best; it is not a day-precise
   observation date.
3. Completely unavailable for very old novae or those with no papers.
4. Every `initialize_nova` triggers `refresh_references` downstream, which issues an
   ADS query. Short-circuiting via priors saves both query cost and latency when the
   nova is already known.

### 1.2 Why Not Just Add Columns to the Nova DDB Item and Backfill Manually

The CSV is already maintained as operator-curated data and will continue to grow. The
build pipeline is trivial. Priors should be reviewable in Git, not editable in
production — changes to canonical identity information for historical novae belong
behind PR review. The DDB Nova item remains the authoritative post-creation record;
priors are the *seed* consumed once at creation.

### 1.3 Scope Boundary

This ADR covers:

- Physical form and storage location of the nova priors artifact
- Source-of-truth CSV location and the build pipeline that produces the bundled artifact
- Entry schema (which CSV fields map to which bundled fields)
- Where schema validation runs (build time, not runtime)
- Python interface for consumers
- Miss semantics — what happens when a candidate name is not in the priors file
- Versioning and maintenance workflow
- Ground-truth updates required in downstream files

This ADR does **not** cover:

- How `nova_resolver.UpsertMinimalNovaMetadata` consumes the priors (integration work
  under `PI3` / item 3 on today's bugs list). A follow-up ADR or short design note
  will specify the consumer contract, including DDB field writeback, conflict
  resolution with `archive_resolver` output, and the ASL outcome for rejection.
- The truncated-MJD hint logic in ingestion pipelines (item 9; reads `discovery_date`
  from DDB, not from priors).
- Consolidation of the nova candidate processor notebook into the build pipeline
  (tracked under OT15 in the master task list).
- Orbital periods, distances, or other future fields that may be added to the priors
  file (see Decision 12).

---

## 2. Alternatives Considered

### Physical form

| Option | Pros | Cons |
|--------|------|------|
| **Static JSON bundled in Lambda package** | Human-readable; Git-diffable; zero network latency; trivially loaded as a Python dict; negligible Lambda package size at ~500 entries (~100–200 KB) | Updating the file requires a `nova_resolver` deployment |
| Raw CSV bundled in Lambda package | Single artifact | CSV requires runtime parsing of pipe-delimited aliases, boolean-as-string columns, and mixed-precision dates; parsing belongs at build time, not in every Lambda cold start |
| S3-resident file loaded at cold start | No-deploy updates | Adds a runtime external dependency to `nova_resolver`; S3 read on cold start |
| DynamoDB items | No-deploy updates; runtime queryable | Runtime DDB read on every `initialize_nova`; diffs not Git-reviewable; migration overhead for schema changes |

### Source-of-truth format

| Option | Pros | Cons |
|--------|------|------|
| **CSV authoring + JSON bundled output** | CSV is hand-editable; the source CSV already exists; JSON is Lambda-friendly; clean separation of authoring surface from runtime surface | Two artifacts to maintain (but only the CSV is manually edited) |
| JSON authoring (edit the bundled file directly) | One artifact | JSON is inconvenient for hand-edits across hundreds of rows; a single-cell typo breaks deserialization for every consumer |
| CSV at runtime (no build step) | One artifact | Parsing tax on every cold start; mixed types and pipe-delimited fields are fragile |

### Lookup keying

| Option | Pros | Cons |
|--------|------|------|
| **Keyed by normalized primary name, with alias index derived at load time** | Single canonical key; matches `nova_resolver` normalization exactly; alias resolution is a single extra dict lookup | Alias resolution is two-step |
| Keyed by every alias directly | Single-step alias lookup | Data duplication; alias updates require touching every entry |
| UUID-keyed | Stable across name changes | Not applicable here — the primary lookup key *is* the name, before a `nova_id` has been assigned |

### Schema validation locus

| Option | Pros | Cons |
|--------|------|------|
| **Pydantic validation at build time; reader trusts the bundled artifact** | Zero runtime cost; mistakes caught before they reach the repository; CI re-validates the committed artifact for belt-and-suspenders | If the reader is ever pointed at a hand-edited JSON, invalid data could slip through — mitigated by Decision 6 forbidding hand-edits |
| Pydantic validation at Lambda cold start | Every load checked | Adds validation cost to cold start; redundant if build-time validation is enforced |
| No formal validation | Minimum complexity | Loses the contract — a typo in the build script silently ships bad data to Lambda |

### Miss semantics

| Option | Pros | Cons |
|--------|------|------|
| **Miss is neutral — proceed with current archive-resolution flow** | Preserves current behavior for unknown candidates; priors ship can land without item-3 integration; long-tail novae keep working | Operators must hand-curate to get enrichment for new novae |
| Miss blocks ingestion | Strong gate against typos and bogus names | Breaks every new-discovery ingestion until the operator adds a CSV row; catastrophic on MVP |
| Miss triggers a warning but proceeds | Visibility without blocking | Log-noise for every legitimately new nova |

---

## 3. Decisions

### Decision 1 — Physical Form: Static Versioned JSON Bundled in Lambda

The nova priors artifact is a static versioned JSON file committed to the repository at:

```
services/nova_resolver/nova_priors/nova_priors.json
```

It is included in the `nova_resolver` Lambda deployment package and loaded into
memory at module initialization. An in-memory alias index is derived from the
entries at load time.

**Rationale.** This is a direct application of ADR-017 Decision 1 to a structurally
identical problem: curated scientific reference data, low change rate, Lambda-
consumed, operator-maintained. All of ADR-017's justifications apply — zero network
latency, no external runtime dependency, reviewable PRs, sub-millisecond load, PR-
gated change discipline. At ~500 entries the JSON is ~100–200 KB; Lambda package
size impact is negligible.

Updating the priors requires a `nova_resolver` deployment. This is a deliberate
constraint: changes to the canonical curated nova list should not bypass review.

---

### Decision 2 — Canonical Lookup Key: Normalized Primary Name

Each entry's canonical key is the primary name normalized with the same function
used by `nova_resolver._normalize_candidate_name`:

1. Strip leading and trailing whitespace.
2. Replace underscores with spaces.
3. Lowercase.
4. Collapse internal whitespace to single spaces.

The bundled JSON entries are stored as a mapping from normalized name to entry. An
alias → normalized-name index is built at module load time from each entry's
`aliases` list, using identical normalization.

Example:

```
"t crb" → { primary_name: "T CrB", simbad_main_id: "T CrB", ... }
```

**Rationale.** Matches `nova_resolver`'s existing normalization exactly, so a
candidate name handed to `NormalizeCandidateName` resolves identically against the
priors and against existing `NameMapping` DDB items. No new normalization surface
is introduced.

---

### Decision 3 — Entry Schema

Each entry conforms to the following schema. Fields are mapped from CSV columns as
specified below. The schema is **expressed and enforced as a Pydantic model**
(`NovaPriorsEntry`) at build time — see Decision 4.

| JSON field | Type | Source CSV column | Transform |
|------------|------|-------------------|-----------|
| `primary_name` | `string` | `Nova_Name` | Verbatim (display name; normalization applied only for the key) |
| `simbad_main_id` | `string \| null` | `SIMBAD_Name` | Empty CSV cell → `null` |
| `aliases` | `string[]` | `Nova_Aliases` | Pipe-split, whitespace-stripped, deduped while preserving order |
| `discovery_date` | `string \| null` | `Discovery_Date` | Normalized from `M/D/YYYY` to `YYYY-MM-DD`; `00`-convention for unknown day/month per ADR-005 amendment; empty → `null` |
| `otypes` | `string[]` | `Nova_Otypes` | Pipe-split SIMBAD object types |
| `is_nova` | `bool` | `is_nova` | String `"TRUE"`/`"FALSE"` → `bool` |
| `is_recurrent` | `bool` | `is_recurrent` | String `"TRUE"`/`"FALSE"` → `bool` |
| `peak_mag` | `number \| null` | `Peak_Mag` | Empty cell → `null` |
| `peak_mag_band` | `string \| null` | `Filter` | The band in which the peak magnitude was measured; empty → `null` |
| `peak_mag_uncertain` | `bool` | `Uncertainty` | `TRUE` means the peak magnitude value carries significant uncertainty |

The CSV `Input_Name` column is **dropped** at build time. It records the string
used by the upstream notebook to query SIMBAD and is not a consumer-facing field.

Co-field invariants enforced by the Pydantic model (see Decision 4):

- `peak_mag_band` is non-null iff `peak_mag` is non-null. A band without a magnitude,
  or a magnitude without a band, is rejected.
- `peak_mag_uncertain == True` requires `peak_mag` to be non-null. An uncertainty
  flag on a missing measurement is rejected.
- `discovery_date` matches `^\d{4}-(0[1-9]|1[0-2]|00)-(0[1-9]|[12]\d|3[01]|00)$`.
- `primary_name` is non-blank after strip.
- `aliases` entries are non-blank after strip.

The top-level JSON structure:

```json
{
  "_schema_version": "1.0.0",
  "_generated_at": "2026-04-17T14:23:05Z",
  "_source_csv": "tools/catalog-expansion/nova_candidates_final_full_year.csv",
  "_source_sha256": "<sha256 of the source CSV>",
  "_note": "AUTO-GENERATED by build_nova_priors.py — operator review required before commit.",
  "entries": {
    "t crb": { "primary_name": "T CrB", ... },
    "ck vul": { "primary_name": "CK Vul", ... },
    ...
  }
}
```

**Rationale.** Every field the source CSV carries is preserved except `Input_Name`,
per the operator's decision to "bake in everything." Each preserved field has a
clear downstream consumer:

- `primary_name`, `aliases`, `simbad_main_id` — feed `UpsertMinimalNovaMetadata` to
  pre-populate alias set without a SIMBAD round-trip.
- `discovery_date` — pre-populates the Nova item's `discovery_date` field (ADR-005
  amendment format).
- `is_nova` — enables front-door rejection of `initialize_nova` for known non-novae
  (consumer contract to be specified in item 3 follow-up).
- `is_recurrent` — flags recurrent novae for special handling (feeds future D2
  recurrent-novae design).
- `peak_mag`, `peak_mag_band`, `peak_mag_uncertain` — seed a new `peak_mag` field
  set on the Nova item (data model change itemized in §7).
- `otypes` — carried for future use (e.g., the F10 nova-type-as-list change, or
  richer classification display).

---

### Decision 4 — Schema Validation: Pydantic at Build Time, Not Runtime

The `NovaPriorsEntry` Pydantic model in `contracts/models/priors.py` is the
authoritative schema contract. Validation runs in **two places**:

1. **Build time (primary).** `build_nova_priors.py` constructs one
   `NovaPriorsEntry` per CSV row. If any row fails validation, the script fails
   the build and emits a diagnostic report naming the offending rows and
   Pydantic errors. No output JSON is emitted on failure. This is the gate that
   prevents malformed data from ever reaching the repository.

2. **CI (belt-and-suspenders).** `validate_nova_priors.py` runs in CI against
   the *committed* JSON. It re-parses every entry through `NovaPriorsEntry`,
   re-checks structural invariants (alias uniqueness, key is a correctly
   normalized form of `primary_name`, `_schema_version` is recognized), and
   fails the build on any mismatch. This catches hand-edits to the bundled
   JSON (which Decision 6 forbids but cannot physically prevent).

The **reader does not re-validate entries at runtime.** It performs three cheap
checks at module initialization — `_schema_version` major-version check, alias
collision check, and top-level shape check — and then trusts the bundled
artifact. Deserialization into `NovaPriorsEntry` instances can be lazy
(constructed on first `lookup` hit) or eager at the operator's discretion; the
ADR does not mandate which.

**Rationale.** The bundled JSON is a build-time deterministic artifact. Every row
was Pydantic-validated before commit and CI-revalidated before deploy. Runtime
validation is redundant work on a hot path (every `initialize_nova` cold start).
The major-version check at load time is the only safety interlock needed to
protect against a future breaking-change skew between the bundled file and the
deployed Lambda code.

---

### Decision 5 — Alias Index: Derived at Load Time

The alias index is not persisted. At `nova_resolver` module initialization, the
reader walks all entries, normalizes each alias, and builds a `dict[str, str]`
mapping normalized alias → normalized primary name.

**Alias collision handling.** If two entries contribute aliases that normalize to
the same string, the build script raises an error and fails. The build script runs
at authoring time; no collision can reach runtime. The CI validator re-checks the
same invariant against the committed JSON.

**Self-aliasing.** Each entry's `primary_name`, normalized, is also inserted into
the alias index mapping to itself. This lets `lookup()` (Decision 8) try the
alias index unconditionally and not special-case the primary-name path.

**Rationale.** Matches ADR-017 Decision 8. Avoids maintaining two artifacts in
sync. Collision detection happens at build time, where the operator sees it clearly.

---

### Decision 6 — Source of Truth and Build Pipeline

The source of truth for operator edits is:

```
tools/catalog-expansion/nova_candidates_final_full_year.csv
```

The build pipeline is a single script:

```
tools/catalog-expansion/build_nova_priors.py
```

The script:

1. Reads the CSV with `utf-8-sig` encoding (transparently strips the UTF-8 BOM
   present on the current header line).
2. Normalizes `Discovery_Date` from `M/D/YYYY` to `YYYY-MM-DD`. The year component
   must be four digits; any row with a two-digit year is rejected with a loud error.
3. Filters blank rows (no `Nova_Name`).
4. Dedupes by normalized primary name, keeping the first occurrence and logging a
   warning naming the duplicate rows so the operator can reconcile the CSV.
5. Validates that every `is_nova`, `is_recurrent`, and `Uncertainty` cell is
   exactly `"TRUE"` or `"FALSE"`.
6. Validates each row through `NovaPriorsEntry` (Decision 4). On any validation
   failure, aborts with a named diagnostic.
7. Builds the alias index in memory and checks for cross-entry alias collisions
   (Decision 5); on collision, fails.
8. Emits the JSON to stdout or `--output`, with `_source_sha256` set from the
   source CSV.

The operator reviews the generated JSON (diffing against the previous commit) and
commits both the CSV and the regenerated JSON in a single PR.

A companion validation script — `tools/catalog-expansion/validate_nova_priors.py`
— runs in CI and verifies structural invariants on the *committed* JSON per
Decision 4. The validation script does not regenerate the JSON.

**Hand-edits to the bundled JSON are forbidden.** The generated artifact is
marked as such by its `_note` header. If a fix is needed, the operator fixes the
CSV and reruns the build. The CI validator is the safety net; reviewers should
reject any PR that modifies `nova_priors.json` without a corresponding CSV change.

**Rationale.** This pattern is identical to the `band_specs.json` →
`seed_band_registry.py` → `band_registry.json` chain from ADR-017 amendment
(2026-04-01). The operator edits a CSV, which is the natural format for tabular
nova data; the JSON is generated deterministically; diffs in the generated JSON
are PR-visible.

The CSV remains in `tools/catalog-expansion/` rather than moving into
`services/nova_resolver/` because it is operator-authored data, not a runtime
artifact. The notebook that originally produced the CSV (OT15) continues to live
in `tools/catalog-expansion/` and may feed the CSV in the future; notebook
consolidation is tracked separately.

---

### Decision 7 — Maintenance Workflow

| Change type | Mechanism |
|-------------|-----------|
| Add a new nova | Operator appends a row to the CSV; reruns `build_nova_priors.py`; commits both files in a PR |
| Correct a discovery date or peak magnitude | Edit the CSV row; regenerate; PR |
| Add a new field to the schema | Schema-version bump (see Decision 11); update `NovaPriorsEntry`, `build_nova_priors.py`, `validate_nova_priors.py`, and reader as needed; update this ADR |
| Remove a non-nova entry | Drop the CSV row (or flip `is_nova` to `FALSE`); regenerate; PR |

There is no interactive CLI. Editing the CSV in a spreadsheet or text editor is
sufficient; the build and validation scripts catch the mistakes that matter.

**Rationale.** Matches ADR-017 Decision 7. At MVP scale and single-operator
cadence, a CLI is premature.

---

### Decision 8 — Python Interface Contract: Minimal Read-Only API

The reader module (`services/nova_resolver/nova_priors/reader.py`) exposes:

| Function | Signature | Purpose |
|----------|-----------|---------|
| `lookup` | `(candidate_name: str) -> NovaPriorsEntry \| None` | Normalize the input, look it up via the alias index (which includes primary names, per Decision 5), return the entry or `None`. Primary consumer API. |
| `get_entry` | `(normalized_name: str) -> NovaPriorsEntry \| None` | Direct lookup by already-normalized name; skips the normalization step. For callers that have already normalized. |
| `is_known_non_nova` | `(candidate_name: str) -> bool` | Convenience: returns `True` iff `lookup(candidate_name)` returns an entry with `is_nova == False`. Scoped to the anticipated `initialize_nova` rejection flow (item 3). |
| `list_entries` | `() -> Iterator[NovaPriorsEntry]` | Iteration for backfill scripts and test assertions. |

`NovaPriorsEntry` is the Pydantic model from Decision 3 / 4. It lives in
`contracts/models/priors.py` and is re-exported from the reader module's
`__init__.py`.

At runtime, the reader performs only the cheap checks enumerated in Decision 4
at module initialization; per-entry Pydantic validation is not re-run on every
cold start.

**Rationale.** Mirrors ADR-017 Decision 8 — four functions, read-only, no
dependencies beyond Pydantic and stdlib. The `is_known_non_nova` convenience is
anticipated by the item-3 rejection flow and keeps that caller one line long.

---

### Decision 9 — Miss Semantics: Priors Are Enrichment, Not a Gate

When `lookup(candidate_name)` returns `None`, `initialize_nova` **proceeds with
its current behavior unchanged**:

- `NormalizeCandidateName` → `CheckExistingNovaByName` → (if miss)
  `ResolveCandidateAgainstPublicArchives` via SIMBAD + TNS
  → `CreateNovaId` → `UpsertMinimalNovaMetadata` with archive-derived fields
  → downstream `ingest_new_nova` / `refresh_references`.

Absence from priors is **not** a signal that the candidate is invalid. Priors
are a **curated enrichment source**, not a canonical registry of all known
novae — new discoveries, rare novae, and novae not yet added to the curated
list must all continue to work. The only gate introduced by this ADR is the
explicit `is_known_non_nova` rejection flow (consumer contract in item-3
follow-up), which requires a *present* entry with `is_nova == False`.

The three-way decision matrix at `initialize_nova` entry is therefore:

| `lookup()` result | `is_nova` | Behavior |
|-------------------|-----------|----------|
| `None` | — | Proceed with current archive-resolution flow; no priors enrichment applied |
| Entry | `True` | Proceed with current flow, but pre-populate `discovery_date`, `peak_mag*`, `is_recurrent`, and enrich `aliases` from priors before archive resolution (details in item-3 follow-up) |
| Entry | `False` | Reject at the front door; emit a new ASL outcome; do not create a Nova item (details in item-3 follow-up) |

**Rationale.** Making priors a gate would break every new discovery that
operators ingest via `batch_ingest.py names ...` before they've had a chance to
hand-curate a CSV row. The priors file is a thick curated snapshot of the
well-characterized historical nova population; the runtime pipeline must continue
to handle the long tail. Treating a miss as neutral — "no extra information
available, proceed as before" — preserves current behavior for unknown candidates
while unlocking real enrichment for known ones. This also ensures ADR-036 can
land independently of any consumer work: `initialize_nova` behavior is unchanged
until item-3 wires the reader in.

---

### Decision 10 — Scope: Priors Are Creation-Time Seed, Not Runtime Authority

Priors are consumed exactly once per nova — at creation, by
`UpsertMinimalNovaMetadata`. After the Nova DDB item is written, DDB is
authoritative. Subsequent updates to the priors file do **not** retroactively
modify existing Nova items.

If an operator wants to backfill changes from the priors file into existing DDB
items, that is a separate operator-tooling task (analogous to `set_nova_dates.py`).
This ADR does not prescribe that tool.

**Rationale.** Keeps the priors file in the same conceptual role as the band
registry: a *seed* consumed by the pipeline, not a live runtime authority. Avoids
introducing a second source of truth on Nova metadata that could drift from the
DDB record.

---

### Decision 11 — Versioning: `_schema_version` Field + Git History

The bundled JSON carries a top-level `_schema_version` field using semver:

- **Patch** (1.0.0 → 1.0.1): Adding, updating, or removing entries. No consumer
  code change required.
- **Minor** (1.0.x → 1.1.0): Adding new fields to the entry schema, with defaults
  or nullability so existing consumers continue to work. `NovaPriorsEntry` is
  updated; consumers may optionally use the new fields.
- **Major** (1.x.y → 2.0.0): Removing or renaming fields, changing normalization
  rules, altering the alias matching contract. Requires coordinated consumer
  updates.

The reader module checks `_schema_version` at load time. On a major version
mismatch the module raises immediately rather than silently operating against an
incompatible schema. Minor and patch mismatches are accepted without error.

Git history is the changelog. PR descriptions document the rationale for each
change; a `_schema_version` bump is part of the same commit as the schema change
it describes.

**Rationale.** Identical pattern and rationale as ADR-017 Decision 9.

---

### Decision 12 — Naming: `nova_priors` (Future-Extensible)

The file and package are named `nova_priors` rather than `discovery_dates` or
`nova_catalog`. Anticipated future additions include:

- Orbital period (when curation begins)
- Distance / parallax (when curation begins)
- Multiple outburst epochs for recurrent novae (pending D2 recurrent-novae design)

Each such addition is a minor version bump (Decision 11) with corresponding CSV
column additions and build-script updates. The package structure and reader API
do not need to change.

**Rationale.** The operator has flagged orbital periods and distances as likely
future columns. Naming the artifact generically now avoids a later rename. The
source CSV is already appropriately named (`nova_candidates_final_full_year`) to
accommodate growth in column count.

---

## 4. Open Items

| # | Item | Resolution target |
|---|------|-------------------|
| 1 | Consumer contract — exactly how `UpsertMinimalNovaMetadata` reads and applies the priors, which DDB fields it writes, and how it reconciles with `archive_resolver` output | Item 3 (PI3) integration work; follow-up ADR or design note |
| 2 | ASL terminal outcome for `is_nova=FALSE` rejection — new outcome name, dispatch, downstream workflow suppression semantics | Item 3 integration work |
| 3 | Whether to represent recurrent-nova status via the existing `nova_type` string field (e.g. `"recurrent"`) or via a dedicated boolean on the Nova item. Priors carry a boolean; the DDB Nova item currently uses a nullable string with `"recurrent"` as an example value. | Item 3 integration work |
| 4 | Backfill of existing Nova items with priors data, if desired | Separate operator-tooling task (analogous to `set_nova_dates.py`); outside the scope of this ADR |
| 5 | Notebook → build-script consolidation | Tracked as OT15 in the master task list |

---

## 5. Notes

**Note A — Relationship to `tools/novacat-tools/discovery_dates.csv`.** That legacy
CSV contained the operator's earlier hand-maintained discovery date list. It is
superseded by `tools/catalog-expansion/nova_candidates_final_full_year.csv` and
may be deleted or archived in a follow-up commit. This ADR does not prescribe the
cleanup.

**Note B — Relationship to `set_nova_dates.py`.** That operator tool remains the
authoritative mechanism for post-hoc edits to `outburst_date` and `discovery_date`
on the DDB Nova item. It is not obsoleted by this ADR. Priors seed the
creation-time values; `set_nova_dates` amends them thereafter.

**Note C — UTF-8 BOM in source CSV.** The current
`nova_candidates_final_full_year.csv` has a UTF-8 BOM on the header line. The
build script reads with `utf-8-sig` encoding to handle this transparently.
Regenerated CSVs do not need to strip the BOM manually; the build script tolerates
both forms.

**Note D — Relationship to `OT15` (nova candidate processor notebook).** The
notebook is the upstream producer of
`nova_candidates_final_full_year.csv`. The decision to keep the CSV as the
authoring surface (Decision 6) leaves the notebook → CSV step untouched. A future
consolidation may fold the notebook's work into `build_nova_priors.py`, but that
is tracked separately under OT15.

**Note E — ADR-014 peak magnitude was previously deferred.** ADR-014's narrative
for `nova.json` notes that peak magnitude, spectroscopic class, and other physical
parameters were deferred pending "manual curation or a reliable automated
sourcing mechanism." This ADR provides the manual curation mechanism for peak
magnitude. The item-3 follow-up amends ADR-014 accordingly (§7).

---

## 6. Consequences

### 6.1 Immediate

Files created by this ADR's implementation work (each will land in its own commit):

- `services/nova_resolver/nova_priors/__init__.py`
- `services/nova_resolver/nova_priors/nova_priors.json` (generated)
- `services/nova_resolver/nova_priors/reader.py`
- `contracts/models/priors.py` — `NovaPriorsEntry` Pydantic model
- `tools/catalog-expansion/build_nova_priors.py` — operator build script
- `tools/catalog-expansion/validate_nova_priors.py` — CI-runnable validator
- Tests under `tests/services/test_nova_priors_reader.py` and
  `tests/tools/test_build_nova_priors.py`

No files are deleted by this ADR.

### 6.2 Deferred to Item-3 Integration Work

The following are **not** part of this ADR's implementation but are enabled by it:

- Wiring the reader into `UpsertMinimalNovaMetadata`
- Front-door rejection flow for `is_nova=FALSE`
- Any DDB field writes triggered by priors
- Any artifact schema changes that surface priors data to the frontend

These are listed in §7 as ground-truth updates that the item-3 follow-up will make.

### 6.3 `initialize_nova` Behavior Is Unchanged by This ADR Alone

Shipping ADR-036's implementation without item-3 makes the priors reader
available as an importable module but changes no pipeline behavior. This is
deliberate (see Decision 9): the ADR can land, be reviewed, and be deployed
independently of the consumer work.

---

## 7. Ground Truth Updates Required (by Item-3 Follow-Up)

This ADR introduces the priors artifact and reader. It does **not** itself
modify the Nova DDB item, the Nova Pydantic contract, or any downstream artifact
schema. Those changes are scoped to the item-3 consumer work and are enumerated
here so they are not forgotten.

| Source | Required change | Owned by |
|---|---|---|
| `contracts/models/entities.py` | Add `peak_mag: float \| None`, `peak_mag_band: str \| None`, and `peak_mag_uncertain: bool` fields to `Nova`. Add a `model_validator` enforcing the peak-mag co-field invariants (Decision 3). Resolve Open Item 3 (`nova_type`-string vs. dedicated boolean for recurrent flag). Bump `Nova.schema_version`. | Item 3 |
| `docs/storage/dynamodb-item-model.md` | Extend the Nova item field list with `peak_mag`, `peak_mag_band`, and `peak_mag_uncertain`. Update the example JSON. Document the peak-mag co-field invariants. | Item 3 |
| `docs/adr/ADR-014-artifact-schemas.md` | Add `peak_mag`, `peak_mag_band`, and `peak_mag_uncertain` to the `nova.json` schema and update the example. Decide whether `peak_mag` (only) also belongs on the `catalog.json` summary record; if so, add it there. Bump the `schema_version` on both artifacts per ADR-014 amendment policy. Remove the "deferred" note on peak magnitude that ADR-014 currently carries. | Item 3 |
| `services/artifact_generator/generators/nova.py` | Read the new fields from the Nova DDB item and emit them in `nova.json`. | Item 3 |
| `services/artifact_generator/generators/catalog.py` | If the ADR-014 amendment includes catalog-level peak magnitude, read and emit it in `catalog.json`. | Item 3 |
| `services/artifact_finalizer/handler.py` | No change expected — the finalizer writes observation counts, not creation-time priors. Priors are written at `initialize_nova` time. Flagged here only to make the non-change explicit. | — |
| `services/nova_resolver/handler.py` — `UpsertMinimalNovaMetadata` | Query the priors reader; apply enrichment per the Decision 9 matrix; write the new Nova DDB fields on hit. | Item 3 |
| `infra/workflows/initialize_nova.asl.json` | Add new terminal outcome state(s) for the `is_nova=FALSE` rejection path. | Item 3 |
| `docs/workflows/initialize-nova.md` | Document the new priors-consumption step and the rejection outcome. | Item 3 |
| `tests/integration/test_initialize_nova_integration.py` | Add cases for: priors miss (current behavior), priors hit with `is_nova=True` (enrichment applied), priors hit with `is_nova=False` (rejection). | Item 3 |
| `frontend/src/components/nova/...` | Display peak magnitude where appropriate on the nova detail page. | Deferred, post-item-3 |

### 7.1 Forward Dependencies (Downstream Design Work)

| Downstream artifact | Dependency on this ADR |
|---------------------|------------------------|
| Item 3 (PI3 — `initialize_nova` enrichment) | Consumes the reader API; owns every entry in §7 above |
| Item 9 (truncated-MJD hints in ingestion) | Reads `discovery_date` from DDB (which priors seed); no direct dependency on this file |
| F10 (nova type as list) | May consume `otypes` from priors at creation time; open question whether to map SIMBAD otypes → nova type is F10's concern, not this ADR's |
| D2 (recurrent novae design) | Consumes `is_recurrent` flag (or the `nova_type=="recurrent"` representation, per Open Item 3) at creation time |
| OT15 (nova candidate processor notebook) | Upstream producer of `nova_candidates_final_full_year.csv`; may be consolidated into the build script in the future |
