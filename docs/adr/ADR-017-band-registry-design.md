# ADR-017: Band Registry Design

**Status:** Accepted
**Date:** 2026-03-21 (accepted 2026-03-24)
**Author:** TF
**Supersedes:** ADR-016 (Band and Filter Resolution Strategy) — fully superseded
**Superseded by:** —
**Amends:** —
**Relates to:**
- `DESIGN-001` §5 Layer 1 — authoritative design basis for this ADR
- `DESIGN-002` §5.4 — `ColorRow` `band1_id`/`band2_id` dependency on registry
- `ADR-016` — superseded; see §1
- `ADR-018` — Band Disambiguation Algorithm (depends on registry interface defined here)
- `ADR-019` — Photometry Table Model Revision (inherits `regime` vocabulary from §3.3;
  demotes `spectral_coord_value` from required source field to registry-derived field)
- `ADR-021` — Layer 0 Pre-Ingestion Normalization (band alias lookup seam)
- `docs/specs/photometry_table_model.md` — `filter_name`, `phot_system`,
  `spectral_coord_*` fields are the primary consumers of registry data

---

## 1. Context

DESIGN-001 §5 Layer 1 identifies the band registry as the single authoritative source
of truth for photometric band identity in NovaCat. It explicitly defers five design
questions to this ADR:

1. Physical form and storage location
2. Registry entry schema (fields and types)
3. Canonical band ID naming convention
4. Relationship to the SVO Filter Profile Service and to high-energy/radio regimes
   outside SVO scope
5. Operator mechanism for additions and maintenance

**Why ADR-016 is superseded.** ADR-016 addressed band resolution as an adapter-internal
concern, encoding band knowledge as Python module-level dicts (`_COMBINED_BAND_LOOKUP`,
`_DEFAULT_PHOT_SYSTEM`, `_PHOT_SYSTEM_SPECTRAL_META`) and a companion
`excluded_filters.json` file. DESIGN-001 §2.1 identifies three structural failures in
that approach: no principled coverage model, band knowledge embedded in application
source code, and no model of band identity as a structured object. This ADR corrects
all three by establishing band knowledge as a first-class versioned data artifact
independent of the code that consumes it.

**Scope boundary.** This ADR covers the registry's physical form, schema, naming
convention, operator tooling, and Python interface. It does not cover:

- The disambiguation algorithm for ambiguous aliases — ADR-018
- The `PhotSystem` enum redesign in `entities.py` — ADR-019
- `ColorRow` schema and persistence — ADR-022
- Any implementation work — Epic A (Band Registry Implementation)

---

## 2. Alternatives Considered

### Physical form

| Option | Pros | Cons |
|--------|------|------|
| **Static JSON bundled in Lambda package** | Human-readable; Git-diffable; zero network latency; trivially loaded as a Python dict; negligible Lambda package size impact | Updating the registry requires a deployment |
| SQLite database bundled in Lambda package | Indexed lookups; handles very large registries | Binary blob, not Git-diffable; no material advantage at MVP scale |
| DynamoDB table managed by operator tooling | Supports editing without a deployment cycle | Network RTT on every ingestion execution; runtime external dependency; operational complexity |

---

## 3. Decisions

### Decision 1 — Physical Form: Static Versioned JSON Bundled in Lambda

The band registry is a static versioned JSON file committed to the repository at:

```
services/photometry_ingestor/band_registry/band_registry.json
```

It is included in the Lambda deployment package and loaded into memory at module
initialization. An in-memory alias index is derived from the registry at load time.

**Rationale:** Zero network latency; no external runtime dependency; every change is
a reviewable, auditable PR; Python dict load is sub-millisecond at MVP scale. Requiring
a deployment for each registry update is a deliberate constraint that ensures all
changes are reviewed and versioned.

**SVO-first principle.** The registry is not built inductively from observed data and
then verified against SVO. The order of operations is:

1. Survey the data to understand which filter strings appear in practice — this informs
   *which* SVO entries to pull, not *what* those entries contain.
2. Pull those entries from the SVO database to build the registry.
3. Match ingested data against the registry.

SVO is the definitional authority for all bands it covers. Any entry outside SVO scope
must have a documented rationale.

---

### Decision 2 — Canonical Band ID Naming Convention

Each registry entry carries a `band_id`: a stable, unique NovaCat identifier for the
filter class. It is the primary reference that `PhotometryRow` and `ColorRow` fields
resolve to.

**Format:**

```
{SystemAbbrev}_{BandLabel}
```

**Rules:**

- Components are separated by a single underscore (`_`).
- No spaces, slashes, dots, or other punctuation. `band_id` values are
  Python-identifier-safe.
- `BandLabel` is the community-standard band identifier, preserving its conventional
  casing (e.g. `V`, `g`, `Ks`, `NUV`).
- `band_id` must appear as the first element of the entry's `aliases` list.
- `band_id` values must be globally unique within the registry.

**`SystemAbbrev` rules.**

The purpose of filter classes is to cluster physically similar filters so that
measurements from different instruments are scientifically comparable — meaning a
researcher can combine them on a single light curve without requiring a color correction
larger than the typical measurement uncertainty. The number of classes should reflect
the number of meaningfully distinct bandpasses in the literature, not the number of
instruments that have ever observed a nova.

`SystemAbbrev` is derived from SVO's `photometric_system` field, with TF's judgment
applied where SVO's naming diverges from community convention. It follows the
community-standard name for the photometric or instrument system that defines the
filter class. Instrument name takes precedence over facility name where the instrument
is the meaningful identifier (e.g. `UVOT` not `Swift`).

For MVP, community-standard naming is TF's judgment call. Post-launch, registry naming
conventions are open to community feedback through a documented process.

**Physical similarity thresholds.** Two filters belong to the same class if their
central wavelengths and bandwidths are sufficiently similar that combining them on a
single light curve introduces no scientifically meaningful systematic error. At MVP
scale (23 entries), filter class membership is determined by the operator's domain
expertise during the seed process, not by a numerical threshold. Formal X%/Y%
thresholds on central wavelength and bandwidth similarity are deferred to post-MVP; if
the registry grows to a scale where class membership decisions become frequent or
contentious, empirical thresholds can be established from analysis of the full SVO
dataset without changing any other aspect of this ADR.

**Generic entries.** `Generic_<BandLabel>` entries (e.g. `Generic_V`) are a first-class
concept in the registry. They represent cases where the band identity is known but the
specific photometric system or instrument is not. They are the conservative fallback
when disambiguation cannot resolve to a more specific filter class. See ADR-018 for the
disambiguation algorithm that governs when Generic entries are selected.

> **Amendment (alias ownership correction, 2026-04-01):** Generic entries own all bare
> single-letter aliases (`U`, `B`, `V`, `R`, `I`, `J`, `H`, `K`) and legacy system
> names (`Johnson_V`, `Cousins_R`, etc.). Instrument-specific entries carry only their
> own `band_id` as an alias. Generic optical entries (U–I) carry Bessell reference
> spectral data from `HCT/HFOSC.Bessell_*` SVO profiles; Generic NIR entries (J, H)
> carry 2MASS reference data. See `ADR-017-amendment-band-id-naming.md` § Alias
> Ownership Invariant for the full rule and rationale.

---

### Decision 3 — Registry Entry Schema

> **Amendment (`epic/22-photometry-doc-reconciliation`):** `photometric_system` field
> removed from the registry entry schema. Band identity is fully captured by `band_id`,
> `regime`, and the SVO-derived spectral fields. See Category 2 of the reconciliation
> delta log.

Each entry represents a **filter class** — an abstract grouping of physically similar
filters that are scientifically comparable. Specific instrument filter details are not
stored as separate entities; they are captured via the `svo_filter_id` cross-reference
and the SVO-derived spectral fields.

All fields are always present on every entry. Use `null` for inapplicable fields rather
than omitting the key.

**Note on sparse entries.** The majority of entries — particularly those covering AAVSO
and other amateur observatory data — will have most SVO-derived fields as `null`. The
schema is designed to accommodate this gracefully. A valid entry requires only `band_id`,
`aliases`, `excluded`, and either `exclusion_reason` (if excluded) or `regime` (if not).

**Example entry (instrument-specific band):**

> **Amendment (alias ownership correction, 2026-04-01):** This example is updated.
> The instrument-specific entry carries only its own `band_id` as an alias. Bare
> aliases (`V`, `Johnson_V`, etc.) are now on `Generic_V`. See
> `ADR-017-amendment-band-id-naming.md` Amended Decision 3 for the corrected examples.
```json
{
  "band_id": "HCT_HFOSC_Bessell_V",
  "svo_filter_id": "HCT/HFOSC.Bessell_V",
  "band_name": "V",
  "regime": "optical",
  "detector_type": "photon",
  "observatory_facility": "HCT",
  "instrument": "HFOSC",
  "aliases": ["HCT_HFOSC_Bessell_V"],
  "excluded": false,
  "exclusion_reason": null,
  "lambda_eff": 5696.92,
  "lambda_pivot": 5772.0,
  "lambda_min": 4800.41,
  "lambda_max": 7856.63,
  "fwhm": 1584.54,
  "effective_width": 1582.22,
  "calibration": {
    "vega": {
      "zero_point_flux_lambda": 3.11501e-09,
      "zero_point_flux_nu": 3461.72,
      "zeropoint_type": "Vega",
      "photcal_id": "HCT/HFOSC.Bessell_V/Vega"
    },
    "ab": null,
    "st": null
  },
  "disambiguation_hints": {}
}
```

**Example entry (Generic fallback — with Bessell reference data):**

> **Amendment (alias ownership correction, 2026-04-01):** This example is updated.
> Generic entries are no longer sparse — they carry reference spectral data from the
> canonical SVO profile for their photometric system. Generic entries own all bare
> ambiguous aliases. See `ADR-017-amendment-band-id-naming.md` Amended Decision 3.
```json
{
  "band_id": "Generic_V",
  "svo_filter_id": "HCT/HFOSC.Bessell_V",
  "band_name": "V",
  "regime": "optical",
  "detector_type": "photon",
  "observatory_facility": null,
  "instrument": null,
  "aliases": ["Generic_V", "Johnson_V", "V", "Johnson V", "Vmag"],
  "excluded": false,
  "exclusion_reason": null,
  "lambda_eff": 5696.92,
  "lambda_pivot": 5772.0,
  "lambda_min": 4800.41,
  "lambda_max": 7856.63,
  "fwhm": 1584.54,
  "effective_width": 1582.22,
  "calibration": {
    "vega": {
      "zero_point_flux_lambda": 3.11501e-09,
      "zero_point_flux_nu": 3461.72,
      "zeropoint_type": "Vega",
      "photcal_id": "HCT/HFOSC.Bessell_V/Vega"
    },
    "ab": null,
    "st": null
  },
  "disambiguation_hints": {}
}
```

**Example entry (excluded mode):**

```json
{
  "band_id": "AAVSO_Vis",
  "svo_filter_id": null,
  "band_name": null,
  "regime": null,
  "detector_type": null,
  "observatory_facility": null,
  "instrument": null,
  "aliases": ["AAVSO_Vis", "Vis.", "Visual", "vis"],
  "excluded": true,
  "exclusion_reason": "visual estimate — not calibrated photometry",
  "lambda_eff": null,
  "lambda_pivot": null,
  "lambda_min": null,
  "lambda_max": null,
  "fwhm": null,
  "effective_width": null,
  "calibration": {
    "vega": null,
    "ab": null,
    "st": null
  },
  "disambiguation_hints": {}
}
```

**Field definitions:**

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `band_id` | string | No | Stable NovaCat canonical band identifier. Naming convention per Decision 2. Globally unique within the registry. Must be first element of `aliases`. |
| `svo_filter_id` | string | Yes | SVO Filter Profile Service identifier. `null` if no SVO entry exists. |
| `band_name` | string | Yes | From SVO `Band Name` (e.g. `V`, `J`, `g`). Coarse band label without system qualifier. |
| `regime` | string | Yes | Broad wavelength regime. Controlled vocabulary: `optical`, `uv`, `nir`, `mir`, `fir`, `xray`, `radio`, `gamma`. `null` for excluded entries. |
| `detector_type` | string | Yes | From SVO `Detector Type`. `null` if unknown or not applicable. |
| `observatory_facility` | string | Yes | From SVO `Obs. Facility`. `null` if unknown. |
| `instrument` | string | Yes | From SVO `Instrument`. `null` if unknown. |
| `aliases` | array[string] | No | All known string forms by which this band appears in real-world source files. Case-sensitive (Decision 4). Must include `band_id` as first element. Never empty. |
| `excluded` | boolean | No | `true` if this entry represents a non-photometric observation mode to be rejected at ingestion. |
| `exclusion_reason` | string | Yes | Human-readable rejection reason. Non-null when `excluded: true`; `null` otherwise. Written to the row failure record at ingestion time. |
| `lambda_eff` | float | Yes | Effective (flux-weighted mean) wavelength in Angstrom. Canonical spectral coordinate value. |
| `lambda_pivot` | float | Yes | Pivot wavelength in Angstrom. The wavelength at which the f_λ → f_ν conversion is exact. |
| `lambda_min` | float | Yes | Minimum wavelength of the bandpass in Angstrom. |
| `lambda_max` | float | Yes | Maximum wavelength of the bandpass in Angstrom. |
| `fwhm` | float | Yes | Full width at half maximum in Angstrom. |
| `effective_width` | float | Yes | Effective width (Weff) in Angstrom. Distinct from FWHM for non-Gaussian profiles. |
| `calibration.vega` | object | Yes | Vega system calibration block. `null` if not available. |
| `calibration.ab` | object | Yes | AB system calibration block. `null` if not available. |
| `calibration.st` | object | Yes | ST system calibration block. `null` if not available. |
| `calibration.*.zero_point_flux_lambda` | float | Yes | Zero-point flux density in erg/cm²/s/Å. |
| `calibration.*.zero_point_flux_nu` | float | Yes | Zero-point flux density in Jy. |
| `calibration.*.zeropoint_type` | string | Yes | Zero-point definition type (e.g. `Pogson`, `Asinh`, `Linear`). |
| `calibration.*.photcal_id` | string | Yes | SVO PhotCal ID for this calibration system. |
| `disambiguation_hints` | object | No | Reserved for ADR-018. Must be `{}` on all entries authored before ADR-018 is adopted. Application code must not read this field until ADR-018 is adopted. |

---

### Decision 4 — Alias Matching Is Case-Sensitive

Alias matching is case-sensitive. This is an intentional policy that preserves semantic
distinctions that case-folding would destroy:

> **Amendment (alias ownership correction, 2026-04-01):** This table is updated to
> reflect corrected alias ownership. See `ADR-017-amendment-band-id-naming.md` Amended
> Decision 4 for the expanded table with resolution types.

| String | Correct band |
|--------|-------------|
| `V` | `Generic_V` |
| `Johnson_V` | `Generic_V` |
| `HCT_HFOSC_Bessell_V` | `HCT_HFOSC_Bessell_V` |
| `i` | `SLOAN_SDSS_i` |
| `I` | `Generic_I` |
| `Ks` | `2MASS_Ks` |
| `K` | `Generic_K` |

Source files must be ingested with their original filter string casing preserved.
Carries forward unchanged from ADR-016 Decision 2.

---

### Decision 5 — `excluded_filters.json` Is Abolished

`services/photometry_ingestor/adapters/excluded_filters.json` is removed. All of its
entries become first-class registry entries with `excluded: true` and an
`exclusion_reason` string. This ensures all band string knowledge lives in one place
and eliminates the need to keep two knowledge bases consistent.

Migration is performed as part of Epic A. The deletion of `excluded_filters.json` and
the introduction of `band_registry.json` are the same commit.

---

### Decision 6 — SVO Sync Pattern: One-Shot Seed, No Recurring Infrastructure

> **Amendment (data/code separation, 2026-04-01):** Band definitions (aliases, SVO
> candidates, metadata) have been extracted from the seed script into a standalone data
> file at `tools/filter_band_reg/band_specs.json`. The seed script now loads this file
> by default. See `ADR-017-amendment-band-id-naming.md` Amended Decision 10 for details.

The registry is populated via a one-shot operator script (`scripts/seed_band_registry.py`)
that queries the SVO Filter Profile Service via `astroquery.svo_fps`, retrieves the
spectral fields for each target filter, and writes `band_registry.json`. This script is
development-time tooling — it is not deployed, not invoked at runtime, and carries no
Lambda dependency.

There is no automated sync mechanism. When a new band is needed that requires SVO data,
the operator updates the script's target list, reruns the script, and commits the
resulting `band_registry.json` via a normal PR. The script is the sole authoring path
for SVO-sourced entries.

**Rationale:** At MVP scale (tens of entries, not thousands), automated sync
infrastructure — polling for SVO updates, diffing against the local registry, handling
SVO downtime — solves a problem that doesn't exist yet. If the registry grows past ~100
entries or SVO data drift becomes a concern, a periodic sync script can be introduced
without changing the registry's physical form or consumer interface.

---

### Decision 7 — Operator Maintenance: Hand-Edit JSON + Validation Script

The registry is maintained through two paths, depending on entry type:

**SVO-sourced entries** (non-excluded bands with spectral data): Update the seed script
target list and rerun, per Decision 6. The script is the single source of truth for
SVO-derived field values — operators do not hand-edit spectral fields on SVO-sourced
entries.

**Excluded entries and non-SVO entries** (e.g. `AAVSO_Vis`, `Generic_K`): Hand-edit
`band_registry.json` directly. These entries have mostly-null spectral fields and
require no SVO query. This is the "fast path for excluded entry addition" called out in
§5 Note A.

**Validation script** (`scripts/validate_band_registry.py`): A standalone script that
checks structural invariants on the committed JSON. It runs in CI and can be invoked
locally before committing. Checks include: unique `band_id` values, `band_id` appears
as first alias, no duplicate aliases across entries, required fields present, excluded
entries have `exclusion_reason`, non-excluded entries have `regime`, and schema shape
conformance.

There is no interactive CLI framework, no `add_excluded_band.py` helper, and no registry
editor. The JSON file is small enough that direct editing with validation-on-save is the
appropriate tool.

**Rationale:** A CLI framework for registry edits is premature at MVP scale. The
validation script catches the mistakes that matter (duplicate aliases, missing fields,
schema violations). If maintenance burden grows, a CLI can be layered on without changing
the registry format.

---

### Decision 8 — Python Interface Contract: Minimal Read-Only API

The registry's Python interface is a single module (`band_registry/registry.py`)
exposing a small read-only API. It is the only code path that reads
`band_registry.json` — no other module parses the file directly.

**Load behavior:** The registry is loaded once at module import time (module-level
initialization). The JSON is parsed into a list of entry dicts, and a case-sensitive
alias index (`dict[str, str]`) mapping every alias to its owning `band_id` is built in
the same pass. Both structures are module-level singletons.

**Public API (four functions):**

```python
def lookup_band_id(alias: str) -> str | None:
    """Return the band_id for an exact alias match, or None."""

def get_entry(band_id: str) -> BandRegistryEntry | None:
    """Return the full registry entry for a band_id, or None."""

def is_excluded(band_id: str) -> bool:
    """Return True if the band_id exists and is excluded."""

def list_all_entries() -> list[BandRegistryEntry]:
    """Return all registry entries. The returned list is a shallow copy."""
```

`BandRegistryEntry` is a frozen Pydantic model mirroring the JSON schema from
Decision 3. It is the only public type exported by the module.

**What this API deliberately does not include:**

- No fuzzy matching, substring search, or case-folded lookup — that's ADR-018's domain.
- No write/mutation methods — the registry is immutable at runtime.
- No dedicated filter methods (by regime, system, or wavelength range). At 23 entries,
  any caller that needs filtering can trivially comprehend over `list_all_entries()`.
  More importantly, we do not yet know what filtering signatures ADR-018's
  disambiguation funnel will need — adding named methods now risks committing to an API
  shape that the actual consumer wants differently. If a recurring filter pattern
  emerges during ADR-018 or Epic D implementation, it can be promoted to a named method
  at that point.

**Rationale:** The adapter's hot path is `lookup_band_id(filter_string)` followed by
`get_entry(band_id)` for the resolved entry's metadata. `is_excluded` is a convenience
that avoids requiring callers to fetch the full entry just to check the flag.
`list_all_entries()` provides iteration without forcing consumers to parse the JSON
directly. This four-function surface is the minimum that satisfies ADR-018's
disambiguation algorithm (which wraps `lookup_band_id` with contextual narrowing logic)
and Epic D's `_resolve_band()` wiring.

---

### Decision 9 — Versioning: Schema Version Field + Git History

The registry carries a top-level `_schema_version` field (already present in the seed
output) using semver:

- **Patch** (1.0.0 → 1.0.1): Adding entries, updating aliases, correcting SVO-derived
  field values. No consumer code changes required.
- **Minor** (1.0.x → 1.1.0): Adding new fields to the entry schema (with defaults/nulls
  so existing entries remain valid). `BandRegistryEntry` Pydantic model updated;
  consumers may optionally use the new fields.
- **Major** (1.x.y → 2.0.0): Removing or renaming fields, changing `band_id` values,
  altering the alias matching contract. Requires coordinated consumer updates.

The `_schema_version` field is checked at load time by `registry.py`. On a major version
mismatch the module raises immediately rather than silently operating against an
incompatible schema. Minor and patch mismatches are accepted without error.

There is no separate changelog file. The registry is a Git-tracked artifact —
`git log --follow band_registry.json` is the changelog, and PR descriptions (per the
project's conventional commit discipline) document the rationale for each change. The
`_schema_version` bump is part of the same commit as the schema change it describes.

**Rationale:** At MVP scale with a single operator, a dedicated changelog or migration
framework is overhead without benefit. The schema version field is the minimum mechanism
that protects against silent incompatibility after a breaking change. Git history
provides full auditability. If multi-contributor workflows emerge, a `CHANGELOG.md`
section can be added without changing the versioning scheme.

---

### Decision 10 — Initial Population: 23 Entries Derived from VizieR Census

> **Amendment (band_id naming revision, 2026-03-25):** Entry count increased to 32.
> Instrument-specific entries use SVO-derived `band_id` values. Generic fallback entries
> added for all ambiguous single-letter band aliases.
>
> **Amendment (alias ownership correction, 2026-04-01):** Bare aliases moved from
> instrument-specific entries to Generic entries. Generic optical entries populated with
> Bessell reference data; Generic NIR J/H entries populated with 2MASS reference data.
> Band definitions extracted to `tools/filter_band_reg/band_specs.json`.
>
> See `ADR-017-amendment-band-id-naming.md` Amended Decision 10 for the current
> population description.

The initial registry population is the 23 entries present in the seed
`band_registry.json` produced by the scan-then-seed process described in Decision 6.
These entries cover every filter string that appears in the current VizieR catalog
manifest (the four tables in `catalog_manifest.csv` that contain photometric data).

**Coverage by regime:**

- **Optical (14):** Johnson U, B, V; Cousins R, I; Sloan u, g, r, i, z; Sloan u', g',
  r', i'
- **NIR (4):** 2MASS J, H, Ks; Generic K (K-band disambiguation fallback per ADR-016)
- **UV (3):** Swift/UVOT UVW1, UVW2, UVM2
- **MIR (2):** Spitzer/IRAC [3.6], [4.5]
- **HST (1):** F555W (WFC3)
- **Excluded (1):** Open (unfiltered observation)

There is one HST entry (F555W) included because it appeared in the scanned data. No
Generic fallback entries beyond `Generic_K` are included at this time — Generic entries
for other ambiguous single-letter aliases (e.g. `Generic_V`, `Generic_B`) are an
ADR-018 concern and will be added when the disambiguation algorithm defines which bands
require them.

**Completeness claim:** This population covers 100% of the filter strings observed in
the current catalog manifest. It does not claim coverage of filter strings that will
appear in future donated data. New entries are added via the mechanisms in Decisions 6
and 7 as new data arrives.

**Rationale:** The registry is data-driven, not speculative. We populated exactly the
entries the real data requires, seeded from SVO where coverage exists, and hand-authored
for the excluded and Generic cases where it doesn't. This avoids carrying hundreds of
SVO entries that no ingested file references.

---

## 4. Open Items

All items resolved. This section retained for historical reference.

| # | Item | Resolution |
|---|------|------------|
| 1 | SVO sync pattern (Decision 6) | Resolved: one-shot seed script, no recurring infrastructure. See Decision 6. |
| 2 | Physical similarity threshold X% (Decision 2) | Resolved: deferred post-MVP. At MVP scale, filter class membership is determined by operator domain expertise during the seed process. See Decision 2. |
| 3 | Physical similarity threshold Y% (Decision 2) | Resolved: deferred post-MVP. Same resolution as item 2. |
| 4 | Operator maintenance mechanisms (Decision 7) | Resolved: hand-edit JSON + validation script. See Decision 7. |
| 5 | Python interface contract (Decision 8) | Resolved: four-function read-only API. See Decision 8. |
| 6 | Versioning strategy (Decision 9) | Resolved: `_schema_version` semver field + Git history. See Decision 9. |
| 7 | Initial population scope (Decision 10) | Resolved: 23 entries from VizieR census. See Decision 10. |
| 8 | Confirm SVO `photometric_system` field maps cleanly to `SystemAbbrev` | Superseded (2026-03-25). `SystemAbbrev` derivation from `photometric_system` is abolished. `band_id` is now derived from SVO filter ID components per the ADR-017 amendment. |

---

## 5. Notes

**Note A — Fast path for excluded entry addition.** Decision 7 satisfies this
requirement. Excluded entries are hand-edited directly into `band_registry.json` — no
SVO queries, no schema expertise beyond following the existing excluded entry pattern,
no tooling beyond a text editor and the validation script.

**Note B — Unique filter string pre-resolution.** At ingestion time, the adapter should
identify the unique filter strings present in a file (typically tens, not thousands) and
resolve those once against the registry, then apply the results to all rows. This avoids
per-row registry lookups at scale. This is an ADR-018 / Epic D concern, but the
registry design must not preclude it.

---

## 6. Consequences

### 6.1 Immediate (Epic A scope)

- `excluded_filters.json` is deleted.
- New files created by Epic A:
  - `services/photometry_ingestor/band_registry/band_registry.json`
  - `services/photometry_ingestor/band_registry/__init__.py`
  - `services/photometry_ingestor/band_registry/registry.py`
  - `scripts/seed_band_registry.py`
  - `scripts/validate_band_registry.py`
- `_COMBINED_BAND_LOOKUP`, `_DEFAULT_PHOT_SYSTEM`, `_PHOT_SYSTEM_SPECTRAL_META` in
  `canonical_csv.py` are removed in Epic D (not Epic A).

### 6.2 Forward Dependencies

| Downstream artifact | Dependency on this ADR |
|---------------------|------------------------|
| **ADR-018** (Disambiguation Algorithm) | Receives the alias index interface and `disambiguation_hints` reservation. Defines the three-stage resolution funnel: (1) instrument context narrowing, (2) band name matching within candidate set, (3) Generic fallback. Defines escalation policy when no Generic entry exists. |
| **ADR-019** (Table Model Revision) | Inherits `regime` controlled vocabulary. Demotes `spectral_coord_value` from a required source field to a registry-derived field using `lambda_eff`. |
| **ADR-021** (Layer 0) | Replaces `synonyms.json` band alias lookups with registry alias lookups in the wide-format column detection pass. |
| **ADR-022** (ColorRow Design) | `ColorRow.band1_id` and `ColorRow.band2_id` are NovaCat canonical `band_id` strings resolved through this registry. |
| **Epic A** | Implements the registry artifact and operator scripts. |
| **Epic D** | Wires `CanonicalCsvAdapter._resolve_band()` to the registry interface. Removes all module-level band knowledge dicts. |
