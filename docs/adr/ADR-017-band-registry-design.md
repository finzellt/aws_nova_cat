# ADR-017: Band Registry Design

**Status:** Draft (incomplete — see Open Items §4)
**Date:** 2026-03-21
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

**SVO sync pattern.** *(Deferred — see Open Items §4.)*

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

**Physical similarity thresholds.** *(Partially deferred — see Open Items §4.)* Two
filters belong to the same class if their central wavelengths are within X% and their
bandwidths are within Y% of each other. X and Y are empirical thresholds to be
determined from analysis of the full SVO dataset. Both are currently open parameters
pending TF's analysis.

**Generic entries.** `Generic_<BandLabel>` entries (e.g. `Generic_V`) are a first-class
concept in the registry. They represent cases where the band identity is known but the
specific photometric system or instrument is not. They are the conservative fallback
when disambiguation cannot resolve to a more specific filter class. See ADR-018 for the
disambiguation algorithm that governs when Generic entries are selected.

---

### Decision 3 — Registry Entry Schema

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

**Example entry (legitimate band):**

```json
{
  "band_id": "Johnson_V",
  "svo_filter_id": "Generic/Johnson.V",
  "photometric_system": "Johnson",
  "band_name": "V",
  "regime": "optical",
  "detector_type": "photon",
  "observatory_facility": "Generic",
  "instrument": null,
  "aliases": ["Johnson_V", "V", "Johnson V", "Vmag"],
  "excluded": false,
  "exclusion_reason": null,
  "lambda_eff": 5512.0,
  "lambda_pivot": 5482.0,
  "lambda_min": 4750.0,
  "lambda_max": 7000.0,
  "fwhm": 827.0,
  "effective_width": 756.0,
  "calibration": {
    "vega": {
      "zero_point_flux_lambda": 3.636e-9,
      "zero_point_flux_nu": 3636.0,
      "zeropoint_type": "Pogson",
      "photcal_id": "Generic/Johnson.V/Vega"
    },
    "ab": null,
    "st": null
  },
  "disambiguation_hints": {}
}
```

**Example entry (Generic fallback):**

```json
{
  "band_id": "Generic_V",
  "svo_filter_id": null,
  "photometric_system": null,
  "band_name": "V",
  "regime": "optical",
  "detector_type": null,
  "observatory_facility": null,
  "instrument": null,
  "aliases": ["Generic_V", "V"],
  "excluded": false,
  "exclusion_reason": null,
  "lambda_eff": 5500.0,
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

**Example entry (excluded mode):**

```json
{
  "band_id": "AAVSO_Vis",
  "svo_filter_id": null,
  "photometric_system": null,
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
| `photometric_system` | string | Yes | From SVO `Phot.System`. Drives `SystemAbbrev`. `null` for excluded and Generic entries. |
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

| String | Correct band |
|--------|-------------|
| `V` | `Johnson_V` (or `Generic_V`) |
| `i` | `Sloan_i` |
| `I` | `Cousins_I` |
| `Ks` | `2MASS_Ks` |

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

### Decision 6 — SVO Sync Pattern

*(Deferred — blocked on SVO SQLite database infrastructure currently under development.
See Open Items §4.)*

---

### Decision 7 — Operator Maintenance Mechanisms

*(Deferred — see Open Items §4.)*

---

### Decision 8 — Python Interface Contract

*(Deferred — see Open Items §4.)*

---

### Decision 9 — Versioning Strategy

*(Deferred — see Open Items §4.)*

---

### Decision 10 — Initial Population Scope

*(Deferred — see Open Items §4.)*

---

## 4. Open Items

The following items must be resolved before this ADR can be marked `Accepted`.

| # | Item | Notes |
|---|------|-------|
| 1 | SVO sync pattern (Decision 6) | Blocked on SVO SQLite database infrastructure currently under development |
| 2 | Physical similarity threshold X% (Decision 2) | Pending TF's analysis of full SVO dataset |
| 3 | Physical similarity threshold Y% (Decision 2) | Pending TF's analysis of full SVO dataset |
| 4 | Operator maintenance mechanisms (Decision 7) | To be designed in next session |
| 5 | Python interface contract (Decision 8) | To be designed in next session |
| 6 | Versioning strategy (Decision 9) | To be designed in next session |
| 7 | Initial population scope (Decision 10) | To be designed in next session |
| 8 | Confirm SVO `photometric_system` field is reliably populated and maps cleanly to `SystemAbbrev` | Pending TF's exploration of the harvested SVO dataset |

---

## 5. Notes

**Note A — Fast path for excluded entry addition.** The operator maintenance mechanism
(Decision 7) must include a fast, low-friction path for adding new excluded entries by
hand, without requiring SVO queries or schema expertise. This is expected to be a
frequent operation as new non-photometric observation modes are encountered in real data.

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
  - `scripts/sync_band_registry_svo.py` *(implementation deferred pending Decision 6)*
  - `scripts/add_excluded_band.py` *(implementation deferred pending Decision 7)*
  - `scripts/validate_band_registry.py` *(implementation deferred pending Decision 7)*
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
