# ADR-019: Photometry Table Model Revision

**Status:** Accepted
**Date:** 2026-03-25
**Author:** TF
**Supersedes:** —
**Superseded by:** —
**Amends:** —
**Relates to:**
- `docs/specs/photometry_table_model.md` v2.0 — authoritative schema output of this ADR
- `contracts/models/entities.py` — Pydantic contract revised per this ADR
- `DESIGN-001` §5 Epic C — authoritative design basis
- `DESIGN-002` §4.2, §4.3 — provenance field inputs (Tier 1 and Tier 2, now Type)
- `ADR-017` — Band Registry Design; `band_id` and `regime` vocabulary inherited here
- `ADR-018` — Band Disambiguation Algorithm; resolution provenance field vocabulary
  adopted here; Tier → Type rename formally recorded here
- `ADR-020` — Canonical Persistence Format; flat-column storage decision confirmed here
- `ADR-021` — Layer 0 Pre-Ingestion Normalization; `spectral_coord_value` demotion
  constraint established there, adopted here
- `epic/22-photometry-doc-reconciliation` — reconciliation pass that produced the
  decisions adopted in this ADR

---

## 1. Context

`photometry_table_model.md` v1.1 and the corresponding `PhotometryRow` Pydantic model
in `contracts/models/entities.py` were authored before the band registry design (ADR-017),
the disambiguation algorithm (ADR-018), the provenance framework (DESIGN-002), and the
Layer 0 normalization design (ADR-021) were complete. As those downstream decisions
accumulated, a reconciliation pass (`epic/22-photometry-doc-reconciliation`) identified
twelve categories of divergence between the documentation corpus and the accumulated
design decisions.

This ADR formally adopts the schema decisions produced by that reconciliation pass,
resolves the remaining open questions identified in DESIGN-001 Epic C, and produces
`photometry_table_model.md` v2.0 as its primary output.

### 1.1 What This ADR Does Not Cover

- `ColorRow` schema — ADR-022
- `CanonicalCsvAdapter` rewire to the revised schema — Epic D
- Removal of `spectral_coord_value` from `_REQUIRED_SOURCE_FIELDS` — Epic D
- `BandEntry` Pydantic model for the band registry — Epic A
- ADR-020 narrative cleanup — deferred until ADR-020 is otherwise complete

---

## 2. Decisions

### Decision 1 — Drop the Photometric System Concept System-Wide

The `PhotSystem` enum and all `phot_system` fields are removed from `PhotometryRow`,
`photometry_table_model.md`, `entities.py`, and the band registry entry schema.

**Rationale.** With `band_id` as the canonical band identifier, `phot_system` is fully
redundant. Every scientifically meaningful attribute of a photometric system — spectral
coordinates, calibration zero points, detector type — is carried by the band registry
entry resolved via `band_id`. The field was also being used as an attempted
disambiguation signal (ADR-016), a role explicitly stripped from it in ADR-018 Decision
3. Retaining a field that is both redundant and stripped of its only active role adds
confusion without adding information.

**Downstream consequence.** The `ColorRow` `phot_system` field and its cross-field
invariant (non-NULL for `magnitude_difference`, NULL otherwise) must be revised in
ADR-022.

---

### Decision 2 — Add `band_id` and `regime` to `PhotometryRow`

`band_id` (NOT NULL) and `regime` (NOT NULL) replace `filter_name` and `phot_system`
as the primary band identity fields on `PhotometryRow`.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `band_id` | `str` | NO | NovaCat canonical band ID resolved from the band registry (ADR-017). |
| `regime` | `str` | NO | Wavelength regime. Controlled vocabulary from ADR-017 §3.3: `optical`, `uv`, `nir`, `mir`, `radio`, `xray`, `gamma`. |

`filter_name` is dropped. The original alias string from the source file is captured in
the per-ingestion column mapping manifest (pointed to by `column_mapping_manifest_s3_key`
on the DataProduct envelope item) rather than on every row.

`zero_point_flux` and `mag_system` are also dropped from `PhotometryRow`. Both belong
exclusively in the band registry entry, not on individual measurement rows.

---

### Decision 3 — Demote `spectral_coord_value` to Nullable

`spectral_coord_value` is demoted from a required source field to a registry-derived
field. The adapter populates it from the resolved band entry's `lambda_eff` when not
supplied by the source file. It is nullable to accommodate sparse registry entries with
no `lambda_eff`.

This decision was established as a constraint in ADR-017 §6.2 and ADR-021 §9.2. This
ADR formally adopts it into the schema.

---

### Decision 4 — Add Resolution Provenance Fields (Type Model)

Every stored `PhotometryRow` carries three resolution provenance fields populated by
the adapter at ingestion time.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `band_resolution_type` | `BandResolutionType` | NO | Mechanism by which `band_id` was resolved. |
| `band_resolution_confidence` | `BandResolutionConfidence` | NO | Trustworthiness of the resolution result. |
| `sidecar_contributed` | `bool` | NO | True if any sidecar field influenced band resolution. Default `False`. |

**Controlled vocabularies (from ADR-018 Decision 6):**

`BandResolutionType`: `canonical`, `synonym`, `generic_fallback`, `sidecar_assertion`

`BandResolutionConfidence`: `high`, `medium`, `low`

#### Formal Record: Tier → Type Rename

DESIGN-002 §4.3 originally proposed a `band_resolution_tier` field with a
tier-based vocabulary (`tier1_canonical`, `tier2_synonym`, etc.). ADR-018 Decision 6
superseded that vocabulary with the Type model adopted here.

**Rationale for the rename.** The tier vocabulary implied a confidence ordering that did
not hold in practice: some resolutions from what would have been Tier 1 (canonical alias
match) carry only medium confidence when no corroborating context signal is present,
while some resolutions from what would have been Tier 2 (synonym disambiguation) carry
high confidence when instrument context is decisive. The Type vocabulary describes the
*mechanism* of resolution without implying a confidence ranking, which is instead
captured independently by `band_resolution_confidence`. This separation is cleaner and
more useful to downstream consumers.

---

### Decision 5 — Add Data Origin and Donor Attribution Fields

Two Tier 1 provenance additions from DESIGN-002 §4.2 are adopted as flat columns on
`PhotometryRow`.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `data_origin` | `DataOrigin` | NO | Origin of this row. Values: `literature`, `operator_upload`, `donor_submission`. Default `literature`. |
| `donor_attribution` | `str` | YES | Free-text attribution for donated data. NULL for literature rows. Max 512 characters. |

These fields enable downstream filtering by data origin and support the donation pathway
forward-compatibility requirements established in DESIGN-002 §6.

---

### Decision 6 — Provenance Fields Stored as Flat Columns

Resolution provenance fields (`band_resolution_type`, `band_resolution_confidence`,
`sidecar_contributed`) and data origin fields (`data_origin`, `donor_attribution`) are
stored as flat columns on the DynamoDB `PhotometryRow` item, not as a nested JSON blob.

**Rationale.** DynamoDB cannot filter on or index into nested JSON attributes without
application-layer deserialization. Flat columns preserve DynamoDB's native query
capabilities, keep items self-describing when inspected via operator tooling, and are
consistent with the rest of the `PhotometryRow` item schema. The `resolution_meta`
nested approach proposed as a possibility in DESIGN-002 §4.3 is rejected.

---

### Decision 7 — Upper Limit Model Is Sufficient Across All Regimes

The existing `is_upper_limit` / `limiting_value` model requires no revision for
multi-regime coverage.

**Rationale.** `limiting_value` carries an upper bound on the primary measurement
quantity for the row. The unit of that bound is already unambiguous:
- For optical/UV/NIR rows where `magnitude` is the primary quantity: `limiting_value`
  is in magnitudes — no unit field needed.
- For radio/X-ray/gamma rows where `flux_density` is the primary quantity:
  `limiting_value` is a flux density bound in the unit given by `flux_density_unit`,
  which is already required when `flux_density` is non-NULL.
- For X-ray count rate rows where `count_rate` is the primary quantity: `limiting_value`
  is in `s⁻¹`, which is the universal convention for count rates.

No additional unit field is required. The cross-field invariants in `PhotometryRow` are
sufficient to make the unit of `limiting_value` unambiguous in all cases.

---

### Decision 8 — Add Gamma Regime; Extend Unit Vocabularies

The gamma regime is added to the `regime` controlled vocabulary and cross-regime
guidance table. Two unit enums are extended:

**`SpectralCoordUnit` additions:** `MeV`, `GeV` (gamma-ray spectral coordinates).
`MeV` is the default for gamma-ray entries.

**`FluxDensityUnit` addition:** `photons/cm2/s` (gamma-ray photon flux). Gamma-ray
flux is conventionally reported as a photon flux rather than a flux density per unit
energy; this unit accommodates that convention within the existing `flux_density` field.

---

## 3. Revised `PhotometryRow` Field Summary

The complete field list for `PhotometryRow` v2.0, grouped by section. Fields carried
forward unchanged from v1.1 are noted; only changes are described in detail above.

**Section 1 — Identity** (unchanged): `nova_id`, `primary_name`, `ra_deg`, `dec_deg`

**Section 2 — Temporal** (unchanged): `time_mjd`, `time_bary_corr`, `time_orig`,
`time_orig_sys`

**Section 3 — Spectral / Bandpass Metadata:**
- `svo_filter_id` — carried forward
- `band_id` — **new** (Decision 2)
- `regime` — **new** (Decision 2)
- `spectral_coord_type` — carried forward
- `spectral_coord_value` — **demoted to nullable** (Decision 3)
- `spectral_coord_unit` — carried forward; **extended** with `MeV`, `GeV` (Decision 8)
- `bandpass_width` — carried forward
- **Dropped:** `filter_name`, `phot_system`, `mag_system`, `zero_point_flux`

**Section 4 — Photometric Measurement** (unchanged): `magnitude`, `mag_err`,
`flux_density`, `flux_density_err`, `flux_density_unit` (**extended** with
`photons/cm2/s`), `count_rate`, `count_rate_err`, `is_upper_limit`, `limiting_value`,
`limiting_sigma`, `quality_flag`, `notes`

**Section 5 — Provenance:**
- Carried forward: `bibcode`, `doi`, `data_url`, `orig_catalog`, `orig_table_ref`,
  `telescope`, `instrument`, `observer`, `data_rights`
- **New:** `band_resolution_type`, `band_resolution_confidence`, `sidecar_contributed`
  (Decision 4); `data_origin`, `donor_attribution` (Decision 5)

---

## 4. New and Revised Enums in `entities.py`

| Enum | Change |
|---|---|
| `PhotSystem` | **Deleted entirely** |
| `BandResolutionType` | **New** — `canonical`, `synonym`, `generic_fallback`, `sidecar_assertion` |
| `BandResolutionConfidence` | **New** — `high`, `medium`, `low` |
| `DataOrigin` | **New** — `literature`, `operator_upload`, `donor_submission` |
| `SpectralCoordUnit` | **Extended** — add `MeV`, `GeV` |
| `FluxDensityUnit` | **Extended** — add `photons/cm2/s` |
| `PhotometryQuarantineReasonCode` | **Extended** — add `UNRECOGNIZED_BAND_STRING`, `CONFLICTING_BAND_CONTEXT`, `BAND_CONTEXT_EXCLUDES_ALL_CANDIDATES`, `AMBIGUOUS_BAND_UNRESOLVABLE` |
| `ConflictClass` | **Extended** — add `BAND_SIGNAL_CONFLICT` |

---

## 5. Consequences

- `photometry_table_model.md` is versioned to v2.0. The v1.1 file is archived at
  `docs/specs/archive/photometry_table_model_v1.1.md` per ADR-030 Decision 2.
- `contracts/models/entities.py` is updated per §4 above. All changes must pass
  `mypy --strict` and `ruff check` before merge.
- `CanonicalCsvAdapter` is not updated in this epic. The adapter continues to target
  the v1.1 schema until Epic D. The `_REQUIRED_SOURCE_FIELDS` frozenset in
  `canonical_csv.py` still includes `spectral_coord_value` until Epic D removes it.
- `ColorRow.phot_system` and its cross-field invariant are revised in ADR-022.
- The band registry (`band_registry.json`, `seed_band_registry.py`) no longer carries
  `photometric_system`; the registry must be reseeded after `seed_band_registry.py`
  is updated.
- Downstream consumers of `PhotometryRow` (bundle generation, frontend artifact
  schemas) must be updated to use `band_id` and `regime` in place of `filter_name`
  and `phot_system`.
