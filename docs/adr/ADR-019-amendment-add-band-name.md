# ADR-019 Amendment: Add `band_name` to `PhotometryRow`

**Amendment date:** 2026-04-03
**Scope:** Decision 2 (band identity fields on `PhotometryRow`), §3 (field summary)
**Trigger:** Operational experience with the artifact generation pipeline revealed that
omitting a human-readable band label from the stored row creates a fragile runtime
dependency on the band registry for every read path, forces ad-hoc display-label
derivation in multiple downstream consumers, and hides the most scientifically
meaningful identifier — the canonical filter/band name — behind an opaque internal key.

---

## 1. Problem Statement

ADR-019 Decision 2 replaced `filter_name` with `band_id` and `regime` as the sole band
identity fields on `PhotometryRow`. The rationale was sound in principle: `band_id`
uniquely identifies a band registry entry, and all metadata — including the short display
label (`band_name`) — can be recovered from the registry at read time.

In practice, this decision has produced three categories of problems:

### 1.1 Fragile Runtime Coupling

Every consumer that needs to display a band label must load the band registry and
perform a lookup. This includes:

- **`generators/photometry.py`** — `_resolve_band()` looks up each `band_id` in the
  registry to extract `band_name` for the `photometry.json` artifact's `band` field.
- **`generators/bundle.py`** — `_aggregate_photometry_sources()` resorts to parsing
  `band_id` by splitting on `_` and taking the last segment, because it lacks registry
  access. This produces incorrect results for multi-segment band labels (e.g.,
  `Bessell_V` yields `V`, but `IRAC_I1` yields `I1` only by accident — `WFC3_UVIS2_F555W`
  would yield `F555W` but `Bessell_V` would yield `V` when the intent is to get the
  `band_name` from the registry, which for some entries may differ from the last `_`
  segment).
- **FITS bundle generation** (DESIGN-003 §10.5) — The `BAND_NAME` column in the
  photometry FITS table must be derived from the registry at generation time, despite
  being a static property of the observation that was known at ingestion.

If the registry is unavailable, stale, or inconsistent with the version used at
ingestion, every one of these consumers silently degrades.

### 1.2 Missing Public-Facing Identity

The `photometry.json` artifact — the primary public interface for photometry data —
uses a `band` field that carries the short display label (e.g., `"V"`, `"B"`,
`"UVW1"`). This is the identifier that astronomers recognise and expect. Yet this
value exists nowhere in the stored data; it is synthesised at generation time from
the internal `band_id`. The consequence is that the most scientifically important
identifier in the photometry table — the one that appears in papers, databases, and
observatory logs — is treated as a derived display concern rather than a first-class
data attribute.

### 1.3 The `filter_name` Removal Was Overcorrected

ADR-019 Decision 2 correctly identified that the *original source alias string* (the
raw column value from the ingested file) should not be stored on every row — that
belongs in the column mapping manifest. But `band_name` is not the source alias.
`band_name` is the **canonical, registry-defined** short label for the band (e.g.,
`"V"` for `Generic_V`, `"UVW2"` for `Swift_UVOT_UVW2`). It is a stable, curated
property of the band registry entry, not a raw input artifact. Dropping `filter_name`
was correct; failing to replace it with `band_name` was the error.

---

## 2. Amended Decision 2 — Add `band_name` to `PhotometryRow`

> **Amendment (2026-04-03):** `band_name` is added to `PhotometryRow` as a NOT NULL
> denormalized field. It is the canonical short display label for the photometric band,
> populated from the band registry entry's `band_name` field at ingestion time.

The band identity fields on `PhotometryRow` are now:

| Field | Type | Nullable | Description |
|---|---|---|---|
| `band_id` | `str` | NO | NovaCat canonical band ID resolved from the band registry (ADR-017). Internal identifier; not intended for public display. |
| `band_name` | `str` | NO | Canonical short display label for the band (e.g., `V`, `B`, `UVW1`, `5 GHz`, `0.3-10 keV`). Populated from the band registry entry's `band_name` field at ingestion time. **This is the default identifier for all public-facing outputs.** |
| `regime` | `str` | NO | Wavelength regime. Controlled vocabulary from ADR-017 §3.3. |

**Semantics:**

- `band_name` is the classic/canonical filter or band name as it appears in the
  astronomical literature. For optical/NIR bands this is the single-letter or
  short-code designation (e.g., `V`, `B`, `R`, `I`, `J`, `H`, `Ks`, `UVW1`,
  `F555W`). For radio it is the frequency designation (e.g., `5 GHz`). For X-ray
  and gamma-ray it is the energy range (e.g., `0.3-10 keV`).
- `band_name` is **not** the raw source alias from the ingested file. It is the
  curated, registry-authoritative display name. The raw source alias remains in the
  column mapping manifest per ADR-019 Decision 2 (unchanged).
- `band_name` is denormalized from the registry. If a band registry entry's
  `band_name` is later corrected, existing stored rows are **not** automatically
  updated. This is acceptable: the registry is expected to be stable after initial
  seeding, and a backfill migration can be run if corrections are needed.
- For excluded bands (where the band registry entry has `band_name: null`), the
  ingestion pipeline must not produce a `PhotometryRow` at all — excluded bands are
  rejected at the band resolution stage (ADR-018), so this case does not arise in
  practice. As a defensive measure, if a code path ever encounters a resolved entry
  with `band_name: null`, it should use the `band_id` as a fallback and log a warning.

**Population rule:** At ingestion time, after `band_id` is resolved via the band
registry (ADR-017/ADR-018), the adapter reads the `band_name` field from the resolved
`BandRegistryEntry` and stamps it onto the `PhotometryRow`. This applies to both the
ticket-driven path (DESIGN-004 `photometry_reader.py`) and the heuristic path
(`CanonicalCsvAdapter`).

**Public-facing default:** All public-facing outputs — `photometry.json`, the
photometry FITS table, the frontend, API responses — use `band_name` as the default
band identifier. `band_id` is available for internal traceability and cross-referencing
but is not the primary display field. Specifically:

- `photometry.json` `band` field on `BandRecord` and `ObservationRecord`: sourced from
  the stored `band_name`, not from a runtime registry lookup.
- Photometry FITS table `BAND_NAME` column (DESIGN-003 §10.5): sourced from the stored
  `band_name`.
- `generators/photometry.py` `_resolve_band()`: becomes a fallback for legacy rows that
  predate the migration. For rows with `band_name` populated, the generator reads it
  directly.
- `generators/bundle.py` `_aggregate_photometry_sources()`: reads `band_name` from the
  row instead of parsing `band_id`.

---

## 3. Amended Field Summary (§3 of ADR-019)

**Section 3 — Spectral / Bandpass Metadata** gains one field:

- `svo_filter_id` — carried forward
- `band_id` — carried forward (Decision 2)
- **`band_name` — new (this amendment)**
- `regime` — carried forward (Decision 2)
- `spectral_coord_type` — carried forward
- `spectral_coord_value` — carried forward (nullable, Decision 3)
- `spectral_coord_unit` — carried forward
- `bandpass_width` — carried forward

---

## 4. Schema Changes

### 4.1 `photometry_table_model.md`

Add `band_name` to the Section 3 table, immediately after `band_id`:

| Column | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `band_name` | TEXT | `instr.filter` | NO | Canonical short display label for the photometric band, sourced from the band registry entry's `band_name` field (ADR-017). The default identifier in all public-facing outputs. E.g., `V`, `B`, `UVW1`, `5 GHz`, `0.3-10 keV`. |

The schema version remains v2.0. This is an additive field addition within the same
major version; it does not alter the semantics of existing fields.

### 4.2 `contracts/models/entities.py`

Add `band_name` to the `PhotometryRow` model in Section 3 (Spectral / Bandpass
Metadata), after `band_id`:

```python
band_name: str = Field(
    ...,
    min_length=1,
    max_length=256,
    description=(
        "Canonical short display label for the band (e.g., 'V', 'B', 'UVW1'). "
        "Populated from the band registry entry's band_name field at ingestion time. "
        "Default identifier for all public-facing outputs."
    ),
)
```

### 4.3 `BandRegistryEntry` (contracts)

No changes required. The `BandRegistryEntry` model in `entities.py` already carries
`band_name: str | None`. The registry module's `BandRegistryEntry` in
`services/photometry_ingestor/band_registry/registry.py` also already carries
`band_name: str | None`. Both are read-only sources; nothing changes on the registry
side.

---

## 5. Downstream Impact

### 5.1 Ingestion Code

| File | Change |
|---|---|
| `services/ticket_ingestor/photometry_reader.py` | Add `band_name=entry.band_name` (with fallback to `band_id`) to the `PhotometryRow` constructor in `_transform_row()`. |
| `services/photometry_ingestor/adapters/canonical_csv.py` | Same: populate `band_name` from the resolved registry entry in the adaptation loop. |

### 5.2 Artifact Generation

| File | Change |
|---|---|
| `services/artifact_generator/generators/photometry.py` | Read `band_name` directly from the DDB row. Retain `_resolve_band()` as a fallback for rows where `band_name` is absent (pre-migration data). |
| `services/artifact_generator/generators/bundle.py` | Read `band_name` from the row in `_aggregate_photometry_sources()` instead of parsing `band_id`. |

### 5.3 Frontend

| File | Change |
|---|---|
| `frontend/src/types/photometry.ts` | No change required. The `BandRecord.band` and `ObservationRecord.band` fields already carry the display label; the change is purely in how the backend populates them. |

### 5.4 Specifications and Documentation

| File | Change |
|---|---|
| `docs/specs/photometry_table_model.md` | Add `band_name` to Section 3 table per §4.1. |
| `docs/design/DESIGN-003-artifact-regeneration-pipeline.md` | Update §8.2 (inputs) and §8.4 (band display label resolution) to note that `band_name` is read from the stored row, with registry lookup as fallback. |
| `docs/design/DESIGN-004-source-profile-schema-and-ticket-driven-ingestion.md` | Note `band_name` population in the photometry reader description. |

### 5.5 Tests

| File | Change |
|---|---|
| `tests/services/test_generators_photometry.py` | Add `band_name` to test DDB items alongside `band_id`. Verify that the generator reads `band_name` directly when present. |
| `tests/services/test_ticket_ingestor_photometry.py` | Verify `band_name` is populated on constructed `PhotometryRow` instances. |

### 5.6 DynamoDB Migration

Existing rows in the dedicated photometry DynamoDB table do not carry `band_name`.
Two strategies are available:

1. **Backfill migration** (recommended): A one-time script scans all `PHOT#` items,
   resolves `band_id` against the current registry, and writes `band_name` to each
   item. This is safe because the registry is append-only and existing `band_id` values
   are stable.
2. **Read-time fallback**: The artifact generator's existing `_resolve_band()` pattern
   serves as a graceful fallback for rows without `band_name`. This means the system
   works correctly before and after migration, with the migration improving performance
   and eliminating the runtime registry dependency.

Both strategies should be employed: the fallback ensures zero-downtime deployment, and
the backfill ensures convergence to the target state.

---

## 6. What This Amendment Does Not Change

- `band_id` remains the internal canonical identifier and the DynamoDB sort key
  component. It is not demoted or removed.
- The band registry schema is unchanged. `band_name` on `BandRegistryEntry` already
  exists.
- The alias resolution algorithm (ADR-018) is unchanged.
- The `regime` field on `PhotometryRow` is unchanged.
- The column mapping manifest (which captures the raw source alias) is unchanged.
- The `filter_name` field remains dropped. `band_name` is not a reinstatement of
  `filter_name`; it is a different field with different semantics (registry-authoritative
  vs. raw source input).

---

## Links

- ADR-017 — Band Registry Design (`band_name` field on registry entries)
- ADR-018 — Band Disambiguation Algorithm (resolution produces a `BandRegistryEntry`
  from which `band_name` is read)
- ADR-019 — Photometry Table Model Revision (this amendment's parent)
- ADR-020 — Photometry Storage Format (DynamoDB schema; `band_name` becomes a new
  top-level attribute on `PHOT#` items)
- DESIGN-003 — Artifact Regeneration Pipeline (§8 photometry generator, §10.5
  photometry FITS table)
- DESIGN-004 — Source Profile Schema and Ticket-Driven Ingestion (photometry reader)
