# ADR-017 Amendment: Band ID Naming Convention Revision

**Amendment date:** 2026-03-25 (alias ownership correction: 2026-04-01)
**Scope:** Decision 2 (Canonical Band ID Naming Convention), Decision 3 (examples),
Decision 4 (example table), Decision 10 (initial population)
**Trigger:** ADR-019 abolished `photometric_system` system-wide; `SystemAbbrev`
derivation anchor no longer exists. Additionally, `photometric_system` was never a
valid uniqueness basis — multiple facilities can implement the same photometric standard
with measurably different filter profiles (e.g. SLOAN/SDSS.g vs. FLWO/KeplerCam.sdss_g
differ by ~15% in effective width for g).

---

> ## ⚠️ ALIAS OWNERSHIP INVARIANT — READ THIS FIRST ⚠️
>
> **Bare single-letter aliases (`U`, `B`, `V`, `R`, `I`, `J`, `H`, `K`) MUST live
> on `Generic_*` entries, NEVER on instrument-specific entries.**
>
> This is a **binding invariant** of the registry. It was violated in the initial
> seed (2026-03-27) and corrected on 2026-04-01. The violation caused incoming
> photometry with unknown instrument provenance to be labeled with instrument-specific
> `band_id` values (e.g. `HCT_HFOSC_Bessell_V`) and `band_resolution_confidence:
> "high"` — both scientifically incorrect.
>
> **Why this matters:**
>
> - A researcher seeing `band_id: HCT_HFOSC_Bessell_V` reasonably concludes the
>   observation was taken with the HFOSC instrument on the Himalayan Chandra Telescope.
>   If the data is actually AAVSO photometry with unknown provenance, that conclusion
>   is false.
> - The `canonical` / `high` resolution metadata makes it worse: it signals "we're
>   sure about this instrument identification" when in fact we have no instrument
>   information at all.
> - The Generic fallback path exists precisely to say "we know this is V-band but we
>   don't know which instrument" with `generic_fallback` / `low` confidence.
>
> **The rule:**
>
> | Alias type | Must be owned by | Example |
> |---|---|---|
> | Bare single letter (`V`) | `Generic_V` | Ambiguous — no instrument claim |
> | Legacy system name (`Johnson_V`) | `Generic_V` | No instrument claim |
> | Combined string (`Johnson V`, `Vmag`) | `Generic_V` | No instrument claim |
> | Instrument-qualified (`HCT_HFOSC_Bessell_V`) | `HCT_HFOSC_Bessell_V` | Specific instrument |
>
> **Enforcement:** The alias uniqueness constraint in `registry.py` (ADR-017
> Decision 8) prevents two entries from sharing an alias. The `band_specs.json`
> data file and the `load_band_specs()` loader both validate this constraint.
> Any PR that moves a bare alias to an instrument-specific entry MUST be rejected.
>
> **See also:**
> - ADR-018 Decision 5 (Generic fallback exit path)
> - DESIGN-004 §6.5 (ticket-driven two-step resolution)
> - `tools/filter_band_reg/band_specs.json` (single source of truth for alias
>   assignments)

---

## Amended Decision 2 — Canonical Band ID Naming Convention

> **Amendment (ADR-019 reconciliation):** The `{SystemAbbrev}_{BandLabel}` convention
> is replaced. `SystemAbbrev` was derived from SVO's `photometric_system` field, which
> ADR-019 Decision 1 abolished system-wide. More fundamentally, `photometric_system`
> was never a valid uniqueness basis: multiple facilities implement the same photometric
> standard with measurably different filter profiles. The revised convention derives
> `band_id` from SVO filter ID components (facility, instrument, band label), ensuring
> each non-Generic entry identifies a **specific filter profile**, not an abstract
> filter class.

Each registry entry carries a `band_id`: a stable, unique NovaCat identifier for the
filter profile. It is the primary reference that `PhotometryRow` and `ColorRow` fields
resolve to.

**Two-track convention:**

#### Track 1 — Instrument-Specific Entries

Entries backed by a specific SVO filter profile. `band_id` is mechanically derived from
the SVO filter ID components.

**Format:**

```
{Facility}_{Instrument}_{BandLabel}
```

**Derivation rules:**

- Components are derived from the matched SVO filter ID. Given a filter ID of the form
  `Facility/Instrument.BandLabel`, the `band_id` is formed by joining the components
  with underscores.
- **Redundancy collapsing:** When `Facility` and `Instrument` are identical (e.g.
  `2MASS/2MASS.Ks`), collapse to `{Facility}_{BandLabel}` — yielding `2MASS_Ks`.
- `BandLabel` is taken from the SVO filter ID path component after the dot, preserving
  its exact form including any prefixes (e.g. `Bessell_V` from `HCT/HFOSC.Bessell_V`,
  `I1` from `Spitzer/IRAC.I1`).
- Dots in the SVO path become underscores in `band_id`. All other punctuation
  (slashes, spaces) is replaced by underscores.
- `band_id` values remain Python-identifier-safe: underscores only, no slashes, dots,
  spaces, or other punctuation.

**Rationale:** Each non-Generic registry entry represents a **specific filter profile**
— a particular facility/instrument combination with a particular transmission curve —
not an abstract filter class. Different instruments implementing the same photometric
standard have measurably different bandpasses and must be tracked as distinct entries.
Deriving `band_id` mechanically from SVO filter ID components ensures the identifier
is unambiguous, traceable to its SVO source, and does not depend on any abstract
classification system.

**Alias ownership:** Instrument-specific entries carry **only** their own `band_id` as
an alias. They do **not** carry bare single-letter aliases, legacy system names, or
combined strings. See the Alias Ownership Invariant at the top of this document.

**Examples:**

| SVO filter ID | `band_id` | Aliases | Notes |
|---|---|---|---|
| `HCT/HFOSC.Bessell_V` | `HCT_HFOSC_Bessell_V` | `[HCT_HFOSC_Bessell_V]` | Instrument-only alias |
| `OAF/Bessell.V` | `OAF_Bessell_V` | `[OAF_Bessell_V]` | Instrument-only alias |
| `SLOAN/SDSS.g` | `SLOAN_SDSS_g` | `[SLOAN_SDSS_g, Sloan_g, g]` | `g` is unambiguous (case-sensitive) |
| `Swift/UVOT.UVW1` | `Swift_UVOT_UVW1` | `[Swift_UVOT_UVW1, UVOT_UVW1, uvw1]` | UV designations are unambiguous |
| `2MASS/2MASS.Ks` | `2MASS_Ks` | `[2MASS_Ks, Ks]` | Redundancy collapse; `Ks` is unambiguous |
| `Spitzer/IRAC.I1` | `Spitzer_IRAC_I1` | `[Spitzer_IRAC_I1, Spitzer_IRAC1, [3.6], 3.6]` | MIR designations are unambiguous |
| `HST/WFC3_UVIS2.F555W` | `HST_WFC3_UVIS2_F555W` | `[HST_WFC3_UVIS2_F555W, HST_F555W, F555W]` | HST filter codes are unambiguous |

#### Track 2 — Generic Fallback Entries

Entries where the band identity is known but the specific instrument/facility is not.
These are **strictly fallback** entries, used only when the disambiguation algorithm
(ADR-018) cannot resolve to an instrument-specific profile.

**Format:**

```
Generic_{BandLabel}
```

**Rules:**

- Generic entries carry reference spectral data from the canonical SVO profile for
  their photometric system (see "Reference Profile Convention" below). They are **not**
  sparse — they carry `svo_filter_id`, `lambda_eff`, `fwhm`, calibration data, etc.
  sourced from the reference profile.
- `observatory_facility` and `instrument` on Generic entries are always `null`. The
  `svo_filter_id` documents which SVO profile supplied the reference data, but Generic
  entries make no claim about the observing instrument.
- Any row resolved to a Generic entry receives `band_resolution_confidence: "low"`
  per ADR-018 Decision 6.
- Generic entries exist for every band label where alias ambiguity could prevent
  resolution to a specific profile (e.g. `Generic_V`, `Generic_K`, `Generic_B`).
- The `Generic_` prefix is self-documenting — downstream consumers can identify
  fallback resolutions without additional metadata.
- **Generic entries own all bare ambiguous aliases.** See the Alias Ownership Invariant
  at the top of this document.

**Reference Profile Convention:**

| Regime | Reference SVO profiles | Rationale |
|---|---|---|
| Optical (UBVRI) | `HCT/HFOSC.Bessell_*` | Bessell (1990) defines the canonical UBVRI transmission curves that operationally are the Johnson-Cousins system in modern CCD photometry. The HCT/HFOSC implementation of Bessell filters in the SVO is used as the reference. |
| NIR (J, H) | `2MASS/2MASS.*` | 2MASS is the de facto standard NIR photometric system for nova photometry. |
| NIR (K) | Sparse (no reference) | Johnson K and 2MASS Ks are scientifically distinct bands (band_grouping_rules.md Rule S3). Using either as a reference for the ambiguous bare `K` would be incorrect. `Generic_K` retains a `lambda_eff` hint of 21900 Å only. |

**Rationale:** Filters with the same name or photometric system are not necessarily
the same in practice. Identifying the specific filter profile is critical for
scientifically defensible photometry. Generic entries are the conservative fallback
when that identification fails, and their low-confidence labeling ensures researchers
know the limitation.

#### Rules (unchanged from original)

- Components are separated by a single underscore (`_`).
- No spaces, slashes, dots, or other punctuation. `band_id` values are
  Python-identifier-safe.
- `BandLabel` preserves its conventional casing (e.g. `V`, `g`, `Ks`, `NUV`).
- `band_id` must appear as the first element of the entry's `aliases` list.
- `band_id` values must be globally unique within the registry.

#### Superseded concepts

The following concepts from the original Decision 2 are removed:

- **`SystemAbbrev`** — no longer used. Replaced by mechanical SVO-derived components.
- **Filter class** — the concept of an abstract grouping of "physically similar"
  filters is removed from the registry model. Each non-Generic entry is a specific
  filter profile. Whether two profiles are similar enough to combine on a light curve
  is a downstream scientific judgment, not an ingestion-time classification.
- **Physical similarity thresholds** — removed. No longer relevant without filter
  classes.
- **`photometric_system` derivation** — removed. `photometric_system` is abolished
  per ADR-019 Decision 1.
- **Sparse Generic entries** — the original amendment described Generic entries as
  "intentionally sparse" with all SVO-derived fields `null`. This is superseded by
  the Reference Profile Convention above (correction 2026-04-01).

---

## Amended Decision 3 — Registry Entry Schema (examples only)

> **Amendment (band_id naming revision):** Examples updated to reflect the new
> two-track naming convention. The `photometric_system` field removal (from the
> `epic/22-photometry-doc-reconciliation` amendment) remains in effect. The schema
> field table is unchanged.
>
> **Amendment (alias ownership correction, 2026-04-01):** Examples updated to reflect
> corrected alias ownership. Generic_V now owns bare aliases and carries Bessell
> reference spectral data. HCT_HFOSC_Bessell_V carries only its instrument-specific
> alias.

**The first paragraph of Decision 3 is revised from:**

> Each entry represents a **filter class** — an abstract grouping of physically similar
> filters that are scientifically comparable. Specific instrument filter details are not
> stored as separate entities; they are captured via the `svo_filter_id` cross-reference
> and the SVO-derived spectral fields.

**To:**

> Each entry represents either a **specific filter profile** (instrument-specific entry)
> or a **fallback band identity** (Generic entry). Instrument-specific entries are backed
> by a specific SVO filter profile and carry full spectral data. Generic entries carry
> reference spectral data from a canonical SVO profile for their photometric system and
> serve as low-confidence fallbacks when disambiguation cannot resolve to a specific
> profile.

**Example entry (instrument-specific band):**

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

Note: `Generic_V` and `HCT_HFOSC_Bessell_V` share the same spectral data (both sourced
from `HCT/HFOSC.Bessell_V`), but differ in three critical ways: (1) `Generic_V` owns
the bare alias `"V"` while `HCT_HFOSC_Bessell_V` does not; (2) `Generic_V` has
`observatory_facility: null` and `instrument: null` — it makes no instrument claim;
(3) resolution to `Generic_V` produces `band_resolution_confidence: "low"` while
resolution to `HCT_HFOSC_Bessell_V` produces `"high"`.

**Example entry (excluded mode) — unchanged:**

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

---

## Amended Decision 4 — Alias Matching (example table only)

> **Amendment (alias ownership correction, 2026-04-01):** The example table is updated
> to reflect corrected alias ownership. Bare single-letter aliases now resolve to
> Generic entries, not instrument-specific entries.

The case-sensitive matching policy is unchanged. The example table is updated to
reflect the corrected alias assignments:

| String | Correct band | Resolution type |
|--------|-------------|-----------------|
| `V` | `Generic_V` | `generic_fallback` / `low` |
| `Johnson_V` | `Generic_V` | `canonical` / `high` |
| `HCT_HFOSC_Bessell_V` | `HCT_HFOSC_Bessell_V` | `canonical` / `high` |
| `i` | `SLOAN_SDSS_i` | `canonical` / `high` |
| `I` | `Generic_I` | `generic_fallback` / `low` |
| `Ks` | `2MASS_Ks` | `canonical` / `high` |
| `K` | `Generic_K` | `generic_fallback` / `low` |

Note: the `photometry_reader._resolve_band()` two-step (DESIGN-004 §6.5) means that
`"V"` hits the alias index in Step 1 and resolves directly to `Generic_V` as a
`canonical` match. However, because `Generic_V` starts with `Generic_`, the
`photometry_reader` assigns `generic_fallback` / `low` provenance regardless of
how the alias was found. The alias ownership invariant ensures this is the correct
outcome for bare ambiguous aliases.

---

## Amended Decision 10 — Initial Population

> **Amendment (band_id naming revision):** Entry count increases from 25 to 32.
> Instrument-specific entries now use SVO-derived `band_id` values. Generic fallback
> entries added for all ambiguous single-letter band aliases per ADR-018 requirements.
>
> **Amendment (alias ownership correction, 2026-04-01):** Bare aliases moved from
> instrument-specific entries to Generic entries. Generic optical entries populated
> with Bessell reference spectral data; Generic NIR J/H entries populated with 2MASS
> reference data. Band definitions extracted from seed script to
> `tools/filter_band_reg/band_specs.json`.

The initial registry population is the 32 entries produced by the seed script from
`band_specs.json`.

**Coverage by category:**

**Instrument-specific entries (23):**

- **Optical — Bessell (5):** HCT_HFOSC_Bessell_U, _B, _V, _R, _I
  (alias: `band_id` only — no bare letter aliases)
- **Optical — Sloan unprimed (5):** SLOAN_SDSS_u, _g, _r, _i, _z
  (bare lowercase aliases retained — unambiguous due to case sensitivity)
- **Optical — Sloan primed (4):** SLOAN_SDSS_up, _gp, _rp, _ip
- **NIR (3):** 2MASS_J, _H, _Ks
  (bare `J` and `H` moved to Generic; `Ks` retained — unambiguous)
- **UV (3):** Swift_UVOT_UVW1, _UVW2, _UVM2
- **MIR (2):** Spitzer_IRAC_I1, _I2
- **HST (1):** HST_WFC3_UVIS2_F555W

Note: `observatory_facility` and `instrument` on each instrument-specific entry reflect
the actual SVO profile source (e.g. `HCT`/`HFOSC` for Bessell entries), not a generic
label.

**Generic fallback entries (8):**

Generic_U, Generic_B, Generic_V, Generic_R, Generic_I, Generic_J, Generic_H, Generic_K

- **Optical (Generic_U through Generic_I):** Carry Bessell reference spectral data from
  `HCT/HFOSC.Bessell_*` SVO profiles. Own all bare uppercase letter aliases (`U`, `B`,
  `V`, `R`, `I`) and legacy system names (`Johnson_V`, `Cousins_R`, etc.).
- **NIR (Generic_J, Generic_H):** Carry 2MASS reference spectral data. Own bare `J` and
  `H` aliases.
- **NIR (Generic_K):** Intentionally sparse — only `lambda_eff` hint of 21900 Å.
  Owns bare `K` alias.

**Excluded entries (1):** Open (unfiltered observation)

**Completeness claim (unchanged):** This population covers 100% of the filter strings
observed in the current catalog manifest, plus the Generic fallback entries required by
ADR-018's disambiguation algorithm.

**Data/code separation:** Band definitions (aliases, SVO candidates, metadata) are
maintained in `tools/filter_band_reg/band_specs.json`, separate from the seed script
(`tools/filter_band_reg/seed_band_registry.py`). The seed script reads `band_specs.json`
by default and queries the SVO API to produce `band_registry.json`. This separation
ensures alias assignments and entry metadata are reviewable independently of script
logic.

---

## Downstream Impact

### ADR-018 (Band Disambiguation Algorithm)

No structural changes required. The disambiguation algorithm's three-stage funnel
operates on the alias index, which is derived from `aliases` lists — not from `band_id`
naming. The corrected alias ownership ensures that bare aliases resolve to Generic
entries as ADR-018 Decision 5 intended.

### ADR-019 (Photometry Table Model Revision)

No changes required. ADR-019 already requires the registry to be reseeded after
`photometric_system` removal. The `band_id` field type (`str`, NOT NULL) is unchanged.

### DESIGN-004 (Ticket-Driven Ingestion)

The two-step resolution in §6.5 now works as designed: bare filter strings like `"V"`
hit the alias index, resolve to `Generic_V`, and receive `generic_fallback` / `low`
provenance. Previously these resolved to `HCT_HFOSC_Bessell_V` / `canonical` / `high`,
which was incorrect.

### Seed Script (`seed_band_registry.py`)

Updated to:
1. Remove embedded `BAND_SPECS` list entirely.
2. Default to loading band definitions from co-located `band_specs.json`.
3. Validate alias uniqueness and first-alias invariant at load time.
4. Include old `band_id` values (`Johnson_V`, `Cousins_R`, etc.) as aliases on the
   Generic entries for backward compatibility.

### photometry_reader.py

No code changes required. The `_resolve_band()` function's two-step logic (alias lookup
→ Generic fallback) is unchanged. The fix is entirely in the data: bare aliases now
point to Generic entries in the alias index, so Step 1 resolves them correctly without
ever reaching Step 2.

### Open Item 8 (resolved)

Original resolution: "confirmed by the seed script execution. All 22 non-excluded
entries successfully mapped `photometric_system` to `SystemAbbrev` with operator review."

Revised resolution: "Superseded. `SystemAbbrev` derivation from `photometric_system`
is abolished. `band_id` is now derived from SVO filter ID components per the amended
Decision 2."
