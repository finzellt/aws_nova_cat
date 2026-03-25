# ADR-017 Amendment: Band ID Naming Convention Revision

**Amendment date:** 2026-03-25
**Scope:** Decision 2 (Canonical Band ID Naming Convention), Decision 3 (examples),
Decision 4 (example table), Decision 10 (initial population)
**Trigger:** ADR-019 abolished `photometric_system` system-wide; `SystemAbbrev`
derivation anchor no longer exists. Additionally, `photometric_system` was never a
valid uniqueness basis — multiple facilities can implement the same photometric standard
with measurably different filter profiles (e.g. SLOAN/SDSS.g vs. FLWO/KeplerCam.sdss_g
differ by ~15% in effective width for g).

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

**Examples:**

| SVO filter ID | `band_id` | Notes |
|---|---|---|
| `HCT/HFOSC.Bessell_V` | `HCT_HFOSC_Bessell_V` | Mechanical derivation |
| `OAF/Bessell.V` | `OAF_Bessell_V` | Mechanical derivation |
| `SLOAN/SDSS.g` | `SLOAN_SDSS_g` | Mechanical derivation |
| `Swift/UVOT.UVW1` | `Swift_UVOT_UVW1` | Mechanical derivation |
| `2MASS/2MASS.Ks` | `2MASS_Ks` | Redundancy collapse (facility = instrument) |
| `Spitzer/IRAC.I1` | `Spitzer_IRAC_I1` | Mechanical derivation |
| `HST/WFC3_UVIS2.F555W` | `HST_WFC3_UVIS2_F555W` | Mechanical derivation |

#### Track 2 — Generic Fallback Entries

Entries where the band identity is known but the specific instrument/facility is not.
These are **strictly fallback** entries, used only when the disambiguation algorithm
(ADR-018) cannot resolve to an instrument-specific profile.

**Format:**

```
Generic_{BandLabel}
```

**Rules:**

- Generic entries have `svo_filter_id: null` and all SVO-derived spectral fields as
  `null`. They are intentionally sparse.
- Any row resolved to a Generic entry receives `band_resolution_confidence: "low"`
  per ADR-018 Decision 6.
- Generic entries exist for every band label where alias ambiguity could prevent
  resolution to a specific profile (e.g. `Generic_V`, `Generic_K`, `Generic_B`).
- The `Generic_` prefix is self-documenting — downstream consumers can identify
  fallback resolutions without additional metadata.

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

---

## Amended Decision 3 — Registry Entry Schema (examples only)

> **Amendment (band_id naming revision):** Examples updated to reflect the new
> two-track naming convention. The `photometric_system` field removal (from the
> `epic/22-photometry-doc-reconciliation` amendment) remains in effect. The schema
> field table is unchanged.

**The first paragraph of Decision 3 is revised from:**

> Each entry represents a **filter class** — an abstract grouping of physically similar
> filters that are scientifically comparable. Specific instrument filter details are not
> stored as separate entities; they are captured via the `svo_filter_id` cross-reference
> and the SVO-derived spectral fields.

**To:**

> Each entry represents either a **specific filter profile** (instrument-specific entry)
> or a **fallback band identity** (Generic entry). Instrument-specific entries are backed
> by a specific SVO filter profile and carry full spectral data. Generic entries are
> intentionally sparse and serve as low-confidence fallbacks when disambiguation cannot
> resolve to a specific profile.

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
  "aliases": ["HCT_HFOSC_Bessell_V", "Johnson_V", "V", "Johnson V", "Vmag"],
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
      "zero_point_flux_lambda": 3.55892e-09,
      "zero_point_flux_nu": 3953.33,
      "zeropoint_type": "Pogson",
      "photcal_id": "HCT/HFOSC.Bessell_V/Vega"
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
  "band_name": "V",
  "regime": "optical",
  "detector_type": null,
  "observatory_facility": null,
  "instrument": null,
  "aliases": ["Generic_V"],
  "excluded": false,
  "exclusion_reason": null,
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

The case-sensitive matching policy is unchanged. The example table is updated to
reflect the new `band_id` values:

| String | Correct band |
|--------|-------------|
| `V` | `HCT_HFOSC_Bessell_V` (or `Generic_V` if unresolvable) |
| `i` | `SLOAN_SDSS_i` |
| `I` | `HCT_HFOSC_Bessell_I` |
| `Ks` | `2MASS_Ks` |

---

## Amended Decision 10 — Initial Population

> **Amendment (band_id naming revision):** Entry count increases from 25 to 32.
> Instrument-specific entries now use SVO-derived `band_id` values. Generic fallback
> entries added for all ambiguous single-letter band aliases per ADR-018 requirements.

The initial registry population is the 32 entries produced by the revised seed script.

**Coverage by category:**

**Instrument-specific entries (23):**

- **Optical — Bessell (5):** HCT_HFOSC_Bessell_U, _B, _V, _R, _I
- **Optical — Sloan unprimed (5):** SLOAN_SDSS_u, _g, _r, _i, _z
- **Optical — Sloan primed (4):** SLOAN_SDSS_up, _gp, _rp, _ip
- **NIR (3):** 2MASS_J, _H, _Ks
- **UV (3):** Swift_UVOT_UVW1, _UVW2, _UVM2
- **MIR (2):** Spitzer_IRAC_I1, _I2
- **HST (1):** HST_WFC3_UVIS2_F555W

Note: `observatory_facility` and `instrument` on each entry now reflect the actual
SVO profile source (e.g. `HCT`/`HFOSC` for Bessell entries), not a generic label.

**Generic fallback entries (8):**

Generic_U, Generic_B, Generic_V, Generic_R, Generic_I, Generic_J, Generic_H, Generic_K

All are intentionally sparse (`svo_filter_id: null`, all spectral fields `null`).
`Generic_K` retains its `lambda_eff` hint of 21900 Å.

**Excluded entries (1):** Open (unfiltered observation)

**Completeness claim (unchanged):** This population covers 100% of the filter strings
observed in the current catalog manifest, plus the Generic fallback entries required by
ADR-018's disambiguation algorithm.

---

## Downstream Impact

### ADR-018 (Band Disambiguation Algorithm)

No structural changes required. The disambiguation algorithm's three-stage funnel
operates on the alias index, which is derived from `aliases` lists — not from `band_id`
naming. The old `band_id` values (`Johnson_V`, `Sloan_g`, etc.) remain as aliases on
the instrument-specific entries, so alias resolution is unaffected.

The Generic fallback entries (Decision 5 of ADR-018) now exist for all commonly
ambiguous band labels, closing the gap noted in ADR-017's original Decision 10.

### ADR-019 (Photometry Table Model Revision)

No changes required. ADR-019 already requires the registry to be reseeded after
`photometric_system` removal. The `band_id` field type (`str`, NOT NULL) is unchanged.

### Seed Script (`seed_band_registry.py`)

Must be updated to:
1. Remove `photometric_system` from `BandSpec` and output dicts.
2. Use new `band_id` values per the two-track convention.
3. Add Generic fallback entries for all ambiguous single-letter band aliases.
4. Include old `band_id` values as aliases for continuity.

### Open Item 8 (resolved)

Original resolution: "confirmed by the seed script execution. All 22 non-excluded
entries successfully mapped `photometric_system` to `SystemAbbrev` with operator review."

Revised resolution: "Superseded. `SystemAbbrev` derivation from `photometric_system`
is abolished. `band_id` is now derived from SVO filter ID components per the amended
Decision 2."
