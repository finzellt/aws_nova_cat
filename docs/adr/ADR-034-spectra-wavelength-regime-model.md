# ADR-034: Spectra Wavelength Regime Model

**Status:** Proposed
**Date:** 2026-04-11
**Author:** TF
**Supersedes:** —
**Superseded by:** -
**Amends:** ADR-013 (adds regime tabs to spectra viewer), ADR-014 (restructures
`spectra.json` artifact schema to include regime metadata)
**Relates to:**
- `ADR-013` — Visualization Design (spectra viewer waterfall plot; photometry regime
  tabs as precedent)
- `ADR-014` — Artifact Schemas (`spectra.json` schema; photometry `regimes` array as
  structural template)
- `DESIGN-003` — Artifact regeneration pipeline (spectra generator §7)
- `ADR-033` — Spectra Compositing Pipeline (composites carry regime like any spectrum)

> **⚠ Amended by ADR-035** (2026-04-12)
> ADR-035 (Spectra Regime Splitting and Per-Regime Display Range) amends this ADR
> as follows:
>
> - **Regime split:** The combined `xuv` regime is replaced by two regimes:
>   `xray` (λ_mid < 91 nm, Lyman limit) and `uv` (91 ≤ λ_mid < 320 nm).
> - **Spectrum splitting:** Cross-boundary spectra (e.g., X-Shooter 350–2500 nm)
>   are split at regime boundaries when the minor-side coverage meets both a
>   15% fractional and 45 nm absolute threshold. Deferred in §5 of this ADR;
>   now decided in ADR-035 Decision 2.
> - **Per-regime trimming:** The median-based display wavelength range is computed
>   independently per regime group, fixing a bug where UV spectra were truncated
>   by the optical-dominated global median.
> - **Schema version:** Bumped from `"1.3"` to `"1.4"`.
>
> See: `docs/adr/ADR-035-spectra-regime-splitting-and-per-regime-display-range.md`

---

## 1. Context

The NovaCat spectra viewer currently treats all spectra as belonging to a single
wavelength domain. The waterfall plot renders every spectrum on a shared wavelength
axis, and the `spectra.json` artifact carries a flat `spectra` array with no regime
concept.

This works when all spectra are ground-based optical observations (~350–900 nm), but
breaks down when spectra span fundamentally different wavelength ranges. HST/STIS, for
example, covers 115–1030 nm across UV MAMA detectors and a CCD — plotting a 120 nm
Lyman-alpha spectrum on the same axis as a 550 nm optical spectrum produces an
unreadable plot with vast empty regions and incompatible wavelength scales.

The photometry system already solved this problem: ADR-013 defines wavelength regime
tabs for the light curve panel, and ADR-014 structures `photometry.json` around a
`regimes` array that drives tab creation in the frontend. The spectra viewer needs the
same treatment.

### 1.1 Why spectra regimes are simpler than photometry regimes

Photometry regimes differ in their **physical quantity** — optical uses magnitudes,
X-ray uses count rates, gamma uses photon flux, radio uses flux density. Each regime
requires a different Y-axis label, scale, and orientation.

Spectra regimes differ only in their **wavelength domain**. The Y-axis is always
normalized flux regardless of regime. The tabs exist to separate incompatible
wavelength ranges, not incompatible measurement types. This means spectra regime
metadata is lighter — no per-regime Y-axis configuration is needed.

### 1.2 Motivating use case

MAST HASP provides science-ready co-added HST spectra. For novae observed with STIS,
the catalog will receive separate spectral products per grating configuration:

- G140L (~115–170 nm) — far-UV
- G230L (~160–320 nm) — near-UV
- G430L (~290–570 nm) — blue optical
- G750L (~525–1030 nm) — red optical / near-IR

The G140L and G230L products belong in a UV tab; G430L and G750L belong in the optical
tab. Without regime separation, these would all land on a single waterfall plot spanning
115–1030 nm, with most of the wavelength axis empty for any given spectrum.

---

## 2. Decisions

### Decision 1 — Four Spectra Wavelength Regimes

Spectra are classified into four wavelength regimes. The regime set is deliberately
aligned with astronomical convention and instrument boundaries, not with the photometry
regime set (which is organized by physical quantity rather than wavelength).

| `id` | `label` | Wavelength range | Boundary rationale |
|---|---|---|---|
| `xuv` | X-ray / UV | λ_mid < 320 nm | 320 nm ≈ atmospheric cutoff; STIS MAMA/CCD detector boundary at ~310 nm. All spectra below this boundary are space-based. |
| `optical` | Optical | 320 nm ≤ λ_mid < 1000 nm | Conventional optical window. Covers ground-based CCD spectroscopy and the red end of STIS CCD gratings. |
| `nir` | Near-IR | 1000 nm ≤ λ_mid < 5000 nm | 1000 nm ≈ silicon CCD sensitivity limit. Covers J/H/K-band spectroscopy (e.g., Gemini/GNIRS, VLT/X-Shooter NIR arm). |
| `mir` | Mid-IR | λ_mid ≥ 5000 nm | 5 μm ≈ conventional NIR/MIR boundary (longward of K-band). Covers Spitzer/IRS and JWST/MIRI spectroscopy. |

**Combined X-ray / UV regime.** X-ray and UV spectra are grouped into a single regime
rather than separated. In practice, X-ray spectra of novae (e.g., from Chandra or
XMM-Newton gratings) are rare, and their wavelength ranges (~0.1–10 nm) do not overlap
with UV (~100–320 nm). If the catalog accumulates sufficient X-ray spectra to warrant
separation, a future ADR can split `xuv` into `xray` and `uv` without breaking the
schema — the `id` field is a string, not an enum.

### Decision 2 — Wavelength Midpoint Assignment Rule

A spectrum's regime is determined by the midpoint of its wavelength coverage:

```
λ_mid = (wavelength_min + wavelength_max) / 2
```

This is computed by the artifact generator from the spectrum's `wavelength_min` and
`wavelength_max` values (which are already present on every spectrum record per
ADR-014).

**Rationale for midpoint over blue edge (`wavelength_min`):** Some gratings have
wavelength coverage that crosses a regime boundary. STIS G430L, for example, starts at
~290 nm (below the 320 nm UV/optical boundary) but extends to ~570 nm — it is
overwhelmingly an optical grating. Using `wavelength_min` would misclassify it as UV.
The midpoint correctly places it in the optical regime.

**Rationale for midpoint over metadata-based assignment:** Archive metadata
(instrument, grating, detector) could in principle determine regime, but metadata
quality and consistency vary across archives. The wavelength midpoint is
self-describing, archive-independent, and always available. It produces the correct
answer for all current and foreseeable instruments without requiring a grating-to-regime
lookup table.

### Decision 3 — `spectra.json` Schema Changes

The `spectra.json` artifact is restructured to include regime metadata, following the
pattern established by `photometry.json` in ADR-014. The schema version is bumped from
`"1.2"` (post-ADR-033) to `"1.3"`.

#### New top-level field: `regimes`

A `regimes` array is added to the top level of `spectra.json`. This array is the
authoritative source for tab structure, exactly as it is for photometry. The frontend
iterates this array to determine how many tabs to render.

| Field | Type | Description |
|---|---|---|
| `id` | string | Regime identifier (`"xuv"`, `"optical"`, `"nir"`, `"mir"`) |
| `label` | string | Human-readable tab label |
| `wavelength_range_nm` | `[number, number] or [number, null]` | Nominal wavelength boundaries in nm for axis context; `null` upper bound for MIR |

The spectra regime metadata record is deliberately simpler than the photometry regime
record. It omits `y_axis_label`, `y_axis_inverted`, and `y_axis_scale_default` because
all spectra regimes share the same Y-axis semantics (normalized flux, standard
orientation, linear scale). It adds `wavelength_range_nm` to provide the frontend with
nominal wavelength boundaries for each regime — this is informational context, not an
axis constraint (the actual axis range is determined by the spectra data within each
regime).

Only regimes for which data exists are included in the array. If all spectra belong to
a single regime, the array contains one element and the frontend hides the tab bar
(consistent with the photometry precedent in ADR-013).

**Defined regimes:**

| `id` | `label` | `wavelength_range_nm` |
|---|---|---|
| `xuv` | X-ray / UV | `[0, 320]` |
| `optical` | Optical | `[320, 1000]` |
| `nir` | Near-IR | `[1000, 5000]` |
| `mir` | Mid-IR | `[5000, null]` |

#### New per-spectrum field: `regime`

Each spectrum record gains a `regime` field:

| Field | Type | Description |
|---|---|---|
| `regime` | string | Regime identifier; matches `id` in the `regimes` array |

This field is computed by the backend using the midpoint rule (Decision 2) and embedded
in the artifact. The frontend never computes regime assignment.

#### Updated schema example

```json
{
  "schema_version": "1.3",
  "generated_at": "2026-04-11T00:00:00Z",
  "nova_id": "3f2a1b4c-...",
  "outburst_mjd": 46123.0,
  "outburst_mjd_is_estimated": false,
  "wavelength_unit": "nm",
  "regimes": [
    {
      "id": "xuv",
      "label": "X-ray / UV",
      "wavelength_range_nm": [0, 320]
    },
    {
      "id": "optical",
      "label": "Optical",
      "wavelength_range_nm": [320, 1000]
    }
  ],
  "spectra": [
    {
      "spectrum_id": "a1b2...",
      "regime": "xuv",
      "epoch_mjd": 59234.312,
      "days_since_outburst": 45.3,
      "instrument": "STIS",
      "telescope": "HST",
      "provider": "MAST",
      "wavelength_min": 115.0,
      "wavelength_max": 170.0,
      "flux_unit": "erg/cm2/s/A",
      "normalization_scale": 1.87e-14,
      "wavelengths": [115.0, 115.5, 116.0],
      "flux_normalized": [0.45, 0.52, 0.61]
    },
    {
      "spectrum_id": "7a3c...",
      "regime": "optical",
      "epoch_mjd": 59234.447,
      "days_since_outburst": 45.4,
      "instrument": "FAST",
      "telescope": "FLWO 1.5m",
      "provider": "CfA",
      "wavelength_min": 370.0,
      "wavelength_max": 750.0,
      "flux_unit": "erg/cm2/s/A",
      "normalization_scale": 2.34e-13,
      "wavelengths": [370.0, 370.5, 371.0],
      "flux_normalized": [0.82, 0.91, 1.03]
    }
  ]
}
```

#### Sort order

Within each regime, spectra are sorted by `epoch_mjd` ascending (oldest first),
preserving the waterfall plot convention. Across the flat `spectra` array, spectra are
grouped by regime (in the order the regimes appear in the `regimes` array), then sorted
by epoch within each group. This co-locates spectra by regime for efficient frontend
iteration, consistent with the photometry observation sort order defined in DESIGN-003
§8.8.

### Decision 4 — Frontend Tab Behavior

The spectra viewer adopts the same tab visibility rules as the light curve panel
(ADR-013):

- **Single regime:** No tab bar is shown. The viewer renders identically to the
  current implementation. This is the common case for novae with only ground-based
  optical spectra — no visual change for existing data.
- **Multiple regimes:** A tab bar appears above the waterfall plot. Each tab contains
  a self-contained waterfall plot with its own wavelength axis range, scoped to the
  spectra in that regime. All existing controls (epoch format toggle, log/linear time
  scale, single-spectrum isolation, spectral feature markers) operate independently
  per tab.

**Tab bar styling** follows the same visual treatment as the photometry tab bar
(ADR-012 design tokens, same component structure) for UI consistency.

**Default active tab:** The regime containing the most spectra is selected by default.
If regimes have equal counts, the regime appearing first in the canonical order
(`xuv` → `optical` → `nir` → `mir`) is selected.

### Decision 5 — Spectral Feature Markers Are Optical-Only

The existing spectral feature marker overlays (Fe II, He-N, Nebular) defined in
ADR-013 are valid only for optical wavelengths. When a non-optical regime tab is
active, the feature marker toggle is hidden (not disabled).

UV spectral feature markers (e.g., C IV 1549 Å, Si IV 1394/1403 Å, Mg II 2800 Å,
N V 1240 Å) are scientifically valuable but require a dedicated feature line list and
are deferred to a future task.

### Decision 6 — Spectra Count Remains Regime-Agnostic

The `spectra_count` field on `nova.json` and `catalog.json` continues to report the
total number of spectra across all regimes. No per-regime count is added to the catalog
or nova metadata artifacts. The frontend can derive per-regime counts from the
`spectra` array if needed for display (e.g., showing a count badge on each tab).

### Decision 7 — Backward Compatibility

The addition of the `regimes` array and per-spectrum `regime` field is purely additive.
Existing spectra artifacts (schema version `"1.2"` and earlier) lack these fields. The
frontend should handle missing `regimes` gracefully:

- If `regimes` is absent or empty, treat all spectra as belonging to a single implicit
  `"optical"` regime and render without a tab bar. This preserves the current behavior
  for artifacts generated before this ADR is implemented.

This fallback is a transitional measure. Once the artifact generator is updated and a
full regeneration sweep has run, all artifacts will carry the new fields.

---

## 3. Consequences

### Benefits

- **Enables space-based UV spectroscopy.** STIS, COS, and IUE spectra can be displayed
  alongside ground-based optical spectra without producing unreadable plots.
- **Future-proofs for NIR and MIR.** JWST/NIRSpec and MIRI spectra can be accommodated
  when those data sources are added, with no further schema changes.
- **Consistent UI pattern.** The tab-based regime separation mirrors the photometry
  panel, giving users a familiar interaction model.
- **Minimal disruption to existing data.** All current spectra are optical. The schema
  change is additive, and the single-regime case (no tab bar) is visually identical to
  the current implementation.

### Costs

- **Schema version bump.** `spectra.json` moves from `"1.2"` to `"1.3"`. The frontend
  needs a backward-compatible fallback until all artifacts are regenerated.
- **Artifact generator changes.** The spectra generator must compute regime assignment,
  construct the `regimes` array, sort spectra by regime, and emit the `regime` field
  per spectrum. These are straightforward additions to the existing generator logic.
- **Frontend changes.** The `SpectraViewer` component needs tab rendering, per-tab
  state management (selected spectrum, epoch format, scale toggles), and regime-aware
  feature marker visibility. The `LightCurvePanel` tab implementation provides a direct
  template.
- **TypeScript type updates.** `SpectraArtifact` and `SpectrumRecord` in
  `frontend/src/types/nova.ts` need the new fields. `RegimeRecord` from
  `frontend/src/types/photometry.ts` cannot be reused directly because spectra regimes
  have different metadata (no Y-axis config, but has `wavelength_range_nm`).

### Risks

- **Cross-boundary spectra.** A spectrum whose wavelength range straddles a regime
  boundary (e.g., 250–500 nm spanning UV and optical) will be assigned to whichever
  regime contains its midpoint. This is acceptable — such spectra are rare, and the
  midpoint rule places them in the regime where most of their data lies. If this proves
  problematic in practice, a future ADR could introduce spectrum splitting, but this is
  not expected to be necessary.
- **Regime proliferation.** Four regimes is manageable. If future instruments or data
  sources require finer subdivision, the tab bar could become cluttered. The photometry
  system noted a similar concern (DESIGN-003 §8.11) and suggested a dropdown menu as a
  future option. The same escape hatch applies here.

---

## 4. Implementation Plan

### 4.1 Backend — Artifact Generator

1. Define regime boundary constants and midpoint assignment function in the spectra
   generator module.
2. After loading and processing spectra (including compositing per ADR-033), assign
   each spectrum a `regime` value using the midpoint rule.
3. Construct the `regimes` array from the set of regimes present in the processed
   spectra.
4. Sort the output `spectra` array by regime group order, then by `epoch_mjd` within
   each group.
5. Emit `regime` on each spectrum record.
6. Bump `schema_version` to `"1.3"`.

### 4.2 Frontend — Types

1. Add `SpectraRegimeRecord` interface to `frontend/src/types/nova.ts`.
2. Add `regimes` field to `SpectraArtifact`.
3. Add `regime` field to `SpectrumRecord`.

### 4.3 Frontend — SpectraViewer

1. Add regime tab bar (conditionally rendered when `regimes.length > 1`).
2. Filter `spectra` by active regime for waterfall plot rendering.
3. Scope all existing controls (epoch format, scale toggles, spectrum selection,
   representative subset sampling) to the active regime's spectra.
4. Hide spectral feature marker toggle when active regime is not `"optical"`.
5. Manage per-regime state (selected spectrum resets on tab switch, etc.).

### 4.4 Mock Fixtures

1. Add UV spectra to the development mock data in `frontend/public/data/` to exercise
   the multi-regime tab behavior during frontend development.

---

## 5. Deferred

- UV spectral feature markers (C IV, Si IV, Mg II, N V, etc.)
- X-ray spectral feature markers
- Spectrum splitting for cross-boundary spectra
- Per-regime spectra counts in `nova.json` / `catalog.json`
- Regime-specific waterfall plot styling (e.g., different color ramps per regime)

---

## 6. Links

- Related ADRs: ADR-013, ADR-014, ADR-031, ADR-033
- Design docs: DESIGN-003 (artifact regeneration pipeline, §7)
- HST/STIS wavelength coverage: 1150–10,300 Å (115–1030 nm) across MAMA + CCD
  detectors
- MAST HASP: science-ready co-added HST spectra
