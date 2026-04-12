# ADR-035: Spectra Regime Splitting and Per-Regime Display Range

**Status:** Proposed
**Date:** 2026-04-12
**Author:** TF
**Supersedes:** —
**Superseded by:** —
**Amends:** ADR-034 (splits `xuv` regime into `xray` + `uv`; promotes spectrum
splitting and per-regime display range from Deferred to Decided)
**Relates to:**
- `ADR-013` — Visualization Design (spectra viewer, feature marker visibility)
- `ADR-014` — Artifact Schemas (`spectra.json` schema)
- `ADR-033` — Spectra Compositing Pipeline (composites carry regime like any spectrum)
- `DESIGN-003` — Artifact regeneration pipeline (spectra generator §7)

---

## 1. Context

ADR-034 introduced wavelength regime classification for spectra, defining four regimes
(`xuv`, `optical`, `nir`, `mir`) with midpoint-based assignment, and restructured the
`spectra.json` artifact to carry per-spectrum regime metadata. The frontend gained
regime tabs for the spectra viewer.

Three issues have emerged during implementation:

### 1.1 Combined X-ray / UV regime is too coarse

ADR-034 Decision 1 combined X-ray and UV into a single `xuv` regime, noting that X-ray
grating spectra are rare. While this remains true, the wavelength gap between X-ray
grating spectra (Chandra HETG/LETG: ~0.1–17 nm; XMM-Newton RGS: ~0.5–3.5 nm) and UV
spectra (IUE/STIS/COS: ~91–320 nm) is enormous. No current or planned facility observes
novae in the ~17–91 nm EUV range. Plotting a 2 nm Chandra spectrum on the same axis as
a 150 nm STIS spectrum would produce the same unreadable result that motivated regime
separation in the first place.

The Lyman limit at 91.2 nm provides a clean physical boundary: everything below it is
ionizing radiation observed by X-ray telescopes, everything above it is UV observed by
UV-capable space telescopes.

### 1.2 Cross-boundary spectra are truncated by global median trimming

The spectra generator computes a single display wavelength range from the median of all
spectra's min/max wavelengths, then trims outliers to that range. This was designed for
a homogeneous optical population and works well when all spectra cover roughly the same
wavelength domain.

When UV spectra (~115–310 nm) are mixed with optical spectra (~300–900 nm), the median
blue edge lands at ~305 nm. UV spectra are trimmed to just their red tail (305–310 nm),
rendering them as a few nanometres of noise. This is the proximate bug that motivated
this amendment.

The fix requires computing display ranges **per regime** rather than globally.

### 1.3 Wide-coverage spectra span regime boundaries

Instruments like VLT/X-Shooter (~350–2500 nm) and HST/STIS CCD gratings (~290–1030 nm)
produce spectra whose wavelength coverage spans a regime boundary. ADR-034 handles
these by assigning the entire spectrum to the regime containing its midpoint, which
works when the cross-boundary portion is small. However, when an X-Shooter spectrum
has 650 nm of optical coverage and 1500 nm of NIR coverage, assigning the entire
spectrum to NIR wastes the optical data — and assigning it to optical wastes the NIR.

Splitting such spectra at the regime boundary allows each portion to participate in the
correct regime's waterfall plot.

---

## 2. Decisions

### Decision 1 — Split `xuv` Into `xray` and `uv`

The ADR-034 `xuv` regime is replaced by two regimes:

| `id` | `label` | Wavelength range | Boundary rationale |
|---|---|---|---|
| `xray` | X-ray | λ_mid < 91 nm | 91 nm ≈ Lyman limit (912 Å). All grating spectra below this boundary are X-ray telescopes (Chandra, XMM-Newton). |
| `uv` | Ultraviolet | 91 nm ≤ λ_mid < 320 nm | Space-based UV window (STIS, COS, IUE). Upper boundary unchanged from ADR-034. |

The remaining regimes (`optical`, `nir`, `mir`) are unchanged.

**Updated complete regime table:**

| `id` | `label` | Range | Sort order |
|---|---|---|---|
| `xray` | X-ray | λ_mid < 91 nm | 0 |
| `uv` | Ultraviolet | 91 ≤ λ_mid < 320 nm | 1 |
| `optical` | Optical | 320 ≤ λ_mid < 1000 nm | 2 |
| `nir` | Near-IR | 1000 ≤ λ_mid < 5000 nm | 3 |
| `mir` | Mid-IR | λ_mid ≥ 5000 nm | 4 |

**Schema impact:** The `regimes` array in `spectra.json` carries the new `id` values.
Existing artifacts with `xuv` regime IDs will not be produced after the generator is
updated. The frontend backward-compatibility fallback (ADR-034 Decision 7) already
handles missing `regimes` gracefully; no additional fallback is needed for the ID
rename because all artifacts will be regenerated in the next sweep.

**Frontend feature marker visibility:** ADR-034 Decision 5 hid feature markers for
non-optical regimes. This remains correct — the toggle is hidden for both `xray` and
`uv` tabs. UV spectral feature markers remain deferred.

### Decision 2 — Cross-Boundary Spectrum Splitting

Spectra whose wavelength coverage straddles a regime boundary are eligible for
splitting. Splitting produces two (or, in rare cases, more) records from a single
input spectrum, one per regime, each containing the subset of wavelength/flux data
points that fall within that regime's domain.

**Regime boundaries for splitting:** 91 nm, 320 nm, 1000 nm, 5000 nm.

**Boundary point rule:** A data point whose wavelength exactly equals a boundary value
is assigned to the **redder** (longer-wavelength) regime. For example, a point at
exactly 320.0 nm goes to `optical`, not `uv`.

**Splitting eligibility thresholds:** A spectrum is split only when the minor-side
coverage (the portion of the spectrum on the smaller side of the boundary) meets
**both** of the following criteria:

1. **Fractional threshold:** The minor-side wavelength span is ≥ 15% of the total
   wavelength span of the spectrum.
2. **Absolute minimum:** The minor-side wavelength span is ≥ 45 nm.

If either threshold is not met, the spectrum is assigned whole to the regime containing
its midpoint (per ADR-034 Decision 2), and no splitting occurs.

**Rationale for 45 nm absolute minimum:** The smallest scientifically useful UV spectra
in the catalog are HST/STIS G140L products covering ~114–168 nm (~54 nm span). The
45 nm floor ensures that split fragments are large enough to be independently useful on
a waterfall plot while preventing creation of tiny slivers from spectra that barely
cross a boundary (e.g., STIS G430L's ~30 nm of coverage below 320 nm).

**Rationale for 15% fractional threshold:** Prevents splitting when the cross-boundary
portion is negligible relative to the spectrum's total coverage, regardless of absolute
width.

**Split record identity:** Each split fragment inherits the parent spectrum's
`data_product_id` and product metadata. To distinguish fragments in the output, the
`spectrum_id` field receives a suffix: `{data_product_id}::{regime_id}`. For example,
a spectrum `abc-123` split into optical and NIR portions becomes `abc-123::optical` and
`abc-123::nir`. This ensures unique `spectrum_id` values in the artifact while
preserving traceability to the source DataProduct.

Non-split spectra continue to use `data_product_id` as their `spectrum_id`, unchanged
from ADR-034.

**Splitting execution point:** Splitting occurs in the spectra generator **after**
Stage 1 processing (CSV parse, dead edge trimming, chip gap cleaning) and multi-arm
merging, but **before** regime-grouped median display range computation and
Stage 2 processing (LTTB, normalization). This ensures that:

- Cleaning is applied to the full spectrum before splitting (chip gaps that span a
  boundary are handled correctly).
- Each split fragment participates in its regime's median display range pool.
- LTTB downsampling and normalization are applied independently per fragment.

**Worked examples:**

| Spectrum | Coverage | Boundary | Minor side | Split? | Reason |
|---|---|---|---|---|---|
| X-Shooter | 350–2500 nm | 1000 nm | 650 nm optical (30%) | Yes | 650 nm > 45 nm, 30% > 15% |
| STIS G430L | 290–570 nm | 320 nm | 30 nm UV (11%) | No | 30 nm < 45 nm |
| STIS G750L | 525–1030 nm | 1000 nm | 30 nm NIR (6%) | No | 30 nm < 45 nm, 6% < 15% |
| STIS CCD full | 290–1030 nm | 320 nm | 30 nm UV (4%) | No | 30 nm < 45 nm |
| Hypothetical | 200–600 nm | 320 nm | 120 nm UV (30%) | Yes | 120 nm > 45 nm, 30% > 15% |

### Decision 3 — Per-Regime Median Display Range Computation

The existing median-based display wavelength range computation (DESIGN-003 §7,
`spectra.py` Step 2b) is restructured to operate **independently per regime group**
rather than globally across all spectra.

**Algorithm (per regime):**

1. After regime classification and splitting, group parsed spectra by regime.
2. For each regime group with ≥ 2 spectra:
   a. Compute `display_wavelength_min` = median of blue edges.
   b. Compute `display_wavelength_max` = median of red edges.
   c. Apply the existing trim logic (red-side and blue-side, with `_TRIM_TOLERANCE`)
      scoped to that regime's spectra only.
3. For regime groups with 0–1 spectra, no trimming is applied (consistent with existing
   behavior for single-spectrum novae).

**Rationale:** This is the minimal change to the existing trimming algorithm that fixes
the cross-regime contamination bug. The algorithm itself is sound — it was designed to
handle detector rolloff outliers within a homogeneous population. The bug was that UV
and optical spectra were treated as a single population, causing the median to reflect
the optical majority and trim the UV minority.

**Bimodal data warning:** The existing >50% trim warnings are now computed per regime.
A bimodal distribution *within* a single regime (e.g., two groups of optical spectra
with very different wavelength ranges) would still trigger the warning, which is the
correct behavior.

### Decision 4 — Updated Spectra Generator Flow

The spectra generator's processing pipeline is restructured as follows. Steps marked
with ★ are new or modified; unmarked steps are unchanged.

1. Query VALID spectra DataProduct items
2. Post-query composite filtering (ADR-033)
3. Stage 1: parse CSV + dead edge trimming + chip gap cleaning (per spectrum)
4. Multi-arm merge
5. ★ **Early regime classification** — assign each parsed spectrum a regime using the
   midpoint rule (ADR-034 Decision 2), based on pre-trim wavelength bounds
6. ★ **Cross-boundary splitting** — apply Decision 2 splitting logic; re-classify
   each fragment (midpoint will now place fragments in the correct regime)
7. ★ **Per-regime median display range + trim** — apply Decision 3 per-regime
   grouping and trimming
8. Stage 2: LTTB downsampling + normalization — regime already assigned; the
   `_assign_spectra_regime` call in `_process_spectrum_stage2` is replaced by
   passing through the pre-assigned regime from step 5/6
9. Regime metadata assembly + sort *(unchanged)*

### Decision 5 — Schema Version Bump

The `spectra.json` schema version is bumped from `"1.3"` to `"1.4"` to reflect the
regime ID changes (`xuv` → `xray` / `uv`) and the potential presence of split-spectrum
`spectrum_id` values with `::` suffixes.

The `regimes` array structure is unchanged — only the `id` and `label` values for the
former `xuv` regime are affected.

**Updated regime definitions for the artifact:**

| `id` | `label` | `wavelength_range_nm` |
|---|---|---|
| `xray` | X-ray | `[0, 91]` |
| `uv` | Ultraviolet | `[91, 320]` |
| `optical` | Optical | `[320, 1000]` |
| `nir` | Near-IR | `[1000, 5000]` |
| `mir` | Mid-IR | `[5000, null]` |

### Decision 6 — Frontend Backward Compatibility

The frontend already handles missing `regimes` gracefully (ADR-034 Decision 7). For
the `xuv` → `xray` / `uv` rename, no additional fallback is needed: all artifacts
will be regenerated in the next sweep after deployment. During the brief window
between deployment and regeneration, stale artifacts with `xuv` regime IDs may be
served. The frontend should treat unrecognized regime IDs as a fallback to the
single-regime (no tab bar) display mode, consistent with the existing missing-`regimes`
fallback.

---

## 3. Consequences

### Benefits

- **Fixes UV spectra display bug.** Per-regime trimming ensures UV spectra are not
  destroyed by the optical-dominated global median.
- **Proper X-ray / UV separation.** If the catalog acquires Chandra or XMM grating
  spectra, they will render on their own tab with an appropriate wavelength scale.
- **Wide-coverage instruments supported.** X-Shooter optical+NIR spectra are split
  so each portion appears in the correct regime with appropriate axis scaling.
- **Minimal algorithmic change.** The median trimming algorithm itself is unchanged;
  only its scope of application (per-regime vs. global) changes.

### Costs

- **Split-spectrum `spectrum_id` values.** The `::` suffix convention adds a new
  pattern to the `spectrum_id` field. Frontend code that uses `spectrum_id` as a key
  (legend strip, selection state) must handle the longer IDs, but no logic change is
  required — they are still unique strings.
- **Additional regime constants.** Five regimes instead of four, with one more
  boundary (91 nm). Regime-related data structures in both backend and frontend gain
  one entry.
- **Generator restructuring.** Moving regime assignment upstream of trimming requires
  reordering the processing pipeline. The individual functions are unchanged; the
  control flow in `generate_spectra_json` is restructured.

### Risks

- **Split fragments with low SNR.** When a wide-coverage spectrum is split, the
  minor-side fragment may have lower SNR than the parent. The 45 nm / 15% thresholds
  mitigate this by preventing creation of scientifically useless slivers, but a split
  fragment near the boundary may still be noisier than the rest of the spectrum. This
  is acceptable — the fragment is genuine data, and low-SNR spectra are displayed as-is
  throughout the system.
- **Multi-boundary splits.** In theory, a spectrum could span two boundaries (e.g.,
  200–6000 nm spanning UV/optical/NIR/MIR). The splitting logic should handle
  arbitrary numbers of boundaries by iterating through them. In practice, no current
  instrument produces single spectra this broad.

---

## 4. Implementation Plan

### 4.1 Backend — Spectra Generator

1. Update regime boundary constants: replace `xuv` entry with `xray` (91 nm) and
   `uv` (320 nm) entries.
2. Update regime definitions dict, sort order dict.
3. Add `_split_cross_boundary_spectrum()` function implementing Decision 2.
4. Add `_assign_and_split_regimes()` orchestration function implementing steps 5–6
   of Decision 4.
5. Add `_trim_per_regime()` function implementing Decision 3.
6. Restructure `generate_spectra_json` control flow per Decision 4.
7. Update `_process_spectrum_stage2` to accept pre-assigned regime rather than
   computing it from post-trim wavelengths.
8. Bump `_SCHEMA_VERSION` to `"1.4"`.

### 4.2 Frontend — Types

1. Update `SpectraRegimeRecord` usage — no structural change, but `id` values now
   include `"xray"` and `"uv"` instead of `"xuv"`.
2. Handle `::` suffixed `spectrum_id` values in legend strip and selection state
   (no logic change expected — they are opaque string keys).

### 4.3 Frontend — SpectraViewer

1. Update any hardcoded `"xuv"` references to handle both `"xray"` and `"uv"`.
2. Feature marker toggle hidden for `"xray"` and `"uv"` (same logic as before,
   expanded to two IDs).
3. Backward compatibility: treat unrecognized regime `id` values as single-regime
   fallback.

### 4.4 Tests

1. Unit tests for `_split_cross_boundary_spectrum()`: X-Shooter case (split),
   STIS G430L case (no split — below thresholds), boundary-point assignment.
2. Unit tests for `_trim_per_regime()`: verify independent median computation
   per regime group; verify UV spectra are not trimmed by optical median.
3. Integration-style test: mixed UV + optical spectra population produces correct
   per-regime display ranges in the artifact output.

---

## 5. Deferred

- UV spectral feature markers (C IV 154.9 nm, Si IV 139.4/140.3 nm, Mg II 280.0 nm,
  N V 124.0 nm, etc.)
- X-ray spectral feature markers
- Per-regime spectra counts in `nova.json` / `catalog.json`
- Regime-specific waterfall plot styling (e.g., different color ramps per regime)

---

## 6. Links

- Related ADRs: ADR-013, ADR-014, ADR-033, ADR-034
- Design docs: DESIGN-003 (artifact regeneration pipeline, §7)
- Lyman limit: 91.2 nm (912 Å) — hydrogen ionization edge
- Chandra HETG wavelength range: 0.12–17 nm (1.2–170 Å)
- XMM-Newton RGS wavelength range: 0.5–3.5 nm (5–35 Å)
- HST/STIS wavelength coverage: 115–1030 nm across MAMA + CCD detectors
- VLT/X-Shooter wavelength coverage: ~300–2500 nm across UVB + VIS + NIR arms
