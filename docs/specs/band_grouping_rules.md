# Photometric Band Grouping Rules

**Document class:** Specification
**Status:** Proposed
**Date:** 2026-03-21
**Upstream dependency:** ADR-016 (Band and Filter Resolution Strategy)
**Downstream consumers:** ADR-017 (Band Registry Design), ADR-019 (Photometry Table Model), frontend light curve panel

---

## Purpose

This specification defines the rules for when photometric measurements from different
filter provenance may be grouped onto the same light curve. It codifies the results of
an empirical analysis of the SVO Filter Profile Service database (10,664 filters, 100%
transmission curve coverage) and a synthetic photometry comparison of commercially
available amateur filters.

These rules govern the `band_group` assignment in the band registry (ADR-017) and the
light curve rendering logic in the frontend photometry panel (ADR-013, ADR-014).

---

## Definitions

- **Band label:** The SVO `Band` field value (e.g., `V`, `J`, `Ks`), or the NovaCat
  band registry's canonical band ID.
- **Filter ID:** The unique SVO filter identifier (e.g., `HST/ACS_WFC.F435W`) or
  NovaCat-assigned equivalent.
- **Normalized overlap:** The Bhattacharyya coefficient between two filters'
  transmission curves, each normalized to unit integral:
  `overlap = ∫ √(T_a(λ) · T_b(λ)) dλ`, where `∫T(λ)dλ = 1` for each filter.
  Bounded [0, 1]; 1 = identical bandpass shapes.
- **Band group:** A set of filters whose measurements may appear on the same light
  curve axis without correction. Each band group has a canonical representative filter.

---

## Tier 1 — Standard Rules (Default)

These rules are appropriate for light curve visualization, discovery-phase analysis,
and any context where ~50–100 mmag systematics are tolerable. This is the default
for the NovaCat website light curve panel.

### Rule S1: Same Band Label → Same Group (with exceptions)

Measurements sharing the same SVO `Band` label may be grouped on a single light curve,
**provided** the band label is not on the exception list (Rule S3).

**Rationale:** The SVO Band field, despite covering only ~12% of the database, correctly
groups the filters most commonly encountered in nova photometry. Within each band group
at this tier, the median pairwise overlap exceeds 0.80 for all core bands after outlier
exclusion.

### Rule S2: Outlier Exclusion at 0.60 Overlap Threshold

Filters whose mean intra-band overlap falls below **0.60** are excluded from the band
group and treated as independent filters. These are typically narrowband emission-line
filters (e.g., HeI 1.083 μm, Pa-gamma, FeII) that happen to sit within a broadband
atmospheric window and received a broadband label in the SVO.

This threshold removes approximately 2.5% of labeled filters and eliminates the most
extreme systematics (e.g., 1750 mmag spread in J-band reduced to < 350 mmag).

### Rule S3: Mandatory Band Splits

The following band labels contain scientifically distinct populations that **must not**
be grouped together, even under Tier 1:

| Band label | Split into | Rationale |
|---|---|---|
| `I` | `I` (Johnson/Bessell I) and `Ic` (Cousins Ic) | 0.65 overlap; 270+ mmag systematic at 10,000 K. AAVSO already distinguishes these. |
| `K` | `K` (Johnson K) and `Ks` (2MASS Ks) | Different red cutoffs; 100+ mmag systematic. SVO already labels these separately. |

**Implementation note:** The AAVSO band codes `I` and `Ic` map to the split groups
directly. If an AAVSO observation is labeled `I` without further qualification, it is
assigned to the Johnson/Bessell I group (conservative default, matching the AAVSO
convention for CCD observers using standard filter sets).

### Rule S4: AAVSO Band Label Mapping

AAVSO band labels are mapped to canonical SVO filter IDs as follows. The canonical
filter serves as the representative for its band group:

| AAVSO band | Canonical SVO filter | Band group |
|---|---|---|
| `U` | `Generic/Bessell.U` | U |
| `B` | `Generic/Bessell.B` | B |
| `V` | `Generic/Bessell.V` | V |
| `R` / `Rc` | `Generic/Bessell.R` | R |
| `I` | `Generic/Bessell.I` | I |
| `Ic` | *Cousins Ic representative (TBD from SVO)* | Ic |
| `J` | `2MASS/2MASS.J` | J |
| `H` | `2MASS/2MASS.H` | H |
| `K` | `2MASS/2MASS.Ks` | Ks |
| `u'` / `SU` | `SLOAN/SDSS.u` | u |
| `g'` / `SG` | `SLOAN/SDSS.g` | g |
| `r'` / `SR` | `SLOAN/SDSS.r` | r |
| `i'` / `SI` | `SLOAN/SDSS.i` | i |
| `z'` / `SZ` | `SLOAN/SDSS.z` | z |

### Rule S5: Unknown Provenance

When the specific filter used for a measurement is unknown (as is typical for AAVSO
data), the measurement is assigned to the band group corresponding to its reported band
label via Rule S4. No correction is applied. The `filter_provenance` field on the
`PhotometryRow` is set to `band_label_only` to indicate that the exact filter is
unknown.

---

## Tier 2 — Strict Rules (Precision Photometry)

These rules are appropriate for science-grade analysis where systematics must be
controlled to < 30 mmag, such as photometric modeling, distance determinations, or
comparison with theoretical light curves.

### Rule P1: Overlap Threshold at 0.90

Only filters with pairwise normalized overlap ≥ **0.90** may be grouped. Below this
threshold, measurements are treated as distinct photometric systems requiring explicit
color corrections before combination.

**Impact:** This threshold excludes approximately 30% of labeled filters from their
nominal band groups. Some bands (notably `z`) are entirely dissolved because no pair
of z-band filters in the SVO achieves 0.90 mutual overlap — a consequence of the
detector-dependent red cutoff defining the z-band edge.

### Rule P2: Temperature-Dependent Systematics Warning

Even at the 0.90 overlap threshold, filter-to-filter systematics remain significant
for cool sources. The following empirical bounds apply (derived from blackbody synthetic
photometry across the temperature range 3,500–25,000 K):

| Band | Systematic floor at 5,800 K | Systematic floor at 3,500 K |
|---|---|---|
| B | ~150 mmag | ~550 mmag |
| V | ~35 mmag | ~320 mmag |
| R | ~10 mmag | ~230 mmag |
| g | ~115 mmag | ~380 mmag |
| u | ~150 mmag | ~870 mmag |
| J | ~30 mmag | ~10 mmag |
| H | ~75 mmag | ~45 mmag |
| K/Ks | ~70 mmag | ~55 mmag |

For novae in late decline (T_eff < 5,000 K), optical-band measurements from different
filter systems should not be combined without correction, even under Tier 1 rules. The
near-IR bands are better behaved at all temperatures.

### Rule P3: Exact Filter Provenance Required

Under Tier 2, every measurement must carry the exact SVO filter ID (or equivalent
NovaCat band registry ID) of the filter used. Measurements with `filter_provenance =
band_label_only` (Rule S5) are excluded from Tier 2 analysis.

### Rule P4: Amateur Filter Equivalences

Based on the head-to-head synthetic photometry comparison of Astrodon, Baader, and
Chroma filter sets against the Bessell reference:

| Pair | Overlap | Max |Δmag| (5,800 K) | Verdict |
|---|---|---|---|
| Baader V vs Bessell V | 0.997 | 0 mmag | Interchangeable |
| Astrodon V vs Bessell V | 0.979 | 11 mmag | Interchangeable (Tier 1) |
| Baader I vs Bessell I | 0.987 | 1 mmag | Interchangeable |
| Astrodon Ic vs Bessell I | 0.655 | 144 mmag | **Not interchangeable** |
| Chroma I vs Bessell I | 0.993 | 16 mmag | Interchangeable |
| Baader B vs Bessell B | 0.892 | 67 mmag | Marginal (Tier 1 only) |
| Chroma B vs Bessell B | 0.959 | 46 mmag | Marginal (Tier 1 only) |

Baader and Chroma both implement Bessell-prescription I; Astrodon implements Cousins
Ic. This is the primary source of the I/Ic split in Rule S3.

---

## Implementation Notes

### Band Registry Integration (ADR-017)

Each band registry entry should carry:

- `band_group`: the grouping label used for light curve display (Tier 1)
- `band_group_strict`: the grouping label used for precision analysis (Tier 2), which
  may be more granular than `band_group`
- `canonical_filter_id`: the SVO filter ID of the representative filter for this group
- `overlap_threshold_tier1`: 0.60 (for outlier exclusion)
- `overlap_threshold_tier2`: 0.90 (for strict grouping)
- `is_outlier`: boolean, true if mean intra-band overlap < 0.60

### Frontend Light Curve Panel (ADR-013, ADR-014)

The light curve panel renders all measurements in the same `band_group` on a single
axis by default (Tier 1 behavior). An advanced mode could allow switching to Tier 2
grouping, revealing the finer structure.

### Provenance Tracking

Every `PhotometryRow` should record:

- `filter_id`: the resolved NovaCat band registry ID
- `svo_filter_id`: the SVO FPS identifier, where known
- `filter_provenance`: one of `exact_match`, `system_match`, `band_label_only`

This enables downstream analysis to retrospectively apply Tier 2 rules to data that
was ingested under Tier 1.

---

## Empirical Basis

These rules are derived from analysis of:

- **10,664 SVO FPS filters** harvested 2026-03-20, with 100% transmission curve coverage
- **Pairwise normalized overlap** (Bhattacharyya coefficient) computed for all filters
  within each of 13 band groups (B, V, R, I, J, H, K, Ks, u, g, r, i, z)
- **Synthetic photometry** through blackbody SEDs at 3,500 K, 5,800 K, 10,000 K, and
  25,000 K, using photon-counting CCD response
- **Head-to-head comparison** of Astrodon, Baader, and Chroma commercial filter sets
  against the OAF/Bessell reference filters from the SVO database
- **Outlier identification** via per-filter mean intra-band overlap, with visual
  verification of flagged filters' transmission curves

The analysis code, data, and outputs are archived in the `SVO_Database_Harvest/`
working directory.
