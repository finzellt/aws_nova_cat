# Intra-Band Filter Consistency in the SVO Filter Profile Service: Implications for Multi-Provenance Nova Photometry

**Document class:** Technical Report
**Date:** 2026-03-21
**Author:** NovaCat Project (analysis conducted in collaboration with Claude, Anthropic)
**Status:** Internal working document — candidate for external publication

---

## Abstract

We present an empirical analysis of filter-to-filter consistency within photometric band
groups, using the complete SVO Filter Profile Service database (10,664 filters) and
synthetic photometry of commercially available amateur filters. The analysis was motivated
by a practical question in the design of the Open Nova Catalog ingestion pipeline: under
what conditions can photometric measurements from different filter provenance be safely
combined on a single light curve?

We find that the SVO's `Band` field, while covering only 11.9% of the database, provides
a usable first-order grouping for the bands most relevant to nova photometry. However,
approximately 2.5% of labeled filters are narrowband interlopers (emission-line filters
misassigned to broadband groups), and the I-band label conflates the genuinely distinct
Johnson/Bessell I and Cousins Ic bandpasses. After outlier exclusion, filter-to-filter
systematics remain strongly temperature-dependent: negligible (< 30 mmag) for hot sources
(> 10,000 K) in most bands, but reaching 200–550 mmag for cool sources (3,500 K) in the
optical. Near-IR bands are better behaved at all temperatures.

A head-to-head comparison of Astrodon, Baader, and Chroma amateur filter sets shows that
all three are generally interchangeable at the 10–70 mmag level for solar-type sources,
with the critical exception of the I-band, where Astrodon's Cousins Ic design diverges
from the Bessell I prescription used by Baader and Chroma (overlap 0.65, systematic
up to 360 mmag).

---

## 1. Motivation

The Open Nova Catalog (NovaCat) aggregates photometric observations of classical novae
from heterogeneous sources: professional surveys, literature compilations, and amateur
observer networks (principally AAVSO). A fundamental challenge in this aggregation is
that observations labeled with the same band name — "V", "J", etc. — may have been
obtained through physically different filters with different transmission profiles. The
question of when such measurements can be combined without introducing scientifically
significant systematics is central to the design of the NovaCat ingestion pipeline and
the presentation of light curves on the NovaCat website.

This question has particular urgency for nova photometry because novae span a wide range
of effective temperatures during their evolution — from > 25,000 K at early outburst to
< 4,000 K in late decline. Filter-to-filter systematics are SED-dependent, and the
magnitude of the effect varies dramatically across this temperature range.

---

## 2. Data

### 2.1 SVO Filter Profile Service Harvest

The SVO FPS database was harvested on 2026-03-20 using a custom two-phase harvester.
The SVO's `fps.php` API endpoint returns only a service description when called without
search parameters; the harvester first extracts the list of 220 facilities from the
service description, then queries each facility individually to collect filter metadata
and transmission curves.

Key statistics of the harvested database:

| Metric | Value |
|---|---|
| Total filters | 10,664 |
| Filters with transmission curves | 10,664 (100%) |
| Total transmission curve data points | 5,592,695 |
| Points per curve (mean / min / max) | 524 / 4 / 15,730 |
| Unique Band labels | 93 |
| Filters with a Band label | 1,269 (11.9%) |
| Facilities | 220 |
| Instruments | 482 |
| Database file size | 428 MB (SQLite) |

The low Band label coverage (11.9%) is expected: the majority of filters in the SVO are
instrument-specific (e.g., HST/ACS_WFC.F435W) and not assigned to a generic photometric
band. The 1,269 labeled filters cover the bands most relevant to ground-based photometry.

### 2.2 Amateur Filter Transmission Curves

Transmission curves for three commercially available filter sets were obtained:

- **Astrodon Photometrics:** B, V, Rc, Ic (interference filters)
- **Baader Planetarium:** U, B, V, R, I (hybrid glass + interference)
- **Chroma Technology:** U, B, V, R, I (interference filters)

The Astrodon curves were digitized from manufacturer PDF datasheets using a custom
plot digitizer (color-based pixel extraction with axis calibration). The Baader and
Chroma curves were obtained from manufacturer-published data. All curves were verified
by visual comparison with the published plots.

### 2.3 Reference Filters

The OAF/Bessell filter set from the SVO database (OAF/Bessell.U, .B, .V, .R, .I)
was used as a reference standard, representing the canonical Bessell glass prescription
for the Johnson-Cousins system.

---

## 3. Methods

### 3.1 Normalization and Interpolation

All transmission curves were normalized to unit integral (∫T(λ)dλ = 1) before
comparison. This normalization models the effect of standard star calibration, which
divides out the absolute throughput of the filter+detector system. The shape of the
normalized curve — where the bandpass edges fall, how the wings taper — is what
determines filter-to-filter systematics.

Curves were interpolated onto a common 1 Å wavelength grid using linear interpolation
with zero-fill outside the native wavelength range. This handles heterogeneous sampling
(some SVO curves have 4 points, digitized curves have uneven spacing) and enables
direct numerical comparison.

### 3.2 Overlap Metric

Filter similarity was quantified using the Bhattacharyya coefficient:

    overlap(A, B) = ∫ √(T_A(λ) · T_B(λ)) dλ

where T_A and T_B are the normalized transmission curves. This metric is bounded [0, 1],
symmetric, equals 1 for identical bandpass shapes, and equals 0 for non-overlapping
filters. It is SED-independent — a purely geometric measure of bandpass similarity.

The Bhattacharyya coefficient was chosen over alternatives (e.g., simple area overlap,
cross-correlation) because it naturally handles filters with different support regions
and has a probabilistic interpretation as the overlap between two probability
distributions.

### 3.3 Outlier Identification

For each filter within a band group, the mean pairwise overlap with all other filters
in the band was computed. Filters with mean overlap below a threshold were flagged as
outliers. Two thresholds were evaluated:

- **0.60 (standard):** Removes narrowband interlopers and mislabeled filters. Affects
  ~2.5% of labeled filters.
- **0.90 (strict):** Collapses each band to a single filter family. Affects ~30% of
  labeled filters.

Flagged filters were visually verified by plotting their transmission curves alongside
the band population. In all cases examined, low-overlap filters were either:
(a) narrowband emission-line filters within a broadband atmospheric window, or
(b) genuinely mislabeled (e.g., a Ks filter assigned Band = J).

### 3.4 Synthetic Photometry

To translate overlap values into physically meaningful systematics, synthetic magnitudes
were computed for each filter through a grid of blackbody SEDs at 3,500 K, 5,800 K,
10,000 K, and 25,000 K. The blackbody function was weighted by λ to model photon-counting
detectors (CCDs).

For each band at each temperature, the spread (max − min synthetic magnitude across all
filters) was computed. This spread represents the worst-case systematic error from
combining measurements through different filters in the same band.

---

## 4. Results

### 4.1 Band Label Coverage and Quality

Of the 93 unique Band labels in the SVO, 13 are relevant to ground-based nova photometry:
B, V, R, I, J, H, K, Ks, u, g, r, i, z. The filter counts and pairwise overlap
statistics for these bands (after 0.60 outlier exclusion) are:

| Band | N filters | Median overlap | Min overlap | % ≥ 0.90 |
|---|---|---|---|---|
| B | 90 | varies | varies | — |
| V | 99 | high | moderate | — |
| R | 64 | high | moderate | — |
| I | 64 | moderate | low (I/Ic mix) | — |
| J | 59 | high | moderate | — |
| H | 60 | high | moderate | — |
| K | 33 | moderate | moderate | — |
| Ks | 28 | very high | high | ~100% |
| u | 75 | moderate | low | — |
| g | 100 | moderate | moderate | — |
| r | 75 | high | moderate | — |
| i | 65 | high | moderate | — |
| z | 91 | low | low | ~0% |

(Exact values are in the machine-readable `overlap_summary.csv` output.)

Notable findings:

- **Ks is the most homogeneous band**, with near-perfect overlap across all 28 filters.
  This reflects the success of 2MASS in establishing Ks as a de facto standard.
- **z is the least homogeneous band.** No z-band filter pair achieves 0.90 overlap
  because the red edge is defined by detector sensitivity cutoff (silicon QE rolloff),
  not by filter design.
- **The I band is bimodal**, containing both Johnson/Bessell I (broad) and Cousins Ic
  (narrower). These are genuinely distinct bandpasses at ~0.65 mutual overlap.

### 4.2 Narrowband Contamination

Visual inspection of outlier filters confirmed that the 0.60 threshold cleanly
separates broadband photometric filters from narrowband interlopers. Examples of
flagged outliers:

- **J band:** HeI 1.083 μm, Pa-gamma, FeII 1.257 μm narrowband filters
- **H band:** Filters from the CIRCE instrument mislabeled as H (actually J and Ks)
- **K band:** Brackett-gamma narrowband filters
- **B band:** A submillimeter-regime filter erroneously assigned Band = B

The flagged filters also included cases of genuine mislabeling in the SVO — e.g., a
filter named `Sirius.Ks` assigned Band = J.

### 4.3 Temperature-Dependent Systematics

The synthetic photometry reveals a strong and physically expected pattern: filter-to-filter
systematics scale with the slope of the SED across the bandpass.

**Optical bands (B, V, R, u, g, r):** Systematics are minimized near the temperature
where the SED is flattest across the bandpass, and grow dramatically for cool sources
where the SED is steeply sloped (Wien tail). For B-band at 3,500 K, the spread exceeds
550 mmag — reflecting the exponential sensitivity of the Planck function at short
wavelengths. At 10,000 K, the same B-band spread is ~63 mmag.

**Near-IR bands (J, H, K, Ks):** Systematics are more uniform across temperature,
typically 50–100 mmag, because the Rayleigh-Jeans regime produces relatively flat SEDs
across narrow wavelength intervals. Ks is exceptionally clean at < 90 mmag at all
temperatures.

**The z-band anomaly:** At the 0.65 threshold, z-band shows 0 mmag spread — not because
the filters are consistent, but because only the most similar filters survived the cut.
This is an artifact of the threshold interacting with the z-band's inherent diversity.

### 4.4 Amateur Filter Comparison

The head-to-head comparison of Astrodon, Baader, and Chroma against the Bessell
reference yielded the following key findings:

**V band:** All three manufacturers produce V filters that are interchangeable at the
< 70 mmag level across the full temperature range. Baader V is essentially a perfect
Bessell clone (0.997 overlap, < 3 mmag at all temperatures). Astrodon and Chroma show
slightly larger offsets at temperature extremes but remain within typical CCD measurement
uncertainty.

**B band:** More variation. Baader B diverges from the Bessell reference at 3,500 K
(212 mmag), while Chroma and Astrodon track the reference more closely. At solar
temperatures, all three are within ~70 mmag.

**R band:** Astrodon Rc, Baader R, and Chroma R are all reasonably consistent
(< 100 mmag), though with different signs of offset — Astrodon and Chroma track each
other closely, while Baader diverges slightly at temperature extremes.

**I band:** The critical outlier. Astrodon sells a Cousins Ic (narrow, hard red cutoff)
while Baader and Chroma sell Bessell I (broad, glass-like rolloff). Overlap between
Astrodon Ic and the Bessell-type filters is only 0.65, with systematics reaching
360 mmag at 25,000 K. Baader I and Chroma I are nearly identical to each other and
to the Bessell reference (overlap > 0.98).

**U band:** Only Baader and Chroma were compared (no Astrodon U available). Moderate
agreement (overlap 0.897), with up to 115 mmag systematic at 5,800 K.

### 4.5 Implications for AAVSO Data

The results establish quantitative bounds on the systematic floor when combining AAVSO
observations from different observers using different commercial filters:

- **For hot novae (T > 10,000 K):** Filter-to-filter systematics are generally < 70 mmag
  in all bands except I. Comparable to or smaller than typical CCD measurement
  uncertainties (10–50 mmag). Band labels are sufficient for grouping.

- **For cool novae or late-decline phases (T < 5,000 K):** Optical-band systematics
  (B, V, u, g) can reach several hundred mmag — well above measurement noise. These
  systematics are irreducible without knowing the exact filter each observer used.

- **For AAVSO I-band data:** The I/Ic ambiguity is the single largest source of
  preventable systematic error. AAVSO's existing distinction between "I" and "Ic"
  band codes should be preserved and enforced in the NovaCat pipeline.

---

## 5. Discussion

### 5.1 The Wien Tail Effect

The dominant pattern in the optical-band systematics — large offsets at low temperatures,
small offsets at high temperatures — has a simple physical explanation. The Planck function
for a 3,500 K blackbody is exponentially steep through the B band (3800–4800 Å). Small
differences in where the blue edge of the filter falls translate to large differences in
the integrated flux. At 10,000 K, the SED is nearly flat across the same wavelength range,
and edge differences become negligible.

This is not a flaw in the filters or the analysis — it is a fundamental property of
broadband photometry of cool sources. It explains why the near-IR bands, where even
cool sources are in the Rayleigh-Jeans regime, show much smaller and more uniform
systematics.

### 5.2 The z-Band Problem

The z-band is unique in that its red edge is defined not by the filter but by the
detector. A silicon CCD cuts off at ~10,000 Å; a deep-depletion CCD extends to
~10,500 Å; an InGaAs detector goes further still. Every z-band filter in the SVO
therefore has a different effective bandwidth depending on what detector response was
folded into the published transmission curve. This makes z-band inherently the most
heterogeneous band in the optical/NIR regime, and explains why no z-band filter pair
achieves 0.90 mutual overlap.

For NovaCat purposes, z-band data should be treated with caution. The band label alone
is insufficient to determine the effective bandpass.

### 5.3 The Ks Success Story

In contrast to z-band, the Ks band is a model of homogeneity. All 28 Ks filters in the
SVO achieve near-perfect mutual overlap because they all derive from the 2MASS Ks
specification, which was precisely defined and widely adopted. The 2MASS survey's
influence effectively created a single Ks standard that all subsequent instruments
replicate. This demonstrates that band homogeneity is achievable when a dominant
survey establishes a clear standard — a lesson that LSST/Rubin may replicate for the
optical bands.

### 5.4 Bessell Is Not a Separate System

A recurring source of confusion in the amateur photometry community — and one we
encountered during this analysis — is the relationship between "Johnson-Cousins" and
"Bessell" filters. These are not competing systems. Bessell designed specific glass
filter combinations that reproduce the Johnson (UBV) and Cousins (RcIc) bandpasses
on silicon detectors. Modern interference filters (Astrodon, Baader, Chroma) are
designed to match the same bandpass shapes with higher throughput and durability.
All are implementations of the Johnson-Cousins system. The practical consequence is
that "Bessell B" and "Johnson B" should map to the same band group in the registry.

---

## 6. Conclusions

1. The SVO Band label provides a usable first-order grouping for nova photometry, but
   requires outlier exclusion (threshold 0.60) and a mandatory I/Ic split.

2. Filter-to-filter systematics are strongly temperature-dependent, reaching hundreds of
   mmag in optical bands for cool sources. Near-IR bands are better behaved at all
   temperatures.

3. Astrodon, Baader, and Chroma amateur filters are generally interchangeable at the
   < 70 mmag level for solar-type sources, with the critical exception of the I-band
   (Astrodon Ic vs Bessell I).

4. The z-band is inherently heterogeneous due to detector-dependent red cutoffs and
   should be treated with caution.

5. The Ks band is a model of filter standardization, demonstrating the value of a
   dominant survey establishing a clear specification.

6. For the NovaCat pipeline, Tier 1 rules (band label grouping with outlier exclusion)
   are appropriate for the website light curve display. Tier 2 rules (0.90 overlap
   threshold, exact filter provenance required) should be available for precision
   analysis.

---

## 7. Future Work

The following extensions are proposed, roughly in priority order.

### 7.1 Wavelength-Based Clustering for Unlabeled Filters (High Priority)

88% of SVO filters lack a Band label. A clustering algorithm based on effective
wavelength, FWHM, and/or transmission curve overlap could assign these filters to
band groups or identify them as narrowband/specialty filters. This would enable
NovaCat to accept photometry from a much wider range of instruments without requiring
manual filter identification.

### 7.2 Temperature-Aware Heterogeneity Flagging (Medium Priority)

When color information is available for a nova at a given epoch (e.g., from near-
simultaneous multi-band observations), the effective temperature can be estimated.
This temperature estimate could be used to flag epochs where the expected filter-to-
filter systematic exceeds a threshold. For example: "V-band measurements at this epoch
are from 5 different observer systems. Given the inferred T_eff = 4,200 K, the
expected systematic scatter is ~150 mmag, which exceeds the reported measurement
uncertainties."

### 7.3 Observer Filter System Inference (Medium Priority)

When multi-band photometry is available from a single AAVSO observer, comparison with
synthetic photometry from known filter sets could be used to infer which filter system
the observer is likely using. For example, if an observer's B−V colors for standard
stars consistently match the Baader synthetic values, their filter system can be
tentatively identified. This would enable retrospective Tier 2 analysis of AAVSO data.

### 7.4 Expanded Amateur Filter Library (Low Priority)

The current amateur comparison covers Astrodon, Baader, and Chroma. Adding Custom
Scientific (former SBIG filter supplier) and Optolong would improve coverage. Custom
Scientific filters are particularly important because SBIG cameras were the dominant
amateur CCD platform for over a decade.

### 7.5 Validation Against Multi-Observer Light Curves (Low Priority)

The synthetic photometry predictions could be validated by analyzing real multi-observer
light curves from the AAVSO database. For a well-observed nova with dense coverage from
many observers, the epoch-to-epoch scatter in excess of reported uncertainties provides
an empirical estimate of the systematic floor. Comparing this with our synthetic
predictions would validate (or refine) the overlap-based grouping thresholds.

### 7.6 Formal Publication (Future)

The methodology and findings in this report are, to our knowledge, novel in their
scope and specificity to the nova/transient photometry context. A peer-reviewed
publication (e.g., in PASP or the AAVSO journal) would serve the broader amateur and
professional photometry community.

---

## Appendix A: Software

All analysis code is archived in the NovaCat project working directory:

| Script | Purpose |
|---|---|
| `svo_harvest.py` | Two-phase SVO FPS database harvester with checkpointing |
| `svo_query.py` | Local query interface to the harvested SQLite database |
| `svo_aws.py` | S3 sync, Parquet export, Lambda helper for AWS deployment |
| `svo_diagnostic.py` | Database summary statistics and band coverage report |
| `svo_analysis.py` | Overlap computation and synthetic photometry (Tracks 1 & 2) |
| `svo_band_diagnostic.py` | Outlier identification with visual verification |
| `amateur_comparison.py` | Head-to-head amateur filter comparison |
| `digitize_curve.py` | Interactive plot digitizer for extracting curves from PDFs |

Dependencies: Python 3.11+, numpy, scipy, matplotlib, requests, astropy.

## Appendix B: Data Availability

The harvested SVO FPS database (SQLite, ~428 MB) and all analysis outputs (plots,
CSV tables, JSON exclusion lists) are available within the NovaCat project. The SVO
harvest was performed on 2026-03-20 against the `fps.php` endpoint at
`http://svo2.cab.inta-csic.es/theory/fps/`, returning 10,664 filters with zero
failures.
