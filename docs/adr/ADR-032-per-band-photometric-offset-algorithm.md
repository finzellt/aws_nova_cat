# ADR-032: Per-Band Photometric Offset Algorithm

**Status:** Proposed
**Date:** 2026-03-30
**Author:** TF
**Supersedes:** —
**Superseded by:** —
**Amends:** —
**Relates to:**
- `ADR-013` — Visualization Design (Spectra and Photometry); defers the offset
  algorithm to this ADR (Open Question 6); establishes half-integer rounding, legend
  display format, and the "Clarity over Completeness" visualization philosophy
- `DESIGN-003` §8 — `photometry.json` generator; the Fargate execution context in
  which this algorithm runs
- `ADR-019` — Photometry Table Model Revision; `PhotometryRow` schema consumed here
- `ADR-017` — Band Registry Design; `band_id` as the canonical band identifier
- `docs/specs/band_grouping_rules.md` — Band grouping rules; defines which filters
  share a light curve trace (this ADR operates on grouped traces, not individual
  filters)

---

## 1. Context

ADR-013 establishes that when multiple photometric bands occupy overlapping magnitude
ranges, a constant per-band magnitude offset is applied to vertically separate the
traces. It specifies the user-facing contract — half-integer increments, explicit legend
annotation (e.g., "R (+1.5)"), zero offset when natural separation is sufficient — and
defers the computation to this ADR.

The problem is non-trivial because nova light curves are time series, not static point
clouds. Two bands may be well separated at early epochs but converge as the nova fades,
or they may cross entirely (e.g., an initially blue nova reddening through decline). A
constant offset must produce adequate separation across the **entire** temporal extent
of the data, not just at the epochs where overlap happens to be worst.

The naive approach — computing pairwise minimum gaps across all raw observation
timestamps — is correct but provides no structural insight into *where* and *why*
overlap occurs. This matters because the offset budget should be spent on genuine
physical overlap, not on noise-induced near-collisions between sparsely sampled bands.

### 1.1 Practical Scale

NovaCat's photometric data is scoped to optical-regime light curves within a single
wavelength regime tab (ADR-013). The number of distinct band groups *n* displayed on a
single plot is small — typically 3–6 (U, B, V, R, I, plus occasionally unfiltered),
rarely exceeding 8. The number of observations *T* per band ranges from tens to low
thousands after the density-preserving log subsampling applied upstream (500-point cap
per regime, per ADR-013).

This scale makes exhaustive algorithmic approaches tractable and eliminates the need
for heuristic approximations in the ordering search.

### 1.2 Generalisation Note

This algorithm is designed for photometric band separation but is not inherently
nova-specific. Any transient catalog displaying multi-band optical light curves faces
the same problem. The algorithm, separation threshold, and offset rounding convention
are candidates for extraction into a shared visualization utility if the project
generalises beyond novae.

---

## 2. Decisions

### Decision 1 — Piecewise Smooth Approximation

Each band's time series is approximated as a **cubic smoothing spline** before offset
computation. The algorithm operates on these spline representations, not on raw
observation points.

**Rationale.** The spline representation provides three structural advantages:

1. **Noise filtering.** Two bands may appear to nearly collide at the raw-point level
   due to photometric scatter, but their smoothed trajectories remain well separated.
   Operating on splines avoids spending offset budget on phantom near-collisions.

2. **Analytic gap structure.** The difference of two cubic splines is itself a piecewise
   polynomial. Its minimum on each segment can be found analytically (root-finding on
   the derivative — a quadratic per segment), and all crossings (roots of the
   difference) can be enumerated exactly. This provides a complete structural picture
   of where bands approach, diverge, and cross.

3. **Computational compression.** Downstream pairwise analysis operates on O(*S*)
   spline segments rather than O(*T*) raw points per pair, where *S* ≪ *T*. The
   fitting cost is O(*nT*) total (one tridiagonal solve per band); the pairwise
   analysis cost is O(*n*² · *S*).

**Smoothing parameter.** The smoothing parameter *s* controls the tradeoff between
fidelity and noise suppression. It must be calibrated so that the spline residual
(maximum deviation from the raw data) is well below the separation threshold ε
(Decision 3). The recommended approach is `scipy.interpolate.UnivariateSpline` with
the default GCV-based smoothing, validated empirically against a representative set
of nova light curves during implementation. If the maximum residual exceeds ε/4 for
any band, the smoothing should be relaxed (higher *s*) for that band.

**Edge case — sparse bands.** A band with fewer than 4 observations cannot support a
cubic spline. Such bands are represented as piecewise linear interpolants. The gap
analysis adapts accordingly (linear difference → constant derivative → trivial
minimum-finding). A band with fewer than 2 observations is excluded from offset
computation entirely; it receives zero offset and is plotted at its natural position.

---

### Decision 2 — Global Ordering with Exhaustive Search

The offset problem is decomposed into two layers: choosing a **vertical ordering** of
all *n* bands (a permutation), then computing the minimum offsets that enforce that
ordering.

#### Why a global ordering?

Each band receives a single constant offset (ADR-013 requirement). This means the
relative vertical position of any two bands after offsets is fixed for all time. The
algorithm must therefore choose a single ordering σ(1) < σ(2) < … < σ(n) (bottom to
top in the inverted-magnitude convention) that holds everywhere.

#### Ordering search

For a fixed ordering, only **consecutive pairs** in the ordering need explicit
separation constraints — transitivity guarantees non-consecutive pairs are separated
by at least 2ε. This reduces the problem to a chain of *n* − 1 pairwise constraints.

For each candidate ordering:

1. For each consecutive pair (σ(k), σ(k+1)), compute the **binding constraint**: the
   minimum additional separation required beyond what the spline trajectories already
   provide. Formally: c_k = ε − min_t[f_{σ(k+1)}(t) − f_{σ(k)}(t)], where the
   minimum is computed analytically over all spline segments (Decision 1). If c_k ≤ 0,
   the pair is already separated; no offset is needed.

2. Anchor one band at δ = 0 and propagate offsets along the chain:
   δ_{σ(k+1)} = δ_{σ(k)} + max(0, c_k). This is deterministic — no optimisation
   solver is required.

3. Evaluate the total offset cost: Σ|δ_i|.

The algorithm evaluates **all *n*! permutations** and selects the ordering with minimum
total offset cost.

**Tractability.** At *n* = 8, this is 40,320 permutations, each requiring *n* − 1
pairwise gap lookups from a precomputed table. Total work: O(*n*! · *n*), which is
negligible at this scale (< 300K operations). The pairwise gap precomputation
(O(*n*² · *S*)) dominates, and even that is modest.

**Assumption.** This decision assumes *n* ≤ ~10 for optical-regime band counts. If
the problem scales beyond this (e.g., a future multi-regime combined plot), the
exhaustive search should be replaced with a heuristic — sorting bands by mean
magnitude as the initial ordering, then improving via adjacent-pair swaps. This is
the **only** part of the algorithm that does not scale to arbitrary *n*, and it is
explicitly flagged here for that reason.

#### Objective function

The algorithm minimises **Σ|δ_i|** (total absolute offset). This concentrates offset
onto the fewest bands, leaving as many as possible at their natural positions — which
is the least surprising outcome for a researcher examining the light curve.

**Rejected alternative — min-max:** Minimising max|δ_i| spreads offset evenly, which
can produce a plot where every band has been shifted and none are at their true
position. This is harder to reason about scientifically.

---

### Decision 3 — Separation Threshold

The minimum required separation between consecutive bands after offsets is
**ε = 0.5 mag**.

**Rationale.** 0.5 mag is the smallest half-integer increment permitted by the
rounding convention (ADR-013). It provides clear visual separation in the Plotly.js
renderer at typical zoom levels while avoiding gratuitous displacement of bands that
are only marginally overlapping.

**Interaction with rounding.** After the optimal ordering and raw offsets are computed,
each non-zero offset is **rounded up** to the nearest half-integer (0.5, 1.0, 1.5, …)
as required by ADR-013. Rounding up (not to nearest) ensures the separation guarantee
is never violated by the rounding step.

---

### Decision 4 — Crossing-Aware Constraint Evaluation

For each pair of bands (i, j), the pairwise analysis produces:

- **min_gap:** min_t[f_i(t) − f_j(t)] — the closest approach when i is above j
- **max_gap:** max_t[f_i(t) − f_j(t)] — equivalently, −min_gap for the reversed
  ordering
- **crossing_count:** the number of times the two spline trajectories cross
  (roots of f_i(t) − f_j(t) = 0)

These are computed analytically from the piecewise polynomial difference (Decision 1).
Crossings are roots of the difference polynomial on each segment, found via standard
polynomial root-finding (quadratic formula for cubic spline differences' extrema;
direct root-finding for the crossings themselves).

The **crossing count** is not used directly in the offset computation but is logged as
diagnostic metadata. A pair with zero crossings has a stable natural ordering; a pair
with many crossings indicates bands that interleave extensively, and the chosen global
ordering will necessarily "fight" the natural trajectory for part of the time range.
High crossing counts correlate with larger required offsets — logging this helps the
operator understand *why* a particular nova's light curve received large offsets.

---

### Decision 5 — Zero-Offset Fast Path

Before running the full algorithm, a **fast-path check** evaluates whether any offsets
are needed at all.

For every pair (i, j), if the absolute minimum gap (min of |f_i(t) − f_j(t)| over all
t) exceeds ε, the pair is naturally separated regardless of ordering. If **all** pairs
pass this check, every band receives zero offset and the algorithm exits immediately.

This is the expected common case for novae with sparse photometry or well-separated
bands, and it avoids the permutation search entirely.

---

### Decision 6 — Output Contract

The offset algorithm produces, for each band group present in the subsampled data:

| Field | Type | Description |
|---|---|---|
| `band_id` | `str` | Canonical band identifier (ADR-017) |
| `offset_mag` | `float` | Applied offset in magnitudes (0.0 if no offset needed). Always a non-negative half-integer multiple: 0.0, 0.5, 1.0, 1.5, … |
| `offset_direction` | `str` | `"fainter"` or `"none"`. Indicates whether the band was shifted toward fainter magnitudes (larger numerical values). `"none"` when `offset_mag == 0.0`. |

The `photometry.json` artifact carries these offsets in its `band_metadata` array. The
frontend applies the offset at render time by adding `offset_mag` to each observation's
magnitude value (since fainter = numerically larger in the inverted magnitude system,
adding a positive offset shifts the trace downward on the inverted axis, i.e., visually
downward = fainter). The frontend displays the offset in the legend per ADR-013:
e.g., "R (+1.5)".

**Sign convention.** Offsets are always non-negative. The ordering algorithm determines
which bands need to move and in which direction; the output normalises this to a
non-negative displacement from the band's natural position in the "fainter" direction.
This is a rendering convenience: the frontend never needs to reason about offset sign.

**No offset applied to the anchor band.** The band that would be at the top of the
inverted-magnitude plot (brightest) in the optimal ordering receives zero offset by
construction (it is the anchor of the propagation chain). All other offsets are relative
displacements downward from it.

---

### Decision 7 — Execution Context

The offset algorithm runs inside the **Fargate artifact generation task** (DESIGN-003
§4.4) as part of the `photometry.json` generator. It executes after density-preserving
log subsampling and non-constraining upper limit suppression (both specified in ADR-013),
operating on the subsampled, filtered dataset.

**Dependency:** `scipy` (for `UnivariateSpline`) must be available in the Fargate
container image. This is the only new dependency introduced by this algorithm; `numpy`
and `itertools` are already available.

---

## 3. Algorithm Summary

The following pseudo-algorithm summarises the full procedure. Variable names correspond
to the decisions above.

```
INPUTS:
  bands[1..n]:  subsampled, filtered observation arrays (MJD, magnitude)
  ε = 0.5:     separation threshold (Decision 3)

STEP 1 — Fit splines (Decision 1)
  FOR each band i:
    IF len(bands[i]) >= 4:
      spline[i] ← cubic smoothing spline fit to bands[i]
    ELIF len(bands[i]) >= 2:
      spline[i] ← piecewise linear interpolant of bands[i]
    ELSE:
      EXCLUDE band i from offset computation; assign offset = 0.0

STEP 2 — Precompute pairwise gaps (Decision 4)
  FOR each pair (i, j) where i < j:
    g_ij(t) ← spline[i](t) − spline[j](t)    // piecewise polynomial
    min_gap[i][j] ← analytic minimum of g_ij(t) over shared time domain
    max_gap[i][j] ← analytic maximum of g_ij(t) over shared time domain
    crossing_count[i][j] ← number of roots of g_ij(t) = 0

STEP 3 — Fast-path check (Decision 5)
  IF for all pairs (i, j): min(|min_gap[i][j]|, |max_gap[i][j]|) > ε:
    RETURN all offsets = 0.0

STEP 4 — Exhaustive ordering search (Decision 2)
  best_cost ← ∞
  best_ordering ← null
  best_offsets ← null

  FOR each permutation σ of [1..n]:
    offsets[σ(1)] ← 0.0
    FOR k = 1 to n − 1:
      // Required separation between consecutive bands in this ordering
      // Note: in inverted magnitude, σ(k) is "above" (brighter) σ(k+1)
      c_k ← ε − min_gap_for_ordering(σ(k), σ(k+1))
      offsets[σ(k+1)] ← offsets[σ(k)] + max(0, c_k)

    cost ← Σ |offsets[i]| for all i
    IF cost < best_cost:
      best_cost ← cost
      best_ordering ← σ
      best_offsets ← offsets

STEP 5 — Round up to half-integer (Decision 3)
  FOR each band i:
    IF best_offsets[i] > 0:
      best_offsets[i] ← ceil(best_offsets[i] * 2) / 2    // round up to 0.5

STEP 6 — Emit output (Decision 6)
  FOR each band i:
    EMIT {
      band_id: band_id[i],
      offset_mag: best_offsets[i],
      offset_direction: "fainter" if best_offsets[i] > 0 else "none"
    }
```

**`min_gap_for_ordering(a, b)`:** Returns the precomputed `min_gap[a][b]` if a < b,
or `−max_gap[b][a]` if a > b. This handles the asymmetry: the minimum of
(f_a − f_b) equals the negation of the maximum of (f_b − f_a).

---

## 4. Consequences

### 4.1 Immediate

- The `photometry.json` generator (DESIGN-003 §8) gains the offset computation as a
  processing step between subsampling and output serialisation.
- `scipy` is added to the Fargate container image dependencies.
- ADR-013 Open Question 6 is resolved.

### 4.2 Testing

The algorithm is unit-testable with synthetic data:

- Two non-overlapping bands → both offsets = 0.0 (fast path)
- Two bands with constant gap < ε → one offset = 0.5 (minimum half-integer)
- Two crossing bands → offset sufficient to separate after rounding
- Sparse band (< 4 points) → piecewise linear fallback
- Single-point band → excluded, offset = 0.0
- Permutation optimality: construct a case where the natural mean-magnitude ordering
  is suboptimal, verify the algorithm finds the better ordering

### 4.3 Risks and Tradeoffs

- **Spline fidelity.** Aggressive smoothing could mask genuine close approaches between
  bands. The ε/4 residual guard (Decision 1) mitigates this, but empirical validation
  against real nova light curves is required during implementation.
- **Constant offset for crossing bands.** When two bands cross, no constant offset can
  keep both at their natural positions everywhere. The algorithm resolves this by
  choosing the ordering that minimises total displacement, but the offset will be
  "wrong" (larger than necessary) on one side of the crossing. This is inherent to the
  constant-offset constraint from ADR-013 and is not a deficiency of the algorithm.
- **Rounding can overshoot.** Rounding up to the nearest half-integer can produce more
  separation than strictly necessary. This is acceptable — ADR-013 requires
  half-integer increments, and the visual result is clean, publication-style spacing.

---

## Links

- ADR-013 — Visualization Design (Open Question 6 resolved here)
- DESIGN-003 — Artifact Regeneration Pipeline (§8, photometry.json generator)
- ADR-017 — Band Registry Design (band_id consumed here)
- ADR-019 — Photometry Table Model Revision (PhotometryRow schema)
- `docs/specs/band_grouping_rules.md` — Band grouping rules (upstream input)
