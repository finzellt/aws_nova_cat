# ADR-033: Spectra Compositing Pipeline

**Status:** Proposed
**Date:** 2026-04-10
**Author:** TF
**Supersedes:** —
**Superseded by:** —
**Amends:** ADR-014 (adds composite spectrum record type to spectra artifact schema)
**Relates to:**
- `DESIGN-003` — Artifact regeneration pipeline (Fargate pre-processing phase)
- `ADR-013` — Visualization Design (waterfall plot; composites replace constituents)
- `ADR-014` — Artifact Schemas (spectrum record fields)
- `ADR-031` — Data Layer Readiness (DataProduct schema)

---

## 1. Context

The NovaCat catalog ingests fully reduced, flux-calibrated spectra from public
astronomical archives (ESO, CfA, AAVSO). For well-observed novae, the catalog may
contain multiple spectra of the same object taken on the same night with the same
instrument — for example, multiple exposures at different signal-to-noise ratios, or
sequential observations covering the same wavelength range.

These spectra are independently valid, but when displayed individually in the waterfall
plot they add visual clutter without adding scientific information. Compositing them —
resampling onto a common wavelength grid and averaging — produces a single higher-SNR
spectrum per instrument per night that better represents the data.

A prerequisite for compositing is cleaning: real archive spectra contain detector
artifacts (bad pixels, CCD chip gap residuals, dead column remnants) that manifest as
narrow (1–3 pixel) drops to zero or near-zero flux. These artifacts must be removed
before resampling, because interpolation would otherwise fold the bad values into the
output grid.

The existing spectra processing pipeline in the artifact generator already contains
cleaning functions (`_trim_dead_edges`, `_remove_interior_dead_runs`,
`_reject_chip_gap_artifacts`) that address these defects. The compositing pipeline
reuses these functions and adds resampling and combination steps.

### 1.1 Scope

This ADR covers **single-instrument compositing only**. Cross-instrument compositing
(e.g., combining a UVES spectrum with an X-Shooter spectrum from the same night)
is explicitly out of scope. Different instruments have independent flux calibrations,
and normalizing across instruments introduces systematic uncertainty that outweighs the
SNR benefit for a display-oriented catalog.

---

## 2. Decisions

### Decision 1 — Same-Instrument, Same-Night Compositing

Spectra are eligible for compositing when they share the same `instrument` value and
were observed on the same night. "Same night" is defined by a clustering algorithm
(Decision 2), not a fixed time window.

Only spectra with ≥ 2000 data points in the native-resolution FITS file are included
in a composite. Spectra below this threshold lack sufficient spectral resolution to
contribute meaningfully to the resampled grid and would degrade rather than improve
the composite. Spectra excluded by this criterion are recorded on the composite's
DynamoDB item (Decision 6) so the system can distinguish "considered and rejected"
from "not yet seen."

A compositing group of size 1 (a single spectrum that passed all criteria but has no
companions) produces no composite — the original spectrum passes through unchanged.

### Decision 2 — Night Clustering via Gap Detection

Spectra are grouped into observing nights using single-linkage clustering on
`observation_date_mjd`, with a gap threshold of **0.5 days (12 hours)**.

**Algorithm:**

1. For a given nova and instrument, collect all VALID spectra DataProducts.
2. Sort by `observation_date_mjd`.
3. Compute the MJD gap between each consecutive pair.
4. Split into groups wherever the gap exceeds 0.5 days.
5. Each resulting group represents one observing night.

This is equivalent to single-linkage hierarchical clustering with a distance cutoff,
which for sorted 1D data reduces to sequential gap detection. It is deterministic,
O(N log N) (dominated by the sort), and produces stable results regardless of which
spectrum is processed first.

**Rationale for 0.5 days:** An exhaustive survey of professional observatory sites
confirms that the minimum separation between consecutive astronomical twilights is
≥ 12 hours at all major spectroscopic facilities (ESO Paranal at −24° latitude,
La Silla at −29°, CTIO at −30°, etc.). Observatories at extreme latitudes (e.g., the
James Gregory Telescope in Scotland at +56°) can have twilight separations as short as
~11 hours near the summer solstice, but these are small teaching telescopes unlikely to
contribute spectra to the catalog. A 12-hour gap threshold therefore cleanly separates
consecutive nights without risk of merging observations from different nights.

The gap-based approach avoids the failure modes of a fixed window: it does not depend
on the starting point, does not chain across nights, and naturally handles long winter
nights where observations may span > 10 hours.

### Decision 3 — Processing Order: Clean → Resample → Combine

Each spectrum in a compositing group is processed through the following pipeline:

1. **Read native FITS** — extract wavelength and flux arrays at full instrument
   resolution using the existing FITS profile infrastructure.
2. **Clean** — apply the existing cleaning functions in order:
   - `_trim_dead_edges()` — strip detector sensitivity rolloff at array boundaries
   - `_remove_interior_dead_runs()` — remove consecutive near-zero flux runs (chip gaps)
   - `_reject_chip_gap_artifacts()` — remove isolated near-zero points at irregular
     wavelength spacing
3. **Resample** — interpolate each cleaned spectrum onto the common wavelength grid
   (Decision 4).
4. **Combine** — average flux values at each grid point across all resampled spectra.
   In wavelength regions where only a subset of spectra contribute, average over the
   contributing subset only.

Cleaning **must** precede resampling. If bad pixel values survive into the resampling
step, interpolation folds them into neighboring grid points, spreading the defect across
a wider wavelength range and making it unrecoverable.

Cosmic ray rejection is not included. The input spectra are fully reduced archival
products; cosmic ray cleaning is the responsibility of the instrument pipeline that
produced them.

### Decision 4 — Common Grid Resolution from Coarsest Input

The common wavelength grid for resampling is determined by the **coarsest-resolution
spectrum** in the compositing group, not by a fixed target point count.

**Algorithm:**

1. For each cleaned spectrum in the group, compute the median wavelength step
   (spacing between consecutive points).
2. The largest median step across all spectra in the group becomes the grid spacing.
3. The grid spans the full wavelength range covered by the union of all spectra
   in the group: `[min(all wl_min), max(all wl_max)]`.
4. The grid is uniformly spaced at the determined step size.

**Rationale:** A fixed grid size (e.g., 3000 points) risks under-resolving spectral
features if the coarsest input has higher native resolution than the fixed grid
implies, or over-resolving (creating phantom resolution) if the coarsest input is
lower resolution. Using the coarsest input as the reference guarantees that the
composite never claims more resolution than the data support.

This also preserves the effectiveness of downstream LTTB downsampling: the composite
may have significantly more points than the LTTB threshold (e.g., a coarse grid of
0.05 nm over 300–1100 nm produces ~16,000 points), giving LTTB real work to do in
selecting visually representative points. A fixed 3000-point grid would leave LTTB
with almost nothing to optimize.

Individual spectra are resampled onto this grid using linear interpolation. After
cleaning, the wavelength spacing within each spectrum is uniform (the irregular
spacing introduced by chip gaps has been removed), so linear interpolation is
well-conditioned.

There is no minimum wavelength overlap requirement between spectra in a compositing
group. If two same-night, same-instrument spectra cover entirely disjoint wavelength
ranges (e.g., blue arm and red arm observed separately), they are still composited
onto the union grid. Each contributes flux only in its own wavelength range, and the
combination step averages over the contributing subset at each grid point.

### Decision 5 — Composites Replace Constituents in Display, Not in Bundles

**Waterfall plot:** The composite spectrum replaces its constituent spectra. The
spectra generator in the artifact pipeline reads the composite's web-ready CSV
instead of the individual CSVs. The waterfall plot shows one entry per composite
per instrument per night, not N entries for N individual exposures.

**Bundle:** The research-grade data bundle (`bundle.zip`) includes the original
individual FITS files only. Composite spectra are derived display products and are
not included in the bundle. The bundle serves researchers who need the raw data;
the composite serves visualization consumers who need a clean summary.

**Observation table:** Individual spectra that are constituents of a composite
remain visible in the observation table (they are real DataProducts with real
metadata). The observation table is a data inventory, not a display summary.

### Decision 6 — Composite DataProduct Item in DynamoDB

A composite spectrum is represented as a DataProduct item in the main DynamoDB table
with `PK=NOVA#<nova_id>`, `SK=PRODUCT#SPECTRA#<provider>#COMPOSITE#<composite_id>`.
This follows the existing individual spectra SK pattern
(`PRODUCT#SPECTRA#<provider>#<data_product_id>`) with a `COMPOSITE` segment inserted
before the ID to distinguish composites at the key level.

This key structure supports the following query patterns:

- `begins_with("PRODUCT#SPECTRA#")` — returns both composites and individuals
  across all providers (used by the spectra generator for the waterfall plot input
  set).
- `begins_with("PRODUCT#SPECTRA#<provider>#COMPOSITE#")` — returns only composites
  for a specific provider (used by the compositing sweep for fingerprint checks,
  since compositing is per-instrument and instruments are provider-scoped).
- `begins_with("PRODUCT#SPECTRA#")` with a filter excluding SKs containing
  `COMPOSITE` — returns only individuals (used by the bundle generator).

**Additional fields beyond standard DataProduct attributes:**

| Field | Type | Description |
|-------|------|-------------|
| `constituent_data_product_ids` | `list[UUID] \| None` | DataProduct IDs of the spectra that were combined. Sorted deterministically. `None` for non-composite DataProducts. |
| `rejected_data_product_ids` | `list[UUID] \| None` | Same-night, same-instrument spectra that were considered but excluded (e.g., below 2000-point threshold). `None` for non-composite DataProducts. |
| `composite_fingerprint` | `str \| None` | Deterministic hash of sorted constituent IDs and their individual `sha256` content fingerprints. |
| `composite_s3_key` | `str \| None` | S3 key for the full-resolution composite CSV (common grid, pre-LTTB). Path: `derived/spectra/<nova_id>/<composite_id>/composite_full.csv`. |
| `web_ready_s3_key` | `str \| None` | S3 key for the LTTB-downsampled composite CSV (≤ 2000 points). Read by the spectra generator like any other web-ready CSV. Path: `derived/spectra/<nova_id>/<composite_id>/web_ready.csv`. |
| `instrument` | `str` | Shared instrument of the compositing group. |
| `observation_date_mjd` | `float` | Mean MJD of the constituent spectra. |

The `composite_fingerprint` enables **idempotent rebuilds** (Decision 7). The explicit
S3 key fields (`composite_s3_key`, `web_ready_s3_key`) allow the spectra generator and
compositing sweep to locate composite artifacts directly from the DDB item without
constructing paths by convention.

**Validator relaxation:** Composite DataProducts do not go through the archive
acquisition pipeline, so the `DataProduct` model validator skips `locator_identity`
and `acquisition_status` requirements when `constituent_data_product_ids` is set.
All other SPECTRA validations (provider, validation_status, eligibility) still apply.
See `contracts/models/entities.py`.

**Spectra generator filtering logic:** When building the waterfall plot, the spectra
generator queries with `begins_with("PRODUCT#SPECTRA#")` and then excludes any
individual DataProduct whose `data_product_id` appears in any composite's
`constituent_data_product_ids` or `rejected_data_product_ids`. Composites are
identified by their SK structure (containing the `COMPOSITE` segment). This prevents both
the composite and its constituents from appearing in the plot simultaneously, and
prevents rejected same-night spectra from appearing as though they were independent
observations.

### Decision 7 — Fingerprint-Based Rebuild Avoidance

Compositing is computationally expensive (FITS reads from S3, cleaning, resampling,
combination). The composite fingerprint prevents unnecessary recomputation.

**Rebuild decision tree (per compositing group):**

1. Identify the compositing group: all VALID spectra for this nova with the same
   instrument and observation night (per Decision 2).
2. Compute the expected composite fingerprint: deterministic hash of sorted
   constituent `data_product_id` values concatenated with their `sha256` content
   fingerprints.
3. Check if a composite DataProduct already exists with this fingerprint → **skip**.
4. If a composite exists with a **different** fingerprint (a new spectrum arrived,
   or a constituent was re-validated with different content) → **rebuild**, replacing
   the old composite DataProduct and its S3 artifacts.
5. If no composite exists → **build new**.

This means that a spectra dirty flag triggers the compositing check, but the
fingerprint short-circuits if the underlying data has not changed.

### Decision 8 — S3 Layout for Composite Artifacts

Each composite produces two S3 objects in the private bucket:

| Path | Description |
|------|-------------|
| `derived/spectra/<nova_id>/<composite_id>/composite_full.csv` | Full-resolution composite (common grid, pre-LTTB). Persisted so that LTTB threshold changes do not require recompositing from FITS. |
| `derived/spectra/<nova_id>/<composite_id>/web_ready.csv` | LTTB-downsampled composite (≤ 2000 points). Read by the spectra generator like any other spectrum's web-ready CSV. |

The `composite_id` is a deterministic UUID derived from the sorted constituent
`data_product_id` values (e.g., UUID v5 with a NovaCat namespace).

Individual spectra's web-ready CSVs and raw FITS files are not modified or removed.
They remain available for the bundle generator and for potential future recompositing
with different parameters.

### Decision 9 — Execution Context: Fargate Pre-Processing Phase

Compositing runs as a **new phase in the existing Fargate artifact generation task**,
before the per-nova artifact generators execute. The Fargate container has all
required dependencies: astropy (for FITS I/O, coordinate formatting, and date-time
conversion), S3 access, and DynamoDB access.

**Execution flow within the Fargate task:**

1. **Phase 1 — Compositing sweep:** For each nova in the regeneration plan, check
   for compositing groups. Build or rebuild composites as needed per Decision 7.
   Write composite CSVs to S3 and composite DataProduct items to DynamoDB.
2. **Phase 2 — Artifact generation:** Existing per-nova generators run as before.
   The spectra generator reads web-ready CSVs (now including composites) and applies
   LTTB, normalization, and artifact output.

Phase 1 is a no-op for novae with no compositable groups (the common case for novae
with sparse spectral coverage).

**Rationale for in-process rather than separate Fargate task:** Compositing needs
the same infrastructure access as artifact generation (S3, DynamoDB, astropy). A
separate task would add operational complexity (new task definition, new Step Functions
state, new IAM role) for no functional benefit. The compositing sweep is lightweight
for most novae and only expensive for the minority with dense same-night coverage.

---

## 3. Consequences

### Positive

- **Improved waterfall plot readability:** Dense same-night observations collapse into
  single high-SNR composites, reducing visual clutter while preserving temporal
  evolution between nights.
- **Higher SNR:** Compositing multiple exposures produces a combined spectrum with
  SNR_combined ≈ √(Σ SNR²_i) in overlap regions.
- **No new infrastructure:** Compositing reuses the existing Fargate task, cleaning
  functions, and web-ready CSV pipeline.
- **Idempotent and incremental:** Fingerprint-based rebuild avoidance ensures
  compositing is a no-op when data hasn't changed, and only recomputes affected
  composites when new spectra arrive.

### Negative

- **Increased Fargate task duration:** Phase 1 adds time for novae with compositable
  groups. The FITS reads from S3 are the dominant cost. For a nova with 10 same-night
  spectra, this might add 30–60 seconds.
- **Composite DataProduct adds schema complexity:** The `constituent_data_product_ids`,
  `rejected_data_product_ids`, `composite_fingerprint`, `composite_s3_key`, and
  `web_ready_s3_key` fields are new to the DataProduct schema, and the `COMPOSITE` SK
  segment introduces a new key pattern. The spectra generator's query logic must
  account for these. Documentation and contract updates have been applied
  (see Section 4).

### Risks

- **Night boundary edge cases:** The 0.5-day gap threshold assumes professional
  observatories at mid-latitudes. If the catalog expands to include data from
  extreme-latitude facilities, the threshold may need per-observatory tuning. This
  risk is mitigated by the gap threshold being a single configurable constant.
- **Flux calibration inconsistency within instrument:** Even same-instrument spectra
  can have relative flux offsets if taken under different atmospheric conditions or
  with different slit widths. The current design averages without flux matching. If
  this produces visible discontinuities in practice, a pre-combination flux scaling
  step could be added within the existing pipeline structure.

---

## 4. Implementation Prerequisites

The following documentation and contract updates have been completed (committed on
`epic/28-spectra-compositing`):

- ✅ `docs/storage/dynamodb-item-model.md` — composite DataProduct item type with
  `PRODUCT#SPECTRA#<provider>#COMPOSITE#` SK pattern
- ✅ `docs/storage/dynamodb-access-patterns.md` — composite query patterns
- ✅ `contracts/models/entities.py` — composite fields on DataProduct, validator
  relaxation for composites (applied via `patch_entities_adr033_composite.py`)
- ✅ `docs/architecture/current-architecture.md` — updated with compositing phase

**Remaining before implementation:**

- ADR-014 — note that `spectrum_id` can be a composite ID; consider adding
  `is_composite` field to the spectrum record for frontend awareness
- Artifact regeneration workflow documentation — add Phase 1 description
- **IAM verification:** Confirm that the Fargate task role has `s3:GetObject` on the
  private bucket for raw FITS paths (not just `derived/`), `s3:PutObject` for composite
  CSV paths, and `dynamodb:PutItem` / `dynamodb:UpdateItem` on the main table. These
  permissions are likely already granted but should be confirmed against the CDK
  construct in `infra/nova_constructs/workflows.py`.

---

## 5. Deferred

- Cross-instrument compositing (requires flux calibration normalization strategy)
- Inverse-variance weighted combination (requires error/uncertainty arrays, which
  not all archive products provide)
- Composite spectra in the research bundle (composites are display-only for now)
- Per-observatory gap threshold tuning

---

## 6. Links

- Related ADRs: ADR-013, ADR-014, ADR-031, ADR-032
- Design docs: DESIGN-003 (artifact regeneration pipeline)
- Prior discussion: chip gap artifacts in V906 Car UVES spectra; multi-arm merge
  design for X-Shooter and UVES in `generators/spectra.py`
