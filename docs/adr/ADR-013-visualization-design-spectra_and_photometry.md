# ADR-013: Visualization Design — Spectra

Status: Proposed
Date: 2026-03-17

> **⚠ Amended by ADR-031** (2026-03-31)
> ADR-031 (Data Layer Readiness for Artifact Generation, Decision 10) corrects the
> backend/frontend responsibility boundary for Days Post-Outburst (DPO) computation.
> DPO is pre-computed backend-side during artifact generation (DESIGN-003 §7.4, §8.9)
> and delivered as `days_since_outburst` in both the spectra and photometry artifacts
> (ADR-014). The boundary tables below are amended accordingly.
>
> ADR-031 Decision 8 also notes that the sparkline input pool (Catalog Sparkline
> section) is broadened by DESIGN-003 §9.2 beyond "Optical band only" to include the
> consolidated optical regime per DESIGN-003 §8.11.
>
> See: `docs/adr/ADR-031-data-layer-readiness-for-artifact-generation.md`

---

## Context

ADR-011 specifies Plotly.js as the visualization library and describes the spectra viewer
in broad terms: a waterfall layout with flux vs. wavelength, vertically offset by epoch.
ADR-012 explicitly defers all visualization and plot design to this ADR.

This ADR defines the complete design specification for the spectra viewer component,
covering layout, axes, normalization, color, interactions, epoch labeling, and spectral
feature markers. This ADR has been extended to include photometry visualizations (light curve panel,
catalog sparklines). Those design decisions were developed in a subsequent design session
and are recorded in the Photometry sections below.

---

## IVOA Compliance Check

The IVOA Spectrum Data Model v1.2 (REC-SpectrumDM-1.2-20231215) was reviewed for
visualization standards. No IVOA standards prescribe visual rendering of spectra — the
standard governs data interchange formats (VOTable, FITS, XML) and field semantics, not
display. Nanometres (nm) are explicitly valid as a spectral axis unit under the standard's
SI-prefix rules.

---

## Visualization Philosophy: Clarity over Completeness

A visualization crowded with every available data point risks becoming a display of
observational volume rather than a communication of scientific content. Where data
density obscures the story — through overlapping traces, compressed dynamic range, or
sheer point count — the catalog makes deliberate accommodations: representative
subsampling, per-band magnitude offsets, and suppression of non-constraining
measurements. The goal is always to surface the signal, not to enumerate the record.

This is not a scientific compromise: the complete dataset is available in the curated
bundle download for researchers who need it.

This principle governs visualization decisions throughout this ADR — for both the
spectra viewer and the light curve panel.

---

## Scope

This ADR covers:

- Waterfall plot layout and rendering
- Axis design (wavelength, temporal, epoch labels)
- Flux normalization strategy
- Color scheme
- Density and temporal spacing rules
- Interactive controls
- Spectral feature markers
- Visualization design philosophy
- Light curve panel design (wavelength regime tabs, per-regime axes, interactions,
  dynamic range)
- Catalog sparkline design
- Backend / frontend responsibility boundary
- Empty and error states

This ADR does **not** cover:

- Artifact schema definitions (deferred to a dedicated artifact schema ADR)
- Per-band offset computation algorithm (deferred to a dedicated backend
  responsibilities ADR)

---

## Backend / Frontend Responsibility Boundary

A clean boundary between backend artifact generation and frontend presentation logic is
required to keep the frontend free of computation-heavy transforms.

### Backend responsibilities (performed during artifact generation)

- Spectral downsampling / de-resampling of high-resolution spectra for render performance
- Per-spectrum flux normalization (to per-spectrum peak; see Normalization section)
- Computing wavelength range coverage per spectrum
- Storing per-spectrum metadata: MJD, instrument, wavelength range
- Determining outburst date from the literature where possible
- Where outburst date cannot be determined from the literature, substituting the earliest
  available observation (photometric or spectroscopic) as the Day 1 reference, and
  recording a flag indicating the substitution was made
- Pre-computing Days Post-Outburst (DPO) per spectrum (`days_since_outburst` field in
  the artifact; see ADR-014 and DESIGN-003 §7.4)

### Frontend responsibilities (performed at render time)

- Axis scaling and epoch label format toggling
- Spectral lane amplitude scaling (dynamic, based on inter-spectrum gap)
- Representative subset sampling for dense plots
- Temporal gap metric calculation for default scale selection
- Color assignment per epoch
- Spectral feature marker rendering
- Spectrum selection / isolation UI

A future ADR defining artifact schemas should treat the fields listed under backend
responsibilities as hard dependencies for the spectra viewer.

---

## Waterfall Plot Layout

Spectra are rendered as a waterfall plot: flux vs. wavelength, with each spectrum offset
vertically by epoch. Oldest spectrum appears at the bottom; most recent at the top. This
layout gives an immediate visual impression of spectral evolution across the nova's
lifetime.

### Wavelength axis (X)

- Unit: **nanometres (nm)**
- Scale: linear
- Range: determined by the union of wavelength coverage across all available spectra for
  the nova, with sensible padding at each end

*Note: The Williams line list used for feature markers records wavelengths in Ångströms.
All values must be divided by 10 for display. This conversion is the frontend's
responsibility.*

### Temporal axis (Y)

The Y axis encodes epoch. Baseline positions are determined by the epoch of each spectrum
under the active temporal scale. The Y axis carries no absolute flux meaning in waterfall
mode; flux is encoded only within each spectrum's local lane.

### Spectral lane amplitude

Each spectrum is normalized to its own peak flux before rendering (see Normalization). The
rendered amplitude within each lane is set dynamically:

```
AMP = min_inter_spectrum_gap × 0.78
```

where `min_inter_spectrum_gap` is the minimum pixel distance between any two adjacent
spectrum baselines under the current scaling. This ensures that the tallest feature in any
spectrum reaches 78% of the available lane height, maximising feature legibility while
providing a clearance margin that prevents overlap between adjacent traces under normal
conditions.

---

## Flux Normalization

Each spectrum is normalized to its own peak flux value before rendering. This is performed
by the backend during artifact generation.

**Rationale:** Spectra sourced from heterogeneous archives carry inconsistent flux
calibration. Normalizing to per-spectrum peak ensures that every spectrum fills its lane
comparably, regardless of origin, and prevents a single bright spectrum from visually
dominating the waterfall.

**Known limitation:** Per-spectrum normalization discards information about the absolute
evolution of the nova's luminosity across epochs. Proper cross-spectrum flux comparison
requires either flux-calibrated spectra or continuum flux measurements, neither of which
can be assumed for archival data. This limitation is accepted for the MVP. A future
enhancement could expose a flux-calibrated view when calibration metadata is available,
but this is explicitly deferred post-MVP and should be noted in a future backlog item.

---

## Density and Temporal Spacing

### Temporal scale

Vertical spacing between spectrum baselines is **proportional to time**. The default
temporal unit is **Days Post-Outburst (DPO)**, as this is the only reference frame in
which logarithmic scaling is physically meaningful.

- **Day 0** = outburst date
- **Day 1** = outburst date + 1 day (or first available observation when outburst date is
  substituted; see Epoch Labels section)
- log(0) is avoided by construction: the earliest plotted spectrum is never Day 0

### Default scale selection

The default temporal scale (linear vs. log) is selected automatically based on the
properties of the available spectra, using the following rules in order:

**Rule 1 — Temporal clustering check:**
Compute the ratio of the largest inter-spectrum time gap to the total time span:

```
gap_ratio = max_gap / total_span
```

If `gap_ratio > 0.5`, default to **log scale** regardless of spectrum count. This catches
edge cases such as a nova with several early observations followed by a single late-epoch
spectrum, where linear scaling would compress the early observations into an unreadably
narrow band.

**Rule 2 — Spectrum count check (applied only if Rule 1 does not trigger):**

- N ≤ 8: default to **linear scale**, all spectra shown
- N > 8: default to **log scale**, representative subset shown (see Dense Mode)

### Dense mode (N > 8 or Rule 1 triggered)

When the default scale is log, a **representative subset** of approximately 8–10 spectra
is selected for the initial view. Spectra are log-sampled in time so that the displayed
subset represents the full temporal evolution of the nova rather than the observational
clustering that typically occurs in early bright epochs.

The full set of spectra remains available via the spectrum selection panel (see
Interactions).

The principle motivating this design is **coverage over density**: the visualization
should convey the shape of the nova's evolution, not the volume of its observational
record.

---

## Color Scheme

### Sparse mode (N ≤ 8, linear scale default)

A sequential blue → amber ramp, keyed to epoch. Oldest spectrum = deep blue; newest
spectrum = warm amber. Colors are assigned by interpolating across the ramp based on
normalized epoch position.

The ramp is selected for:
- Intuitive temporal mapping (cooler = older is a familiar convention)
- Colorblind safety (deuteranopia and protanopia safe; no red-green dependency)
- Alignment with the ADR-012 color token system (blue and amber ramps)

### Dense mode (N > 8, log scale default)

A curated **maximally-distinct 10-color palette** is used. Colors are assigned in an order
that guarantees no two temporally adjacent spectra share similar hues, using a
golden-ratio HSL spacing approach or equivalent Glasbey-style algorithm.

The palette must be validated for deuteranopia and protanopia before implementation. Some
compromise on maximum distinctiveness is acceptable to maintain colorblind safety.

---

## Epoch Labels

Epoch labels appear on the **right-hand side of the plot**, aligned to each spectrum's
baseline. Three label formats are available and user-togglable:

| Format | Example | Notes |
|---|---|---|
| Days Post-Outburst | `Day 34` | Default. Day 0 = outburst date; Day 1 = first observation. |
| MJD | `58432` | Integer only. |
| Calendar Date | `2018 Nov 14` | Year, abbreviated month, day. No time component. |

**Default format:** DPO.

**Unknown outburst date:** When the outburst date cannot be determined from the literature,
the backend substitutes the earliest available observation (photometric or spectroscopic)
as Day 1. The Y axis label in DPO mode is marked with an asterisk (`Day 34*`), and a
tooltip on the axis explains the substitution.

**Label crowding:** In log-time mode, a minimum pixel separation between adjacent epoch
labels is enforced frontend-side. Labels that would fall closer than this threshold are
dropped rather than overlapped. In practice, log-sampling of the representative subset
largely prevents crowding; this rule is a safety net for edge cases.

---

## Interactions

All interactions are implemented as React state managed within the spectra viewer
component. No server round-trips are required for any interaction.

### Epoch label format toggle

A three-way toggle (DPO / MJD / Calendar Date) in the viewer header. Switching format
updates all epoch labels on the right-hand Y axis. DPO is the default.

If the DPO option is unavailable (outburst date unknown and no observation date available
as fallback), the DPO button is disabled with a tooltip explaining why.

### Temporal scale toggle

A two-way toggle (Log / Linear) in the viewer header. Switches the Y axis scaling and
recomputes all baseline positions. The current scale is indicated by a small badge inside
the plot area.

### Spectrum selection

A legend strip below the plot lists all spectra as clickable chips, each labeled with the
active epoch format and colored to match the spectrum. Clicking a chip isolates that
spectrum (all others dim to 10% opacity). Clicking again returns to the full view.

When exactly one spectrum is isolated, the viewer enters **single-spectrum mode**:

- The Y axis relabels from epoch offset positions to normalized flux values
- The log Y-axis toggle becomes available (hidden in waterfall mode)
- The amplitude fills the full plot height for maximum feature legibility

### Log Y-axis toggle (single-spectrum mode only)

A two-way toggle (Linear / Log) for the flux Y axis. **Hidden** (not merely disabled) in
waterfall mode, as log Y is meaningless when the axis encodes epoch offsets.

Becomes visible only when the viewer is in single-spectrum mode.

### Spectral feature markers

A row of toggle buttons grouped by nova spectral type / phase:

- **Fe II** — lines characteristic of Fe II-type novae
- **He / N** — lines characteristic of He/N-type novae
- **Nebular** — forbidden and coronal lines from the nebular phase

When a group is toggled on, full-height vertical dashed lines are drawn at each line's
wavelength, colored by group. Line wavelength labels appear above the plot area.

Lines with blended wavelengths (e.g., [S II] 6716/31) are plotted at the mean wavelength
with the blend noted in the annotation label.

The exact line list is drawn from a curated subset of the Williams nova line list. The
full list is trimmed to the most prominent features (target: fewer than 10% of the
original line count) before implementation. The trimmed list is an implementation detail
and is not specified in this ADR.

*Implementation note: Wavelengths in the Williams line list are in Ångströms and must be
converted to nm (divide by 10) before use.*

### Zoom, pan, reset

Provided by Plotly.js built-ins. No custom implementation required.

### Hover tooltip

On cursor hover, a tooltip displays:

- Wavelength (nm, to 1 decimal place)
- Normalized flux value
- Epoch (active label format)
- Instrument name

---

## Empty and Error States

**No validated spectra available:** The visualization region displays the `LineChart`
Lucide icon (32px, per ADR-012 iconography spec) with a short explanatory message. No
broken layout or empty canvas.

**Partial data (some spectra invalid):** Valid spectra are displayed normally. Invalid
spectra are silently excluded. If this results in fewer spectra than expected, no
additional UI is shown — the count shown in the nova page header reflects only validated
spectra.

**Render error:** An inline error message is shown within the visualization region using
`--color-status-error-fg` and a `CircleAlert` icon, per ADR-012 error state spec. A
"Try again" ghost button is shown. The error is scoped to the visualization region and
does not affect the metadata panel.

---

## Light Curve Panel

The light curve panel is a Plotly.js scatter plot component displayed on the nova page
below the spectra viewer. It renders photometric time-series data for the nova across
all available wavelength regimes.

### Wavelength regime tabs

Photometric observations span wavelength regimes whose physical interpretations,
instrumental characteristics, and natural units are mutually incompatible. Displaying
them on a shared axis would produce a misleading or unreadable plot. The light curve
panel is therefore organized into tabs by wavelength regime, each tab containing a
self-contained plot with its own axes and color scheme.

**Available tabs:**

| Tab | Y axis | Y orientation | Y scale |
|---|---|---|---|
| Optical | Magnitude | Inverted (brighter = top) | Linear |
| X-ray | Count rate (cts/s) | Standard (higher = top) | Linear or log |
| Gamma-ray | Photon flux (ph/cm²/s) | Standard (higher = top) | Linear or log |
| Radio / Sub-mm | Flux density (mJy) | Standard (higher = top) | Log (default) |

**Tab visibility:** Only tabs for which data exists are shown. Absent regime tabs are
hidden entirely rather than shown in a disabled state. A nova with only optical data
shows a single unlabeled plot; the tab bar appears only when two or more regimes have
data.

---

### X axis (shared across all tabs)

- **Unit:** Time, with format user-togglable (DPO / MJD / Calendar Date)
- **Scale:** Linear or log, user-togglable
- **Default scale:** Selected automatically using the same gap-ratio rule as the spectra
  viewer temporal axis (see Density and Temporal Spacing). If `gap_ratio > 0.5`, default
  to log scale.
- **Log scale availability:** The log toggle is hidden (not merely disabled) when the
  active epoch format is MJD or Calendar Date. Log time is only physically meaningful in
  the DPO frame. This mirrors the log Y-axis hide behavior in the spectra viewer's
  single-spectrum mode.
- **Epoch label format toggle:** Same three-way toggle as the spectra viewer (DPO / MJD
  / Calendar Date), same header placement, same DPO-unavailable disable behavior.

---

### Optical tab

#### Multi-band color scheme

Each photometric band is assigned a fixed, semantically anchored color. Band color is a
categorical distinction, not a temporal one — unlike the spectra viewer color ramp, which
encodes epoch.

Proposed anchor assignments:

| Band group | Color |
|---|---|
| U / UV | Violet / purple |
| B | Blue |
| V | Green |
| R / r | Orange-red |
| I / i | Deep red |
| Other / unfiltered | Neutral gray |

Johnson and Sloan bands (e.g., V vs. g', R vs. r') are treated as **distinct bands** and
assigned distinct colors, even when they occupy similar wavelength ranges. They are not
interchangeable and should not be presented as equivalent.

The exact palette must be validated for deuteranopia and protanopia before
implementation.

#### Upper limits

Non-detections are plotted as downward-pointing triangles at the limiting magnitude, in
the same color as the corresponding band, at 40% opacity. They are included in hover
tooltips with a clear "Upper limit" label.

**Non-constraining upper limit suppression (per-band):** An upper limit is dropped from
the artifact if its magnitude value is brighter than the brightest detection in the same
band. Such a limit provides no constraint on the data already shown and would only
compress the y-axis dynamic range. This filtering is applied per-band so that the rule
remains correct when a user isolates a single band and the y-axis rescales accordingly.
This is a backend operation, applied during artifact generation.

Upper limits are excluded from automatic Y-axis range fitting.

#### Dynamic range and band offsets

When multiple bands occupy overlapping magnitude ranges, their data points can obscure
one another, producing a dense unreadable cluster rather than a legible set of distinct
light curves. In these cases, a constant per-band magnitude offset is applied to
vertically separate the traces.

Offsets are computed by the backend during artifact generation, using a nearest-neighbor
density analysis (kd-tree approach) on the subsampled per-band data. An offset is applied
only when a band's data overlaps significantly with adjacent bands; if natural separation
is sufficient, the offset is zero. When applied, offsets are rounded to the nearest
half-integer increment (e.g., +0.5, +1.0, +1.5) to produce clean, publication-style
separations.

Applied offsets are displayed explicitly in the band legend strip (e.g., "R (+1.5)") so
researchers are always aware that a shift has been applied. The exact overlap threshold
and density metric are deferred to the backend responsibilities ADR.

---

### X-ray tab

Count rate (cts/s) on the Y axis. Instrument provenance is encoded by color, since count
rates are instrument-dependent and direct comparison across instruments is not valid. The
expected instrument set for MVP is small (primarily Swift/XRT, with occasional Chandra
and XMM-Newton observations).

Energy flux conversion is explicitly deferred post-MVP. The model-dependence of
ECF-based conversion introduces scientific ambiguity that is not appropriate for a
catalog positioned as a trusted reference resource.

---

### Gamma-ray tab

Photon flux (ph/cm²/s) above 100 MeV on the Y axis, consistent with standard Fermi/LAT
reporting. Upper limits are expected to dominate this tab for most novae and are rendered
using the same downward-pointing triangle convention as the optical tab.

Non-constraining upper limit suppression is applied using the same per-dataset logic as
the optical tab, adapted for flux orientation: an upper limit is dropped if it falls
below the faintest detection in the same dataset (since higher flux = higher on this
axis).

---

### Radio / Sub-mm tab

Flux density (mJy) on the Y axis, log scale by default. The log default reflects the
typical dynamic range of radio nova light curves and the potential presence of
sub-millimetre observations spanning multiple orders of magnitude in flux density
alongside centimetre-wavelength radio detections.

---

### Density and subsampling

Nova photometric datasets can be extremely large. A hard cap of **500 data points per
wavelength regime tab** is applied during artifact generation. The subsampled artifact is
the render target; the full dataset is available in the bundle download.

Subsampling uses a **density-preserving log sampler**: the time axis is divided into N
log-spaced intervals, and interval boundaries are stretched dynamically so that no
interval is empty. This ensures that sparse late-epoch observations — which would be lost
under uniform log binning — are always represented, and that the displayed subset
reflects the shape of the nova's temporal evolution rather than the density of its
observational record.

The 500-point cap applies independently per regime tab. Radio and gamma-ray tabs will
rarely approach this ceiling; the subsampler is primarily active for optical data.

---

### Interactions

All interactions are implemented as React state within the light curve panel component.
No server round-trips are required.

- **Epoch label format toggle** (DPO / MJD / Calendar Date) — same three-way toggle as
  spectra viewer, same header placement.
- **Log / Linear time axis toggle** — hidden when MJD or Calendar Date is the active
  epoch format.
- **Band visibility toggles** — a legend strip below the plot lists each band as a
  clickable chip, colored to match the band. All bands visible by default. Toggling a
  band on/off dynamically re-evaluates which upper limits are displayed (per-band
  suppression rule).
- **Error bars** — magnitude uncertainties are shown as error bars by default. A toggle
  to hide them is provided in the viewer header.
- **Hover tooltip** — displays: time (active epoch format), magnitude or flux (to 2
  decimal places), band, instrument / source, and "Upper limit" label where applicable.
- **Zoom, pan, reset** — Plotly.js built-ins.

---

### Backend / Frontend Responsibility Boundary (photometry)

**Backend generates (during artifact generation):**

- Per-observation records: MJD, band, magnitude or flux value, uncertainty, upper limit
  flag, instrument / source
- Outburst reference date (shared with spectra artifact)
- Non-constraining upper limit suppression (per-band)
- Density-preserving log subsampling (500-point cap per regime)
- Per-band magnitude offsets (kd-tree density analysis; exact algorithm deferred to
  backend responsibilities ADR)
- Sparkline artifact (separate stripped-down artifact; see Catalog Sparkline)

**Frontend computes (at render time):**

- Default log/linear time scale selection (gap-ratio rule)
- Y-axis range (excluding upper limits from fitting)
- Band color assignment
- Dynamic re-evaluation of upper limit visibility on band toggle

---

### Empty and Error States (light curve panel)

**No photometry available:** The visualization region displays the `Activity` Lucide icon
(32px, per ADR-012 iconography spec) with a short explanatory message.

**Render error:** Inline error message in `--color-status-error-fg` with a `CircleAlert`
icon and a "Try again" ghost button. Scoped to the light curve panel; does not affect the
spectra viewer or metadata region.

---

## Catalog Sparkline

The catalog sparkline is a small, non-interactive thumbnail rendering of the optical
light curve, displayed in the "Light Curve" column of the catalog table.

**Dimensions:** 90×55px. Temporal and magnitude resolution are treated as equally
important; the aspect ratio reflects this.

**Content:** Optical band only. A single representative trace showing the overall shape
of the light curve — the rise, peak, and decline. No axes, no labels, no tooltips, no
legend.

**Rendering:** Pre-generated by the backend as a standalone artifact (separate from
`photometry.json`). The frontend consumes this artifact as a static image; no
client-side Plotly rendering occurs for sparklines.

**Empty state:** The "No data" placeholder text specified in ADR-011, per ADR-012 empty
state spec.

---

## Open Questions

The following decisions are not resolved in this ADR and should be addressed before or
during implementation:

1. **Artifact schema:** The exact JSON structure of the spectra artifact (including how
   per-spectrum metadata, outburst date, and normalization values are represented) is not
   specified here. This is a hard dependency for both backend generation and frontend
   consumption and must be resolved in a dedicated artifact schema ADR.

2. **Trimmed Williams line list:** The curated subset of spectral feature lines for the
   marker overlay is not finalized. This is an implementation detail that does not block
   the ADR but must be resolved before the feature marker interaction is implemented.

3. **Dense mode palette:** The exact 10-color maximally-distinct palette for dense mode
   has not been selected. Palette selection, colorblind validation, and integration with
   ADR-012 color tokens should occur during the frontend implementation phase.

4. **Minimum epoch label pixel separation:** The exact pixel threshold for label crowding
   suppression has not been specified. This value should be determined empirically during
   implementation.

5. **Photometry artifact schema:** The exact JSON structure of the `photometry.json`
   artifact (including per-observation records, outburst reference date, band metadata,
   and pre-computed offsets) is not specified here. This is a hard dependency for both
   backend generation and frontend consumption and must be resolved in a dedicated
   artifact schema ADR.

6. **Per-band offset algorithm:** The exact overlap threshold and density metric for the
   kd-tree-based band offset computation are not specified here. These are implementation
   details to be resolved in a dedicated backend responsibilities ADR.

7. **Band color palette:** The exact colors for the optical multi-band color scheme have
   not been selected. Palette selection, colorblind validation, and integration with
   ADR-012 color tokens should occur during the frontend implementation phase.

8. **Sparkline artifact format:** Whether the sparkline is pre-rendered as a PNG/SVG
   image or as a stripped-down JSON data artifact rendered by a minimal client-side
   component is not resolved here.

---

## Consequences

- The spectra viewer is a self-contained React component consuming a pre-processed spectra
  artifact. It performs no heavy computation at render time.
- The backend artifact generation pipeline is responsible for normalization, downsampling,
  and outburst date resolution. These are hard dependencies for a functional viewer.
- Plotly.js built-in interactions (zoom, pan, reset, hover) are used where sufficient.
  Custom React state handles all toggle and selection interactions.
- The design is intentionally scoped to the MVP. Post-MVP enhancements (flux-calibrated
  view, cross-nova comparison, wavelength range selector) are compatible with the component
  architecture but are not specified here.
- The light curve panel is a tabbed React component. Only tabs for which photometric
  data exists are rendered. Tab structure is determined at artifact consumption time,
  not hardcoded.
- The backend photometry artifact pipeline is responsible for subsampling, upper limit
  suppression, and band offset computation before the artifact is written. The frontend
  performs no heavy data transforms at render time.
- The 500-point-per-regime subsampling cap is a hard constraint on artifact generation.
  Full photometric datasets are available exclusively via the bundle download.
- The log time axis toggle is hidden (not disabled) when MJD or Calendar Date epoch
  formats are active. This is consistent with the spectra viewer's log Y-axis hide
  behavior and avoids presenting a meaningless interaction.
