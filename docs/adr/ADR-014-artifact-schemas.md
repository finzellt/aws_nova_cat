# ADR-014: Artifact Schemas

Status: Proposed
Date: 2026-03-17

> **⚠ Amended by ADR-031** (2026-03-31)
> ADR-031 (Data Layer Readiness for Artifact Generation) amends this ADR as follows:
>
> - **Decision 8:** The sparkline band selection is no longer "V-band only." DESIGN-003
>   §9.3 defines a ranked fallback algorithm that prefers V-band but selects the
>   best-sampled optical band when V-band data is absent or insufficient. The sparkline
>   specification table below is updated accordingly.
> - **Decision 9:** Open Question 5 (recurrent nova outburst selection) is resolved by
>   DESIGN-003 §7.6. See the Open Questions section below.
>
> See: `docs/adr/ADR-031-data-layer-readiness-for-artifact-generation.md`

> **⚠ Amended by ADR-033** (2026-04-10)
> ADR-033 (Spectra Compositing Pipeline) amends this ADR as follows:
>
> - **Spectrum record type:** `spectrum_id` may now refer to either an individual
>   DataProduct or a composite DataProduct. A new optional `is_composite` boolean
>   field distinguishes composites in the artifact. The `spectra.json` schema
>   version is bumped from `"1.1"` to `"1.2"`.
> - **Display filtering:** Composite spectra replace their constituent individual
>   spectra in the `spectra` array. The frontend never sees both a composite and
>   its constituents in the same artifact.
>
> See: `docs/adr/ADR-033-spectra-compositing-pipeline.md`

> **⚠ Amended by ADR-034** (2026-04-11)
> ADR-034 (Spectra Wavelength Regime Model) amends this ADR as follows:
>
> - **Spectra regime metadata:** A new top-level `regimes` array is added to
>   `spectra.json`, containing regime metadata records that drive tab creation
>   in the spectra viewer. Each spectrum record gains a `regime` field
>   assigned by wavelength midpoint. The `spectra.json` schema version is
>   bumped from `"1.2"` to `"1.3"`.
> - **Sort order:** The `spectra` array is sorted by regime group order, then
>   by `epoch_mjd` within each regime.
>
> See: `docs/adr/ADR-034-spectra-wavelength-regime-model.md`

---

## Context

ADR-009 established pre-generated static artifacts as the primary data delivery mechanism
for the Open Nova Catalog website. ADR-011 defined the frontend technology stack and page
layouts that consume those artifacts. ADR-012 defined the catalog table column
specification. ADR-013 defined the visualization design for spectra and photometry, and
established the backend/frontend responsibility boundary.

However, none of those ADRs defined the concrete schemas of the published artifacts. ADR-013
explicitly deferred three schema decisions as hard dependencies for both backend generation
and frontend consumption:

1. The JSON structure of the spectra artifact
2. The JSON structure of the photometry artifact
3. The format and structure of the catalog sparkline

This ADR resolves all three of those open questions, defines all remaining static artifact
schemas required by the frontend, specifies the per-nova downloadable bundle structure, and
establishes governing principles for artifact design and evolution.

---

## Scope

This ADR covers:

- Artifact design principles
- Inventory of all published static artifacts
- Schema definitions for all static JSON artifacts consumed by the frontend
- Sparkline artifact specification
- Per-nova downloadable bundle structure, contents, and file naming conventions
- Schema versioning and evolution strategy

This ADR does **not** cover:

- DynamoDB data model or internal backend schemas
- IVOA VOTable or FITS format specifications beyond referencing the relevant standards
- Backend artifact generation logic or pipeline architecture
- S3 bucket layout or deployment configuration
- Frontend component implementation

---

## Artifact Design Principles

The following principles govern all artifact schemas defined in this ADR and any future
amendments or additions.

### Backend owns computation; frontend owns presentation

All non-trivial computation is the backend's responsibility and must be completed before
the artifact is written. This encompasses normalization, offset computation, unit
conversion, subsampling, and summary aggregation. Artifacts delivered to the frontend
carry the *results* of that computation — not the raw inputs from which results must be
derived. The frontend applies visual styling, layout, and interaction; it never derives
scientific values from raw data.

This principle is consistent with the backend/frontend responsibility boundary established
in ADR-013, and it is the reason that pre-computed fields such as `days_since_outburst`,
`flux_normalized`, and per-band `vertical_offset` appear explicitly in the schemas below
rather than being left for the frontend to calculate.

### Stable field names

Field names in published artifacts are part of the public contract between the backend
generation pipeline and the frontend. Renaming a field is a breaking change. Fields should
be named for what they represent, not for how they are currently used.

### Explicit schema versioning

Every artifact includes a top-level `schema_version` field. This enables the frontend to
detect and handle schema changes gracefully, and provides a clear signal when a breaking
change has been introduced.

### Minimal redundancy

Each value lives in the artifact where it is most semantically appropriate. When a value
is needed in multiple render contexts, it is duplicated only when the duplication serves a
clear practical purpose.

The principal example of acceptable redundancy is `primary_name` and observation counts,
which appear in both `catalog.json` (to power the catalog table row) and `nova.json` (to
power the nova page header). These two artifacts serve genuinely different render contexts
and are fetched independently, making duplication unavoidable.

The principal example of redundancy that is *avoided* is the observation summary table on
the nova page. Rather than pre-aggregating instrument, epoch, and wavelength range data
into `nova.json`, the frontend derives this summary directly from `spectra.json` at render
time, because that is where the data naturally lives and the derivation is trivial.

### Research-grade and frontend-grade data are separate products

The static JSON artifacts are optimized for frontend rendering: values are pre-normalized,
pre-subsampled, and structured for direct consumption by React components. The
downloadable bundle is optimized for research use: FITS files in original flux units,
full unsubsampled datasets, and provenance metadata. These are distinct products with
distinct requirements and must not be conflated.

---

## Artifact Inventory

The following table enumerates all published static artifacts, their paths, their scope,
and their consumers.

| Artifact | Path | Scope | Consumers |
|---|---|---|---|
| Catalog | `catalog.json` | Global | Homepage stats bar, catalog table, search page |
| Nova metadata | `nova/<identifier>/nova.json` | Per-nova | Nova page metadata region |
| References | `nova/<identifier>/references.json` | Per-nova | Nova page references table |
| Spectra | `nova/<identifier>/spectra.json` | Per-nova | Spectra viewer component |
| Photometry | `nova/<identifier>/photometry.json` | Per-nova | Light curve panel component |
| Sparkline | `nova/<identifier>/sparkline.svg` | Per-nova | Catalog table light curve column |
| Bundle | `nova/<identifier>/<primary-name-hyphenated>_bundle_<YYYYMMDD>.zip` | Per-nova | Nova page download link |

The `<identifier>` path segment is the nova's stable UUID, consistent with the routing
model defined in ADR-010.

---

## Catalog Artifact (`catalog.json`)

The catalog artifact is a single global file consumed by the homepage stats bar, the
catalog table, and the search page. It is the only artifact fetched on initial page load
and must support client-side filtering and sorting without any additional requests.

This section defines the **MVP schema**. Additional fields are expected in post-MVP
iterations as the data pipeline matures — in particular, fields supporting wavelength
coverage indicators, recurrent and extragalactic flags, and physical nova parameters noted
as post-MVP in ADR-012.

Aggregate statistics in the `stats` block are computed by the backend at artifact
generation time and must not be derived client-side.

### Top-level fields

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Schema version identifier |
| `generated_at` | string | ISO 8601 UTC timestamp of artifact generation |
| `stats` | object | Aggregate catalog statistics for the homepage stats bar |
| `stats.nova_count` | integer | Total number of novae in the catalog |
| `stats.spectra_count` | integer | Total number of validated spectra across all novae |
| `stats.photometry_count` | integer | Total number of photometric observations across all novae |
| `novae` | array | Ordered array of nova summary records |

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-03-17T00:00:00Z",
  "stats": {
    "nova_count": 47,
    "spectra_count": 312,
    "photometry_count": 8940
  },
  "novae": [ ... ]
}
```

### Nova summary record fields

Each entry in the `novae` array represents one nova and carries all fields required to
render a catalog table row and support client-side name/alias search.

| Field | Type | Description |
|---|---|---|
| `nova_id` | string | Stable nova UUID |
| `primary_name` | string | Primary designation |
| `aliases` | array of string | All known aliases; used for client-side search |
| `ra` | string | Right ascension in HH:MM:SS.ss format |
| `dec` | string | Declination in ±DD:MM:SS.s format |
| `discovery_year` | integer | Four-digit discovery year |
| `spectra_count` | integer | Count of validated spectra |
| `photometry_count` | integer | Count of photometric observations; `0` when none available |
| `references_count` | integer | Count of associated literature references |
| `has_sparkline` | boolean | Whether a sparkline SVG has been generated for this nova |

```json
{
  "nova_id": "3f2a1b4c-...",
  "primary_name": "GK Per",
  "aliases": ["Nova Per 1901", "V650 Per"],
  "ra": "03:31:11.82",
  "dec": "+43:54:16.8",
  "discovery_year": 1901,
  "spectra_count": 24,
  "photometry_count": 1840,
  "references_count": 12,
  "has_sparkline": true
}
```

---

## Nova Metadata Artifact (`nova.json`)

The nova metadata artifact is a per-nova file powering the metadata region of the nova
page. It carries core object properties and observation counts.

References are intentionally excluded and delivered in a separate artifact (see
`references.json`) to allow independent generation and lazy loading.

This section defines the **MVP schema**. Additional fields are expected in post-MVP
iterations, including physical nova parameters (peak magnitude, spectroscopic class,
t₂ decay time, distance, reddening) noted as deferred in ADR-012. Those fields require
either manual curation or a reliable automated sourcing mechanism before they can be added.

### Fields

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Schema version identifier |
| `generated_at` | string | ISO 8601 UTC timestamp of artifact generation |
| `nova_id` | string | Stable nova UUID |
| `primary_name` | string | Primary designation |
| `aliases` | array of string | All known aliases |
| `ra` | string | Right ascension in HH:MM:SS.ss format |
| `dec` | string | Declination in ±DD:MM:SS.s format |
| `discovery_date` | string | Discovery date in `YYYY-MM-DD` format; day set to `00` when only month precision is available; month and day set to `00` when only year precision is available |
| `nova_type` | string | Nova classification (e.g., `"classical"`, `"recurrent"`) |
| `spectra_count` | integer | Count of validated spectra |
| `photometry_count` | integer | Count of photometric observations |

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-03-17T00:00:00Z",
  "nova_id": "3f2a1b4c-...",
  "primary_name": "GK Per",
  "aliases": ["Nova Per 1901", "V650 Per"],
  "ra": "03:31:11.82",
  "dec": "+43:54:16.8",
  "discovery_date": "1901-02-00",
  "nova_type": "classical",
  "spectra_count": 24,
  "photometry_count": 1840
}
```

---

## References Artifact (`references.json`)

The references artifact is a per-nova file powering the references table on the nova page.
It is fetched independently of `nova.json` to allow the metadata region to render before
the references table is populated.

### Fields

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Schema version identifier |
| `generated_at` | string | ISO 8601 UTC timestamp of artifact generation |
| `nova_id` | string | Stable nova UUID |
| `references` | array | Array of reference records |
| `references[].bibcode` | string | ADS bibcode |
| `references[].title` | string | Publication title |
| `references[].authors` | array of string | Author list |
| `references[].year` | integer | Publication year |
| `references[].doi` | string or null | DOI if available |
| `references[].arxiv_id` | string or null | Bare arXiv ID if available (no `arXiv:` prefix) |
| `references[].ads_url` | string | Full ADS abstract page URL |

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-03-17T00:00:00Z",
  "nova_id": "3f2a1b4c-...",
  "references": [
    {
      "bibcode": "1901MNRAS..61..337W",
      "title": "Nova Persei",
      "authors": ["Williams, R."],
      "year": 1901,
      "doi": null,
      "arxiv_id": null,
      "ads_url": "https://ui.adsabs.harvard.edu/abs/1901MNRAS..61..337W"
    }
  ]
}
```

---

## Spectra Artifact (`spectra.json`)

The spectra artifact is a per-nova file consumed exclusively by the spectra viewer
component. It carries all data required to render the waterfall plot as defined in
ADR-013, with no computation deferred to the frontend.

### Backend responsibilities (pre-computed before artifact generation)

Per ADR-013's backend/frontend responsibility boundary, the following are computed by the
backend and embedded in the artifact:

- Flux normalization (median normalization per spectrum)
- Normalization scale factor (for tooltip display of original flux values)
- Outburst reference date (MJD) for the temporal axis
- Days since outburst per spectrum

### Top-level fields

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Schema version identifier |
| `generated_at` | string | ISO 8601 UTC timestamp of artifact generation |
| `nova_id` | string | Stable nova UUID |
| `outburst_mjd` | number or null | Reference outburst date in MJD; `null` if unresolved |
| `wavelength_unit` | string | Wavelength unit for all spectra in this artifact (always `"nm"` per ADR-013) |
| `spectra` | array | Array of spectrum records |

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-03-17T00:00:00Z",
  "nova_id": "3f2a1b4c-...",
  "outburst_mjd": 46123.0,
  "wavelength_unit": "nm",
  "spectra": [ ... ]
}
```

### Spectrum record fields

| Field | Type | Description |
|---|---|---|
| `spectrum_id` | string | Stable spectrum UUID |
| `epoch_mjd` | number | Observation epoch in MJD (fractional, for uniqueness within a night) |
| `days_since_outburst` | number or null | Computed from `epoch_mjd` minus `outburst_mjd`; `null` if outburst date unresolved |
| `instrument` | string | Instrument name |
| `telescope` | string | Telescope name |
| `provider` | string | Data provider / archive source |
| `wavelength_min` | number | Minimum wavelength in nm |
| `wavelength_max` | number | Maximum wavelength in nm |
| `flux_unit` | string | Original flux unit prior to normalization (e.g., `"erg/cm2/s/A"`); for tooltip display |
| `normalization_scale` | number | Median flux value used for normalization, in original flux units; for tooltip reconstruction |
| `wavelengths` | array of number | Wavelength array in nm |
| `flux_normalized` | array of number | Median-normalized flux array; parallel to `wavelengths` |

`flux_unit` is a per-spectrum field because spectra sourced from different archives may
carry different flux units.

```json
{
  "spectrum_id": "7a3c...",
  "epoch_mjd": 46134.4471,
  "days_since_outburst": 11.4,
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
```

---

## Photometry Artifact (`photometry.json`)

The photometry artifact is a per-nova file consumed by the light curve panel component.
It carries all data required to render the multi-regime, multi-band light curve as defined
in ADR-013, with per-band vertical offsets pre-computed by the backend.

Photometric observations span wavelength regimes whose physical interpretations,
instrumental characteristics, and natural units are mutually incompatible. The artifact
is therefore organized around regimes, with a top-level `regimes` array that drives tab
creation in the frontend. Only regimes for which data exists are included; the frontend
renders a tab bar only when two or more regimes are present.

Because different regimes use fundamentally different physical quantities, the observation
record carries separate nullable fields for each quantity type — `magnitude`,
`flux_density`, `count_rate`, and `photon_flux` — rather than a single generic value
field. For any given observation, exactly one of these fields will be non-null, determined
by the regime.

### Backend responsibilities (pre-computed before artifact generation)

Per ADR-013's backend/frontend responsibility boundary, the following are computed by the
backend and embedded in the artifact:

- Per-band vertical offsets (kd-tree-based overlap avoidance, per ADR-013)
- Subsampling to the 500-point-per-regime cap (per ADR-013)
- Non-constraining upper limit suppression (per ADR-013)
- Outburst reference date (MJD)
- Days since outburst per observation

### Top-level fields

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Schema version identifier |
| `generated_at` | string | ISO 8601 UTC timestamp of artifact generation |
| `nova_id` | string | Stable nova UUID |
| `outburst_mjd` | number or null | Reference outburst date in MJD; `null` if unresolved |
| `regimes` | array | Regime metadata records; one per regime present in the data; drives tab creation |
| `bands` | array | Band metadata records; one per photometric band present |
| `observations` | array | Array of individual photometric observation records |

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-03-17T00:00:00Z",
  "nova_id": "3f2a1b4c-...",
  "outburst_mjd": 46123.0,
  "regimes": [ ... ],
  "bands": [ ... ],
  "observations": [ ... ]
}
```

### Regime metadata record fields

The `regimes` array is the authoritative source for tab structure. The frontend iterates
this array directly to determine how many tabs to render and what to label them, without
scanning the observations array.

| Field | Type | Description |
|---|---|---|
| `id` | string | Regime identifier; matches `regime` field on band and observation records |
| `label` | string | Human-readable tab label |
| `y_axis_label` | string | Y axis label for this regime's plot |
| `y_axis_inverted` | boolean | Whether the Y axis is inverted (true for optical magnitude: brighter = top) |
| `y_axis_scale_default` | string | Default Y axis scale: `"linear"` or `"log"` |
| `bands` | array of string | Band identifiers belonging to this regime; matches `band` field on band records |

Per ADR-013, the defined regimes and their display properties are:

| `id` | `label` | `y_axis_label` | `y_axis_inverted` | `y_axis_scale_default` |
|---|---|---|---|---|
| `optical` | Optical | Magnitude | true | `"linear"` |
| `xray` | X-ray | Count rate (cts/s) | false | `"linear"` |
| `gamma` | Gamma-ray | Photon flux (ph/cm²/s) | false | `"linear"` |
| `radio` | Radio / Sub-mm | Flux density (mJy) | false | `"log"` |

```json
{
  "id": "optical",
  "label": "Optical",
  "y_axis_label": "Magnitude",
  "y_axis_inverted": true,
  "y_axis_scale_default": "linear",
  "bands": ["B", "V", "R", "I"]
}
```

### Band metadata record fields

| Field | Type | Description |
|---|---|---|
| `band` | string | Band identifier (e.g., `"V"`, `"B"`, `"UVW1"`, `"0.3-10keV"`) |
| `regime` | string | Regime this band belongs to; matches `id` in the `regimes` array |
| `wavelength_eff_nm` | number or null | Effective wavelength in nm; `null` for X-ray and gamma-ray bands |
| `vertical_offset` | number | Pre-computed vertical offset for display; `0.0` for the reference band or when no offset is needed |
| `display_color_token` | string or null | CSS design token name for this band's plot color per ADR-012; `null` for X-ray bands, which are colored by instrument instead |

```json
{
  "band": "V",
  "regime": "optical",
  "wavelength_eff_nm": 551.0,
  "vertical_offset": 0.0,
  "display_color_token": "--color-plot-band-V"
}
```

### Observation record fields

| Field | Type | Description |
|---|---|---|
| `observation_id` | string | Stable observation UUID |
| `epoch_mjd` | number | Observation epoch in MJD |
| `days_since_outburst` | number or null | Computed from `epoch_mjd` minus `outburst_mjd`; `null` if outburst date unresolved |
| `band` | string | Band identifier; matches a `band` value in the `bands` array |
| `regime` | string | Regime identifier; matches `id` in the `regimes` array |
| `magnitude` | number or null | Observed magnitude; non-null for optical bands only |
| `magnitude_error` | number or null | Magnitude uncertainty; `null` if not reported or not applicable |
| `flux_density` | number or null | Flux density in mJy; non-null for radio / sub-mm observations |
| `flux_density_error` | number or null | Flux density uncertainty in mJy; `null` if not reported or not applicable |
| `count_rate` | number or null | Count rate in cts/s; non-null for X-ray observations |
| `count_rate_error` | number or null | Count rate uncertainty in cts/s; `null` if not reported or not applicable |
| `photon_flux` | number or null | Photon flux in ph/cm²/s above 100 MeV; non-null for gamma-ray observations |
| `photon_flux_error` | number or null | Photon flux uncertainty; `null` if not reported or not applicable |
| `is_upper_limit` | boolean | Whether this observation is a non-detection upper limit |
| `provider` | string | Data provider / archive source |
| `telescope` | string | Telescope name; `"unknown"` if not recorded |
| `instrument` | string | Instrument name; `"unknown"` if not recorded |

For any given observation, exactly one of `magnitude`, `flux_density`, `count_rate`, or
`photon_flux` will be non-null, determined by the regime. X-ray instrument provenance is
encoded via the `instrument` field rather than a color token, per ADR-013 (Swift/XRT,
Chandra, and XMM-Newton are the expected instrument set for MVP).

```json
{
  "observation_id": "9b1e...",
  "epoch_mjd": 46134.4471,
  "days_since_outburst": 11.4,
  "band": "V",
  "regime": "optical",
  "magnitude": 12.34,
  "magnitude_error": 0.02,
  "flux_density": null,
  "flux_density_error": null,
  "count_rate": null,
  "count_rate_error": null,
  "photon_flux": null,
  "photon_flux_error": null,
  "is_upper_limit": false,
  "provider": "AAVSO",
  "telescope": "unknown",
  "instrument": "unknown"
}
```

---

## Sparkline Artifact (`sparkline.svg`)

The sparkline is a pre-rendered SVG file generated by the backend and served as a static
asset. This resolves ADR-013 Open Question 8.

### Decision: pre-rendered SVG

The sparkline is generated backend-side as a static SVG file, not as a JSON data artifact
rendered by a client-side component.

**Rationale:**

- Keeps the catalog table component free of any Plotly.js dependency in the sparkline
  render path, consistent with ADR-013's backend/frontend responsibility boundary.
- A pre-rendered SVG is a self-contained, zero-computation asset that the browser renders
  natively without JavaScript.
- SVG is resolution-independent, rendering crisply on high-DPI and Retina displays without
  requiring multiple image sizes — an important consideration for research-grade monitors.
- For a simple single-band line chart, an SVG is typically smaller in file size than an
  equivalent raster PNG.
- Reduces frontend complexity at the most performance-sensitive render path — the catalog
  table, which may eventually contain hundreds of rows.

### Specification

| Property | Value |
|---|---|
| Format | SVG |
| Dimensions | 90 × 55 pixels |
| Photometric band | V-band preferred; falls back to best-sampled optical band (see DESIGN-003 §9.3) |
| Background | Transparent |
| Axis labels | Minimal scale labels at the extremes of both axes (first/last x-axis value; min/max y-axis value) |
| Axis label implementation | Conditional on visual review during implementation — labels must not obscure the light curve shape at this size; if they do, they should be dropped without requiring an ADR amendment |
| Line color | Backend resolves `--color-plot-band-V` token value at generation time |

The `has_sparkline` field in `catalog.json` indicates whether a sparkline has been
generated for a given nova. When `false`, the catalog table renders the "No data"
placeholder as specified in ADR-012.

---

## Bundle Structure

The downloadable bundle is a per-nova zip archive intended for research use. It is a
distinct product from the static JSON artifacts and is optimized for programmatic
consumption by researchers, not for frontend rendering.

### Bundle filename

```
<primary-name-hyphenated>_bundle_<YYYYMMDD>.zip
```

Example:
```
GK-Per_bundle_20260317.zip
```

The `YYYYMMDD` date reflects the generation date of the bundle, allowing researchers to
identify the most recently updated version. Nova primary names are hyphenated — spaces
replaced with hyphens — for filesystem compatibility. This hyphenation rule is canonical
and must be applied consistently by the bundle generation pipeline.

### Bundle contents

```
GK-Per_bundle_20260317.zip
├── GK-Per_metadata.json
├── GK-Per_sources.json
├── spectra/
│   ├── GK-Per_spectrum_CfA_FLWO15m_FAST_46134.4471.fits
│   ├── GK-Per_spectrum_CfA_FLWO15m_FAST_46135.6832.fits
│   └── ...
└── photometry/
    ├── GK-Per_photometry_AAVSO_unknown_unknown_46134.4471.fits
    └── ...
```

### Individual file naming convention

FITS files within the bundle follow this convention:

```
<primary-name-hyphenated>_<data_product_type>_<provider>_<telescope>_<instrument>_<epoch_mjd>.fits
```

| Segment | Description |
|---|---|
| `primary-name-hyphenated` | Nova primary name with spaces replaced by hyphens |
| `data_product_type` | `spectrum` or `photometry` |
| `provider` | Data provider / archive source |
| `telescope` | Telescope name; `unknown` if not recorded |
| `instrument` | Instrument name; `unknown` if not recorded |
| `epoch_mjd` | Observation epoch as fractional MJD to 4 decimal places, providing sub-night uniqueness |

All five naming segments are always present. `unknown` is used as an explicit sentinel
rather than omitting a segment, ensuring that every segment's position is unambiguous and
programmatically parseable. Fractional MJD to 4 decimal places provides approximately
8 seconds of temporal precision, which is sufficient to uniquely identify any two spectra
taken during the same observing night.

Example filenames:
```
GK-Per_spectrum_CfA_FLWO15m_FAST_46134.4471.fits
GK-Per_spectrum_CfA_FLWO15m_FAST_46135.6832.fits
RS-Oph_photometry_AAVSO_unknown_unknown_46134.4471.fits
```

### Non-FITS bundle files

| File | Format | Contents |
|---|---|---|
| `<n>_metadata.json` | JSON | Nova properties: primary name, aliases, coordinates, discovery date, nova type |
| `<n>_sources.json` | JSON | Provenance records for all data in the bundle: provider, archive, original identifiers, retrieval date |

### FITS file contents

**Spectrum FITS files** conform to the IVOA Spectrum Data Model v1.2
(REC-SpectrumDM-1.2-20231215), the same standard reviewed in ADR-013. Each file contains
a BINTABLE extension with `WAVELENGTH` (nm) and `FLUX` columns in original
(non-normalized) flux units. All relevant metadata — instrument, telescope, epoch,
provider — is recorded in the FITS header using standard IVOA keywords.

**Photometry FITS files** conform to the IVOA Photometry Data Model (IVOA PhotDM 1.1).
Each file contains a BINTABLE extension with one row per observation, with columns for
epoch (MJD), the appropriate physical quantity (magnitude, flux density, count rate, or
photon flux depending on regime), associated uncertainty, band identifier, and upper limit
flag. Band and provenance metadata are recorded in the FITS header.

Specific FITS keyword names and header conventions are implementation details to be
resolved during backend development and are out of scope for this ADR.

---

## Versioning and Evolution

### Schema version field

Every JSON artifact includes a top-level `schema_version` string field. The initial
version for all artifacts defined in this ADR is `"1.0"`.

### Versioning rules

| Change type | Version increment | Notes |
|---|---|---|
| New optional field added | Minor (e.g., `1.0` → `1.1`) | Frontend must handle absence gracefully |
| Field renamed or removed | Major (e.g., `1.0` → `2.0`) | Breaking change; requires coordinated frontend update |
| Field semantics changed | Major | Even if the field name is unchanged |
| New artifact type added | No version change | Does not affect existing schemas |

### Frontend handling

The frontend checks `schema_version` on artifact load. If the major version does not match
the expected value, the frontend surfaces a graceful error rather than attempting to render
potentially mismatched data.

---

## Open Questions

1. **References in the bundle:** Whether to include a `references.bib` or equivalent file
   in the downloadable bundle is not resolved here. The case for inclusion is that it makes
   the bundle a more self-contained research package. This should be decided when the
   bundle generation pipeline is implemented.

2. **Sparkline axis label legibility:** Axis labels are conditional on a visual review pass
   at 90×55px. If labels are found to obscure the light curve shape at this size, they
   should be dropped. This is an implementation-time decision that does not require an ADR
   amendment.

3. **Photometry FITS keyword conventions:** Specific FITS keyword names and header
   conventions for photometry files are not defined here. These are implementation details
   to be resolved during backend development in accordance with IVOA PhotDM 1.1.

4. **Band color token values:** The `display_color_token` field on band records references
   CSS design tokens (e.g., `--color-plot-band-V`). Exact token values have not been
   finalized per ADR-013 Open Question 7. Token finalization should occur during the
   frontend implementation phase.

5. ~~**Recurrent nova outburst selection:**~~ **Resolved by DESIGN-003 §7.6.** Recurrent
   novae always use the earliest-observation fallback regardless of `discovery_date`, and
   always set `outburst_mjd_is_estimated = true`. The `discovery_date` for a recurrent
   nova typically refers to the earliest known outburst (potentially centuries ago), which
   is not a meaningful reference for DPO computation. Full outburst segmentation is
   deferred to a post-MVP ADR.

---

## Consequences

- All static artifact schemas are now fully specified. Backend generation and frontend
  consumption can proceed against a stable contract.
- ADR-013 Open Questions 1, 2, and 8 (spectra artifact schema, photometry artifact schema,
  sparkline format) are resolved by this ADR.
- The bundle structure and per-file naming convention provide an unambiguous spec for the
  `generate_nova_bundle` pipeline component.
- The `schema_version` field on all artifacts creates a formal breaking-change mechanism.
  Schema changes that remove or rename fields require a major version increment and a
  coordinated frontend update.
- The separation of `nova.json` and `references.json` into distinct artifacts allows the
  references pipeline (ADS-dependent) to be regenerated independently of core nova
  metadata, and allows the references table on the nova page to lazy-load without blocking
  the metadata region.
- The `regimes` top-level array in `photometry.json` gives the frontend a direct,
  scan-free source of truth for tab structure. The frontend never needs to inspect the
  observations array to determine which tabs to render.
- The per-observation nullable quantity fields (`magnitude`, `flux_density`, `count_rate`,
  `photon_flux`) generalize cleanly across all four wavelength regimes without requiring a
  generic value field that would obscure physical meaning.
- Frontend components are insulated from raw scientific data. All computation —
  normalization, offset, subsampling, outburst date resolution — is the backend's
  responsibility and is embedded in the artifact at generation time.
