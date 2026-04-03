# Classical Novae Photometry Table — Data Model Specification

**Version:** 2.0
**Replaces:** `docs/specs/archive/photometry_table_model_v1.1.md` — replaced in `epic/22-photometry-doc-reconciliation`
**Standard alignment:** IVOA PhotDM 1.1, IVOA Data Origin Note 1.1, SVO Filter Profile Service, VOTable UCD vocabulary
**Format:** Long/tidy — one row per photometric measurement

---

## Overview

This table stores multi-wavelength photometric measurements of classical novae compiled
from heterogeneous literature sources. Each row represents a single flux or magnitude
measurement in a single photometric band at a single epoch for a single object.
The schema is designed to span optical (Johnson-Cousins, Sloan), ultraviolet (Swift/UVOT),
near-infrared (2MASS/JHK), radio, and X-ray regimes in a unified structure.

The tidy long format is preferred over a wide (pivoted) format because:
- Bands differ across sources and epochs; a wide format would be extremely sparse.
- Provenance and quality metadata can be attached per measurement, not per epoch.
- Filtering and aggregation by band, object, or source is straightforward.

This specification is **storage-format agnostic**. It defines the logical schema —
column names, types, nullability, and semantics — independently of any particular
serialization format. Serialization format decisions (e.g. Parquet, CSV, Feather,
VOTable) are governed separately and do not affect this specification.

---

## Time Standard

All epochs **must** be stored as **Modified Julian Date (MJD) in the TDB (Barycentric
Dynamical Time) scale** wherever possible. This is the modern VO-recommended standard
for time series data.

When ingesting data from sources that use other systems (HJD, JD, UT calendar dates),
the original value should be preserved in `time_orig` and the system noted in
`time_orig_sys`. The `time_bary_corr` flag records whether a barycentric correction
has been applied to `time_mjd`.

> **Rationale:** Nova light curves span timescales of days to decades and are compiled
> from heterogeneous sources. A single canonical time system is essential for reliable
> time-series analysis across sources.

---

## Column Definitions

Columns are grouped into five logical sections. All UCD values follow the IVOA UCD1+
controlled vocabulary. `Nullable` = YES means the field may be NULL for some
photometric regimes (e.g. `magnitude` is not meaningful for radio data).

---

### Section 1 — Source Identification

| Column | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `row_id` | INTEGER | `meta.id` | NO | Unique primary key for this row. Auto-incremented within the table. |
| `nova_id` | UUID | `meta.id` | NO | Internal stable UUID for the nova. The canonical join key to all other NovaCat entities. Always populated. |
| `primary_name` | TEXT | `meta.id.main` | NO | IAU/GCVS canonical display name of the nova (e.g. `V1500 Cyg`, `GK Per`). Human-readable label; not used as a join key. Use the name as listed in the GCVS where possible. |
| `ra_deg` | REAL | `pos.eq.ra;meta.main` | NO | Right Ascension of the nova, ICRS, decimal degrees, J2000. |
| `dec_deg` | REAL | `pos.eq.dec;meta.main` | NO | Declination of the nova, ICRS, decimal degrees, J2000. |

**Notes:**
- `nova_id` is the authoritative object identifier throughout NovaCat. All joins to
  other internal entities (DynamoDB items, published artifacts, downloadable bundles)
  use `nova_id`, never `primary_name`.
- `primary_name` is a convenience label for human consumption and external display only.
  Aliases may be stored in a separate alias lookup table keyed on `nova_id`.
- Coordinates are those of the nova's quiescent counterpart, not the outburst centroid.

---

### Section 2 — Temporal Metadata

| Column | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `time_mjd` | REAL | `time.epoch` | NO | Epoch of the observation in Modified Julian Date, TDB scale. This is the canonical time column. |
| `time_bary_corr` | BOOLEAN | `meta.code` | NO | TRUE if `time_mjd` has been corrected to the Solar System barycentre (TDB). FALSE if the value is geocentric or heliocentric and only approximately converted. |
| `time_orig` | REAL | `time.epoch` | YES | Original time value as reported in the source, before any conversion. NULL if the source already provided MJD(TDB). |
| `time_orig_sys` | TEXT | `meta.code` | YES | Time system of `time_orig`. Allowed values: `MJD_UTC`, `MJD_TT`, `HJD_UTC`, `HJD_TT`, `JD_UTC`, `JD_TT`, `ISOT` (ISO 8601 calendar), `OTHER`. NULL if `time_orig` is NULL. |

**Notes:**
- For sources reporting calendar dates (e.g. AAVSO visual estimates), convert to MJD and set `time_bary_corr = FALSE`.
- For precision timing (X-ray, UV), a barycentric correction should always be sought.

---

### Section 3 — Spectral / Bandpass Metadata

This section describes the photometric band. The design must accommodate wavelength
(optical/UV/NIR), frequency (radio), and energy (X-ray) regimes.

| Column | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `svo_filter_id` | TEXT | `instr.filter.id` | YES | SVO Filter Profile Service identifier, e.g. `SDSS/SDSS.g`, `Swift/UVOT.UVW1`, `2MASS/2MASS.Ks`. NULL for radio, X-ray, and gamma-ray where no SVO entry exists. |
| `band_id` | TEXT | `instr.bandpass` | NO | NovaCat canonical band ID from the band registry (ADR-017), e.g. `HCT_HFOSC_Bessell_V`, `2MASS_Ks`, `Generic_V`. Internal identifier for band identity; not intended for public display. |
| `band_name` | TEXT | `instr.filter` | NO | Canonical short display label for the photometric band, sourced from the band registry entry's `band_name` field (ADR-017). The default identifier in all public-facing outputs. E.g., `V`, `B`, `UVW1`, `5 GHz`, `0.3-10 keV`. See ADR-019 amendment (2026-04-03). |
| `regime` | TEXT | `meta.code` | NO | Wavelength regime of the band. Controlled vocabulary: `optical`, `uv`, `nir`, `mir`, `radio`, `xray`, `gamma`. |
| `spectral_coord_type` | TEXT | `meta.code` | NO | Type of spectral coordinate. Allowed values: `wavelength`, `frequency`, `energy`. Determines the unit of `spectral_coord_value`. |
| `spectral_coord_value` | REAL | `em.wl` / `em.freq` / `em.energy` | YES | Central wavelength (Å), frequency (GHz), or energy (keV) of the bandpass. Registry-derived from the resolved band entry's `lambda_eff` when not supplied by the source file. NULL only for sparse registry entries with no `lambda_eff`. |
| `spectral_coord_unit` | TEXT | `meta.unit` | NO | Unit of `spectral_coord_value`. Allowed values: `Angstrom`, `nm`, `GHz`, `MHz`, `keV`, `MeV`, `GeV`. |
| `bandpass_width` | REAL | `instr.bandwidth` | YES | Effective width of the bandpass in the same units as `spectral_coord_value`. NULL if unknown. |

**Notes on spectral coordinate by regime:**

| `regime` | `spectral_coord_type` | `spectral_coord_unit` | Example |
|---|---|---|---|
| `optical`, `nir`, `mir` | `wavelength` | `Angstrom` | V band → 5500 Å |
| `uv` | `wavelength` | `Angstrom` | UVW2 → 1928 Å |
| `radio` | `frequency` | `GHz` | 5 GHz continuum → 5.0 |
| `xray` | `energy` | `keV` | 0.3–10 keV; use midpoint 5.15 keV |
| `gamma` | `energy` | `MeV` | 100 MeV–300 GeV Fermi-LAT band; `MeV` is the default unit, `GeV` and `keV` also permitted |

For non-optical bands with no SVO entry, `svo_filter_id` is NULL and `band_id`
should be drawn from the band registry. The registry carries all spectral metadata.


---

### Section 4 — Photometric Measurement

This section holds the actual measured values. Because the table spans regimes from
optical magnitudes to X-ray flux densities, both magnitude and flux columns are
present; one set will typically be NULL depending on the regime.

| Column | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `magnitude` | REAL | `phot.mag` | YES | Observed magnitude. NULL for radio and X-ray data, and for rows where only flux is available. |
| `mag_err` | REAL | `stat.error;phot.mag` | YES | 1-sigma uncertainty on `magnitude`. NULL if not reported or if `magnitude` is NULL. |
| `flux_density` | REAL | `phot.flux.density` | YES | Flux density in units given by `flux_density_unit`. Used for radio (Jy, mJy) and as an alternative to magnitude in optical/UV/NIR. |
| `flux_density_err` | REAL | `stat.error;phot.flux.density` | YES | 1-sigma uncertainty on `flux_density`. |
| `flux_density_unit` | TEXT | `meta.unit` | YES | Unit of `flux_density`. Allowed values: `Jy`, `mJy`, `uJy`, `erg/cm2/s/Hz`, `erg/cm2/s/keV`. NULL if `flux_density` is NULL. |
| `count_rate` | REAL | `phot.count;em.X-ray` | YES | Detected count rate (counts/s). X-ray only; NULL otherwise. |
| `count_rate_err` | REAL | `stat.error;phot.count` | YES | 1-sigma uncertainty on `count_rate`. |
| `is_upper_limit` | BOOLEAN | `meta.code.qual` | NO | TRUE if this row is a non-detection upper limit rather than a measurement. Defaults to FALSE. |
| `limiting_value` | REAL | `phot.mag;stat.max` / `phot.flux.density;stat.max` | YES | The limiting magnitude or flux density (3-sigma unless noted otherwise) for non-detection rows. In the same units as `magnitude` or `flux_density`, whichever is applicable. NULL if `is_upper_limit = FALSE`. |
| `limiting_sigma` | REAL | `stat.confidence` | YES | Confidence level of the upper limit in units of sigma. Typically 3.0. NULL if `is_upper_limit = FALSE`. |
| `quality_flag` | INTEGER | `meta.code.qual` | NO | Data quality flag. Allowed values: `0` = good, `1` = uncertain/marginal, `2` = poor/use with caution, `3` = bad/do not use. |
| `notes` | TEXT | `meta.note` | YES | Free-text notes on this measurement (e.g. "contaminated by comparison star", "pre-eruption detection"). |

**Constraint:** For every row, at least one of `magnitude`, `flux_density`, or `count_rate`
must be non-NULL (unless `is_upper_limit = TRUE`, in which case `limiting_value` must be
non-NULL).

---

### Section 5 — Provenance

This section follows the IVOA Data Origin Note 1.1 recommendations for second-hand,
literature-compiled data.

| Column | Type | UCD | Nullable | Description |
|---|---|---|---|---|
| `bibcode` | TEXT | `meta.bib.bibcode` | YES | 19-character ADS bibcode of the paper from which this measurement was taken, e.g. `2023ApJ...945..100S`. Preferred over DOI for journal articles. |
| `doi` | TEXT | `meta.ref.doi` | YES | DOI of the source paper or dataset, e.g. `10.3847/1538-4357/acb56f`. Use when a bibcode is unavailable (e.g. for data releases without a journal paper). |
| `data_url` | TEXT | `meta.ref.url` | YES | Direct URL to the original data table, e.g. a VizieR table URL or journal supplement. |
| `orig_catalog` | TEXT | `meta.id` | YES | Name of the originating catalogue or survey (e.g. `AAVSO`, `SMARTS`, `XMM-Newton`, `ATCA archival`). |
| `orig_table_ref` | TEXT | `meta.id` | YES | Table number or identifier within the source paper (e.g. `Table 3`, `Online Table A1`). Useful when a paper contains multiple photometry tables. |
| `telescope` | TEXT | `instr.tel` | YES | Telescope name (e.g. `Swift`, `VLA`, `CTIO 1.3m`, `Chandra`). |
| `instrument` | TEXT | `instr` | YES | Instrument name (e.g. `UVOT`, `ACIS-S`, `ANDICAM`). |
| `observer` | TEXT | `meta.id.PI` | YES | Name of the observer or team, if identified. For AAVSO data this may be the observer code. |
| `data_rights` | TEXT | `meta.rights` | NO | Data rights/licence. Allowed values: `public`, `CC-BY`, `CC-BY-SA`, `proprietary`, `OTHER`. Defaults to `public` for published literature data. |
| `band_resolution_type` | TEXT | `meta.code` | NO | Mechanism by which `band_id` was resolved. Allowed values: `canonical`, `synonym`, `generic_fallback`, `sidecar_assertion`. See ADR-018 Decision 6. |
| `band_resolution_confidence` | TEXT | `meta.code.qual` | NO | Trustworthiness of the band resolution. Allowed values: `high`, `medium`, `low`. See ADR-018 Decision 6. |
| `sidecar_contributed` | BOOLEAN | `meta.code` | NO | True if any sidecar field influenced band or photometric system resolution for this row. Defaults to `false`. |
| `data_origin` | TEXT | `meta.code` | NO | Origin of this row. Allowed values: `literature`, `operator_upload`, `donor_submission`. Defaults to `literature`. |
| `donor_attribution` | TEXT | `meta.note` | YES | Free-text attribution for donated data. NULL for literature rows. Max 512 characters. |

---

## Cross-Regime Guidance

The applicable measurement fields vary by regime. `magnitude` is meaningful only for
optical/UV/NIR data; flux-based quantities apply across all regimes.

| `regime` | `magnitude` | `flux_density` | `count_rate` | Notes |
|---|---|---|---|---|
| `optical` | ✓ | optional | — | |
| `uv` | ✓ | optional | — | |
| `nir` | ✓ | optional | — | |
| `mir` | ✓ | optional | — | |
| `radio` | — | ✓ | — | |
| `xray` | — | ✓ | optional | Count rate in `s⁻¹` is instrument-specific; flux density in `erg/cm²/s/keV`. Both are in common use. |
| `gamma` | — | ✓ | — | Reported as photon flux (`photons/cm²/s`). Use `flux_density_unit = photons/cm2/s`. |


---

## Common Query Patterns

The following are the logical access patterns this schema is optimized to support.
These are independent of storage format; any serialization or indexing strategy should
be evaluated against these patterns.

1. `(nova_id, time_mjd)` — light curve retrieval for a single object (primary pattern)
2. `(nova_id, band_id)` — single-band light curve for a single object
3. `(nova_id, regime)` — all measurements for a single object in a given regime
4. `(bibcode)` — all measurements from a given paper (provenance audit)
5. `(is_upper_limit)` — separating detections from non-detections across the table

`nova_id` appears as the leading key in the four most common patterns and should be
treated as the primary partition dimension in any storage or indexing strategy.

---

## Relationship to IVOA Standards

| Standard | Relevance |
|---|---|
| **IVOA PhotDM 1.1** | Defines the metadata structure for filters, photometric systems, magnitude systems, and zero points. `svo_filter_id`, `mag_system`, and `zero_point_flux` directly implement PhotDM fields. |
| **SVO Filter Profile Service** | Canonical registry for `svo_filter_id`. Access at `https://svo2.cab.inta-csic.es/theory/fps/`. |
| **IVOA UCD1+ vocabulary** | All UCD values in this schema follow the IVOA UCD1+ controlled vocabulary. |
| **IVOA Data Origin Note 1.1** | Provenance columns (`bibcode`, `doi`, `data_url`, etc.) follow its recommendations for dataset citation. |
| **IVOA TimeSeries / Light Curve Note** | `time_mjd` and associated columns follow VO time-series annotation conventions (Nadvornik et al.). |
| **VOTable** | If this table is serialised for VO exchange, it should be serialised as a VOTable with UCD and utype annotations on each FIELD element. |

---

## Example Rows

### Optical (Johnson V, magnitude)

| Field | Value |
|---|---|
| `nova_id` | `4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1` |
| `primary_name` | `GK Per` |
| `time_mjd` | `51234.5123` |
| `time_bary_corr` | `FALSE` |
| `svo_filter_id` | `Generic/Johnson.V` |
| `filter_name` | `V` |
| `phot_system` | `Johnson-Cousins` |
| `spectral_coord_type` | `wavelength` |
| `spectral_coord_value` | `5512.0` |
| `spectral_coord_unit` | `Angstrom` |
| `mag_system` | `Vega` |
| `magnitude` | `12.43` |
| `mag_err` | `0.05` |
| `is_upper_limit` | `FALSE` |
| `quality_flag` | `0` |
| `bibcode` | `2002MNRAS.334..699Z` |
| `telescope` | `SAAO 1.0m` |

### Radio (5 GHz, flux density)

| Field | Value |
|---|---|
| `nova_id` | `7c3a1f22-9e4b-4c81-b2d3-1a2b3c4d5e6f` |
| `primary_name` | `RS Oph` |
| `time_mjd` | `53780.100` |
| `time_bary_corr` | `FALSE` |
| `svo_filter_id` | `NULL` |
| `filter_name` | `5 GHz VLA` |
| `phot_system` | `Radio` |
| `spectral_coord_type` | `frequency` |
| `spectral_coord_value` | `5.0` |
| `spectral_coord_unit` | `GHz` |
| `mag_system` | `NULL` |
| `magnitude` | `NULL` |
| `flux_density` | `8.3` |
| `flux_density_err` | `0.4` |
| `flux_density_unit` | `mJy` |
| `is_upper_limit` | `FALSE` |
| `quality_flag` | `0` |
| `bibcode` | `2008ApJ...688..559R` |
| `telescope` | `VLA` |

### X-ray upper limit (0.3–10 keV)

| Field | Value |
|---|---|
| `nova_id` | `9d7e2a11-3f5c-4b92-c4e5-2b3c4d5e6f7a` |
| `primary_name` | `V838 Her` |
| `time_mjd` | `48821.300` |
| `time_bary_corr` | `TRUE` |
| `svo_filter_id` | `NULL` |
| `filter_name` | `0.3-10 keV ROSAT` |
| `phot_system` | `X-ray` |
| `spectral_coord_type` | `energy` |
| `spectral_coord_value` | `5.15` |
| `spectral_coord_unit` | `keV` |
| `magnitude` | `NULL` |
| `flux_density` | `NULL` |
| `is_upper_limit` | `TRUE` |
| `limiting_value` | `2.1e-13` |
| `limiting_sigma` | `3.0` |
| `flux_density_unit` | `erg/cm2/s/keV` |
| `quality_flag` | `0` |
| `bibcode` | `1992A&A...266..232O` |
| `telescope` | `ROSAT` |

---

*Specification compiled with reference to: IVOA PhotDM 1.1 (Salgado et al. 2022),
IVOA Data Origin Note 1.1 (Demleitner et al. 2024), SVO Filter Profile Service
(Rodrigo et al. 2024), IVOA UCD1+ vocabulary, IVOA TimeSeries annotation Note
(Nadvornik et al.).*
