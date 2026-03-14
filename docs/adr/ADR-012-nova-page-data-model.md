# ADR-012: Nova Page Data Model

Status: Proposed
Date: 2026-03-14

---

## Context

ADR-010 defines the navigation structure of the Open Nova Catalog website and introduces dedicated pages for individual novae.

These nova pages present detailed information about a single nova, including observational datasets, metadata, references, and access to curated data bundles.

The frontend requires a structured representation of nova-specific data to render these pages.

This ADR defines the data model used by the frontend to represent nova pages.

---

## Decision

Each nova page will be generated from a structured dataset describing the nova and its associated observations.

The dataset must include:

- core nova metadata
- observational datasets (primarily spectra for the MVP)
- literature references
- download links for curated nova bundles

This information will be delivered to the frontend as a structured JSON document.

---

## Nova Dataset Location

Each nova page will load a dedicated dataset describing the nova.

Example location:

`/data/nova/<nova-id>.json`


Example:

`/data/nova/v1324-sco.json`


The exact location may vary depending on deployment architecture.

---

## Nova Dataset Structure

Example dataset:

```json
{
  "nova_id": "uuid",
  "primary_name": "V1324 Sco",
  "aliases": [
    "Nova Sco 2012",
    "PNV J17583612-2952581"
  ],
  "ra": 269.6505,
  "dec": -29.8828,
  "eruption_year": 2012,

  "spectra": [
    {
      "instrument": "UVES",
      "telescope": "VLT",
      "observation_date": "2012-06-23",
      "wavelength_min": 3000,
      "wavelength_max": 10000,
      "data_file": "/data/spectra/v1324-sco/uves-20120623.fits"
    }
  ],

  "references": [
    {
      "citation": "Guillochon et al. 2017",
      "ads_url": "https://ui.adsabs.harvard.edu/..."
    }
  ],

  "nova_bundle": {
    "bundle_name": "v1324-sco-bundle",
    "bundle_url": "/bundles/v1324-sco.zip",
    "bundle_size_mb": 210
  }
}
```

## Metadata Section

The metadata section of the nova page presents basic identifying information about the nova.

This information should be displayed in a structured table on the nova page.

Typical metadata fields include:

- primary name
- aliases
- right ascension (RA)
- declination (Dec)
- eruption year

These fields provide the basic astronomical context for the object and allow users to quickly confirm the identity and location of the nova.

---

## Spectra Data

Spectra are the primary observational dataset emphasized in the MVP.

Each spectrum entry should include sufficient metadata to support visualization and identification.

Recommended fields include:

- instrument
- telescope
- observation date
- minimum wavelength
- maximum wavelength
- data file location

Example spectrum entry fields:

- instrument: `UVES`
- telescope: `VLT`
- observation_date: `2012-06-23`
- wavelength_min: `3000`
- wavelength_max: `10000`
- data_file: `/data/spectra/v1324-sco/uves-20120623.fits`

The spectra dataset is used to generate the primary visualization on the nova page.

---

## Photometry Data

Photometry data may be included when available.

Photometry datasets typically include:

- observation timestamp
- magnitude value
- photometric filter
- source dataset or instrument

Example photometry fields:

- timestamp: `2012-06-20T03:14:00Z`
- magnitude: `11.4`
- filter: `V`
- source: `AAVSO`

Photometry ingestion is still under development, and many novae in the MVP may not yet include photometric datasets.

The nova page design must therefore remain functional even when photometry data is absent.

---

## Literature References

Nova pages should include a list of relevant literature references.

Each reference entry should include:

- citation text
- link to an external literature database (for example ADS)

Example fields:

- citation: `Guillochon et al. 2017`
- ads_url: `https://ui.adsabs.harvard.edu/...`

These references allow users to quickly locate published research associated with the nova.

---

## Data Bundle Access

Each nova page provides access to a curated dataset bundle containing aggregated observational data.

The bundle metadata should include:

- bundle name
- download URL
- approximate file size

Example:

- bundle_name: `v1324-sco-bundle`
- bundle_url: `/bundles/v1324-sco.zip`
- bundle_size_mb: `210`

The download link provides direct access to the curated nova dataset.

---

## Consequences

This data model provides a clear structure for representing nova pages and allows the frontend to render:

- observational visualizations
- metadata tables
- literature references
- dataset downloads

Because the dataset is delivered as a structured JSON document, the frontend can render nova pages without complex backend queries.

Future versions of the catalog may expand this data model to include additional observational datasets, derived products, or cross-nova comparison features.
