# ADR-011: Catalog Data Model for Frontend

Status: Proposed
Date: 2026-03-14

---

## Context

ADR-010 defines the navigation model for the Open Nova Catalog website, where the catalog page is the primary interface for browsing and discovering novae.

To support this interface, the frontend requires a structured dataset containing summary information about each nova in the catalog.

Because the MVP website prioritizes simplicity and low operational overhead (ADR-009), catalog data should be delivered to the frontend in a format that can be loaded efficiently and processed directly in the browser.

This ADR defines the data model used to represent catalog entries and describes how catalog data is delivered to the frontend.

---

## Decision

Catalog data will be delivered to the frontend as a **static JSON dataset** containing a list of nova summary records.

Each record represents a single nova and contains the metadata necessary to populate the catalog table and navigate to the corresponding nova page.

The catalog dataset must be small enough to load entirely within the browser for client-side browsing, searching, sorting, and filtering.

---

## Catalog Dataset Location

Catalog data will be delivered as a static JSON resource.

Example location:

`/data/catalog.json`



The exact location may vary depending on deployment architecture, but the dataset should be accessible to the frontend without requiring authenticated requests or complex backend queries.

---

## Catalog Record Structure

Each entry in the catalog dataset represents a single nova and contains summary metadata used by the catalog interface.

Example structure:

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
  "spectra_count": 18,
  "photometry_count": 0,
  "nova_page_route": "/nova/V1324-Sco"
}
```

## Required Fields

The following fields must be present in every catalog record.

### `nova_id`

Stable internal identifier for the nova.

This identifier provides a canonical reference even when the nova name changes or aliases are added.

---

### `primary_name`

The preferred display name for the nova.

This value is used in the catalog table and in human-readable routes.

---

### `aliases`

List of known alternate identifiers for the nova.

These identifiers are used by the search system so that users can locate novae even when they search for alternate names.

Example:

`["Nova Sco 2012", "PNV J17583612-2952581"]`

---

### `ra`

Right Ascension of the nova in decimal degrees.

Example:

`269.6505`

---

### `dec`

Declination of the nova in decimal degrees.

Example:

`-29.8828`

---

### `eruption_year`

The year of the nova eruption.

This field provides temporal context and may be used for sorting or filtering within the catalog interface.

Example:

`2012`

---

### `spectra_count`

Number of spectra currently available for the nova.

This value is used for both display and default catalog sorting.

Example:

`18`

---

### `photometry_count`

Number of photometric observations currently available.

This field allows the catalog interface to indicate whether light curve data exists for a given nova.

Example:

`0`

---

### `nova_page_route`

The route used by the frontend to navigate to the nova detail page.

Example:

`/nova/V1324-Sco`

---

## Optional Fields

Some catalog fields may be included when available but are not required for the MVP.

### `photometry_thumbnail`

Small image or visualization preview representing the nova light curve.

If present, this thumbnail may be displayed as part of the catalog row.

Example:

`/thumbnails/V1324-Sco-lightcurve.png`

Catalog functionality must not depend on the presence of this field.

---

## Client-Side Behavior

The catalog dataset is intended to be loaded entirely into the browser.

The frontend is responsible for:

- sorting catalog entries
- searching names and aliases
- applying lightweight filtering
- handling pagination

This design simplifies infrastructure requirements and aligns with the MVP constraint that the catalog will contain a relatively small number of novae.

---

## Consequences

This data model enables the frontend to render the catalog table and perform basic search and filtering operations without requiring a complex backend system.

Because the catalog dataset is static and relatively small, it can be cached and delivered efficiently.

As the catalog grows in size, future ADRs may introduce server-side search infrastructure or API-based catalog queries. However, the structure defined here provides a stable foundation for the MVP implementation.
