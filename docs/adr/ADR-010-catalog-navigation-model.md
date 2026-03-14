# ADR-010: Catalog Navigation Model

Status: Proposed
Date: 2026-03-14

---

## Context

ADR-009 defines the strategy for the Open Nova Catalog MVP website, emphasizing simple catalog exploration, visual inspection of observations, and download of curated nova datasets.

The catalog itself is the central feature of the system. Users should be able to quickly locate novae, inspect their available observations, and access the associated datasets.

This ADR defines the navigation structure and information architecture of the MVP website, including:

- site entry points
- navigation structure
- catalog page behavior
- nova page routing
- high-level page layout

The goal is to ensure that users can move through the system easily while maintaining flexibility for future expansion.

---

## Decision

The Open Nova Catalog website will use a **catalog-centered navigation model**.

The catalog is the primary interaction point for the system, allowing users to browse, search, and discover novae.

Dedicated nova pages provide detailed information and access to observational datasets.

The navigation model prioritizes simplicity, clarity, and discoverability.

---

## Site Entry Point

The root URL of the website (`/`) will redirect to the catalog page (`/catalog`).

This allows the catalog to function as the primary entry point while preserving the possibility of introducing a dedicated homepage in the future without changing the catalog route.

Example:
```
/ → redirect → /catalog
```


---

## Navigation Bar

The site will include a minimal top-level navigation bar with the following links:

```
Search | Catalog | Documentation | About
```

Each item provides access to a major functional area of the site.

### Search

Links to the dedicated search interface.

### Catalog

Links to the primary catalog browsing page.

### Documentation

Links to documentation explaining the catalog structure and data products.

### About

Provides a brief description of the Open Nova Catalog project and its goals.

---

## Catalog Page

The catalog page (`/catalog`) is the primary browsing interface.

The page displays a table of novae that allows users to explore the catalog and identify objects of interest.

### Catalog Columns

The catalog table will include the following visible columns:

- Primary Name
- Right Ascension (RA)
- Declination (Dec)
- Year of eruption
- Spectra Available
- Photometry Available

These fields provide a concise overview of each nova and its available observations.

### Catalog Row Behavior

Each catalog row represents a single nova.

The **Primary Name** column links directly to the corresponding nova detail page.

### Default Sorting

The catalog should prioritize novae with rich observational datasets.

The default sorting order should therefore emphasize entries with the greatest number of available spectra.

This ensures that the most data-rich objects are visible near the top of the catalog.

### Pagination

The catalog will use classic pagination rather than infinite scrolling.

Approximately 25 novae will be displayed per page.

Pagination improves navigability and maintains consistent page structure.

### Optional Photometry Thumbnails

Catalog entries may optionally include small photometry or light-curve thumbnails.

If implemented, these thumbnails will appear at the trailing edge of the catalog row.

These thumbnails are considered enhancements and are **not required for the MVP**.
The catalog must remain fully usable without them.

---

## Search Interface

The website will provide a dedicated search route:

```
/search
```


This page presents a search-focused interface that allows users to locate novae by name or alias.

The search page is intentionally minimal and closely mirrors the search functionality available within the catalog page.

The existence of a dedicated search route helps accommodate users who expect search and catalog browsing to be separate interactions.

Internally, the search interface may reuse the same components used by the catalog page.

---

## Nova Page Routes

Each nova will have a dedicated detail page.

The routing model must support both stable internal identifiers and human-readable object names.

### Stable Identifier Routes

Newly ingested novae may initially be referenced by a stable internal identifier.

Example:

`/nova/<nova-id-uuid>`


This ensures that each nova has a persistent canonical route even before an official designation exists.

### Human-Readable Routes

Once a nova has a stable primary designation, the site should support a human-readable route:

`/nova/<nova-primary-name>`


Both routes should resolve to the same nova page.

This approach allows the catalog to accommodate novae whose official names are assigned after initial ingestion.

---

## Nova Page Layout

The nova page presents detailed information about a single nova.

The layout is organized into two primary regions.

### Visualization Region (Primary)

The main visualization area displays observational data.

For the MVP this includes:

- spectra visualization

Photometry visualization may be added when photometric data becomes available.

### Metadata and Reference Region

A secondary region presents structured information about the nova.

This includes:

- core metadata
- literature references
- links to download the curated nova bundle

Metadata and references should be displayed using structured tables where appropriate.

---

## Primary User Flow

The navigation structure supports the following primary user flow:

```
Catalog / Search
↓
Nova Page
↓
Download Data
```


This workflow allows users to quickly move from discovery to dataset access.

---

## Consequences

The catalog-centered navigation model reinforces the primary goals of the Open Nova Catalog website:

- enabling rapid discovery of novae
- highlighting available observational datasets
- providing direct access to curated data products

By keeping the navigation structure simple and the catalog interface central, the MVP remains easy to implement while still supporting future expansion.

Future ADRs will define more detailed aspects of the frontend architecture, including catalog search behavior, visualization components, and data delivery mechanisms.
