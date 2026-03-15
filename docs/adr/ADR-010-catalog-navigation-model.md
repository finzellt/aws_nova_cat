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

## Meaning of "Catalog"

Within the Open Nova Catalog system, the term **catalog** refers to the complete curated collection of nova-related information published by the website.

This includes:

• the listing of known novae
• observational datasets associated with each nova
• visualizations of observational data
• metadata and literature references
• curated downloadable data bundles

The catalog page (`/catalog`) provides a browsing interface into this larger collection, but the catalog itself encompasses all information associated with the novae represented in the system.

The conceptual structure of the catalog and its core entities are described in:

`docs/architecture/catalog-ontology.md`

---

## Decision

The Open Nova Catalog website will use a **catalog-centered navigation model**.

The catalog is the primary interaction point for the system, allowing users to browse, search, and discover novae.

Dedicated nova pages act as the central information hub for each object.

Each nova page aggregates observational data, metadata, literature references, and curated datasets associated with the object. This object-centric design allows users to understand the observational history of a nova and explore its available data in a single location.

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
Catalog | Search | Documentation | About
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

The page displays a structured listing of novae that allows users to explore the catalog and identify objects of interest.

### Catalog Columns

The catalog table displays a concise set of fields summarizing each nova and the observational coverage available for that object.

The exact set of fields presented in the catalog table is defined in ADR-011 (Catalog Website Data Model).

### Catalog Row Behavior

Each catalog row represents a single nova.

The **Primary Name** column links directly to the corresponding nova detail page.

### Default Sorting

The catalog should prioritize novae with substantial observational coverage

The default sorting order should therefore emphasize entries with the greatest number of available spectra.

This ensures that the most data-rich objects are visible near the top of the catalog.

### Pagination

The catalog will use classic pagination rather than infinite scrolling.

Approximately 25 novae will be displayed per page.

Pagination improves navigability and maintains consistent page structure.

---

## Search Interface

The website will provide a dedicated search route:

```
/search
```


This page presents a search-focused interface that allows users to locate novae by name or alias.

The search page is intentionally minimal and closely mirrors the search functionality available within the catalog page.

Internally, the search interface may reuse the same components used by the catalog page.

### Rationale for a Dedicated Search Page

Although name-based search functionality is also available within the catalog page, the system provides a dedicated `/search` route for usability reasons.

Many users expect search to exist as a clearly identifiable page within a website's navigation structure. Providing a dedicated search page makes this capability immediately discoverable and reduces friction for users who prefer direct name-based lookup over catalog browsing.

The search page therefore serves as an explicit entry point for users who already know the nova they are looking for, while the catalog page remains the primary interface for exploratory discovery.

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

## Architectural Expansion Areas

While the initial focus of the Open Nova Catalog is classical novae, the broader goal is to establish a robust and extensible framework for curated transient-event catalogs.

Future versions of the system may support:

- Comparison of observational data across multiple novae
- Richer visualization tools
- Programmatic access through APIs
- Integration with external analysis tools
- Community data contributions
- Extension to other classes of transient astronomical phenomena

---

## Consequences

The catalog-centered navigation model reinforces the primary goals of the Open Nova Catalog website:

- enabling rapid discovery of novae
- highlighting available observational datasets
- providing direct access to curated data products

By keeping the navigation structure simple and the catalog interface central, the MVP remains easy to implement while still supporting future expansion.

Future ADRs will define more detailed aspects of the frontend architecture, including catalog search behavior, visualization components, and data delivery mechanisms.
