# Catalog Ontology

## Purpose

This document defines the conceptual entities that make up the Open Nova Catalog.

The ontology describes how astronomical objects, observations, and datasets are organized within the catalog. It provides a conceptual model used by both the backend ingestion pipeline and the frontend website.

This document describes **conceptual entities**, not implementation details or database schemas.

---

## Core Concept

The Open Nova Catalog is an **object-centric scientific catalog**.

Information in the system is organized around **novae**, with observational datasets and metadata attached to each object.

Conceptually:

```text
Catalog
└── Nova
    ├── Observations
    │   └── Data Products
    ├── Metadata
    ├── References
    ├── Visualizations
    └── Data Bundles
```


The nova acts as the central organizing entity for all associated information.

---

## Primary Entities

### Catalog

The **catalog** is the complete curated collection of nova-related information published by the Open Nova Catalog website.

It includes:

- all nova objects represented in the system
- observational datasets associated with those objects
- metadata describing those observations
- literature references
- derived visualizations
- curated downloadable data bundles

The catalog page on the website provides a browsing interface into this collection.

---

### Nova

A **nova** represents a single astronomical transient object.

Each nova acts as the central hub for all associated information within the catalog.

Information associated with a nova may include:

- coordinates
- eruption year
- aliases and designations
- literature references
- observational datasets
- derived visualizations
- curated data bundles

Each nova has a dedicated **nova page** within the website.

---

### Observation

An **observation** represents a single scientific observation of a nova.

Examples include:

- a spectroscopic observation
- a photometric measurement
- an observation reported in the literature

Observations are associated with:

- a specific nova
- an observing instrument or survey
- a timestamp or observing interval

Multiple observations may exist for the same nova.

---

### Data Product

A **data product** represents a dataset derived from one or more observations.

Examples include:

- reduced spectra
- photometric time-series
- aggregated photometry datasets

Data products may be visualized directly within the website or distributed as part of curated bundles.

---

### Data Bundle

A **data bundle** is a curated downloadable package containing observational datasets associated with a specific nova.

Bundles allow users to download observational data in a convenient format without requiring complex archive queries.

Bundles typically include:

- observational datasets
- associated metadata
- documentation describing the contents

---

## Relationship to the Website

The website provides interfaces for navigating this ontology:

```text
Catalog Page
    ↓
Nova Page
    ↓
Observations and Data Products
    ↓
Downloadable Bundles
```




The catalog page supports discovery of novae, while nova pages provide detailed access to observations and datasets associated with each object.
