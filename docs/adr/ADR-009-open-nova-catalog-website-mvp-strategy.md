# ADR-009: Open Nova Catalog Website MVP Strategy

Status: Proposed
Date: 2026-03-14

---

## Context

ADR-008 defines the long-term product vision and guiding design principles for the Open Nova Catalog website. The vision describes a system that enables astronomers to explore nova observations, visually inspect datasets, and obtain curated observational data.

However, the initial implementation of the website must operate within several practical constraints related to funding, infrastructure, development capacity, and the current state of the catalog.

The MVP strategy defines a minimal but useful version of the website that advances the goals described in ADR-008 while respecting these constraints.

---

## Constraints

### Limited Funding

The project currently operates with minimal funding. Infrastructure and operational costs must therefore be kept low.

This constraint strongly favors simple architectures and discourages complex backend systems or expensive managed services.

---

### Serverless Infrastructure

To minimize operational overhead and cost, the project uses a serverless architecture.

Implications include:

- compute performed through serverless functions
- limited memory environments
- avoidance of persistent backend services where possible

---

### DynamoDB Data Store

The catalog backend currently uses DynamoDB with a limited set of secondary indexes.

Implications include:

- complex relational queries are difficult or expensive
- flexible metadata queries are limited
- search capabilities must be implemented carefully

---

### No Dedicated Search Infrastructure

The project does not currently include a dedicated search system such as OpenSearch or ElasticSearch.

Implications include:

- full-text search and complex filtering are not available
- catalog exploration should not rely on backend search queries

---

### Limited Development Capacity

The project currently has a single primary developer.

Implications include:

- the MVP must avoid complex multi-system architectures
- implementation complexity must remain manageable

---

### Small Initial Catalog Size

The initial catalog will contain roughly tens of novae (approximately 50 or fewer).

Implications include:

- the entire catalog can be loaded and explored in the browser
- large-scale database infrastructure is unnecessary for the MVP

---

### Data Ingestion Pipeline Still Developing

The ingestion pipeline for observational data is still under active development.

In particular:

- photometry aggregation is incomplete
- spectra ingestion is more mature

Implications include:

- spectra visualization will be emphasized in the MVP
- photometry features may be limited initially

---

### Curated Bundles as the Primary Data Product

Due to cost constraints and serverless execution limits, dynamically generating custom datasets is expensive.

Lambda memory limits and execution costs make on-demand dataset assembly impractical.

Instead, the system produces **prebuilt curated nova bundles** that contain aggregated observational datasets.

Implications include:

- the MVP will distribute data through prebuilt bundles
- users will not construct bespoke datasets

---

## Constraint Relationships

Several of the constraints described above are closely related and influence one another.

For example:

Limited funding encourages the use of serverless infrastructure.

Serverless infrastructure encourages the use of DynamoDB as the primary data store.

DynamoDB’s query model limits complex metadata querying, which in turn favors frontend-driven catalog browsing rather than archive-style queries.

Similarly:

Limited developer capacity slows the development of ingestion adapters and therefore limits the size of the initial catalog.

The small catalog size makes browser-based catalog exploration practical and reduces the need for complex backend infrastructure.

Finally:

Serverless execution limits and cost constraints discourage dynamically assembling datasets.

As a result, the MVP distributes observational data through curated prebuilt nova bundles.

---

## Decision

The MVP website will prioritize **simple catalog exploration and curated dataset access** rather than advanced querying or programmatic data retrieval.

The site will focus on enabling users to:

- discover novae
- inspect available observations
- download curated datasets

To minimize infrastructure complexity and operational cost, the catalog browsing interface will be implemented primarily in the browser.

Catalog data may be delivered as static data files rather than generated dynamically by backend queries.

The frontend architecture should remain flexible so that catalog data could later be provided by a backend API without requiring major changes to the user interface.

---

## MVP Capabilities

The MVP website will provide the following capabilities.

### Catalog Browsing

Users can browse the catalog of novae through a tabular interface.

---

### Catalog Search

Users can search for novae by primary name and known aliases.

---

### Catalog Sorting

Users can sort the catalog using available metadata fields.

---

### Catalog Pagination

The catalog will display a limited number of entries per page with classic page navigation.

---

### Optional Catalog Thumbnails

Light-curve thumbnails may be included in the catalog view if they can be implemented without significantly increasing complexity.

These thumbnails are not required for the MVP to be considered successful.

---

### Catalog Filtering

The MVP may include lightweight catalog filtering implemented within the browser interface.

These filters are intended to assist catalog browsing rather than to provide full archive-style querying.

---

### Nova Detail Pages

Each nova will have a dedicated page containing:

- core metadata
- references
- observational visualizations
- download access to the nova bundle

---

### Spectra Visualization

Nova pages will include interactive visualizations of available spectra.

---

### Dataset Download

Users will be able to download curated nova bundles containing aggregated observational datasets.

---

### References Display

Nova pages will provide a list of relevant literature references and may include links to external literature search tools.

---

### Lightweight Documentation

The website will include a minimal documentation page explaining the purpose of the catalog and how to access its data.

---

## Non-Goals for the MVP

The following capabilities are explicitly outside the scope of the MVP.

### Archive-Style Query System

The MVP will not support complex archive-style queries over observational metadata.

This includes queries that dynamically return datasets matching arbitrary parameter combinations.

---

### Programmatic API Access

The MVP will not expose a public API for programmatic data access.

Programmatic interfaces may be introduced in future versions.

---

### User Accounts

The MVP will not include authentication, user profiles, or personalized features.

---

### Data Submission

External users will not be able to upload or contribute datasets.

---

### Cross-Nova Comparison Tools

Tools that aggregate and compare data across multiple novae are considered a long-term goal but are not included in the MVP.

---

### Advanced Interactive Analysis

The MVP will not provide advanced analysis environments or complex in-browser data analysis tools.

---

## Alternative MVP Strategies Considered

### Archive-Style Query Portal

One possible approach would be to implement the website as a fully queryable astronomical archive similar to MAST, IRSA, or Vizier.

Such systems allow users to construct complex queries over observational metadata.

This approach was rejected for the MVP because it would require significantly more infrastructure and development effort and would be difficult to implement given the current DynamoDB data model.

---

### API-First System

Another possible approach would expose the catalog primarily through a programmatic API.

API-first systems typically require users to interact with the catalog through scripts, notebooks, or command-line tools.

This creates a barrier to entry for users who lack the necessary software expertise and does not support the project's goal of enabling visual discovery and exploration of nova datasets.

---

### Static Documentation Website

Another alternative would be a documentation-focused website that explains the catalog but does not provide interactive exploration of nova data.

This approach was rejected because it would fail to demonstrate the richness of the available observational datasets.

---

### Browser-Based Analysis Platform

The website could have been designed as an interactive analysis platform with advanced visualization and analysis tools.

This approach was rejected for the MVP because it would significantly increase complexity and shift the focus away from rapid data access.

---

## Consequences

This MVP strategy emphasizes simplicity, discoverability, and rapid data access.

The catalog interface becomes the primary interaction model for the website.

Advanced capabilities such as API access, complex queries, and cross-nova comparisons are intentionally deferred but remain compatible with the long-term vision described in ADR-008.

Future ADRs will define the detailed design of catalog navigation, nova pages, visualization tools, and the frontend architecture used to implement the website.
