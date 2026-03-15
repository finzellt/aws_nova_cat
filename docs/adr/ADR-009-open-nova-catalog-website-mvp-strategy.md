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

The MVP of the Open Nova Catalog website will follow a **published artifact architecture**.

Internal ingestion and processing systems generate **derived website artifacts** that are exposed through a public static hosting layer. The browser-based frontend retrieves these artifacts directly and renders the catalog interface.

This architecture is described in:

`docs/architecture/frontend-artifact-architecture.md`

Under this model:

• Internal systems ingest and normalize observational data and literature.
• Internal pipelines generate derived artifacts optimized for website consumption.
• These artifacts are published to a public storage bucket and served through static hosting.
• The frontend loads these artifacts directly and renders catalog browsing, nova pages, and visualizations.

The frontend therefore interacts only with **published artifacts**, not with internal databases or ingestion systems.

Because the frontend consumes published artifacts rather than live services, the public website remains operational even if ingestion pipelines or internal
processing systems are temporarily unavailable.

---

## Website Artifacts

The frontend consumes several classes of derived artifacts produced by the internal system:

• **Catalog artifact**
  A dataset used to populate the catalog browsing interface.

• **Nova page artifacts**
  Per-object datasets containing metadata, references, and summaries of available observations.

• **Plot-ready visualization artifacts**
  Data prepared for client-side visualization (e.g. downsampled spectra, photometric time-series data, axis/unit metadata).

• **Curated downloadable bundles**
  Packaged observational datasets intended for convenient scientific inspection.

These artifacts are optimized for direct use by the website and are independent from the internal data storage format.

---

## Frontend Rendering Model

The frontend is responsible for rendering catalog views, nova pages, and scientific visualizations.

Visualization rendering occurs in the browser using client-side plotting libraries. Internal systems provide **plot-ready data artifacts**, while the frontend performs the final rendering of spectra and photometric visualizations.

This separation keeps the website artifacts lightweight and avoids coupling artifact formats to a specific visualization implementation.

---

## MVP Constraints

The MVP architecture prioritizes simplicity and low operational complexity.

Key constraints include:

• The frontend should operate without requiring a live backend API.
• Catalog browsing, searching, and filtering should occur in the browser when feasible.
• Infrastructure complexity should be minimized while the catalog dataset remains small.

This approach allows the system to remain inexpensive to operate and easy to evolve while ingestion pipelines and data coverage continue to mature.

---

## MVP Data Coverage

The initial data acquisition pipeline is expected to produce uneven coverage across different observation types.

In particular, spectral datasets may be more readily available during early catalog development, while photometric datasets and other observations may expand more gradually.

The architecture should therefore support incomplete observational coverage while remaining capable of incorporating additional dataset types as ingestion pipelines mature.

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
