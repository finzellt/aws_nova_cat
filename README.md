# Nova Cat — The Open Nova Catalog

Nova Cat is a serverless data platform for aggregating, validating, and publishing
classical nova observational data. It serves professional astronomers with a curated,
open-access catalog of spectra and photometry — discoverable through an interactive web
interface and downloadable as research-grade data bundles.

The system ingests data from public astronomical archives
([ESO](https://archive.eso.org/scienceportal/home), CfA,
[VizieR](https://vizier.cds.unistra.fr/viz-bin/VizieR), and others), resolves object
identity across archives, normalizes observations against
[IVOA](https://www.ivoa.net/documents/) (International Virtual Observatory Alliance) standards,
generates seven artifact types per object through a Fargate-based regeneration pipeline,
and publishes them through an immutable release model that separates data generation from
presentation. The project draws inspiration from the Open Supernova Catalog (Guillochon
et al. 2017, ApJ 835:64).

**This is a solo project** — architecture, infrastructure, backend services, data
modeling, artifact generation, frontend, and documentation are all the work of a single
developer. 313 commits and counting.

**Live site:** [Nova-Cat](https://aws-nova-cat.vercel.app/)

---

## What This Project Demonstrates

- **End-to-end system design:** From ingestion pipeline through persistence layer through
  artifact generation through CDN delivery through interactive frontend — a complete data
  platform, not just a service or a UI
- **Contract-first architecture:** Pydantic models define every inter-service boundary.
  JSON Schemas are auto-generated from contracts. Services are developed and tested
  against typed interfaces, not ad hoc payloads
- **Infrastructure as code:** Full AWS CDK deployment (Python) with two isolated stacks
  (production and smoke test), 17 Lambda functions, 7 Step Functions workflows, a Fargate
  task for artifact generation, CloudFront distribution, EventBridge scheduling, and
  single-table-plus-dedicated-secondary DynamoDB design
- **Multi-stage data pipeline:** Ingest → validate → persist → detect changes → plan
  regeneration → generate artifacts → publish immutable release → serve via CDN. Each
  stage has its own failure model, retry semantics, and observability
- **Domain-driven data modeling:** Multi-regime data (optical through radio), IVOA-aligned
  normalization, deterministic identity resolution with coordinate deduplication, SHA-256
  content fingerprinting, and profile-driven validation with explicit quarantine semantics
- **Artifact generation engine:** A Fargate-based pipeline transforms internal database
  state into seven published artifact types per object — metadata JSON, reference lists,
  plot-ready spectra, multi-regime photometry with computed band offsets, SVG sparklines,
  and research-grade data bundles with consolidated FITS tables
- **Documentation discipline:** 30+ architectural decision records, 4 pre-ADR design
  documents, per-workflow operational docs, and formal schema specifications — written
  before implementation, not after
- **Scientific visualization:** Interactive Plotly.js components for spectra (waterfall
  plots with epoch controls and spectral feature markers) and photometry (multi-regime
  tabbed light curves with per-band offsets, density-preserving subsampling, and upper
  limit handling)
- **Rigorous quality gates:** mypy strict, ruff, CI with path-based filtering, end-to-end
  smoke tests against live AWS infrastructure

---

## Architecture Overview

Nova Cat runs on AWS serverless infrastructure with a Fargate-based artifact generation
layer, optimized for low operational cost. The expected dataset is modest (<250 GB,
<1000 objects) and the system is designed for single-operator maintenance.

```
┌──────────────────────────────────────────────────────────────┐
│                    Public Archives                            │
│              (ESO, CfA, VizieR, ADS, SIMBAD)                 │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              Ingestion Pipeline (Step Functions)              │
│                                                              │
│  Archive-driven: ingest_new_nova → initialize_nova →         │
│  discover_spectra → acquire_and_validate → refresh_refs      │
│                                                              │
│  Ticket-driven: ingest_ticket → resolve_nova →               │
│  branch (photometry | spectra) → finalize                    │
│                                                              │
│  17 Lambda functions · profile-driven FITS validation ·      │
│  SHA-256 fingerprinting · quarantine semantics ·             │
│  deterministic identity resolution                           │
└──────────────────────┬───────────────────────────────────────┘
                       │  WorkItem queue (DynamoDB)
                       ▼
┌──────────────────────────────────────────────────────────────┐
│        Artifact Regeneration (EventBridge + Fargate)          │
│                                                              │
│  Coordinator Lambda → RegenBatchPlan → Fargate task          │
│  (2 vCPU / 8 GB) → Finalize Lambda                          │
│                                                              │
│  Per-object: references.json · spectra.json ·                │
│  photometry.json · sparkline.svg · nova.json · bundle.zip    │
│  Global: catalog.json                                        │
│                                                              │
│  Immutable release model · atomic pointer switchover         │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                  Persistence Layer                            │
│                                                              │
│  DynamoDB (single-table + dedicated photometry table) +      │
│  S3 (raw data, derived artifacts, quarantine zone,           │
│  published releases)                                         │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              Delivery (S3 + CloudFront + Vercel)              │
│                                                              │
│  Published artifacts: S3 → CloudFront (OAC)                  │
│  App shell: Next.js → Vercel                                 │
│                                                              │
│  Pre-computed, frontend-ready · schema-versioned ·           │
│  zero backend API calls at runtime                           │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                     Frontend (Next.js)                        │
│                                                              │
│  Catalog table · Detail pages · Spectra waterfall viewer ·   │
│  Multi-regime light curve panel · Client-side search ·       │
│  Semantic design token system · SVG sparklines               │
└──────────────────────────────────────────────────────────────┘
```

### Key Architectural Decisions

- **UUID-first identity** — Names are resolved once during ingestion; all downstream
  operations use stable UUIDs. Coordinate-based deduplication prevents duplicate objects
  across archives (<2" = alias, 2–10" = quarantine, >10" = new object)
- **Atomic data products** — Each spectrum is an independent unit with its own
  acquisition status, validation state, SHA-256 fingerprint, and provenance. No
  dataset-level abstractions
- **Profile-driven validation** — FITS files are validated against per-instrument
  profiles that define expected wavelength ranges, flux units, and header keywords.
  Non-conforming data is quarantined, not silently accepted
- **Immutable release model** — Artifact generation produces a complete release to a
  new S3 prefix. An atomic pointer update (`current.json`) makes it visible. Rollback
  is a single JSON write. Users never see a partially updated catalog
- **Published artifact architecture** — The frontend consumes pre-built static JSON
  artifacts. All scientific computation (normalization, subsampling, offset calculation)
  happens at artifact generation time. The website runs with zero backend API calls
- **Explicit quarantine semantics** — Irreconcilable conflicts (identity ambiguity,
  validation failure, metadata conflicts) are routed to quarantine for operator review
  rather than silently resolved

---

## System Components

### Spectra Ingestion Pipeline

**Status: Operational — validated through end-to-end smoke tests against real data.**

Discovers spectra from provider archives, acquires and fingerprints the raw
**FITS** (Flexible Image Transport System — the standard binary file format for
astronomical data) files, validates them against instrument-specific profiles,
normalizes to an IVOA-aligned canonical representation, and persists validated
products with full provenance.

Services: `nova_resolver` (identity resolution), `archive_resolver` (SIMBAD/TNS
queries), `spectra_discoverer` (provider adapter dispatch), `spectra_acquirer`
(download, fingerprint, ZIP unpack), `spectra_validator` (FITS profile validation and
normalization), `reference_manager` ([ADS](https://ui.adsabs.harvard.edu/) bibcode integration, discovery date
computation), `quarantine_handler` (conflict isolation), `workflow_launcher`
(cross-workflow orchestration).

### Ticket-Driven Ingestion Pipeline

**Status: Operational — spectra and photometry ingestion via hand-curated metadata
tickets.**

The MVP primary ingestion path. A ticket is a plain-text file that fully describes one
data file's structure (column indices, filter names, wavelength units). The pipeline
parses the ticket, resolves object identity, and branches into photometry or spectra
ingestion. Photometry rows are resolved against a versioned band registry seeded from the
SVO Filter Profile Service. Spectra are converted to FITS with reconstructed headers and
uploaded to S3.

### Artifact Regeneration Pipeline

**Status: Operational — Fargate-based, cron-triggered, immutable release publication.**

Connects the backend persistence layer to the frontend presentation layer. Ingestion
workflows write WorkItems to a DynamoDB queue. An EventBridge-triggered Coordinator
Lambda builds a regeneration plan and launches a Fargate task that generates up to seven
artifacts per object in dependency order. A four-phase publication sequence writes
artifacts to a new S3 release prefix, copies unchanged objects from the previous release,
generates a global catalog artifact, and performs an atomic pointer switchover.

Key engineering: dependency-matrix-driven regeneration planning, sequential Fargate
processing with per-object failure isolation, kd-tree-based photometry band offset
computation with union-find clustering and DynamoDB caching, **LTTB**
(Largest-Triangle-Three-Buckets) and density-preserving log subsampling, peak-flux normalization, multi-arm spectra merging, streaming ZIP
assembly with consolidated FITS tables.

### Frontend

**Status: Feature-complete MVP — all pages and visualization components implemented.**

A React/Next.js application providing catalog browsing, per-object detail pages, and
interactive scientific visualizations. The design system uses a two-layer semantic CSS
token architecture (primitive → semantic) that supports dark mode without component
changes.

Key components:
- **Catalog table** — TanStack Table with sorting, pagination, and client-side
  name/alias search
- **Spectra viewer** — Plotly.js waterfall plot with epoch format toggles
  (**DPO** — days post-outburst / **MJD** — Modified Julian Date / Calendar Date),
  log/linear temporal scale, single-spectrum isolation mode, and spectral feature
  marker overlays
- **Light curve panel** — Tabbed Plotly.js scatter plot (one tab per wavelength regime),
  multi-band color scheme with toggleable legend, upper limit markers, error bar toggle
- **Data client** — Release-aware artifact fetcher with dev-mode local fixture fallback

Technology: React, Next.js, TypeScript, Plotly.js, TanStack Table, Tailwind CSS v4,
DM Sans + DM Mono typography, Lucide icons.

---

## Project Structure

```
contracts/          Pydantic models, event schemas, JSON Schema exports
docs/
  adr/              30+ architectural decision records
  architecture/     Architectural baselines and system diagrams
  design/           Pre-ADR scoping documents (4 design docs)
  specs/            Schema specifications (photometry table, FITS profiles)
  storage/          DynamoDB item model, access patterns, S3 layout
  workflows/        Per-workflow operational documentation
frontend/           React/Next.js web application
  src/components/   Catalog table, spectra viewer, light curve panel
  src/styles/       Semantic design tokens
infra/              AWS CDK infrastructure (Python)
  nova_constructs/  CDK constructs (compute, storage, workflows)
  workflows/        Step Functions ASL definitions (7 workflows)
services/           17 Lambda services + 1 Fargate service
  artifact_generator/  Fargate-based artifact generation (6 generators + shared utils)
tests/
  services/         Unit tests
  integration/      Workflow integration tests
  smoke/            Live deployment smoke tests
tools/              Operator tooling and research scripts
```

---

## Infrastructure

- **CDK (Python):** Two-stack deployment — `NovaCat` (production) and `NovaCatSmoke`
  (isolated smoke test)
- **Lambda:** 17 functions (13 zip-bundled, 4 container-based for compiled dependencies)
- **Step Functions:** 7 workflows (6 Express, 1 Standard for Fargate orchestration)
- **Fargate:** 1 task definition (artifact generator, 2 vCPU / 8 GB), 1 ECS cluster
- **DynamoDB:** Main table (single-table design with namespaced partition keys and one
  GSI) plus dedicated photometry table
- **S3:** Private bucket (raw data, derived artifacts, quarantine) and public site
  bucket (published releases with 7-day lifecycle)
- **CloudFront:** 1 distribution (OAC, public site bucket origin)
- **EventBridge:** 1 scheduled rule (6-hour artifact regeneration cron)
- **SNS:** Quarantine notification topic

---

## Development

### Prerequisites

- Python 3.11, Node.js 20, AWS CDK CLI

### Backend

```bash
pip install -r requirements-dev.txt -r requirements.txt
python -m ruff check .          # Lint
python -m ruff format --check . # Format
python -m mypy .                # Type check (strict for services)
pytest --ignore=tests/smoke -q  # Tests
cd infra && cdk synth           # CDK synthesis
```

### Frontend

```bash
cd frontend && npm install
npm run dev     # Development server at localhost:3000
npm run build   # Production build + type check
```

### CI

Backend and frontend CI jobs run in parallel with path-based filtering. Frontend-only
changes skip the full Python suite; backend-only changes skip the Node.js build.

---

## Documentation

Architecture decisions are captured in numbered ADRs (`docs/adr/`), written before
implementation begins. Pre-ADR scoping work lives in `docs/design/`. The ADR series
covers:

- **ADR-001 – ADR-007:** Core system design (contracts, workflows, persistence,
  identity, references, architecture baseline)
- **ADR-008 – ADR-014:** Frontend design (product vision, MVP strategy, navigation,
  architecture, visual design system, visualization design, artifact schemas)
- **ADR-015 – ADR-021:** Photometry pipeline design (column mapping, band/filter
  resolution, band registry, storage format, pre-ingestion normalization)
- **ADR-030 – ADR-032:** Governance, data layer readiness, artifact generation
  algorithms

Design documents: `DESIGN-001` (photometry ingestion redesign), `DESIGN-002` (data
provenance), `DESIGN-003` (artifact regeneration pipeline), `DESIGN-004` (ticket-driven
ingestion).

---

## Current Status

| Component | Status |
|-----------|--------|
| Spectra ingestion pipeline | Operational (smoke-tested against real data) |
| Ticket-driven ingestion | Operational (spectra and photometry) |
| Reference management | Operational |
| Identity resolution | Operational |
| Artifact regeneration pipeline | Operational (Fargate, immutable releases, CloudFront delivery) |
| Frontend MVP | Feature-complete (catalog, detail pages, spectra viewer, light curve panel) |
| Hosting / deployment | Vercel (app) + S3/CloudFront (data artifacts) |
| Heuristic photometry pipeline | In design (ADR-015 → ADR-021, 7-layer architecture) |

---

## License

MIT
