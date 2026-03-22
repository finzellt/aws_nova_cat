# Nova Cat — The Open Nova Catalog

Nova Cat is a serverless platform for aggregating, validating, and publishing classical
nova observational data. It serves professional astronomers with a curated, open-access
catalog of spectra and photometry — discoverable through an interactive web interface
and downloadable as research-grade data bundles.

The system ingests data from public astronomical archives (ESO, CfA, AAVSO, and others),
resolves nova identity across archives, normalizes observations against IVOA standards,
and publishes them through a static artifact architecture that separates data generation
from presentation. The project draws inspiration from the Open Supernova Catalog
(Guillochon et al. 2017, ApJ 835:64).

**This is a solo project** — architecture, infrastructure, backend services, data
modeling, frontend, and documentation are all the work of a single developer with domain
expertise in nova astronomy.

**Live site:** [Nova-Cat](https://aws-nova-cat.vercel.app/)

---

## What This Project Demonstrates

- **End-to-end system design:** From ingestion pipeline through persistence layer through
  published data products through interactive frontend — a complete data platform, not
  just a service or a UI
- **Contract-first architecture:** Pydantic models define every inter-service boundary.
  JSON Schemas are auto-generated from contracts. Services are developed and tested
  against typed interfaces, not ad hoc payloads
- **Infrastructure as code:** Full AWS CDK deployment (Python) with two isolated stacks
  (production and smoke test), 12 Lambda functions, 5 Step Functions workflows, and
  single-table DynamoDB design
- **Domain-driven data modeling:** Multi-regime photometry (optical through radio),
  IVOA-aligned spectral normalization, deterministic identity resolution with coordinate
  deduplication, and a seven-layer ingestion pipeline designed to handle real-world data
  heterogeneity
- **Documentation discipline:** 21 architectural decision records, pre-ADR design
  documents, per-workflow operational docs, and formal schema specifications — written
  before implementation, not after
- **Scientific visualization:** Interactive Plotly.js components for spectra (waterfall
  plots with epoch controls and spectral feature markers) and photometry (multi-regime
  tabbed light curves with band toggles)
- **Rigorous quality gates:** mypy strict, ruff, CI with path-based filtering, end-to-end
  smoke tests against live AWS infrastructure

---

## Architecture Overview

Nova Cat runs on AWS serverless infrastructure, optimized for low operational cost. The
expected dataset is modest (<250 GB, <1000 novae) and the system is designed for single-
operator maintenance.

```
┌──────────────────────────────────────────────────────────────┐
│                    Public Archives                            │
│              (ESO, CfA, AAVSO, ADS, SIMBAD)                  │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              Ingestion Pipeline (Step Functions)              │
│                                                              │
│  ingest_new_nova → initialize_nova → discover_spectra →      │
│  acquire_and_validate_spectra → refresh_references           │
│                                                              │
│  12 Lambda services · FITS profile validation · quarantine   │
│  semantics · deterministic identity resolution               │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                  Persistence Layer                            │
│                                                              │
│  DynamoDB (single-table, namespaced PK) + S3 (FITS files,   │
│  derived artifacts, quarantine zone)                         │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              Published Artifacts (S3 + CloudFront)            │
│                                                              │
│  catalog.json · nova.json · spectra.json · photometry.json   │
│  sparkline.svg · bundle.zip                                  │
│                                                              │
│  Pre-computed, frontend-ready · schema-versioned             │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                     Frontend (Next.js)                        │
│                                                              │
│  Catalog table · Nova detail pages · Spectra waterfall       │
│  viewer · Multi-regime light curve panel · Client-side       │
│  search · Semantic design token system                       │
└──────────────────────────────────────────────────────────────┘
```

### Key Architectural Decisions

- **UUID-first identity** — Names are resolved once during ingestion; all downstream
  operations use stable UUIDs. Coordinate-based deduplication prevents duplicate novae
  across archives (<2" = alias, 2–10" = quarantine, >10" = new object)
- **Atomic data products** — Each spectrum is an independent unit with its own
  acquisition status, validation state, fingerprint, and provenance. No dataset-level
  abstractions
- **Profile-driven validation** — FITS files are validated against per-instrument
  profiles that define expected wavelength ranges, flux units, and header keywords.
  Non-conforming data is quarantined, not silently accepted
- **Published artifact architecture** — The frontend consumes pre-built static JSON
  artifacts. All scientific computation (normalization, subsampling, offset calculation)
  happens at artifact generation time. The website runs with zero backend API calls
- **Explicit quarantine semantics** — Irreconcilable conflicts (identity ambiguity,
  validation failure, metadata conflicts) are routed to quarantine for operator review
  rather than silently resolved

---

## System Components

### Spectra Ingestion Pipeline

**Status: Operational — validated through end-to-end smoke tests against real novae.**

The pipeline discovers spectra from provider archives, acquires and fingerprints the raw
FITS files, validates them against instrument-specific profiles, normalizes to an
IVOA-aligned canonical representation, and persists validated products with full
provenance.

Services: `nova_resolver` (identity resolution), `archive_resolver` (SIMBAD/TNS
queries), `spectra_discoverer` (provider adapter dispatch), `spectra_acquirer`
(download, fingerprint, ZIP unpack), `spectra_validator` (FITS profile validation and
normalization), `reference_manager` (ADS bibcode integration, discovery date
computation), `quarantine_handler` (conflict isolation), `workflow_launcher`
(cross-workflow orchestration).

### Photometry Ingestion Pipeline

**Status: In design — foundational ADRs complete, implementation paused pending
design chain completion.**

The photometry system handles multi-regime data (optical magnitudes, X-ray count rates,
gamma-ray photon fluxes, radio flux densities) through a seven-layer architecture: source
file unpacking, pre-ingestion normalization (wide-to-long pivot, multi-nova splitting,
sidecar context loading), a versioned band registry seeded from the SVO Filter Profile
Service, a disambiguation algorithm for ambiguous filter strings, a revised photometry
table model with provenance fields, column mapping via a tiered adapter architecture
(canonical CSV → synonym → UCD → AI-assisted), and row-level DynamoDB persistence.

Design artifacts: `DESIGN-001` (full pipeline redesign), `DESIGN-002` (data provenance
and derived quantities), ADR-015 through ADR-021.

### Frontend

**Status: Feature-complete MVP — all pages and visualization components implemented.**

A React/Next.js application providing catalog browsing, per-nova detail pages, and
interactive scientific visualizations. The design system uses a two-layer semantic CSS
token architecture (primitive → semantic) that supports dark mode without component
changes.

Key components:
- **Catalog table** — TanStack Table with sorting, pagination, and client-side
  name/alias search
- **Spectra viewer** — Plotly.js waterfall plot with epoch format toggles (DPO/MJD/
  Calendar Date), log/linear temporal scale, single-spectrum isolation mode, and
  spectral feature marker overlays (Fe II / He-N / Nebular emission lines)
- **Light curve panel** — Tabbed Plotly.js scatter plot (one tab per wavelength regime),
  multi-band color scheme with toggleable legend, upper limit markers, error bar toggle

Technology: React, Next.js, TypeScript, Plotly.js, TanStack Table, Tailwind CSS v4,
DM Sans + DM Mono typography, Lucide icons.

---

## Project Structure

```
contracts/          Pydantic models, event schemas, JSON Schema exports
docs/
  adr/              21 architectural decision records (ADR-001 through ADR-021)
  architecture/     Architectural baselines and system diagrams
  design/           Pre-ADR scoping documents
  specs/            Schema specifications (photometry table, FITS profiles)
  storage/          DynamoDB item model, access patterns, S3 layout
  workflows/        Per-workflow operational documentation
frontend/           React/Next.js web application
  src/components/   Catalog table, spectra viewer, light curve panel
  src/styles/       Semantic design tokens
infra/              AWS CDK infrastructure (Python)
  nova_constructs/  CDK constructs (compute, storage, workflows)
  workflows/        Step Functions ASL definitions
services/           12 Lambda services (3 Docker-based for astropy/numpy)
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
- **Lambda:** 12 functions; 3 Docker-based (astropy/numpy compiled dependencies)
- **Step Functions:** 5 workflows orchestrating the ingestion pipeline
- **DynamoDB:** Single-table design with namespaced partition keys and one GSI
- **S3:** Private bucket (raw data, derived artifacts, quarantine) and public site
  bucket (published frontend artifacts)
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

---

## Current Status

| Component | Status |
|-----------|--------|
| Spectra ingestion pipeline | Operational (smoke-tested against real novae) |
| Reference management | Operational |
| Identity resolution | Operational |
| Photometry pipeline design | ADR-017 → ADR-018 → ADR-019 design chain in progress |
| Frontend MVP | Feature-complete (spectra viewer, light curve panel, catalog) |
| Artifact generation pipeline | Not yet built (connects backend to frontend) |
| Hosting / deployment | Vercel (app) + S3/CloudFront (data) — not yet formalized |

---

## License

TBD
