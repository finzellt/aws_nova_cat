# Nova Data Catalog

# Nova Cat

Nova Cat is a serverless, contract-first platform for aggregating, validating, and publishing classical nova data.

The system is designed to:

- Resolve and stabilize nova identity (UUID-first)
- Discover and ingest publicly available spectra
- Normalize spectral data using IVOA-aligned validation profiles
- Persist data using a deterministic single-table DynamoDB model
- Maintain clean workflow boundaries via AWS Step Functions
- Preserve operational traceability through structured logging and execution records
- Publish curated datasets for the astronomy community

Nova Cat prioritizes architectural clarity, cost-efficiency, and long-term maintainability over hyperscale optimization.
The expected dataset size is modest (<250 GB), and the platform is intentionally optimized for low operational overhead.

---

## Architectural Principles

Nova Cat is built around a few core invariants:

- **UUID-first identity** — All downstream operations use stable UUIDs (`nova_id`, `data_product_id`, `reference_id`).
- **Atomic data products** — Spectra are modeled as independent `DataProduct` units with deterministic identity.
- **Profile-driven validation** — Spectral data are normalized into an internal, IVOA-aligned canonical model using FITS profiles.
- **Single-table persistence** — DynamoDB is used with a namespaced single-table design.
- **Continuation-style workflows** — Step Functions workflows pass forward validated continuation payloads.
- **Operational/Scientific separation** — Retry and execution state are distinct from scientific validation state.
- **Documented architecture** — Major decisions are captured via ADRs and architecture snapshots.

---

## Project Structure

High-level repository layout:

```
infra/ # AWS CDK infrastructure (Python)
services/ # Lambda services
schemas/ # Versioned JSON schemas (generated from Pydantic)
docs/ # Architecture, storage, workflow, and engineering documentation
```


The authoritative architectural baseline lives at:

```
docs/architecture/current-architecture.md
```


Major design decisions are captured under:

```
docs/adr
```

Engineering evolution notes and spike retrospectives are under:

```
docs/engineering/
```


---

## Status

Nova Cat is currently in active implementation.

The architectural design phase is complete and aligned.
Implementation is proceeding in controlled, incremental stages.

---

## Motivation

Classical nova data are distributed across multiple public archives and are often inconsistently structured.

Nova Cat provides:

- Deterministic identity resolution
- Standards-aligned spectral normalization
- Clean ingestion boundaries
- Reproducible storage semantics
- Lightweight publication infrastructure

The project also serves as a demonstration of disciplined serverless architecture and contract-first system design.

---

## License

TBD

## Engineering Process & Governance

Nova Cat documents not only architectural decisions (via ADRs) but also major implementation inflection points.

- Epic 5 Vertical Slice Spike Retrospective
- ADR-004 Architecture Baseline
