# Nova Cat — Current Architecture Snapshot

_Last Updated: 2026-04-08_

This document captures the **authoritative architectural baseline** of Nova Cat at this
point in time.

It defines:
- System boundaries
- Core domain model
- Workflow architecture (ingestion + artifact regeneration)
- Persistence model
- Identity rules
- Validation strategy
- Artifact generation and publication pipeline
- Frontend architecture and delivery layer
- Observability model
- Deployment model

This document supersedes informal descriptions elsewhere.
If drift is detected between artifacts, this file represents intended reality.

---

# 1. System Overview

Nova Cat is a serverless AWS application for aggregating, validating, publishing, and
delivering classical nova data to a public website.

Core characteristics:

- Python-based backend, TypeScript/React frontend
- AWS serverless (Lambda, Step Functions, DynamoDB, S3, Fargate, CloudFront,
  EventBridge)
- UUID-first identity
- Contract-first design (Pydantic → JSON Schema)
- Atomic spectra data products
- Profile-driven validation
- IVOA-aligned canonical representation
- Immutable release publication model
- Low throughput, cost-aware architecture

The system spans four subsystems:

1. **Ingestion pipelines** — discover, acquire, validate, and persist spectra and
   photometry from public archives and hand-curated tickets.
2. **Artifact regeneration pipeline** — transform internal DynamoDB/S3 state into
   published JSON/SVG/ZIP artifacts for frontend consumption.
3. **Delivery layer** — serve published artifacts to browsers via S3 + CloudFront.
4. **Frontend** — React/Next.js website consuming static artifacts with no runtime
   backend communication.

Two ingestion paths coexist:

- **Archive-driven pipeline** (operational): Discovers and acquires spectra from public
  astronomical archives (ESO, CfA, AAVSO), validates FITS files against instrument
  profiles, and persists normalized products.
- **Ticket-driven pipeline** (MVP primary, operational): Ingests photometry
  and spectra from hand-curated metadata tickets that completely describe each data
  file's structure. Bypasses runtime heuristics and the disambiguation algorithm.
  Designed in DESIGN-004; governed by `ingest_ticket` workflow.

---

# 2. Identity Model

## 2.1 Stable Identifiers

All persistent entities use UUIDs:

- `nova_id`
- `data_product_id`
- `bibcode`

Names are never used as identifiers downstream of `initialize_nova`.

## 2.2 Name Resolution

Name-based resolution occurs only in `initialize_nova`.

Mechanism:
- `NAME#<normalized_name>` global partition
- Maps alias → `nova_id`
- Deterministic normalization applied

Duplicate detection via coordinate thresholds:

| Separation | Action |
|------------|--------|
| < 2″       | Attach alias to existing nova |
| 2–10″      | Identity quarantine |
| > 10″      | Create new nova |

Identity quarantine persists as `Nova.status = QUARANTINED`.

## 2.3 Locator Identity and data_product_id

**`data_product_id` — stable, deterministic UUID (SPECTRA products)**

Minted during `discover_spectra_products`. Derived as follows:
- **Preferred:** `UUID(hash(provider + provider_product_key))` — when a provider-native
  product ID is available.
- **Fallback:** `UUID(hash(provider + normalized_canonical_locator))` — when no native
  ID exists.

For ticket-ingested spectra, `data_product_id` is derived as
`UUID(hash(bibcode + spectrum_filename + nova_id))` to ensure idempotency without a
provider-native key.

Immutable once assigned; never reused across distinct products. See ADR-003 for full
specification.

`LOCATOR#<provider>#<locator_identity>` ensures stable deduplication.

---

# 3. Workflow Architecture — Ingestion

## 3.1 Workflow Inventory

| Workflow | Type | Purpose |
|----------|------|---------|
| `initialize_nova` | Express | Resolve a candidate name/coordinates to a stable `nova_id`. Creates or attaches to existing nova. |
| `ingest_new_nova` | Standard | Orchestrates downstream launches (`refresh_references`, `discover_spectra_products`) for a newly resolved nova. |
| `refresh_references` | Standard | Fetches ADS references for a nova, upserts Reference and NovaReference items, computes `discovery_date`. |
| `discover_spectra_products` | Standard | Queries provider archives, assigns `data_product_id` values, persists product stubs, fans out `acquire_and_validate_spectra`. |
| `acquire_and_validate_spectra` | Standard | Downloads spectra bytes, validates FITS against instrument profiles, persists normalized products to S3 + DDB. |
| `ingest_ticket` | Standard | Ingests hand-curated photometry or spectra tickets. Resolves nova identity, then branches by ticket type. |

All Standard Workflows follow a consistent execution pattern: `BeginJobRun` →
`AcquireIdempotencyLock` → domain-specific states → `FinalizeJobRunSuccess` (or
`TerminalFailHandler` → `FinalizeJobRunFailed`). Quarantine outcomes route through
`QuarantineHandler` → `FinalizeJobRunQuarantined`.

## 3.2 ingest_ticket Workflow (DESIGN-004)

The ticket-driven ingestion path is the MVP primary ingestion mechanism. A ticket is a
plain-text `.txt` file that fully describes one data file's structure (column indices,
filter names, wavelength units, etc.).

States: `ParseTicket` → `ResolveNova` → `TicketTypeBranch` → (`IngestPhotometry` |
`IngestSpectra`) → `FinalizeJobRunSuccess`.

**Photometry branch:** reads headerless CSV using ticket-supplied column indices,
resolves filter strings against the band registry (two-step: alias lookup → Generic
fallback), constructs `PhotometryRow` objects, writes to the dedicated photometry DDB
table via conditional `PutItem`, updates the `PRODUCT#PHOTOMETRY_TABLE` envelope item.

**Spectra branch:** reads metadata CSV (two-hop indirection), converts each spectrum CSV
to FITS with reconstructed headers, uploads to the Public S3 bucket, inserts DataProduct
+ FileObject reference items in the main NovaCat table. Container-based Lambda (astropy
dependency).

### Relationship to Existing Workflows

- `initialize_nova` — called (sync invocation) by ResolveNova for unknown object names;
  not modified.
- `ingest_photometry` — not called. `ingest_ticket` writes `PhotometryRow` items
  directly using the same DDB schema (ADR-020) but through a different code path.
- `acquire_and_validate_spectra` — not called. `ingest_ticket` produces FITS files and
  DDB references directly, bypassing the discovery/acquisition/validation pipeline.
  Output artifacts are compatible.

## 3.3 WorkItem Emission

All ingestion workflows write a **WorkItem** to the `WORKQUEUE` DynamoDB partition as a
best-effort final step before `FinalizeJobRunSuccess` (ADR-031 Decision 7). WorkItems
signal to the artifact regeneration pipeline which novae have new data. Each WorkItem
names the nova and what changed (`spectra`, `photometry`, or `references`).

| Workflow | dirty_type |
|----------|------------|
| `ingest_ticket` (photometry branch) | `photometry` |
| `ingest_ticket` (spectra branch) | `spectra` |
| `acquire_and_validate_spectra` | `spectra` |
| `refresh_references` | `references` |

WorkItems carry a 30-day TTL and are deleted by the Finalize Lambda after successful
artifact generation.

---

# 4. Workflow Architecture — Artifact Regeneration

Designed in DESIGN-003. Connects the backend persistence layer to the frontend
presentation layer.

## 4.1 Pipeline Overview

```
Ingestion Workflows → WorkItem Queue → EventBridge (6h cron) → Coordinator Lambda
→ RegenBatchPlan → Step Functions → Fargate Task → S3 Release → CloudFront → Browser
```

Three mechanisms:
1. **Work queue** — tracks what changed (WorkItems in the `WORKQUEUE` DDB partition).
2. **Sweep process** — generates artifacts (Coordinator Lambda → Fargate task).
3. **Immutable release model** — delivers artifacts to browsers (S3 → CloudFront).

## 4.2 Coordinator Lambda (`artifact_coordinator`)

Invoked by EventBridge on a 6-hour schedule (configurable via CDK context) or manually.
The coordinator is a planning and dispatch step — it never generates artifacts.

Execution:
1. Query the work queue (`PK=WORKQUEUE`, paginated).
2. Check for stale or in-progress batch plans.
   - `PENDING` plan → abandon and rebuild.
   - `IN_PROGRESS` plan → exit immediately.
3. Build per-nova manifests via the dependency matrix.
4. Emit structured warnings for stale WorkItems (>7 days).
5. Persist a `RegenBatchPlan` item with status `PENDING`.
6. Start the `regenerate_artifacts` Step Functions workflow.

Exit paths: empty queue (no-op), in-progress plan (exit with log), or normal dispatch.

## 4.3 Dependency Matrix

Maps dirty types to artifacts requiring regeneration:

| dirty_type | Artifacts regenerated |
|------------|----------------------|
| `spectra` | `references.json`, `spectra.json`, `sparkline.svg`, `nova.json`, `bundle.zip` |
| `photometry` | `photometry.json`, `sparkline.svg`, `nova.json`, `bundle.zip` |
| `references` | `references.json`, `nova.json`, `bundle.zip` |

`catalog.json` is always regenerated on every sweep (it is global, not per-nova).

## 4.4 Regeneration Workflow (`regenerate_artifacts`)

Standard Step Functions workflow with four states:

1. **UpdatePlanInProgress** (Task → Lambda) — Sets plan status to `IN_PROGRESS`.
2. **RunArtifactGenerator** (Task → ECS RunTask .sync) — Launches the Fargate task and
   waits for completion.
3. **Finalize** (Task → Lambda) — Commits succeeded novae: deletes consumed WorkItems,
   writes observation counts (`spectra_count`, `photometry_count`, `references_count`,
   `has_sparkline`) to Nova DDB items, updates plan status.
4. **FailHandler** (Task → Lambda) — If Fargate crashes: marks plan as `FAILED`. All
   WorkItems retained for the next sweep.

## 4.5 Fargate Task (`artifact_generator`)

Container-based ECS Fargate task (2 vCPU / 8 GB). Processes novae sequentially,
generating up to seven artifacts per nova in dependency order:

```
references.json → spectra.json → photometry.json → sparkline.svg → nova.json → bundle.zip
```

After all per-nova artifacts, a four-phase publication sequence runs:
1. **Phase 1** — Write swept novae artifacts to `releases/<YYYYMMDD-HHMMSS>/`.
2. **Phase 2** — Copy unchanged ACTIVE novae from previous release via `s3:CopyObject`.
3. **Phase 3** — Generate `catalog.json` and write to release prefix.
4. **Phase 4** — Update `current.json` pointer (atomic switchover).

Dependencies: astropy (coordinate formatting, FITS I/O), numpy, scipy (kd-tree for
photometry band offsets), band registry (loaded once per execution).

## 4.6 Per-Nova Artifact Generators

| Artifact | Input Sources | Key Computation |
|----------|---------------|-----------------|
| `references.json` | NovaReference + Reference items (DDB) | Orphan handling, year-descending sort |
| `spectra.json` | VALID DataProduct items (DDB) + web-ready CSVs (S3) | Peak-flux normalization, epoch sorting, DPO derivation |
| `photometry.json` | PhotometryRow items (dedicated DDB table) | Regime grouping, upper limit suppression, LTTB subsampling, kd-tree band offset, DPO derivation |
| `sparkline.svg` | Optical photometry (in-memory from photometry generator) | Band selection, coordinate transform to 90×55px viewport, SVG polyline + fill |
| `nova.json` | Nova item (DDB) + in-process counts from spectra/photometry generators | Coordinate formatting (decimal → sexagesimal), discovery date pass-through |
| `bundle.zip` | Raw FITS files (S3), PhotometryRow items (DDB), Reference items (DDB) | BibTeX generation, consolidated photometry FITS table, streaming ZIP assembly |
| `catalog.json` | DDB Scan of all ACTIVE novae + in-memory sweep results | Stats block computation, merge of sweep counts with persisted counts |

All generators produce artifacts conforming to ADR-014 schemas.

## 4.7 Concurrency

Only one sweep runs at a time. The batch plan status (`PENDING` / `IN_PROGRESS` /
`COMPLETED` / `FAILED`) serves as the coordination mechanism — no distributed lock
required.

---

# 5. Validation Architecture

Spectra validation is profile-driven.

Layered model:

1. Provider Adapter (discovery + acquisition)
2. FITS Profile (header normalization rules)
3. Generic Validator (IVOA-aligned canonicalization)

Unknown profile or missing critical metadata → QUARANTINE.

IVOA Spectrum DM / ObsCore conventions used where possible.

Ticket-ingested spectra bypass the validation pipeline entirely. The ticket provides
all structural metadata; FITS files are constructed by `ticket_ingestor` with
reconstructed headers rather than validated against instrument profiles.

---

# 6. Persistence Model

## 6.1 Main DynamoDB Table (NovaCat)

Single DynamoDB table:

### Per-nova partition

```
PK = <nova_id>
```

Item types:
- `NOVA` — core nova metadata (coordinates, status, names, discovery_date,
  observation counts, nova_type)
- `PRODUCT#...` — data product metadata (spectra, photometry envelope)
- `FILE#...` — S3 file references
- `NOVAREF#...` — nova-to-reference links
- `JOBRUN#...` — operational execution records
- `ATTEMPT#...` — per-attempt execution records

### Global identity partitions
- `NAME#<normalized_name>` — alias → nova_id mapping
- `LOCATOR#<provider>#<locator_identity>` — deduplication
- `REFERENCE#<bibcode>` — global reference metadata
- `WORKFLOW#<correlation_id>` — pre-nova workflow artifacts (e.g. FileObjects written
  during `initialize_nova` quarantine before a `nova_id` exists)
- `WORKQUEUE` — WorkItem records for the regeneration pipeline (§4)
- `REGEN_PLAN` — RegenBatchPlan records for sweep coordination (§4)

## 6.2 Dedicated Photometry Table (NovaCatPhotometry)

Separate DynamoDB table for `PhotometryRow` items (ADR-020 Decision 1). Kept separate
from the main NovaCat table because:

- `PhotometryRow` has a distinct schema and independent lifecycle
- `ticket_ingestor` and `ingest_photometry` need a narrowly scoped IAM grant that does
  not extend to all NovaCat entities
- Separate table simplifies future GSI design for cross-nova photometry queries without
  touching the main table

Primary key (ADR-020 Decision 2):
- `PK` (String) = `"<nova_id>"`
- `SK` (String) = `"PHOT#<row_id>"`

No GSI provisioned at this time.

The `PRODUCT#PHOTOMETRY_TABLE` envelope item (row counts, ingestion metadata) remains
in the main NovaCat table under the nova's partition.

## 6.3 S3 Layout

**Private bucket:**
- `raw/` — raw acquired spectra FITS files
- `derived/` — derived products including web-ready CSVs
  (`derived/spectra/<nova_id>/<data_product_id>/web_ready.csv`)
- `quarantine/` — quarantined files pending operator review
- `bundles/` — (legacy, unused by current pipeline)
- `diagnostics/` — row-level failure records for photometry ingestion
  (`diagnostics/photometry/<nova_id>/row_failures/<sha256>.json`)

**Public site bucket:**
- `releases/<YYYYMMDD-HHMMSS>/` — immutable release directories
  - `catalog.json` — global catalog artifact
  - `nova/<nova_id>/` — per-nova artifacts (nova.json, references.json,
    spectra.json, photometry.json, sparkline.svg, bundle.zip)
- `current.json` — pointer to the active release (updated atomically on each sweep)

Ticket-ingested FITS files: `raw/{nova_id}/ticket_ingestion/{data_product_id}.fits`
(in the Public S3 bucket).

---

# 7. Execution Governance

Failure taxonomy:
- `RETRYABLE` — transient failures eligible for Step Functions retry
- `TERMINAL` — permanent failures that fail the JobRun
- `QUARANTINE` — data quality issues requiring operator review

Idempotency:
- Internal only
- Enforced via JobRun + Attempt records

Correlation ID:
- Optional in boundary schemas
- Propagated across workflows

No idempotency keys exposed in boundary schemas.

---

# 8. Publication and Delivery

## 8.1 Immutable Release Model

Each sweep writes all artifacts — freshly generated and unchanged — to a new, uniquely
prefixed release directory (`releases/<YYYYMMDD-HHMMSS>/`). A stable pointer file
(`current.json`) at the bucket root identifies the active release. The frontend reads
the pointer first, then constructs artifact URLs relative to the release prefix.

Properties:
- **Atomic switchover.** Users see either the previous complete release or the new
  complete release — never a mix.
- **Trivial rollback.** Reverting is a single `put_object` updating the pointer.
- **Cache-friendly.** Every release produces new URL paths; no CloudFront invalidation
  needed under normal operations.

Old releases are expired by a 7-day S3 lifecycle rule.

## 8.2 CloudFront Distribution

Serves the public site S3 bucket via Origin Access Control (OAC). All public access
goes through CloudFront; the bucket blocks direct access.

Two cache behaviors:

| Path Pattern | Default TTL | Purpose |
|---|---|---|
| `/current.json` | 60 seconds | Mutable pointer; short TTL ensures ≤60s staleness after sweep |
| `/releases/*` | 7 days | Immutable release content; long TTL, no invalidation needed |

Additional configuration:
- CORS: `Access-Control-Allow-Origin: *` via response headers policy (not S3 CORS)
- Custom error response: 403 → 404 (standard OAC behavior for missing keys)
- Price Class All (cost difference negligible at MVP traffic)
- Compression: gzip + Brotli on all behaviors
- No Origin Shield at MVP
- No invalidation required under normal operations

The distribution domain name is exported as a CDK output for consumption by the Vercel
environment configuration.

Design reference: DESIGN-003 §13.

---

# 9. Frontend

## 9.1 Overview

The Open Nova Catalog website is a React/Next.js application that provides catalog
browsing, nova detail pages, and interactive scientific visualizations. It consumes
pre-built static JSON artifacts and performs no backend communication at runtime.

The frontend is a pure presentation layer. All scientific computation — normalization,
subsampling, offset calculation, outburst date resolution — is the backend's
responsibility and is embedded in the artifacts at generation time. The frontend applies
visual styling, layout, and client-side interaction only.

This architecture is defined in ADR-009 (published artifact architecture) and the
contracts are specified in ADR-014 (artifact schemas).

## 9.2 Technology Stack

- **Framework:** React with Next.js (App Router, TypeScript)
- **Visualization:** Plotly.js via react-plotly.js
- **Data table:** TanStack Table (@tanstack/react-table)
- **Styling:** Tailwind CSS v4, semantic CSS design tokens
- **Typography:** DM Sans (UI) + DM Mono (scientific data)
- **Icons:** Lucide React

Design decisions are captured in ADR-011 (architecture and tech stack) and ADR-012
(visual design system).

## 9.3 Design System

All colors are mediated through a two-layer CSS custom property system: primitive tokens
(raw color values) map to semantic tokens (intent-based names like `--color-interactive`
or `--color-surface-primary`). Components reference only semantic tokens. This
architecture enables dark mode without touching component code.

The token definitions live in `frontend/src/styles/tokens.css`. Tailwind utility class
mappings are defined in `frontend/src/app/globals.css` via the Tailwind v4 `@theme inline`
block.

Full specification: ADR-012.

## 9.4 Site Structure

| Route | Page | Description |
|-------|------|-------------|
| `/` | Homepage | Stats bar, hero explainer, 10-row catalog preview |
| `/catalog` | Catalog | Full paginated table of all novae, sortable, searchable |
| `/search` | Search | Search-focused variant of the catalog table |
| `/nova/[identifier]` | Nova detail | Two-column layout: visualizations (left) + metadata (right) |
| `/docs` | Documentation | Placeholder (content TBD) |
| `/about` | About | Placeholder (content TBD) |

Nova pages are accessible by both UUID (`/nova/<uuid>`) and primary name
(`/nova/<primary-name>`). Navigation model defined in ADR-010.

## 9.5 Component Architecture

**Catalog layer:**
- `CatalogTable` — TanStack Table component with sorting, pagination (25 rows),
  client-side name/alias search, and ADR-012-compliant visual treatment.

**Nova page layer:**
- `VisualizationRegion` — Container for the two visualization components; handles
  loading, error, and empty states independently for each.
- `SpectraViewer` — Plotly.js waterfall plot. Vertically offsets spectra by epoch.
  Supports three epoch label formats (DPO/MJD/Calendar Date), log/linear temporal
  scale, single-spectrum isolation mode, and spectral feature marker overlays
  (Fe II / He-N / Nebular). Full spec: ADR-013.
- `LightCurvePanel` — Tabbed Plotly.js scatter plot. One tab per wavelength regime
  (optical/X-ray/gamma/radio). Per-regime axis configuration. Multi-band color
  scheme with toggleable legend. Upper limit markers. Error bar toggle. Full spec:
  ADR-013.

**Shared utilities** (`src/lib/`):
- Epoch format conversion (MJD ↔ DPO ↔ Calendar Date)
- Catalog data loading (server-side filesystem read for SSG)
- Data client (`dataClient.ts`) for artifact fetching

## 9.6 Data Client

The data client (`src/lib/dataClient.ts`) centralizes all data-layer access behind
three functions:

- `resolveRelease()` — discovers the active release ID by fetching `current.json`
- `getArtifactUrl()` — constructs full artifact URLs relative to the release prefix
- `fetchArtifact<T>()` — convenience: resolve + fetch + parse

Environment modes:
- **Production:** `NEXT_PUBLIC_DATA_URL` is set to the CloudFront domain. URLs resolve
  to `${DATA_URL}/releases/${releaseId}/${path}`.
- **Development:** `NEXT_PUBLIC_DATA_URL` is unset. Returns a `"local"` release ID and
  URLs resolve to `/data/${path}` (Next.js public directory with mock fixtures).

Design reference: DESIGN-003 §14.

## 9.7 Artifact Consumption

The frontend fetches static JSON artifacts via the data client. In development, mock
fixtures are served from `frontend/public/data/`. In production, they are served from
the S3 bucket via CloudFront.

| Artifact | Path | Consumer |
|----------|------|----------|
| `catalog.json` | Root of release | Homepage, catalog table, search |
| `nova/<id>/nova.json` | Per-nova | Nova page metadata region |
| `nova/<id>/references.json` | Per-nova | Nova page references table |
| `nova/<id>/spectra.json` | Per-nova | Spectra viewer |
| `nova/<id>/photometry.json` | Per-nova | Light curve panel |
| `nova/<id>/sparkline.svg` | Per-nova | Catalog table light curve column |
| `nova/<id>/bundle.zip` | Per-nova | Download link on nova detail page |

Schemas: ADR-014.

---

# 10. Photometry Pipeline

The photometry system handles multi-regime data (optical magnitudes, X-ray count rates,
gamma-ray photon fluxes, radio flux densities). Two ingestion paths exist:

- **Ticket-driven path** (MVP primary, operational): Uses hand-curated metadata tickets
  that supply all structural information explicitly. Bypasses Layer 0 heuristics and the
  ADR-018 disambiguation algorithm. Implemented via the `ingest_ticket` workflow (§3.2).
  Photometry reader, DDB write layer, and band registry integration are operational.
- **Heuristic path** (future): Runtime inference via the seven-layer architecture
  described below. Designed but not yet implemented.

## 10.1 Layer Architecture

The seven-layer architecture governs the heuristic ingestion path. The ticket-driven
path bypasses Layers 0–4 by providing explicit structural metadata.

| Layer | Name | Status |
|-------|------|--------|
| UnpackSource | Source file unpacking | Designed (ADR-021) |
| 0 | Pre-ingestion normalization | Designed (ADR-021) |
| 1 | Band registry | Implemented (ADR-017); seeded from SVO FPS |
| 2 | Band resolution/disambiguation | Designed (ADR-018); not needed for ticket path |
| 3 | Photometry table model revision | Accepted (ADR-019 v2.0); `PhotometryRow` in contracts |
| 4 | Column mapping and adapter | Contracts merged; implementation paused |
| 5 | Ingestion workflow handlers | Ticket path implemented; heuristic path pending |
| 6 | Persistence and query | Implemented (ADR-020); dedicated DDB table provisioned |

## 10.2 Key Design Decisions

- **Row-level DynamoDB storage** (ADR-020) rather than canonical Parquet files
- **Dedicated photometry DynamoDB table** separate from the main NovaCat table (§6.2)
- **Band registry as a versioned data artifact** seeded from SVO FPS
- **Two-track `band_id` convention** (ADR-017 amendment): instrument-specific
  (`{Facility}_{Instrument}_{BandLabel}`) and Generic fallback (`Generic_{BandLabel}`)
- **`photometric_system` field dropped** system-wide (ADR-019); `band_id` makes it
  redundant
- **Row-level failure persistence** to S3 diagnostics for operator review
- **AI-assisted adapter registration** at development time only; no runtime dependency
- **Conservative photometric system defaults** and case-sensitive filter matching
  (ADR-016)

## 10.3 Design Chain Status

The foundational ADR chain is complete: ADR-017 (band registry) → ADR-018
(disambiguation) → ADR-019 (table model) → ADR-020 (storage format). The ticket-driven
path (DESIGN-004) provides the MVP implementation; the heuristic path (Layer 0 → adapter
→ persistence) remains as the future fallback for files without tickets.

Full design context: DESIGN-001, DESIGN-002, DESIGN-004.

---

# 11. Observability Model

Structured logs include:

- `workflow_name`
- `execution_arn`
- `job_run_id`
- `state_name`
- `attempt_number`
- `correlation_id`
- Primary UUID(s)
- `error_classification`
- `error_fingerprint`

Metrics:
- Success / failure / quarantine counts
- Retry rate
- Provider health
- Latency

Regeneration pipeline observability (DESIGN-003 §15):
- `plan_id`, `release_id`, `nova_id` in all Fargate task logs
- Per-nova success/failure tracking in RegenBatchPlan results
- CloudWatch alarms: sweep failure alarm, sweep skip alarm (48-hour no-success
  detection), wired to the existing SNS quarantine topic
- AWS Budget alert: $5/month on S3 + CloudFront + ECS combined spend

---

# 12. Architectural Invariants

The following must remain true:

- UUID-first downstream execution
- No name-based operations beyond resolution boundary
- Atomic spectra products
- Explicit quarantine semantics
- Deterministic locator identity
- Operational state separate from scientific state
- Continuation payload event model
- Minimal Step Functions branching
- Profile-driven validation
- ADS calls are never routed through archive_resolver. archive_resolver is
  scoped to nova identity resolution (SIMBAD + TNS) only.
- References use ADS bibcodes as their stable global key. No internal UUID is
  assigned to Reference or NovaReference items.
- Ticket-driven and heuristic ingestion paths produce compatible output artifacts.
  The same `PhotometryRow` schema (ADR-019) and DDB key structure (ADR-020) are
  shared across both paths.
- Published artifacts conform to ADR-014 schemas. The frontend never computes
  scientific quantities — all computation is the backend's responsibility and is
  embedded in artifacts at generation time.
- The immutable release model ensures atomic switchover and trivial rollback.
  `current.json` is the sole coordination point between the backend and frontend.

---

# 13. Deferred / Non-MVP

- Global multi-nova sweeps
- Spatial indexing
- Full VO compliance enforcement
- Advanced photometry version diffing
- Provider auto-discovery scaling
- Heuristic ingestion path (Layers 0–4 runtime implementation)
- Incremental / differential artifact updates (full regeneration per nova on every sweep)
- Multi-operator concurrency controls
- Programmatic API access to artifact data (deferred per ADR-011)
- Custom CloudFront domain (e.g. `data.nova-cat.org`)
- Origin Shield (CloudFront)
- `nova_type` enrichment (post-MVP; field exists as null)

---

# 14. Deployment Model

Nova Cat is deployed as two independent CDK stacks in the same AWS account and region:

## NovaCat (production)

The live stack. All production workflows, Lambdas, state machines, the Fargate task
definition, the CloudFront distribution, and the primary DynamoDB tables (`NovaCat` +
`NovaCatPhotometry`) live here. CloudFormation exports are prefixed `NovaCat-`.

## NovaCatSmoke (smoke test)

An isolated parallel deployment used exclusively by the smoke test suite. Identical to
`NovaCat` in every functional respect — same Lambda code, same ASL, same IAM grants —
but with independently namespaced resources:

- Lambda functions: `nova-cat-smoke-*`
- State machines: `nova-cat-smoke-*`
- DynamoDB tables: `NovaCatSmoke`, `NovaCatSmokePhotometry`
- CloudFormation exports: `NovaCatSmoke-*`

The smoke stack uses `DESTROY` removal policy throughout. Its DynamoDB tables are wiped
between test runs, eliminating any risk of smoke tests touching production data.

Smoke tests resolve all stack outputs from `NovaCatSmoke` via CloudFormation exports.
If the smoke stack is not deployed, all smoke tests skip cleanly with a descriptive
message.

Both stacks are deployed together via `./deploy.sh`. Individual stack targeting is
supported: `./deploy.sh NovaCat` or `./deploy.sh NovaCatSmoke`.

## Resource Counts

- **Lambda functions:** 17 (13 zip-bundled, 4 container-based)
  - Zip-bundled: `nova_resolver`, `job_run_manager`, `idempotency_guard`,
    `workflow_launcher`, `reference_manager`, `spectra_acquirer`,
    `photometry_ingestor`, `quarantine_handler`, `name_reconciler`,
    `ticket_parser`, `nova_resolver_ticket`, `artifact_coordinator`,
    `artifact_finalizer`
  - Container-based (DockerImageFunction): `archive_resolver`,
    `spectra_discoverer`, `spectra_validator`, `ticket_ingestor`
- **Step Functions workflows:** 7 (`initialize_nova` [Express], `ingest_new_nova`,
  `discover_spectra_products`, `acquire_and_validate_spectra`, `refresh_references`,
  `ingest_ticket`, `regenerate_artifacts`)
- **ECS resources:** 1 Fargate task definition (`artifact_generator`, 2 vCPU / 8 GB),
  1 ECS cluster
- **DynamoDB tables:** 2 per stack (main `NovaCat` + dedicated `NovaCatPhotometry`)
- **S3 buckets:** 2 (private data + public site)
- **CloudFront distributions:** 1 (public site bucket, OAC)
- **EventBridge rules:** 1 (6-hour cron for artifact coordinator)
- **SNS topics:** 1 (quarantine notifications)

---

# 15. ADR and Design Document Index

Architecture decisions and design documents govern this system:

- **ADR-001 – ADR-007:** Core system design (contracts, workflows, persistence,
  identity, references, architecture baseline)
- **ADR-008 – ADR-014:** Frontend design (product vision, MVP strategy, navigation,
  architecture, visual design system, visualization design, artifact schemas)
- **ADR-015 – ADR-021:** Photometry pipeline design (column mapping, band/filter
  resolution, band registry, storage format, pre-ingestion normalization)
- **ADR-030:** ADR amendment policy revision
- **ADR-031:** Data layer readiness for artifact generation (schema evolution,
  WorkItem integration, documentation alignment)
- **DESIGN-001:** Photometry ingestion redesign
- **DESIGN-002:** Data provenance and derived quantities
- **DESIGN-003:** Artifact regeneration pipeline (the "middle end")
- **DESIGN-004:** Source profile schema and ticket-driven ingestion

---

# End of Snapshot
