# Nova Cat — Current Architecture Snapshot

_Last Updated: 2026-03-28_

This document captures the **authoritative architectural baseline** of Nova Cat at this point in time.

It defines:
- System boundaries
- Core domain model
- Workflow architecture
- Persistence model
- Identity rules
- Validation strategy
- Observability model

This document supersedes informal descriptions elsewhere.
If drift is detected between artifacts, this file represents intended reality.

---

# 1. System Overview

Nova Cat is a serverless AWS application for aggregating, validating, and publishing classical nova data.

Core characteristics:

- Python-based
- AWS serverless (Lambda, Step Functions, DynamoDB, S3)
- UUID-first identity
- Contract-first design (Pydantic → JSON Schema)
- Atomic spectra data products
- Profile-driven validation
- IVOA-aligned canonical representation
- Low throughput, cost-aware architecture

The system is designed for **singular nova ingestion** in MVP.

Two ingestion paths coexist:

- **Archive-driven pipeline** (operational): Discovers and acquires spectra from public
  astronomical archives (ESO, CfA, AAVSO), validates FITS files against instrument
  profiles, and persists normalized products.
- **Ticket-driven pipeline** (MVP primary, partially implemented): Ingests photometry
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

---

## 2.2 Name Resolution

Name-based resolution occurs only in `initialize_nova`.

Mechanism:
- `NAME#<normalized_name>` global partition
- Maps alias → `nova_id`
- Deterministic normalization applied

Duplicate detection via coordinate thresholds:

| Separation | Action |
|------------|--------|
| < 2"       | Attach alias to existing nova |
| 2–10"      | Identity quarantine |
| > 10"      | Create new nova |

Identity quarantine persists as:

`Nova.status = QUARANTINED`


---

## 2.3 Locator Identity and data_product_id

**`data_product_id` — stable, deterministic UUID (SPECTRA products)**

Minted during `discover_spectra_products`. Derived as follows:
- **Preferred:** `UUID(hash(provider + provider_product_key))` — when a provider-native product ID is available.
- **Fallback:** `UUID(hash(provider + normalized_canonical_locator))` — when no native ID exists.

For ticket-ingested spectra, `data_product_id` is derived as
`UUID(hash(bibcode + spectrum_filename + nova_id))` to ensure idempotency
without a provider-native key.

Immutable once assigned; never reused across distinct products. See ADR-003 for full specification.

`LOCATOR#<provider>#<locator_identity>` ensures stable deduplication.

---

# 3. Domain Model (Persistent Entities)

## 3.1 Nova

Fields include:

- nova_id
- primary_name
- primary_name_normalized
- status
- position (ra_deg, dec_deg, frame, epoch)
- discovery_date — YYYY-MM-DD string; day `00` when only month precision is available
  (e.g. `"2013-06-00"`); never use `01` as a proxy for unknown day.
  See ADR-005 Amendment (Discovery Date Precision).

Derived astronomical metadata (e.g., constellation) is not persisted.

---

## 3.2 DataProduct (Spectra)

Atomic unit of ingestion and validation.

Each product has independent:

- acquisition_status
- validation_status
- cooldown metadata
- fingerprint (sha256)
- header signature hash
- fits_profile_id
- quarantine_reason_code (if applicable)

No dataset abstraction exists.

---

## 3.3 PhotometryRow

Individual photometric observation. Stored in the dedicated photometry DynamoDB table
(§6), not the main NovaCat table. Schema defined in ADR-019 (v2.0).

Each row carries:

- `row_id` — deterministic UUID derived from
  `SHA-256(nova_id|epoch_raw|band_id|magnitude_raw|filename)[:16 bytes]`
- `nova_id` — FK to Nova
- `band_id` — canonical band identifier (ADR-017 two-track convention:
  `{Facility}_{Instrument}_{BandLabel}` or `Generic_{BandLabel}`)
- `time_mjd` — epoch in Modified Julian Date (converted from source time system)
- `magnitude`, `mag_err` — observed value and uncertainty
- `is_upper_limit` — boolean flag for non-detection limits
- Band resolution provenance: `band_resolution_type` (canonical / synonym /
  generic_fallback) and `band_resolution_confidence` (high / low)
- Per-row metadata: `telescope`, `observer`, `bibcode`, `data_origin`, `data_rights`

Photometry rows are written via conditional `PutItem` keyed on `row_id` to ensure
idempotency. Duplicate rows (same `row_id`) are silently suppressed.

---

## 3.4 BandRegistryEntry

Frozen Pydantic model representing a single entry in the band registry (ADR-017).
Read-only at runtime; the registry is a versioned JSON data artifact seeded from the
SVO Filter Profile Service.

Fields include:

- `band_id` — canonical identifier
- `regime` — wavelength regime (optical, uv, nir, etc.)
- `svo_filter_id` — SVO FPS identifier (if sourced from SVO)
- `lambda_eff`, `bandpass_width` — spectral metadata
- `aliases` — list of known alternative filter strings
- `is_excluded` — flag for bands that should not be ingested

The registry module exposes a read-only Python API: `lookup_band_id`, `get_entry`,
`is_excluded`, `list_all_entries`.

---

## 3.5 Ticket Models

Typed Pydantic models for the ticket-driven ingestion path (DESIGN-004). Defined in
`contracts/models/tickets.py`.

Two ticket types exist as a discriminated union:

- **PhotometryTicket** — describes a headerless CSV of photometric observations.
  Carries column index mappings (0-based) for time, flux, error, filter string, and
  optional per-row telescope/observer columns, plus ticket-level defaults.
- **SpectraTicket** — describes a metadata CSV whose rows each reference individual
  spectrum data files (two-hop indirection: ticket → metadata CSV → spectrum CSVs).
  Carries column index mappings for the metadata CSV fields.

Both types share common fields via `_TicketCommon` (not exported): `object_name`,
`wavelength_regime`, `time_system`, `assumed_outburst_date`, `reference`, `bibcode`,
`ticket_status`. All models use `extra = "forbid"`.

---

## 3.6 FileObject

Represents a raw or derived file stored in S3, linked to a DataProduct or Nova.

---

## 3.7 Reference and NovaReference

Reference: global entity keyed by ADS `bibcode`.

reference_type values: journal_article, conference_abstract, poster, catalog,
software, atel, cbat_circular, arxiv_preprint, other.

The nova-to-reference relationship is recorded in NovaReference items
(many-to-many; no duplication of the Reference item). NovaReference carries:
- role (DISCOVERY | SPECTRA_SOURCE | PHOTOMETRY_SOURCE | OTHER)
- added_by_workflow, notes, and link-level provenance

The link is fully identified by (nova_id, bibcode). No UUID on the link item itself.

See dynamodb-item-model.md sections 6 and 7 for full item shapes.
See ADR-005 for the global entity decision and ADS integration strategy.


---

## 3.8 Operational Records

### JobRun
One per workflow execution.

### Attempt
One per task invocation (including retries).

Operational state is separate from scientific state.

---

# 4. Workflow Architecture

All workflows operate on UUIDs only.

## 4.1 initialize_nova
- Accepts candidate_name
- Performs name resolution
- Creates nova if necessary
- Launches ingest_new_nova

Terminal outcomes:
- CREATED_AND_LAUNCHED
- EXISTS_AND_LAUNCHED
- NOT_FOUND
- NOT_A_CLASSICAL_NOVA

---

## 4.2 ingest_new_nova
Coordinator:
- refresh_references
- discover_spectra_products

---

## 4.3 discover_spectra_products
- Map across providers
- Adapter-based discovery
- Assign `data_product_id` (stable UUID derived as `UUID(hash(provider + provider_product_key))`
  or `UUID(hash(provider + normalized_canonical_locator))`; see ADR-003)
- Publish AcquireAndValidateSpectra continuation event

---

## 4.4 acquire_and_validate_spectra
- Download bytes
- Unzip if necessary
- Compute fingerprint
- Select FITS profile
- Normalize to canonical model
- Classify outcome (RETRYABLE / TERMINAL / QUARANTINE)
- Persist results

Eligibility index removed immediately after validation.

---

## 4.5 refresh_references
- Upsert references
- Link nova to references
- Compute discovery_date

---

## 4.6 ingest_photometry

- Accepts API-driven photometry upload
- Resolves name → `nova_id`
- Rebuilds and overwrites the canonical photometry table
- Updates ingestion summary fields
- If schema version changes (future capability), snapshots prior table before overwrite

No dataset abstraction exists.

---

## 4.7 name_check_and_reconcile

- Accepts `nova_id`
- Performs name normalization and reconciliation checks against the global `NameMapping` index
- Proposed naming inputs (`proposed_public_name`, `proposed_aliases`) are passed via
  `attributes` rather than typed fields, consistent with the minimal stable contract pattern
- Operates entirely within the UUID-first execution model; no name-based downstream
  operations are performed beyond the resolution boundary

---

## 4.8 ingest_ticket

Ticket-driven ingestion of photometry and spectra from hand-curated metadata tickets.
This is the **primary ingestion path for MVP**. Designed in DESIGN-004; full workflow
specification in `docs/workflows/ingest-ticket.md`.

### State Machine

```
ticket.txt → ParseTicket → ResolveNova → TicketTypeBranch
                                              │
                                         ┌────┴────┐
                                         │         │
                                    photometry   spectra
                                         │         │
                                    CSV rows    metadata CSV
                                         │         │
                                    band reg    per-spectrum:
                                    resolve      CSV → FITS
                                         │         │
                                    DDB PutItem   S3 upload +
                                  (PhotometryRow)  DDB ref
```

States: ValidateInput → EnsureCorrelationId → BeginJobRun → AcquireIdempotencyLock
→ ParseTicket → ResolveNova → TicketTypeBranch (Choice) → IngestPhotometry /
IngestSpectra → FinalizeJobRunSuccess. Quarantine and terminal failure branches
follow the standard shared-handler pattern.

### Lambda Handlers

| Handler | Service Module | Task States | Description |
|---|---|---|---|
| `ticket_parser` | `services/ticket_parser/` | ParseTicket | Reads `.txt` ticket file from S3, parses key-value pairs into raw dict, discriminates ticket type (`DATA FILENAME` → photometry, `METADATA FILENAME` → spectra), validates and coerces into `PhotometryTicket` or `SpectraTicket` via Pydantic. Two-stage pipeline: raw key-value parse → type discrimination/coercion/Pydantic construction. Single error surface: `TicketParseError`. |
| `nova_resolver_ticket` | `services/nova_resolver_ticket/` | ResolveNova | Preflight DDB `NameMapping` lookup for existing `nova_id`. If not found, fires `initialize_nova` via `sfn:StartExecution` and polls `describe_execution` until terminal (max 30 poll attempts). Returns `nova_id`, `primary_name`, `ra_deg`, `dec_deg`. Quarantine outcomes: `UNRESOLVABLE_OBJECT_NAME` (NOT_FOUND), `IDENTITY_AMBIGUITY` (QUARANTINED). |
| `ticket_ingestor` | `services/ticket_ingestor/` | IngestPhotometry, IngestSpectra | Single Lambda with `task_name` dispatch. **Photometry branch:** reads headerless CSV using ticket-supplied column indices, resolves filter strings against band registry (two-step: alias lookup → Generic fallback), constructs `PhotometryRow` objects, writes to dedicated photometry DDB table via conditional `PutItem`, updates `PRODUCT#PHOTOMETRY_TABLE` envelope item. **Spectra branch:** reads metadata CSV (two-hop indirection), converts each spectrum CSV to FITS with reconstructed headers, uploads to Public S3 bucket, inserts DataProduct + FileObject reference items in main NovaCat table. Container-based Lambda (astropy dependency). |

### Relationship to Existing Workflows

- `initialize_nova` — called (fire-and-poll) by ResolveNova for unknown object names;
  not modified.
- `ingest_photometry` — not called. `ingest_ticket` writes `PhotometryRow` items
  directly using the same DDB schema (ADR-020) but through a different code path.
- `acquire_and_validate_spectra` — not called. `ingest_ticket` produces FITS files and
  DDB references directly, bypassing the discovery/acquisition/validation pipeline.
  Output artifacts are compatible.

### Input Event

- `ticket_path` (required) — S3 key or local path to the `.txt` ticket file
- `data_dir` (required) — S3 prefix or local directory containing referenced data files
- `correlation_id` (optional) — generated if missing

No downstream workflow event is published. This is a terminal ingestion path.

---

# 5. Validation Architecture

Spectra validation is profile-driven:

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
- NOVA
- PRODUCT#...
- FILE#...
- NOVAREF#...
- JOBRUN#...
- ATTEMPT#...

### Global identity partitions
- NAME#<normalized_name>
- LOCATOR#<provider>#<locator_identity>
- REFERENCE#<bibcode>
- WORKFLOW#<correlation_id> — pre-nova workflow artifacts (e.g. FileObjects written
  during `initialize_nova` quarantine before a `nova_id` exists)

## 6.2 Dedicated Photometry Table (NovaCatPhotometry)

Separate DynamoDB table for `PhotometryRow` items (ADR-020 Decision 1). Kept separate
from the main NovaCat table because:

- `PhotometryRow` has a distinct schema and independent lifecycle
- `ticket_ingestor` and `ingest_photometry` need a narrowly scoped IAM grant that
  does not extend to all NovaCat entities
- Separate table simplifies future GSI design for cross-nova photometry queries
  without touching the main table

Primary key (ADR-020 Decision 2):
- `PK` (String) = `"<nova_id>"`
- `SK` (String) = `"PHOT#<row_id>"`

No GSI provisioned at this time. A future GSI on band + epoch fields will enable
cross-nova queries (ADR-020 OQ-5); it can be added without storage migration.

The `PRODUCT#PHOTOMETRY_TABLE` envelope item (row counts, ingestion metadata) remains
in the main NovaCat table under the nova's partition.

## 6.3 S3 Layout

S3 layout:
- raw/
- derived/
- quarantine/
- bundles/
- site/releases/
- diagnostics/ — row-level failure records for photometry ingestion

Ticket-ingested FITS files: `raw/{nova_id}/ticket_ingestion/{data_product_id}.fits`
(in the Public S3 bucket).

Photometry row failure diagnostics:
`diagnostics/photometry/<nova_id>/row_failures/<ticket_filename_sha256>.json`
(in the Private S3 bucket).

---

# 7. Execution Governance

Failure taxonomy:

- RETRYABLE
- TERMINAL
- QUARANTINE

Idempotency:
- Internal only
- Enforced via JobRun + Attempt records

Correlation ID:
- Optional in boundary schemas
- Propagated across workflows

No idempotency keys exposed in boundary schemas.

---

# 8. Frontend

## 8.1 Overview

The Open Nova Catalog website is a React/Next.js application that provides catalog
browsing, nova detail pages, and interactive scientific visualizations. It consumes
pre-built static JSON artifacts and performs no backend communication at runtime.

The frontend is a pure presentation layer. All scientific computation — normalization,
subsampling, offset calculation, outburst date resolution — is the backend's
responsibility and is embedded in the artifacts at generation time. The frontend applies
visual styling, layout, and client-side interaction only.

This architecture is defined in ADR-009 (published artifact architecture) and the
contracts are specified in ADR-014 (artifact schemas).

## 8.2 Technology Stack

- **Framework:** React with Next.js (App Router, TypeScript)
- **Visualization:** Plotly.js via react-plotly.js
- **Data table:** TanStack Table (@tanstack/react-table)
- **Styling:** Tailwind CSS v4, semantic CSS design tokens
- **Typography:** DM Sans (UI) + DM Mono (scientific data)
- **Icons:** Lucide React

Design decisions are captured in ADR-011 (architecture and tech stack) and ADR-012
(visual design system).

## 8.3 Design System

All colors are mediated through a two-layer CSS custom property system: primitive tokens
(raw color values) map to semantic tokens (intent-based names like `--color-interactive`
or `--color-surface-primary`). Components reference only semantic tokens. This
architecture enables dark mode without touching component code.

The token definitions live in `frontend/src/styles/tokens.css`. Tailwind utility class
mappings are defined in `frontend/src/app/globals.css` via the Tailwind v4 `@theme inline`
block.

Full specification: ADR-012.

## 8.4 Site Structure

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

## 8.5 Component Architecture

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

## 8.6 Artifact Consumption

The frontend fetches static JSON artifacts at page load time. During development, these
are mock fixtures served from `frontend/public/data/`. In production, they will be served
from an S3 bucket via CloudFront (hosting architecture not yet formalized — see §8.7).

| Artifact | Path | Consumer |
|----------|------|----------|
| `catalog.json` | Root | Homepage, catalog table, search |
| `nova/<id>/nova.json` | Per-nova | Nova page metadata region |
| `nova/<id>/references.json` | Per-nova | Nova page references table |
| `nova/<id>/spectra.json` | Per-nova | Spectra viewer |
| `nova/<id>/photometry.json` | Per-nova | Light curve panel |
| `nova/<id>/sparkline.svg` | Per-nova | Catalog table light curve column |

Schemas: ADR-014.

## 8.7 Open Architecture Gaps (Frontend)

The following are identified but not yet designed:

- **Hosting and deployment:** How Vercel (app hosting) connects to S3/CloudFront
  (data artifact serving). URL patterns, environment variables, CORS, cache headers.
  Flagged as ADR-011 Open Question 3.
- **Artifact generation pipeline:** The `generate_site_data` pipeline that transforms
  internal DynamoDB/S3 data into the published ADR-014 JSON artifacts. Not yet designed.
- **Publication gate:** The trigger mechanism for artifact regeneration (automatic on
  ingestion vs. operator-initiated vs. scheduled). Not yet designed.

---

# 9. Photometry Pipeline

The photometry system handles multi-regime data (optical magnitudes, X-ray count rates,
gamma-ray photon fluxes, radio flux densities). Two ingestion paths exist:

- **Ticket-driven path** (MVP primary, partially implemented): Uses hand-curated
  metadata tickets that supply all structural information explicitly. Bypasses
  Layer 0 heuristics and the ADR-018 disambiguation algorithm. Implemented via the
  `ingest_ticket` workflow (§4.8). Photometry reader, DDB write layer, and band
  registry integration are operational.
- **Heuristic path** (future): Runtime inference via the seven-layer architecture
  described below. Designed but not yet implemented.

## 9.1 Layer Architecture

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

## 9.2 Key Design Decisions Made

- **Row-level DynamoDB storage** (ADR-020) rather than canonical Parquet files
- **Dedicated photometry DynamoDB table** separate from the main NovaCat table (§6.2)
- **Band registry as a versioned data artifact** seeded from SVO FPS
- **Two-track `band_id` convention** (ADR-017 amendment): instrument-specific
  (`{Facility}_{Instrument}_{BandLabel}`) and Generic fallback (`Generic_{BandLabel}`)
- **`photometric_system` field dropped** system-wide (ADR-019); `band_id` makes it
  redundant
- **Row-level failure persistence** to S3 diagnostics for operator review
- **AI-assisted adapter registration** at development time only; no runtime dependency
- **Conservative photometric system defaults** and case-sensitive filter matching (ADR-016)

## 9.3 Design Chain Status

The foundational ADR chain is complete: ADR-017 (band registry) → ADR-018
(disambiguation) → ADR-019 (table model) → ADR-020 (storage format). The
ticket-driven path (DESIGN-004) provides the MVP implementation; the heuristic path
(Layer 0 → adapter → persistence) remains as the future fallback for files without
tickets.

Full design context: DESIGN-001, DESIGN-002, DESIGN-004.

---

# 10. Observability Model

Structured logs include:

- workflow_name
- execution_arn
- job_run_id
- state_name
- attempt_number
- correlation_id
- primary UUID(s)
- error_classification
- error_fingerprint

Metrics:
- Success / failure / quarantine counts
- Retry rate
- Provider health
- Latency

---

# 11. Architectural Invariants

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

---

# 12. Deferred / Non-MVP

- Global multi-nova sweeps
- Spatial indexing
- Full VO compliance enforcement
- Advanced photometry version diffing
- Provider auto-discovery scaling
- Heuristic ingestion path (Layers 0–4 runtime implementation)

---

# 13. Deployment Model

Nova Cat is deployed as two independent CDK stacks in the same AWS account and region:

## NovaCat (production)

The live stack. All production workflows, Lambdas, state machines, and the primary
DynamoDB tables (`NovaCat` + `NovaCatPhotometry`) live here. CloudFormation exports
are prefixed `NovaCat-`.

## NovaCatSmoke (smoke test)

An isolated parallel deployment used exclusively by the smoke test suite.
Identical to `NovaCat` in every functional respect — same Lambda code, same ASL,
same IAM grants — but with independently namespaced resources:

- Lambda functions: `nova-cat-smoke-*`
- State machines: `nova-cat-smoke-*`
- DynamoDB tables: `NovaCatSmoke`, `NovaCatSmokePhotometry`
- CloudFormation exports: `NovaCatSmoke-*`

The smoke stack uses `DESTROY` removal policy throughout. Its DynamoDB tables are
wiped between test runs, eliminating any risk of smoke tests touching production data.

Smoke tests resolve all stack outputs from `NovaCatSmoke` via CloudFormation exports.
If the smoke stack is not deployed, all smoke tests skip cleanly with a descriptive message.

Both stacks are deployed together via `./deploy.sh`. Individual stack targeting is
supported: `./deploy.sh NovaCat` or `./deploy.sh NovaCatSmoke`.

## Resource Counts

- **Lambda functions:** 15 (4 container-based for astropy/numpy: `archive_resolver`,
  `spectra_discoverer`, `spectra_validator`, `ticket_ingestor`)
- **Step Functions workflows:** 6 (`initialize_nova`, `ingest_new_nova`,
  `discover_spectra_products`, `acquire_and_validate_spectra`, `refresh_references`,
  `ingest_ticket`)
- **DynamoDB tables:** 2 per stack (main `NovaCat` + dedicated `NovaCatPhotometry`)
- **S3 buckets:** 2 (private data + public site)
- **SNS topics:** 1 (quarantine notifications)

---

# End of Snapshot
