# Nova Cat — Current Architecture Snapshot

_Last Updated: 2026-03-12_

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

`data_product_id` is a stable UUID minted during `discover_spectra_products` via deterministic
derivation — `UUID(hash(provider + provider_product_key))`, falling back to
`UUID(hash(provider + normalized_canonical_locator))` when no provider-native ID exists.
Immutable once assigned. See ADR-003 for full specification.

---

## 3.3 Photometry Table

One logical photometry table exists per nova.

The photometry table is modeled as a `DataProduct` of type `PHOTOMETRY_TABLE` and is stored at a stable canonical S3 key.

### Canonical Behavior (MVP)

- The photometry table is rebuilt and overwritten **in place** on each ingestion.
- There is exactly one authoritative current table per nova.
- No snapshotting occurs during routine ingestion under the same schema version.

### Schema Versioning Policy

Photometry versioning is triggered **only when the photometry schema version changes**.

When a schema change occurs:

1. The existing canonical table is copied to an immutable snapshot location.
2. A new canonical table is written using the new schema version.

Snapshots are therefore:
- Schema-boundary artifacts
- Immutable
- Not created during normal ingestion

In MVP:
- `photometry_schema_version` is fixed.
- Schema migration workflows are documented but may not yet be implemented.

DynamoDB stores:
- The canonical S3 key
- The current `photometry_schema_version`
- Ingestion summary metadata

---

## 3.4 Reference

Reference is a global entity - one item per unique ADS-sourced bibliographic work,
shared across all novas that cite it. It is not scoped to a nova partition.

The ADS bibcode is both the stable canonical identifier and the DDB partition key
(REFERENCE#<bibcode>). No internal UUID is assigned to Reference items.
Lookup is a direct GetItem. No secondary index required.

Fields: bibcode (required; partition key), reference_type, title, year, authors,
doi (optional), arxiv_id (optional, bare ID), provenance.

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

## 3.5 Operational Records

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

# 5. Validation Architecture

Spectra validation is profile-driven:

Layered model:

1. Provider Adapter (discovery + acquisition)
2. FITS Profile (header normalization rules)
3. Generic Validator (IVOA-aligned canonicalization)

Unknown profile or missing critical metadata → QUARANTINE.

IVOA Spectrum DM / ObsCore conventions used where possible.

---

# 6. Persistence Model

Single DynamoDB table:

## Per-nova partition

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

## Global identity partitions
- NAME#<normalized_name>
- LOCATOR#<provider>#<locator_identity>
- REFERENCE#<bibcode>
- WORKFLOW#<correlation_id> — pre-nova workflow artifacts (e.g. FileObjects written
  during `initialize_nova` quarantine before a `nova_id` exists)

S3 layout:
- raw/
- derived/
- quarantine/
- bundles/
- site/releases/

Photometry canonical key:
- derived/photometry/<nova_id>/photometry_table.parquet

Photometry snapshots (schema change only):
- derived/photometry/<nova_id>/snapshots/schema=<old_schema_version>/...

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

# 9. Photometry Pipeline (Design Phase)

The photometry ingestion pipeline is in active design. The spectra pipeline (§5) is
operational; the photometry pipeline follows the same architectural patterns but
introduces additional complexity from multi-regime, multi-band data.

## 9.1 Layer Architecture

The photometry system is organized into seven layers, each a coherent unit of design
and implementation. Layers 0–2 are foundational data-model layers; Layers 3–6 build
on them.

| Layer | Name | Status |
|-------|------|--------|
| UnpackSource | Source file unpacking | Designed (ADR-021) |
| 0 | Pre-ingestion normalization | Designed (ADR-021) |
| 1 | Band registry | In design (ADR-017) |
| 2 | Band resolution/disambiguation | Pending (ADR-018) |
| 3 | Photometry table model revision | Pending (ADR-019) |
| 4 | Column mapping and adapter | Contracts merged; implementation paused |
| 5 | Ingestion workflow handlers | Pending |
| 6 | Persistence and query | Designed (ADR-020) |

## 9.2 Key Design Decisions Made

- **Row-level DynamoDB storage** (ADR-020) rather than canonical Parquet files
- **Separate photometry and color tables** with independent schema versioning
- **Band registry as a versioned data artifact** seeded from SVO FPS
- **Row-level failure persistence** to S3 diagnostics for operator review
- **AI-assisted adapter registration** at development time only; no runtime dependency
- **Conservative photometric system defaults** and case-sensitive filter matching (ADR-016)

## 9.3 Active Design Chain

ADR-017 (band registry) → ADR-018 (disambiguation) → ADR-019 (table model) → Epic D
(implementation). The `CanonicalCsvAdapter` implementation is paused pending completion
of this chain.

Full design context: DESIGN-001, DESIGN-002.

---

# 9. Observability Model

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

# 10. Architectural Invariants

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

---

# 11. Deferred / Non-MVP

- Global multi-nova sweeps
- Spatial indexing
- Full VO compliance enforcement
- Advanced photometry version diffing
- Provider auto-discovery scaling

---

# 12. Deployment Model

Nova Cat is deployed as two independent CDK stacks in the same AWS account and region:

## NovaCat (production)

The live stack. All production workflows, Lambdas, state machines, and the primary
DynamoDB table (`NovaCat`) live here. CloudFormation exports are prefixed `NovaCat-`.

## NovaCatSmoke (smoke test)

An isolated parallel deployment used exclusively by the smoke test suite.
Identical to `NovaCat` in every functional respect — same Lambda code, same ASL,
same IAM grants — but with independently namespaced resources:

- Lambda functions: `nova-cat-smoke-*`
- State machines: `nova-cat-smoke-*`
- DynamoDB table: `NovaCatSmoke`
- CloudFormation exports: `NovaCatSmoke-*`

The smoke stack uses `DESTROY` removal policy throughout. Its DynamoDB table is
wiped between test runs, eliminating any risk of smoke tests touching production data.

Smoke tests resolve all stack outputs from `NovaCatSmoke` via CloudFormation exports.
If the smoke stack is not deployed, all smoke tests skip cleanly with a descriptive message.

Both stacks are deployed together via `./deploy.sh`. Individual stack targeting is
supported: `./deploy.sh NovaCat` or `./deploy.sh NovaCatSmoke`.

---

# End of Snapshot
