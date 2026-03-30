# DESIGN-003: Artifact Regeneration Pipeline

_Status: Drafting_
_Date: 2026-03-29_

---

## 1. Problem Statement

Nova Cat's backend and frontend are independently functional but disconnected. The
ingestion pipeline (spectra: operational; photometry: ticket-driven path partially
implemented) writes scientific data to DynamoDB and S3. The frontend (feature-complete
MVP) consumes pre-built static JSON artifacts conforming to the ADR-014 schemas. During
development, the frontend reads mock fixtures from `frontend/public/data/`. In
production, it expects to read from an S3 bucket via CloudFront.

Nothing exists between these two systems. Three gaps, identified in
`current-architecture.md` §8.7 and open since ADR-011, block the path from "data
ingested" to "data visible on the website":

**Gap 1 — Artifact generation pipeline.** No code exists to read internal DDB/S3 state
and produce the seven published artifacts defined in ADR-014: `catalog.json`,
`nova.json`, `references.json`, `spectra.json`, `photometry.json`, `sparkline.svg`, and
`bundle.zip`. Each artifact has distinct input sources, computation requirements (flux
normalization, subsampling, offset calculation, SVG rendering, ZIP assembly), and output
contracts. This is the largest gap by volume of work.

**Gap 2 — Publication gate.** No mechanism exists to determine *when* artifacts should be
regenerated or to trigger that regeneration. Ingestion workflows write data to DDB/S3 and
terminate — they have no awareness of downstream publication. The system needs a way to
flag stale artifacts and a trigger to initiate regeneration.

**Gap 3 — Hosting and delivery.** No infrastructure exists to serve published artifacts to
the production frontend. The Vercel-hosted Next.js application needs to resolve artifact
URLs against an S3-backed origin, with appropriate CORS headers, cache behavior, and a
path from "artifact written to S3" to "browser receives current data." This includes the
CloudFront distribution, origin access controls, and the Vercel environment configuration
that connects the app to the data layer.

This document designs the complete solution spanning all three gaps — the "middle end"
that connects the backend persistence layer to the frontend presentation layer. It is
scoped to MVP: single-operator, modest dataset (<1000 novae), no real-time freshness
requirements.

**Out of scope:** This document does not cover incremental or differential artifact
updates (full regeneration per nova on every sweep), multi-operator concurrency controls,
CDN-level A/B testing or blue/green artifact deployments, programmatic API access to
artifact data (deferred to post-MVP per ADR-011), or changes to the ADR-014 artifact
schemas themselves. The heuristic photometry ingestion path (Layers 0–4) is also out of
scope; the artifact generators are agnostic to which ingestion path produced the
underlying data.

---

## 2. Solution Overview

_Placeholder — to be filled in after the full design is complete._

---

## 3. Invalidation Model

### 3.1 Design Approach

The regeneration pipeline needs to answer two questions: *which novae have changed?* and
*what changed about them?* The answer to both determines which artifacts need to be
regenerated.

Nova Cat uses an **additive work item model** rather than boolean dirty flags on the Nova
entity. Each ingestion event writes a discrete `WorkItem` to the main DynamoDB table. The
coordinator (§4) consumes these items to build a per-nova regeneration manifest, then
deletes them on successful artifact generation.

This approach is consistent with Nova Cat's existing preference for explicit, item-per-
event operational records (JobRun, Attempt). It handles bulk ingestion sessions cleanly —
30 photometry tickets for one nova produce 30 WorkItems, the coordinator regenerates
once, and all 30 are consumed — without losing the audit trail of what changed and why.

### 3.2 WorkItem Entity

WorkItems are work orders for the regeneration pipeline, not nova domain state. They live
in a dedicated global partition, separate from per-nova partitions.

**Key structure:**

```
PK = "WORKQUEUE"
SK = "<nova_id>#<dirty_type>#<created_at>"
```

The sort key is ordered nova → dirty_type → timestamp. This means all items for a given
nova are contiguous, and within a nova, all items of the same dirty_type are contiguous.
The coordinator can derive the per-nova regeneration manifest directly from the sort key
structure without an additional grouping pass.

**Fields:**

| Field | Type | Description |
|---|---|---|
| `nova_id` | UUID string | Which nova needs regeneration |
| `dirty_type` | string | `spectra`, `photometry`, or `references` |
| `source_workflow` | string | Workflow that produced this item (e.g. `ingest_ticket`, `acquire_and_validate_spectra`, `refresh_references`) |
| `job_run_id` | UUID string | Audit trail back to the specific ingestion JobRun |
| `correlation_id` | string | Cross-workflow tracing identifier |
| `created_at` | string | ISO 8601 UTC timestamp |
| `ttl` | number | DynamoDB TTL attribute (Unix epoch seconds). Set to 30 days from `created_at`. |

`job_run_id` is the universal audit pointer. From any WorkItem, an operator can trace
back to the JobRun record to see exactly what was ingested, by which workflow, with full
operational context. This is consistent across all dirty types — unlike a
`data_product_id`, which only applies to spectra.

### 3.3 Who Writes WorkItems

Ingestion workflows write WorkItems as a final side effect before job finalization. The
write is best-effort — a failed WorkItem write should not fail the ingestion itself,
since the data is already persisted and a manual operator-triggered regeneration can
always recover.

| Workflow | Trigger | dirty_type |
|---|---|---|
| `ingest_ticket` (photometry branch) | Successful `IngestPhotometry` task | `photometry` |
| `ingest_ticket` (spectra branch) | Successful `IngestSpectra` task | `spectra` |
| `acquire_and_validate_spectra` | Successful validation (VALID outcome) | `spectra` |
| `refresh_references` | Successful reference upsert | `references` |
| `ingest_photometry` (heuristic path, future) | Successful photometry ingestion | `photometry` |

The write happens after the scientific data is persisted but before
`FinalizeJobRunSuccess`. This ensures a WorkItem is only created when actual data has
landed.

### 3.4 Dirty Type → Artifact Dependency Matrix

The coordinator uses the set of dirty types present for a given nova to determine which
artifacts to regenerate. The matrix:

| dirty_type | `spectra.json` | `photometry.json` | `sparkline.svg` | `references.json` | `nova.json` | `bundle.zip` | `catalog.json` |
|---|---|---|---|---|---|---|---|
| `spectra` | ✓ | | | | ✓ | ✓ | ✓ |
| `photometry` | | ✓ | ✓ | | ✓ | ✓ | ✓ |
| `references` | | | | ✓ | ✓ | ✓ | ✓ |

Three invariants govern the matrix:

- **`nova.json` regenerates on any change.** It carries metadata (spectra_count,
  photometry_count, references_count) that reflects the current state of all data
  products.
- **`bundle.zip` regenerates on any change.** The bundle is the complete research-grade
  data package for a nova — observational data files, photometry tables, and
  bibliographic references. Any change to the nova's data or metadata makes the bundle
  stale.
- **`catalog.json` regenerates on any change to any nova.** It is a global artifact that
  aggregates counts and metadata across all novae. It must run *after* all per-nova
  artifacts for the current sweep are complete.

### 3.5 WorkItem Lifecycle

1. **Created** by an ingestion workflow after data is persisted. A `ttl` attribute is set
   to 30 days from `created_at`.
2. **Read** by the coordinator during the cron sweep (§4). The coordinator queries
   `PK=WORKQUEUE` and derives the per-nova regeneration manifest from the sort keys.
3. **Deleted** after the corresponding nova's artifacts are successfully regenerated.
   Deletion is per-item — the coordinator deletes only the WorkItems that were present
   when it built the manifest, not any that arrived during execution.
4. **Retained on failure** — if artifact generation fails for a nova, its WorkItems
   remain in the queue and will be picked up by the next sweep. No signal is lost.
5. **TTL expiry** — if a WorkItem has not been consumed within 30 days, DynamoDB
   automatically deletes it. No data is lost (the underlying scientific data remains in
   DDB/S3), but the stale artifact will not be regenerated until the operator
   investigates and either resolves the underlying failure or triggers a manual rebuild.

### 3.6 Stuck WorkItem Mitigation

WorkItems that survive repeated sweep cycles indicate a persistent failure in artifact
generation for that nova — typically a generator bug or a corrupt data state rather than
a problem with the WorkItem itself.

The mitigation strategy has two layers:

**Early warning:** The coordinator logs a warning when it encounters any WorkItem older
than 7 days (configurable via Lambda environment variable
`WORKITEM_STALE_THRESHOLD_DAYS`). This gives the operator time to investigate while the
WorkItem is still active.

**Automatic cleanup:** The DynamoDB TTL attribute (set to 30 days at creation) provides a
hard ceiling. WorkItems that cannot be successfully consumed within the TTL window are
automatically removed. The underlying data is unaffected — the nova's DDB items and S3
files remain intact. The operator can create fresh WorkItems or trigger a full rebuild
once the root cause is resolved.

This approach avoids custom retry-counting logic in the coordinator, uses a DynamoDB
built-in for the cleanup mechanism, and degrades gracefully — the worst case is
"artifacts for one nova remain stale until the operator notices the warnings."

### 3.7 Future: Color Dirty Type

When color ingestion lands (ADR-022 / `ingest_color`), a `color` dirty type will be
added. Its artifact dependencies will be: `photometry.json` (colors are rendered in the
photometry panel), `nova.json`, `bundle.zip`, and `catalog.json`. The sparkline is not
affected — it renders magnitude vs. time only, not color indices. The WorkItem schema
requires no structural changes; `color` is simply a new `dirty_type` value.

---

## 4. Sweep Trigger and Coordinator

### 4.1 Trigger Mechanism

An **EventBridge scheduled rule** invokes the coordinator Lambda on a fixed cadence. The
default schedule is every 6 hours (configurable via CDK parameter). This cadence reflects
the operational reality that Nova Cat has no real-time freshness requirement — artifacts
that are a few hours stale are acceptable, and bulk ingestion sessions typically complete
well within one sweep interval.

The operator can also invoke the coordinator Lambda manually (via the AWS console, CLI, or
a future operator script) at any time. A manual invocation behaves identically to a
scheduled one — there is no distinction in the coordinator's logic.

### 4.2 Coordinator Lambda

The coordinator is a single Lambda function (`artifact_coordinator`) whose job is to read
the work queue, build a per-nova regeneration plan, and launch execution. It is a
planning and dispatch step, not an execution step — it does not generate artifacts itself.

**Input:** None (the EventBridge rule invokes with an empty or minimal event). The
coordinator reads all of its input from the `WORKQUEUE` partition.

**Execution steps:**

1. **Query the work queue.** Paginated Query against `PK=WORKQUEUE`. Returns all pending
   WorkItems across all novae.

2. **Check for stale plan.** If a `RegenBatchPlan` item exists in `PENDING` state from a
   previous sweep (§4.3), the coordinator abandons it — sets its status to `ABANDONED`
   and proceeds. The WorkItems backing that plan were never deleted (deletion happens
   only on success), so they are still in the queue and will be included in the new plan.
   This is the "abandon and rebuild" strategy: always create a fresh plan from current
   state.

3. **Build per-nova manifests.** Group WorkItems by `nova_id`. For each nova, derive the
   set of distinct `dirty_type` values present, then apply the dependency matrix (§3.4)
   to produce the list of artifacts that need regeneration. This is the **nova
   regeneration manifest** — a per-nova record of which artifacts to generate.

4. **Emit stale WorkItem warnings.** For any WorkItem older than
   `WORKITEM_STALE_THRESHOLD_DAYS` (default 7), log a structured warning with the
   `nova_id`, `dirty_type`, `created_at`, and `job_run_id`. This provides the operator
   early notice of stuck items before the TTL fires.

5. **Persist the batch plan.** Write a `RegenBatchPlan` item (§4.3) to DynamoDB with
   status `PENDING` and the full set of nova manifests. This makes the plan inspectable —
   the operator can query it to see exactly what the coordinator decided.

6. **Launch execution.** Start the `regenerate_artifacts` Step Functions workflow (§4.5),
   passing the batch plan ID. The coordinator's job is done — artifact generation is the
   workflow's responsibility.

If the work queue is empty (no WorkItems), the coordinator exits immediately with no
side effects. No batch plan is created, no workflow is launched.

### 4.3 RegenBatchPlan Item

The batch plan is a DynamoDB item that records the coordinator's decisions for
auditability and recovery.

**Key structure:**

```
PK = "REGEN_PLAN"
SK = "<created_at>#<plan_id>"
```

**Fields:**

| Field | Type | Description |
|---|---|---|
| `plan_id` | UUID string | Unique identifier for this batch plan |
| `status` | string | `PENDING`, `IN_PROGRESS`, `COMPLETED`, `FAILED`, `ABANDONED` |
| `nova_manifests` | map | Per-nova regeneration manifests: `{ nova_id: { dirty_types: [...], artifacts: [...] } }` |
| `nova_count` | number | Count of novae in this plan |
| `workitem_sks` | list of string | Sort keys of all WorkItems consumed by this plan (for deletion on success) |
| `created_at` | string | ISO 8601 UTC timestamp |
| `completed_at` | string or null | Set on terminal status |
| `execution_arn` | string or null | Step Functions execution ARN, set after launch |
| `ttl` | number | DynamoDB TTL, 7 days from creation |

The `workitem_sks` list is the coordinator's snapshot of which WorkItems existed at plan
creation time. This is critical for correct cleanup: only these items are deleted on
success, not any WorkItems that arrived while the workflow was executing.

The TTL on the plan itself is short (7 days) — plans are ephemeral operational records,
not long-term audit artifacts. The JobRun records from the regeneration workflow provide
the durable audit trail.

### 4.4 Execution Model: Single Fargate Task

Artifact generation is performed by a **single Fargate task** that processes the entire
batch plan sequentially. This is a deliberate architectural choice driven by three
concerns:

**Memory and runtime constraints.** Bundle generation (§10) requires reading potentially
gigabytes of FITS files and assembling large ZIP archives — a workload that exceeds
Lambda's 10 GB memory ceiling and 15-minute timeout for novae with extensive spectral
datasets.

**Cost efficiency.** One Fargate task cycling through 50 novae in sequence is dramatically
cheaper than 50 independent invocations. There is no cold-start overhead per nova, no
parallel billing, and the task can be right-sized to the actual workload (e.g., 2 vCPU /
8 GB for MVP).

**State continuity.** The Fargate task maintains execution state across novae — running
totals for `catalog.json` aggregation, a success/failure ledger for cleanup, and
in-memory data that can be reused across artifacts for the same nova (e.g., photometry
data loaded once, used for both `photometry.json` and the bundle).

The Fargate task receives the `plan_id`, loads the batch plan from DynamoDB, and
processes novae sequentially:

1. For each nova in the plan, generate all artifacts specified in its manifest, in
   dependency order: `references.json` → `spectra.json` → `photometry.json` →
   `sparkline.svg` → `nova.json` → `bundle.zip`. The bundle is generated last because
   it may include the freshly generated artifacts.
2. Write each artifact to S3 as it is produced (§12).
3. Track per-nova success/failure and accumulate catalog-level aggregates. For each
   successful nova, record the observation counts computed during artifact generation
   (`spectra_count`, `photometry_count`, `references_count`) in the per-nova result
   payload.
4. After all novae are processed, generate `catalog.json` from the accumulated
   aggregates.
5. Report results as task output: success count, failure count, and per-nova status
   including computed observation counts for successful novae.

A single nova's failure does not abort the batch. The task logs the failure, skips to the
next nova, and continues. This ensures that one corrupt nova doesn't block regeneration
for the entire catalog.

### 4.5 Workflow: regenerate_artifacts

A thin **Standard Step Functions workflow** wraps the Fargate task. It provides timeout
handling, failure capture, and execution history without adding orchestration complexity.

**States:**

1. **UpdatePlanInProgress** (Task → Lambda) — Sets the `RegenBatchPlan` status to
   `IN_PROGRESS` and records the `execution_arn`.
2. **RunArtifactGenerator** (Task → ECS RunTask `.sync`) — Launches the Fargate task and
   waits for completion. Standard Workflows can wait up to one year at zero cost (billed
   per state transition, not per wall-clock second). The `.sync` integration pattern
   means Step Functions polls ECS on your behalf until the task reaches a terminal state.
3. **Finalize** (Task → Lambda) — Performs the atomic commit sequence for each
   successfully regenerated nova. This is the point at which the system commits to the
   new artifacts — no state is mutated until artifacts have been successfully published
   to S3 by the Fargate task. The Finalize Lambda:
   - Reads the Fargate task's per-nova result payload (including computed observation
     counts).
   - For each nova that **succeeded**: deletes the consumed WorkItems (using the
     `workitem_sks` snapshot from the plan, filtered to the successful nova's items),
     and writes `spectra_count` and `photometry_count` to the Nova DDB item
     (`PK=<nova_id>`, `SK=NOVA`).
   - For each nova that **failed**: leaves its WorkItems in the queue for the next sweep.
     No counts are updated.
   - Updates the `RegenBatchPlan` status to `COMPLETED` (all novae succeeded) or
     `FAILED` (at least one nova failed).
4. **FailHandler** (Task → Lambda) — If the Fargate task itself fails (OOM, crash,
   timeout): updates the `RegenBatchPlan` status to `FAILED`. All WorkItems are
   retained — nothing is lost, and the next sweep will rebuild.

The observation counts written by Finalize serve a dual purpose: they are the
authoritative record on the Nova item and they are read during `catalog.json` generation
(§11), which runs at the end of the Fargate task before Finalize executes. Because
`catalog.json` is generated from the same in-memory counts that Finalize will
subsequently persist, the published catalog and the DDB state are consistent by
construction.

This gives the regeneration pipeline the same operational visibility as every other
workflow in the system — execution history in the SFn console, structured failure
capture, and a clear audit trail — while keeping the actual architecture minimal: four
state transitions per sweep.

### 4.6 Concurrency

Only one coordinator invocation should be active at a time. If the EventBridge rule fires
while a previous sweep's workflow is still executing, the coordinator will find a
`PENDING` or `IN_PROGRESS` batch plan. The handling:

- **`PENDING` plan** (coordinator crashed before launching workflow): abandon and rebuild,
  as described in §4.2.
- **`IN_PROGRESS` plan** (workflow still executing): the coordinator exits immediately
  with a log message. The in-flight workflow will complete and clean up its own
  WorkItems. Any new WorkItems that arrived since the plan was created will be picked up
  by the next sweep.

This avoids concurrent artifact generation for the same nova without requiring a
distributed lock — the batch plan status serves as the coordination mechanism.

---

## 5. Artifact Generation: nova.json

### 5.1 Purpose

`nova.json` is a per-nova metadata artifact powering the header region of the nova detail
page. It carries core object properties and observation counts. References are
intentionally excluded (delivered separately in `references.json` per ADR-014's minimal
redundancy principle) to allow independent generation and lazy loading.

### 5.2 Input Sources

The generator reads from a single DynamoDB table and requires no S3 access.

**Main table — Nova item:**

```
GetItem: PK = "<nova_id>", SK = "NOVA"
```

Fields consumed: `nova_id`, `primary_name`, `aliases`, `ra_deg`, `dec_deg`,
`discovery_date`, `status`.

The Nova item is the sole input. Observation counts (`spectra_count`,
`photometry_count`) are *not* read from DDB at generation time — they are computed as
byproducts of the `spectra.json` and `photometry.json` generators (§7, §8) during the
same Fargate run and passed to the `nova.json` generator in memory. See §5.4 for
details.

### 5.3 Computation

The `nova.json` generator performs three transformations. All other fields are direct
pass-throughs from the Nova item.

**Coordinate formatting.** `ra_deg` (float, ICRS decimal degrees) is converted to
sexagesimal `HH:MM:SS.ss` format. `dec_deg` (float, ICRS decimal degrees) is converted
to `±DD:MM:SS.s` format. The conversion uses `astropy.coordinates.SkyCoord` with
`to_string(style='hmsdms')` and appropriate precision parameters. RA/DEC are required
fields on ACTIVE Nova items (see §5.8, prerequisite P-1); the generator does not handle
a nullable coordinate case.

**Discovery date pass-through.** The `discovery_date` field on the Nova item is stored
as a string in `YYYY-MM-DD` format with the `00` convention for missing precision (per
ADR-005 Amendment), which is exactly the format ADR-014 specifies. This is a direct
pass-through. When `discovery_date` is `None` on the Nova item (no references resolved
yet), the artifact emits `null`.

**Nova type.** The Nova item does not currently carry a `nova_type` field. The
generator emits `null`. This is noted as a post-MVP enrichment task, likely paired with
discovery date refinement (see §16, Open Questions).

### 5.4 Observation Counts

`spectra_count` and `photometry_count` are not independently computed by the
`nova.json` generator. They are produced as side effects of the `spectra.json` generator
(§7) and the `photometry.json` generator (§8) respectively, during the same Fargate
execution.

The Fargate task processes per-nova artifacts in dependency order (§4.4):
`references.json` → `spectra.json` → `photometry.json` → `sparkline.svg` → `nova.json`
→ `bundle.zip`. By the time `nova.json` runs, both counts are available in the
in-process nova context.

These counts are the authoritative values for the published artifact. They reflect what
is actually packaged in the published data products — not a raw DDB query count. If a
FITS file is marked VALID in DDB but fails to process during generation, it is excluded
from the published artifact and the count reflects that exclusion. The three per-nova
data artifacts (`spectra.json`, `photometry.json`, `bundle.zip`) are consistent by
construction because they share a single Fargate execution context reading from the same
DDB state.

The counts are also written back to the Nova DDB item — but not by the Fargate task
itself. The Fargate task emits the counts in its per-nova result payload. The **Finalize
Lambda** (§4.5, state 3) writes them to the Nova item as part of the atomic commit
sequence: delete consumed WorkItems, update observation counts, mark the
`RegenBatchPlan` as completed. This ensures counts are only persisted after artifacts
have been successfully published to S3.

The counts written to the Nova item serve a second purpose: they are read during
`catalog.json` generation (§11). Because `catalog.json` is generated after all per-nova
artifacts in the sweep, the in-memory counts from the current run are used directly. The
Nova item writes by Finalize ensure the DDB state is consistent for subsequent sweeps
that may only regenerate a subset of novae.

**Spectra count definition:** The number of spectra successfully included in the
published `spectra.json` artifact for this nova. This is determined by the `spectra.json`
generator (§7), which reads `PRODUCT#SPECTRA#*` items with `validation_status == "VALID"`
and includes only those it can successfully process. The count reflects what is published,
not what exists in DDB.

**Photometry count definition:** The number of photometric observations successfully
included in the published `photometry.json` artifact for this nova. This is determined by
the `photometry.json` generator (§8), which reads `PHOT#*` items from the dedicated
photometry table and includes only those it can successfully process.

### 5.5 Output Mapping

| ADR-014 field | Source | Transformation |
|---|---|---|
| `schema_version` | Constant | `"1.0"` |
| `generated_at` | Runtime | ISO 8601 UTC timestamp at generation time (shared utility) |
| `nova_id` | Nova item `.nova_id` | Direct |
| `primary_name` | Nova item `.primary_name` | Direct |
| `aliases` | Nova item `.aliases` | Direct (list of strings) |
| `ra` | Nova item `.ra_deg` | Decimal degrees → `HH:MM:SS.ss` (shared utility) |
| `dec` | Nova item `.dec_deg` | Decimal degrees → `±DD:MM:SS.s` (shared utility) |
| `discovery_date` | Nova item `.discovery_date` | Direct pass-through; `None` → `null` |
| `nova_type` | Not persisted | `null` (post-MVP enrichment) |
| `spectra_count` | In-process context | Computed by `spectra.json` generator |
| `photometry_count` | In-process context | Computed by `photometry.json` generator |

### 5.6 Edge Cases and Error Handling

**Missing coordinates.** RA/DEC are required for all ACTIVE Nova items. A Nova item
lacking `ra_deg` or `dec_deg` indicates a data integrity issue upstream. The generator
logs an error, skips the nova, and records it as a failure in the Fargate task's
per-nova result ledger. The nova's WorkItems are retained for the next sweep.

**Missing discovery date.** `discovery_date` may legitimately be `None` — the
`refresh_references` workflow may not have run, or ADS may have returned no results.
The generator emits `null`. This is not an error condition.

**Nova status filter.** The generator only processes novae with `status == "ACTIVE"`.
Novae in QUARANTINED, MERGED, or DEPRECATED states are skipped. If a WorkItem exists
for a non-ACTIVE nova, the generator logs a warning and skips it; the WorkItem will
expire via TTL (§3.6).

**Empty aliases.** If `aliases` is an empty list or not present on the Nova item, the
generator emits an empty array `[]`.

### 5.7 ADR-014 Amendment Note

Two minor amendments to ADR-014 are required based on design decisions made in this
document:

1. **`discovery_date`** — change type from `string` to `string | null`. The `null`
   value indicates the discovery date has not been resolved.
2. **`nova_type`** — change type from `string` to `string | null`. The `null` value
   indicates nova classification has not been determined.

### 5.8 Prerequisites

**P-1: RA/DEC required on ACTIVE Nova items.** Verify that `initialize_nova` always
writes `ra_deg` and `dec_deg` before a Nova item reaches ACTIVE status. Update
`dynamodb-item-model.md` to document these fields as required (not optional) on ACTIVE
items. Update the Nova entity section in `current-architecture.md` accordingly.

---

## 6. Artifact Generation: references.json

### 6.1 Purpose

`references.json` is a per-nova file powering the references table on the nova detail
page. It is fetched independently of `nova.json` to allow the metadata region to render
before the references table is populated. This separation also allows the references
pipeline (`refresh_references`) to trigger regeneration of `references.json` without
touching `nova.json`.

### 6.2 Input Sources

The generator reads from the main DynamoDB table only. No S3 access is required.

**Main table — NovaReference link items:**

```
Query: PK = "<nova_id>", SK begins_with "NOVAREF#"
```

Returns all NovaReference items for the nova. Each item carries a `bibcode` field that
serves as the foreign key to the global Reference entity. No filtering by `role` is
applied — all linked references are included regardless of role (DISCOVERY,
SPECTRA_SOURCE, PHOTOMETRY_SOURCE, OTHER). At MVP scale, per-nova reference lists are
modest (tens of items) and a researcher visiting the nova page benefits from the
complete bibliographic picture.

**Main table — Reference global items (batch fetch):**

```
BatchGetItem: PK = "REFERENCE#<bibcode>", SK = "METADATA"
    (for each bibcode returned by the NovaReference query)
```

Fields consumed from each Reference item: `bibcode`, `title`, `authors`, `year`,
`doi`, `arxiv_id`.

`BatchGetItem` is used rather than individual `GetItem` calls because the bibcode list
is known upfront from the NovaReference query. DynamoDB's `BatchGetItem` accepts up to
100 keys per call; for novae with more than 100 references (unlikely at MVP scale), the
generator pages through multiple batch requests.

### 6.3 Computation

The generator performs two transformations. All other fields are direct pass-throughs
from the Reference items.

**ADS URL derivation.** The `ads_url` field is not stored on the Reference DDB item —
it is always derivable as `https://ui.adsabs.harvard.edu/abs/<bibcode>`. The generator
constructs this URL from the bibcode. This is consistent with the design decision in
`dynamodb-item-model.md` §6 to avoid storing derivable URLs.

**Sort order.** The output `references` array is sorted chronologically: ascending by
`year`, with lexicographically smallest `bibcode` as tiebreaker for references
published in the same year. This gives a natural reading order for a scientific
audience — earliest publications first, matching the convention in most astronomical
review papers.

### 6.4 Output Mapping

| ADR-014 field | Source | Transformation |
|---|---|---|
| `schema_version` | Constant | `"1.0"` |
| `generated_at` | Runtime | ISO 8601 UTC timestamp at generation time (shared utility) |
| `nova_id` | Nova context | Direct (passed from the Fargate per-nova loop) |
| `references[].bibcode` | Reference item `.bibcode` | Direct |
| `references[].title` | Reference item `.title` | Direct |
| `references[].authors` | Reference item `.authors` | Direct (list of strings) |
| `references[].year` | Reference item `.year` | Direct |
| `references[].doi` | Reference item `.doi` | Direct; `null` if absent |
| `references[].arxiv_id` | Reference item `.arxiv_id` | Direct; `null` if absent |
| `references[].ads_url` | Reference item `.bibcode` | Derived: `https://ui.adsabs.harvard.edu/abs/<bibcode>` |

### 6.5 Edge Cases and Error Handling

**No references.** If the NovaReference query returns zero items, the generator emits a
valid `references.json` with an empty `references` array `[]`. This is not an error —
a nova may be ingested before `refresh_references` has run. The nova page renders a
graceful empty state for the references table.

**Orphaned NovaReference.** If a NovaReference item references a bibcode for which no
`REFERENCE#<bibcode>` global item exists (a `BatchGetItem` miss), this indicates a data
integrity issue — the link was created but the reference entity was not. The generator
logs a warning with the `nova_id` and orphaned `bibcode`, and omits that reference from
the output array. It does not fail the nova.

**Missing optional fields on Reference items.** The `title`, `authors`, `doi`, and
`arxiv_id` fields on Reference items may be absent or null (e.g., an ADS entry with
incomplete metadata). The generator passes these through as-is: `null` for missing
string fields, `[]` for missing author lists. The `year` field is expected to always be
present on a well-formed Reference item; if it is missing, the generator logs a warning
and sorts the reference to the end of the array (treated as year `9999` for sort
purposes).

**References count for catalog.json.** The count of references for this nova
(`references_count` in `catalog.json`) is the length of the output `references` array
— i.e., the number of references that were successfully included in the published
artifact, excluding orphaned entries. This count is passed forward in the per-nova
context for use by `catalog.json` generation (§11), following the same "count what we
publish" principle established for spectra and photometry counts in §5.4.

---

## 7. Artifact Generation: spectra.json

### 7.1 Purpose

`spectra.json` is a per-nova file consumed exclusively by the spectra viewer component.
It carries all data required to render the waterfall plot as defined in ADR-013, with no
computation deferred to the frontend. This is the most data-intensive per-nova JSON
artifact — each spectrum record carries parallel wavelength and normalized flux arrays.

### 7.2 Input Sources

The generator reads from two sources: DynamoDB for metadata and S3 for spectral data.

**Main table — Spectra DataProduct items:**
```
Query: PK = "<nova_id>", SK begins_with "PRODUCT#SPECTRA#"
    FilterExpression: validation_status = "VALID"
```

Returns all validated spectra DataProduct items for the nova. Each item provides:
`data_product_id`, `provider`, `instrument`, `telescope`, `observation_date_mjd`.

The `instrument` and `telescope` fields are prerequisites added by the DataProduct field
enrichment (see §7.9, prerequisite P-2). `observation_date_mjd` is likewise a
prerequisite (P-3). These fields must be present on the DataProduct item; the generator
does not fall back to reading FITS headers.

**S3 — Web-ready CSV files (private bucket):**
```
derived/spectra/<nova_id>/<data_product_id>/web_ready.csv
```

One file per validated spectrum. Each CSV contains two columns: `wavelength_nm` and
`flux`. Wavelengths are pre-converted to nanometres and the array is pre-downsampled
to ≤2,000 data points. These files are written by the ingestion pipeline at validation
time (see §7.9, prerequisite P-4) and are not modified by the artifact generator.

**Per-nova context — Outburst MJD:**

The `outburst_mjd` value and `outburst_mjd_is_estimated` flag are computed by the
Fargate per-nova loop (shared utility, §7.6) and passed to the generator. The
generator does not compute these values itself.

### 7.3 Computation

The generator performs four operations per spectrum and one top-level computation.

**Flux normalization.** Each spectrum's flux array is divided by its peak flux value.
The peak is the maximum absolute value in the flux array. The result is a normalized
array where the tallest feature reaches 1.0 (or -1.0 for absorption-dominated
spectra, though this is rare for novae). The peak value is recorded as
`normalization_scale` in the output, enabling the frontend to reconstruct original
flux values for tooltip display.

Both peak and median normalization implementations are maintained in the generator
codebase, selectable via a configuration constant. Peak is the default for MVP, chosen
because it guarantees no spectrum feature exceeds the waterfall lane boundary. See
ADR-013 (Flux Normalization) for rationale. This is noted as an ADR-014 amendment:
`normalization_scale` is the peak flux value, correcting ADR-014's original
description of "median."

**Wavelength range extraction.** `wavelength_min` and `wavelength_max` are read
directly from the first and last elements of the wavelength array in the web-ready CSV
(which is monotonically ordered by wavelength).

**Days since outburst.** For each spectrum: `days_since_outburst = observation_date_mjd
- outburst_mjd`. When `outburst_mjd` is `null` (no discovery date and no observations
— should not occur for a nova with spectra, but handled defensively),
`days_since_outburst` is `null`.

**Flux unit extraction.** The original flux unit (prior to normalization) is needed for
tooltip display. This value is not currently stored on the DataProduct item or in the
web-ready CSV. Two options: (a) add `flux_unit` to the DataProduct item as part of the
field enrichment prerequisite, or (b) read it from the raw FITS header. Option (a) is
preferred for consistency with the other metadata fields. Added to the prerequisite
list as P-5.

### 7.4 Output Mapping

**Top-level fields:**

| ADR-014 field | Source | Transformation |
|---|---|---|
| `schema_version` | Constant | `"1.1"` (reflects `outburst_mjd_is_estimated` addition) |
| `generated_at` | Runtime | ISO 8601 UTC timestamp (shared utility) |
| `nova_id` | Nova context | Direct |
| `outburst_mjd` | Per-nova context | Shared utility output; `null` if unresolved |
| `outburst_mjd_is_estimated` | Per-nova context | Shared utility output; `true` when derived from earliest observation |
| `wavelength_unit` | Constant | `"nm"` (per ADR-013) |
| `spectra` | Generated | Array of spectrum records, ordered by `epoch_mjd` ascending |

**Per-spectrum fields:**

| ADR-014 field | Source | Transformation |
|---|---|---|
| `spectrum_id` | DataProduct `.data_product_id` | Direct (ADR-014 amendment: `spectrum_id` is `data_product_id`) |
| `epoch_mjd` | DataProduct `.observation_date_mjd` | Direct |
| `days_since_outburst` | Computed | `observation_date_mjd - outburst_mjd`; `null` if `outburst_mjd` is `null` |
| `instrument` | DataProduct `.instrument` | Direct; `"unknown"` if absent |
| `telescope` | DataProduct `.telescope` | Direct; `"unknown"` if absent |
| `provider` | DataProduct `.provider` | Direct |
| `wavelength_min` | Web-ready CSV | First element of wavelength array |
| `wavelength_max` | Web-ready CSV | Last element of wavelength array |
| `flux_unit` | DataProduct `.flux_unit` | Direct (prerequisite P-5) |
| `normalization_scale` | Computed | Peak flux value from the raw (pre-normalized) flux array |
| `wavelengths` | Web-ready CSV | Wavelength array in nm; passed through directly |
| `flux_normalized` | Computed | Flux array divided by peak flux value |

The `spectra` array is sorted by `epoch_mjd` ascending (oldest first), matching the
waterfall plot convention where the oldest spectrum appears at the bottom.

### 7.5 Spectra Count

The spectra count for this nova is the number of spectrum records in the output
`spectra` array — i.e., the number of validated DataProduct items for which a web-ready
CSV was successfully read and processed. If a DataProduct item is VALID in DDB but its
web-ready CSV is missing or unreadable, that spectrum is excluded from the artifact and
the count reflects the exclusion.

This count is passed forward in the per-nova context for use by `nova.json` (§5.4) and
`catalog.json` (§11).

### 7.6 Shared Utility: Outburst MJD Resolution

This computation is performed once per nova in the Fargate per-nova loop, before any
generator runs. The result is passed to `spectra.json`, `photometry.json`, and
`sparkline.svg` generators.

**Primary source — discovery date:**

1. Read `discovery_date` from the Nova DDB item.
2. If non-null, parse the `YYYY-MM-DD` string. Handle imprecise dates:
   - Day component is `00` → default to the 1st of the month.
   - Month and day components are both `00` → default to January 1st.
3. Convert the resolved date to MJD using `astropy.time.Time`.
4. Set `outburst_mjd_is_estimated = false`.

**Fallback — earliest observation:**

1. If `discovery_date` is `null`, query all SPECTRA DataProduct items
   (`validation_status = "VALID"`) and all PhotometryRow items (`PHOT#*` in the
   dedicated photometry table) for the nova.
2. Take `min(observation_date_mjd)` across both sets.
3. Subtract 1 day: `outburst_mjd = min_epoch - 1.0`. This places the estimated
   outburst one day before the earliest observation, so the earliest observation
   becomes approximately Day 1 on DPO axes. This avoids Day 0 (which breaks log
   scales) while keeping the estimate conservative.
4. Set `outburst_mjd_is_estimated = true`.

**Edge case — recurrent nova.** If `nova_type == "recurrent"` on the Nova item, the
generator always uses the earliest-observation fallback regardless of whether
`discovery_date` is present, and always sets `outburst_mjd_is_estimated = true`.
The `discovery_date` for a recurrent nova typically refers to the earliest known
outburst (potentially centuries ago), which is not a meaningful reference for DPO
computation. Full outburst segmentation for recurrent novae is deferred to a
dedicated post-MVP ADR (see §16, Open Questions).

**Edge case — no observations at all.** If both `discovery_date` is `null` and no
observations exist (no spectra, no photometry), `outburst_mjd` is `null` and
`outburst_mjd_is_estimated` is `false`. This should not occur in practice for a nova
with a WorkItem in the queue — the WorkItem implies data was ingested — but is handled
defensively.

### 7.7 Edge Cases and Error Handling

**Missing web-ready CSV.** If a DataProduct item is VALID but no web-ready CSV exists
at the expected S3 key, this indicates either a gap in the ingestion pipeline (the
prerequisite P-4 web-ready CSV step was not implemented when this spectrum was ingested)
or an S3 deletion. The generator logs a warning with the `nova_id` and
`data_product_id`, excludes the spectrum from the artifact, and continues. The spectrum
does not count toward `spectra_count`. For pre-existing spectra ingested before the
web-ready CSV step is implemented, a backfill script will be needed.

**Empty flux array.** If the web-ready CSV contains zero data rows, the generator logs
a warning and excludes the spectrum. This should not occur — the ingestion pipeline
validates that spectra contain data before marking them VALID — but is handled
defensively.

**Zero or negative peak flux.** If the peak flux value is zero or negative (indicating
a corrupt or physically nonsensical spectrum), normalization cannot be performed. The
generator logs a warning, excludes the spectrum, and continues.

**No valid spectra.** If all spectra for a nova fail processing (missing CSVs, corrupt
data), the generator emits a valid `spectra.json` with an empty `spectra` array `[]`
and a `spectra_count` of 0. This is not a nova-level failure — the nova may still have
photometry and references.

**Artifact size.** At ≤2,000 data points per spectrum and ~20 spectra per typical nova,
the expected artifact size is approximately 640 KB — within comfortable bounds for
CloudFront delivery and browser parsing. Novae with unusually large spectral collections
(>50 spectra) may produce artifacts exceeding 1 MB but remain within acceptable limits
for a lazy-loaded per-nova file.

### 7.8 ADR-014 Amendment Notes

1. **`spectrum_id`** — Clarification: `spectrum_id` is the `data_product_id` from the
   spectra DataProduct item. No separate ID is minted.
2. **`normalization_scale`** — Correction: the value is the peak flux (not median) used
   for normalization. ADR-013's original "peak" language is authoritative; ADR-014's
   "median" description was an error.
3. **`outburst_mjd_is_estimated`** — New boolean field at the top level. `true` when
   `outburst_mjd` was derived from the earliest observation rather than from a
   literature discovery date. Schema version incremented from `"1.0"` to `"1.1"`.

### 7.9 Prerequisites

**P-2: `instrument` and `telescope` on SPECTRA DataProduct items.** Add as first-class
fields, populated at validation/write time by both ingestion paths. Backfill existing
items from FITS headers.

**P-3: `observation_date_mjd` on SPECTRA DataProduct items.** Add as a first-class
field, populated at validation/write time. Backfill existing items from FITS headers.

**P-4: Web-ready CSV generation during ingestion.** After a spectrum is validated, the
ingestion pipeline writes a downsampled (≤2,000 points), unit-converted (wavelengths in
nm) CSV to `derived/spectra/<nova_id>/<data_product_id>/web_ready.csv` in the private
S3 bucket. Backfill existing validated spectra from raw FITS files.

**P-5: `flux_unit` on SPECTRA DataProduct items.** Add as a field carrying the original
flux unit string (e.g., `"erg/cm2/s/A"`). Populated at validation/write time from the
FITS header `BUNIT` keyword. Backfill existing items.

---

## 8. Artifact Generation: photometry.json

_To be designed._

---

## 9. Artifact Generation: sparkline.svg

_To be designed._

---

## 10. Artifact Generation: bundle.zip

_To be designed._

---

## 11. Artifact Generation: catalog.json

_To be designed._

---

## 12. Publication to S3

_To be designed._

---

## 13. Delivery: CloudFront

_To be designed._

---

## 14. Delivery: Vercel ↔ S3 Integration

_To be designed._

---

## 15. Operational Model

_To be designed._

---

## 16. Open Questions

| # | Question | Source | Blocking? |
|---|---|---|---|
| OQ-1 | **`nova_type` enrichment.** The Nova DDB item does not currently carry a `nova_type` field, and the `initialize_nova` workflow no longer classifies novae as classical vs. recurrent. A post-MVP enrichment mechanism is needed — likely paired with discovery date refinement. Until then, `nova_type` is `null` in published artifacts. | §5.3 | No (nullable for MVP) |
| OQ-2 | **Outburst MJD resolution strategy.** Multiple generators (`spectra.json`, `photometry.json`) require an `outburst_mjd` value. The proposed approach: use `discovery_date` when available (converted to MJD), fall back to earliest observation + 1 day when not, and carry an `outburst_mjd_is_estimated` boolean flag in the artifact schema. The "+1 day" convention avoids day-zero issues on log-scaled temporal axes. The flag enables a frontend warning so users are not misled. This needs to be finalized before §7/§8 are drafted. | §5 design discussion | Blocks §7, §8 |
| OQ-3 | **`discovery_date` → MJD conversion for imprecise dates.** When `discovery_date` has `00` for the day component (month precision only), what MJD value is produced? Proposed: default to the 1st of the month. When both month and day are `00` (year precision only), default to January 1st. Document the imprecision in the `outburst_mjd_is_estimated` flag. | §5 design discussion | Blocks §7, §8 |
| OQ-4 | **References in the bundle.** Whether to include a `references.bib` or equivalent file in the downloadable bundle. ADR-014 Open Question 1. | ADR-014 | Blocks §10 |

---

## 17. Work Decomposition

_To be written after design is complete._
