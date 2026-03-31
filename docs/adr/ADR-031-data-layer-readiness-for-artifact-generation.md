# ADR-031: Data Layer Readiness for Artifact Generation

**Status:** Proposed
**Date:** 2026-03-31
**Author:** TF
**Supersedes:** —
**Superseded by:** —
**Amends:** ADR-012 (sparkline MVP status), ADR-013 (DPO responsibility boundary),
  ADR-014 (sparkline amendment notes, OQ-5 resolution), ADR-020 (Decision 7
  supersession)
**Relates to:**
- `DESIGN-003` §17.2 (Epic 1 — Data Backfill & WorkItem Integration) — authoritative
  design basis; this ADR is the implementation contract for that epic
- `DESIGN-002` §6, §7, §9 — forward-reference correction (DESIGN-005 renumbering)
- `ADR-005` — Reference Model and ADS Integration (refresh_references WorkItem addition)
- `ADR-012` — Visual Design System (sparkline MVP promotion)
- `ADR-013` — Visualization Design (DPO responsibility boundary amendment)
- `ADR-014` — Artifact Schemas (sparkline amendments, OQ-5 resolution)
- `ADR-020` — Photometry Storage Format (Decision 7 supersession)

---

## 1. Context

DESIGN-003 defines the artifact regeneration pipeline — a cron-driven sweep that reads
data from DynamoDB and S3, generates per-nova and catalog-level JSON/SVG/ZIP artifacts,
and publishes them to a public S3 bucket for CloudFront delivery. The pipeline's
generators make specific assumptions about what fields exist on DynamoDB items and what
derived files exist in S3. Those assumptions are not met by the current data layer.

DESIGN-003 §17.2 identifies six prerequisites (P-1 through P-6) that must be
satisfied before any generator can run, plus a WorkItem integration that wires change
detection into the existing ingestion workflows. During review, six additional items
(P-8 through P-13) were identified: documentation alignment gaps where the regeneration
pipeline's design decisions diverge from or extend earlier ADRs without cross-reference.

A seventh prerequisite (P-7: Finalize Lambda writeback extension) is listed in
DESIGN-003 §11.10 but is a design constraint on a new component built in Epic 2, not a
change to existing code. It is not included here — DESIGN-003 §4.5 and §11.10 are the
authoritative specification for that component.

This ADR consolidates twelve items into a single implementation contract. It records
three categories of decision:

1. **Schema evolution and backfill** (P-1 through P-6) — new fields on DynamoDB items,
   new derived files in S3, and the one-time backfill strategy for existing data.
2. **WorkItem integration** — a new cross-cutting concern wired into three existing
   ingestion workflows.
3. **Documentation alignment** (P-8 through P-13) — amendments and cross-references to
   earlier ADRs whose decisions are extended or superseded by DESIGN-003.

The schema evolution and WorkItem integration items are implementation prerequisites:
Epic 2 (Pipeline Infrastructure) and Epic 3 (Artifact Generators) cannot begin until
they are complete. The documentation alignment items are not implementation blockers but
are included here to ensure the ADR corpus remains navigable and internally consistent.

### 1.1 Why a Single ADR

The twelve items share a common cause (the artifact regeneration pipeline needs data
that does not yet exist) and a common implementation pattern (forward-write changes to
ingestion handlers + one-time backfill scripts for existing data). Splitting them across
multiple ADRs would scatter related decisions without adding clarity. Grouping them here
provides a single checklist for the implementor and a single document to reference when
asking "what changed in the data layer for artifact generation?"

---

## 2. Decisions

### Category 1 — Schema Evolution and Backfill

All schema additions in this category follow the same two-phase pattern:

- **Forward-write:** Update the relevant ingestion handler(s) to populate the new field
  at write time, so all future items carry the field from creation.
- **Backfill:** Run a one-time script against existing items to populate the field
  retroactively. Backfill scripts are operator tooling (not production services) and
  live in `tools/`.

Backfill scripts read from S3 (FITS headers) or DynamoDB (existing item data) and write
directly to DynamoDB via `update_item`. They are idempotent — safe to re-run if
interrupted — and emit structured logs for audit. They do not trigger ingestion
workflows or create WorkItems.

#### Decision 1 — Validate RA/DEC on ACTIVE Nova Items (P-1)

**What:** Verify that all ACTIVE Nova items have `ra_deg` and `dec_deg` populated.
Write a one-time audit script; manually fix or quarantine any items that fail.

**Why:** The `nova.json` generator (DESIGN-003 §5) emits coordinates as required
fields. A missing value would produce an invalid artifact or require the generator to
handle a nullable case that should not exist for ACTIVE novae.

**Implementation:**

- Write an audit script (`tools/audit_nova_coordinates.py`) that scans all Nova items
  with `status = ACTIVE` and reports any with missing or null `ra_deg`/`dec_deg`.
- Manually resolve failures: either populate from SIMBAD/TNS via `initialize_nova`
  re-run, or demote the item to `QUARANTINED` with reason code
  `MISSING_REQUIRED_COORDINATES`.
- Update `dynamodb-item-model.md` to document `ra_deg` and `dec_deg` as required (not
  optional) on ACTIVE items. Update `current-architecture.md` accordingly.

**Verification:** Audit script reports zero failures against production data.

#### Decision 2 — Add `instrument` and `telescope` to SPECTRA DataProduct Items (P-2)

**What:** Add `instrument` and `telescope` as first-class fields on SPECTRA DataProduct
items.

**Why:** The `spectra.json` generator (DESIGN-003 §7) emits these as per-spectrum
metadata fields. Currently they exist only in the FITS headers stored in S3; the
generator must not fall back to reading FITS headers at generation time (this would
require S3 access per spectrum and astropy as a dependency in the Fargate container).

**Implementation:**

- **Forward-write:** Update `spectra_validator` (in `acquire_and_validate_spectra`) and
  `ingest_ticket` (spectra branch) to extract `TELESCOP` and `INSTRUME` from FITS
  headers and write them to the DataProduct item at validation/persistence time.
- **Backfill:** One-time script reads each existing VALID SPECTRA DataProduct item,
  fetches the corresponding FITS file from S3, extracts the header keywords, and
  updates the DDB item. Script lives at `tools/backfill_spectra_metadata.py`.
- Both fields are `string | null`. Null indicates the FITS header keyword was absent.

**Verification:** Query all SPECTRA DataProduct items with `validation_status = VALID`;
assert zero items with missing `instrument` field (some null values are acceptable if
the FITS header genuinely lacks the keyword).

#### Decision 3 — Add `observation_date_mjd` to SPECTRA DataProduct Items (P-3)

**What:** Add `observation_date_mjd` as a first-class field on SPECTRA DataProduct
items.

**Why:** The `spectra.json` generator uses `observation_date_mjd` as the per-spectrum
epoch for waterfall plot ordering and days-since-outburst computation (DESIGN-003 §7.4).
Same rationale as Decision 2: the generator must not parse FITS headers at generation
time.

**Implementation:** Same forward-write and backfill pattern as Decision 2. The value is
derived from the FITS `DATE-OBS` (or `MJD-OBS` if present) header keyword, converted to
Modified Julian Date. The field is `float | null`. The backfill script
(`tools/backfill_spectra_metadata.py`) handles this field in the same pass as
`instrument` and `telescope` to avoid redundant S3 reads.

**Verification:** Same audit pattern as Decision 2.

#### Decision 4 — Web-Ready CSV Generation During Ingestion (P-4)

**What:** After a spectrum is validated, write a downsampled (≤2,000 points),
wavelength-in-nm CSV to
`derived/spectra/<nova_id>/<data_product_id>/web_ready.csv` in the private S3 bucket.

**Why:** The `spectra.json` generator reads pre-processed CSV files rather than raw
FITS (DESIGN-003 §7.2). This avoids requiring astropy in the Fargate container and
ensures consistent downsampling across all spectra.

**Implementation:**

- **Forward-write:** Add a post-validation step to `spectra_validator` and
  `ingest_ticket` (spectra branch). After the spectrum passes validation, read the
  validated FITS file, convert wavelengths to nanometres, downsample to ≤2,000 points
  (preserving spectral features via a density-aware algorithm, not naive stride), and
  write the two-column CSV (`wavelength_nm`, `flux`) to the derived path.
- **Backfill:** One-time script (`tools/backfill_web_ready_csv.py`) processes all
  existing VALID SPECTRA DataProduct items. For each, it reads the raw FITS from S3,
  applies the same conversion and downsampling, and writes the CSV. This script requires
  astropy and numpy (same dependencies as the existing spectra ingestion handlers).

**Verification:** For each VALID SPECTRA DataProduct item, assert that the corresponding
`web_ready.csv` exists in S3 and contains between 1 and 2,000 rows.

#### Decision 5 — Add `flux_unit` to SPECTRA DataProduct Items (P-5)

**What:** Add `flux_unit` as a field on SPECTRA DataProduct items, populated from the
FITS `BUNIT` header keyword.

**Why:** The `spectra.json` generator emits `flux_unit` as a per-spectrum metadata field
for tooltip display of original flux values (DESIGN-003 §7.4, ADR-014 spectrum record
schema). The value varies per spectrum (different archives use different flux
calibrations), so it must be stored per DataProduct item.

**Implementation:** Same forward-write and backfill pattern as Decisions 2–3. The field
is `string | null`. The backfill script handles this in the same pass as the other
FITS-header-derived fields.

**Verification:** Same audit pattern as Decision 2.

#### Decision 6 — Add `nova_type` to Nova DDB Items (P-6)

**What:** Add `nova_type` to the Nova DDB item. Initially populated as `null` for all
novae.

**Why:** The outburst MJD shared utility (DESIGN-003 §7.6) references `nova_type` to
determine whether a nova is recurrent (which affects the outburst date selection
strategy). The enrichment mechanism for populating non-null values is post-MVP (OQ-1 in
DESIGN-003), but the field must exist for the utility to reference it without
conditional field-existence checks.

**Implementation:**

- **Forward-write:** Update `initialize_nova` to write `nova_type: null` on new Nova
  items.
- **Backfill:** One-time script adds `nova_type: null` to all existing Nova items via
  `update_item`. This is a trivial write — no external data source is needed.
- The field is `string | null`. Allowed non-null values (when the enrichment mechanism
  is implemented) will be defined in a future ADR.
- Update `dynamodb-item-model.md` to document the field.

**Verification:** Query all Nova items; assert all have a `nova_type` attribute (value
may be null).

### Category 2 — WorkItem Integration

#### Decision 7 — Wire WorkItem Creation into Ingestion Workflows

**What:** Add WorkItem creation as a best-effort final step in three ingestion
workflows:

| Workflow | dirty_type |
|---|---|
| `ingest_ticket` (photometry branch) | `photometry` |
| `ingest_ticket` (spectra branch) | `spectra` |
| `acquire_and_validate_spectra` | `spectra` |
| `refresh_references` | `references` |

**Why:** WorkItems are the change-detection mechanism that tells the artifact
regeneration pipeline which novae have new data (DESIGN-003 §3). Without them, the
pipeline has nothing to sweep.

**Implementation:**

- Each workflow writes a WorkItem after scientific data is persisted but before
  `FinalizeJobRunSuccess`. The WorkItem key structure is defined in DESIGN-003 §3.1:
  `PK = WORKQUEUE`, `SK = <nova_id>#<dirty_type>#<job_run_id>`.
- The write is best-effort: a failed WorkItem write logs a warning but does not fail
  the ingestion. The data is already persisted, and a manual operator-triggered
  regeneration can recover.
- WorkItems carry a `ttl` attribute set to 30 days from `created_at` (DESIGN-003 §3.5).
- Add the WorkItem entity type to `dynamodb-item-model.md`.
- Add the `WORKQUEUE` partition to the DynamoDB access patterns documentation.

**Verification:** Integration tests for each workflow assert that a WorkItem exists in
DDB after a successful run.

### Category 3 — Documentation Alignment

These items correct cross-reference gaps and decision drift between DESIGN-003 and
earlier ADRs. They are not implementation blockers for Epic 1 but should be completed
before the relevant generators are built (Epic 3) to prevent implementors from working
against stale specifications.

Per ADR-030 Decision 1, the ADRs amended here (ADR-012, ADR-013, ADR-014, ADR-020)
record decisions that have not yet been exercised in deployed infrastructure. They are
eligible for direct amendment rather than annotation-only correction.

#### Decision 8 — Sparkline Amendment Notes for DESIGN-003 §9 (P-8)

**What:** Add an "ADR-014/ADR-013 Amendment Notes" subsection to DESIGN-003 §9
(sparkline generator), consistent with the amendment notes present in every other
generator section.

**Amendments to record:**

- The band selection algorithm (§9.3) expands beyond ADR-014's "V-band only"
  specification to include a ranked fallback that can select non-V optical bands when
  V-band data is absent or insufficient.
- The input pool (§9.2) draws from the consolidated optical regime including UV/NIR/MIR
  per §8.11, broadening ADR-013's "Optical band only" language.

**Implementation:** Add a §9.N "ADR-014 Amendment Notes" subsection to DESIGN-003.
Directly amend ADR-014's sparkline schema section to note the band selection expansion.
Directly amend ADR-013's sparkline specification to note the broadened input pool.

#### Decision 9 — ADR-014 OQ-5 Resolution Cross-Reference (P-9)

**What:** Flag that ADR-014 Open Question 5 (recurrent nova outburst selection) is
resolved by DESIGN-003 §7.6: recurrent novae always use the earliest-observation
fallback regardless of `discovery_date`.

**Implementation:** Add a note to DESIGN-003 §7.8 (amendment notes) recording the
resolution. Directly amend ADR-014 to annotate OQ-5 as resolved by DESIGN-003 §7.6.

#### Decision 10 — ADR-013 DPO Responsibility Boundary Amendment (P-10)

**What:** ADR-013 lists `days_since_outburst` (DPO) computation under "Frontend
computes (at render time)" for both spectra and photometry. DESIGN-003 §7.4 and §8.9
pre-compute it backend-side, and ADR-014 already carries it as a pre-computed artifact
field. The responsibility boundary tables in ADR-013 are stale.

**Implementation:** Directly amend ADR-013's backend/frontend responsibility boundary
tables to move DPO to "Backend pre-computes." Add a note referencing DESIGN-003 §7.4
and §8.9 as the authoritative specification.

#### Decision 11 — Sparkline Promotion from Post-MVP to MVP (P-11)

**What:** ADR-012 classifies the sparkline column in the catalog table as Post-MVP.
DESIGN-003 includes sparkline generation in Epic 3 (MVP scope), and `has_sparkline` is
part of the `catalog.json` schema (DESIGN-003 §11.5).

**Implementation:** Directly amend ADR-012's catalog table column specification to
reclassify the sparkline column from Post-MVP to MVP.

#### Decision 12 — ADR-020 Decision 7 Supersession (P-12)

**What:** ADR-020 Decision 7 specifies a standalone `generate_nova_bundle` Fargate task
with per-nova idempotency guards. DESIGN-003 replaces this with a unified Fargate task
where bundle generation is step 6 in the per-nova dependency chain (§4.4), coordinated
via WorkItem/RegenBatchPlan.

**Implementation:** Directly amend ADR-020 Decision 7 to note that it is superseded by
DESIGN-003 §4.4 and §10. The bundle generation logic specified in ADR-020 remains
valid; the execution model (standalone task vs. unified pipeline step) is what changes.

#### Decision 13 — DESIGN-002 Forward-Reference Correction (P-13)

**What:** DESIGN-002 §6, §7 (OQ-11, OQ-12), and §9 forward-reference "DESIGN-003" as
the future donation workflow document. The actual DESIGN-003 is the Artifact
Regeneration Pipeline — a different document entirely.

**Decision:** DESIGN-003 retains its current identifier (Artifact Regeneration
Pipeline). The donation workflow, when authored, will be DESIGN-005. DESIGN-004 is
already assigned (Source Profile Schema and Ticket-Driven Ingestion).

**Implementation:** Update all DESIGN-002 forward references from "DESIGN-003" to
"DESIGN-005" for the donation workflow. Specifically:

- §6 (donation workflow context section header and body references)
- §7 OQ-11 and OQ-12 (which reference DESIGN-003 as the donation design target)
- §9 relationship table (DESIGN-003 row → DESIGN-005)

No changes to DESIGN-003 itself. No changes to any other document that references
DESIGN-003 as the artifact regeneration pipeline.

**Verification:** `grep -r "DESIGN-003" docs/` returns no results referencing a donation
workflow.

---

## 3. Implementation Ordering

The twelve decisions have a natural partial order driven by two constraints: backfill
script efficiency (consolidating S3 reads) and downstream dependencies.

**Phase A — Schema additions and backfill (Decisions 1–6):**

Decisions 2, 3, 4, and 5 all require reading FITS headers from S3 for backfill. A
single backfill script should handle all four in one pass per spectrum to avoid
redundant S3 `GetObject` calls. Decision 1 (RA/DEC audit) and Decision 6
(`nova_type` addition) are independent and can proceed in parallel.

Recommended script consolidation:

- `tools/backfill_spectra_metadata.py` — Decisions 2, 3, 5 (FITS header fields on
  DataProduct items). One S3 read per spectrum; extracts `TELESCOP`, `INSTRUME`,
  `DATE-OBS`/`MJD-OBS`, and `BUNIT` in a single pass.
- `tools/backfill_web_ready_csv.py` — Decision 4 (web-ready CSV generation). Separate
  script because it writes to S3 (not DDB) and has heavier computational requirements
  (wavelength conversion, downsampling). Can run in parallel with the metadata backfill.
- `tools/audit_nova_coordinates.py` — Decision 1. Read-only audit.
- `tools/backfill_nova_type.py` — Decision 6. Trivial DDB-only write.

**Phase B — WorkItem integration (Decision 7):**

Depends on the forward-write changes from Phase A being merged (the same ingestion
handlers are being modified). WorkItem writes are added in the same PR as the
forward-write changes where practical, or in an immediately subsequent PR.

**Phase C — Documentation alignment (Decisions 8–13):**

No implementation dependency. Can proceed in parallel with Phases A and B, but should
be completed before Epic 3 (Artifact Generators) begins to prevent implementors from
working against stale specs. Decision 13 (DESIGN-002 forward-reference correction)
should be done first since it affects document identity.

---

## 4. Consequences

### 4.1 What Becomes Possible

- The artifact regeneration pipeline (Epics 2–4) can proceed with confidence that its
  data layer assumptions are met.
- All existing VALID spectra have the enriched metadata fields and web-ready CSVs
  required by the `spectra.json` generator.
- Ingestion workflows emit WorkItems, enabling the cron-driven sweep to detect changes
  without polling or timestamp comparison.
- The ADR corpus is internally consistent with DESIGN-003's design decisions.

### 4.2 What Becomes Harder

- Ingestion handlers grow slightly more complex: each successful write path now includes
  a best-effort WorkItem creation step.
- The spectra validation path gains a post-validation CSV generation step, adding ~1–2
  seconds of compute and one S3 `PutObject` per spectrum.

### 4.3 Risks and Mitigations

- **Backfill script failure mid-run.** All backfill scripts are idempotent — they can be
  re-run safely. Each script logs progress per item so the operator can identify where a
  failure occurred.
- **FITS header keyword missing.** Fields derived from FITS headers (`instrument`,
  `telescope`, `observation_date_mjd`, `flux_unit`) are nullable. A missing header
  keyword results in a null field value, not a backfill failure. The generator handles
  null values gracefully (DESIGN-003 §7.7).
- **WorkItem write failure.** WorkItem creation is best-effort. A missed WorkItem means
  the nova's artifacts are not regenerated until the next ingestion event or a manual
  operator trigger. This is acceptable at MVP scale.

### 4.4 Documents Amended

| Document | Amendment | Authority |
|---|---|---|
| `dynamodb-item-model.md` | Add `nova_type` to Nova item. Add WorkItem entity. Add `WORKQUEUE` partition. Document `ra_deg`/`dec_deg` as required on ACTIVE items. | Decisions 1, 6, 7 |
| `current-architecture.md` | Update Nova entity section for `ra_deg`/`dec_deg` required status. | Decision 1 |
| `dynamodb-access-patterns.md` | Add `WORKQUEUE` query pattern. | Decision 7 |
| `ADR-012` | Reclassify sparkline column from Post-MVP to MVP. | Decision 11 |
| `ADR-013` | Move DPO to "Backend pre-computes" in responsibility boundary tables. | Decision 10 |
| `ADR-014` | Add sparkline amendment notes (band selection, input pool). Annotate OQ-5 as resolved. | Decisions 8, 9 |
| `ADR-020` | Annotate Decision 7 as superseded by DESIGN-003 §4.4/§10. | Decision 12 |
| `DESIGN-002` | Update forward references from DESIGN-003 to DESIGN-005 (donation workflow). | Decision 13 |
| `DESIGN-003` | Add §9.N sparkline amendment notes. Add §7.8 OQ-5 resolution note. | Decisions 8, 9 |

---

## 5. Open Questions

None. All implementation details are resolved by DESIGN-003 or by the decisions above.
