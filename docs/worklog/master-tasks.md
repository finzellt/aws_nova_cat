# Nova Cat — Post-MVP Master Task List

_Generated: 2026-04-04 · Updated: 2026-04-09 (v7) · Living document_

---

## How to Read This Document

Each task has:
- **CC rating**: Claude Code suitability (🟢 autonomous, 🟡 needs some guidance, 🔴 needs discussion/human judgment)
- **Dep**: Dependencies on other tasks (by ID)
- **Status**: ⬜ Not started · 🔲 In progress · ✅ Done

---

# Open Tasks

---

## Bugs

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| B12 | **Sparkline left/right margins misaligned in catalog table.** CSS/table cell alignment issue. | 🟢 | — | ⬜ |
| BN2 | **Bundle metadata.json count vs README count mismatch.** Audit after BN1 fix — may self-resolve. | 🟡 | — | ⬜ |
| BN4 | **Empty sources.json spectra array.** Direct consequence of BN1 — audit whether it self-resolved after the BN1 fix. | 🟡 | — | ⬜ |
| B17 | **Fargate artifact generator logs not sortable.** Log format doesn't include a field that supports proper chronological sorting in CloudWatch. Fix the logger configuration so Fargate operations can be traced sequentially. | 🟡 | — | ⬜ |
| B18 | **Radio band registry test fixture missing radio entries.** All 17 `test_band_registry_radio.py` tests fail — resolver returns `None` for every lookup. Radio entries were likely dropped from the test fixture during a band registry rebuild. | 🟢 | — | ⬜ |

---

## Spectra

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| S6 | **Spectra quality audit.** Identify and triage all skipped/edge-trimmed spectra across the catalog. X-Shooter skips already confirmed — investigate remaining instruments. | 🟡 | — | ⬜ |
| S7 | **Multi-regime spectra display.** Display spectra from instruments covering different wavelength regimes (UV, optical, NIR) on separate panels or with regime indicators. | 🟡 | — | ⬜ |
| S13 | **Dichroic gap trace splitting.** Frontend: split merged-spectrum Plotly traces at dichroic gap boundaries so Plotly doesn't draw a connecting line across the gap (e.g., UVES blue/red join). | 🟢 | — | ⬜ |
| S14 | **Use all spectra from a given night via interpolation.** Currently not all spectra from a single night are stitched together. Add interpolation to incorporate all available wavelength regimes from the same observation epoch. | 🟡 | — | ⬜ |
| S15 | **Change spectral line feature units from Å to nm.** Update the line list and feature marker labels. Also remove the Å symbol from hydrogen feature labels. | 🟢 | — | ✅ |
| S16 | **STIS adapter for spectra.** Build a validation profile and adapter for HST/STIS spectral data products. | 🟡 | — | ⬜ |
| S17 | **"Spectral Visits" data column.** Add a column to the catalog table showing the number of unique observation nights with spectra (distinct from total spectrum count). | 🟢 | — | ✅ |

---

## Photometry

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| P4 | **Photometry color palette expansion.** The current palette runs out of distinct colors when many bands are present. Expand with more distinguishable hues. | 🟡 | — | ⬜ |

---

## Pipeline & Infrastructure

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| I2 | **Coordinate dedup ASL wiring.** Wire the coordinate-based dedup logic into the identity resolution step function so near-duplicate novae are caught automatically. | 🟡 | — | ⬜ |
| R4 | **Smooth pipeline (end-to-end).** Integration and polish pass — the "it all just works" capstone. Depends on most other work being done. | 🔴 | R5, most others | ⬜ |
| PI1 | **`refresh_references` payload reduction.** Hits the 256 KB SFn payload limit for novae with many references (e.g., IM Nor, recurrent novae). Needs a payload reduction refactor similar to the `discover_spectra_products` fix. | 🟡 | — | ⬜ |
| PI2 | **CloudWatch log group cleanup.** Stale/unrecognized log groups accumulating. Add cleanup tooling or a retention policy sweep. | 🟢 | — | ⬜ |

---

## Operator Tooling

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| R5 | **Unified reseed/redeploy/rebuild app (with GUI).** Consolidate the scattered operator scripts (reseed_work_items, set_nova_dates, batch_ingest, propose_filters, etc.) into a single Streamlit or similar app. Replaces R5a–R5d Streamlit console concept. | 🔴 | — | ⬜ |
| OT8 | **UVES spectra reduction pipeline.** Build our own reduction pipeline for raw UVES data, rather than relying solely on ESO Phase 3 reduced products. | 🔴 | — | ⬜ |

---

## Testing

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| T1 | **Frontend test suite.** Jest + React Testing Library. Catalog table, spectra viewer, light curve panel, nova page, alias display. | 🟡 | — | ⬜ |
| T2 | **Backend test coverage expansion.** Prioritize artifact generation pipeline integration tests. | 🟡 | — | ⬜ |

---

## Logging & Observability

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| L1 | **Enrich structured log context across all handlers.** Ensure `data_product_id`, `bibcode`, `provider`, `instrument` are appended whenever in scope. | 🟡 | — | ⬜ |

---

## Documentation & Design

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| D1 | **Update current-architecture.md.** Dated 2026-03-28. Reflects pre-Epic-5 state. | 🟡 | — | ⬜ |
| D2 | **Recurrent novae design.** Develop a plan/design for how to handle recurrent novae in the catalog — multiple outbursts, cross-outburst identity, epoch disambiguation, UI for multiple light curves per object. | 🔴 | — | ⬜ |

---

## Scoreboard

| Category | Count |
|----------|-------|
| ✅ Done | 77 |
| ⬜ Remaining (bugs) | 5 |
| ⬜ Remaining (features/tasks) | 17 |
| **Total open** | **22** |

---

## Suggested Priority Order

**Quick wins / unblock further ingestion:**
1. BN2/BN4 — Audit bundle fixes (may already be resolved)
2. B18 — Radio test fixture rebuild
3. B17 — Fargate logger sorting + blank rows

**Spectra quality & display:**
4. S13 — Dichroic gap trace splitting
5. S6 — Spectra quality audit
6. S14 — Same-night interpolation
7. S7 — Multi-regime display

**Pipeline robustness:**
8. PI1 — refresh_references payload reduction
9. PI2 — CloudWatch log group cleanup
10. B12 — Sparkline margins

**New capabilities:**
11. S16 — STIS adapter
12. P4 — Photometry color palette
13. I2 — Coordinate dedup ASL wiring

**Bigger arcs:**
14. R5 — Unified operator app
15. T1, T2 — Test suites
16. L1 — Log enrichment
17. D1 — Architecture doc update
18. D2 — Recurrent novae design
19. OT8 — UVES reduction pipeline
20. R4 — Smooth pipeline (capstone)

---

# Completed Tasks

_74 tasks completed across all workstreams._

| ID | Task |
|----|------|
| B1 | Release copy-forward for swept novae |
| B2 | Nova context counts for partial sweeps |
| B3 | Docker image rebuild verification |
| B4 | Web-ready CSV backfill |
| B5 | Sparkline rendering on catalog page |
| B6 | Bundle AccessDenied (S3 key mismatch) |
| B7 | Radio photometry rendering |
| B8 | Spectral line hover full y-range |
| B9 | Duplicate wavelength hover text |
| B10 | Truncated MJD values for radio observations |
| B11 | Month-precision date comparison |
| B13 | NaN sentinel in spectra.json |
| B14 | Zoom state lost on feature/scale toggle |
| B15 | Hover labels broken while zoomed |
| B16 | Spectra below x-axis (negative flux packing) |
| BN1 | Bundle generator hardcoded S3 path |
| S1 | LTTB downsampling / flat spectra fix |
| S2 | X-axis truncation |
| S3 | Flux floor for log scale |
| S4 | Multi-arm spectra merge (X-Shooter/UVES) |
| S4b | NaN sentinel fix for merged spectra |
| S4c | Merge MJD tolerance widening + overlap rejection |
| S5 | Spectral feature hover (scatter trace approach) |
| S8 | Wavelength/SNR enrichment pipeline |
| S9 | ADS collection filter (spurious references fix) |
| S10 | Same-day spectra deduplication |
| S11 | Collision-aware waterfall packing |
| S12 | Dense color palette fix |
| P1 | Radio ingestion diagnosis |
| P2 | Photometry observation count |
| P3 | Hover errors on light curve panel |
| P4a | Photometry warm/cool palette alternation (spectra) |
| P5 | Filter ordering |
| P6 | Radio flux density mapping |
| F1 | Homepage latest-release wiring |
| F2 | Catalog table sort/filter |
| F3 | Nova page alias display |
| F4 | Empty state placeholders |
| F5 | Observations table (rename + DataProduct fields) |
| I1 | Underscore normalization |
| R1 | Idempotency override |
| R2 | DDB check before initialize_nova |
| R3 | discover_spectra Standard workflow + fan-out stagger |
| R6 | ESO adapter timeout + retry |
| R7 | workflow_launcher timeout increase (30s → 300s) |
| R8 | SFn IAM role dependency fix (fresh stack race) |
| R9 | deploy.sh env var injection |
| L2 | Observation-list test unblocking (MagicMock pagination) |
| L3 | Test file relocation (module path conflicts) |
| L4 | Structured error context on failure paths |
| T3 | Identity resolution edge-case tests |
| T4 | Spectra merge test suite |
| OT1 | Reseed work items script |
| OT2 | CloudWatch log viewer |
| OT3 | Bundle diagnostic (root-caused BN1–BN4) |
| OT4 | CloudWatch log viewer Fargate support |
| OT5 | Pull dev artifacts script |
| OT6 | set_nova_dates operator tool |
| OT7 | batch_ingest try-parse subcommand |
| OT8a | propose_filters.py |
| OT9 | CloudWatch log tracker configurable time/records |
| S15 | Spectral line feature units Å → nm |
| S17 | Spectral visits catalog column |
| BN3 | Empty .bib on partial sweeps (references pre-population) |
