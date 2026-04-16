# Nova Cat — Post-MVP Master Task List

_Generated: 2026-04-04 · Updated: 2026-04-14 (v8) · Living document_

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
| B17 | **Fargate logger sorting, blank rows, and structured formatting.** Log format doesn't support chronological sorting in CloudWatch. Also ensure Fargate logs are structured correctly so they can be parsed by the reader app and Logs Insights. | 🟡 | — | ⬜ |
| B18 | **Radio band registry test fixture missing radio entries.** All 17 `test_band_registry_radio.py` tests fail — resolver returns `None` for every lookup. Radio entries were likely dropped from the test fixture during a band registry rebuild. | 🟢 | — | ⬜ |
| B19 | **Hard floor for spectra y-axis.** When plotting individual spectra, large chunks get cut off — not just the bottom spectrum. Need hard controls on the minimum y-value. Includes log-scale variant. Part of the long-range spectra.py rebuild (R10). | 🟡 | — | ⬜ |

---

## Spectra — Display

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| S6 | **Spectra quality audit.** Identify and triage all skipped/edge-trimmed spectra across the catalog. X-Shooter skips already confirmed — investigate remaining instruments. | 🟡 | — | ⬜ |
| S18 | **sqrt(flux) scaling option.** Add a square-root flux scaling toggle when viewing an individual spectrum. | 🟢 | — | ⬜ |
| S19 | **X-ray x-axis units (keV).** The spectra viewer x-axis needs to support energy (keV) for X-ray data, rather than wavelength (nm). | 🟡 | — | ⬜ |
| S20 | **Spectral features for X-ray, UV, and NIR.** Extend the spectral feature marker system beyond the current optical-only set (Fe II, He/N, Nebular). | 🟡 | — | ⬜ |

---

## Spectra — Pipeline

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| S21 | **DER_SNR at ingestion time.** Add DER_SNR computation to the ticket validation profile so new ingestions get SNR at write time, rather than relying solely on the artifact generator fallback. | 🟢 | — | ⬜ |
| S22 | **SNR gate integration tests.** Tests for the display gate (SNR < 5 excluded from waterfall) and compositing relative gate (< 1/3 group median → rejected). | 🟢 | — | ⬜ |

---

## Photometry

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| P4 | **Photometry color palette expansion.** The current palette runs out of distinct colors when many bands are present. Expand with more distinguishable hues. | 🟡 | — | ⬜ |

---

## New Data Regimes

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| DR1 | **Gamma-ray nova detection pipeline.** Add support for detecting gamma-ray novae via Fermi-LAT references. | 🔴 | — | ⬜ |
| DR2 | **Gamma-ray visualization.** Add support for visualizing gamma-ray detections in the light curve panel. | 🟡 | DR1 | ⬜ |

---

## Frontend

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| F6 | **Splash page = catalog page.** Merge so people access the catalog without navigating away. Show stats on every page. | 🟡 | — | ⬜ |
| F7 | **Total unique spectral visits in stats display.** | 🟢 | — | ⬜ |
| F8 | **Stats broken out by spectral regime.** Separate stats per regime (optical, UV, NIR, etc.). | 🟡 | — | ⬜ |
| F9 | **Sources column for ticket-ingested data should show bibcode.** | 🟢 | — | ⬜ |

---

## Pipeline & Infrastructure

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| PI1 | **`refresh_references` payload reduction.** Hits the 256 KB SFn payload limit for novae with many references (e.g., IM Nor, recurrent novae). Needs a payload reduction refactor similar to the `discover_spectra_products` fix. | 🟡 | — | ⬜ |
| PI2 | **CloudWatch log group cleanup formalization.** Cleanup script exists; formalize as recurring task or retention policy. | 🟢 | — | ⬜ |
| PI3 | **Initial ingestion enrichment.** `initialize_nova` should look up the discovery date, add the nova type(s) (e.g., Symbiotic, Fe II, He/N), and ensure clean display names. Underscore removal is done; discovery date lookup and type assignment are new. | 🟡 | F10 | ⬜ |
| I2 | **Coordinate dedup ASL wiring.** Wire the coordinate-based dedup logic into the identity resolution step function so near-duplicate novae are caught automatically. | 🟡 | — | ⬜ |
| R4 | **Smooth pipeline (end-to-end).** Integration and polish pass — the "it all just works" capstone. Depends on most other work being done. | 🔴 | R5, most others | ⬜ |

---

## Data Model

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| F10 | **Change nova "Type" field to a list.** Currently a string; should support multiple types (e.g., classical + symbiotic). Prerequisite for PI3 automatic type assignment. | 🟡 | — | ⬜ |

---

## Operator Tooling

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| R5 | **Unified reseed/redeploy/rebuild app (with GUI).** Consolidate the scattered operator scripts (reseed_work_items, set_nova_dates, batch_ingest, propose_filters, etc.) into a single Streamlit or similar app. | 🔴 | — | ⬜ |
| OT8 | **UVES spectra reduction pipeline.** Build own reduction pipeline for raw UVES data, rather than relying solely on ESO Phase 3 reduced products. FORS2 via esorex/esoreflex also explored. | 🔴 | — | ⬜ |

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
| L5 | **Reduce CloudWatch Logs Insights cost.** Stricter selection criteria for which log groups to query, to reduce data scanned per query. Charged per GB scanned, not just per GB ingested. | 🟢 | — | ⬜ |
| L6 | **Minimize per-item logging.** Reduce chattiness — e.g., one aggregated log entry for a batch of warnings instead of one entry per warning. Cuts both log volume and Insights scan cost. | 🟡 | — | ⬜ |

---

## Documentation & Design

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| D1 | **Update current-architecture.md.** Dated 2026-03-28. Reflects pre-Epic-5 state. | 🟡 | — | ⬜ |
| D2 | **Recurrent novae design.** Multiple outbursts, cross-outburst identity, epoch disambiguation. Includes redesigning spectra and photometry viewer to accommodate multiple outburst epochs per object. | 🔴 | — | ⬜ |
| D3 | **Audit .py file documentation.** Ensure docstrings and inline comments across services are accurate and up to date. | 🟡 | — | 🔲 |

---

## Long-Range

| ID | Task | CC | Dep | Status |
|----|------|----|-----|--------|
| R10 | **Rebuild spectra.py.** Eliminate redundancies and clean up the artifact generator's spectra pipeline end-to-end. Hard floor fix (B19) is part of this. | 🔴 | — | ⬜ |
| LR1 | **Data donation page + backend infrastructure.** Allow external contributors to submit observational data to the catalog. | 🔴 | — | ⬜ |
| LR2 | **Provenance and accreditation system for donated data.** Rules and enforcement for how donated data can be used: creator provisions (co-authorship, acknowledgement, bundle inclusion), downloader obligations (sign terms, notification to donor, README attribution). | 🔴 | LR1 | ⬜ |

---

## Scoreboard

| Category | Count |
|----------|-------|
| ✅ Done | 104 |
| ⬜ Remaining (bugs) | 3 |
| ⬜ Remaining (features/tasks) | 31 |
| **Total open** | **34** |

---

## Suggested Priority Order

**Quick wins / high impact:**
1. B18 — Radio test fixture rebuild (10 min, unblocks 17 tests)
2. S22 — SNR gate integration tests (close out the epic)
3. F9 — Sources bibcode column (small artifact generator + frontend change)
4. F7 — Spectral visits in stats display

**Spectra quality & display:**
5. S6 — Spectra quality audit
6. B19 — Spectra y-axis hard floor
7. S18 — sqrt(flux) scaling
8. S19 — X-ray keV x-axis
9. S20 — Non-optical spectral features

**Data model & ingestion enrichment:**
10. F10 — Nova Type as list
11. PI3 — Initial ingestion enrichment (discovery date, types)
12. S21 — DER_SNR at ingestion time

**Pipeline robustness:**
13. PI1 — refresh_references payload reduction
14. PI2 — CloudWatch log group cleanup formalization
15. L5 — Reduce Logs Insights cost
16. L6 — Minimize per-item logging

**Frontend overhaul:**
17. F6 — Splash page = catalog page
18. F8 — Stats by regime
19. P4 — Photometry color palette

**New capabilities:**
20. DR1/DR2 — Gamma-ray pipeline + visualization
21. I2 — Coordinate dedup ASL wiring

**Bigger arcs:**
22. R5 — Unified operator app
23. T1, T2 — Test suites
24. L1 — Log enrichment
25. D1 — Architecture doc update
26. D2 — Recurrent novae design
27. OT8 — UVES reduction pipeline
28. R10 — spectra.py rebuild
29. LR1/LR2 — Data donation + provenance
30. R4 — Smooth pipeline (capstone)

---

# Completed Tasks

_104 tasks completed across all workstreams._

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
| B12 | Sparkline left/right margins misaligned |
| B13 | NaN sentinel in spectra.json |
| B14 | Zoom state lost on feature/scale toggle |
| B15 | Hover labels broken while zoomed |
| B16 | Spectra below x-axis (negative flux packing) |
| B20 | Red wavelength edge clipping (ADR-035 regime updates) |
| B21 | Reset axes scaling regression in single-spectrum mode |
| B22 | Zoom + spectral line toggle state bug |
| B23 | Radio upper limits silently dropped by artifact generator |
| B24 | Spectral visits not persisted to DDB (finalizer writeback gap) |
| B25 | Spectral visits miscounting midnight-spanning runs |
| B26 | Underscore nova names (pipeline using candidate_name not SIMBAD main_id) |
| B27 | CDK orphan log groups (AssetHashType.OUTPUT for Docker assets) |
| B28 | SNR only displaying for ~1 in 5 observations (UVES UV_SOBF vs UV_SFLX) |
| B29 | Spectral visits regression (pre-feature novae missing DDB field) |
| BN1 | Bundle generator hardcoded S3 path |
| BN2 | Bundle metadata.json count vs README count mismatch |
| BN3 | Empty .bib on partial sweeps (references pre-population) |
| BN4 | Empty sources.json spectra array |
| S1 | LTTB downsampling / flat spectra fix |
| S2 | X-axis truncation |
| S3 | Flux floor for log scale |
| S4 | Multi-arm spectra merge (X-Shooter/UVES) |
| S4b | NaN sentinel fix for merged spectra |
| S4c | Merge MJD tolerance widening + overlap rejection |
| S5 | Spectral feature hover (scatter trace approach) |
| S7 | Multi-regime spectra display (ADR-034 + frontend regime tabs) |
| S8 | Wavelength/SNR enrichment pipeline |
| S9 | ADS collection filter (spurious references fix) |
| S10 | Same-day spectra deduplication |
| S11 | Collision-aware waterfall packing |
| S12 | Dense color palette fix |
| S13 | Dichroic gap trace splitting |
| S14 | Same-night interpolation / cross-instrument stitching |
| S15 | Spectral line feature units Å → nm |
| S16 | STIS adapter for spectra (MAST HASP discovery + FITS profile) |
| S17 | Spectral visits catalog column |
| S23 | ADR-033 spectra compositing pipeline (full implementation) |
| S24 | DER_SNR fallback + SNR display gate + compositing relative gate |
| S25 | Equitable vertical spacing (gap capping attempted + reverted; pending R10) |
| S26 | Long-wavelength spectra regime slicing (ADR-034 framework) |
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
| F11 | Discovery date MJD on nova page |
| F12 | Spectral visits on nova page |
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
| T5 | DER_SNR unit tests (24 tests) |
| OT1 | Reseed work items script |
| OT2 | CloudWatch log viewer |
| OT3 | Bundle diagnostic (root-caused BN1–BN4) |
| OT4 | CloudWatch log viewer Fargate support |
| OT5 | Pull dev artifacts script |
| OT6 | set_nova_dates operator tool |
| OT7 | batch_ingest try-parse subcommand |
| OT8a | propose_filters.py |
| OT9 | CloudWatch log tracker configurable time/records |
| OT10 | CloudWatch reader app — Fargate + Standard workflow log coverage |
| OT11 | VOTable → ticket converters (Chomiuk+2021 radio + Strope+2010 V-band) |
| OT12 | add_radio_freq.py (radio band registry CLI) |
| OT13 | deploy.sh default changed to NovaCat only |
| OT14 | Chip-gap zero-run diagnostic script |
| OT15 | Nova candidate processor notebook |
| OT16 | ESO instrument reduction feasibility survey (FORS2 + UVES) |
| DOC1 | ADR-034 spectra wavelength regime model |
| DOC2 | ADR-035 spectra regime splitting and per-regime display range |
| DOC3 | ADR-033 amendment — compositing purpose reframing |
