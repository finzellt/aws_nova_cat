# DESIGN-001: Photometry Ingestion System — Full Redesign

**Status:** Draft
**Date:** 2026-03-18
**Author:** TF
**Document class:** Design / Scoping (feeds future ADRs; does not itself constitute decisions)

**Relates to:**
- `ADR-015` — Photometry Ingestion Mechanism and Column Mapping Strategy *(partially superseded by this document's successor ADRs; see §8)*
- `ADR-016` — Band and Filter Resolution Strategy *(superseded by this document's successor ADRs; see §8)*
- `docs/specs/photometry_table_model.md` *(under revision; see §5)*

---

## 1. Executive Summary

The NovaCat photometry ingestion system was initially designed as a second-tier concern,
with spectra taking priority for MVP. As a result, the current system is a collection of
locally-reasonable patches applied to an undesigned foundation. The patches work for the
narrow cases they address, but they have no principled generalization path — and nova
photometry, spanning optical through gamma-ray regimes and drawing from dozens of
heterogeneous source formats, is not a narrow problem.

This document promotes photometry to first-class status and lays out the full design space
that must be addressed before any production-quality photometric data can enter NovaCat.
It is deliberately exploratory: it maps the terrain, identifies the key design questions,
proposes a layered architecture, and decomposes the work into ordered epics. It does not
make binding architectural decisions. Those will follow as individual ADRs, grounded in
the shared understanding this document establishes.

### 1.1 Guiding Principle: IVOA Standards as the Backbone

A foundational commitment of this redesign is that **IVOA standards are the primary
reference for all conceptual vocabulary, data models, and interoperability decisions.**
This means:

- **Terminology** follows IVOA UCD1+ controlled vocabulary wherever a UCD exists for the
  concept. UCDs provide unambiguous, regime-agnostic definitions that are independent of
  any particular instrument, survey, or community convention. Where this document uses a
  term that has a UCD counterpart, that UCD is the authoritative definition.
- **Data model structure** follows IVOA PhotDM 1.1 for photometric metadata (bands,
  photometric systems, magnitude systems, zero points) and IVOA ObsCore / STC for
  spatial and temporal coordinates.
- **Band spectral metadata** is sourced from or cross-referenced against the IVOA SVO
  Filter Profile Service (SVO FPS) wherever SVO entries exist.
- **Column mapping** uses IVOA UCD-based resolution as a first-class tier (not an
  afterthought) for VO-annotated source files.

Bespoke NovaCat conventions are introduced only where IVOA standards are absent, silent,
or insufficiently specific for the multi-regime nova context.

---

## 2. Motivation: What Is Wrong With the Current State

The problems are structural, not incidental. They fall into three categories.

### 2.1 The filter/band identity problem is unsolved

The current system treats filter identity as a string-matching problem: given a string
like `"V"` or `"Johnson V"` or `"Ks"`, look it up in a Python dict and return a
`(filter_name, phot_system)` pair. This approach has several fatal flaws:

**It has no principled coverage model.** The current lookup tables (`_COMBINED_BAND_LOOKUP`,
`_DEFAULT_PHOT_SYSTEM`, etc.) were populated by thinking of examples at design time.
There is no process for knowing when they are complete, no definition of what "complete"
means, and no authoritative external source they are derived from or checked against.
By contrast, the IVOA UCD1+ vocabulary and SVO Filter Profile Service together provide
an external, community-maintained reference that can anchor both the terminology and
the coverage boundary of the band registry.

**It encodes band knowledge in Python source code.** Band properties — canonical name,
photometric system membership, spectral coordinate metadata, known aliases — are a *data*
concern, not a *logic* concern. Embedding them as module-level dicts makes them invisible
to any tooling, impossible to version independently of the code, and awkward to extend
without modifying application logic.

**It has no model of band identity itself.** A photometric band is not just a string.
It has a photometric system, a spectral coordinate (central wavelength, frequency, or
energy), a magnitude system (Vega, AB, ST), and a set of aliases by which it appears in
real-world data. None of this structure exists anywhere in the current system — not in
the code, not in JSON, not in the schema.

**It treats ambiguity as a special case.** ADR-016 Decision 4 flags context-aware
disambiguation as a problem to be aware of, but provides no systematic algorithm. `"K"`
was identified as ambiguous because the author happened to think of it. There is no
process for identifying other ambiguous strings, no definition of what "ambiguous" means
precisely, and no principled resolution strategy.

### 2.2 The photometry table model is under-specified for full regime coverage

The current `photometry_table_model.md` (v1.1) and the corresponding `PhotometryRow`
Pydantic model were designed with optical-through-X-ray coverage in mind, but the schema
decisions were made before the full wavelength scope was established. Several issues are
now apparent:

**`PhotSystem` is a coarse enum that conflates distinct concepts.** `"Johnson-Cousins"`,
`"Sloan"`, and `"2MASS"` are photometric systems in the traditional sense. `"Swift-UVOT"`
is an instrument system. `"Radio"` and `"X-ray"` are wavelength regime labels. These are
different kinds of things, and treating them as peer enum values will cause problems as
coverage expands.

**`SpectralCoordUnit` is missing units for new regimes.** The current enum covers
`Angstrom`, `nm`, `GHz`, `MHz`, `keV`. Gamma-ray astronomy typically uses `MeV` or
`GeV`. JWST far-IR photometry uses `μm`. These are absent.

**`FluxDensityUnit` is sparse for multi-regime coverage.** The current enum covers `Jy`,
`mJy`, `μJy`, `erg/cm²/s/Hz`, `erg/cm²/s/keV`. X-ray count-rate-based measurements
may use `counts/s`. Gamma-ray measurements may use `erg/cm²/s` (energy flux, not
flux density). The enum does not cover these cases.

**Upper limit semantics are inconsistently supported across regimes.** The current
`is_upper_limit` / `limiting_value` model was designed with optical non-detections in
mind. Multi-wavelength upper limits (radio, X-ray, gamma-ray) have regime-specific
conventions that may require richer representation.

### 2.3 The ingestion pipeline architecture is unfinished

A handler stub for `photometry_ingestor` exists, but it is a structural placeholder —
it does not yet contain the logic that would make the pipeline operational. No handler
currently reads a staged S3 file, parses it into rows, dispatches to the adapter, applies
the quarantine threshold, writes valid rows to storage, or writes failures to the
diagnostics bucket. The Step Functions state machine for `ingest_photometry` references
these handlers by name but their implementations are empty.

This is the correct sequencing: design before implementation. But it means the pipeline
is not yet operational, and before it can be built out, the foundational data model
questions (§2.1, §2.2) must be resolved, because the handlers depend on them.

---

## 3. Ecosystem Assessment: What Already Exists

Before committing to bespoke implementation, a survey of the existing astronomy Python
software ecosystem was conducted to identify tools that address any of the five
sub-problems this pipeline must solve. The findings are summarized here. The full survey
is available in `docs/research/astro_software_report.tex` and
`docs/research/deep-research-report.md`.

### 3.1 Verdict: Scenario B — Partial Coverage

The ecosystem provides strong building blocks for two specific sub-problems — filter
metadata retrieval and heterogeneous file parsing — but does not provide the end-to-end
normalization layer NovaCat requires. No existing tool covers alias resolution,
systematic disambiguation, or a unified multi-regime measurement schema spanning
magnitudes, flux densities, count rates, and energy fluxes. **The bespoke components
planned in this design are genuine gaps, not reinventions of existing wheels.**

### 3.2 What We Adopt

**SVO Filter Profile Service (SVO FPS) + `astroquery.svo_fps`** is the authoritative
external registry for UV/optical/NIR band metadata. The SVO VO endpoint currently
indexes 11,013 filters (as of 2025-09-04) and supports lookup by filter ID, `PhotCalID`,
wavelength range, facility, and photometric system. The `astroquery.svo_fps` Python
client returns Astropy tables with wavelength statistics and calibration fields including
`PhotCalID`, `MagSys`, `ZeroPoint`, and `ZeroPointType`. SVO FPS is the seeding
mechanism for NovaCat's band registry for all regimes it covers (optical through NIR).

**`svo_filters`** (MIT-licensed) is a viable offline alternative: it ships a large set of
filter definitions locally as XML, covering GALEX, 2MASS, WISE, JWST/NIRCam+MIRI, and
others. This removes any ingestion-time network dependency at the cost of Lambda
deployment artifact size. It is the preferred option if SVO FPS query latency or
reliability is a concern for the out-of-band registry synchronization job.

**Astropy table I/O + ECSV** is the backbone for reading source files. Astropy's unified
`Table.read/write` covers CSV-like inputs, FITS tables, and VOTables. ECSV (Enhanced
CSV) is specifically recommended for operator-prepared canonical files: it preserves
column data types and units in a plain-text header, materially reducing ingestion
ambiguity compared to plain CSV. This upgrades the Tier 1 canonical CSV recommendation
— canonical operator files should be ECSV, not bare CSV.

**Astropy UCD parsing + `pyvo` field-by-UCD helpers** are the implementation foundation
for Tier 3 (UCD-based column mapping). Astropy provides `parse_ucd` with optional
validation against the controlled vocabulary; `pyvo` DAL results support
`fieldname_with_ucd` and `fieldname_with_utype` for locating fields by semantic
annotation. Together these enable the UCD-based mapping tier for VO-annotated source
files without bespoke string matching.

**`sncosmo`'s documented column alias pattern** is the precedent and design template for
NovaCat's synonym registry. `sncosmo` explicitly documents a case-insensitive alias set
for its canonical column names (`time`, `band`, `flux`, `fluxerr`, `zp`, `zpsys`). This
is precisely the pattern the Tier 2 synonym registry follows; the scope is simply
expanded to cover all `PhotometryRow` fields.

### 3.3 What We Build Bespoke

The following components have no adequate off-the-shelf equivalent:

**Band string normalization layer (SP1 + SP2):** SVO FPS does not provide an alias
dictionary that maps community shorthand (`"V"`, `"Johnson V"`, `"uvw2"`, `"W1"`,
`"Ks"`) to canonical filter identities from heterogeneous source files. This is the
band registry alias set and the disambiguation model — both fully bespoke (Layers 1
and 2).

**High-energy and radio band entries:** SVO FPS coverage ends at NIR/MIR. For X-ray,
gamma-ray, and radio regimes, observations are described as energy or frequency *range
strings* (e.g., `"0.3–10 keV"`, `"15 GHz"`) rather than discrete transmission curves
with community-wide identifiers. There is no standardized registry for these band
descriptions comparable to SVO. Band registry entries for these regimes are fully
bespoke, anchored to IVOA UCD vocabulary (`em.X-ray`, `em.radio.*`) and instrument
documentation rather than SVO FPS.

**Multi-regime `PhotometryRow` schema (SP4):** PhotDM provides vocabulary and conceptual
structure; Astropy provides physical primitives (`Time`, `SpectralCoord`, `Quantity`).
Neither provides a ready-to-use Python class hierarchy for a single measurement row
spanning all regimes with upper limits, multiple measurement kinds, and provenance as
first-class fields. This is `PhotometryRow` — fully bespoke, IVOA-aligned in naming and
semantics (Layer 3).

**Validation and mapping layer (SP3 gap):** The ecosystem consistently stops at "return
an Astropy Table." Column mapping to a canonical schema, type coercion, band
canonicalization, and failure/escalation policies are application-specific code. This is
the adapter architecture (Layer 4).

### 3.4 Lambda Deployment Risks

**Astroquery caching:** `astroquery` manages caching under `$HOME/.astropy/cache` by
default. In Lambda, this path is ephemeral and per-invocation, increasing cold-start
variance. **Mitigation:** The band registry synchronization job runs out-of-band (CI or
scheduled batch), snapshots the required SVO metadata into a compact bundled artifact
(JSON, SQLite, or Parquet), and the Lambda reads from that artifact — no runtime SVO
queries. SVO FPS explicitly supports verbosity controls (omit transmission curves) to
keep snapshot payloads small.

**`svo_filters` artifact size:** Shipping all filter XML locally avoids network
dependency but may meaningfully increase Lambda deployment package size. Evaluate
against Lambda size limits before adopting; a curated subset (only regimes NovaCat
actually uses) is likely sufficient.

**`pyphot` HDF5 dependency:** `pyphot` requires HDF5, which is a non-trivial native
dependency for Lambda packaging. Avoid as a direct dependency; use SVO FPS or
`svo_filters` instead.

**`pysynphot` is EOL:** `pysynphot` is explicitly end-of-life and unsupported on
Python 3.11+. Do not introduce as a dependency. Use `synphot` (its maintained
successor) if STScI synthetic photometry functionality is ever needed.

---

## 4. Scope

### 4.1 Wavelength regimes in scope

The redesigned photometry system must support the following regimes. The systems and
instruments listed under each regime are **illustrative examples, not an exhaustive or
closed list** — the design must accommodate any system or instrument within each regime
without requiring structural changes.

| Regime | Example systems / instruments | Notes |
|---|---|---|
| Optical — broadband | Johnson-Cousins UBVRI, Sloan/SDSS ugriz, Bessel, Gaia BP/RP | Core nova photometry regime |
| Optical — narrowband | Hα, [O III], and similar | Occasionally reported in nova tables |
| UV | Swift/UVOT (uvw2, uvm2, uvw1, u, b, v), GALEX (FUV, NUV) | Common in well-studied novae |
| Near-infrared | 2MASS JHKs, WISE W1–W4, UKIRT WFCAM | Important for dusty novae |
| Mid- to far-infrared | JWST/MIRI, Spitzer IRAC/MIPS | Rarer; JWST coverage growing |
| X-ray | Swift/XRT, Chandra, XMM-Newton | SSS phase characterization |
| Radio | VLA, ATCA, AMI, e-MERLIN | Shock-interaction measurements |
| Gamma-ray | Fermi/LAT | Detected in handful of bright novae |

This scope is deliberately broad. Not all regimes need full support on day one. The
design must, however, be able to accommodate all of them without requiring structural
schema changes as new regimes are added.

### 4.2 Data types in scope for this design

This document covers the ingestion of **primary photometric measurements**: flux, magnitude,
count rate, and energy flux in a single band at a single epoch. The following related
data types are in scope for NovaCat but are addressed in separate design documents:

- **Colors and flux ratios** (magnitude differences or flux ratios between two bands,
  e.g. `B-V`, `g-r`, `X/O ratio`) — these are derived quantities with a fundamentally
  different structure from single-band measurements. They will be modeled as a dedicated
  `ColorRow` type. Source files that contain *only* colors or flux ratios (with no
  single-band measurements) are a known real-world occurrence and must be handled
  gracefully — they are valid input, not malformed data. Design deferred to **DESIGN-002**.

### 4.3 Out of scope for this design

- Optical interferometry / aperture masking data (no standard photometric framework)
- Astrometric measurements (not photometry)
- Spectrophotometry (handled by the spectra pipeline)
- Resolved radio imaging (flux density measurements derived from interferometric image
  synthesis; distinct from unresolved point-source flux densities from single-dish or
  short-baseline observations)
- The frontend visualization of light curves (governed by ADR-013; separate concern)
- The data donation user interface (post-MVP)
- Automated nightly sweep / batch re-ingestion (post-MVP)

---

## 5. Layer Model: What Needs To Be Built

The redesigned photometry system has seven distinct layers. Each layer is a coherent unit
of design and implementation that can be addressed in a separate epic.

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 6 — Persistence & Query                              │
│  DynamoDB / S3 storage, access patterns, versioning         │
├─────────────────────────────────────────────────────────────┤
│  Layer 5 — Ingestion Workflow                               │
│  Lambda handlers, Step Functions states, quarantine logic   │
├─────────────────────────────────────────────────────────────┤
│  Layer 4 — Column Mapping & Adapter Architecture            │
│  Tiers 1–4, adapter protocol, synonym registry, UCD mapping │
├─────────────────────────────────────────────────────────────┤
│  Layer 3 — Photometry Table Model (Revised)                 │
│  PhotometryRow schema, enums, provenance fields             │
├─────────────────────────────────────────────────────────────┤
│  Layer 2 — Band Resolution Model                            │
│  Disambiguation algorithm, ambiguity taxonomy,              │
│  resolution provenance, operator review gates               │
├─────────────────────────────────────────────────────────────┤
│  Layer 1 — Band Registry                                    │
│  Versioned data artifact: band identity, aliases,           │
│  spectral metadata, photometric system membership           │
├─────────────────────────────────────────────────────────────┤
│  Layer 0 — Pre-Ingestion Normalization                      │
│  File shape normalization, multi-nova splitting,            │
│  sidecar context ingestion                                  │
├─────────────────────────────────────────────────────────────┤
│  UnpackSource — Source File Unpacking                       │
│  Zip detection, format filtering, per-file fan-out          │
└─────────────────────────────────────────────────────────────┘
```

`UnpackSource` is a pre-pipeline gate that runs before Layer 0. It is not a numbered
layer because it carries no data model responsibility — it is a pure routing and
dispatch component. Layer 0 sits below the adapter pipeline and runs first for each
individual file: it takes a raw, heterogeneous input file in whatever shape it arrives
and produces a normalized, per-nova, long-format table ready for the adapter. Layers 1
and 2 are the foundational data-model layers that must be designed before any
implementation above them. The remainder of this document focuses primarily on the
design questions in Layers 0–3, which are the most novel and the least addressed by
prior work.

**Structured diagnostic residuals.** Multiple layers in the pipeline encounter inputs
they cannot fully interpret: Layer 0 encounters header lines it cannot parse and
synonym keys it does not recognise; the adapter (Layer 4) encounters band strings it
cannot resolve; the validation handler (Layer 5) encounters rows that fail structural
checks. These partial-interpretation artifacts are operationally valuable — they are the
primary signal for expanding `synonyms.json` coverage, improving parser tiers, and
extending the band registry. Rather than scattering these artifacts across layer-specific
ad hoc fields, the pipeline uses a single consolidated residual model
(`HeaderResidual`) that tags each residual by source layer and failure category. This
gives operator review tooling one artifact to inspect per ingestion event. The normative
specification for the residual model is in ADR-021 §7.5; any pipeline stage that
produces diagnostic residuals should emit into the same model.

### UnpackSource — Source File Unpacking

`UnpackSource` is the entry point for all photometry ingestion. Its sole responsibility is to determine whether a staged source file is
a supported archive format (`.zip`, `.gz`, `.tar.gz`), and if so, to
unpack it and fan out each valid file entry as an independent ingestion
event. Archive format detection precedes the four-case decision tree:
`.gz` single-file archives are unpacked to their contained file and
processed as a single-entry archive; `.tar.gz` archives are unpacked to
their full entry list and each entry is processed independently per the
decision tree. The decision tree itself applies uniformly across all
supported archive formats once unpacked.It is a
fire-and-forget dispatcher: once all events are published, the workflow exits without
waiting for downstream results.

**Decision tree:**

1. **Not a zip:** Publish a single `IngestPhotometryEvent` for the staged file and exit.
2. **Is a zip, contains at least one file of acceptable format:** Unpack the archive.
   For each entry whose extension is in the accepted format allowlist, publish one
   independent `IngestPhotometryEvent`. Skip (log, do not quarantine) any entries with
   unrecognized extensions. Exit after all events are published.
3. **Is a zip, all entries are themselves zip files:** Quarantine with reason code
   `nested_zip_archive`. Recursive zip handling is explicitly out of scope; the operator
   must manually flatten the archive.
4. **Is a zip, no entries of acceptable format remain after filtering:** Quarantine with
   reason code `no_processable_entries`.

**Accepted format allowlist:** `.csv`, `.ecsv`, `.fits`, `.fts`, `.vot`, `.xml`, `.json`.
This list is the authoritative gate; additions require an explicit allowlist update.
`.json` entries are written to S3 as-is and their resulting S3 key is
passed to `prep_photometry_file` as `sidecar_s3_key` alongside the
primary data file from the same archive. `UnpackSource` does not
interpret the `.json` entry as a sidecar — that association is
`prep_photometry_file`'s responsibility.

**Fan-out mechanics:** Each `IngestPhotometryEvent` published by `UnpackSource` is
independent. `UnpackSource` does not track downstream execution status. This means a
zip containing five files produces five fully independent ingestion executions, each
with its own idempotency key, quarantine scope, and failure surface. Failures in one
file do not affect the others.

**What `UnpackSource` does not do:** It does not parse file contents, resolve nova
identity, validate column structure, or make any data model decisions. It is a
structural gate only. All content decisions belong to Layer 0 and above.

### Layer 1 — Band Registry

The band registry is the single authoritative source of truth for photometric band
identity in NovaCat. It is a *data artifact*, not application logic. The term "band"
is used throughout NovaCat's design vocabulary as the regime-agnostic term for a
photometric passband. The term "filter" is reserved for contexts where it refers
specifically to the physical or conceptual object in the IVOA SVO Filter Profile Service
(SVO FPS), whose API uses "filter" as its core term.

**What a band entry must capture:**

- **Canonical identity**: a stable, unique band ID (the NovaCat canonical name, e.g.
  `"Johnson_V"`, `"2MASS_Ks"`, `"Swift_UVOT_uvw2"`). The naming convention must be
  defined in ADR-017.
- **Photometric system**: which system this band belongs to (see Layer 3 discussion of
  the `PhotSystem` redesign)
- **Spectral coordinate**: central wavelength/frequency/energy and its unit; this is
  regime-dependent. The UCD for the spectral coordinate value is `em.wl` (wavelength),
  `em.freq` (frequency), or `em.energy` (energy) per IVOA UCD1+.
- **Magnitude system**: Vega, AB, ST, or N/A (for radio/X-ray/gamma-ray)
- **Alias set**: all known string forms by which this band appears in real-world data
  (e.g. `"V"`, `"Johnson V"`, `"Vis"` as candidate aliases — though `"Vis"` resolves
  to an excluded entry; see below)
- **Disambiguation context**: if this band's aliases overlap with aliases of another
  band, the registry records which aliases are shared and what contextual signals
  resolve the ambiguity (see Layer 2)
- **SVO Filter Profile Service ID**: the SVO FPS identifier for cross-referencing the
  authoritative filter database (e.g. `"Generic/Johnson.V"`), where one exists. This
  is the bridge between NovaCat's band registry and the IVOA-maintained filter data.
- **Regime tag**: a controlled-vocabulary regime label (`optical`, `uv`, `nir`, `mir`,
  `fir`, `xray`, `radio`, `gamma`) used by the disambiguation model and the table model
- **Excluded flag + reason**: bands that represent known non-photometric observation
  modes (e.g. AAVSO visual estimates `"Vis."`, unfiltered `"CV"`, tri-color channels
  `"TG"`) are first-class registry entries with an `excluded: true` flag and a
  human-readable rejection reason. They are not maintained in a separate file. This
  ensures all band string knowledge lives in one place.

**Adding excluded band entries:** The registry must support a simple, low-friction
mechanism for adding new excluded entries as they are encountered during ingestion
(e.g. a CLI command, a small operator script, or a well-documented manual edit
protocol). Discovering a new excluded string during a real ingestion run must never
require a code change.

**Key open question:** The physical form of the registry — static versioned JSON/YAML
files in the repository vs. a DynamoDB table managed by an operator tool vs. something
else — is an architectural decision that must be made in a dedicated ADR. The
considerations include: how often entries are added, whether the registry must be
queryable at ingestion time with low latency, and whether the operator tooling needs to
support interactive editing. This is deferred to **ADR-017**.

**Relationship to SVO FPS and high-energy regimes:**

The IVOA SVO Filter Profile Service (http://svo2.cab.inta-csic.es/theory/fps/) is the
canonical external reference for band spectral data across optical, UV, and NIR/MIR
regimes. Where SVO entries exist, the registry is seeded from SVO via
`astroquery.svo_fps` and the SVO FPS ID stored. The registry is not a runtime SVO
client — it does not call SVO at ingestion time. It is a curated, versioned snapshot
produced by an out-of-band synchronization job (CI or scheduled batch), bundled as a
compact artifact (JSON, SQLite, or Parquet) that the Lambda reads directly. SVO FPS
supports verbosity controls to minimize snapshot payload size.

**`svo_filters`** (MIT-licensed) is a viable alternative for the bundled artifact: it
ships filter definitions locally as XML, covering GALEX, 2MASS, WISE, JWST/NIRCam+MIRI,
and others, removing any network dependency at the cost of deployment artifact size.
Evaluate against Lambda package size limits; a curated regime-specific subset is likely
sufficient.

**High-energy and radio bands are outside SVO FPS scope.** For X-ray, gamma-ray, and
radio regimes, the concept of a discrete filter transmission curve with a
community-wide identifier does not apply. Observations are described as energy or
frequency range strings (e.g., `"0.3–10 keV"`, `"15 GHz"`). There is no standardized
registry for these descriptions comparable to SVO FPS. Band registry entries for these
regimes are fully bespoke, anchored to IVOA UCD vocabulary (`em.X-ray`, `em.radio.*`)
and instrument documentation rather than SVO. This is a genuine gap in the broader
ecosystem, not an oversight in NovaCat's design.

### Layer 2 — Band Resolution Model

Band resolution is the process of mapping a filter string, as it appears in a source
data file, to a confirmed filter identity in the registry. This process must be
principled and systematic.

**A taxonomy of filter string cases:**

The following cases must be explicitly handled by the resolution model. This taxonomy
is the conceptual foundation that ADR-016 Decision 4 gestured at but did not provide.

| Case | Description | Example |
|---|---|---|
| **Canonical match** | String is the registry's canonical band ID | `"Johnson_V"` |
| **Unambiguous alias** | String is an alias of exactly one registry entry | `"Ks"` → `2MASS_Ks` |
| **Combined value** | String encodes both system and band, parseable by splitting | `"Johnson V"`, `"Sloan g'"` |
| **Ambiguous alias** | String is an alias of two or more registry entries | `"K"` → `2MASS_K` *or* radio K-band |
| **Excluded type** | String is a registry entry with `excluded: true` | `"Vis."`, `"CV"`, `"TG"` |
| **Unrecognized** | String appears in no registry entry and matches no known pattern | `"Hα-cust"` |

**Resolution provenance must be persisted.** The outcome of band resolution is not just
a `(band_id, phot_system)` pair — it must include a structured record of *how* that
resolution was reached. Each resolved row must carry metadata indicating:

- Whether the band and photometric system were explicitly provided in the source file
- Whether the band was resolved from an alias (and which alias)
- Whether the photometric system was inferred from the band identity (not stated in
  the source)
- Whether disambiguation was required, and if so, which contextual signal resolved it
- Whether any spectral coordinate metadata was inferred from the registry rather than
  read from the source

This provenance is part of the `PhotometryRow` contract (see Layer 3) and must be
stored alongside the measurement data. It enables downstream consumers to filter by
confidence level and operators to audit inferred values. The specific fields and their
allowed values will be defined in ADR-019.

**The disambiguation algorithm (key open question):**

For ambiguous aliases, a systematic algorithm is needed. The current ADR-016 Decision 4
proposes using adjacent column values (e.g. a present `phot_system` column) as context.
This is the right intuition but needs to be fully specified:

- What contextual signals are considered, and in what priority order?
- What happens when context is absent or itself ambiguous?
- Is the resolution deterministic (always pick the most common system for this alias)?
  Or does it escalate to operator review?
- Can file-level context (e.g. the `orig_catalog` column value = `"AAVSO"`) resolve
  ambiguity that row-level context cannot?

These questions must be answered in a dedicated ADR (**ADR-018**) with a fully specified
algorithm, expressible as a decision table or flowchart — not prose — to ensure it is
unambiguous and testable.

**Excluded band handling:**

Excluded bands are first-class registry entries with `excluded: true` and a human-readable
rejection reason. They are not maintained in a separate file. The rejection reason is
sourced from the registry entry and written to the row failure record. Adding a new
excluded entry must be a simple, low-friction operator action (see Layer 1).

### Layer 3 — Photometry Table Model (Revised)

The table model needs revision before the ingestion pipeline can be built against it.
The key issues identified in §2.2 must be resolved.

**`PhotSystem` redesign:**

The current `PhotSystem` enum mixes photometric systems, instrument systems, and regime
labels. A cleaner model separates these concerns:

- A **photometric system** is a defined set of band bandpasses with a calibrated
  magnitude or flux scale (e.g. Johnson-Cousins, Sloan/SDSS, 2MASS, AB, Vega).
- An **instrument system** is a specific implementation of a photometric system by a
  particular instrument (e.g. Swift/UVOT implements a UV photometric system; WISE
  implements a MIR photometric system).
- A **regime** is a broad wavelength domain label used for display and routing purposes.

Whether these should be separate fields in `PhotometryRow` or folded into the band
registry (where a band's system membership is already recorded) is a key design
question for **ADR-019**.

**Band resolution provenance fields:**

As described in Layer 2, `PhotometryRow` must carry structured provenance metadata
recording how band identity and photometric system were determined. The UCD vocabulary
provides a natural anchor for these fields (`meta.code.qual` is the closest UCD for
data quality/confidence metadata). The exact field set is deferred to **ADR-019**.

**Spectral coordinate coverage:**

The `SpectralCoordUnit` enum must be extended to cover:
- `μm` (mid- to far-infrared, JWST)
- `MeV`, `GeV` (gamma-ray)
- `eV` (soft X-ray, alternative to keV)

Whether these are added to the existing enum or whether the enum is replaced with a more
flexible model is deferred to **ADR-019**.

**Flux unit coverage:**

The `FluxDensityUnit` enum must be extended or restructured. The key question is whether
a single `flux_density` field with a unit enum can cleanly cover all regimes, or whether
regime-specific fields are needed (e.g. a separate `energy_flux` for gamma-ray, distinct
from `flux_density` which conventionally implies per-Hz or per-wavelength normalization).


### Layer 4 — Column Mapping & Adapter Architecture

The three-tier column mapping strategy defined in ADR-015 (canonical CSV → synonym
registry → UCD-based mapping → AI-assisted registration) is architecturally sound at
a high level, but several aspects require revision now that photometry is first-class.

**Tier count clarification:** ADR-015 describes a "three-tier" strategy, but the
implementation has four tiers: Canonical CSV (Tier 1), Synonym Registry (Tier 2),
UCD-based mapping (Tier 3), and AI-assisted registration (Tier 4). The "three-tier"
label emerged because Tier 4 (AI-assisted) was not planned for MVP. In this redesign,
Tier 3 (UCD-based mapping) is elevated to MVP status — it is critical for VO-annotated
source files and provides the principled, standards-grounded definition of "complete"
column coverage that the synonym registry alone cannot supply. The four tiers are all
first-class citizens of the architecture; implementation priority is a separate concern.

**Tier 1 upgrade — ECSV over plain CSV:** Based on the ecosystem assessment (§3),
operator-prepared canonical files should use **ECSV (Astropy Enhanced CSV)** rather
than plain CSV. ECSV preserves column data types and units in a plain-text header,
which materially reduces ingestion ambiguity and allows the Tier 1 parser to skip
type-coercion guesswork for well-formed input files. Plain CSV remains acceptable as
a fallback for files from sources that cannot produce ECSV.

**Tier 2 design precedent — `sncosmo` alias pattern:** The `sncosmo` library explicitly
documents a case-insensitive alias set for its canonical column names (`time`, `band`,
`flux`, `fluxerr`, `zp`, `zpsys`). This is precisely the pattern the NovaCat synonym
registry follows; the scope is expanded to cover all `PhotometryRow` fields.

**Changes required at this layer:**

- The adapter's internal band resolution logic (`_resolve_band()`) is removed from
  application code and replaced by a lookup against the band registry (Layer 1)
- The module-level Python dicts that currently encode band knowledge are eliminated
- Tier 3 (UCD-based mapping) is specified and implemented for VO-annotated source files
- The `MissingRequiredColumnsError` and quarantine threshold logic are reviewed against
  the revised table model

**Error handling at this layer is a first-class concern.** Column mapping failures,
band resolution failures, type coercion failures, and Pydantic validation failures are
distinct failure modes with distinct diagnostic value. The error taxonomy, the
information captured in each failure record, and the routing of failures to S3
diagnostics vs. quarantine must be explicitly designed — not left as implementation
details. Error handling design is a required deliverable of Epic D.

A revised `CanonicalCsvAdapter` is the primary implementation output of this layer's epic.

### Layer 5 — Ingestion Workflow

The Lambda handlers for the `ingest_photometry` Step Functions workflow must be
specified and implemented. The handler responsibilities, as currently understood, are:

1. **StagePhotometry** (or inline in event entry): Validate the S3 key, compute
   `file_sha256` if absent, perform idempotency check.
2. **ValidatePhotometry**: Parse the staged CSV, dispatch to adapter, collect
   `AdaptationResult`, apply quarantine threshold, write row failures to S3 diagnostics.
   Row failures written to S3 diagnostics must be **deduplicated**: if the same logical
   failure (same error type + same raw band string, for example) recurs across many rows,
   the diagnostics record should collapse these into a single entry with a count, rather
   than writing thousands of identical failure records. The deduplication key and
   collapsing strategy must be defined in the handler spec.
3. **PersistPhotometry**: Write valid `PhotometryRow` records to the canonical storage
   target (storage format TBD; see Layer 6).
4. **FinalizeIngestion**: Write a provenance/audit record; update nova-level metadata.

Handler input/output contracts must be specified as boundary schemas before
implementation. This layer depends on Layers 1–4 being stable.

### Layer 6 — Persistence & Query

The persistence format for `PhotometryRow` records is an open question documented in
ADR-015 as "Open Question 1" (Parquet vs. alternatives). This must be resolved before
Layer 5 can be fully implemented. Key considerations:

- The access patterns defined in `photometry_table_model.md` §Common Query Patterns
  should drive the storage format decision
- The existing architecture uses DynamoDB for operational/entity state and S3 for
  scientific data products — photometry almost certainly belongs in S3 as columnar data
- The choice of serialization format (Parquet, Feather, VOTable, CSV) affects the
  frontend's ability to read data directly vs. requiring a Lambda intermediary

This layer's decisions belong in **ADR-020**.

### Layer 0 — Pre-Ingestion Normalization

Layer 0 is the stage that runs before the adapter pipeline sees any data. Its job is to
take a raw input file — in whatever shape it arrives — and produce a normalized,
per-nova, long-format table that the adapter can process. The adapter currently assumes
this normalization has already happened; Layer 0 makes that assumption explicit and
defines the component responsible for satisfying it.

Layer 0 must handle three distinct problems:

**Wide-format files (multi-band columns).** Real-world photometry tables frequently
present multiple bands as separate columns rather than as separate rows — e.g., a file
with columns `Date`, `g`, `g_err`, `r`, `r_err`, `i`, `i_err`. This is a wide/pivoted
format. The adapter pipeline assumes tidy/long format (one measurement per row). Layer 0
is responsible for detecting wide-format input and pivoting it to long format before
passing rows to the adapter. The pivot must preserve all metadata columns (date,
telescope, observer, etc.) on every resulting row. The pivot also needs to handle the
case where some band columns are entirely null for a given epoch — those rows should be
dropped cleanly rather than ingested as null-magnitude measurements.

This is also the mechanism by which color-only or flux-ratio-only files are routed
correctly: if the pivot produces columns that map to `ColorRow` fields rather than
`PhotometryRow` fields, the routing logic at this layer dispatches them to the color
ingestion path (DESIGN-002) rather than the photometry adapter.

**Multi-nova files.** A single source file may contain measurements for more than one
nova — e.g., a survey paper's machine-readable table covering multiple objects. Before
any per-nova processing can occur, the file must be split by object identity. This
requires: (a) identifying the object name column, (b) resolving each distinct name to a
NovaCat `nova_id` using the existing nova resolution machinery, and (c) splitting the
file into per-nova subsets for independent ingestion. Rows whose object name cannot be
resolved to a known nova should be quarantined with a dedicated reason code rather than
silently dropped. The object-resolution step may be expensive (SIMBAD lookups); the
Layer 0 design must account for this in the Step Functions execution budget.

**Sidecar context files.** Source files sometimes arrive with supplementary metadata
that enriches or constrains the ingestion context — a companion file provided by a data
donor, a structured filename convention, or a manually prepared annotation. This
external context can resolve band ambiguities, establish the photometric system, identify
the object, or supply provenance fields absent from the main file. Layer 0 defines the
sidecar contract: what fields it may contain, how it is associated with its primary file,
and how its contents are injected into the ingestion context for downstream use.

The sidecar model is a foundational concept for the donation pathway, but it is also
useful in the MVP operator-controlled context: an operator preparing a file can include
a sidecar to assert context that would otherwise require manual disambiguation. The
detailed design of the sidecar schema and its relationship to the provenance model is
deferred to **DESIGN-002**.

**Key invariant:** Layer 0 is a *transformation* stage, not a *validation* stage. Its
output is a normalized table and an enriched context object. It does not make quarantine
decisions — those remain in Layer 5 (ValidatePhotometry). Layer 0 failures (e.g.,
multi-nova split failure due to unresolvable names) are propagated to Layer 5 as
structured errors, not raised as exceptions.

---

## 6. Key Design Questions

The following questions are explicitly open. Each will be resolved in a dedicated ADR.

| # | Question | Target ADR |
|---|---|---|
| 1 | What is the physical form and schema of the band registry? | ADR-017 |
| 2 | How is band registry data synchronized from SVO FPS and bundled with the Lambda? | ADR-017 |
| 3 | What is the complete, algorithmic band disambiguation procedure? | ADR-018 |
| 4 | How is `PhotSystem` restructured, and what schema changes (including provenance fields) does that drive? | ADR-019 |
| 5 | What is the canonical storage format for photometry table data? | ADR-020 |
| 6 | How are row-level duplicates detected across multiple ingestion events for the same nova? | ADR-015 (already flagged; resolution deferred) |
| 7 | How does the `ingest_photometry` workflow handle schema version migrations when `PhotometryRow` changes? | ADR-020 or separate |
| 8 | Does the disambiguation model escalate unresolvable ambiguities to operator review, or always fail-fast at the row level? | ADR-018 |
| 9 | How are upper limits represented across regimes? Is the current `is_upper_limit` / `limiting_value` model sufficient? | ADR-019 |
| 10 | What is the deduplication key for row failures in S3 diagnostics? | Layer 5 handler spec |
| 11 | How does the wide-to-long pivot handle files that mix `PhotometryRow` and `ColorRow` columns in the same table? | DESIGN-002 / Layer 0 spec |
| 12 | What is the sidecar contract schema, and how is a sidecar associated with its primary file at upload time? | DESIGN-002 |
| 13 | How does multi-nova object resolution interact with the Step Functions Express 5-minute execution budget? | Layer 0 spec / ADR-021 |
| 14 | What fan-out mechanism does `UnpackSource` use to publish per-file events (EventBridge, SQS, direct SFN execution)? | ADR-021 |

---

## 7. Proposed Work Decomposition

The following epics are proposed in dependency order. Each epic is expected to produce
one or more ADRs, updated contracts, and implementation artifacts.

### Epic A-00 — UnpackSource Design and Implementation

**Output:** `UnpackSource` Lambda handler, accepted format allowlist, new quarantine
reason codes (`nested_zip_archive`, `no_processable_entries`), fan-out mechanism
decision, ADR-021 (shared with Epic A-0).

**Dependency:** None. `UnpackSource` has no dependency on the band registry, table
model, or adapter — it is a pure routing component.

**Scope:** Implement zip detection and unpacking; define and enforce the accepted format
allowlist; implement the four-case decision tree; choose and implement the fan-out
mechanism for publishing independent `IngestPhotometryEvent`s; add the two new
quarantine reason codes to `PhotometryQuarantineReasonCode`; write unit tests covering
all four decision tree cases including the nested-zip quarantine path.

### Epic A-0 — Pre-Ingestion Normalization Design *(design-only; no implementation)*

**Output:** Layer 0 specification: wide-to-long pivot rules, multi-nova split protocol,
sidecar contract (stub), ADR-021.

**Dependency:** None (parallel with Epic A). However, the sidecar schema design depends
on DESIGN-002 for its full specification; Epic A-0 may produce a stub sidecar contract
that DESIGN-002 completes.

**Scope:** Define the wide-format detection heuristic and pivot algorithm; specify the
multi-nova object name column detection and SIMBAD resolution protocol; define the
quarantine reason code for unresolvable object names; specify the Layer 0 output
contract (normalized table + context object) that Layer 4 consumes; stub the sidecar
schema with a forward reference to DESIGN-002.

### Epic A — Band Registry Design *(design-only; no implementation)*

**Output:** Band registry schema spec, initial registry population for core optical and
UV bands, ADR-017.

**Dependency:** None. This is the starting point for all downstream work.

**Scope:** Define the registry entry schema (including the `excluded` flag and alias
set); identify the authoritative external source for each regime (SVO FPS for
optical/UV/NIR via `astroquery.svo_fps`; instrument documentation for X-ray/radio/gamma);
populate an initial registry covering Johnson-Cousins, Sloan, Swift/UVOT, 2MASS, GALEX;
fold `excluded_filters.json` into the registry; define the operator mechanism for adding
new excluded entries; define the out-of-band SVO FPS synchronization pattern and the
bundled Lambda artifact format; define versioning strategy.

### Epic B — Band Resolution Algorithm Design *(design-only; no implementation)*

**Output:** Complete disambiguation algorithm spec (decision table or flowchart), ADR-018.

**Dependency:** Epic A (requires the registry to be defined before the algorithm can
reference it).

**Scope:** Formalize the ambiguity taxonomy from §5 Layer 2; specify the full resolution
procedure for each case; define the escalation policy for unresolvable ambiguities;
document the test cases that the algorithm must pass.

### Epic C — Photometry Table Model Revision

**Output:** `photometry_table_model.md` v2.0, updated `entities.py` (enums + `PhotometryRow`),
ADR-019.

**Dependency:** Epic A (filter registry schema informs what fields `PhotometryRow` needs).

**Scope:** Redesign `PhotSystem`; extend `SpectralCoordUnit` and `FluxDensityUnit`;
review upper-limit semantics; audit cross-field invariants for correctness across all
regimes; bump the schema version.

### Epic D — Adapter Revision

**Output:** Revised `CanonicalCsvAdapter`, updated `synonyms.json`, Tier 3 (UCD-based
mapping) specification and implementation, removal of all module-level band knowledge
dicts from application code, error handling taxonomy and failure record schema.

**Dependency:** Epics A, B, C.

**Scope:** Replace `_resolve_band()` with registry-backed lookup; remove `_COMBINED_BAND_LOOKUP`,
`_DEFAULT_PHOT_SYSTEM`, `_PHOT_SYSTEM_SPECTRAL_META`; fold `excluded_filters.json` into
the registry; implement Tier 3 UCD-based column mapping; update the adapter to target
the revised `PhotometryRow` schema; design and document the full error taxonomy.

### Epic E — Ingestion Workflow Implementation

**Output:** All Lambda handlers for `ingest_photometry`, handler boundary schemas,
ADR-020 (persistence format), updated Step Functions definition.

**Dependency:** Epics C, D (requires stable schema and working adapter).

**Scope:** Specify and implement StagePhotometry, ValidatePhotometry, PersistPhotometry,
FinalizeIngestion; resolve Open Question 1 (storage format) via ADR-020; implement
idempotency check against file SHA-256; implement quarantine threshold and diagnostics
write.

### Epic F — Test Suite

**Output:** Comprehensive test coverage for the adapter, registry lookups, and workflow
handlers.

**Dependency:** Epics A–E.

**Scope:** Unit tests for all `CanonicalCsvAdapter` pipeline stages; parametric tests
covering each ambiguity case from the taxonomy; integration smoke tests for the full
ingestion workflow against real nova data.

---

## 8. Relationship to Existing Documents

| Document | Status after this design | Action |
|---|---|---|
| `ADR-015` | Partially superseded. Decisions 1 (staging), 3 (idempotency key), 4 (row deduplication), 5 (file size guard), and the Tier 1–4 column mapping strategy remain valid. Decision 2's implementation details are superseded by Epic D. | Amend status to `Superseded-in-part`; annotate with reference to this document and successor ADRs. |
| `ADR-016` | Fully superseded. The band resolution strategy it defines is replaced by the band registry (ADR-017) and disambiguation algorithm (ADR-018). | Amend status to `Superseded`; annotate accordingly. |
| `photometry_table_model.md` v1.1 | Superseded by the revised model produced in Epic C. | Retain as historical reference; version to v2.0 in Epic C. |
| `entities.py` | Under revision in Epic C (enums) and Epic D (adapter-adjacent changes). | No action now; Epic C is the revision vehicle. |
| `docs/research/astro_software_report.tex` | Ecosystem survey supporting §3. | Retain as reference; no action required. |
| `docs/research/deep-research-report.md` | Ecosystem survey supporting §3. | Retain as reference; no action required. |
| `DESIGN-002` *(forthcoming)* | Covers `ColorRow` / derived quantities, the sidecar metadata model, provenance framework, and donation workflow context. Layer 0 of this design depends on DESIGN-002 for the full sidecar contract. | Author in a future conversation; Epic A-0 produces a stub in the interim. |

---

## 9. What This Document Is Not

This document does not make binding architectural decisions. It does not specify the
band registry schema, the disambiguation algorithm, the revised `PhotSystem` model, or
the storage format. Each of those decisions will be made in a dedicated ADR at the
appropriate time, with the benefit of the full design context this document establishes.

The current `CanonicalCsvAdapter` implementation and the existing ADRs remain the
operative artifacts until their successor ADRs are written and adopted. The code is not
broken; it is a placeholder that will be methodically replaced.
