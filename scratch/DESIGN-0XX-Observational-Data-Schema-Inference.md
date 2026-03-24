# DESIGN-0XX: Observational Data Schema Inference & Setup Resolution Pipeline

**Status:** Draft
**Date:** 2026-03-23
**Author:** TF
**Document class:** Design / Scoping (feeds future ADRs; does not itself constitute decisions)

---

## 1. Executive Summary

This document outlines the design space for a generalized ingestion intelligence layer
capable of interpreting heterogeneous astronomical data files and mapping them into
NovaCat’s canonical data models. The system addresses two tightly coupled problems:

1. **Schema inference** — determining the semantic meaning of columns in arbitrary input files.
2. **Photometric setup resolution** — determining the observational context (instrument,
   telescope, filter, regime) associated with each measurement.

The system is designed as a **proposal-and-review engine**, not a fully autonomous
decision-maker. Its primary goal is to generate high-quality candidate interpretations
with transparent rationale, which are then validated by human review and fed into a
persistent registry to improve future ingestion.

This document defines:
- the inference pipeline
- the ontology underlying semantic interpretation
- the registry and learning model
- the relationship to existing ingestion layers

It does not define concrete algorithms or schemas; those will be specified in future ADRs.

---

## 2. Motivation

### 2.1 The schema inference problem is unsolved

Astronomical data files are highly heterogeneous:
- inconsistent column naming (`mag`, `Mag`, `magnitude`, `Vmag`, `F4.8GHz`)
- varying structure (long vs wide, single vs multi-target)
- missing or implicit metadata
- multi-regime ambiguity (optical vs radio vs X-ray)

No existing system provides a robust, general solution for mapping arbitrary tables
into a canonical schema across all regimes.

### 2.2 The setup resolution problem is context-dependent

Filter identity and observational setup are rarely explicit:
- `"V"` may refer to multiple systems
- `"K"` is ambiguous across optical/NIR/radio
- instrument/telescope may only appear in filenames or headers

Resolution requires integrating:
- column-level signals
- file-level context
- source priors
- registry knowledge

### 2.3 The system must learn over time

Because:
- ingestion is heterogeneous
- edge cases are inevitable
- human review is already required

The system must act as a **bootstrap mechanism for a growing knowledge base** rather
than a static rules engine.

---

## 3. Design Philosophy

### 3.1 Proposal-first, validation-second

- One strong signal is sufficient to generate a proposal
- Multiple signals increase confidence
- Ambiguity is preserved, not forced
- Human review is part of the system

### 3.2 Evidence-driven inference

All decisions must be accompanied by:
- explicit evidence
- scoring rationale
- alternative candidates

### 3.3 Registry-backed learning

Approved interpretations are stored as reusable patterns:
- schema patterns
- alias mappings
- setup hints

Future inference is accelerated by past decisions.

### 3.4 Graph-oriented conceptual model

The system is fundamentally relational:
- files, columns, instruments, filters, schemas are interconnected
- inference operates over relationships

Implementation may begin without a graph database, but the ontology is graph-shaped.

---

## 4. System Architecture

The inference system is a layered pipeline:

```text
┌──────────────────────────────────────────────┐
│ Final Output │
│ Canonical schema + setup resolution │
├──────────────────────────────────────────────┤
│ Stage 7 — Review & Registry Update │
├──────────────────────────────────────────────┤
│ Stage 6 — Setup / Filter Resolution │
├──────────────────────────────────────────────┤
│ Stage 5 — Global Schema Resolution │
├──────────────────────────────────────────────┤
│ Stage 4 — Candidate Generation │
├──────────────────────────────────────────────┤
│ Stage 3 — Column Profiling │
├──────────────────────────────────────────────┤
│ Stage 2 — File Classification │
├──────────────────────────────────────────────┤
│ Stage 1 — Context Extraction │
└──────────────────────────────────────────────┘
```


---

## 5. Pipeline Stages

### Stage 1 — Context Extraction

Extract all available information:
- table(s)
- column names
- header/preamble text
- filename
- source/provider
- sidecar metadata

Output: `FileContext`

---

### Stage 2 — File Classification

Classify file properties:
- photometry vs spectra vs mixed
- long vs wide
- single vs multi-target
- regime (optical, radio, etc.)

Outputs are probabilistic and evidence-backed.

---

### Stage 3 — Column Profiling

For each column:
- normalize name
- infer datatype
- detect units
- analyze value distribution

Output: `ColumnProfile`

---

### Stage 4 — Candidate Generation

Generate possible semantic roles for each column:
- time, magnitude, flux, error, filter, etc.

Each candidate includes:
- score
- evidence
- rationale

---

### Stage 5 — Global Schema Resolution

Construct coherent schema interpretations:
- enforce structural consistency
- detect wide-format patterns
- identify color vs photometry vs spectra

Output:
- best schema
- alternative schemas (if ambiguity remains)

---

### Stage 6 — Setup / Filter Resolution

Infer observational context:
- instrument
- telescope
- filter family
- SVO filter (if possible)

Resolution levels:
- exact
- likely
- family-only
- unknown
- exempt (AAVSO, non-optical)

---

### Stage 7 — Review & Registry Update

Human reviewer:
- approves or edits schema
- confirms setup resolution
- flags unresolved ambiguity

System stores:
- approved schema patterns
- alias mappings
- setup hints

---

## 6. Ontology

### 6.1 Semantic field hierarchy

Fields are hierarchical:
- `time.mjd`, `time.jd`
- `measurement.mag`, `measurement.flux`
- `measurement.color_index`
- `band_or_filter`
- `instrument`, `telescope`

### 6.2 Setup ontology

Entities:
- instrument
- telescope
- facility
- filter family
- filter
- SVO filter

### 6.3 Evidence model

All inference is supported by:
- alias matches
- unit detection
- value patterns
- contextual text
- registry matches

---

## 7. Registry Model

### 7.1 Schema patterns

Approved mappings from columns → semantic roles.

### 7.2 Setup hints

Mappings from context terms → likely instrument/filter.

### 7.3 Alias mappings

Synonym expansion for column identification.

---

## 8. Key Design Questions

| # | Question | Future ADR |
|---|---|---|
| 1 | What is the full semantic field taxonomy? | ADR-XXX |
| 2 | How are candidate scores computed and combined? | ADR-XXX |
| 3 | How are conflicting schema interpretations ranked? | ADR-XXX |
| 4 | What is the registry storage format and access pattern? | ADR-XXX |
| 5 | How is similarity to prior schemas computed? | ADR-XXX |
| 6 | When does the system abstain vs propose? | ADR-XXX |
| 7 | How is setup resolution integrated with band registry? | ADR-XXX |
| 8 | What is the review interface and workflow? | ADR-XXX |

---

## 9. What This Document Is Not

This document:
- does not define implementation details
- does not specify scoring algorithms
- does not define storage schemas

It defines the conceptual architecture and roadmap for future ADRs.
