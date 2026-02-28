# 2. Surface Map

All Surface Facilitators receive this full map (read-only awareness).

---

## Surface S1 — Identity & Locator Authority

Description:
Canonical identity formation, name normalization, locator identity normalization, deterministic UUID derivation, identity quarantine thresholds.

Primary Responsibilities:

- Enforce UUID-first identity (downstream of name boundary)
- Implement deterministic locator identity ladder
- Maintain NAME# and LOCATOR# partitions
- Coordinate duplicate detection thresholds (<2", 2–10", >10")
- Provide identity utilities to workflows
- Preserve UUID stability under alias updates

Invariants:

- INV-001 — UUID Primary Identity
- INV-002 — Deterministic Locator Identity

Public Interface IDs:

- IFC-020 — Persisted Entity Schema
- IFC-050 — Error Taxonomy + Classification Surface

Wrapper Dependencies:

- Persistence Wrapper
- Error Classification Wrapper

Stability Level:
Stabilizing

---

## Surface S2 — Persistence & Data Modeling

Description:
Single-table DynamoDB modeling, item shapes, GSIs, S3 pointer integrity, conditional write enforcement.

Primary Responsibilities:

- Enforce single-table model
- Enforce wrapper-only persistence access
- Enforce idempotency conditional writes
- Maintain eligibility index correctness
- Maintain product-type-first sort ordering
- Maintain photometry singleton-per-nova rule
- Maintain schema_version evolution safety

Invariants:

- INV-010 — Single-Table Model
- INV-011 — Wrapper-Only Persistence Access
- INV-012 — Workflow Idempotency Enforcement

Public Interface IDs:

- IFC-030 — Persistence Wrapper API Surface
- IFC-020 — Persisted Entity Schemas

Wrapper Dependencies:

- Persistence Wrapper (primary)
- Observability Wrapper (secondary logging)

Stability Level:
Stabilizing

---

## Surface S3 — Workflow Orchestration & Semantics

Description:
Step Functions orchestration semantics, idempotency keys, cooldown/backoff behavior, classification routing, parallel launch coordination.

Primary Responsibilities:

- Maintain minimal state transitions
- Enforce explicit failure classification
- Enforce cooldown semantics
- Enforce JobRun / Attempt lifecycle emissions
- Maintain idempotency key internal-only rule
- Maintain time-bucketed idempotency where required

Invariants:

- INV-020 — Explicit Failure Classification
- INV-021 — Minimal State Transitions

Public Interface IDs:

- IFC-010 — Workflow Entry Payloads
- IFC-050 — Error Classification Surface

Wrapper Dependencies:

- Persistence Wrapper
- Observability Wrapper
- Error Classification Wrapper

Stability Level:
Plastic → Stabilizing

---

## Surface S4 — Validation & Scientific Normalization

Description:
Profile-driven FITS validation, byte-level duplicate detection, fingerprinting, quarantine gating.

Primary Responsibilities:

- Profile selection rules
- Header signature hashing
- Byte-level fingerprint generation
- Duplicate-by-fingerprint resolution
- Quarantine classification triggers
- Preserve separation of scientific vs operational state

Invariants:

- INV-020 (classification compliance)
- INV-040 (canonical error taxonomy usage)

Public Interface IDs:

- IFC-050 — Error Taxonomy Surface
- IFC-020 — Persisted DataProduct Schema

Wrapper Dependencies:

- Persistence Wrapper
- Error Classification Wrapper
- Observability Wrapper

Stability Level:
Plastic

---

## Surface S5 — Observability & Telemetry

Description:
Structured logging enforcement, correlation propagation, golden telemetry shape enforcement.

Primary Responsibilities:

- Enforce mandatory structured log fields
- Correlation ID minting and propagation
- JobRun/Attempt log consistency
- Metric shape consistency
- Golden telemetry tests

Invariants:

- INV-030 — Mandatory Structured Log Fields
- INV-031 — Correlation ID Required Internally

Public Interface IDs:

- IFC-040 — Observability Wrapper API Surface

Wrapper Dependencies:

- Observability Wrapper (primary)

Stability Level:
Stabilizing

---

## Surface S6 — Governance & Drift Enforcement

Description:
Contract evolution control, snapshot enforcement, wrapper freeze gates, invariant enforcement tests.

Primary Responsibilities:

- Maintain Drift Suite definitions
- Enforce contract compatibility
- Maintain wrapper API surface snapshots
- Maintain workflow definition snapshots
- Enforce invariant test coverage

Invariants:

- INV-050 — Breaking Changes Require Version Bump
- INV-051 — Optional Additions Are Backward Compatible

Public Interface IDs:

- IFC-020 — Persisted Schemas
- IFC-030 — Persistence Wrapper
- IFC-040 — Observability Wrapper
- IFC-050 — Error Classification Surface

Wrapper Dependencies:

- All wrappers (governance oversight)

Stability Level:
Frozen (policy-level)
