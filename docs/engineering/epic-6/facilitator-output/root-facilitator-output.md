# Root Facilitator Bootstrap Output
Nova Cat — Single-Root Orchestration Pass
Status: Bootstrap Complete

---

# 1. Bootstrap Sufficiency Evaluation

After full ingestion of:

- Architecture Snapshot
- Wrapper Charter
- Invariant Registry
- Drift Suite
- Public Interface Catalog
- Execution Governance
- DynamoDB Model + Access Patterns
- S3 Layout
- All Workflow Specifications

Conclusion:

A single-root orchestration pass is sufficient.

Signals:

- Domain scope is singular (classical nova ingestion).
- Persistence model is single-table and cohesive.
- Workflow semantics share strong structural symmetry.
- Cross-cutting concerns are wrapper-guarded.
- No domain requiring separate meta-domain roots detected.
- Context volume is large but partitionable by concern strata.

Meta-root escalation not required.

---

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

---

# 3. Context Packets

Each packet is self-contained and bounded.
Surface Facilitators receive only their packet + full Surface Map.

---

# S1 — Identity & Locator Authority
## Context Packet

Surface-Specific Invariants:

INV-001
All canonical entities are identified by system-generated UUIDs.
Names are never used downstream of initialize_nova.

INV-002
Locator identity must be deterministic.

Precedence:

1. provider_product_id:<id>
2. url:<normalized_url>

Key form:

LOCATOR#<provider>#<locator_identity>

Workflow-Specific Extracted Strata:

From initialize_nova:

- NAME# partition lookup
- Coordinate threshold logic (<2", 2–10", >10")
- Alias upsert rules

From discover_spectra_products:

- identity_strategy: NATIVE_ID | METADATA_KEY | WEAK
- LocatorAlias enforcement

From acquire_and_validate_spectra:

- Byte-level duplicate detection
- duplicate_of_data_product_id marking
- UUID stability requirement

Contract Constraints:

- Only UUIDs published downstream
- idempotency_key never part of payload
- Name normalization must be deterministic

Forbidden Cross-Surface Operations:

- May not introduce DB access outside Persistence Wrapper
- May not redefine error taxonomy
- May not modify workflow state transitions

---

# S2 — Persistence & Data Modeling
## Context Packet

Surface-Specific Invariants:

INV-010
Single-table model only.

INV-011
All DB interactions through Persistence Wrapper.

INV-012
Workflow idempotency enforced via conditional writes + JobRun uniqueness.

Workflow Extracted Strata:

Common across workflows:

- BeginJobRun → conditional write
- AcquireIdempotencyLock → conditional write
- JobRun and Attempt item structure
- Eligibility index shape:
  GSI1PK = nova_id
  GSI1SK = ELIG#<eligibility>#SPECTRA#...
- Photometry singleton rule:
  SK = PRODUCT#PHOTOMETRY_TABLE

S3 Layout Constraints:

- Raw bytes immutable
- Photometry canonical overwrite rule
- Snapshot only on schema boundary

Forbidden Cross-Surface Operations:

- No direct DB client calls
- No workflow branching logic modifications
- No identity normalization logic changes

---

# S3 — Workflow Orchestration & Semantics
## Context Packet

Surface-Specific Invariants:

INV-020
Every failure classified: RETRYABLE | TERMINAL | QUARANTINE.

INV-021
Minimal state transitions.

Shared Semantic Strata:

- EnsureCorrelationId
- BeginJobRun
- AcquireIdempotencyLock
- Explicit Retry policies
- Cooldown enforcement via next_eligible_attempt_at
- Idempotency key internal-only

Failure Routing:

Retryable → Retry with backoff
Terminal → FinalizeJobRunFailed
Quarantine → QuarantineHandler → SNS best-effort notification

Forbidden Cross-Surface Operations:

- No schema evolution
- No persistence model changes
- No wrapper bypass

---

# S4 — Validation & Scientific Normalization
## Context Packet

Responsibilities Extracted:

- FITS profile selection
- Header signature hash
- Content fingerprint SHA-256
- Duplicate detection post-acquisition
- Quarantine gating for:
  - UNKNOWN_PROFILE
  - MISSING_CRITICAL_METADATA
  - INVALID_UNITS

Scientific vs Operational Separation:

Scientific:

- validation_status
- acquisition_status

Operational:

- last_attempt_outcome
- retry classification

Scientific enums must not encode retryability.

Forbidden:

- No modification of UUID identity
- No persistence writes outside wrapper
- No introduction of dataset abstraction

---

# S5 — Observability & Telemetry
## Context Packet

Surface-Specific Invariants:

INV-030
Required log fields:

- workflow_name
- job_run_id
- correlation_id
- classification
- error_type

INV-031
correlation_id must exist post-entry.

Required Fields (Unified):

All logs must include:

- execution_arn
- state_name
- attempt_number
- primary UUID(s)

Forbidden:

- No raw logging calls
- No omission of mandatory fields

---

# S6 — Governance & Drift Enforcement
## Context Packet

Enforcement Areas:

- Contract schema snapshots
- Wrapper API surface snapshots
- Workflow definition snapshots
- Static scan for forbidden imports
- Golden telemetry tests

Merge Gate Rules:

- Invariant change → ADR required
- Breaking change → Version bump
- Wrapper API change → Snapshot update + approval

---

# 4. Surface Facilitator Bootstrap Prompts

Each Surface Facilitator receives:

- Surface Map (read-only)
- Its Context Packet

Authority Boundaries:

- Cannot change invariants
- Cannot change public interfaces without escalation
- Must escalate cross-surface redesign

Escalation Protocol:

- Halt
- Produce conflict analysis
- Recommend ADR if required

Merge Expectations:

- Drift Suite passes
- Wrapper Charter preserved
- No invariant violation

---

# 5. Global Implementation Ladder

This is a maturation sequence.

Phase 1 — Enforcement Primitives

- Implement Persistence Wrapper surface freeze
- Implement Observability Wrapper freeze
- Implement Error Classification choke point
- Implement idempotency conditional write enforcement
- Implement static scan for wrapper bypass

Phase 2 — Invariant Mechanization

- Deterministic locator identity tests
- Correlation ID propagation tests
- Cooldown enforcement tests
- Duplicate fingerprint enforcement tests

Phase 3 — Contract Stabilization

- Snapshot JSON schemas
- Snapshot wrapper API surfaces
- Add compatibility matrix tests

Phase 4 — Workflow Hardening

- Snapshot Step Function definitions
- Verify minimal branching
- Harden quarantine SNS notifications

Phase 5 — Scientific Validation Hardening

- Profile selection coverage tests
- Byte-level duplicate enforcement tests
- Sanity validation regression corpus

Phase 6 — Observability Hardening

- Golden telemetry fixture tests
- Metrics shape enforcement
- Alarm threshold baselining

---

# 6. Initial Task Queue (Phase 1 Slices)

---

## Slice 1 — Persistence Wrapper Freeze

Primary Surface: S2
Secondary Surface: S6
Invariants: INV-010, INV-011

Acceptance Criteria:

- Static scan blocks direct DB client usage
- Wrapper API snapshot committed
- Tests fail if bypass introduced

---

## Slice 2 — Observability Wrapper Enforcement

Primary Surface: S5
Secondary Surface: S6
Invariants: INV-030, INV-031

Acceptance Criteria:

- Golden log test verifies mandatory fields
- correlation_id minted if missing
- Raw logger calls fail scan

---

## Slice 3 — Error Taxonomy Enforcement

Primary Surface: S3
Secondary Surface: S6
Invariants: INV-020, INV-040

Acceptance Criteria:

- All workflows classify errors via shared helper
- No string literals allowed
- Classification completeness test passes

---

## Slice 4 — Deterministic Locator Identity Tests

Primary Surface: S1
Secondary Surface: S2
Invariants: INV-002

Acceptance Criteria:

- provider_product_id precedence enforced
- URL normalization deterministic
- Same input always yields same key

---

## Slice 5 — Idempotency Conditional Write Enforcement

Primary Surface: S2
Secondary Surface: S3
Invariants: INV-012

Acceptance Criteria:

- Duplicate BeginJobRun prevented
- Duplicate AcquireIdempotencyLock prevented
- Tests simulate concurrent invocation

---

## Slice 6 — Cooldown Enforcement Gate

Primary Surface: S3
Secondary Surface: S2
Invariants: INV-020

Acceptance Criteria:

- next_eligible_attempt_at blocks execution
- Backoff increases with attempt_count
- SKIPPED_BACKOFF outcome recorded

---

# 7. Exit Declaration

- Surface Map defined
- Context Packets bounded and self-contained
- Surface Facilitator bootstraps prepared
- Implementation Ladder defined
- Initial Task Queue generated

Root Facilitator authority is now relinquished.

Surface Facilitators may begin execution under protocol.

Bootstrap orchestration complete.
