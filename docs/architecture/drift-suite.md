# Drift Suite v1
*(Architectural Drift Detection + Merge Gate Model)*

## 0. Purpose

The Drift Suite is a collection of automated checks designed to detect when the implementation has diverged from the architecture’s intended reality.

The Drift Suite exists to:

- Detect architectural entropy early (especially under LLM-assisted development)
- Provide objective merge gates for invariants and public interfaces
- Prevent “silent” breaking changes to contracts, wrappers, and conventions
- Reduce integration friction by failing fast and locally

**Key principle:**
Drift detection should bias toward **false positives early** rather than false negatives.

---

## 1. Scope

The Drift Suite targets high-risk surfaces:

1. **Contracts** (schemas, event shapes, compatibility)
2. **Wrappers** (persistence, observability, error classification choke points)
3. **Invariants** (identity, idempotency, classification, correlation)
4. **Workflow semantics** (definition-level constraints, cost-conscious structure)
5. **Dependency boundaries** (prevent bypasses and “just this one import” creep)

---

## 2. Suite Structure (Fast vs Slow)

The Drift Suite is divided into:

### 2.1 Fast Drift Checks (Local + Pre-commit eligible)
Goal: complete in seconds to low minutes.

Typical members:
- lint / format
- static scans for forbidden imports or bypass patterns
- schema generation + snapshot checks
- wrapper API surface checks

### 2.2 Slow Drift Checks (CI-only or nightly)
Goal: deeper semantics and integration confidence.

Typical members:
- contract compatibility matrix checks
- workflow definition assertions (state machine constraints)
- integration tests against local AWS emulators (if used)
- golden telemetry validations across end-to-end paths

---

## 3. Drift Artifacts (What We Snapshot)

Snapshots are the core drift mechanism: store “known-good” canonical outputs and fail if they change unexpectedly.

### 3.1 Contract Snapshots
- Canonical JSON Schemas for public contracts (events/entities/external APIs)
- Canonical fixtures for representative payloads

**Rule:** Any change to a contract snapshot must be accompanied by:
- version bump (if required by compatibility policy)
- updated compatibility tests
- updated fixtures (if applicable)

### 3.2 Workflow Definition Snapshots (Recommended)
- Canonical rendered Step Function definitions (or normalized state machine JSON)

**Rule:** Any change must be intentional and reviewed for cost/state-transition impact.

### 3.3 Public Wrapper API Surface Snapshots
- Snapshot of wrapper function signatures (public internal APIs)
- Snapshot of required log fields list (observability wrapper)

**Rule:** Wrapper APIs are stable by default; breaking changes require ADR.

---

## 4. Drift Checks (What We Enforce)

This section describes what must be mechanically checked.

### 4.1 Contract Compatibility Checks
Verify evolution rules for each public contract category:

- Add optional fields: allowed as MINOR/PATCH
- Tighten validation rejecting previously valid payloads: MAJOR
- Remove/rename/change meaning: MAJOR
- Enum expansion: allowed only if consumers tolerate unknown values, otherwise MAJOR

**Output:** A clear failure message indicating:
- which contract changed
- what rule was violated
- what version bump is required (if any)

### 4.2 Wrapper Choke-Point Enforcement
Verify that protected surfaces cannot be bypassed:

- Persistence: no direct DB client usage outside persistence wrapper modules
- Observability: no ad hoc structured logging outside observability wrapper
- Error classification: no ad hoc classification strings outside shared taxonomy

**Mechanisms (examples):**
- import allowlist/denylist
- static scan for forbidden modules
- review gate checklist (backstop)

### 4.3 Invariant Enforcement Tests (Behavioral)
Each L1 invariant must have at least one mechanical enforcement check.

Minimum recommended L1 checks:
- Deterministic locator identity computation (precedence + normalization)
- Correlation ID required after workflow entry (mint if absent)
- Failure classification always present and one of {RETRYABLE, TERMINAL, QUARANTINE}
- Mandatory structured log fields always emitted via wrapper
- Idempotency locking behavior (at-most-once side effects per idempotency key)

### 4.4 Golden Telemetry Checks
Validate that structured logs include required fields and are shape-consistent.

Approach:
- Provide fixtures and/or test harness that captures emitted log records
- Validate required fields exist
- Validate classification/error_type formatting when errors occur

**Note:** Golden telemetry checks validate *shape*, not full content.

### 4.5 Dependency Drift Checks
Prevent dependency creep that increases operational complexity and context cost.

Enforce:
- a small allowlist of approved libraries per runtime layer
- no new heavyweight dependencies without facilitator review

---

## 5. Merge Gates

A PR may merge only if:

1. Fast drift suite passes
2. Relevant slow drift checks pass (or are explicitly waived by facilitator)
3. Any changes to public interfaces are reflected in:
   - Public Interface Catalog entry updates
   - contract version bumps (if required)
   - updated snapshots/fixtures
4. Any invariant changes include:
   - ADR
   - registry update
   - enforcement test update

---

## 6. Execution Model

Recommended checks by stage:

### Pre-commit / Local “Quick Gate”
- lint/format/type checks (as configured)
- static bypass scans (wrappers)
- contract schema generation + snapshot verification (public only)

### PR CI “Standard Gate”
- everything in quick gate
- contract compatibility tests
- wrapper API surface snapshot checks
- invariant enforcement tests (L1)

### Nightly / Optional “Deep Gate”
- workflow definition snapshots + cost checks
- integration tests / end-to-end smoke tests
- golden telemetry in end-to-end context (if applicable)

---

## 7. Failure Reporting Requirements

Drift Suite failures must be:

- Specific (exact file/interface/invariant identified)
- Actionable (what to change / version bump required)
- Minimal (avoid cascading failures from one root cause)

A good failure message answers:
- What drifted?
- Why is it forbidden?
- What is the correct fix path (bump version / update snapshot / add ADR / etc.)?

---

## 8. Governance

- Drift Suite definitions are owned by the Root Facilitator.
- Adding a new drift check requires:
  - rationale
  - expected false positive profile
  - runtime cost estimate (fast vs slow)
- Removing or weakening a drift check requires an ADR.

---

## 9. Relationship to Other Artifacts

- Wrapper Charter: defines the choke points the drift suite protects
- Public Interface Catalog: defines what is public and must be snapshotted
- Invariant Registry: defines what must be mechanically enforced
- ADRs: the only sanctioned way to change invariants or break interfaces

---
