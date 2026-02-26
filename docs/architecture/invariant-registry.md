# Invariant Registry v1
*(Architectural Stability Artifact)*

## 0. Purpose

The Invariant Registry defines the system’s **non-negotiable architectural truths**.

An invariant is:

> A property of the system that must always remain true unless explicitly changed via ADR.

This document exists to:

- Prevent architectural drift
- Enable safe parallel implementation
- Support LLM-facilitated development
- Make merge gates objective
- Distinguish flexible design from fixed guarantees

If a change violates an invariant, it requires:

- Explicit ADR
- Facilitator approval
- Updated enforcement tests

---

## 1. Invariant Classification Levels

Each invariant is assigned an enforcement level:

### L1 — Hard Invariant (Mechanically Enforced)
Must be verified by tests, static analysis, or CI gates.

### L2 — Guarded Invariant (Test + Review)
Partially enforced mechanically; also reviewed.

### L3 — Governance Invariant (Review-Enforced)
Not directly testable; enforced via PR review.

---

## 2. Invariant Entry Template

Each invariant is defined as follows:

- **Invariant ID:** `INV-###`
- **Statement:** (Precise description of invariant)
- **Category:** (Identity / Persistence / Workflow / Observability / Error / Governance)
- **Level:** (L1 / L2 / L3)
- **Rationale:** (Why it exists)
- **Enforcement Mechanism:** (Tests / static scan / PR review / etc.)
- **Violation Handling:** (ADR required? Version bump? Migration?)
- **Owner:** (Surface owner or facilitator)

---

# 3. Identity Invariants

---

## INV-001 — UUID Primary Identity

- **Category:** Identity
- **Level:** L1
- **Statement:** All canonical entities are identified by system-generated UUIDs.
- **Rationale:** Prevents ambiguity, ensures stable cross-surface references.
- **Enforcement Mechanism:** Schema validation + entity construction tests.
- **Violation Handling:** Requires ADR.
- **Owner:** Identity Surface Owner

---

## INV-002 — Deterministic Locator Identity

- **Category:** Identity
- **Level:** L1
- **Statement:** Locator records must derive their identity deterministically from the provider plus a canonical `locator_identity`, producing a stable key of the form:
  `LOCATOR#<provider>#<locator_identity>`

  The `locator_identity` must be computed using the following precedence:

  1. **Preferred:** `provider_product_id:<id>` (where `provider_product_id` is the provider's native product identifier, if present)
  2. **Fallback:** `url:<normalized_url>` (where `normalized_url` is a canonicalized/normalized URL)

- **Rationale:** Ensures stable deduplication and aliasing even when providers do not supply native product IDs.
- **Enforcement Mechanism:** Persistence wrapper tests verifying:
  - correct precedence selection when `provider_product_id` exists
  - deterministic URL normalization and key formation when it does not
- **Violation Handling:** Requires ADR.
- **Owner:** Persistence Surface Owner

---

# 4. Persistence Invariants

---

## INV-010 — Single-Table Model

- **Category:** Persistence
- **Level:** L2
- **Statement:** The system uses a single-table persistence model.
- **Rationale:** Simplifies access patterns and identity partitioning.
- **Enforcement Mechanism:** Architectural review.
- **Violation Handling:** ADR required.
- **Owner:** Persistence Surface Owner

---

## INV-011 — Wrapper-Only Persistence Access

- **Category:** Persistence
- **Level:** L1
- **Statement:** All database interactions must occur through the Persistence Wrapper API.
- **Rationale:** Centralizes invariant enforcement and reduces drift.
- **Enforcement Mechanism:** Static scan / allowlist + PR checklist.
- **Violation Handling:** Merge blocked.
- **Owner:** Root Facilitator

---

## INV-012 — Idempotency Enforcement at Workflow Level

- **Category:** Persistence
- **Level:** L1
- **Statement:** Workflow idempotency must be enforced via JobRun uniqueness and conditional writes.
- **Rationale:** Prevents duplicate execution side effects.
- **Enforcement Mechanism:** Persistence tests verifying conditional write behavior.
- **Violation Handling:** ADR required.
- **Owner:** Workflow Surface Owner

---

# 5. Workflow Invariants

---

## INV-020 — Explicit Failure Classification

- **Category:** Workflow
- **Level:** L1
- **Statement:** All workflow failures must be classified as RETRYABLE, TERMINAL, or QUARANTINE.
- **Rationale:** Ensures deterministic Step Function behavior.
- **Enforcement Mechanism:** Error classification mapping tests.
- **Violation Handling:** Merge blocked.
- **Owner:** Workflow Surface Owner

---

## INV-021 — Minimal State Transitions

- **Category:** Workflow
- **Level:** L3
- **Statement:** Step Function definitions must minimize unnecessary branching and state transitions.
- **Rationale:** Controls cost and complexity.
- **Enforcement Mechanism:** PR review.
- **Violation Handling:** Facilitator review required.
- **Owner:** Workflow Surface Owner

---

# 6. Observability Invariants

---

## INV-030 — Mandatory Structured Log Fields

- **Category:** Observability
- **Level:** L1
- **Statement:** All structured logs must include:
  - workflow_name
  - job_run_id
  - correlation_id
  - classification
  - error_type (if applicable)
- **Rationale:** Enables deterministic debugging across distributed execution.
- **Enforcement Mechanism:** Golden log tests.
- **Violation Handling:** Merge blocked.
- **Owner:** Observability Surface Owner

---

## INV-031 — Correlation ID Required Internally

- **Category:** Observability
- **Level:** L1
- **Statement:** correlation_id must always exist after workflow entry; if not provided, it must be minted.
- **Rationale:** Ensures traceability across workflows.
- **Enforcement Mechanism:** Workflow entry tests.
- **Violation Handling:** Merge blocked.
- **Owner:** Observability Surface Owner

---

# 7. Error Handling Invariants

---

## INV-040 — Canonical Error Taxonomy

- **Category:** Error
- **Level:** L1
- **Statement:** Error classification must use the shared error taxonomy and not ad hoc string literals.
- **Rationale:** Prevents retry logic drift.
- **Enforcement Mechanism:** Static analysis + classification tests.
- **Violation Handling:** Merge blocked.
- **Owner:** Workflow Surface Owner

---

# 8. Contract Evolution Invariants

---

## INV-050 — Breaking Changes Require Version Bump

- **Category:** Governance
- **Level:** L2
- **Statement:** Removing fields, renaming fields, tightening validation, or incompatible type changes require MAJOR version bump.
- **Rationale:** Preserves contract stability.
- **Enforcement Mechanism:** Compatibility tests + PR review.
- **Violation Handling:** ADR + version increment required.
- **Owner:** Root Facilitator

---

## INV-051 — Optional Additions Are Backward Compatible

- **Category:** Governance
- **Level:** L2
- **Statement:** Adding optional fields is considered backward compatible unless consumers strictly validate enums or types.
- **Rationale:** Allows safe evolution.
- **Enforcement Mechanism:** Schema compatibility tests.
- **Violation Handling:** Review + test update required.
- **Owner:** Root Facilitator

---

# 9. Change Control

Modifying an invariant requires:

1. ADR documenting the rationale.
2. Update to this registry.
3. Update to enforcement tests.
4. Facilitator approval.

Invariants may not be implicitly altered.

---

# 10. Relationship to Other Documents

- Wrapper Charter — defines structural choke points
- Public Interface Catalog — defines what is publicly depended upon
- Architecture Snapshot — defines system shape
- ADRs — document intentional changes

---
