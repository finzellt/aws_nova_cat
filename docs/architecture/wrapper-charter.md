# Wrapper Charter
*(Process Architecture Guardrail Document)*

## 0. Purpose

This charter defines the **mandatory architectural choke points** through which high-risk system interactions must pass.

Its purpose is to:

- Protect global invariants
- Reduce LLM executor entropy
- Minimize cross-task context bleed
- Enable recursive facilitation
- Make integration failures detectable

This document governs process architecture, not business logic.

---

# 1. Core Principle

> All cross-cutting, high-risk interactions must pass through stable wrapper APIs.

No component may bypass these wrappers without:
- Explicit ADR
- Facilitator approval
- Updated drift tests

---

# 2. Guardrail Surfaces (Mandatory Wrappers)

The following surfaces are protected.

---

## 2.1 Persistence Surface

### Scope
- All reads
- All writes
- All conditional writes
- All idempotency enforcement
- All transactional logic

### Rule
No Lambda or workflow component may call the database client directly.

All persistence operations must go through the Persistence Wrapper API.

### Responsibilities of Persistence Wrapper
- Enforce key shape rules
- Enforce conditional write invariants
- Enforce idempotency behavior
- Normalize item structure
- Centralize serialization/deserialization
- Emit structured persistence logs

### Why
Persistence drift is the fastest way to corrupt invariants.

---

## 2.2 Observability Surface

### Scope
- Structured logs
- Correlation ID handling
- Error logging
- Metrics emission
- (Optional) tracing

### Rule
All logs must be emitted through the Observability Wrapper.

No direct `print()` or raw logger calls for structured events.

### Responsibilities of Observability Wrapper
- Require mandatory fields:
  - `workflow_name`
  - `job_run_id`
  - `correlation_id`
  - `classification`
  - `error_type` (if error)
- Allow flexible optional fields
- Guarantee consistent log shape
- Standardize metric emission

### Why
Observability entropy makes debugging nonlinear systems nearly impossible.

---

## 2.3 Error Classification Surface

### Scope
- Error taxonomy
- Retryable vs terminal vs quarantine
- Mapping exceptions to classifications

### Rule
Error classification must use shared error types/helpers.

No ad hoc string-based classification.

### Responsibilities of Error Wrapper
- Define canonical error classes
- Map raw exceptions → classification
- Prevent silent misclassification
- Provide stable error contract

### Why
Workflow semantics depend on error classification consistency.

---

# 3. Public vs Private API Policy

Wrappers define public internal APIs.

### Public Internal APIs Include:
- Workflow entry/exit payload shapes
- Persistence wrapper functions
- Observability wrapper functions
- Error classification interfaces

### Private APIs Include:
- Internal lambda helper functions
- Pure transformation utilities
- Module-local logic

Private APIs may change freely as long as public APIs are preserved.

---

# 4. Executor Constraints

Every executor task packet must include:

- Explicit statement of which wrappers are in scope
- Explicit prohibition against bypassing them
- Acceptance tests verifying wrapper usage (when relevant)

Executors may not:
- Introduce direct database calls
- Emit unstructured logs
- Invent new error classifications

Without facilitator approval.

---

# 5. Drift Enforcement

The following drift protections must exist:

- Static scan / allowlist preventing direct DB client usage
- Test asserting structured log required fields
- Test asserting classification mapping completeness
- Interface freeze test on wrapper APIs

Wrappers are merge-gated.

---

# 6. Wrapper Evolution Policy

Wrappers are allowed to evolve, but:

- Changes must preserve existing public behavior unless versioned
- Breaking changes require:
  - Interface Catalog update
  - Invariant Registry update (if affected)
  - ADR
  - Version bump (if applicable)

---

# 7. Recursive Facilitation Compatibility

Sub-facilitators inherit this charter automatically.

They may:
- Extend wrappers within their surface
- Add surface-local helper APIs

They may not:
- Bypass root-level wrappers
- Redefine wrapper responsibilities

---

# 8. Merge Gate Checklist (for PR Review)

Before merging:

- [ ] No direct persistence client usage introduced
- [ ] No raw structured logging introduced
- [ ] Error classification uses shared taxonomy
- [ ] Tests exist for any new wrapper behavior
- [ ] Public API changes reflected in Interface Catalog
- [ ] Invariants preserved

---

# 9. Philosophy

Wrappers are not bureaucracy.

They are:
- Context compression tools
- Invariant concentrators
- Entropy suppressors
- Integration stabilizers

The cost is early discipline.
The reward is scalable parallel development.
