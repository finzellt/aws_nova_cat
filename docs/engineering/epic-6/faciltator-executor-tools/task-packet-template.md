# Task Packet Template
*(Facilitator → Executor Contract)*

A Task Packet defines a single bounded implementation slice.

It must be complete, explicit, and narrow.

If a Task Packet is ambiguous, it must be clarified before execution begins.

---

# 1. Task Metadata

- **Task ID:** TS-###
- **Title:** (Concise, specific)
- **Primary Surface:** (Must match Surface Facilitator ownership)
- **Secondary Surface:** (Optional; at most one)
- **Stability Level:** (Plastic / Stabilizing / Frozen)

---

# 2. Goal

One sentence describing the intended outcome.

Example:
> Introduce deterministic locator identity computation into persistence wrapper.

---

# 3. Architectural Context

## 3.1 Relevant Invariants

List invariant IDs explicitly:

- INV-###
- INV-###
- INV-###

Executor must preserve these.

---

## 3.2 Relevant Public Interfaces

List interface IDs explicitly:

- IFC-###
- IFC-###
- IFC-###

Executor may not modify these unless explicitly instructed.

---

## 3.3 Wrapper Constraints

Explicitly state:

- Persistence Wrapper usage required: Yes/No
- Observability Wrapper usage required: Yes/No
- Error Classification Wrapper usage required: Yes/No
- Idempotency Lock involvement: Yes/No

---

# 4. Scope Definition

## 4.1 In-Scope Files / Modules

Explicit list of files or directories allowed to change.

Example:

- `persistence/locator_identity.py`
- `tests/persistence/test_locator_identity.py`

---

## 4.2 Explicitly Out of Scope

List prohibited modifications.

Example:

- No modification to workflow definitions
- No schema changes
- No Public Interface changes
- No dependency additions

If executor determines out-of-scope changes are required, escalate.

---

# 5. Acceptance Criteria

Define measurable completion conditions.

Examples:

- Deterministic identity computed according to precedence rules
- URL normalization tested
- No duplicate locator identity produced for identical inputs
- All referenced invariants preserved

Must be testable.

---

# 6. Required Tests

List required tests explicitly:

- Unit tests covering edge cases
- Invariant enforcement tests (if L1 involved)
- Snapshot updates (if contract involved)
- Golden telemetry tests (if logging involved)

Tests must be written or updated as part of slice.

---

# 7. Drift Considerations

Specify if any of the following are expected:

- Schema snapshot change: Yes/No
- Workflow definition snapshot change: Yes/No
- Wrapper API surface change: Yes/No
- Public Interface Catalog update required: Yes/No
- Version bump required: Yes/No

If "Yes", specify expected impact.

---

# 8. Context Budget Rules

Executor may:

- Request specific file excerpts
- Ask targeted clarification questions

Executor may not:

- Request full repository dumps
- Expand surface scope
- Redesign architecture

If context proves insufficient, escalate.

---

# 9. Risks / Edge Cases to Consider

List known tricky areas.

Example:

- URL normalization collisions
- Missing provider_product_id
- Concurrent execution attempts

---

# 10. Deliverables

Executor must return:

- Summary of changes
- Files modified
- Tests added/updated
- Invariant preservation confirmation
- Interface impact confirmation
- Wrapper compliance confirmation
- Assumptions
- Escalation notes (if any)

---

# 11. Definition of Done

Task is complete when:

- Acceptance criteria met
- Required tests pass
- Drift Suite passes
- No invariant violations
- No wrapper bypass
- Scope adhered to
- Return Packet provided

---

# 12. Escalation Trigger

Executor must halt and escalate if:

- Invariant conflict detected
- Public interface change required
- Cross-surface modification required
- Wrapper insufficient for required behavior
- Task requires architectural reinterpretation

No silent deviation permitted.

---
