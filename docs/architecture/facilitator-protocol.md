# Facilitator Protocol v1
*(Recursive Orchestration Framework for LLM-Assisted Implementation)*

## 0. Purpose

The Facilitator Protocol defines how complex engineering work is:

- Decomposed
- Delegated
- Executed
- Integrated
- Merge-gated

It enables scalable, parallel development under:

- Strict architectural invariants
- LLM context-window constraints
- Multi-surface cross-cutting concerns
- Contract-first design

This protocol is recursive by design.

---

# 1. Roles

## 1.1 Root Facilitator

Owns:

- Architecture Snapshot
- Wrapper Charter
- Public Interface Catalog
- Invariant Registry
- Drift Suite definition
- Merge Gate policy

Authority:

- Approves invariant changes
- Approves breaking interface changes
- Spawns sub-facilitators
- Resolves cross-surface conflicts

---

## 1.2 Surface Facilitator (Sub-Facilitator)

Owns a bounded surface (e.g., Persistence, Workflow, Observability).

Responsibilities:

- Decompose work within surface
- Issue executor task packets
- Enforce wrapper and invariant compliance locally
- Escalate invariant or interface changes to Root Facilitator

Cannot:

- Modify global invariants
- Bypass Wrapper Charter
- Change public interfaces without approval

---

## 1.3 Executor

Operates within a bounded task packet.

Responsibilities:

- Implement only the scoped slice
- Respect wrapper constraints
- Preserve invariants
- Provide return packet with explicit assumptions

Executors may not:

- Redesign architecture
- Modify unrelated surfaces
- Change public interfaces without explicit instruction

---

# 2. Recursive Structure

Facilitation is recursive:

Root Facilitator
→ Surface Facilitator
→ Executor

A Surface Facilitator may temporarily become a Root for a sub-surface, but must inherit:

- Wrapper Charter
- Invariant Registry
- Drift Suite rules
- Merge gate checklist

Invariants flow downward.
Authority to change invariants flows upward.

---

# 3. Work Decomposition Model

## 3.1 Bounded Slice Principle

Each task must:

- Touch exactly one primary surface
- Touch at most one secondary surface
- Define explicit non-goals
- Include acceptance criteria
- Include required tests

If a task spans multiple primary surfaces, it must be split.

---

## 3.2 Slice Size Heuristic

A task is appropriately sized if:

- It can be reviewed in one sitting
- It modifies ≤ ~10 files across ≤ 2 surfaces
- It adds or updates tests
- It does not require architectural reinterpretation

Large refactors must be staged into multiple slices.

---

# 4. Task Packet Format

Every executor task begins with a Task Packet.

## 4.1 Required Fields

- **Task ID**
- **Title**
- **Primary Surface**
- **Secondary Surface (if any)**
- **Goal (one sentence)**
- **In-Scope Files/Modules**
- **Out-of-Scope (Explicit Prohibitions)**
- **Relevant Invariants (IDs only)**
- **Relevant Public Interfaces (IDs only)**
- **Wrapper Constraints**
- **Acceptance Criteria**
- **Required Tests**
- **Context Budget Rules**

---

## 4.2 Context Budget Rules

Executor may:

- Request specific file excerpts (not full repo dumps)
- Ask clarification questions limited to surface

Executor may not:

- Expand scope
- Request global architectural reinterpretation
- Introduce new surfaces

If context expansion becomes necessary, task must be escalated.

---

# 5. Return Packet Format

Executor must return:

- Summary of changes
- Files modified
- Tests added/updated
- Assumptions made
- Potential invariant/interface impacts
- Risks or edge cases
- Open questions (if blocking)

Return packet must explicitly confirm:

- No wrapper bypass
- No invariant violations
- No public interface changes (unless instructed)

---

# 6. Escalation Protocol

If executor detects:

- Potential invariant conflict
- Ambiguous interface evolution
- Need for breaking change
- Cross-surface redesign

They must:

1. Halt implementation.
2. Return packet with analysis.
3. Surface Facilitator reviews.
4. Escalate to Root Facilitator if necessary.

Architectural changes may not occur implicitly.

---

# 7. Merge Gate Protocol

A task may merge only if:

- Drift Suite passes
- Wrapper constraints preserved
- Invariants unchanged (or ADR provided)
- Public Interface Catalog updated if needed
- Snapshot updates justified
- Scope adhered to

If a PR violates declared scope, it must be split.

---

# 8. Integration Friction Management

When two slices conflict:

1. Root Facilitator identifies boundary mismatch.
2. Create a "Bridge Slice":
   - Small task aligning surfaces.
3. Merge bridge slice first.
4. Resume parallel slices.

Never resolve cross-surface conflict inside an executor slice.

---

# 9. Architectural Freeze Levels

The Facilitator may declare surfaces as:

- **Plastic:** evolving; low stability; frequent change allowed
- **Stabilizing:** minor changes allowed; strong test coverage required
- **Frozen:** only breaking with ADR; minimal churn allowed

Surface state affects review strictness.

---

# 10. Definition of Done (Per Slice)

A slice is complete when:

- Acceptance criteria satisfied
- Required tests pass
- Drift Suite passes
- No wrapper bypass
- No invariant violation
- No undocumented public interface change
- Return packet provided

---

# 11. Anti-Entropy Rules

Executors must avoid:

- Drive-by refactors outside scope
- Renaming unrelated symbols
- Reformatting large unrelated regions
- Introducing new dependencies without approval
- Expanding validation logic without version consideration

Scope discipline preserves parallel velocity.

---

# 12. Philosophy

The Facilitator Protocol is not bureaucracy.

It is:

- Context compression
- Risk containment
- Parallelization safety
- Integration predictability

It allows:

- Many small safe changes
- Recursive delegation
- Controlled evolution
- Architectural coherence at scale

Without it, large LLM-assisted systems drift rapidly.

---
