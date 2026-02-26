# Epic 5 — Vertical Slice Spike Retrospective

## 1. Executive Summary

Epic 5 (“Vertical Slice”) aimed to deliver a deployable, end-to-end execution path for Nova Cat using the fully aligned architecture produced in Epics 1–4. While the architectural foundation is sound, the initial implementation attempt exceeded manageable complexity. The effort surfaced structural execution constraints—particularly around LLM-assisted development, cross-cutting invariants, and workflow granularity—that made the original orchestration strategy ineffective.

The spike did not expose architectural flaws. Instead, it revealed that implementation requires a different execution model: tighter scoping, invariant isolation, staged integration, and more deliberate orchestration of AI-assisted development.

This retrospective captures those findings and defines a refined path forward.

---

## 2. Original Goal of Epic 5

The goal of Epic 5 was to:

- Deploy a minimal but real AWS stack (CDK-based).
- Implement one end-to-end nova ingestion path:
  - `initialize_nova → ingest_new_nova → discover_spectra_products → acquire_and_validate_spectra`
- Use stub logic where appropriate.
- Preserve all architectural invariants:
  - UUID-first identity
  - Deterministic `data_product_id`
  - Single-table DynamoDB model
  - Eligibility semantics
  - JobRun and Attempt operational tracking
  - Structured logging and error taxonomy
- Avoid overengineering while preserving correctness.

This was intended to produce a vertical skeleton suitable for iterative thickening.

---

## 3. What Was Attempted

The spike attempted to:

- Stand up infrastructure (CDK, DynamoDB, S3, Step Functions, IAM).
- Introduce a task-family Lambda routing pattern.
- Preserve full workflow state topology.
- Implement governance scaffolding (JobRun, Attempt, logging).
- Slim large workflows while keeping semantic fidelity.
- Integrate runtime scaffolding and persistence simultaneously.

This combined infrastructure, runtime utilities, workflow logic, invariants, and orchestration design into a single implementation phase.

---

## 4. What Broke Down (Root Causes)

The breakdown was not due to technical defects but execution coupling. Key root causes:

### 4.1 Cross-Cutting Invariants Everywhere

Operational invariants (idempotency, eligibility removal, correlation IDs, retry taxonomy, deterministic identity) were enforced across:

- All workflows
- All Lambdas
- All persistence writes
- All logging

This created a situation where nearly every code unit required global architectural awareness.

### 4.2 Workflow Granularity

Each workflow contained 15+ task states. Even when thinned, the topology remained complex. Maintaining:

- Attempt semantics
- Retry boundaries
- Downstream publication logic
- Idempotency discipline

meant that simplification was constrained by architectural commitments.

### 4.3 LLM Context Limitations

LLM-assisted development was used as a planning and scaffolding partner. However:

- The architecture spans multiple documents.
- DynamoDB patterns, observability rules, governance rules, and workflow specs are interdependent.
- Context window limits made sustained high-fidelity reasoning across all invariants difficult.
- Repeated ingestion of normative docs introduced cognitive overhead.

The issue was not model capability but sustained multi-document coherence under complex invariants.

### 4.4 Concurrent Phases

Infrastructure provisioning, runtime scaffolding, workflow slimming, invariant enforcement, and testing design were attempted simultaneously.

This removed natural boundaries that would otherwise constrain complexity.

### 4.5 Loss of Cognitive Coherence

At a certain threshold of cross-referenced invariants and workflow state handling, the implementation lost conceptual compression. The system remained architecturally sound, but the execution path no longer had a clear, bounded mental model.

---

## 5. Key Constraints Discovered

1. **LLM-assisted development is most effective within bounded domains**, not when orchestrating full-stack architectural invariants simultaneously.
2. **Workflow topology is not easily compressible** without altering semantics.
3. **Invariant enforcement must be centralized**, or complexity multiplies.
4. **Infrastructure and runtime scaffolding should be decoupled from workflow logic during initial implementation.**
5. **Testing-first integration is required to stabilize architectural invariants early.**

---

## 6. Architectural vs Process Issues

### Architectural Status

The architecture is stable and coherent:

- Contract-first design is correct.
- Single-table DynamoDB model is consistent.
- Eligibility semantics are clear.
- UUID-first identity discipline is sound.
- Workflow boundaries are well-defined.
- Observability and governance models are aligned.

No architectural rollback is required.

### Process Issues

The spike revealed execution orchestration issues:

- Overly broad initial slice.
- Insufficient invariant isolation.
- Lack of staged integration gates.
- Heavy reliance on a single conversational thread for cross-cutting reasoning.

These are process-level concerns, not architectural defects.

---

## 7. Lessons Learned

1. **Vertical slices must be narrower than anticipated.**
2. **Governance and persistence invariants should be implemented and tested in isolation first.**
3. **Workflow topology can be scaffolded independently from business logic.**
4. **AI assistance should operate in bounded executor sessions, not as a global orchestrator.**
5. **Testing must anchor invariants before workflow wiring.**

---

## 8. New Strategy Going Forward

The implementation phase will shift to:

### 8.1 Two-Tier Orchestration Model

- **Facilitator Context**
  Maintains architecture, invariants, and global contracts.

- **Bounded Executor Contexts**
  Implements narrow units:
  - DynamoDB model tests
  - Governance utilities
  - One workflow at a time
  - One task family at a time

Each executor conversation will be scoped to a single subsystem.

### 8.2 Invariant-First Implementation

Phase ordering:

1. DynamoDB item model + access pattern tests.
2. Governance layer (JobRun, Attempt, structured logs).
3. Deterministic identity minting (unit tested).
4. Single workflow with minimal states.
5. Downstream workflow wiring.

### 8.3 Testing-First Integration

- Write unit tests before wiring Step Functions.
- Introduce integration tests once invariants are stable.
- Keep workflows skeletal until invariants are verified.

### 8.4 Reduce Initial Surface Area

Instead of implementing all four workflows at once:

- Start with a single minimal workflow.
- Add downstream workflow only after stability.
- Avoid parallel infrastructure + runtime refactors.

---

## 9. Structural Changes to Epic Planning

Future implementation epics will:

- Explicitly separate:
  - Infrastructure provisioning
  - Runtime scaffolding
  - Workflow logic
  - Testing
- Limit each epic to one primary invariant domain.
- Avoid bundling more than one new cross-cutting concern per phase.
- Introduce a formal “implementation playbook” before code begins.
- Treat AI-assisted sessions as bounded execution tasks, not continuous threads.

---

## 10. Why This Is a Positive Evolution

This spike clarified the execution constraints of a mature architecture. The design did not fail; the orchestration strategy required refinement.

The result is:

- Stronger invariant discipline.
- Clearer separation of architecture vs implementation.
- More deliberate integration sequencing.
- A scalable approach to AI-assisted development.
- Reduced risk of semantic drift during implementation.

The system remains well-designed. The implementation strategy is now better aligned with its complexity.

Epic 5 should proceed — but with tighter bounds, staged integration, and invariant-first sequencing.
