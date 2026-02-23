# ADR-003: Workflow Orchestration and Execution Model

## Status
Proposed

## Context
Nova Cat uses a contract-first architecture:
- Persistent entities and workflow events are defined via versioned Pydantic models with exported JSON Schemas.
- Downstream processing operates on stable UUIDs (nova_id, dataset_id, reference_id).
- Scientific state and operational state are separated.
Primary system constraint is cost-efficiency.

We need an orchestration model that binds event contracts into executable workflows while preserving loose coupling, idempotency, and observability.

## Decision
1. Use AWS Step Functions as the orchestration backbone for Nova Cat workflows.
2. Adopt a modular workflow strategy: each “business capability” is a distinct workflow with narrow, versioned input/output event contracts.
3. Enforce the UUID-first execution rule:
   - Workflows consume/emit stable UUIDs as primary identifiers.
   - Names are permitted only at the NameCheckAndReconcile boundary.
4. Require workflow-level and step-level idempotency keys to ensure exactly-once logical effects under at-least-once execution.
5. Standardize failure classification into Retryable, Terminal, and Quarantine with bounded retries and explicit quarantine handling.
6. Require workflow-level observability through:
   - JobRun records for executions
   - Attempt records for task invocations (including retries)
   - structured logs with correlation_id propagation

## Rationale
- Step Functions provides explicit orchestration graphs, built-in retries/timeouts, and operational visibility.
- Modular workflows reduce blast radius and enable independent evolution of capabilities and contracts.
- UUID-first execution prevents coupling to mutable names and supports long-term identity stability.
- Idempotency is essential for safe retries, replays, and periodic refresh workflows.
- A quarantine path allows data-quality and ambiguity issues to be handled without destabilizing the system.
- JobRun/Attempt plus structured logging provides an auditable execution history suitable for debugging and portfolio-quality demonstration.

## Consequences
### Benefits
- Clear orchestration boundaries aligned with contracts
- Safe replay and periodic refresh via idempotency
- Reduced coupling and improved maintainability
- Uniform observability across workflows

### Tradeoffs
- Requires discipline around error normalization and idempotency key design
- Quarantine introduces an operational review loop (human or automated later)

## Notes
- ComputeDiscoveryDate is defined as part of RefreshReferences (post-reconciliation) to avoid scattering derived scientific metadata logic across workflows.
- Provider-specific normalization is contained within DiscoverSpectraProducts as an explicit boundary step.
