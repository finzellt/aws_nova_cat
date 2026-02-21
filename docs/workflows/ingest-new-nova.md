# Workflow Spec: ingest_new_nova

## Purpose
Coordinator workflow that bootstraps ingestion for an already-established `nova_id` by launching modular workflows.
This workflow is intentionally short-lived and cost-aware.

**Important change:** this workflow no longer performs any “ensure initialized” step.
It assumes the nova exists (created earlier by initialize_nova or other established processes).

## Triggers
- Triggered by initialize_nova (name-only front door) after `nova_id` is known
- Manual/operator trigger for an existing `nova_id`

## Event Contracts
### Input Event Schema
- Schema name: `ingest_new_nova`
- Schema path: `schemas/events/ingest_new_nova/latest.json`
- Required identifiers: `nova_id`
- Optional: `correlation_id` (workflow generates if missing)

### Output Event Schema (Downstream Published Event)
- This workflow is a coordinator; it primarily launches downstream workflows.
- It MAY publish an internal “launched” event for auditing, but downstream consumers are optional.
  - If present: `schemas/events/ingest_new_nova_launched/latest.json` (optional, if such a schema exists)

## State Machine (Explicit State List)
1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
3. **BeginJobRun** (Task)
4. **AcquireIdempotencyLock** (Task)
5. **LaunchDownstream** (Parallel)
   - **LaunchRefreshReferences** (Task) -> publishes `schemas/events/refresh_references/latest.json`
   - **LaunchDiscoverSpectraProducts** (Task) -> publishes `schemas/events/discover_spectra_products/latest.json`
   - (Optional future) launch other workflows
6. **SummarizeLaunch** (Task)
7. **FinalizeJobRunSuccess** (Task)
8. **TerminalFailHandler** (Task)
9. **FinalizeJobRunFailed** (Task)

## Retry / Timeout Policy (per state)
- BeginJobRun:
  - Timeout: 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- AcquireIdempotencyLock:
  - Timeout: 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- LaunchRefreshReferences / LaunchDiscoverSpectraProducts:
  - Timeout: 10s; Retry MaxAttempts 2; Backoff 2s, 10s
- SummarizeLaunch:
  - Timeout: 10s; Retry MaxAttempts 2; Backoff 2s, 10s
- Target overall workflow duration: <= 10 minutes

## Failure Classification Policy
- Retryable:
  - transient start-execution failures, throttling
- Terminal:
  - schema/version mismatch
  - missing/invalid `nova_id`
- Note:
  - downstream workflow failures do NOT retroactively fail this coordinator

## Idempotency Guarantees & Invariants
- Workflow idempotency key: `IngestNewNova:{nova_id}:{schema_version}`
- Invariant: This workflow does not depend on names.
- Invariant: `idempotency_key` is internal-only (not in event schemas).

## JobRun / Attempt Emissions and Required Log Fields
- JobRun emits STARTED and SUCCEEDED/FAILED.
- Attempts emitted for each Task (including each launch Task and retries).
- Required structured log fields:
  - workflow_name, execution_arn, job_run_id, state_name, attempt_number
  - schema_version, correlation_id, nova_id
  - workflow_idempotency_key (internal)
  - launched_workflows[] (names) and launch_status per branch
