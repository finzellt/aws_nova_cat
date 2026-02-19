# Workflow Spec: IngestNewNova

## Purpose
Coordinator workflow that bootstraps ingestion for a newly established `nova_id` by launching modular workflows.
This workflow is intentionally short-lived (cost-aware) and does not perform heavy work.

## Triggers
- After NameCheckAndReconcile yields a nova_id (optional)
- Manual/operator trigger for an existing nova_id

## Event Contracts
### Input Event Schema
- Schema name: `IngestNewNova`
- Required: `nova_id`
- Optional: correlation_id

### Output Event Schema
- Schema name: `IngestNewNovaLaunched` (or equivalent)
- Includes: list of launched workflows and their execution references

## State Machine (Explicit State List)
1. ValidateInput (Pass)
2. BeginJobRun (Task)
3. AcquireIdempotencyLock (Task)
4. EnsureInitialized (Task OR StartExecution InitializeNova + wait-for-completion)
5. LaunchDownstream (Parallel)
   - LaunchRefreshReferences (Task)
   - LaunchDiscoverSpectraProducts (Task)
   - (Optional later) LaunchPhotometryDiscovery/Ingest (Task)
6. SummarizeLaunch (Task)
7. PublishIngestNewNovaLaunched (Task)
8. FinalizeJobRunSuccess (Task)
9. TerminalFailHandler (Task)
10. FinalizeJobRunFailed (Task)

## Retry / Timeout Policy
- EnsureInitialized:
  - Timeout 2m (if synchronous); Retry MaxAttempts 2; Backoff 2s, 10s
- Launch* tasks:
  - Timeout 10s; Retry MaxAttempts 2; Backoff 2s, 10s
- Entire workflow timeout target: <= 10 minutes

## Failure Classification Policy
- Retryable: transient start-execution failures, throttling
- Terminal: invalid nova_id; schema/version mismatch; InitializeNova terminal failure
- NOTE: downstream workflow failures do NOT retroactively fail this coordinator

## Idempotency Guarantees & Invariants
- Workflow idempotency key: `IngestNewNova:{nova_id}:{schema_version}`
- Invariant: only launches UUID-based workflows; no name usage.

## JobRun / Attempt Emissions + Required Log Fields
(same conventions)
