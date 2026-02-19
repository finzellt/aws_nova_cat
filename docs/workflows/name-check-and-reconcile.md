# Workflow Spec: NameCheckAndReconcile

## Purpose
Resolve an unresolved name to a stable `nova_id` and update alias/public_name as needed.
This is the ONLY workflow allowed to use unresolved names as primary inputs.

## Triggers
- Manual/operator trigger (given candidate_name)
- Upstream ingestion pipeline that discovers a name without stable ID

## Event Contracts
### Input Event Schema
- Schema name: `NameCheckAndReconcile`
- Required: `candidate_name`
- Optional: hint fields, correlation_id

### Output Event Schema
- Schema name: `NameResolved` (or equivalent)
- Required: `nova_id`
- Includes: confidence, canonical_name, alias updates summary

## State Machine (Explicit State List)
1. ValidateInput (Pass)
2. BeginJobRun (Task)
3. AcquireIdempotencyLock (Task)
4. NormalizeName (Task)
5. QueryResolvers (Parallel)
   - QueryInternalAliases (Task)
   - QueryExternalResolverA (Task)
   - QueryExternalResolverB (Task)
6. ReconcileResults (Task)
7. Decision (Choice)
   - Resolved -> ApplyNameUpdates
   - Ambiguous -> QuarantineHandler
   - No match -> CreateNovaIdAndInitialize (Task) -> ApplyNameUpdates
8. ApplyNameUpdates (Task)
9. PublishNameResolved (Task)
10. FinalizeJobRunSuccess (Task)
11. QuarantineHandler (Task)
12. FinalizeJobRunQuarantined (Task)
13. TerminalFailHandler (Task)
14. FinalizeJobRunFailed (Task)

## Retry / Timeout Policy
- NormalizeName:
  - Timeout 10s; Retry MaxAttempts 2; Backoff 2s, 10s
- Query* resolvers:
  - Timeout 30s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- ReconcileResults:
  - Timeout 20s; Retry only on internal transient errors; MaxAttempts 2
- ApplyNameUpdates:
  - Timeout 20s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- PublishNameResolved:
  - Timeout 10s; Retry MaxAttempts 2

## Failure Classification Policy
- Retryable: transient resolver failures, throttling, 5xx
- Terminal: schema/version mismatch; invalid candidate_name
- Quarantine: multiple plausible matches; conflicting authoritative sources; non-monotonic identity changes (merge ambiguity)

## Idempotency Guarantees & Invariants
- Workflow idempotency key (time-bucketed): `NameCheckAndReconcile:{normalized_name}:{schema_version}:{time_bucket}`
- Invariant: outputs always include `nova_id` and downstream workflows MUST use UUIDs only.

## JobRun / Attempt Emissions + Required Log Fields
(same conventions as InitializeNova)
