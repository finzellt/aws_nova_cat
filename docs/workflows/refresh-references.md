# Workflow Spec: refresh_references

## Purpose
Refresh reference/citation data for `nova_id`, reconcile into stable `reference_id` entities, and link relationships.
Also computes and sets `discovery_date` metadata using the earliest credible reference.

**Note:** `ComputeDiscoveryDate` lives here (post-reconciliation), by design.

## Triggers
- Scheduled refresh (time-bucketed)
- Triggered after ingest_new_nova
- Manual/operator re-run

## Event Contracts
### Input Event Schema
- Schema name: `refresh_references`
- Schema path: `schemas/events/refresh_references/latest.json`
- Required identifiers: `nova_id`
- Optional: `correlation_id` (workflow generates if missing)

### Output Event Schema (Downstream Published Event)
- Typically no downstream workflow event required.
- If you maintain a “completed” event schema, it would be:
  - `schemas/events/refresh_references_completed/latest.json` *(optional, if exists)*

## State Machine (Explicit State List)
1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
3. **BeginJobRun** (Task)
4. **AcquireIdempotencyLock** (Task)
5. **FetchReferenceCandidates** (Task)
6. **ReconcileReferences** (Map)
   - NormalizeReference (Task)
   - UpsertReferenceEntity (Task) -> yields reference_id
   - LinkNovaReference (Task)
   - ItemFailureHandler (Catch -> QuarantineItem + Continue)
7. **ComputeDiscoveryDate** (Task)
8. **UpsertDiscoveryDateMetadata** (Task) (no-op if unchanged)
9. **FinalizeJobRunSuccess** (Task)
10. **TerminalFailHandler** (Task)
11. **FinalizeJobRunFailed** (Task)

## Retry / Timeout Policy (per state)
- BeginJobRun / AcquireIdempotencyLock:
  - Timeout 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- FetchReferenceCandidates:
  - Timeout 60s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- Map item tasks:
  - Timeout 20s each; Retry MaxAttempts 2; Backoff 2s, 10s
  - Map MaxConcurrency: MVP default 5 (tunable)
- ComputeDiscoveryDate:
  - Timeout 20s; Retry MaxAttempts 2 (internal transient only)
- UpsertDiscoveryDateMetadata:
  - Timeout 20s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s

## Failure Classification Policy
- Retryable:
  - transient upstream/service failures; throttling; timeouts
- Terminal:
  - schema/version mismatch
  - missing/invalid `nova_id`
- Quarantine:
  - item-level reference parse failures (continue Map)
  - discovery date cannot be selected due to irreconcilable conflicts (rare)

## Idempotency Guarantees & Invariants
- Workflow idempotency key (time-bucketed): `RefreshReferences:{nova_id}:{schema_version}:{time_bucket}`
- Reference upsert dedupe key: `ReferenceUpsert:{source}:{source_key}:{schema_version}`
- Relationship dedupe key: `NovaReferenceLink:{nova_id}:{reference_id}`
- DiscoveryDate dedupe key: `DiscoveryDate:{nova_id}:{earliest_reference_id}:{rule_version}`
- Invariant: discovery_date update should be monotonic earlier (unless explicitly configured otherwise).
- Invariant: `idempotency_key` is internal-only (not in event schemas).

## JobRun / Attempt Emissions and Required Log Fields
- Map item failures MUST emit Attempt with `error_classification=QUARANTINE` and continue.
- Required structured log fields:
  - workflow_name, execution_arn, job_run_id, state_name, attempt_number
  - schema_version, correlation_id, nova_id
  - reference_source, candidate_count, upsert_count, link_count, quarantined_count
  - discovery_date_old, discovery_date_new
