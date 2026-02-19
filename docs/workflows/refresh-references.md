# Workflow Spec: RefreshReferences

## Purpose
Refresh reference/citation data for a `nova_id`, reconcile into stable `reference_id` entities, and link relationships.
Also computes and sets `discovery_date` metadata using the earliest credible reference.

## Triggers
- Scheduled refresh (time-bucketed)
- Triggered after InitializeNova / IngestNewNova
- Manual/operator re-run

## Event Contracts
### Input Event Schema
- Schema name: `RefreshReferences`
- Required: `nova_id`
- Optional: source selector, cursor, correlation_id

### Output Event Schema
- Schema name: `RefreshReferencesCompleted`
- Includes: counts (candidates, upserts, links, quarantined), discovery_date update summary

## State Machine (Explicit State List)
1. ValidateInput (Pass)
2. BeginJobRun (Task)
3. AcquireIdempotencyLock (Task)
4. FetchReferenceCandidates (Task)
5. ReconcileReferences (Map)
   - NormalizeReference (Task)
   - UpsertReferenceEntity (Task) -> yields reference_id
   - LinkNovaReference (Task)
   - ItemFailureHandler (Catch -> QuarantineItem + Continue)
6. ComputeDiscoveryDate (Task)  <-- IMPORTANT: Discovery Date lives here
7. UpsertDiscoveryDateMetadata (Task) (no-op if unchanged)
8. PublishRefreshReferencesCompleted (Task)
9. FinalizeJobRunSuccess (Task)
10. TerminalFailHandler (Task)
11. FinalizeJobRunFailed (Task)

## Retry / Timeout Policy
- FetchReferenceCandidates:
  - Timeout 60s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- Map item tasks (Normalize/Upsert/Link):
  - Timeout 20s each; Retry MaxAttempts 2; Backoff 2s, 10s
  - Map MaxConcurrency: MVP default 5 (tunable)
- ComputeDiscoveryDate:
  - Timeout 20s; Retry MaxAttempts 2 (internal transient only)
- UpsertDiscoveryDateMetadata:
  - Timeout 20s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s

## Failure Classification Policy
- Retryable: transient upstream/service failures; throttling
- Terminal: invalid/missing nova_id; schema/version mismatch
- Quarantine (item-level): unparseable reference payloads; conflicting minimal metadata
- Discovery date:
  - Missing dates -> no-op (not failure)
  - Conflicts preventing earliest selection -> Quarantine (rare)

## Idempotency Guarantees & Invariants
- Workflow idempotency key (time-bucketed): `RefreshReferences:{nova_id}:{source}:{schema_version}:{time_bucket}`
- Reference upsert dedupe key: `ReferenceUpsert:{source}:{source_key}:{schema_version}`
- Relationship dedupe key: `NovaReferenceLink:{nova_id}:{reference_id}`
- DiscoveryDate dedupe key: `DiscoveryDate:{nova_id}:{earliest_reference_id}:{rule_version}`
- Invariant: discovery_date update should be monotonic earlier (unless explicitly configured otherwise).

## JobRun / Attempt Emissions + Required Log Fields
- Map item failures MUST emit Attempt with `error_classification=QUARANTINE` and continue.
- Required log fields include: nova_id, reference_source, candidate_count, quarantined_count, discovery_date_old/new.
