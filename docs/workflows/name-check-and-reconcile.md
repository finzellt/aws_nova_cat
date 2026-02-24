# Workflow Spec: name_check_and_reconcile

## Purpose
Post-establishment naming enrichment workflow.

This workflow checks whether an already-established nova (`nova_id`) has received:
- a new official designation, or
- newly recognized aliases / naming changes

It is designed to run a limited number of times (e.g., during the first ~6 weeks after eruption) and can stop once naming stabilizes.

**Invariant:** This workflow is NOT a name-only front door. It operates on `nova_id`.

## Triggers
- Scheduled runs for newly established novae (time-bucketed)
- Manual/operator trigger for an existing `nova_id`
- Optional future trigger after references refresh if new aliases are discovered

## Event Contracts
### Input Event Schema
- Schema name: `name_check_and_reconcile`
- Schema path: `schemas/events/name_check_and_reconcile/latest.json`
- Required identifiers: `nova_id`
- Optional: `correlation_id` (workflow generates if missing)

### Output Event Schema (Downstream Published Event)
- If naming updates occur, publish an update event:
  - Schema name: `name_reconciled` (or equivalent)
  - Schema path: `schemas/events/name_reconciled/latest.json` *(only if such a schema exists in repo)*
- If no changes occur: no downstream workflow event required.

## State Machine (Explicit State List)
1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
3. **BeginJobRun** (Task)
4. **AcquireIdempotencyLock** (Task)
5. **FetchCurrentNamingState** (Task)
6. **QueryNamingAuthorities** (Parallel)
   - QueryAuthorityA (Task)
   - QueryAuthorityB (Task)
7. **ReconcileNaming** (Task)
8. **NamingChanged?** (Choice)
   - No -> **FinalizeJobRunSuccess** (outcome = `NO_CHANGE`)
   - Yes -> continue
9. **ApplyNameUpdates** (Task)
10. **PublishNameReconciled** (Task) *(optional, if event exists)*
11. **FinalizeJobRunSuccess** (Task) (outcome = `UPDATED`)
12. **QuarantineHandler** (Task)
13. **FinalizeJobRunQuarantined** (Task)
14. **TerminalFailHandler** (Task)
15. **FinalizeJobRunFailed** (Task)

## Retry / Timeout Policy (per state)
- BeginJobRun / AcquireIdempotencyLock:
  - Timeout 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- FetchCurrentNamingState:
  - Timeout 20s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- QueryAuthority*:
  - Timeout 60s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- ReconcileNaming:
  - Timeout 20s; Retry MaxAttempts 2 (internal transient only)
- ApplyNameUpdates:
  - Timeout 20s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- PublishNameReconciled:
  - Timeout 10s; Retry MaxAttempts 2

## Failure Classification Policy
- Retryable:
  - transient authority failures, throttling, timeouts, 5xx
- Terminal:
  - schema/version mismatch
  - missing/invalid `nova_id`
- Quarantine:
  - conflicting authorities producing ambiguous canonical designation
  - non-monotonic changes that would imply identity merge/split ambiguity

### Quarantine Handling

When a workflow transitions to **QuarantineHandler**, it MUST:

1. Persist quarantine status and relevant diagnostic metadata.
2. Emit a JobRun outcome of `QUARANTINED`.
3. Publish a notification event to an SNS topic for operational review.

SNS notification requirements:
- Include workflow name
- Include primary identifier (e.g., `nova_id` or `data_product_id`)
- Include `correlation_id`
- Include `error_fingerprint`
- Include brief classification reason

The SNS notification is best-effort and MUST NOT cause the workflow to fail if notification delivery fails.

## Idempotency Guarantees & Invariants
- Workflow idempotency key (time-bucketed): `NameCheckAndReconcile:{nova_id}:{schema_version}:{time_bucket}`
- Invariant: names are updated only as enrichment; UUID identity remains stable.
- Invariant: `idempotency_key` is internal-only (not in event schemas).

## JobRun / Attempt Emissions and Required Log Fields
- JobRun outcome includes: `UPDATED` or `NO_CHANGE`.
- Required structured log fields:
  - workflow_name, execution_arn, job_run_id, state_name, attempt_number
  - schema_version, correlation_id, nova_id
  - naming_sources[], updates_applied (bool), update_summary
  - error_classification, error_fingerprint (if applicable)
