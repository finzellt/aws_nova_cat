# Workflow Spec: InitializeNova

## Purpose
Initialize or enrich a Nova record using stable identity (nova_id). This workflow:
- Ensures the nova entity exists
- Verifies the candidate is a **classical nova**
- Gathers critical metadata: **coordinates**, **constellation**, **aliases**

## Triggers
- Manual/operator trigger for a known nova_id
- Upstream workflow trigger (e.g., IngestNewNova)
- Re-run permitted for enrichment under time-bucketed idempotency

## Event Contracts
### Input Event Schema
- Schema name: `InitializeNova`
- Required identifiers: `nova_id`
- Optional: resolver hints (future), correlation_id

### Output Event Schema
- Schema name: `InitializeNovaCompleted` (or equivalent “completed” event contract)
- Required identifiers: `nova_id`
- Includes: metadata summary, provenance summary, correlation_id

## State Machine (Explicit State List)
1. **ValidateInput** (Pass)
2. **BeginJobRun** (Task)
3. **AcquireIdempotencyLock** (Task)
4. **ResolveCandidate** (Task)
5. **VerifyClassicalNova** (Choice)
   - If classical nova: continue
   - If ambiguous: Quarantine
   - If not a classical nova: TerminalFail
6. **UpsertCriticalMetadata** (Task)
7. **PublishInitializeNovaCompleted** (Task)
8. **FinalizeJobRunSuccess** (Task)
9. **QuarantineHandler** (Task)
10. **FinalizeJobRunQuarantined** (Task)
11. **TerminalFailHandler** (Task)
12. **FinalizeJobRunFailed** (Task)

## Retry / Timeout Policy (per state)
- BeginJobRun:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- AcquireIdempotencyLock:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- ResolveCandidate:
  - Timeout: 30s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- UpsertCriticalMetadata:
  - Timeout: 20s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- PublishInitializeNovaCompleted:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 2; Backoff 2s, 10s
- FinalizeJobRun*:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s

## Failure Classification Policy
- Retryable:
  - transient network failures, timeouts, throttling, 5xx from dependencies
- Terminal:
  - schema/version mismatch
  - missing/invalid nova_id
  - resolved type explicitly not classical nova
- Quarantine:
  - ambiguous classification (insufficient/conflicting evidence)
  - metadata parse/normalization ambiguity requiring review

## Idempotency Guarantees & Invariants
- Workflow idempotency key: `InitializeNova:{nova_id}:{schema_version}`
- Invariant: downstream workflows depend only on `nova_id` (no names required).
- If lock indicates prior terminal success, workflow returns “duplicate/short-circuit” via FinalizeJobRunSuccess with `result=SKIPPED_DUPLICATE`.

## JobRun / Attempt Emissions
- JobRun:
  - Emit STARTED at BeginJobRun
  - Emit SUCCEEDED / FAILED / QUARANTINED at Finalize states
- Attempt (per Task invocation, including retries):
  - ResolveCandidate, UpsertCriticalMetadata, PublishInitializeNovaCompleted, etc.
- Required structured log fields (minimum):
  - workflow_name, execution_arn, job_run_id, state_name, attempt_number
  - nova_id, schema_version
  - correlation_id, idempotency_key
  - error_classification, error_fingerprint (on failures)
