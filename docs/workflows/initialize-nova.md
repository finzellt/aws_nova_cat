# Workflow Spec: initialize_nova

## Purpose
Name-only front door for establishing a nova in Nova Cat.

Given a `candidate_name` (public name or alias), this workflow:
1) Checks whether the name already exists in the database (by alias/public name resolution).
2) If it exists: short-circuits and triggers `ingest_new_nova` using the existing `nova_id`.
3) If it does not exist: queries public archives/resolvers to determine whether this name corresponds to a classical nova.
   - If it corresponds to a classical nova: creates a new `nova_id`, gathers minimal metadata, upserts it, and triggers `ingest_new_nova`.
   - If it does not correspond to a nova (yet): exits successfully with outcome `NOT_FOUND` and does not trigger downstream workflows.

**Invariant:** initialize_nova is the ONLY front door when the system has only a name (no `nova_id`).
Other workflows may be triggered directly when `nova_id` already exists.

---

## Triggers
- Operator / manual trigger with `candidate_name`
- External trigger from upstream systems that only know a name

---

## Event Contracts

### Input Event Schema
- Schema name: `initialize_nova`
- Schema path: `schemas/events/initialize_nova/latest.json`
- Required identifiers: `candidate_name`
- Optional: `correlation_id` (workflow generates if missing)

### Output Event Schema (Downstream Published Event)
- On outcomes `CREATED_AND_LAUNCHED` or `EXISTS_AND_LAUNCHED`:
  - Published schema name: `ingest_new_nova`
  - Published schema path: `schemas/events/ingest_new_nova/latest.json`
  - Includes: `nova_id`, `correlation_id`
- On outcome `NOT_FOUND`:
  - No downstream workflow event published (terminal success outcome)

---

## State Machine (Explicit State List)

1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
   - If `correlation_id` missing: generate new UUID
3. **BeginJobRun** (Task)
4. **AcquireIdempotencyLock** (Task)
5. **NormalizeCandidateName** (Task)
6. **CheckExistingNovaByName** (Task)
7. **ExistsInDB?** (Choice)
   - Yes -> **PublishIngestNewNova** -> **FinalizeJobRunSuccess** (outcome = `EXISTS_AND_LAUNCHED`)
   - No  -> continue
8. **ResolveCandidateAgainstPublicArchives** (Task)

9. **CheckExistingNovaByCoordinates** (Task)
   - Inputs: resolved coordinates (RA/Dec + epoch if available)
   - Behavior:
     - Retrieve existing nova coordinates from persistent store
     - Compute angular separation to each candidate
     - Determine minimum separation

10. **CoordinateMatchClassification?** (Choice)
   - Separation < 2"  -> **UpsertAliasForExistingNova** -> **PublishIngestNewNova**
     -> **FinalizeJobRunSuccess** (outcome = `EXISTS_AND_LAUNCHED`)
   - Separation 2"–10" -> **QuarantineHandler** -> **FinalizeJobRunQuarantined**
   - Separation > 10"  -> continue

11. **CandidateIsNova?** (Choice)
   - No -> **FinalizeJobRunSuccess** (outcome = `NOT_FOUND`)
   - Yes -> continue

12. **CandidateIsClassicalNova?** (Choice)
   - No -> **FinalizeJobRunSuccess** (outcome = `NOT_A_CLASSICAL_NOVA`) *(terminal success; does not launch)*
   - Ambiguous -> **QuarantineHandler** -> **FinalizeJobRunQuarantined**
   - Yes -> continue

13. **CreateNovaId** (Task)
14. **UpsertMinimalNovaMetadata** (Task)
15. **PublishIngestNewNova** (Task)
16. **FinalizeJobRunSuccess** (Task) (outcome = `CREATED_AND_LAUNCHED`)
17. **UpsertAliasForExistingNova** (Task)
18. **QuarantineHandler** (Task)
19. **FinalizeJobRunQuarantined** (Task)
20. **TerminalFailHandler** (Task)
21. **FinalizeJobRunFailed** (Task)

---

## Retry / Timeout Policy (per state)
- BeginJobRun:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- AcquireIdempotencyLock:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- NormalizeCandidateName:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 2; Backoff 2s, 10s
- CheckExistingNovaByName:
  - Timeout: 20s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- ResolveCandidateAgainstPublicArchives:
  - Timeout: 60s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- CheckExistingNovaByCoordinates:
  - Timeout: 20s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- UpsertAliasForExistingNova:
  - Timeout: 20s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- CreateNovaId:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- UpsertMinimalNovaMetadata:
  - Timeout: 30s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- PublishIngestNewNova:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 2; Backoff 2s, 10s
- FinalizeJobRun*:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s

---

## Failure Classification Policy

- Retryable:
  - transient network errors, timeouts, throttling, 5xx dependency failures
- Terminal:
  - schema/version mismatch
  - missing/invalid `candidate_name`
  - resolver did not return coordinates when required for identity checks
  - internal invariant violations
- Quarantine:
  - ambiguous resolver results (cannot safely decide nova / classical nova status)
  - conflicting authoritative sources
  - coordinate match in ambiguous band (2"–10")

**Note:** outcomes `NOT_FOUND` and `NOT_A_CLASSICAL_NOVA` are NOT failures; they are terminal-success outcomes without downstream launch.

---

## Idempotency Guarantees & Invariants

- Workflow idempotency key (time-bucketed): `InitializeNova:{normalized_candidate_name}:{schema_version}:{time_bucket}`

- Invariant: downstream workflow launches use UUIDs only (`nova_id`).
- Invariant: `idempotency_key` is INTERNAL ONLY and must not be required in event schemas.

- Short-circuit behaviors:
  - If name exists -> launch `ingest_new_nova` and do not create a new `nova_id`.
  - If coordinates match an existing nova within 2" -> upsert alias and launch `ingest_new_nova` with the existing `nova_id`.

---

## JobRun / Attempt Emissions and Required Log Fields

- JobRun:
  - Emit STARTED at BeginJobRun
  - Emit SUCCEEDED with outcome (`CREATED_AND_LAUNCHED` | `EXISTS_AND_LAUNCHED` | `NOT_FOUND` | `NOT_A_CLASSICAL_NOVA`)
  - Emit QUARANTINED / FAILED at terminal states
- Attempt:
  - Emit for each Task invocation, including retries
- Required structured log fields (minimum):
  - workflow_name, execution_arn, job_run_id, state_name, attempt_number
  - schema_version, correlation_id
  - candidate_name, normalized_candidate_name
  - nova_id (when known)
  - resolved_ra, resolved_dec, resolved_epoch (when available)
  - coordinate_match_min_sep_arcsec (when computed)
  - coordinate_match_outcome (DUPLICATE|AMBIGUOUS|NONE)
  - workflow_idempotency_key (internal), step_dedupe_key (internal, when used)
  - error_classification, error_fingerprint (on failures/quarantine)
