# Execution Governance

This document defines Step Functions execution conventions for Nova Cat workflows.
It is intentionally “code-adjacent” and designed to be translated into definitions later.

---

## Standard Retry & Backoff Conventions

### Default (MVP)

- MaxAttempts: 3
- Backoff: 2s, 10s, 30s
- Retry only errors classified as RETRYABLE
- No unbounded retries

### Long-running acquisition tasks (MVP)

- Timeout: up to 15 minutes
- MaxAttempts: 3
- Backoff: 10s, 60s, 180s
- Checksum mismatch is NOT retryable (Quarantine)
- Explicit throttling signals (e.g., HTTP 429) are RETRYABLE

---

## Acquisition Attempt Persistence & Cooldown Policy (MVP)

To prevent repeated hammering of external providers, acquisition workflows MUST persist attempt metadata on each `data_product_id`.

### Persisted Fields (Minimum Required)

Each spectra data product SHOULD persist:

Scientific state:

- `acquisition_status`
  - `STUB | ACQUIRED | FAILED`
- `validation_status`
  - `UNVALIDATED | VALID | QUARANTINED`

Operational metadata:

- `attempt_count_total`
- `last_attempt_at`
- `last_attempt_outcome`
  - `SUCCESS | RETRYABLE_FAILURE | TERMINAL_FAILURE | QUARANTINE`
- `last_error_fingerprint`
- `next_eligible_attempt_at`  ← primary anti-ping control
- `last_successful_fingerprint` (when validated)

---

### Cooldown Enforcement

Before acquisition:

1. If `validation_status == VALIDATED`
   → short-circuit success (`SKIPPED_DUPLICATE`)
2. If `now < next_eligible_attempt_at`
   → short-circuit success (`SKIPPED_BACKOFF`)
3. Otherwise proceed to acquisition

---

### Backoff Strategy

On RETRYABLE acquisition failure:

- Increment `attempt_count_total`
- Compute new cooldown:
- next_eligible_attempt_at = now + backoff(attempt_count_total)
- Persist updated fields

Backoff SHOULD be capped exponential.

Providers MAY override default backoff parameters.

---

## Error Taxonomy

### RETRYABLE

Examples:
- throttling
- transient network errors
- timeouts
- 5xx dependency/service failures

### TERMINAL

Examples:
- schema_version unsupported
- missing/invalid UUIDs (nova_id/data_product_id/reference_id)
- invalid required fields (e.g., missing locator/provenance)
- logical impossibility (e.g., resolved object explicitly not a classical nova)

### QUARANTINE

Examples:
- ambiguous name resolution matches
- spectra content fails integrity/format/domain sanity checks
- provider returns malformed or inconsistent records requiring review
- unknown or incompatible FITS profile

### Scientific vs Operational State

Scientific state (validation_status, acquisition_status) reflects
data quality and lifecycle stage only.

Retryability and terminal classification are operational concepts
and are recorded in JobRun/Attempt records and last_attempt_outcome.

Scientific enums MUST NOT encode retryability.

---

## Correlation ID Rules

- Every workflow input SHOULD include `correlation_id`.
- If absent, the workflow MUST create one (UUID) and propagate it.
- `correlation_id` MUST be included in:
- all published downstream events
- all JobRun/Attempt records
- structured logs

---

## Idempotency Key Rules

### Workflow-level

- Every workflow defines a deterministic idempotency key.
- Workflows that poll/refresh external sources MUST be time-bucketed.

### Step-level

Each side-effecting task defines a dedupe key where duplicates are harmful/costly:

- data product identity assignment
- acquisition (AcquireArtifact)
- validation result recording
- linking relationships

Idempotency keys are strictly internal and MUST NOT be part of event payload schemas.

---

## Concurrency Guidance (MVP)

- DiscoverSpectraProducts provider Map:
- MaxConcurrency: 1 (sequential providers)
- Reference reconciliation Map:
- MaxConcurrency: 5 (tunable)
- AcquireAndValidateSpectra:
- One `data_product_id` per execution (atomic Mode 1)

Principle:
Prefer lower concurrency to reduce throttling and retries (cost + stability).

---

## Throttling Principles

- Treat throttling as RETRYABLE.
- Prefer reducing concurrency over increasing retries.
- Persist cooldown windows to prevent repeated provider hammering.
- Record provider-level failure summaries in JobRun output for diagnosis.
