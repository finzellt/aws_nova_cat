# Execution Governance

This document defines Step Functions execution conventions for Nova Cat workflows.
It is intentionally “code-adjacent” and designed to be translated into definitions later.

## Standard Retry & Backoff Conventions
### Default (MVP)
- MaxAttempts: 3
- Backoff: 2s, 10s, 30s
- Retry only errors classified as RETRYABLE
- No unbounded retries

### Long-running download tasks (MVP)
- Timeout: up to 15 minutes
- MaxAttempts: 3
- Backoff: 10s, 60s, 180s
- Checksum mismatch is NOT retryable (Quarantine)

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
- missing/invalid UUIDs (nova_id/dataset_id/reference_id)
- invalid required fields (e.g., missing locator/provenance)
- logical impossibility (e.g., resolved object explicitly not a classical nova)

### QUARANTINE
Examples:
- ambiguous name resolution matches
- spectra content fails integrity/format/domain sanity checks
- provider returns malformed or inconsistent records requiring review

## Correlation ID Rules
- Every workflow input SHOULD include `correlation_id`.
- If absent, the workflow MUST create one (e.g., UUID) and propagate it.
- `correlation_id` MUST be included in:
  - all published downstream events
  - all JobRun/Attempt records
  - structured logs

## Idempotency Key Rules
### Workflow-level
- Every workflow defines a deterministic idempotency key.
- Workflows that poll/refresh external sources MUST be time-bucketed.

### Step-level
- Each side-effecting task defines a dedupe key where duplicates are harmful/costly:
  - dataset identity assignment
  - downloads
  - validation result recording
  - linking relationships

## Concurrency Guidance (MVP)
- DiscoverSpectraProducts provider Map MaxConcurrency: 1 (sequential providers)
- Reference reconciliation Map MaxConcurrency: 5 (tunable)
- Principle: prefer lower concurrency to reduce throttling and retries (cost + stability)

## Throttling Principles
- Treat throttling as RETRYABLE.
- Prefer reducing concurrency over increasing retries.
- Record provider-level failure summaries in JobRun output for diagnosis.
