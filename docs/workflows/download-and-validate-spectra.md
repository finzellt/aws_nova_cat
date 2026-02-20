# Workflow Spec: DownloadAndValidateSpectra

## Purpose
Dataset-scoped acquisition and validation of a spectra dataset, producing a validated result suitable for downstream scientific use.

Terminology used here:
- Exists: some artifact is present
- Downloaded: bytes acquired and fingerprinted
- Validated: bytes passed integrity + format + lightweight domain sanity checks
- Published-ready/Ingested: (future) transformed/registered for publication

## Triggers
- Triggered by DiscoverSpectraProducts via dataset discovered events
- Manual/operator re-run for a dataset_id

## Event Contracts
### Input Event Schema
- Schema name: `DownloadAndValidateSpectra`
- Required: `dataset_id`, `nova_id`
- Required: product_locator (provider pointer) and/or expected identity fields
- Optional: expected_fingerprint, correlation_id

### Output Event Schema
- Schema name: `SpectraDatasetValidated` (or equivalent)
- Includes: dataset_id, validation summary, fingerprint, provenance summary

## State Machine (Explicit State List)
1. ValidateInput (Pass)
2. BeginJobRun (Task)
3. AcquireIdempotencyLock (Task)
4. CheckOperationalStatus (Task)  <-- checks prior Downloaded/Validated status
5. AlreadyValidated? (Choice)
   - Yes (same fingerprint/identity): PublishValidated (optional) + FinalizeSuccess (SKIPPED_DUPLICATE)
   - No: continue
6. EnsureDownloaded (Task)  <-- may short-circuit download if already Downloaded for same fingerprint/identity
7. ValidateBytes (Task)
8. RecordValidationResult (Task)
9. PublishSpectraDatasetValidated (Task)
10. FinalizeJobRunSuccess (Task)
11. QuarantineHandler (Task)
12. FinalizeJobRunQuarantined (Task)
13. TerminalFailHandler (Task)
14. FinalizeJobRunFailed (Task)

## Retry / Timeout Policy
- CheckOperationalStatus:
  - Timeout 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- EnsureDownloaded:
  - Timeout 15m; Retry MaxAttempts 3 on Retryable only; Backoff 10s, 60s, 180s
  - DO NOT retry on checksum mismatch -> Quarantine
- ValidateBytes:
  - Timeout 5m
  - Retry: none for deterministic validation failures; Retry MaxAttempts 2 for internal transient errors only
- RecordValidationResult:
  - Timeout 20s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- PublishSpectraDatasetValidated:
  - Timeout 10s; Retry MaxAttempts 2

## Failure Classification Policy
- Retryable: transient download failures, throttling, 5xx, timeouts
- Terminal: schema/version mismatch; missing/invalid dataset_id; missing required locator
- Quarantine:
  - checksum mismatch / fingerprint mismatch
  - parseable but fails format requirements
  - lightweight domain sanity checks fail (units missing/unknown, wavelength axis invalid, etc.)

## Idempotency Guarantees & Invariants
- Workflow idempotency key: `DownloadAndValidateSpectra:{dataset_id}:{schema_version}`
- Step dedupe keys:
  - Download: `Download:{dataset_id}:{expected_identity_or_fingerprint}`
  - Validate: `Validate:{dataset_id}:{content_fingerprint}`
- Invariant: validation is stronger than existence; “Exists” does not imply “Validated”.

## JobRun / Attempt Emissions + Required Log Fields
- Required fields: nova_id, dataset_id, provider, product_locator, content_fingerprint, validation_status, error_fingerprint.
