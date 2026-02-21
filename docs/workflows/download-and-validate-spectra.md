# Workflow Spec: download_and_validate_spectra

## Purpose

Dataset-scoped acquisition and validation of a spectra dataset.

Validation is profile-driven and aligned with IVOA Spectrum Data Model conventions where possible (see docs/specs/spectra-fits-profiles.md).

Terminology:
- Exists
- Downloaded
- Validated
- Published-ready (future)

---

## Triggers
- Triggered by `discover_spectra_products` via `download_and_validate_spectra` schema
- Scheduled (time-bucketed)

## Event Contracts

### Input Event Schema
- Schema name: `download_and_validate_spectra`
- Schema path: `schemas/events/download_and_validate_spectra/latest.json`
- Required: `nova_id`, `dataset_id`
- Optional: `correlation_id` (generated if missing)

### Output Event Schema
- Optional validated event if defined:
  `schemas/events/spectra_dataset_validated/latest.json`

---

## State Machine
1. ValidateInput
2. EnsureCorrelationId
3. BeginJobRun
4. AcquireIdempotencyLock
5. LoadDatasetMetadata
6. CheckOperationalStatus
7. AlreadyValidated?
8. AcquireArtifact
9. ValidateBytes (Profile-Driven)
10. RecordValidationResult
11. FinalizeJobRunSuccess
12. QuarantineHandler
13. FinalizeJobRunQuarantined
14. TerminalFailHandler
15. FinalizeJobRunFailed

---

## Acquisition
AcquireArtifact:

- Download file(s) using provider adapter
- If ZIP → unpack
- Compute fingerprint
- Store temporary artifacts for validation

Provider-specific acquisition logic may be implemented via:
- Single Lambda with provider dispatch
- Internal provider plugin modules

No Step Functions branching required.

---

## Validation (Profile-Driven)
ValidateBytes:

1. Open FITS file
2. Select FITS Profile using:
   - provider
   - optional header signature fields
3. Extract spectral arrays using profile mapping
4. Normalize header fields into canonical metadata
5. Apply lightweight domain sanity checks
6. Produce validation result summary

If no profile matches → QUARANTINE.

---

## Retry / Timeout Policy
- AcquireArtifact:
  Timeout: 15m
  Retry: Retryable only
- ValidateBytes:
  Timeout: 5m
  Deterministic validation failures are NOT retried

---

## Failure Classification
- Retryable:
  transient download failures
- Terminal:
  invalid identifiers
- Quarantine:
  checksum mismatch
  unreadable FITS
  unknown profile
  missing required header fields
  invalid units

---

## Idempotency
Workflow idempotency key:
`DownloadAndValidateSpectra:{dataset_id}:{schema_version}`

Step-level dedupe keys:
- Download:{dataset_id}:{expected_identity}
- Validate:{dataset_id}:{content_fingerprint}

Internal only.

---

## Invariants
- Validation is stronger than existence.
- Profile logic must not alter UUID identity.
- correlation_id propagated to all records/logs.
- No provider branching in state machine.
