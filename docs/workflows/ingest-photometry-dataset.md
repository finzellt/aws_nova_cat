# Workflow Spec: IngestPhotometryDataset

## Purpose
Dataset-scoped validation and ingestion of a photometry dataset, registering provenance and readiness for use.

## Triggers
- Manual upload/registration yields dataset_id and locator
- Future: discovery workflows (out of MVP scope)

## Event Contracts
### Input Event Schema
- Schema name: `IngestPhotometryDataset`
- Required: `dataset_id`, `nova_id`, data_locator, provenance
- Optional: correlation_id

### Output Event Schema
- Schema name: `PhotometryDatasetReady` (or equivalent)
- Includes: dataset_id, validation summary, provenance summary

## State Machine (Explicit State List)
1. ValidateInput (Pass)
2. BeginJobRun (Task)
3. AcquireIdempotencyLock (Task)
4. CheckIfAlreadyIngested (Task)
5. AlreadyIngested? (Choice)
   - Yes: FinalizeSuccess (SKIPPED_DUPLICATE)
   - No: continue
6. ValidatePhotometry (Task)
7. IngestMetadataAndProvenance (Task)
8. PublishPhotometryDatasetReady (Task)
9. FinalizeJobRunSuccess (Task)
10. QuarantineHandler (Task)
11. FinalizeJobRunQuarantined (Task)
12. TerminalFailHandler (Task)
13. FinalizeJobRunFailed (Task)

## Retry / Timeout Policy
- ValidatePhotometry:
  - Timeout 5m
  - Retry: none for deterministic validation errors; Retry MaxAttempts 2 for internal transient only
- IngestMetadataAndProvenance:
  - Timeout 30s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- PublishPhotometryDatasetReady:
  - Timeout 10s; Retry MaxAttempts 2

## Failure Classification Policy
- Retryable: transient infra failures, throttling
- Terminal: schema mismatch; missing dataset_id/nova_id; missing required provenance fields
- Quarantine: data readable but invalid columns/units/time formats; suspicious timestamps

## Idempotency Guarantees & Invariants
- Workflow idempotency key: `IngestPhotometryDataset:{dataset_id}:{schema_version}`
- Invariant: downstream usage depends on UUIDs only.

## JobRun / Attempt Emissions + Required Log Fields
- Required fields: nova_id, dataset_id, data_locator, validation_status, provenance_key(s).
