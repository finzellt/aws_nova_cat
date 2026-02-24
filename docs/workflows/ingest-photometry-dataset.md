# Workflow Spec: ingest_photometry_dataset

## Purpose
Dataset-scoped validation and ingestion of a photometry dataset, registering provenance and readiness for use.

Note: This workflow can act as a front door when the system has a photometry file/locator and can assign a dataset_id in that process
(if your upstream establishes dataset_id before launch, this workflow assumes it is provided).

## Triggers
- Manual upload/registration yields dataset_id and locator
- Operator trigger for known dataset_id

## Event Contracts
### Input Event Schema
- Schema name: `ingest_photometry_dataset`
- Schema path: `schemas/events/ingest_photometry_dataset/latest.json`
- Required identifiers: `dataset_id`, `nova_id` (if known), plus locator/provenance fields
- Optional: `correlation_id` (workflow generates if missing)

### Output Event Schema (Downstream Published Event)
- Typically no downstream workflow event required.
- If you maintain a “ready” event schema, it would be:
  - `schemas/events/photometry_dataset_ready/latest.json` *(optional, if exists)*

## State Machine (Explicit State List)
1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
3. **BeginJobRun** (Task)
4. **AcquireIdempotencyLock** (Task)
5. **CheckOperationalStatus** (Task)  <-- checks prior ingestion status for dataset_id
6. **AlreadyIngested?** (Choice)
   - Yes -> **FinalizeJobRunSuccess** (outcome = `SKIPPED_DUPLICATE`)
   - No  -> continue
7. **ValidatePhotometry** (Task)
8. **IngestMetadataAndProvenance** (Task)
9. **FinalizeJobRunSuccess** (Task) (outcome = `INGESTED`)
10. **QuarantineHandler** (Task)
11. **FinalizeJobRunQuarantined** (Task)
12. **TerminalFailHandler** (Task)
13. **FinalizeJobRunFailed** (Task)

## Retry / Timeout Policy (per state)
- BeginJobRun / AcquireIdempotencyLock:
  - Timeout 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- CheckOperationalStatus:
  - Timeout 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- ValidatePhotometry:
  - Timeout 5m
  - Retry: none for deterministic validation errors; MaxAttempts 2 for internal transient only
- IngestMetadataAndProvenance:
  - Timeout 30s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s

## Failure Classification Policy
- Retryable:
  - transient infra failures, throttling
- Terminal:
  - schema mismatch/version mismatch
  - missing required identifiers/locator/provenance fields
- Quarantine:
  - data readable but invalid columns/units/time formats
  - suspicious timestamps or ambiguous units

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
- Workflow idempotency key: `IngestPhotometryDataset:{dataset_id}:{schema_version}`
- Invariant: `idempotency_key` is internal-only (not in event schemas).

## JobRun / Attempt Emissions and Required Log Fields
- Required structured log fields:
  - workflow_name, execution_arn, job_run_id, state_name, attempt_number
  - schema_version, correlation_id, dataset_id, nova_id (if present)
  - validation_status, provenance_summary_key(s)
  - error_classification, error_fingerprint (if applicable)
