# Workflow Spec: ingest_photometry

## Purpose

Ingest a photometry source file via API, update the per-nova photometry table (`PHOTOMETRY_TABLE` data product), and persist provenance and ingestion summary metadata.

There is no dataset abstraction.

This workflow rebuilds and overwrites the canonical photometry table for a nova. Snapshots occur only when the photometry schema version changes.

---

## Triggers

- API upload of a photometry file
- Operator-triggered ingestion for a known nova

The workflow may accept a `candidate_name` and resolve it to `nova_id`, or accept `nova_id` directly if already known.

---

## Event Contracts

### Input Event Schema

- Schema name: `ingest_photometry`
- Schema path: `schemas/events/ingest_photometry/latest.json`
- Required identifiers:
  - `candidate_name` OR `nova_id`
- Optional:
  - `correlation_id`

No `dataset_id` is used.

If `correlation_id` is missing, the workflow MUST generate one.

### Output Event Schema

No downstream workflow event is required.

---

## State Machine (Explicit State List)

1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
3. **ResolveNovaId** (Task) — if input is `candidate_name`
4. **BeginJobRun** (Task)
5. **AcquireIdempotencyLock** (Task)
6. **CheckOperationalStatus** (Task)
7. **AlreadyIngested?** (Choice)
   - Yes → **FinalizeJobRunSuccess** (outcome = `SKIPPED_DUPLICATE`)
   - No  → continue
8. **ValidatePhotometry** (Task)
9. **RebuildPhotometryTable** (Task)
10. **PersistPhotometryMetadata** (Task)
11. **FinalizeJobRunSuccess** (Task) (outcome = `INGESTED`)
12. **QuarantineHandler** (Task)
13. **FinalizeJobRunQuarantined** (Task)
14. **TerminalFailHandler** (Task)
15. **FinalizeJobRunFailed** (Task)

---

## Retry / Timeout Policy (per state)

- BeginJobRun / AcquireIdempotencyLock:
  - Timeout 10s
  - Retry MaxAttempts 3; Backoff 2s, 10s, 30s

- CheckOperationalStatus:
  - Timeout 10s
  - Retry MaxAttempts 3; Backoff 2s, 10s, 30s

- ValidatePhotometry:
  - Timeout 5m
  - Retry: none for deterministic validation errors
  - MaxAttempts 2 for transient internal failures only

- RebuildPhotometryTable:
  - Timeout 5m
  - Retry MaxAttempts 2 for transient failures

- PersistPhotometryMetadata:
  - Timeout 30s
  - Retry MaxAttempts 3; Backoff 2s, 10s, 30s

---

## Canonical Overwrite Semantics

Routine ingestion under the same schema version:

- Rebuild the canonical photometry table
- Overwrite:
  derived/photometry/<nova_id>/photometry_table.parquet

No snapshot is created.

---

## Schema-Change Snapshot Semantics (Forward-Compatible)

If `photometry_schema_version` changes:

1. Copy the existing canonical table to:
   derived/photometry/<nova_id>/snapshots/schema=<old_schema_version>/...

2. Write the new canonical table using the new schema version.

Snapshots represent schema boundaries only and are not created for normal ingestion.

Schema migration workflows may be documented but not implemented in MVP.

---

## Failure Classification Policy

- Retryable:
  - transient infrastructure failures
  - throttling
  - temporary S3/Dynamo errors

- Terminal:
  - missing required identifiers
  - malformed file
  - unsupported schema version

- Quarantine:
  - data readable but invalid columns
  - invalid units
  - malformed timestamps
  - ambiguous or inconsistent data

---

## Quarantine Handling

When transitioning to **QuarantineHandler**, the workflow MUST:

1. Persist quarantine status and diagnostic metadata.
2. Emit a JobRun outcome of `QUARANTINED`.
3. Publish a notification event to an SNS topic for operational review.

SNS notification requirements:
- workflow name
- primary identifier (`nova_id`)
- `correlation_id`
- `error_fingerprint`
- brief classification reason

SNS delivery is best-effort and MUST NOT cause workflow failure.

---

## Idempotency Guarantees & Invariants

Workflow idempotency key:

IngestPhotometry:{nova_id}:{photometry_schema_version}

Invariant:
- Idempotency keys are internal-only.
- No idempotency key appears in boundary schemas.

---

## JobRun / Attempt Emissions and Required Log Fields

Required structured log fields:

- workflow_name
- execution_arn
- job_run_id
- state_name
- attempt_number
- photometry_schema_version
- correlation_id
- nova_id
- error_classification (if applicable)
- error_fingerprint (if applicable)
