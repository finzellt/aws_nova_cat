# Observability Plan (Workflow Level)

## Logging

### CloudWatch (platform logs)

- Step Functions execution history and Lambda logs are the baseline.
- All logs MUST be structured JSON.

---

## Required Structured Log Fields (minimum)

- workflow_name
- execution_arn
- job_run_id
- state_name
- attempt_number
- correlation_id
- idempotency_key (internal-only; not part of boundary schemas)
- primary identifiers (one or more):
  - nova_id
  - data_product_id
  - reference_id
- error_classification (RETRYABLE | TERMINAL | QUARANTINE)
- error_fingerprint (stable hash of normalized error cause)
- duration_ms (for task completion logs)

No `dataset_id` exists in the system.

---

## Metrics (key signals)

Per workflow:

- executions_started
- executions_succeeded
- executions_failed_terminal
- executions_quarantined
- executions_skipped_duplicate
- execution_duration_ms (p50/p95)

Cross-cutting:

- task_attempts_total (by workflow/state)
- retry_count (by workflow/state)
- quarantine_count (by provider/source)
- spectra_validation_status_counts
- photometry_ingestion_counts

---

## Alarms (high-level)

- Elevated terminal failure rate:
  - failed_terminal / started above threshold over N runs

- Elevated quarantine rate:
  - quarantined / started above threshold
  - especially provider-specific for spectra

- Provider health:
  - spikes in RETRYABLE failures for a given provider/source

- Latency regressions:
  - p95 duration exceeds historical baseline (workflow-specific)

---

## Join Keys / Traceability

- `job_run_id` is the operational join key across:
  - execution history
  - Attempt records
  - logs

- `correlation_id` joins cross-workflow chains
  (e.g., initialize_nova → ingest_new_nova → downstream workflows)

- `nova_id` and `data_product_id` are the canonical domain join keys.
