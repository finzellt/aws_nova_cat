# Epic 5 Implementation Playbook (Vertical Skeleton)

This document is the compact “context anchor” for Epic 5 implementation.
It is derived from authoritative docs:
- current-architecture.md
- dynamodb-item-model.md
- dynamodb-access-patterns.md
- execution-governance.md
- observability-plan.md
- events.py (Pydantic boundary event schemas)

If this playbook conflicts with those sources, the sources win.

---

## 1) Non-negotiable invariants

### Identity & boundaries
- Downstream of initialize_nova, workflows operate on UUIDs only (nova_id, data_product_id, reference_id).
- Names are not identifiers beyond initialize_nova.
- Spectra are atomic DataProducts; no dataset_id exists anywhere.

### Deterministic spectra identity
- Spectra identity is deterministic via (provider + provider_product_key + canonical_locator) → identity_key → UUID.
- LocatorAlias mapping is the dedupe authority:
  - PK = LOCATOR#<provider>#<locator_identity>
  - SK = DATA_PRODUCT#<data_product_id>

### Operational records
- One JobRun per workflow execution.
- One Attempt per Task invocation (including retries).
- Operational state is separate from scientific state.

### Eligibility removal semantics (critical)
- Spectra discovered are eligible for acquisition until validated/quarantined/terminal.
- Eligibility uses a GSI1 entry on the DataProduct.
- Once validation outcome is definitive, set eligibility=NONE and REMOVE eligibility index attributes so it no longer appears in eligibility queries.

### Correlation ID
- correlation_id SHOULD be present; if absent, workflow MUST create one and propagate it.
- correlation_id MUST be included in:
  - downstream events
  - JobRun/Attempt records
  - structured logs

### Idempotency keys
- Deterministic idempotency keys exist per workflow / per side-effecting task.
- Idempotency keys are internal-only and MUST NOT be part of boundary event schemas.

### Error taxonomy
- Errors are classified as: RETRYABLE, TERMINAL, QUARANTINE.
- Retry policies are owned by Step Functions definitions; lambdas classify errors consistently.

---

## 2) Required structured log contract (minimum)

All logs MUST be structured JSON.

Required fields:
- workflow_name
- execution_arn
- job_run_id
- state_name
- attempt_number
- correlation_id
- idempotency_key (internal-only; not in boundary schemas)
- primary identifier(s): nova_id and/or data_product_id and/or reference_id
- error_classification (RETRYABLE | TERMINAL | QUARANTINE)
- error_fingerprint (stable hash of normalized error cause)
- duration_ms (for completion logs)

A completion log line MUST include duration_ms.

---

## 3) DynamoDB contract matrix (Epic 5 scope)

### A) Nova
- PK = <nova_id>
- SK = NOVA

### B) NameMapping (initialize only)
- PK = NAME#<normalized_name>
- SK = NOVA#<nova_id>   (note: item-model and access-pattern docs differ in examples; follow item model formatting as implemented in repo)

### C) Spectra DataProduct
- PK = <nova_id>
- SK = PRODUCT#SPECTRA#<provider>#<data_product_id>

Must persist at minimum (MVP):
- data_product_id, product_type=SPECTRA, provider
- locator_identity + locators[]
- acquisition_status, validation_status
- eligibility (+ GSI1PK/GSI1SK when eligible)
- attempt_count (or attempt_count_total per governance doc; reconcile naming in code consistently)
- created_at/updated_at

### D) LocatorAlias
- PK = LOCATOR#<provider>#<locator_identity>
- SK = DATA_PRODUCT#<data_product_id>

### E) JobRun
- PK = <nova_id>
- SK = JOBRUN#<workflow_name>#<started_at>#<job_run_id>

### F) Attempt
- PK = <nova_id>
- SK = ATTEMPT#<job_run_id>#<task_name>#<attempt_no>#<timestamp>

---

## 4) Workflow → Task-state → Lambda-family mapping (Epic 5)

Epic 5 deploys 4 state machines:
- initialize_nova
- ingest_new_nova
- discover_spectra_products
- acquire_and_validate_spectra

We implement a small set of “task families” (shared Lambdas). Each SFN Task passes:
- workflow_name
- state_name
- attempt_number (from retry context or passed through)
- op (string)
- payload (the business payload)

Recommended families:
1) governance_task
2) idempotency_task
3) dispatch_task
4) nova_identity_task
5) spectra_discovery_task
6) spectra_av_task

Rules:
- Do not collapse an entire workflow into one Lambda.
- Preserve SFN Task states to preserve Attempt semantics and retry policies.

---

## 5) Governance + retries (implementation rules)

### Retry policy defaults
- Default (MVP): MaxAttempts=3; backoff 2s, 10s, 30s; retry only RETRYABLE.
- Long acquisition tasks: timeout up to 15 minutes; backoff 10s, 60s, 180s.

### Cooldown enforcement (acquire_and_validate_spectra)
Before acquisition:
1) If validation_status indicates already valid → short-circuit as SKIPPED_DUPLICATE
2) If now < next_eligible_attempt_at → short-circuit as SKIPPED_BACKOFF
3) Else proceed to acquisition attempt

On retryable failure:
- increment attempt_count_total
- compute next_eligible_attempt_at via capped exponential backoff
- persist last_error_fingerprint and last_attempt_outcome

Scientific enums MUST NOT encode retryability.

---

## 6) S3 layout (Epic 5)
Buckets:
- raw/
- derived/
- site/releases/

For spectra raw bytes (when implemented):
- raw/spectra/<nova_id>/<data_product_id>/primary.fits

Photometry canonical key (not Epic 5 goal, but invariant):
- derived/photometry/<nova_id>/photometry_table.parquet

---

## 7) “If you touch X, re-check Y” index

- Deterministic spectra ID minting → current-architecture.md + LocatorAlias item model
- Eligibility removal semantics → dynamodb-item-model.md + dynamodb-access-patterns.md + current-architecture.md
- Logging fields → observability-plan.md
- Retry/backoff/cooldown → execution-governance.md
- Event shapes → events.py
