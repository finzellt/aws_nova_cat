# ADR-006: Documentation and Contract Alignment Pass

**Status:** Accepted
**Date:** 2026-03-05
**Epic:** Post-Epic-12 housekeeping

---

## Context

Following the completion of ADR-005 (Reference Model and ADS Integration) and its
Amendment (Discovery Date Precision), a systematic review of all authoritative
sources was performed to identify drift between the Pydantic contract models,
the DynamoDB item model, execution governance documentation, ADRs, and the
architecture baseline.

Drift was found across five categories:
1. Stale or incorrect content in ADRs
2. Enum values in documentation that did not match the contract models
3. Field name mismatches between code and documentation
4. Fields present in the contract models but absent from documentation
5. Fields present in documentation but absent from the contract models

This ADR records all changes made, preserving the audit trail required by ADR-004.

---

## Decisions

### 1. ADR Corrections

**ADR-0001 (Contract Definition and Schema Governance)**
- Enhanced the top-of-file disclaimer to explicitly name the current authoritative
  entity list and to clarify that `Dataset` references below it are pre-Epic-4
  historical context only. The body of the ADR is unchanged per the policy of not
  rewriting history.

**ADR-002 (Workflow Orchestration and Execution Model)**
- Corrected the document heading from `ADR-003` to `ADR-002` (filename and header
  were mismatched).
- Changed `Status` from `Proposed` to `Accepted`. The ADR had been implemented and
  partially superseded by ADR-004 and ADR-005, but was never formally accepted.

**ADR-005 (Reference Model and ADS Integration) — Decision 3**
- Decision 3 incorrectly described the partition key as `PK=REFERENCE#<reference_id>`
  and the dedup strategy as `source + source_key`. The implemented model uses
  `PK=REFERENCE#<bibcode>` with a direct `GetItem` dedup on bibcode only. No
  `reference_id`, `source`, or `source_key` fields exist on `Reference` items.
  Decision 3 was rewritten to reflect the actual implementation.

**ADR-005 (Reference Model and ADS Integration) — Open Item (Discovery Date Precision)**
- The "Open Item" section still described `Nova.discovery_date` as `datetime | None`
  and the precision question as unresolved. This was resolved by ADR-005 Amendment
  (accepted 2026-03-03). The section has been annotated as resolved with a summary
  of the decision and a pointer to the Amendment.

---

### 2. Enum Value Synchronization

The following enum fields in documentation were missing values present in the
contract models. All documentation has been updated to match the models.

| Entity | Field | Was | Now |
|---|---|---|---|
| Nova | `status` | `ACTIVE \| MERGED \| DEPRECATED` | + `QUARANTINED` |
| DataProduct (spectra) | `acquisition_status` | `STUB \| ACQUIRED \| FAILED` | + `FAILED_RETRYABLE \| SKIPPED_DUPLICATE \| SKIPPED_BACKOFF` |
| DataProduct (spectra) | `validation_status` | `UNVALIDATED \| VALID \| QUARANTINED` | + `TERMINAL_INVALID` |
| JobRun | `status` | `RUNNING \| SUCCEEDED \| FAILED` | + `QUEUED \| QUARANTINED \| CANCELLED` |
| Attempt | `status` | `STARTED \| SUCCEEDED \| FAILED` | + `TIMED_OUT \| CANCELLED` |

Changes applied to: `execution-governance.md`, `dynamodb-item-model.md`.

---

### 3. Canonical Field Names

The following field names were inconsistent across code and documentation. The
canonical names below are now used in all authoritative sources.

| Canonical name | Was (in code) | Was (in docs) | Applies to |
|---|---|---|---|
| `started_at` | `initiated_at` | `started_at` | `JobRun` |
| `ended_at` | `finished_at` | `ended_at` | `JobRun`, `Attempt` |
| `attempt_number` | `attempt_number` | `attempt_no` | `Attempt` |
| `error_type` | `error_code` | `error_type` | `Attempt` |

Code changes applied to: `entities.py` (`JobRun`, `Attempt` classes and their
`model_validator` guards).
Documentation changes applied to: `dynamodb-item-model.md` (Attempt SK, fields,
example).

Note: `attempt_number` and `attempt_no` were found to be used interchangeably with
no semantic difference. `attempt_number` is adopted as the canonical name for
explicitness and consistency with the existing code.

---

### 4. Fields Added to Documentation

The following fields existed in the contract models but were absent from the
DynamoDB item model documentation. They have been added.

| Entity | Field | Notes |
|---|---|---|
| DataProduct (spectra) | `last_attempt_outcome` | Operational outcome of most recent attempt; kept separate from `validation_status` per scientific/operational state separation invariant |
| DataProduct (spectra) | `duplicate_of_data_product_id` | Set when a byte-level duplicate of an existing validated product is detected |
| JobRun | `initiated_by` | Actor or service that initiated the run |

Changes applied to: `dynamodb-item-model.md`.

---

### 5. Fields Added to Contract Models

The following fields were present in the DynamoDB item model and item key structure
but absent from the contract models. They have been added to `entities.py`.

| Entity | Field | Notes |
|---|---|---|
| Attempt | `task_name: str \| None` | Step Functions state name; embedded in the Attempt SK |
| Attempt | `duration_ms: int \| None` | Wall-clock duration of the attempt |

---

### 6. Fields Removed from Documentation

| Document | Field | Reason |
|---|---|---|
| `execution-governance.md` | `last_successful_fingerprint` | Superseded by explicit checksum fields (`sha256`, `header_signature_hash`) on `DataProduct`. Removed to eliminate ambiguity. |

---

### 7. Architecture Baseline Additions

Two minor omissions in `current-architecture.md` were corrected:

- **Section 3.1 (Nova):** `discovery_date` field now annotated with its format
  (`YYYY-MM-DD` string; day `00` for month-only precision). References ADR-005
  Amendment.
- **Section 6 (Persistence Model):** `WORKFLOW#<correlation_id>` global partition
  added to the list of DynamoDB partition types, with a note that it holds
  pre-nova `FileObject` records (e.g., during `initialize_nova` quarantine).

---

## Consequences

- All five authoritative sources (contracts, DynamoDB item model, workflow specs,
  architecture baseline, ADRs) are now aligned as of 2026-03-05.
- Schema regeneration from updated contracts is required before deploying any code
  that references `JobRun.started_at`, `JobRun.ended_at`, `Attempt.ended_at`,
  `Attempt.error_type`, `Attempt.task_name`, or `Attempt.duration_ms`.
- Any existing DynamoDB items that contain `initiated_at`, `finished_at`, or
  `error_code` keys will need a migration or read-time coercion if backward
  compatibility is required.
- The policy of preserving ADR body text as historical record (established
  informally during Epic 4) is now explicit: obsolete ADRs are annotated at the
  top with disclaimers, but their body text is not rewritten.

---

## References

- ADR-004 — Architecture Baseline and Alignment Policy (authoritative source
  hierarchy; this ADR follows that hierarchy)
- ADR-005 — Reference Model and ADS Integration
- ADR-005 Amendment — Discovery Date Precision
