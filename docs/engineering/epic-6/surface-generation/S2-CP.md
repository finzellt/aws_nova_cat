# Surface: Surface S2 — Persistence & Data Modeling

## Document Sources Supplied

The following authoritative documents were provided for expansion:

- /mnt/data/surface-map.md
- /mnt/data/S2-info.md
- /mnt/data/invariant-registry.md
- /mnt/data/public-interface-catalog.md
- /mnt/data/wrapper-charter.md
- /mnt/data/execution-governance.md
- /mnt/data/s3-layout.md
- /mnt/data/initialize-nova.md
- /mnt/data/discover-spectra-products.md
- /mnt/data/acquire-and-validate-spectra.md

---

## 1. Surface Map Entry (Verbatim Copy)

Source: /mnt/data/S2-info.md

## Surface S2 — Persistence & Data Modeling

Description:
Single-table DynamoDB modeling, item shapes, GSIs, S3 pointer integrity, conditional write enforcement.

Primary Responsibilities:

- Enforce single-table model
- Enforce wrapper-only persistence access
- Enforce idempotency conditional writes
- Maintain eligibility index correctness
- Maintain product-type-first sort ordering
- Maintain photometry singleton-per-nova rule
- Maintain schema_version evolution safety

Invariants:

- INV-010 — Single-Table Model
- INV-011 — Wrapper-Only Persistence Access
- INV-012 — Workflow Idempotency Enforcement

Public Interface IDs:

- IFC-030 — Persistence Wrapper API Surface
- IFC-020 — Persisted Entity Schemas

Wrapper Dependencies:

- Persistence Wrapper (primary)
- Observability Wrapper (secondary logging)

Stability Level:
Stabilizing

---

## 2. Expanded Invariants (Verbatim)

Source: /mnt/data/invariant-registry.md

### INV-010 — Single-Table Model

- **Category:** Persistence
- **Level:** L2
- **Statement:** The system uses a single-table persistence model.
- **Rationale:** Simplifies access patterns and identity partitioning.
- **Enforcement Mechanism:** Architectural review.
- **Violation Handling:** ADR required.
- **Owner:** Persistence Surface Owner

---

### INV-011 — Wrapper-Only Persistence Access

- **Category:** Persistence
- **Level:** L1
- **Statement:** All database interactions must occur through the Persistence Wrapper API.
- **Rationale:** Centralizes invariant enforcement and reduces drift.
- **Enforcement Mechanism:** Static scan / allowlist + PR checklist.
- **Violation Handling:** Merge blocked.
- **Owner:** Root Facilitator

---

### INV-012 — Idempotency Enforcement at Workflow Level

- **Category:** Persistence
- **Level:** L1
- **Statement:** Workflow idempotency must be enforced via JobRun uniqueness and conditional writes.
- **Rationale:** Prevents duplicate execution side effects.
- **Enforcement Mechanism:** Persistence tests verifying conditional write behavior.
- **Violation Handling:** ADR required.
- **Owner:** Workflow Surface Owner

---

## 3. Expanded Public Interface Definitions (Verbatim)

Source: /mnt/data/public-interface-catalog.md

### IFC-030 — Persistence Wrapper API Surface

- **Category:** Wrapper API
- **Status:** Active
- **Owner:** Persistence Surface Owner
- **Approval Authority:** Root Facilitator
- **Primary Consumers:** All lambdas/workflows
- **Location:** `.../persistence/...` (placeholder)
- **Source of Truth:** Wrapper module API
- **Version:** v1
- **Stability:** Stable
- **Compatibility Policy:** Breaking changes require MAJOR + ADR; prefer additive expansion
- **Change Process:** Update wrapper + update access-pattern tests
- **Verification:** allowlist/static scan + persistence invariant test suite
- **Notes:** Direct DB client access is forbidden by Wrapper Charter

---

### IFC-020 — Example Placeholder: Persisted Entity Schema

- **Category:** Persisted Schema
- **Status:** Draft
- **Owner:** TBD
- **Approval Authority:** Root Facilitator
- **Primary Consumers:** Any reader across time (future deployments)
- **Location:** `contracts/entities/...` (placeholder)
- **Source of Truth:** Pydantic model(s)
- **Version:** v0
- **Stability:** Evolving
- **Compatibility Policy:** Strong backward compatibility; breaking changes require migration plan + ADR
- **Change Process:** Update schema + update fixture corpus + add migration notes if needed
- **Verification:** schema snapshots + compatibility tests
- **Notes:** Persisted data is effectively a “consumer”

---

## 4. Expanded Workflow Excerpts (Verbatim)

### Workflow: initialize_nova

Source: /mnt/data/initialize-nova.md

Relevant Sections:

- **BeginJobRun** (Task)
- **AcquireIdempotencyLock** (Task)

- BeginJobRun:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- AcquireIdempotencyLock:
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s

---

### Workflow: discover_spectra_products

Source: /mnt/data/discover-spectra-products.md

Relevant Sections:

5. **DiscoverAcrossProviders** (Map)
   - **QueryProviderForProducts** (Task)
   - **NormalizeProviderProducts** (Task)
   - **DeduplicateAndAssignDataProductIds** (Task)
   - **PersistDataProductMetadata** (Task)
   - **PublishAcquireAndValidateSpectraRequests** (Task)

---

### Workflow: acquire_and_validate_spectra

Source: /mnt/data/acquire-and-validate-spectra.md

Relevant Sections:

4. **AcquireIdempotencyLock** (Task)

Persisted Operational Fields (Data Product):

Minimum viable fields:
- `validation_status`
- `attempt_count_total`
- `last_attempt_at`
- `last_attempt_outcome`
- `last_error_fingerprint`
- `next_eligible_attempt_at`
- `last_successful_fingerprint`
- `content_fingerprint`
- `duplicate_of_data_product_id`

---

### Eligibility Index Shape

GSI1PK = nova_id
GSI1SK = ELIG#<eligibility>#SPECTRA#...

---

### Photometry Singleton Rule

SK = PRODUCT#PHOTOMETRY_TABLE

---

## 5. Wrapper Obligations (Verbatim)

Source: /mnt/data/wrapper-charter.md

### 2.1 Persistence Surface

### Rule
No Lambda or workflow component may call the database client directly.

All persistence operations must go through the Persistence Wrapper API.

---

### Responsibilities of Persistence Wrapper
- Enforce key shape rules
- Enforce conditional write invariants
- Enforce idempotency behavior
- Normalize item structure
- Centralize serialization/deserialization
- Emit structured persistence logs

---

## 6. Governance Constraints (Verbatim)

Source: /mnt/data/execution-governance.md

### Idempotency Key Rules

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

Source: /mnt/data/s3-layout.md

### S3 Layout Constraints

- Raw bytes immutable
- Photometry canonical overwrite rule
- Snapshot only on schema boundary

---

## 7. Explicit Forbidden Cross-Surface Operations

- No direct DB client calls
- No workflow branching logic modifications
- No identity normalization logic changes

---

## 8. Context Boundary Declaration

- This surface may not expand scope beyond its assigned responsibilities.
- Cross-surface changes require escalation.
- All persistence must go through wrapper.
- All classification must use shared taxonomy.
- All listed invariants are binding.
- Any invariant modification requires ADR.
