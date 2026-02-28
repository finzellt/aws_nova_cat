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


# S2 — Persistence & Data Modeling
## Context Packet

Surface-Specific Invariants:

INV-010
Single-table model only.

INV-011
All DB interactions through Persistence Wrapper.

INV-012
Workflow idempotency enforced via conditional writes + JobRun uniqueness.

Workflow Extracted Strata:

Common across workflows:

- BeginJobRun → conditional write
- AcquireIdempotencyLock → conditional write
- JobRun and Attempt item structure
- Eligibility index shape:
  GSI1PK = nova_id
  GSI1SK = ELIG#<eligibility>#SPECTRA#...
- Photometry singleton rule:
  SK = PRODUCT#PHOTOMETRY_TABLE

S3 Layout Constraints:

- Raw bytes immutable
- Photometry canonical overwrite rule
- Snapshot only on schema boundary

Forbidden Cross-Surface Operations:

- No direct DB client calls
- No workflow branching logic modifications
- No identity normalization logic changes
