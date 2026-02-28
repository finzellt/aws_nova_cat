# Surface: Surface S1 — Identity & Locator Authority

---

## 1. Surface Map Entry (Verbatim Copy)

Source: /mnt/data/S1-info.md

## Surface S1 — Identity & Locator Authority

Description:
Canonical identity formation, name normalization, locator identity normalization, deterministic UUID derivation, identity quarantine thresholds.

Primary Responsibilities:

- Enforce UUID-first identity (downstream of name boundary)
- Implement deterministic locator identity ladder
- Maintain NAME# and LOCATOR# partitions
- Coordinate duplicate detection thresholds (<2", 2–10", >10")
- Provide identity utilities to workflows
- Preserve UUID stability under alias updates

Invariants:

- INV-001 — UUID Primary Identity
- INV-002 — Deterministic Locator Identity

Public Interface IDs:

- IFC-020 — Persisted Entity Schema
- IFC-050 — Error Taxonomy + Classification Surface

Wrapper Dependencies:

- Persistence Wrapper
- Error Classification Wrapper

Stability Level:
Stabilizing

---

## 2. Expanded Invariants (Verbatim)

Source: /mnt/data/invariant-registry.md

### INV-001 — UUID Primary Identity

- **Category:** Identity
- **Level:** L1
- **Statement:** All canonical entities are identified by system-generated UUIDs.
- **Rationale:** Prevents ambiguity, ensures stable cross-surface references.
- **Enforcement Mechanism:** Schema validation + entity construction tests.
- **Violation Handling:** Requires ADR.
- **Owner:** Identity Surface Owner

---

### INV-002 — Deterministic Locator Identity

- **Category:** Identity
- **Level:** L1
- **Statement:** Locator records must derive their identity deterministically from the provider plus a canonical `locator_identity`, producing a stable key of the form:
  `LOCATOR#<provider>#<locator_identity>`

  The `locator_identity` must be computed using the following precedence:

  1. **Preferred:** `provider_product_id:<id>` (where `provider_product_id` is the provider's native product identifier, if present)
  2. **Fallback:** `url:<normalized_url>` (where `normalized_url` is a canonicalized/normalized URL)

- **Rationale:** Ensures stable deduplication and aliasing even when providers do not supply native product IDs.
- **Enforcement Mechanism:** Persistence wrapper tests verifying:
  - correct precedence selection when `provider_product_id` exists
  - deterministic URL normalization and key formation when it does not
- **Violation Handling:** Requires ADR.
- **Owner:** Persistence Surface Owner

---

## 3. Expanded Public Interface Definitions (Verbatim)

Source: /mnt/data/public-interface-catalog.md

### IFC-020 — Persisted Entity Schema

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

### IFC-050 — Error Taxonomy + Classifier Surface

- **Category:** Error API
- **Status:** Active
- **Owner:** Workflow Semantics Surface Owner
- **Approval Authority:** Root Facilitator
- **Primary Consumers:** All workflows
- **Location:** `.../errors/...` (placeholder)
- **Source of Truth:** Shared error types + classification helper
- **Version:** v1
- **Stability:** Stable
- **Compatibility Policy:** Classification meanings are stable; new error types allowed as MINOR
- **Change Process:** Update classifier + update mapping tests
- **Verification:** classification mapping completeness tests
- **Notes:** Prevent ad hoc string classification

---

## 4. Expanded Workflow Excerpts (Verbatim)

### Workflow: initialize_nova

Source: /mnt/data/initialize-nova.md

#### CoordinateMatchClassification? (Choice)

- Separation < 2"  -> **UpsertAliasForExistingNova** -> **PublishIngestNewNova**
  -> **FinalizeJobRunSuccess** (outcome = `EXISTS_AND_LAUNCHED`)
- Separation 2"–10" -> **QuarantineHandler** -> **FinalizeJobRunQuarantined**
- Separation > 10"  -> continue

---

#### Idempotency Guarantees & Invariants

- Invariant: downstream workflow launches use UUIDs only (`nova_id`).
- Invariant: `idempotency_key` is INTERNAL ONLY and must not be required in event schemas.

- Short-circuit behaviors:
  - If name exists -> launch `ingest_new_nova` and do not create a new `nova_id`.
  - If coordinates match an existing nova within 2" -> upsert alias and launch `ingest_new_nova` with the existing `nova_id`.

---

### Workflow: discover_spectra_products

Source: /mnt/data/discover-spectra-products.md

#### Identity Ladder (Discovery-time)

For each discovered provider record, the workflow MUST determine an `identity_strategy`:

1. **NATIVE_ID (Strong)**
   If a provider-native product identifier is available, it MUST be used as the primary identity key.

2. **METADATA_KEY (Strong, fallback)**
   If no native ID exists, a strong metadata key MAY be used if sufficient fields are present.
   Recommended minimum fields:
   - `provider`
   - `instrument`
   - `observation_time` (or equivalent timestamp)
   Additional fields may be included (e.g., telescope, program_id, pipeline_tag).

3. **WEAK (Defer)**
   If neither (1) nor (2) can be constructed deterministically, identity is considered WEAK and definitive dedupe MUST be deferred to `acquire_and_validate_spectra` (byte-level fingerprint match).

The workflow SHOULD persist `identity_strategy` (`NATIVE_ID|METADATA_KEY|WEAK`) on the data product record for diagnostics.

---

#### Locator Alias Rule

If a discovered record resolves to an existing `data_product_id`, and the newly discovered locator is not already recorded, it MUST be persisted as an additional locator alias.

---

#### Invariants

- No name-based logic (UUID-first)
- Only UUIDs are published downstream (`nova_id`, `data_product_id`)
- `correlation_id` is propagated when present; generated if missing
- `idempotency_key` is internal-only and MUST NOT be part of event payloads

---

### Workflow: acquire_and_validate_spectra

Source: /mnt/data/acquire-and-validate-spectra.md

#### Post-acquisition Duplicate Detection (Byte-level)

After acquisition (and once bytes are available), the workflow MUST:

1. Compute a stable `content_fingerprint` (e.g., SHA-256 of canonical bytes or a deterministic normalization).
2. Check whether an existing **VALIDATED** data product already has the same fingerprint.
3. If a match exists:
   - Mark the current data product as a duplicate of the canonical product (e.g., `duplicate_of_data_product_id = <canonical>`).
   - Optionally append this product’s locator(s) as aliases to the canonical product.
   - Finalize the JobRun successfully with outcome `DUPLICATE_OF_EXISTING`.
   - The current data product MUST NOT be marked `VALIDATED`.
4. If no match exists:
   - Continue normal validation result recording and mark `VALIDATED`.

This preserves stable UUIDs while avoiding duplicate scientific products downstream.

---

#### Idempotency Guarantees & Invariants

- Exactly one `data_product_id` per execution (MVP Mode 1)
- “AlreadyValidated?” is a defensive guardrail (replays happen)
- Cooldown prevents hammering providers across repeated executions
- Profile logic must not alter UUID identity
- `idempotency_key` remains internal-only (never part of event payload)

---

## 5. Wrapper Obligations (Verbatim)

Source: /mnt/data/wrapper-charter.md

### Persistence Surface — Rule

No Lambda or workflow component may call the database client directly.

All persistence operations must go through the Persistence Wrapper API.

---

### Error Classification Surface — Rule

Error classification must use shared error types/helpers.

No ad hoc string-based classification.

---

## 6. Governance Constraints (Verbatim)

Source: /mnt/data/execution-governance.md

### Correlation ID Rules

- Every workflow input SHOULD include `correlation_id`.
- If absent, the workflow MUST create one (UUID) and propagate it.
- `correlation_id` MUST be included in:
  - all published downstream events
  - all JobRun/Attempt records
  - structured logs

---

### Idempotency Key Rules

#### Workflow-level

- Every workflow defines a deterministic idempotency key.
- Workflows that poll/refresh external sources MUST be time-bucketed.

#### Step-level

Each side-effecting task defines a dedupe key where duplicates are harmful/costly:

- data product identity assignment
- acquisition (AcquireArtifact)
- validation result recording
- linking relationships

Idempotency keys are strictly internal and MUST NOT be part of event payload schemas.

---

## 7. Explicit Forbidden Operations

- May not introduce DB access outside Persistence Wrapper
- May not redefine error taxonomy
- May not modify workflow state transitions

---

## 8. Context Boundary Declaration

- This surface may not expand scope beyond canonical identity and locator authority.
- All cross-surface changes require escalation.
- All persistence must go through the Persistence Wrapper.
- All classification must use the shared error taxonomy.
- All invariants listed herein are binding.
- Modifying any invariant requires ADR, registry update, enforcement update, and facilitator approval.
