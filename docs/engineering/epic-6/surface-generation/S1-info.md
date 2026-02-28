
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


# Context Packets

---

# S1 — Identity & Locator Authority
## Context Packet

Surface-Specific Invariants:

INV-001
All canonical entities are identified by system-generated UUIDs.
Names are never used downstream of initialize_nova.

INV-002
Locator identity must be deterministic.

Precedence:

1. provider_product_id:<id>
2. url:<normalized_url>

Key form:

LOCATOR#<provider>#<locator_identity>

Workflow-Specific Extracted Strata:

From initialize_nova:

- NAME# partition lookup
- Coordinate threshold logic (<2", 2–10", >10")
- Alias upsert rules

From discover_spectra_products:

- identity_strategy: NATIVE_ID | METADATA_KEY | WEAK
- LocatorAlias enforcement

From acquire_and_validate_spectra:

- Byte-level duplicate detection
- duplicate_of_data_product_id marking
- UUID stability requirement

Contract Constraints:

- Only UUIDs published downstream
- idempotency_key never part of payload
- Name normalization must be deterministic

Forbidden Cross-Surface Operations:

- May not introduce DB access outside Persistence Wrapper
- May not redefine error taxonomy
- May not modify workflow state transitions
