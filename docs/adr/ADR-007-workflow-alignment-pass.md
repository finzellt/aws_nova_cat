# ADR-007: ADS Query Strategy, Quarantine Handler Scope, and Discovery Simplifications

**Status:** Accepted
**Date:** 2026-03-06

---

## Context

Three architectural decisions made during or after Epic 12 had no explicit record:
the ADS query strategy actually implemented (which differed from ADR-005 Decision 2),
the intended scope of the shared `quarantine_handler`, and the removal of a workflow
state whose purpose had been subsumed by adjacent infrastructure.

---

## Decisions

### 1. ADS Query Strategy: Name-Only (supersedes ADR-005 Decision 2)

`FetchReferenceCandidates` issues a **single name-based query** against the ADS
search API. All known nova aliases are OR-joined into one query string:

    "V1324 Sco" OR "Nova Sco 2013" OR "PNV J17291350-3846120"

Results are deduplicated by bibcode before the `ReconcileReferences` Map step.

ADR-005 Decision 2 described a parallel name + coordinate strategy (two concurrent
queries merged on bibcode). That strategy was **never implemented.**

**Rationale for deviation:** The ADS positional search feature (`pos(circle, ...)`)
is not reliably documented and could not be validated against the live API. The
name-only strategy is sufficient for MVP and avoids a dependency on undocumented
API behaviour.

**Future path:** A coordinate cone search remains a natural future increment if ADS
publishes stable documentation. It would be additive (a second query merged on
bibcode) and would constitute a `rule_version` bump on the `DiscoveryDate`
idempotency key to avoid collisions with prior runs.

ADR-005 has been annotated with a supersession note pointing here.

---

### 2. `quarantine_handler` Is a Workflow-Agnostic Shared Sink

`quarantine_handler` is the single quarantine path for **all** Nova Cat workflows,
not only `initialize_nova`.

The handler resolves a `primary_id` from whichever identifier is present in the
event, in priority order: `nova_id → data_product_id → candidate_name`. This makes
the handler callable from any workflow context without requiring a specific
identifier to be present.

**Rationale:** Duplicating quarantine persistence and SNS notification logic per
workflow would scatter a cross-cutting concern and create divergence risk. A single
handler with flexible identifier resolution is the correct design. The
`_CLASSIFICATION_REASONS` table is extended as new quarantine codes are introduced.

---

### 3. `SummarizeDiscovery` Removed from `discover_spectra_products`

The `SummarizeDiscovery` state was listed in the `discover_spectra_products` state
machine but never implemented. Its intended purpose — counting discovered products
and surfacing item-level quarantine counts — is adequately served by
`FinalizeJobRunSuccess` (which receives Map output) and structured logs.

**Decision:** The state is removed. No replacement is required at MVP scale.

---

## Consequences

- `refresh_references.md` is the authoritative spec for the ADS query strategy;
  ADR-005 Decision 2 is superseded and annotated accordingly.
- Any future coordinate-based ADS query must be introduced as an additive change
  with a `rule_version` bump — it must not silently change existing discovery date
  computations.
- `quarantine_handler` must be kept free of workflow-specific assumptions. New
  quarantine reason codes must be added to `_CLASSIFICATION_REASONS` when
  introduced.

---

## References

- ADR-005 — Reference Model and ADS Integration (Decision 2 superseded here)
- ADR-006 — ADR Corpus Governance and Field Name Canonicalization
- `refresh_references.md` — authoritative ADS query strategy
