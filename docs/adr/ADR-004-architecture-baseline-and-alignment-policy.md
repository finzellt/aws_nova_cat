# ADR-004: Architecture Baseline and Alignment Policy

## Status

Accepted

## Context

During Epics 2 and 3, architectural drift occurred between:

- Workflow specifications
- DynamoDB persistence model
- Contract models (Pydantic)
- Generated JSON schemas
- Observability documentation
- ADRs

This resulted in inconsistencies including:
- Continued use of `dataset_id` after the Dataset abstraction was removed
- Workflow boundary schemas diverging from documented execution semantics
- Conflicting photometry versioning descriptions

A full architectural alignment pass was performed in Epic 4.

## Decision

The following sources are authoritative:

1. DynamoDB item model
2. Workflow specifications
3. Contracts (Pydantic models in `contracts/models`)
4. Generated schemas (exported from contracts)

Documentation must conform to these sources.

Key architectural commitments:

- UUID-first identity model (`nova_id`, `data_product_id`, `reference_id`)
- No Dataset abstraction
- DataProduct is the atomic scientific unit
- Spectra workflows operate on one `data_product_id` per execution
- Photometry table is canonical and overwritten in place
- Photometry snapshots occur only on schema version changes
- Idempotency keys are internal-only and never part of boundary schemas

## Consequences

- All schemas must be regenerated from contracts.
- Any documentation referencing dataset_id is considered stale.
- Future architectural changes must update:
  - Contracts
  - Schema export
  - Workflow specs
  - Architecture baseline
  - ADRs (if architectural)

This ADR supersedes any prior ADR language referencing Dataset or dataset_id.
