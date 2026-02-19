# ADR-0001: Contract Definition and Schema Governance

## Status
Accepted

## Context
Nova Cat is a serverless AWS application composed of loosely coupled components (Lambda functions, Step Functions workflows, and supporting services). To preserve modularity and enable independent evolution of components, the system requires explicit, versioned data contracts at service boundaries.

We must define contracts before implementing ingestion or persistence details, and we need strong governance to prevent accidental drift between implementations and published interfaces.

## Decision
1. **Pydantic models are the source of truth** for all contracts:
   - Persistent entities (e.g., Nova, Dataset, Reference, NovaReference, JobRun)
   - Workflow event payloads used at Step Functions boundaries
   - Bibliographic works are modeled as global `Reference` entities,
     with nova-scoped `NovaReference` link records to support
     many-to-many relationships without duplication.

2. **JSON Schema artifacts are generated from Pydantic models and committed to the repository** under `/schemas`:
   - These files are the published interface artifacts for consumers and reviewers
   - They are treated as stable, reviewable, and diffable outputs

3. **Versioning policy**
   - Every persistent entity model includes `schema_version` (SemVer).
   - Every event payload model includes `event_version` (SemVer).
   - Backwards-compatible changes (MINOR/PATCH):
     - Adding optional fields
     - Relaxing validations in a compatible way
     - Expanding enums only if consumers can tolerate unknown values (otherwise treat as breaking)
   - Breaking changes (MAJOR):
     - Removing fields
     - Renaming fields
     - Changing field meaning/type in incompatible ways
     - Tightening validation in a way that rejects previously valid payloads

4. **Validation policy**
   - All boundary inputs (events) MUST be validated at the start of each Lambda/Step Functions task using the corresponding Pydantic model.
   - Persistent entities MUST be validated before being written to any storage backend.
   - CI enforces:
     - Schema regeneration from models
     - No drift between regenerated schema output and committed schema artifacts
     - Example fixture payloads validate successfully (and invalid fixtures fail as expected)

## Consequences
- Contracts become explicit, reviewable artifacts and provide stable integration points between loosely coupled serverless components.
- Pydantic provides strong runtime validation and high-quality developer ergonomics, reducing ambiguity at boundaries.
- Committed JSON Schemas enable downstream tooling, documentation generation, and language-agnostic validation where required.
- Strict CI checks prevent silent schema drift and make contract changes intentional and auditable.

## Notes
This ADR deliberately avoids storage modeling (DynamoDB keys, S3 layout, GSIs). Contracts define interfaces and invariants, not persistence strategy.
