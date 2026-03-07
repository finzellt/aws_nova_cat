# ADR-006: ADR Corpus Governance and Field Name Canonicalization

**Status:** Accepted
**Date:** 2026-03-05

---

## Context

During an alignment pass following Epic 12, two governance questions arose that had
no prior explicit decision record:

1. **ADR preservation policy.** Several ADRs contained body text that referenced
   removed abstractions (`dataset_id`, `reference_id`, `Dataset`). The question was
   whether to rewrite that text or annotate it.

2. **Canonical field names for operational entities.** `JobRun` and `Attempt` field
   names were inconsistent between the Pydantic contract models and the DynamoDB item
   model documentation, with no prior decision establishing which was authoritative.

---

## Decisions

### 1. ADR Bodies Are Immutable; Corrections Are Annotations

ADR body text is never rewritten after acceptance. Superseded or incorrect content
is annotated with a disclaimer at the top of the document that:

- States what is superseded or incorrect
- References the ADR or amendment that supersedes it
- Preserves the original text below unchanged

**Rationale:** ADRs are a historical record of *why* decisions were made, not just
*what* was decided. Rewriting them destroys the reasoning trail. Annotations
preserve context for future readers while clearly signalling what is no longer
current. This is consistent with standard ADR practice (Nygard, 2011).

**Applies retroactively** to all existing ADRs in the corpus.

---

### 2. Canonical Field Names for `JobRun` and `Attempt`

The following field names are canonical across all authoritative sources (Pydantic
models, DynamoDB item model, workflow specs, logs):

| Entity | Canonical name | Replaces |
|---|---|---|
| `JobRun` | `started_at` | `initiated_at` |
| `JobRun` | `ended_at` | `finished_at` |
| `Attempt` | `ended_at` | `finished_at` |
| `Attempt` | `attempt_number` | `attempt_no` |
| `Attempt` | `error_type` | `error_code` |

**Rationale:** The DynamoDB item model and workflow specs consistently used
`started_at` / `ended_at` / `attempt_number` / `error_type`. The Pydantic models
had drifted to `initiated_at` / `finished_at` / `error_code`. The item model names
are more consistent with AWS Step Functions and CloudWatch conventions and are
adopted as canonical. Pydantic models are updated to match.

**Migration note:** Any existing DynamoDB items written with the old field names
(`initiated_at`, `finished_at`, `error_code`) will need read-time coercion or a
migration if backward compatibility is required.

---

## Consequences

- All future ADR corrections must follow the annotation policy.
- The `started_at` / `ended_at` field names are stable contract commitments.
  Downstream consumers (log parsers, dashboards, monitoring) should use these names.
- Schema regeneration from updated contracts is required before deploying code
  that references the renamed fields.
