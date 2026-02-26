# Public Interface Catalog
*(Architectural Governance Artifact)*

## 0. Purpose

This catalog enumerates the system’s **public interfaces**—interfaces that other components are allowed to depend on without ad hoc coordination.

It exists to:

- Prevent accidental breaking changes
- Enable parallel implementation with bounded context
- Provide a single source of truth for “what is public”
- Support compatibility testing and drift detection
- Clarify ownership and change approval requirements

**Definition (public interface):**
An interface is “public” if changing it would require coordinated updates in other components or would break persisted/queued data.

This document is not a data contract repository. It is a map of what contracts exist, where they live, who owns them, and how they may evolve.

---

## 1. Interface Taxonomy

Public interfaces are grouped into the following categories:

1. **External APIs** (human/client-facing; e.g., HTTP request/response)
2. **Workflow APIs** (Step Functions entry/exit payloads; inter-workflow messages)
3. **Contract Artifacts** (schemas persisted or referenced across time)
4. **Persistence APIs** (wrapper calls that define allowed read/write behaviors)
5. **Observability APIs** (logging/correlation/metrics wrapper calls)
6. **Error Classification APIs** (canonical error taxonomy + mapping helpers)

Each interface MUST declare its type and evolution policy.

---

## 2. Ownership and Authority Model

Each interface has:

- **Owner:** the surface owner responsible for correctness and evolution
- **Approval Authority:** who must approve changes (e.g., facilitator/root facilitator)
- **Consumers:** known dependent components (may be “unknown/varies”)

Rules of thumb:

- Interface changes that affect multiple surfaces require facilitator approval.
- Breaking changes require an ADR and explicit version bump.

---

## 3. Evolution Policy (Compatibility Rules)

Each interface entry must specify:

- **Versioning scheme:** MAJOR/MINOR/PATCH (or equivalent)
- **Compatibility stance:** backward compatible, forward compatible, both, or neither
- **Breaking change triggers:** what forces MAJOR

Recommended default policies:

- External API payloads: strict compatibility + explicit versioning
- Workflow payloads: backward compatible preference
- Persisted schemas: backward compatible preference (across time)
- Wrapper APIs: stable; breaking changes require strong justification

---

## 4. Catalog Format

Each interface is represented by a single entry with the following fields.

### 4.1 Interface Entry Template

- **Interface ID:** `IFC-###`
- **Name:**
- **Category:** (External API / Workflow API / Persisted Schema / Wrapper API / Observability API / Error API)
- **Status:** (Draft / Active / Deprecated)
- **Owner:**
- **Approval Authority:**
- **Primary Consumers:**
- **Location:** (repo path or canonical reference)
- **Source of Truth:** (Pydantic model, schema file, wrapper module, etc.)
- **Version:**
- **Stability:** (Stable / Evolving / Experimental)
- **Compatibility Policy:** (summary of forward/backward stance + breaking rules)
- **Change Process:** (what is required for change: tests, ADR, version bump)
- **Verification:** (tests or drift checks that enforce it)
- **Notes:** (constraints, invariants, examples)

---

## 5. Interface Entries

> The following sections list actual public interfaces.
> Each section may grow independently.

---

# 5.1 External APIs (Client-Facing)

## IFC-001 — Example Placeholder: Submit Request
- **Category:** External API
- **Status:** Draft
- **Owner:** TBD
- **Approval Authority:** Root Facilitator
- **Primary Consumers:** Public clients
- **Location:** `contracts/api/...` (placeholder)
- **Source of Truth:** Pydantic model(s) → JSON Schema
- **Version:** v0
- **Stability:** Evolving
- **Compatibility Policy:** Breaking changes require MAJOR; additive optional fields are MINOR
- **Change Process:** Update schema + add contract tests + update docs
- **Verification:** `tests/contracts/test_api_schema_compat.py` (placeholder)
- **Notes:** Provide example fixtures for requests/responses

---

# 5.2 Workflow APIs (Orchestration Layer)

## IFC-010 — Example Placeholder: Workflow Entry Payload
- **Category:** Workflow API
- **Status:** Draft
- **Owner:** TBD
- **Approval Authority:** Facilitator (or Root Facilitator if widely consumed)
- **Primary Consumers:** Step Functions + workflow lambdas
- **Location:** `contracts/events/...` (placeholder)
- **Source of Truth:** Pydantic event model(s)
- **Version:** v0
- **Stability:** Evolving
- **Compatibility Policy:** Prefer backward compatibility; breaking changes require MAJOR + ADR
- **Change Process:** Update schema + update fixtures + update compatibility tests
- **Verification:** schema snapshot + workflow contract tests
- **Notes:** `correlation_id` may be optional on input but required internally

---

# 5.3 Contract Artifacts (Persisted Across Time)

## IFC-020 — Example Placeholder: Persisted Entity Schema
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

# 5.4 Persistence Wrapper APIs (Choke Point)

## IFC-030 — Persistence Wrapper API Surface
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

# 5.5 Observability Wrapper APIs (Choke Point)

## IFC-040 — Observability Wrapper API Surface
- **Category:** Observability API
- **Status:** Active
- **Owner:** Observability Surface Owner
- **Approval Authority:** Root Facilitator
- **Primary Consumers:** All lambdas/workflows
- **Location:** `.../observability/...` (placeholder)
- **Source of Truth:** Wrapper module API
- **Version:** v1
- **Stability:** Stable
- **Compatibility Policy:** Required fields may not be removed without MAJOR + ADR
- **Change Process:** Update wrapper + update golden log tests
- **Verification:** golden telemetry tests
- **Notes:** Logs emitted to CloudWatch; wrapper standardizes shape only

---

# 5.6 Error Classification APIs

## IFC-050 — Error Taxonomy + Classifier Surface
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

## 6. Deprecation Policy

Interfaces may be deprecated, but must specify:

- Replacement interface ID
- Deprecation date
- Removal criteria (if any)

Deprecated interfaces remain compatible until formally removed.

---

## 7. Related Documents

- Wrapper Charter (`docs/architecture/wrapper-charter.md`)
- Invariant Registry (`docs/architecture/invariant-registry.md`)
- Architecture Snapshot (`docs/architecture/current-architecture.md`)
- ADR Index (`docs/governance/adr/`)

---
