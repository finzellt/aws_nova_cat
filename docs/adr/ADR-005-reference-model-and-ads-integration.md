# ADR-005 — Reference Model and ADS Integration

**Status:** Accepted
**Date:** 2026-03-03
**Epic:** 12 (prerequisites pass)
**Deciders:** Nova Cat core team

---

## Context

Before Epic 12 implementation of `reference_manager` and the `refresh_references`
workflow could begin, five open questions were flagged in the architecture docs as
unresolved blockers:

1. ADS API query strategy (by name, coordinates, or both?)
2. `Reference` entity DDB item model (not yet defined)
3. Normalization and dedup key definition for a reference
4. Discovery date computation rule ("earliest credible reference" undefined)
5. Whether `archive_resolver` should be involved in ADS queries

This ADR records the decisions made to resolve all five.

---

## Decisions

### 1. ADS Belongs Exclusively to `reference_manager`

`archive_resolver` is scoped to nova identity resolution (SIMBAD + TNS) for
`initialize_nova` only. All ADS queries are owned by `reference_manager`.

**Rationale:** The two Lambdas serve non-overlapping workflow chains. ADS requires
only a plain HTTP API client — no `astropy`/`astroquery` dependency — so there is no
technical reason to route through the container-based `archive_resolver`. Merging them
would create an unnecessary cross-cutting failure point and conflate two distinct
domain concerns: "what is this object?" vs. "what has been written about it?"

---

### 2. ADS Query Strategy: Parallel Name + Coordinates, Merge on Bibcode

`FetchReferenceCandidates` issues two ADS queries in parallel — one by object name
(all known names for the nova), one by coordinate cone search (10 arcsecond radius,
matching the nova uniqueness threshold) — and merges results, deduplicating by bibcode.

**Rationale:** Neither query alone is complete. Name-based queries miss papers that
use non-standard designations; coordinate-based queries miss papers that name the nova
without reporting coordinates. Parallel execution avoids the latency cost of
sequential fallback. Bibcode is globally unique in ADS and is the only dedup key needed.

---

### 3. `Reference` Is a Global Entity with Its Own Top-Level DDB Partition

A `Reference` item lives at `PK=REFERENCE#<bibcode>` / `SK=METADATA`, not under
a `nova_id` partition. The Nova↔Reference link (`NOVAREF`) remains nova-scoped.

**Rationale:** A paper describing multiple novas must not be duplicated. The
global/scoped split — one canonical `Reference` item, many `NOVAREF` link items — is
the correct many-to-many model. The prior placeholder layout (`PK=<nova_id>` /
`SK=REF#<reference_id>`) was written before the global/scoped decision was made and
is incorrect.

**Dedup key:** ADS bibcode. `UpsertReferenceEntity` performs a direct `GetItem` on
`REFERENCE#<bibcode>` to detect existing items and update mutable metadata fields
(title, authors, year, publication_date, doi, arxiv_id, provenance, updated_at)
without affecting the stable partition key. No internal UUID is assigned to
`Reference` or `NovaReference` items; bibcode is both the stable identifier and the
DDB partition key component.

---

### 4. Discovery Date Rule v1.0: Earliest Publication Date Across All ADS Results

`discovery_date` on the Nova item is set to the `publication_date` of the
Reference with the earliest `publication_date` among all ADS results linked to that
nova, regardless of reference type. Tiebreaker: lexicographically smallest bibcode.

`UpsertDiscoveryDateMetadata` enforces a **monotonically earlier** invariant: it will
only overwrite a stored `discovery_date` with a strictly earlier value, never a later
one. This makes `refresh_references` safely re-runnable with no risk of regressing
the discovery date.

The rule version is `"1.0"` and is stored on the idempotency key
(`DiscoveryDate:{nova_id}:{earliest_reference_id}:1.0`) to allow future rule changes
without idempotency collisions.

**Rationale:** At MVP scale, "present in ADS" is a sufficient credibility filter.
No reference type is excluded — ATels and CBAT circulars are often the earliest
records of a nova discovery and must not be filtered out. Future rule versions can
tighten the definition without retroactive breakage.

---
## Consequences

### Ground Truth Updates Required

The following authoritative sources must be updated before Epic 12 implementation
begins. This ADR is not complete until these changes are made.

| Source | Required change |
|---|---|
| dynamodb-item-model.md | Replace sections 6 and 7 with simplified bibcode-keyed model. RETIRE SK=REF#... under nova_id partitions. REMOVE section 6.1 (REFINDEX) entirely. See companion doc. |
| entities.py | Apply the References section replacement. Removes: IdentifierType, Identifier, reference_id, nova_reference_id, identifiers. Adds: bibcode as required field on Reference and NovaReference, doi and arxiv_id as flat optional fields, atel/cbat_circular/arxiv_preprint to ReferenceType. Regenerate schemas after applying. |
| events.py | Document ads_name_hints: list[str] as an optional behavioral knob on RefreshReferencesEvent.attributes docstring. No model change required. |
| current-architecture.md | Section 3.4: expand to cover global entity design, NovaReferenceRole, flat identifier fields, and link-level provenance. Section 6: remove REF#... from per-nova item types; add REFERENCE#<bibcode> to global identity partitions. Section 9: add new architectural invariants. |
| refresh-references.md | Add ADS query strategy detail, ReferenceType classification table, normalization rules, rate limit and auth notes. |
| ADR-003 | Add inline annotation at the Reference section noting it is superseded in part by ADR-005. Note REFERENCE#<bibcode> partition as addendum to single-table design. |

---

### Open Item - Discovery Date Precision

> **RESOLVED** — See ADR-005 Amendment (Discovery Date Precision), accepted 2026-03-03.
> The decision below is preserved for historical context only.

~~Requires a decision before ComputeDiscoveryDate is implemented.~~

~~Nova.discovery_date in entities.py is already typed datetime or None (full
timezone-aware datetime). No model change needed on Nova.~~

~~However, Reference stores only year as an integer. The best precision
available from ADS pubdate is YYYY-MM. The question is whether to
preserve that month-level precision on the Reference item itself.~~

**Resolution:** Both `Nova.discovery_date` and `Reference.publication_date` are
stored as `str | None` in `YYYY-MM-DD` format. When only month-level precision is
available, day is `00` (e.g. `"2013-06-00"`). Python's `date` type rejects day `0`,
making string storage necessary. The `_DISCOVERY_DATE_RE` regex validator (month
`01–12`, day `00–31`) is shared by both fields. The `00` day convention is an
unambiguous signal that day-level precision is unavailable — `01` must never be used
as a proxy for unknown day. See ADR-005 Amendment for full rationale.

---

### New Architectural Invariants

Add to section 9 of current-architecture.md:

1. ADS calls are never routed through archive_resolver. archive_resolver is
   scoped to nova identity resolution (SIMBAD + TNS) only.

2. References use ADS bibcodes as their stable global key. No internal UUID is
   assigned to Reference or NovaReference items.

---

### Deferred

- Non-ADS reference sources are out of scope for Epic 12 and have no planned
  future implementation. Donated data provenance belongs on DataProduct.provenance.
- Quality filtering for discovery date beyond present in ADS is deferred to a
  future rule_version increment.
- NovaReferenceRole promotion (e.g. to DISCOVERY) is deferred to a future workflow
  or manual curation step; refresh_references assigns OTHER by default.


---

## References

> 📄 `ADR-001-contracts-and-schema.md` — schema governance; `Reference` models
> subject to same versioning and CI enforcement rules.
> 📄 `ADR-003-Persistence-Model-for-DynamoDB_S3` — single-table design this ADR
> extends.
> 📄 `ADR-004-architecture-baseline-and-alignment-policy.md` — authoritative source
> hierarchy; ground truth update list above reflects that hierarchy.
> 📄 `refresh-references.md` — workflow spec; receives normalization and field detail
> from this ADR.
> 📄 `dynamodb-item-model.md` — receives corrected Reference partition layout.
