# ADR-030: ADR Amendment Policy Revision

**Status:** Accepted
**Date:** 2026-03-25
**Author:** TF
**Supersedes:** ADR-006 Decision 1 (ADR Bodies Are Immutable; Corrections Are
  Annotations) — superseded in part; ADR-006 Decision 2 is unaffected and remains
  authoritative
**Superseded by:** —
**Amends:** —
**Relates to:**
- `ADR-006` — ADR Corpus Governance and Field Name Canonicalization (Decision 1
  superseded here)
- `epic/22-photometry-doc-reconciliation` — first exercise of this policy

---

## 1. Context

ADR-006 Decision 1 established that ADR body text is immutable after acceptance and
that all corrections must be made via annotation. The rationale was that ADRs are a
historical record of *why* decisions were made, and rewriting them destroys the
reasoning trail.

This rationale is sound for decisions that have been exercised in deployed
infrastructure. It is unnecessarily burdensome for decisions that exist only on paper.
NovaCat is a solo-operator project in active pre-deployment development. A substantial
portion of the ADR corpus records decisions that have never been reflected in a live
system. Treating those documents as immutable adds overhead without preserving anything
of historical value: there is no production system whose behavior the documents explain,
and no external consumers whose understanding of the record depends on the original text
being preserved verbatim.

The annotation discipline also tends to produce documents that are harder to read than
their replacements would be — a reader must parse both the original text and its
overlaid corrections to understand the current state of a decision.

---

## 2. Decisions

### Decision 1 — Direct Amendment Is Permitted for Pre-Deployment Documents

A documentation file may be directly amended — body text rewritten, decisions revised,
or the document replaced in its entirety — provided the decisions it records have not
yet been reflected in deployed infrastructure.

**The criterion is deployment, not acceptance status.** An ADR with status `Accepted`
that records decisions not yet exercised in a live system is eligible for direct
amendment. An ADR with status `Draft` whose decisions are already running in production
is not.

Once a decision has been exercised in a live system, the document body becomes
immutable. Corrections from that point forward are made via annotation, supersession,
or a new ADR, per the original ADR-006 Decision 1 policy.

### Decision 2 — Replaced Files Must Carry a Provenance Note

When any documentation file — ADR or otherwise — is replaced in its entirety, the
original is moved to an `archive/` subdirectory within its parent directory (e.g.
`docs/adr/archive/`, `docs/specs/archive/`) and annotated with a forward reference to
its replacement. The replacement file must include a header note of the form:

> **Replaces:** `<archive path>/<original filename>` — replaced in `epic/NN-<name>`.

This ensures that the audit trail is preserved even when documents are rewritten, and
that a reader encountering the original file is immediately directed to the current
version.

---

## 3. Consequences

- The annotation-only discipline established in ADR-006 Decision 1 is retired for
  pre-deployment documents. It remains in force for any ADR whose decisions are
  running in production.
- ADR-006 itself is annotated to indicate that Decision 1 has been superseded by this
  ADR. ADR-006 Decision 2 (canonical field names for `JobRun` and `Attempt`) is
  unaffected and remains authoritative.
- ADR-006 is preserved as a historical record explaining why some older files in the
  corpus carry annotation-style corrections rather than direct replacements.
- `epic/22-photometry-doc-reconciliation` is the first exercise of this policy.
  Files amended or replaced within that epic carry provenance notes per Decision 2.
