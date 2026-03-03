# ADR-005 Amendment — Discovery Date Precision

**Amends:** ADR-005 (Reference Model and ADS Integration)
**Status:** Accepted
**Date:** 2026-03-03
**Resolves:** Open Item 2 from ADR-005 Consequences

---

## Decision

Discovery dates are stored as `str` in `YYYY-MM-DD` format. When only month-level
precision is available, the day field is `00` (e.g. `"2013-06-00"`).

This applies to two fields:
- `Nova.discovery_date: str | None`
- `Reference.publication_date: str | None`

---

## Rationale

**Why string, not `date`:** Python's `date` type rejects day `0`. Storing
`YYYY-MM-00` requires `str`.

**Why `00` day, not `01`:** Publishing `01` when the actual day is unknown is a
data integrity problem. A consumer cannot distinguish "first of the month" from
"day unknown." The `00` convention is an explicit, unambiguous signal that
day-level precision is unavailable for this record.

**Why not full `datetime`:** Discovery dates are calendar dates, not timestamps.
Timezone-aware datetimes add no scientific value and introduce false precision.

**ADS sourcing:** ADS `pubdate` is already in `YYYY-MM-00` format. Storing
`Reference.publication_date` directly from ADS requires zero transformation.
Day-precise dates from any future source slot in as `YYYY-MM-DD` without any
format change.

**Monotonicity:** Lexicographic comparison on `YYYY-MM-DD` strings is correct
for this format. The `UpsertDiscoveryDateMetadata` monotonically-earlier invariant
holds without any parsing.

---

## Format Validator

Module-level constant `_DISCOVERY_DATE_RE`, shared by both fields:

- Month must be `01-12`
- Day must be `00-31` (`00` signals month-only precision; upper bound is a logical
  range check only — semantic validity not enforced at model layer)

Valid: `"2013-06-00"` (month known, day unknown), `"2013-06-14"` (full date known)

Invalid: `"2013-6-0"` (no zero-padding), `"2013-00-00"` (month 00 not valid),
`"2013-06"` (missing day component)

---

## Ground Truth Updates Required

| Source | Change |
|---|---|
| `entities.py` | Add `import re` to stdlib imports. Add `_DISCOVERY_DATE_RE` constant near module-level helpers. |
| `entities.py` | `Nova.discovery_date`: `datetime or None` changed to `str or None`. Replace `ensure_discovery_tz_aware_if_present` validator with `validate_discovery_date_format`. See companion change doc. |
| `entities-references-replacement.py` | `Reference`: add `publication_date: str or None` field after `year`. Add `validate_publication_date_format` validator. See companion change doc. |
| `dynamodb-item-model.md` (Nova item) | Update `discovery_date` field note: YYYY-MM-DD string; day 00 when only month precision available. |
| `dynamodb-item-model.md` (Reference item) | Add `publication_date` field: YYYY-MM-DD string; day 00 when only month precision available; sourced directly from ADS pubdate. |
