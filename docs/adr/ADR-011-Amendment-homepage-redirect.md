# ADR-011 Amendment — Homepage Redirect

**Amends:** ADR-011 (Frontend Architecture and Design Specification)
**Status:** Accepted
**Date:** 2026-04-17
**Relates to:** ADR-010 (Catalog Navigation Model), ADR-030 (ADR Amendment
  Policy Revision)

---

## Decision

The root route (`/`) redirects to `/catalog`. No dedicated homepage is rendered.

The aggregate catalog statistics block (formerly on the homepage) now renders at
the top of every catalog-flavor page — `/catalog` and `/search` — via the
extracted `StatsBar` component.

This revises ADR-011 §"Page Specifications → Homepage (`/`)" and reinstates
ADR-010 §"Site Entry Point" as written.

---

## Rationale

**Jump directly into the catalog.** ADR-011 specified a dedicated splash page
with a hero explainer, stats bar, and top-10 preview table above a "View Full
Catalog →" link. In practice this introduced an interstitial between the user
and the catalog — the primary reason the site exists. A researcher typing the
root URL, or clicking the logo in the nav, should land in the full catalog
without an additional click.

**The preview table had a sorting defect.** The splash page pre-sorted the full
catalog by spectra count descending, sliced to the top 10, and handed those 10
rows to `CatalogTable`. Clicking a different column header re-sorted only those
10 rows, not the full catalog — users saw a correctly-sorted sample rather than
the top 10 under their chosen sort. Redirecting to `/catalog` eliminates this
class of defect entirely: `CatalogTable` always receives the full catalog.

**Stats bar promoted from splash-only to catalog-flavor pages.** Aggregate
totals are useful context anywhere the catalog table is visible, not only on
the landing page. Hoisting the stats bar onto `/catalog` and `/search`
preserves the information while eliminating the splash page that previously
hosted it. The stats bar is not rendered on nova detail pages, `/docs`, or
`/about` — those surfaces have their own contextual framing and do not need
catalog totals.

**Hero explainer is absorbed elsewhere or deleted.** The 2–4 sentence explainer
copy from the splash page is superseded by the `/about` page, which already
serves that role. No prose content is lost.

---

## Amendment Classification

The decisions in ADR-011 §"Homepage" have been reflected in deployed
infrastructure (the splash page is live at the public Vercel deployment).
Per ADR-030 Decision 1, this makes direct body amendment of ADR-011
inappropriate. This separate amendment file records the revised decision while
preserving ADR-011's original text unchanged.

ADR-011's other sections — tech stack, catalog page spec, nova page layout,
visual design direction, typography, accessibility guidance — are unaffected by
this amendment.

---

## Implementation

| Source | Change |
|---|---|
| `frontend/src/components/catalog/StatsBar.tsx` | New component. Extracted `StatsBar` + private `StatCard` helper from the former `app/page.tsx`. |
| `frontend/src/app/page.tsx` | Collapsed to a three-line server-component redirect to `/catalog`. Former splash layout removed. |
| `frontend/src/app/catalog/page.tsx` | Pulls `stats` from `getCatalogData()` and renders `<StatsBar>` above the page heading. |
| `frontend/src/app/search/page.tsx` | Same addition as `/catalog`. |
| `docs/adr/ADR-011-frontend-architecture-and-design-spec.md` | Unchanged. A top-level annotation pointing to this amendment may be added in a subsequent pass if ADR-011 is revisited for other reasons; not required by this amendment. |

No changes to `NavBar` are required. The logo already links to `/` and rides
the new redirect correctly.

---

## Forward Compatibility

The `StatsBar` component is the surface on which two queued frontend tasks will
extend the stats display:

- **F7** — Total unique spectral visits in the stats display. Adds a fourth
  card or replaces one of the existing three.
- **F8** — Stats broken out by spectral regime. Likely introduces a variant
  layout (per-regime breakdown), rendered in place of or below the current
  aggregate cards on `/catalog` and `/search`.

Both tasks operate on the extracted component rather than per-page duplication.
