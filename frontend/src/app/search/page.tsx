/**
 * Search page — /search
 *
 * Search-focused catalog interface (ADR-010, ADR-011).
 * Allows users who already know the nova they are looking for to search
 * directly by name or alias without having to navigate to the catalog first.
 *
 * This page is functionally identical to /catalog with two differences:
 *   1. The heading and copy are oriented around search rather than browsing.
 *   2. The search input is auto-focused on mount (autoFocusSearch prop).
 *
 * PREREQUISITE — one prop addition needed on CatalogTable before deploying:
 * ─────────────────────────────────────────────────────────────────────────
 * In CatalogTableProps (src/components/catalog/CatalogTable.tsx), add:
 *
 *   /** When true, auto-focuses the search input on mount. Search page only. *\/
 *   autoFocusSearch?: boolean;
 *
 * In the function signature destructuring, add:
 *
 *   autoFocusSearch = false,
 *
 * On the <input> element inside the {!preview && ...} search bar block, add:
 *
 *   autoFocus={autoFocusSearch}
 *
 * That is the complete change — three lines touched, no logic affected.
 * Until this prop is added, the search page works correctly but the input
 * will not be auto-focused on load.
 * ─────────────────────────────────────────────────────────────────────────
 *
 * Server Component: catalog data is read at build time.
 */

import { CatalogTable } from '@/components/catalog/CatalogTable';
import { getCatalogData } from '@/lib/catalog';

export const metadata = {
  title: 'Search — Open Nova Catalog',
  description:
    'Search the Open Nova Catalog by nova name or alias.',
};

export default async function SearchPage() {
  const { novae } = await getCatalogData();

  return (
    <div className="py-10 flex flex-col gap-8">

      {/* ── Page heading ─────────────────────────────────────────────── */}
      {/*
       * ADR-010: "Users who already know the nova they are looking for
       * should have a clearly labeled entry point."
       * ADR-012: page heading text-2xl semibold; body text-base leading-relaxed.
       */}
      <div>
        <h1 className="text-2xl font-semibold text-text-primary">Search</h1>
        <p className="text-sm text-text-secondary mt-1">
          Search by primary name or alias. Filtering is client-side and instant.
        </p>
      </div>

      {/* ── Search-focused catalog table ─────────────────────────────── */}
      {/*
       * autoFocusSearch places the cursor in the search field immediately on
       * page load, reducing friction for direct-lookup workflows.
       * Requires the prop addition described in the file header above.
       */}
      <CatalogTable novae={novae} autoFocusSearch />

    </div>
  );
}
