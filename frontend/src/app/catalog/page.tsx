/**
 * Catalog page — /catalog
 *
 * Primary browsing interface (ADR-010, ADR-011).
 * Renders the full catalog as a paginated, sortable, searchable table.
 *
 * CatalogTable owns the search bar and pagination controls internally —
 * this page only needs to supply the novae array and a heading.
 *
 * Server Component: catalog data is read at build time. No client-side
 * fetch is needed; the component tree is interactive via CatalogTable's
 * own 'use client' boundary.
 */

import { CatalogTable } from '@/components/catalog/CatalogTable';
import { getCatalogData } from '@/lib/catalog';

export const metadata = {
  title: 'Catalog — Open Nova Catalog',
  description:
    'Browse all novae in the Open Nova Catalog. Sortable and searchable by name, alias, and observational coverage.',
};

export default async function CatalogPage() {
  const { novae } = await getCatalogData();

  return (
    <div className="py-10 flex flex-col gap-8">

      {/* ── Page heading ─────────────────────────────────────────────── */}
      {/*
       * ADR-012: page heading text-2xl semibold. The row count sub-line
       * uses text-sm text-text-secondary — a secondary detail, not a heading.
       */}
      <div>
        <h1 className="text-2xl font-semibold text-text-primary">Catalog</h1>
        {novae.length > 0 && (
          <p className="text-sm text-text-secondary mt-1">
            {novae.length.toLocaleString()} novae — sorted by spectra count by
            default. Click any column header to re-sort, or use the search bar
            to filter by name or alias.
          </p>
        )}
      </div>

      {/* ── Full catalog table ───────────────────────────────────────── */}
      {/*
       * No `preview` prop: CatalogTable renders with its built-in search bar
       * and pagination controls (25 rows/page, ADR-010).
       */}
      <CatalogTable novae={novae} />

    </div>
  );
}
