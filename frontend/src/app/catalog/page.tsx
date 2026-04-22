/**
 * Catalog page — /catalog
 *
 * Primary browsing interface and the canonical entry point for the site
 * (ADR-010; reinstated by ADR-011 Amendment: Homepage Redirect).
 *
 * Renders the full catalog as a paginated, sortable, searchable table,
 * preceded by the aggregate stats bar. CatalogTable owns the search bar
 * and pagination controls internally — this page supplies the novae
 * array, the stats block, and the heading.
 *
 * Server Component: catalog data is read at build time. No client-side
 * fetch is needed; the component tree is interactive via CatalogTable's
 * own 'use client' boundary.
 */

import { CatalogTable } from '@/components/catalog/CatalogTable';
import { StatsBar } from '@/components/catalog/StatsBar';
import { getCatalogData } from '@/lib/catalog';
import { resolveRelease } from '@/lib/dataClient';

export const metadata = {
  title: 'Catalog — Open Nova Catalog',
  description:
    'Browse all novae in the Open Nova Catalog. Sortable and searchable by name, alias, and observational coverage.',
};

export default async function CatalogPage() {
  const [{ stats, novae }, releaseId] = await Promise.all([
    getCatalogData(),
    resolveRelease().catch(() => 'local'),
  ]);

  return (
    <div className="py-10 flex flex-col gap-8">

      {/* ── Stats bar ────────────────────────────────────────────────── */}
      {/*
       * ADR-011 Amendment: stats render at the top of every catalog-flavor
       * page. Numbers reflect the release currently being served.
       */}
      <StatsBar stats={stats} />

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
      <CatalogTable novae={novae} releaseId={releaseId} />

    </div>
  );
}
