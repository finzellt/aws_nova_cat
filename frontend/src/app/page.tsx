/**
 * Homepage — /
 *
 * Layout (ADR-011):
 *   1. Hero / explainer section
 *   2. Stats bar  (stats block from catalog.json — ADR-014)
 *   3. Catalog preview table (top 10 novae, preview mode — ADR-011)
 *
 * This is a Server Component. Catalog data is read at build time via the
 * shared getCatalogData() helper. No client-side fetch is required.
 */

import Link from 'next/link';
import { CatalogTable } from '@/components/catalog/CatalogTable';
import { getCatalogData } from '@/lib/catalog';
import { resolveRelease } from '@/lib/dataClient';
import type { CatalogStats } from '@/types/catalog';

// ── Stats bar ────────────────────────────────────────────────────────────────

interface StatCardProps {
  value: number;
  label: string;
}

/**
 * Individual stats bar card.
 * ADR-012 card pattern: bg-surface-secondary, border border-border-subtle,
 * rounded-lg, p-6. Numeral: text-4xl semibold. Label: text-sm normal.
 */
function StatCard({ value, label }: StatCardProps) {
  return (
    <div className="bg-surface-secondary border border-border-subtle rounded-lg p-6">
      {/*
       * toLocaleString() formats large numbers with locale-appropriate
       * thousands separators (e.g. 8,940 rather than 8940).
       */}
      <div className="text-4xl font-semibold leading-tight text-text-primary">
        {value.toLocaleString()}
      </div>
      <div className="text-sm text-text-secondary mt-2">{label}</div>
    </div>
  );
}

function StatsBar({ stats }: { stats: CatalogStats }) {
  return (
    <section aria-label="Catalog statistics">
      <div className="grid grid-cols-3 gap-6">
        <StatCard value={stats.nova_count} label="Novae in catalog" />
        <StatCard value={stats.spectra_count} label="Validated spectra" />
        <StatCard
          value={stats.photometry_count}
          label="Photometric observations"
        />
      </div>
    </section>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────────

export default async function HomePage() {
  const [{ stats, novae }, releaseId] = await Promise.all([
    getCatalogData(),
    resolveRelease().catch(() => 'local'),
  ]);

  /*
   * The homepage preview table shows the top 10 novae by spectra count —
   * the same default sort CatalogTable applies internally. Sorting here on
   * the server ensures the preview matches what the full catalog page shows
   * at the top of its first page.
   */
  const previewNovae = [...novae]
    .sort((a, b) => b.spectra_count - a.spectra_count)
    .slice(0, 10);

  return (
    <div className="py-12 flex flex-col gap-16">

      {/* ── Hero ─────────────────────────────────────────────────────── */}
      {/*
       * ADR-011: "2–4 sentences. Aimed at a researcher encountering the site
       * for the first time. No large decorative imagery."
       * ADR-012: page heading text-3xl semibold; body text-base leading-relaxed.
       */}
      <section>
        <h1 className="text-3xl font-semibold leading-tight text-text-primary mb-6">
          Open Nova Catalog
        </h1>
        <p className="text-base leading-relaxed text-text-secondary max-w-2xl">
          The Open Nova Catalog is a curated, publicly accessible repository of
          observational data for classical novae. It collects spectroscopic and
          photometric records drawn from published literature and public archives,
          structured for direct browsing and programmatic access. The catalog is
          designed to support reproducible nova research and to serve as a shared
          reference resource for the community.
        </p>
      </section>

      {/* ── Stats bar ────────────────────────────────────────────────── */}
      <StatsBar stats={stats} />

      {/* ── Catalog preview ──────────────────────────────────────────── */}
      {/*
       * ADR-011: "A non-paginated sample of the full catalog (e.g. the top
       * 10 entries by default sort order). Uses the same component and column
       * configuration as the full catalog page."
       * The `preview` prop suppresses the search bar and pagination controls
       * inside CatalogTable. We slice to 10 rows above; the component renders
       * all rows it receives when preview=true.
       */}
      <section>
        <div className="flex items-baseline justify-between mb-6">
          <h2 className="text-2xl font-semibold text-text-primary">
            Catalog preview
          </h2>
          {novae.length > 0 && (
            <Link
              href="/catalog"
              className="text-sm font-medium text-interactive no-underline hover:underline"
            >
              View full catalog →
            </Link>
          )}
        </div>

        {previewNovae.length > 0 ? (
          <>
            <CatalogTable novae={previewNovae} releaseId={releaseId} preview />
            {/*
             * "View Full Catalog" link below the table as well, per ADR-011.
             * The one in the heading is the primary affordance; this one is
             * a convenience for users who read to the bottom of the preview.
             */}
            <div className="mt-6 flex justify-end">
              <Link
                href="/catalog"
                className="text-sm font-medium text-interactive no-underline hover:underline"
              >
                View full catalog →
              </Link>
            </div>
          </>
        ) : (
          // Empty state: catalog.json not yet populated during development.
          <p className="py-8 text-sm text-text-tertiary">
            Catalog data is not yet available. Run the generation pipeline to
            populate{' '}
            <code className="font-mono text-xs">public/data/catalog.json</code>.
          </p>
        )}
      </section>

    </div>
  );
}
