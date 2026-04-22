/**
 * Stats bar — aggregate catalog statistics.
 *
 * Renders a three-card horizontal strip summarizing the catalog at a glance:
 * nova count, validated spectra count, photometric observation count.
 *
 * Used at the top of every "catalog-flavor" page — /catalog and /search
 * (ADR-011 Amendment: Homepage Redirect). Not rendered on nova detail pages,
 * the documentation page, or the about page.
 *
 * Values are drawn from catalog.json's stats block (ADR-014) and are therefore
 * always current for the release being rendered.
 *
 * Design tokens (ADR-012):
 *   - Card: bg-surface-secondary, border border-border-subtle, rounded-lg, p-6
 *   - Numeral: text-4xl semibold, leading-tight, text-text-primary
 *   - Label: text-sm text-text-secondary
 */

import type { CatalogStats } from '@/types/catalog';

// ── Individual card ──────────────────────────────────────────────────────────

interface StatCardProps {
  value: number;
  label: string;
}

/**
 * Single stats card. Private to this file — StatsBar is the only consumer,
 * and the card composition isn't meaningful on its own.
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

// ── Stats bar ────────────────────────────────────────────────────────────────

interface StatsBarProps {
  stats: CatalogStats;
}

/**
 * Three-card horizontal stats strip. Drops in above a page's primary content.
 *
 * The `aria-label` on the containing section makes the block addressable by
 * assistive technology as a single landmark rather than three orphan cards.
 */
export function StatsBar({ stats }: StatsBarProps) {
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
