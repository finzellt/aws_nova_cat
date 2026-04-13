/**
 * ObjectSummary — renders the object summary section of the nova page.
 *
 * Contains:
 *   - Primary name as page heading (text-3xl semibold, ADR-011)
 *   - Aliases (dot-separated)
 *   - RA / Dec in DM Mono, selectable for copy (ADR-011)
 *   - Discovery date (formatted from YYYY-MM-DD with "00" for unknown parts)
 *   - Nova type badge (pill chip, ADR-012)
 *   - Observation counts
 *   - Download bundle button (primary button, ADR-012)
 */

import { Download } from 'lucide-react';
import type { NovaMetadata } from '@/types/nova';

interface ObjectSummaryProps {
  nova: NovaMetadata;
  /**
   * Href for the download button. Placeholder until bundle generation is
   * implemented — expected to be the pre-generated bundle zip path.
   */
  bundleHref: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const MONTH_NAMES = [
  '', // index 0 unused; months are 1-indexed
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
] as const;

/**
 * Format a YYYY-MM-DD discovery_date string for display.
 *
 * ADR-014: the day component is "00" when only month precision is available;
 * month and day are "00" when only year precision is available.
 *
 * Examples:
 *   "1901-02-22"  →  "22 February 1901"
 *   "1901-02-00"  →  "February 1901"
 *   "1901-00-00"  →  "1901"
 */
function formatDiscoveryDate(date: string): string {
  const parts = date.split('-');
  if (parts.length !== 3) return date; // fallback: display raw string

  const [year, month, day] = parts;

  if (month === '00') return year;

  const monthIndex = parseInt(month, 10);
  const monthName = MONTH_NAMES[monthIndex] ?? month;

  if (day === '00') return `${monthName} ${year}`;

  // parseInt removes a leading zero from the day ("02" → 2)
  return `${parseInt(day, 10)} ${monthName} ${year}`;
}

/** Capitalise the first letter of a nova_type string (e.g. "classical" → "Classical"). */
function formatNovaType(type: string): string {
  if (!type) return type;
  return type.charAt(0).toUpperCase() + type.slice(1);
}

// ── Shared sub-components ─────────────────────────────────────────────────────

/** A simple label / value pair in the metadata grid. */
function MetaRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <>
      <dt className="text-sm font-medium text-[var(--color-text-secondary)] whitespace-nowrap">
        {label}
      </dt>
      <dd className="text-sm">{children}</dd>
    </>
  );
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function ObjectSummary({ nova, bundleHref }: ObjectSummaryProps) {
  return (
    <div className="flex flex-col gap-5">

      {/* Primary name — page heading per ADR-011 */}
      <div>
        <h1 className="text-3xl font-semibold text-[var(--color-text-primary)] leading-tight">
          {nova.primary_name}
        </h1>

        {/* Aliases — comma-separated, labeled, below the heading */}
        {(() => {
          const filtered = nova.aliases.filter(
            (a) => a.trim().toLowerCase() !== nova.primary_name.trim().toLowerCase(),
          );
          return filtered.length > 0 ? (
            <p className="mt-1.5 text-sm italic text-[var(--color-text-tertiary)]">
              <span className="not-italic text-[var(--color-text-secondary)]">Also known as: </span>
              {filtered.join(', ')}
            </p>
          ) : null;
        })()}
      </div>

      {/*
       * Metadata grid
       *
       * Uses a two-column <dl> (definition list): labels on the left,
       * values on the right. <dl>/<dt>/<dd> is the correct semantic HTML
       * for labelled data pairs.
       *
       * `grid-cols-[auto_1fr]` makes the label column shrink-wrap to its
       * content, and the value column takes the remaining space.
       */}
      <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-2.5">

        {/* Coordinates — DM Mono font, `select-all` so one click selects the value */}
        <MetaRow label="RA">
          <span className="font-mono text-[var(--color-text-primary)] select-all cursor-text">
            {nova.ra}
          </span>
        </MetaRow>

        <MetaRow label="Dec">
          <span className="font-mono text-[var(--color-text-primary)] select-all cursor-text">
            {nova.dec}
          </span>
        </MetaRow>

        <MetaRow label="Discovery">
          <span className="text-[var(--color-text-primary)]">
            {formatDiscoveryDate(nova.discovery_date)}
            {nova.discovery_date_mjd != null && (
              <span className="text-[var(--color-text-tertiary)]">
                {' '}(MJD {nova.discovery_date_mjd.toFixed(1)})
              </span>
            )}
          </span>
        </MetaRow>

        {/* Nova type badge — pill chip per ADR-012 */}
        <MetaRow label="Type">
          <span
            className={[
              'inline-flex items-center px-2 py-0.5 rounded-full',
              'text-xs font-medium',
              'bg-[var(--color-interactive-subtle)] text-[var(--color-interactive)]',
            ].join(' ')}
            aria-label={`Nova type: ${formatNovaType(nova.nova_type)}`}
          >
            {formatNovaType(nova.nova_type)}
          </span>
        </MetaRow>

        <MetaRow label="Spectra">
          <span className="font-mono tabular-nums text-[var(--color-text-primary)]">
            {nova.spectra_count}
          </span>
        </MetaRow>

        <MetaRow label="Spectral visits">
          <span className="font-mono tabular-nums text-[var(--color-text-primary)]">
            {nova.spectral_visits > 0
              ? `${nova.spectral_visits} night${nova.spectral_visits !== 1 ? 's' : ''}`
              : '—'}
          </span>
        </MetaRow>

        <MetaRow label="Photometry">
          <span className="font-mono tabular-nums text-[var(--color-text-primary)]">
            {nova.photometry_count === 0 ? '—' : nova.photometry_count}
          </span>
        </MetaRow>

      </dl>

      {/*
       * Download bundle button — primary button per ADR-012.
       *
       * Uses <a> rather than <button> because it navigates to a file URL.
       * The href is a placeholder until the bundle generation pipeline is
       * implemented (ADR-014 bundle structure).
       */}
      <div className="pt-1">
        <a
          href={bundleHref}
          className={[
            'inline-flex items-center gap-2',
            'px-4 py-2 rounded-md',
            'text-sm font-medium',
            'bg-[var(--color-interactive)] text-[var(--color-text-inverse)]',
            'hover:bg-[var(--color-interactive-hover)]',
            'transition-colors',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus-ring)]',
          ].join(' ')}
          aria-label={`Download data bundle for ${nova.primary_name}`}
        >
          <Download size={15} aria-hidden="true" />
          Download bundle
        </a>
      </div>

    </div>
  );
}
