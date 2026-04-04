'use client';

/**
 * ReferencesTable — renders the literature references table on the nova page.
 *
 * Columns: Author / Year | Title | Bibcode (linked to ADS)
 *
 * Per ADR-014, references.json is fetched independently of nova.json to
 * allow this table to lazy-load without blocking the metadata region.
 * The loading/error states here are therefore scoped to this section only.
 */

import { useState } from 'react';
import { BookOpen, ExternalLink } from 'lucide-react';
import type { Reference } from '@/types/nova';

const COLLAPSED_ROW_COUNT = 4;

interface ReferencesTableProps {
  /** Reference records from references.json. Empty array while loading or on error. */
  references: Reference[];
  loading: boolean;
  error: boolean;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Format an author list and year into a short citation label.
 *
 * Authors arrive as "LastName, F." strings (ADS format).
 * We extract the last name only and apply the following rule:
 *   1 author  → "Smith 2001"
 *   2 authors → "Smith & Jones 2001"
 *   3+ authors → "Smith et al. 2001"
 */
function formatCitation(authors: string[], year: number): string {
  // Split on the first comma to get the last name from "LastName, F." format.
  // If the author string has no comma (some ADS records), use the full string.
  const lastNames = authors.map((a) => {
    const commaIdx = a.indexOf(',');
    return commaIdx !== -1 ? a.slice(0, commaIdx).trim() : a.trim();
  });

  const yearStr = String(year);

  if (lastNames.length === 0) return yearStr;
  if (lastNames.length === 1) return `${lastNames[0]} ${yearStr}`;
  if (lastNames.length === 2) return `${lastNames[0]} & ${lastNames[1]} ${yearStr}`;
  return `${lastNames[0]} et al. ${yearStr}`;
}

// ── Loading skeleton ───────────────────────────────────────────────────────────

function LoadingSkeleton() {
  return (
    <div className="animate-pulse flex flex-col gap-2" aria-busy="true" aria-label="Loading references">
      <div className="h-8 bg-[var(--color-surface-tertiary)] rounded" />
      <div className="h-8 bg-[var(--color-surface-tertiary)] rounded opacity-70" />
    </div>
  );
}

// ── Empty / error state ────────────────────────────────────────────────────────

function EmptyState({ error }: { error: boolean }) {
  return (
    <div
      className={[
        'flex flex-col items-center justify-center py-10 gap-2',
        'rounded-md border border-[var(--color-border-subtle)]',
        'bg-[var(--color-surface-secondary)]',
      ].join(' ')}
      aria-label="No references available"
    >
      <BookOpen
        size={28}
        className="text-[var(--color-text-tertiary)]"
        aria-hidden="true"
      />
      <p className="text-sm text-[var(--color-text-tertiary)]">
        {error
          ? 'Could not load references.'
          : 'No references recorded for this nova.'}
      </p>
    </div>
  );
}

// ── Component ─────────────────────────────────────────────────────────────────

const COLUMNS = ['Author / Year', 'Title', 'Bibcode'] as const;

export default function ReferencesTable({
  references,
  loading,
  error,
}: ReferencesTableProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  if (loading) return <LoadingSkeleton />;
  if (error || references.length === 0) return <EmptyState error={error} />;

  const canCollapse = references.length > COLLAPSED_ROW_COUNT;
  const visibleRows = canCollapse && !isExpanded
    ? references.slice(0, COLLAPSED_ROW_COUNT)
    : references;

  return (
    <div>
      <div className="overflow-x-auto rounded-md border border-[var(--color-border-subtle)]">
        <table
          className="w-full border-collapse text-sm"
          aria-label="Literature references"
        >
          <thead>
            <tr className="bg-[var(--color-surface-secondary)] border-b border-[var(--color-border-subtle)]">
              {COLUMNS.map((col) => (
                <th
                  key={col}
                  scope="col"
                  className="px-3 py-2 text-xs font-semibold text-left text-[var(--color-text-secondary)] whitespace-nowrap"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>

          <tbody>
            {visibleRows.map((ref, idx) => (
              <tr
                key={ref.bibcode}
                className={[
                  'border-b border-[var(--color-border-subtle)] last:border-0',
                  idx % 2 === 0
                    ? 'bg-[var(--color-surface-primary)]'
                    : 'bg-[var(--color-surface-secondary)]',
                ].join(' ')}
              >
                {/* Author / Year — linked to ADS, with ExternalLink icon */}
                <td className="px-3 py-2 font-mono text-xs whitespace-nowrap align-top">
                  <a
                    href={ref.ads_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={[
                      'inline-flex items-center gap-1',
                      'text-[var(--color-interactive)]',
                      'hover:underline',
                      'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus-ring)] rounded',
                    ].join(' ')}
                    aria-label={`View ${formatCitation(ref.authors, ref.year)} on ADS (opens in new tab)`}
                  >
                    {formatCitation(ref.authors, ref.year)}
                    <ExternalLink size={11} aria-hidden="true" />
                  </a>
                </td>

                {/* Title — allows wrapping so long titles don't blow out the layout */}
                <td className="px-3 py-2 text-[var(--color-text-primary)] max-w-xs align-top">
                  {ref.title}
                </td>

                {/* Bibcode — plain text identifier */}
                <td className="px-3 py-2 font-mono text-xs text-[var(--color-text-primary)] whitespace-nowrap align-top">
                  {ref.bibcode}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {canCollapse && (
        <button
          type="button"
          onClick={() => setIsExpanded((prev) => !prev)}
          className="mt-2 text-xs text-[var(--color-text-tertiary)] hover:underline hover:text-[var(--color-text-secondary)] cursor-pointer"
        >
          {isExpanded
            ? 'Show less \u25B4'
            : `Show all ${references.length} items \u25BE`}
        </button>
      )}
    </div>
  );
}
