/**
 * VisualizationRegion — left column of the nova page.
 *
 * Contains:
 *   - Spectra viewer (renders when spectra.json is loaded and has data)
 *   - Light curve panel placeholder (Chunk 6)
 *
 * State handling follows a priority chain:
 *   1. Loading → skeleton placeholder
 *   2. Error   → error state (scoped; does not affect other sections)
 *   3. Empty   → ADR-012 empty state (LineChart icon + message)
 *   4. Data    → SpectraViewer component
 */

import type { ReactNode } from 'react';
import { TrendingDown, CircleAlert } from 'lucide-react';
import type { SpectraArtifact } from '@/types/nova';
import SpectraViewer from './SpectraViewer';

// ── Props ─────────────────────────────────────────────────────────────────────

interface VisualizationRegionProps {
  /** The full spectra artifact, or null while loading / on error. */
  spectraData: SpectraArtifact | null;
  /** True while spectra.json is still being fetched. */
  spectraLoading: boolean;
  /** True if the spectra.json fetch failed. */
  spectraError: boolean;
  /** Called when the user clicks "Try again" after a spectra fetch error. */
  onSpectraRetry?: () => void;
}

// ── PlaceholderBox ────────────────────────────────────────────────────────────
//
// Reusable placeholder for visualization slots that aren't implemented yet.
// Uses a dashed border to signal "intentionally empty" (vs. solid + error color
// for a data error). Per ADR-012 empty state pattern.

interface PlaceholderBoxProps {
  icon: ReactNode;
  label: string;
  sublabel: string;
  heightClass?: string;
}

function PlaceholderBox({
  icon,
  label,
  sublabel,
  heightClass = 'py-20',
}: PlaceholderBoxProps) {
  return (
    <div
      className={[
        'flex flex-col items-center justify-center gap-3',
        'rounded-md border border-dashed border-[var(--color-border-default)]',
        'bg-[var(--color-surface-secondary)]',
        heightClass,
      ].join(' ')}
      aria-label={label}
    >
      <div className="text-[var(--color-text-tertiary)]">{icon}</div>
      <div className="text-center px-4">
        <p className="text-sm font-medium text-[var(--color-text-secondary)]">
          {label}
        </p>
        <p className="text-xs text-[var(--color-text-tertiary)] mt-0.5">
          {sublabel}
        </p>
      </div>
    </div>
  );
}

// ── Spectra loading skeleton ──────────────────────────────────────────────────

function SpectraLoadingSkeleton() {
  return (
    <div
      className="animate-pulse rounded-md bg-[var(--color-surface-tertiary)] w-full"
      style={{ height: 480 }}
      aria-busy="true"
      aria-label="Loading spectra viewer"
    />
  );
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function VisualizationRegion({
  spectraData,
  spectraLoading,
  spectraError,
  onSpectraRetry,
}: VisualizationRegionProps) {
  return (
    <div className="flex flex-col gap-4">
      {/* ── Spectra viewer ─────────────────────────────────────────── */}
      {spectraLoading ? (
        <SpectraLoadingSkeleton />
      ) : spectraData !== null ? (
        // SpectraViewer handles its own empty state (no spectra in array)
        // and its own render error state internally.
        <SpectraViewer data={spectraData} onRetry={onSpectraRetry} />
      ) : spectraError ? (
        // Fetch-level error: spectra.json could not be loaded.
        <div
          className={[
            'flex flex-col items-center justify-center gap-3 py-24',
            'rounded-md border border-[var(--color-border-subtle)]',
            'bg-[var(--color-surface-secondary)]',
          ].join(' ')}
          aria-label="Spectra viewer error"
        >
          <CircleAlert
            size={32}
            className="text-[var(--color-status-error-fg)]"
            aria-hidden="true"
          />
          <p className="text-sm font-medium text-[var(--color-text-primary)]">
            Could not load spectra
          </p>
          <p className="text-sm text-[var(--color-text-secondary)]">
            The spectra data file could not be retrieved.
          </p>
          {onSpectraRetry && (
            <button
              onClick={onSpectraRetry}
              className={[
                'text-sm font-medium',
                'text-[var(--color-text-secondary)]',
                'hover:text-[var(--color-interactive)]',
                'transition-colors',
              ].join(' ')}
            >
              Try again
            </button>
          )}
        </div>
      ) : null}

      {/* ── Light curve panel placeholder ──────────────────────────── */}
      <PlaceholderBox
        icon={<TrendingDown size={32} aria-hidden="true" />}
        label="Light curve panel"
        sublabel="Light curve panel coming in Chunk 6."
        heightClass="py-16"
      />
    </div>
  );
}
