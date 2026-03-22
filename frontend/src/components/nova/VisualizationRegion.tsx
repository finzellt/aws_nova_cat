'use client';

/**
 * VisualizationRegion — left column of the nova page.
 *
 * Contains:
 *   - Spectra viewer (renders when spectra.json is loaded and has data)
 *   - Light curve panel (renders when photometry.json is loaded and has data)
 *
 * State handling follows a priority chain per section:
 *   1. Loading → skeleton placeholder or loading message
 *   2. Error   → error state (scoped; does not affect other sections)
 *   3. Empty   → ADR-012 empty state (icon + message)
 *   4. Data    → rendered component
 *
 * The photometry fetch lives here rather than in NovaPage because:
 *   - It keeps loading/error states scoped to the light curve panel
 *   - It avoids fetching photometry.json when photometry_count is 0
 *   - It mirrors the independent-fetch pattern used for references
 */

import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { Activity, CircleAlert } from 'lucide-react';
import type { SpectraArtifact } from '@/types/nova';
import type { PhotometryArtifact } from '@/types/photometry';
import SpectraViewer from './SpectraViewer';
import LightCurvePanel from './LightCurvePanel';

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
  /** Whether the nova has photometry data (from nova.photometry_count > 0). */
  hasPhotometry: boolean;
  /**
   * Base path for fetching per-nova artifacts, e.g. "/data/nova/GK%20Per".
   * Used to fetch photometry.json independently within this component.
   */
  basePath: string;
}

// ── PlaceholderBox ────────────────────────────────────────────────────────────
//
// Reusable placeholder for visualization slots that aren't implemented yet
// or have no data. Uses a dashed border to signal "intentionally empty"
// (vs. solid + error color for a data error). Per ADR-012 empty state pattern.

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
  hasPhotometry,
  basePath,
}: VisualizationRegionProps) {
  // ── Photometry fetch ────────────────────────────────────────────────────
  /**
   * Photometry is fetched here rather than in NovaPage because:
   *   1. It keeps the loading/error states scoped to the light curve panel
   *   2. It avoids fetching photometry.json when photometry_count is 0
   *   3. It mirrors the independent-fetch pattern used for references
   *
   * We skip the fetch entirely when hasPhotometry is false.
   */
  const [photometry, setPhotometry] = useState<PhotometryArtifact | null>(null);
  const [photometryError, setPhotometryError] = useState(false);
  const [photometryLoading, setPhotometryLoading] = useState(false);

  useEffect(() => {
    if (!hasPhotometry) return;

    let cancelled = false;
    setPhotometryLoading(true);
    setPhotometryError(false);

    async function fetchPhotometry() {
      try {
        const res = await fetch(`${basePath}/photometry.json`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = (await res.json()) as PhotometryArtifact;
        if (!cancelled) {
          setPhotometry(data);
          setPhotometryLoading(false);
        }
      } catch {
        if (!cancelled) {
          setPhotometryError(true);
          setPhotometryLoading(false);
        }
      }
    }

    void fetchPhotometry();

    /**
     * Cleanup function: if the component unmounts or basePath changes
     * before the fetch completes, we set `cancelled = true` to prevent
     * calling setState on an unmounted component. Standard React pattern
     * for async effects.
     */
    return () => { cancelled = true; };
  }, [hasPhotometry, basePath]);

  // ── Retry handler for photometry errors ─────────────────────────────────
  function handleRetryPhotometry() {
    setPhotometry(null);
    setPhotometryError(false);
    setPhotometryLoading(true);

    fetch(`${basePath}/photometry.json`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => {
        setPhotometry(data as PhotometryArtifact);
        setPhotometryLoading(false);
      })
      .catch(() => {
        setPhotometryError(true);
        setPhotometryLoading(false);
      });
  }

  // ── Light curve section renderer ────────────────────────────────────────
  /**
   * Four possible states:
   *   1. No photometry data → ADR-013 empty state with Activity icon
   *   2. Fetch in progress  → loading indicator
   *   3. Fetch error        → scoped error with "Try again" ghost button
   *   4. Data loaded        → LightCurvePanel
   */
  function renderLightCurveSection(): ReactNode {
    // No photometry data for this nova
    if (!hasPhotometry) {
      return (
        <PlaceholderBox
          icon={<Activity size={32} aria-hidden="true" />}
          label="Light curve"
          sublabel="No photometry data available for this nova."
          heightClass="py-16"
        />
      );
    }

    // Fetch in progress
    if (photometryLoading) {
      return (
        <div className="flex items-center justify-center py-16">
          <p className="text-sm text-[var(--color-text-tertiary)]">
            Loading photometry…
          </p>
        </div>
      );
    }

    // Fetch failed — ADR-013 scoped error state
    if (photometryError) {
      return (
        <div
          className={[
            'flex flex-col items-center justify-center py-12 gap-3',
            'rounded-md border border-[var(--color-border-subtle)]',
            'bg-[var(--color-surface-secondary)]',
          ].join(' ')}
        >
          <CircleAlert
            size={28}
            className="text-[var(--color-status-error-fg)]"
            aria-hidden="true"
          />
          <p className="text-sm text-[var(--color-status-error-fg)]">
            Could not load photometry data.
          </p>
          <button
            onClick={handleRetryPhotometry}
            className={[
              'px-3 py-1.5 text-sm font-medium rounded-md',
              'text-[var(--color-interactive)]',
              'border border-[var(--color-interactive)]',
              'hover:bg-[var(--color-interactive-subtle)]',
              'focus-visible:outline-none focus-visible:ring-2',
              'focus-visible:ring-[var(--color-focus-ring)]',
              'transition-colors',
            ].join(' ')}
          >
            Try again
          </button>
        </div>
      );
    }

    // Data loaded
    return <LightCurvePanel data={photometry} />;
  }

  return (
    <div className="flex flex-col gap-4">
      {/* ── Spectra viewer ─────────────────────────────────────────── */}
      {spectraLoading ? (
        <SpectraLoadingSkeleton />
      ) : spectraData !== null ? (
        <SpectraViewer data={spectraData} onRetry={onSpectraRetry} />
      ) : spectraError ? (
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

      {/* ── Light curve panel ──────────────────────────────────────── */}
      {renderLightCurveSection()}
    </div>
  );
}
