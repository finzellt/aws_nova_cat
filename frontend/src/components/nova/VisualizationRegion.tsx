/**
 * VisualizationRegion — left column of the nova page.
 *
 * In the MVP, this contains placeholder boxes for:
 *   - Spectra viewer (Chunk 5)
 *   - Light curve panel (Chunk 6)
 *
 * Each placeholder follows the ADR-012 empty-state pattern:
 * a dashed-border box with a centred icon and descriptive text.
 * The dashed border signals "this slot is intentionally empty" rather
 * than a data error (which would use a solid border + error colour).
 */

import type { ReactNode } from 'react';
import { LineChart, TrendingDown } from 'lucide-react';

interface VisualizationRegionProps {
  /** True while nova.json is still loading — determines subtitle text. */
  loading: boolean;
  /** Whether the nova has validated spectra (from nova.spectra_count > 0). */
  hasSpectra: boolean;
}

// ── PlaceholderBox ─────────────────────────────────────────────────────────────

interface PlaceholderBoxProps {
  /** Lucide icon element (or any ReactNode) centred in the box. */
  icon: ReactNode;
  /** Short label — names the component that will replace this placeholder. */
  label: string;
  /** Secondary line — current status or "coming in Chunk N" notice. */
  sublabel: string;
  /** Controls height of the box via Tailwind class. Default: py-20. */
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

// ── Component ─────────────────────────────────────────────────────────────────

export default function VisualizationRegion({
  loading,
  hasSpectra,
}: VisualizationRegionProps) {
  // Determine the spectra placeholder subtitle based on data state.
  let spectraSublabel: string;
  if (loading) {
    spectraSublabel = 'Loading…';
  } else if (!hasSpectra) {
    spectraSublabel = 'No validated spectra available for this nova.';
  } else {
    spectraSublabel = 'Spectra viewer coming in Chunk 5.';
  }

  return (
    <div className="flex flex-col gap-4">
      {/*
       * Spectra viewer slot.
       * Taller than the light curve slot because it is the primary
       * visualisation element (ADR-011).
       */}
      <PlaceholderBox
        icon={<LineChart size={32} aria-hidden="true" />}
        label="Spectra viewer"
        sublabel={spectraSublabel}
        heightClass="py-24"
      />

      {/* Light curve panel slot */}
      <PlaceholderBox
        icon={<TrendingDown size={32} aria-hidden="true" />}
        label="Light curve panel"
        sublabel="Light curve panel coming in Chunk 6."
        heightClass="py-16"
      />
    </div>
  );
}
