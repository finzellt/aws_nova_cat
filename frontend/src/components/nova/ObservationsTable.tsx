/**
 * ObservationsTable — renders the observations summary table on the nova page.
 *
 * Per ADR-014, this table is derived from spectra.json at render time, not
 * pre-aggregated. Each spectrum record becomes one row.
 *
 * Columns: Instrument | Telescope | Epoch (MJD) | λ Range (nm) | Provider
 */

import { Telescope } from 'lucide-react';
import type { SpectrumRecord } from '@/types/nova';

interface ObservationsTableProps {
  /** Spectrum records from spectra.json. Empty array while loading or on error. */
  spectra: SpectrumRecord[];
  loading: boolean;
  error: boolean;
}

// ── Loading skeleton ───────────────────────────────────────────────────────────

function LoadingSkeleton() {
  return (
    <div className="animate-pulse flex flex-col gap-2" aria-busy="true" aria-label="Loading observations">
      <div className="h-8 bg-[var(--color-surface-tertiary)] rounded" />
      <div className="h-8 bg-[var(--color-surface-tertiary)] rounded opacity-70" />
      <div className="h-8 bg-[var(--color-surface-tertiary)] rounded opacity-40" />
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
      aria-label="No observations available"
    >
      <Telescope
        size={28}
        className="text-[var(--color-text-tertiary)]"
        aria-hidden="true"
      />
      <p className="text-sm text-[var(--color-text-tertiary)]">
        {error
          ? 'Could not load observation data.'
          : 'No spectra available for this nova.'}
      </p>
    </div>
  );
}

// ── Component ─────────────────────────────────────────────────────────────────

const COLUMNS = [
  'Instrument',
  'Telescope',
  'Epoch (MJD)',
  'λ Range (nm)',
  'Provider',
] as const;

export default function ObservationsTable({
  spectra,
  loading,
  error,
}: ObservationsTableProps) {
  if (loading) return <LoadingSkeleton />;
  if (error || spectra.length === 0) return <EmptyState error={error} />;

  return (
    <div className="overflow-x-auto rounded-md border border-[var(--color-border-subtle)]">
      <table
        className="w-full border-collapse text-sm"
        aria-label="Spectra observations"
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
          {spectra.map((row, idx) => (
            <tr
              key={row.spectrum_id}
              className={[
                'border-b border-[var(--color-border-subtle)] last:border-0',
                idx % 2 === 0
                  ? 'bg-[var(--color-surface-primary)]'
                  : 'bg-[var(--color-surface-secondary)]',
              ].join(' ')}
            >
              {/* Instrument */}
              <td className="px-3 py-2 text-[var(--color-text-primary)]">
                {row.instrument}
              </td>

              {/* Telescope */}
              <td className="px-3 py-2 text-[var(--color-text-secondary)]">
                {row.telescope}
              </td>

              {/* Epoch — 4 decimal places gives ~8-second precision, per ADR-014 */}
              <td className="px-3 py-2 font-mono tabular-nums text-[var(--color-text-primary)]">
                {row.epoch_mjd.toFixed(4)}
              </td>

              {/* Wavelength range */}
              <td className="px-3 py-2 font-mono tabular-nums text-[var(--color-text-secondary)] whitespace-nowrap">
                {row.wavelength_min}–{row.wavelength_max}
              </td>

              {/* Provider */}
              <td className="px-3 py-2 text-[var(--color-text-secondary)]">
                {row.provider}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
