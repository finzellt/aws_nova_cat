'use client';

/**
 * SpectraViewer — waterfall spectra plot for the nova page.
 *
 * Design spec: ADR-013 | Data contract: ADR-014 | Visual tokens: ADR-012
 *
 * Layer A: basic waterfall rendering, color, hover, empty/error states.
 * Layer B: epoch format toggle, temporal scale toggle.
 * Layer C: legend strip, spectrum isolation, single-spectrum mode.
 * Layer D (this version) adds:
 *   - Three toggle button groups: Fe II, He/N, Nebular
 *   - Full-height vertical dashed lines at spectral feature wavelengths
 *   - Line labels above the plot area
 *   - Curated subset of Williams nova line list (~20 prominent lines)
 *   - Blended lines at mean wavelength with blend noted in label
 */

import { useCallback, useMemo, useState } from 'react';
import dynamic from 'next/dynamic';
import { LineChart, CircleAlert } from 'lucide-react';
import type { SpectraArtifact, SpectraRegimeRecord, SpectrumRecord } from '@/types/nova';

// ── Dynamic Plotly import ─────────────────────────────────────────────────────

const Plot = dynamic(() => import('react-plotly.js'), {
  ssr: false,
  loading: () => <PlotSkeleton />,
});

// ── Types ─────────────────────────────────────────────────────────────────────

export interface SpectraViewerProps {
  data: SpectraArtifact;
  onRetry?: () => void;
}

type EpochFormat = 'dpo' | 'mjd' | 'calendar';
type FluxScale = 'sqrt' | 'linear';
/** The three spectral feature groups from ADR-013. */
type FeatureGroup = 'fe2' | 'hen' | 'nebular';

interface PreparedSpectrum {
  record: SpectrumRecord;
  color: string;
  epochLabel: string;
  displayIndex: number;
}

// ── Spectral feature line list ────────────────────────────────────────────────
//
// A curated subset of the Williams nova line list, trimmed to the most
// prominent features seen in classical nova spectra. ADR-013 target:
// fewer than 10% of the original line count.
//
// Wavelengths are in nanometres (Williams list is in Ångströms; divided by 10).
// Blended lines are plotted at the mean wavelength with the blend noted.
//
// Each entry belongs to one of three groups:
//   - fe2: Fe II emission lines, characteristic of Fe II-type novae
//   - hen: Hydrogen, helium, and nitrogen lines, characteristic of He/N-type novae
//   - nebular: Forbidden and coronal lines from the nebular phase
//
// This list is an implementation detail (ADR-013 Open Question 2) and can
// be refined based on real data.

interface SpectralLine {
  /** Wavelength in nm (mean for blends). */
  wavelength_nm: number;
  /** Display label (shown above the plot). */
  label: string;
  /** Which toggle group this line belongs to. */
  group: FeatureGroup;
  /** When true, line is shown whenever ANY feature group is active. */
  universal?: boolean;
}

const SPECTRAL_LINES: SpectralLine[] = [
  // ── Fe II group ─────────────────────────────────────────────────────────
  { wavelength_nm: 393.4, label: 'Ca II 393',       group: 'fe2' },
  { wavelength_nm: 396.8, label: 'Ca II 397',       group: 'fe2' },
  { wavelength_nm: 449.1, label: 'Fe II 449',       group: 'fe2' },
  { wavelength_nm: 450.8, label: 'Fe II 451',       group: 'fe2' },
  { wavelength_nm: 531.7, label: 'Fe II 532',       group: 'fe2' },
  { wavelength_nm: 536.3, label: 'Fe II 536',       group: 'fe2' },
  { wavelength_nm: 589.2, label: 'Na D 589',        group: 'fe2' },
  { wavelength_nm: 614.8, label: 'Fe II 615',       group: 'fe2' },
  { wavelength_nm: 634.7, label: 'Si II 635',       group: 'fe2' },
  { wavelength_nm: 637.1, label: 'Si II 637',       group: 'fe2' },
  { wavelength_nm: 777.3, label: 'O I 777',         group: 'fe2' },
  { wavelength_nm: 822.7, label: 'O I 823',         group: 'fe2' },
  { wavelength_nm: 844.6, label: 'O I 845',         group: 'fe2' },

  // ── He/N group ──────────────────────────────────────────────────────────
  { wavelength_nm: 463.8, label: 'N III 464',       group: 'hen' },
  { wavelength_nm: 468.6, label: 'He II 469',       group: 'hen' },
  { wavelength_nm: 567.9, label: 'N II 568',        group: 'hen' },
  { wavelength_nm: 587.6, label: 'He I 588',        group: 'hen' },
  { wavelength_nm: 706.5, label: 'He I 707',        group: 'hen' },
  { wavelength_nm: 823.7, label: 'He II 824',       group: 'hen' },

  // ── Nebular group ───────────────────────────────────────────────────────
  { wavelength_nm: 515.8, label: '[Fe VII] 516',    group: 'nebular' },
  { wavelength_nm: 572.1, label: '[Fe VII] 572',    group: 'nebular' },
  { wavelength_nm: 608.6, label: '[Fe VII] 609',    group: 'nebular' },
  { wavelength_nm: 630.0, label: '[O I] 630',       group: 'nebular' },
  { wavelength_nm: 700.6, label: '[Ar V] 701',      group: 'nebular' },
  { wavelength_nm: 732.0, label: '[O II] 732',      group: 'nebular' },
  { wavelength_nm: 772.5, label: '[S I] 773',       group: 'nebular' },
  { wavelength_nm: 906.9, label: '[S III] 907',     group: 'nebular' },
  { wavelength_nm: 953.1, label: '[S III] 953',     group: 'nebular' },

  // ── Balmer series (universal) ───────────────────────────────────────────
  { wavelength_nm: 656.279, label: 'Hα',            group: 'hen', universal: true },
  { wavelength_nm: 486.135, label: 'Hβ',            group: 'hen', universal: true },
  { wavelength_nm: 434.072, label: 'Hγ',            group: 'hen', universal: true },
  { wavelength_nm: 410.173, label: 'Hδ',            group: 'hen', universal: true },
  { wavelength_nm: 397.007, label: 'Hε',            group: 'hen', universal: true },
  { wavelength_nm: 388.906, label: 'Hζ',            group: 'hen', universal: true },

  // ── Paschen series (universal) ──────────────────────────────────────────
  { wavelength_nm: 954.86,  label: 'Pa 8',          group: 'hen', universal: true },
  { wavelength_nm: 923.15,  label: 'Pa 9',          group: 'hen', universal: true },
  { wavelength_nm: 901.74,  label: 'Pa 10',         group: 'hen', universal: true },
  { wavelength_nm: 886.52,  label: 'Pa 11',         group: 'hen', universal: true },
  { wavelength_nm: 875.29,  label: 'Pa 12',         group: 'hen', universal: true },
  { wavelength_nm: 866.74,  label: 'Pa 13',         group: 'hen', universal: true },
  { wavelength_nm: 860.08,  label: 'Pa 14',         group: 'hen', universal: true },
  { wavelength_nm: 854.77,  label: 'Pa 15',         group: 'hen', universal: true },
  { wavelength_nm: 850.48,  label: 'Pa 16',         group: 'hen', universal: true },
  { wavelength_nm: 846.96,  label: 'Pa 17',         group: 'hen', universal: true },
];

/** Colors for each feature group. Muted so they don't compete with spectra. */
const FEATURE_GROUP_COLORS: Record<FeatureGroup, string> = {
  fe2:     '#B07D2B',  // warm brown — evokes iron
  hen:     '#7B68A0',  // muted purple — distinct from blue/amber spectra ramp
  nebular: '#5A8F6E',  // sage green — associated with emission nebulae
};

const FEATURE_GROUP_LABELS: Record<FeatureGroup, string> = {
  fe2:     'Fe II',
  hen:     'He / N',
  nebular: 'Nebular',
};

// ── Color palettes ────────────────────────────────────────────────────────────

// ── Color palettes ────────────────────────────────────────────────────────────
//
// Design principles:
//   1. Warm/cool alternation baked into array order — sequential indexing
//      guarantees adjacent spectra land on contrasting hues (four-color-theorem
//      style).
//   2. No yellows. Every color must be legible on a white/light background.
//   3. Colorblind-safe: no adjacent pair relies on a red↔green distinction.
//      Verified against deuteranopia and protanopia simulations.
//   4. SPARSE_RAMP (≤8 spectra) interpolates across a blue→teal→rose gradient.
//      DENSE_PALETTE (>8 spectra) uses 14 maximally-distinct colors with
//      warm/cool alternation so that modular indexing keeps neighbors distinct
//      even when wrapping.

/** Blue → teal → rose ramp for ≤8 spectra. No yellows. */
const SPARSE_RAMP = [
  '#0072B2',  // strong blue
  '#2E91C4',  // sky blue
  '#56B4E9',  // light blue
  '#009E73',  // bluish green
  '#5EBD98',  // medium teal
  '#CC79A7',  // muted pink
  '#D55E00',  // vermilion
  '#882255',  // wine
];

/**
 * 14-color palette with warm/cool alternation.
 *
 * Index:  0        1        2        3        4        5        6
 * Hue:   cool     warm     cool     warm     cool     warm     cool
 *        blue     verm.    teal     rose     indigo   coral    green
 *
 * Index:  7        8        9       10       11       12       13
 * Hue:   warm     cool     warm     cool     warm     cool     warm
 *        magenta  cyan     brick    purple   salmon   slate    wine
 *
 * Any two adjacent indices (including 13→0 wrap) contrast in temperature.
 */
const DENSE_PALETTE = [
  '#0072B2',  //  0  strong blue        (cool)
  '#D55E00',  //  1  vermilion          (warm)
  '#009E73',  //  2  bluish green       (cool)
  '#CC79A7',  //  3  muted rose         (warm)
  '#4477AA',  //  4  steel indigo       (cool)
  '#EE6677',  //  5  soft coral         (warm)
  '#228833',  //  6  forest green       (cool)
  '#AA3377',  //  7  magenta            (warm)
  '#66CCEE',  //  8  bright cyan        (cool)
  '#A85032',  //  9  burnt sienna       (warm)
  '#6644AA',  // 10  deep purple        (cool)
  '#E07B53',  // 11  terracotta/salmon  (warm)
  '#5F7F8A',  // 12  cool slate         (cool)
  '#882255',  // 13  wine               (warm)
];

/**
 * Assign a color to a spectrum by its sort position in the waterfall.
 *
 * Dense mode (>8 spectra): direct sequential index into DENSE_PALETTE.
 *   Warm/cool alternation in the palette guarantees adjacent traces contrast.
 *   Wraps via modulo for collections larger than 14.
 *
 * Sparse mode (≤8 spectra): nearest-neighbor pick from SPARSE_RAMP based on
 *   normalized position (unchanged from prior implementation).
 */
function assignColor(sortIndex: number, totalCount: number, isDense: boolean): string {
  if (isDense) {
    return DENSE_PALETTE[sortIndex % DENSE_PALETTE.length];
  }
  if (totalCount === 1) return SPARSE_RAMP[0];
  const t = sortIndex / (totalCount - 1);
  const scaledIndex = t * (SPARSE_RAMP.length - 1);
  const lo = Math.floor(scaledIndex);
  const hi = Math.min(lo + 1, SPARSE_RAMP.length - 1);
  return scaledIndex - lo < 0.5 ? SPARSE_RAMP[lo] : SPARSE_RAMP[hi];
}

// ── Helpers: temporal ─────────────────────────────────────────────────────────

/**
 * Pick the best spectrum from a same-day group.
 * Priority: wavelength range > 100nm, then point count ≥ 2000, then broadest range.
 */
function pickBestInGroup(group: SpectrumRecord[]): SpectrumRecord {
  if (group.length === 1) return group[0];
  return group.reduce((best, cur) => {
    const bestRange = best.wavelength_max - best.wavelength_min;
    const curRange = cur.wavelength_max - cur.wavelength_min;
    const bestWide = bestRange > 100 ? 1 : 0;
    const curWide = curRange > 100 ? 1 : 0;
    if (curWide !== bestWide) return curWide > bestWide ? cur : best;
    const bestDense = best.wavelengths.length >= 2000 ? 1 : 0;
    const curDense = cur.wavelengths.length >= 2000 ? 1 : 0;
    if (curDense !== bestDense) return curDense > bestDense ? cur : best;
    return curRange > bestRange ? cur : best;
  });
}

/** Collapse same-day spectra (epochKey within 0.5) to one per day. */
function deduplicateByDay(
  spectra: SpectrumRecord[],
  epochKey: (s: SpectrumRecord) => number,
): SpectrumRecord[] {
  if (spectra.length <= 1) return spectra;
  const sorted = [...spectra].sort((a, b) => epochKey(a) - epochKey(b));
  const groups: SpectrumRecord[][] = [[sorted[0]]];
  for (let i = 1; i < sorted.length; i++) {
    const groupStart = epochKey(groups[groups.length - 1][0]);
    if (epochKey(sorted[i]) - groupStart <= 0.5) {
      groups[groups.length - 1].push(sorted[i]);
    } else {
      groups.push([sorted[i]]);
    }
  }
  return groups.map(pickBestInGroup);
}

/**
 * Drop spectra that are too close together on a logarithmic DPO scale.
 * Always keeps the first and last spectrum. For positive epoch values,
 * requires at least `minRatio` multiplicative separation between adjacent
 * kept spectra. Non-positive epochs (pre-outburst / unknown) are always kept.
 */
function enforceMinLogSeparation(
  spectra: SpectrumRecord[],
  epochKey: (s: SpectrumRecord) => number,
  minRatio: number = 1.25,
): SpectrumRecord[] {
  if (spectra.length <= 2) return spectra;
  const sorted = [...spectra].sort((a, b) => epochKey(a) - epochKey(b));
  const kept: SpectrumRecord[] = [sorted[0]];
  for (let i = 1; i < sorted.length - 1; i++) {
    const lastKeptEpoch = epochKey(kept[kept.length - 1]);
    const thisEpoch = epochKey(sorted[i]);
    if (lastKeptEpoch > 0 && thisEpoch > 0 && thisEpoch / lastKeptEpoch >= minRatio) {
      kept.push(sorted[i]);
    } else if (lastKeptEpoch <= 0 || thisEpoch <= 0) {
      kept.push(sorted[i]);
    }
  }
  kept.push(sorted[sorted.length - 1]);
  return kept;
}

function selectRepresentativeSubset(
  spectra: SpectrumRecord[],
  epochKey: (s: SpectrumRecord) => number,
  targetCount: number = 8,
): SpectrumRecord[] {
  const deduped = deduplicateByDay(spectra, epochKey);
  if (deduped.length <= targetCount) return deduped;
  const sorted = [...deduped].sort((a, b) => epochKey(a) - epochKey(b));
  const minEpoch = epochKey(sorted[0]);
  const maxEpoch = epochKey(sorted[sorted.length - 1]);
  const logMin = Math.log10(1);
  const logMax = Math.log10(maxEpoch - minEpoch + 1);
  const targets: number[] = [];
  for (let i = 0; i < targetCount; i++) {
    const logVal = logMin + (i / (targetCount - 1)) * (logMax - logMin);
    targets.push(Math.pow(10, logVal) + minEpoch - 1);
  }
  const selected = new Set<number>();
  for (const target of targets) {
    let bestIdx = -1;
    let bestDist = Infinity;
    for (let i = 0; i < sorted.length; i++) {
      if (selected.has(i)) continue;
      const dist = Math.abs(epochKey(sorted[i]) - target);
      if (dist < bestDist) { bestDist = dist; bestIdx = i; }
    }
    if (bestIdx >= 0) selected.add(bestIdx);
  }
  selected.add(0);
  selected.add(sorted.length - 1);
  const logSpaced = [...selected].sort((a, b) => a - b).map((i) => sorted[i]);
  return enforceMinLogSeparation(logSpaced, epochKey);
}

// ── Helpers: epoch formatting ─────────────────────────────────────────────────

const MONTH_ABBR = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
];

function mjdToCalendarDate(mjd: number): string {
  const MJD_UNIX_EPOCH = 40587;
  const MS_PER_DAY = 86_400_000;
  const date = new Date((mjd - MJD_UNIX_EPOCH) * MS_PER_DAY);
  return `${date.getUTCFullYear()} ${MONTH_ABBR[date.getUTCMonth()]} ${date.getUTCDate()}`;
}

function formatEpochLabel(spectrum: SpectrumRecord, format: EpochFormat): string {
  switch (format) {
    case 'dpo': return `Day ${Math.round(spectrum.days_since_outburst!)}`;
    case 'mjd': return Math.round(spectrum.epoch_mjd).toString();
    case 'calendar': return mjdToCalendarDate(spectrum.epoch_mjd);
  }
}

function getEpochValue(spectrum: SpectrumRecord, format: EpochFormat): number {
  if (format === 'dpo' && spectrum.days_since_outburst !== null) {
    return spectrum.days_since_outburst;
  }
  return spectrum.epoch_mjd;
}

// ── Helpers: collision-aware waterfall packing ───────────────────────────────

/** Linear interpolation of (wavelengths, flux) onto a common grid. */
function interpolateToGrid(
  wavelengths: number[],
  flux: number[],
  grid: number[],
): number[] {
  const result = new Array<number>(grid.length);
  let j = 0; // pointer into wavelengths
  for (let g = 0; g < grid.length; g++) {
    const gw = grid[g];
    // Outside spectrum range → NaN (no coverage)
    if (gw < wavelengths[0] || gw > wavelengths[wavelengths.length - 1]) {
      result[g] = NaN;
      continue;
    }
    // Advance j so wavelengths[j] <= gw < wavelengths[j+1]
    while (j < wavelengths.length - 2 && wavelengths[j + 1] < gw) j++;
    const w0 = wavelengths[j], w1 = wavelengths[j + 1];
    const t = (gw - w0) / (w1 - w0);
    result[g] = flux[j] + t * (flux[j + 1] - flux[j]);
  }
  return result;
}

const PACKING_PADDING = 0.096;
const PACKING_MIN_GAP = 0.05;
const PACKING_GAP_CAP_RATIO = 2.5;  // max gap = 2.5× median gap
const GRID_POINTS = 500;

interface PackingResult {
  baselines: number[];
  globalScale: number;
  yMin: number;
  yMax: number;
}

/**
 * Collision-aware baseline computation for waterfall mode.
 * Places spectra as close together as possible without trace overlap.
 */
function computeWaterfallPacking(
  preparedSpectra: PreparedSpectrum[],
  compressed: boolean,
  globalWlMin: number,
  globalWlMax: number,
): PackingResult {
  const N = preparedSpectra.length;
  if (N === 0) return { baselines: [], globalScale: 1, yMin: 0, yMax: 1 };

  // Build common wavelength grid
  const grid = Array.from(
    { length: GRID_POINTS },
    (_, i) => globalWlMin + (i / (GRID_POINTS - 1)) * (globalWlMax - globalWlMin),
  );

  // Compute display flux (optionally sqrt-compressed) interpolated onto grid
  const gridFlux: number[][] = [];
  const peakHeights: number[] = [];
  const troughDepths: number[] = [];
  for (const ps of preparedSpectra) {
    const spec = ps.record;
    const displayFlux = compressed
      ? spec.flux_normalized.map((f) => Math.sqrt(Math.max(f, 0)))
      : spec.flux_normalized;
    const interpolated = interpolateToGrid(spec.wavelengths, displayFlux, grid);
    gridFlux.push(interpolated);
    let peak = 0;
    let trough = 0;
    for (const f of interpolated) {
      if (!isNaN(f) && f > peak) peak = f;
      if (!isNaN(f) && f < trough) trough = f;
    }
    peakHeights.push(peak);
    troughDepths.push(trough);
  }

  const avgPeak = peakHeights.reduce((a, b) => a + b, 0) / N;
  const padding = PACKING_PADDING * avgPeak;

  // Place baselines
  const baselines = new Array<number>(N);
  baselines[0] = -1 * Math.min(0, troughDepths[0]);
  for (let i = 1; i < N; i++) {
    const prev = gridFlux[i - 1];
    const cur = gridFlux[i];
    let maxSep = -Infinity;
    for (let w = 0; w < GRID_POINTS; w++) {
      // Skip wavelengths where either spectrum has no coverage
      if (isNaN(prev[w]) || isNaN(cur[w])) continue;
      const sep = prev[w] - cur[w];
      if (sep > maxSep) maxSep = sep;
    }
    // No overlapping wavelengths at all → use minimum gap only
    if (maxSep === -Infinity) maxSep = 0;
    const rawGap = maxSep + padding;
    baselines[i] = baselines[i - 1] + Math.max(rawGap, PACKING_MIN_GAP);
  }

  // ── Gap capping — equitable vertical spacing ────────────────────────
  // Collision-aware packing can produce extreme gaps when two adjacent
  // spectra have very different flux profiles.  Cap any gap that exceeds
  // GAP_CAP_RATIO × median to prevent a few spectra from dominating the
  // y-range while the rest are compressed into a sliver.
  if (N >= 3) {
    const gaps: number[] = [];
    for (let i = 1; i < N; i++) {
      gaps.push(baselines[i] - baselines[i - 1]);
    }
    const sortedGaps = [...gaps].sort((a, b) => a - b);
    const medianGap = sortedGaps[Math.floor((sortedGaps.length - 1) / 2)];
    const maxAllowedGap = PACKING_GAP_CAP_RATIO * medianGap;

    let needsRebuild = false;
    for (const g of gaps) {
      if (g > maxAllowedGap) { needsRebuild = true; break; }
    }

    if (needsRebuild) {
      for (let i = 1; i < N; i++) {
        const gap = baselines[i] - baselines[i - 1];
        baselines[i] = baselines[i - 1] + Math.min(gap, maxAllowedGap);
      }
    }
  }

  // Rescale to fill plot: total extent is top baseline + top peak
  const totalExtent = baselines[N - 1] + peakHeights[N - 1];
  // We want to fill ~N units of vertical space (similar visual density to before)
  const targetExtent = N;
  const globalScale = totalExtent > 0 ? targetExtent / totalExtent : 1;

  const yMin = -padding * globalScale;
  const yMax = (baselines[N - 1] + peakHeights[N - 1] + padding) * globalScale;

  return { baselines, globalScale, yMin, yMax };
}

// ── Presentational sub-components ─────────────────────────────────────────────

function PlotSkeleton() {
  return (
    <div
      className="animate-pulse rounded-md bg-[var(--color-surface-tertiary)] w-full"
      style={{ height: 480 }}
      aria-busy="true"
      aria-label="Loading spectra viewer"
    />
  );
}

function EmptyState() {
  return (
    <div
      className={[
        'flex flex-col items-center justify-center gap-3 py-24',
        'rounded-md border border-[var(--color-border-subtle)]',
        'bg-[var(--color-surface-secondary)]',
      ].join(' ')}
      aria-label="No spectra available"
    >
      <LineChart size={32} className="text-[var(--color-text-tertiary)]" aria-hidden="true" />
      <div className="text-center px-4">
        <p className="text-base font-medium text-[var(--color-text-secondary)]">No spectra available</p>
        <p className="text-sm text-[var(--color-text-tertiary)] mt-1">
          No validated spectra have been ingested for this nova yet.
        </p>
      </div>
    </div>
  );
}

function ErrorState({ onRetry }: { onRetry?: () => void }) {
  return (
    <div
      className={[
        'flex flex-col items-center justify-center gap-3 py-24',
        'rounded-md border border-[var(--color-border-subtle)]',
        'bg-[var(--color-surface-secondary)]',
      ].join(' ')}
      aria-label="Spectra viewer error"
    >
      <CircleAlert size={32} className="text-[var(--color-status-error-fg)]" aria-hidden="true" />
      <p className="text-sm font-medium text-[var(--color-text-primary)]">Failed to render spectra</p>
      <p className="text-sm text-[var(--color-text-secondary)]">
        An error occurred while building the waterfall plot.
      </p>
      {onRetry && (
        <button
          onClick={onRetry}
          className={[
            'text-sm font-medium text-[var(--color-text-secondary)]',
            'hover:text-[var(--color-interactive)] transition-colors',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus-ring)]',
          ].join(' ')}
        >
          Try again
        </button>
      )}
    </div>
  );
}

// ── ToggleGroup ───────────────────────────────────────────────────────────────

interface ToggleOption<T extends string> {
  value: T;
  label: string;
  disabled?: boolean;
  disabledTooltip?: string;
}

interface ToggleGroupProps<T extends string> {
  options: ToggleOption<T>[];
  value: T;
  onChange: (value: T) => void;
  ariaLabel: string;
}

function ToggleGroup<T extends string>({
  options, value, onChange, ariaLabel,
}: ToggleGroupProps<T>) {
  return (
    <div
      role="radiogroup"
      aria-label={ariaLabel}
      className="inline-flex rounded-md overflow-hidden border border-[var(--color-border-default)]"
    >
      {options.map((opt) => {
        const isActive = opt.value === value;
        const isDisabled = opt.disabled === true;
        return (
          <button
            key={opt.value}
            role="radio"
            aria-checked={isActive}
            aria-disabled={isDisabled}
            title={isDisabled ? opt.disabledTooltip : undefined}
            disabled={isDisabled}
            onClick={() => onChange(opt.value)}
            className={[
              'px-3 py-1.5 text-xs font-medium transition-colors',
              'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--color-focus-ring)]',
              isActive
                ? 'bg-[var(--color-interactive)] text-[var(--color-text-inverse)]'
                : isDisabled
                  ? 'bg-[var(--color-surface-secondary)] text-[var(--color-text-disabled)] cursor-not-allowed'
                  : 'bg-[var(--color-surface-primary)] text-[var(--color-text-secondary)] hover:bg-[var(--color-surface-secondary)] hover:text-[var(--color-text-primary)]',
            ].join(' ')}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}

// ── Feature group toggle buttons ──────────────────────────────────────────────
//
// ADR-013: "A row of toggle buttons grouped by nova spectral type / phase:
// Fe II, He / N, Nebular."
//
// These are independent on/off toggles (not a radio group — multiple can be
// active at once). Each button uses the feature group's color as its active
// background so the user can associate lines on the plot with their toggle.

interface FeatureTogglesProps {
  activeGroups: Set<FeatureGroup>;
  onToggle: (group: FeatureGroup) => void;
}

function FeatureToggles({ activeGroups, onToggle }: FeatureTogglesProps) {
  const groups: FeatureGroup[] = ['fe2', 'hen', 'nebular'];

  return (
    <div className="flex items-center gap-2">
      <span className="text-xs font-medium text-[var(--color-text-tertiary)]">Lines</span>
      <div className="inline-flex rounded-md overflow-hidden border border-[var(--color-border-default)]">
        {groups.map((group) => {
          const isActive = activeGroups.has(group);
          return (
            <button
              key={group}
              aria-pressed={isActive}
              onClick={() => onToggle(group)}
              className={[
                'px-3 py-1.5 text-xs font-medium transition-colors',
                'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--color-focus-ring)]',
                isActive
                  ? 'text-white'
                  : 'bg-[var(--color-surface-primary)] text-[var(--color-text-secondary)] hover:bg-[var(--color-surface-secondary)] hover:text-[var(--color-text-primary)]',
              ].join(' ')}
              style={isActive ? { backgroundColor: FEATURE_GROUP_COLORS[group] } : undefined}
            >
              {FEATURE_GROUP_LABELS[group]}
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ── Legend strip ───────────────────────────────────────────────────────────────

interface LegendStripProps {
  spectra: PreparedSpectrum[];
  selectedId: string | null;
  onSelect: (spectrumId: string | null) => void;
}

function LegendStrip({ spectra, selectedId, onSelect }: LegendStripProps) {
  return (
    <div
      className={[
        'flex flex-wrap gap-1.5 px-4 py-3',
        'border-t border-[var(--color-border-subtle)]',
        'bg-[var(--color-surface-secondary)]',
      ].join(' ')}
      role="listbox"
      aria-label="Spectrum selection"
    >
      {spectra.map((ps) => {
        const isSelected = ps.record.spectrum_id === selectedId;
        const isDimmed = selectedId !== null && !isSelected;
        return (
          <button
            key={ps.record.spectrum_id}
            role="option"
            aria-selected={isSelected}
            onClick={() => onSelect(isSelected ? null : ps.record.spectrum_id)}
            className={[
              'inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full',
              'text-xs font-medium transition-all',
              'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus-ring)]',
              isSelected
                ? 'ring-1 ring-[var(--color-border-strong)] bg-[var(--color-surface-tertiary)] text-[var(--color-text-primary)]'
                : isDimmed
                  ? 'text-[var(--color-text-disabled)] opacity-60 hover:opacity-100'
                  : 'text-[var(--color-text-secondary)] hover:bg-[var(--color-surface-tertiary)] hover:text-[var(--color-text-primary)]',
            ].join(' ')}
          >
            <span
              className="w-2.5 h-2.5 rounded-full shrink-0"
              style={{ backgroundColor: ps.color, opacity: isDimmed ? 0.3 : 1 }}
              aria-hidden="true"
            />
            {ps.epochLabel}
          </button>
        );
      })}
    </div>
  );
}

// ── Spectra regime tab bar (ADR-034) ─────────────────────────────────────────

interface SpectraRegimeTabBarProps {
  regimes: SpectraRegimeRecord[];
  activeRegimeId: string;
  onSelect: (regimeId: string) => void;
}

function SpectraRegimeTabBar({ regimes, activeRegimeId, onSelect }: SpectraRegimeTabBarProps) {
  return (
    <div
      role="tablist"
      aria-label="Wavelength regime"
      className="flex gap-1 border-b border-[var(--color-border-subtle)]"
    >
      {regimes.map((regime) => {
        const isActive = regime.id === activeRegimeId;
        return (
          <button
            key={regime.id}
            role="tab"
            aria-selected={isActive}
            aria-controls={`tabpanel-${regime.id}`}
            onClick={() => onSelect(regime.id)}
            className={[
              'px-4 py-2 text-sm font-medium transition-colors',
              'focus-visible:outline-none focus-visible:ring-2',
              'focus-visible:ring-inset focus-visible:ring-[var(--color-focus-ring)]',
              'rounded-t-md',
              isActive
                ? 'text-[var(--color-interactive)] shadow-[inset_0_-2px_0_var(--color-interactive)]'
                : 'text-[var(--color-text-secondary)] hover:text-[var(--color-interactive-hover)] hover:bg-[var(--color-interactive-subtle)]',
            ].join(' ')}
          >
            {regime.label}
          </button>
        );
      })}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function SpectraViewer({ data, onRetry }: SpectraViewerProps) {
  const [renderError, setRenderError] = useState(false);
  const hasDpo = data.outburst_mjd !== null;

  const [epochFormat, setEpochFormat] = useState<EpochFormat>(hasDpo ? 'dpo' : 'mjd');
  const [selectedSpectrumId, setSelectedSpectrumId] = useState<string | null>(null);
  const [logFluxY, setLogFluxY] = useState(false);
  const [fluxScale, setFluxScale] = useState<FluxScale>('sqrt');

  // ── Regime state (ADR-034) ──────────────────────────────────────────────
  const regimes = data.regimes ?? [];
  const showRegimeTabs = regimes.length > 1;

  const REGIME_ORDER: Record<string, number> = { xray: 0, uv: 1, optical: 2, nir: 3, mir: 4 };
  const defaultRegimeId = useMemo(() => {
    if (regimes.length === 0) return 'optical';
    const counts = new Map<string, number>();
    for (const sp of data.spectra) {
      const r = sp.regime ?? 'optical';
      counts.set(r, (counts.get(r) ?? 0) + 1);
    }
    return [...counts.entries()]
      .sort((a, b) => b[1] - a[1] || (REGIME_ORDER[a[0]] ?? 99) - (REGIME_ORDER[b[0]] ?? 99))
      .at(0)?.[0] ?? 'optical';
  }, [data.spectra, regimes]);

  const [activeRegimeId, setActiveRegimeId] = useState<string>(defaultRegimeId);

  const resolvedRegimeId = regimes.length > 0 && regimes.some(r => r.id === activeRegimeId)
    ? activeRegimeId
    : regimes[0]?.id ?? 'optical';

  const regimeSpectra = useMemo(
    () => data.spectra.filter(sp => (sp.regime ?? 'optical') === resolvedRegimeId),
    [data.spectra, resolvedRegimeId],
  );

  // ── Feature marker state ──────────────────────────────────────────────────
  //
  // A Set of active feature groups. Multiple can be active simultaneously
  // (they're independent toggles, not radio buttons). We use a Set because
  // it makes the toggle logic clean: has() / add() / delete().
  //
  // Why useState with Set?
  //   React doesn't detect mutations to objects — you must create a new
  //   reference to trigger a re-render. So the toggle handler creates a
  //   new Set each time, which React sees as a changed value.

  const [activeFeatureGroups, setActiveFeatureGroups] = useState<Set<FeatureGroup>>(
    new Set()
  );

  // ── User zoom state ──────────────────────────────────────────────────────
  // null = not zoomed (use default computed range)
  const [userXRange, setUserXRange] = useState<[number, number] | null>(null);
  const [userYRange, setUserYRange] = useState<[number, number] | null>(null);

  // Revision counter — incremented on every explicit range reset.
  // Fed into layout.uirevision so Plotly drops cached zoom/pan state
  // and re-applies our computed default ranges from scratch.
  const [plotRevision, setPlotRevision] = useState(0);

  const handleRelayout = useCallback((update: Record<string, unknown>) => {
    if ('xaxis.autorange' in update || 'yaxis.autorange' in update) {
      setUserXRange(null);
      setUserYRange(null);
      setPlotRevision(r => r + 1);
      return;
    }
    const x0 = update['xaxis.range[0]'] as number | undefined;
    const x1 = update['xaxis.range[1]'] as number | undefined;
    const y0 = update['yaxis.range[0]'] as number | undefined;
    const y1 = update['yaxis.range[1]'] as number | undefined;
    if (x0 !== undefined && x1 !== undefined) {
      setUserXRange([x0, x1]);
    }
    if (y0 !== undefined && y1 !== undefined) {
      setUserYRange([y0, y1]);
    }
  }, []);

  const handleFeatureToggle = (group: FeatureGroup) => {
    setActiveFeatureGroups((prev) => {
      const next = new Set(prev);
      if (next.has(group)) {
        next.delete(group);
      } else {
        next.add(group);
      }
      return next;
    });
  };

  const handleRegimeSwitch = useCallback((regimeId: string) => {
    setActiveRegimeId(regimeId);
    setSelectedSpectrumId(null);
    setUserXRange(null);
    setUserYRange(null);
    setPlotRevision(r => r + 1);
    if (regimeId !== 'optical') {
      setActiveFeatureGroups(new Set());
    }
  }, []);

  const isSingleMode = selectedSpectrumId !== null;

  if (data.spectra.length === 0) {
    return <EmptyState />;
  }

  // eslint-disable-next-line react-hooks/exhaustive-deps
  const plotData = useMemo(() => {
    try {
      return buildPlotData(
        data, regimeSpectra, epochFormat,
        selectedSpectrumId, logFluxY, fluxScale, activeFeatureGroups,
        userXRange, userYRange,
      );
    } catch {
      setRenderError(true);
      return null;
    }
  }, [data, regimeSpectra, epochFormat, selectedSpectrumId, logFluxY, fluxScale, activeFeatureGroups, userXRange, userYRange]);

  if (renderError || plotData === null) {
    return <ErrorState onRetry={onRetry} />;
  }

  const { traces, layout: plotLayout, config, preparedSpectra } = plotData;

  // Plotly caches zoom/pan state internally and may restore stale ranges
  // after React re-renders (e.g., toggling feature markers after a reset,
  // or clicking 'reset axes' in single-spectrum mode).  Setting uirevision
  // to a counter that increments on every explicit reset forces Plotly to
  // drop its cached UI state and re-apply our layout ranges.
  const layout = { ...plotLayout, uirevision: plotRevision };

  return (
    <div className="rounded-md border border-[var(--color-border-subtle)] overflow-hidden bg-[var(--color-surface-primary)]">

      {/* ── Regime tabs (ADR-034) ──────────────────────────────────────── */}
      {showRegimeTabs && (
        <SpectraRegimeTabBar
          regimes={regimes}
          activeRegimeId={resolvedRegimeId}
          onSelect={handleRegimeSwitch}
        />
      )}

      {/* ── Viewer header ──────────────────────────────────────────────── */}
      <div
        className={[
          'flex flex-wrap items-center gap-4 px-4 py-2.5',
          'border-b border-[var(--color-border-subtle)]',
          'bg-[var(--color-surface-secondary)]',
        ].join(' ')}
      >
        {/* Epoch format */}
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-[var(--color-text-tertiary)]">Epoch</span>
          <ToggleGroup<EpochFormat>
            ariaLabel="Epoch label format"
            value={epochFormat}
            onChange={(v) => { setEpochFormat(v); setUserXRange(null); setUserYRange(null); setPlotRevision(r => r + 1); }}
            options={[
              {
                value: 'dpo', label: 'DPO',
                disabled: !hasDpo,
                disabledTooltip: 'Days Post-Outburst unavailable: outburst date not resolved',
              },
              { value: 'mjd', label: 'MJD' },
              { value: 'calendar', label: 'Calendar' },
            ]}
          />
        </div>

        {/* Log Y toggle — single-spectrum mode only */}
        {isSingleMode && (
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-[var(--color-text-tertiary)]">Y Axis</span>
            <ToggleGroup<'linear' | 'log'>
              ariaLabel="Flux Y axis scale"
              value={logFluxY ? 'log' : 'linear'}
              onChange={(v) => setLogFluxY(v === 'log')}
              options={[
                { value: 'linear', label: 'Linear' },
                { value: 'log', label: 'Log' },
              ]}
            />
          </div>
        )}

        {/* Flux scale toggle — waterfall mode only */}
        {!isSingleMode && (
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-[var(--color-text-tertiary)]">Flux Scale</span>
            <ToggleGroup<FluxScale>
              ariaLabel="Flux scale"
              value={fluxScale}
              onChange={setFluxScale}
              options={[
                { value: 'sqrt', label: '√f' },
                { value: 'linear', label: 'Linear' },
              ]}
            />
          </div>
        )}

        {/* ── Feature marker toggles (optical only) ──────────────────── */}
        {resolvedRegimeId === 'optical' && (
          <FeatureToggles
            activeGroups={activeFeatureGroups}
            onToggle={handleFeatureToggle}
          />
        )}
      </div>

      {/* ── Merge info note ──────────────────────────────────────────── */}
      {data.total_data_products != null && data.total_data_products > data.spectra.length && (
        <p className="px-4 pt-2 text-xs text-[var(--color-text-tertiary)] italic">
          {data.spectra.length} spectra from {data.total_data_products} files — same-night, same-instrument observations are merged for display.
        </p>
      )}

      {/* ── Plot ──────────────────────────────────────────────────────── */}
      <Plot
        data={traces}
        layout={layout}
        config={config}
        onRelayout={handleRelayout}
        useResizeHandler
        style={{ width: '100%', height: 480 }}
      />

      {/* ── Legend strip ───────────────────────────────────────────────── */}
      <LegendStrip
        spectra={preparedSpectra}
        selectedId={selectedSpectrumId}
        onSelect={(id) => { setSelectedSpectrumId(id); setUserXRange(null); setUserYRange(null); setPlotRevision(r => r + 1); }}
      />
    </div>
  );
}

// ── Plot data builder ─────────────────────────────────────────────────────────

function buildPlotData(
  data: SpectraArtifact,
  spectra: SpectrumRecord[],
  epochFormat: EpochFormat,
  selectedSpectrumId: string | null,
  logFluxY: boolean,
  fluxScale: FluxScale,
  activeFeatureGroups: Set<FeatureGroup>,
  userXRange: [number, number] | null,
  userYRange: [number, number] | null,
) {
  const { outburst_mjd } = data;
  const hasDpo = outburst_mjd !== null;
  const sorted = [...spectra].sort((a, b) => a.epoch_mjd - b.epoch_mjd);

  const effectiveFormat: EpochFormat =
    epochFormat === 'dpo' && !hasDpo ? 'mjd' : epochFormat;
  const epochKey = (s: SpectrumRecord) => getEpochValue(s, effectiveFormat);

  const isDense = sorted.length > 8;
  const displaySpectra = isDense
    ? selectRepresentativeSubset(sorted, epochKey, 10)
    : sorted;

  const isSingleMode = selectedSpectrumId !== null;

  const preparedSpectra: PreparedSpectrum[] = displaySpectra.map((rec, idx) => ({
    record: rec,
    color: assignColor(idx, displaySpectra.length, isDense),
    epochLabel: formatEpochLabel(rec, effectiveFormat),
    displayIndex: idx,
  }));

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const traces: any[] = [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const allAnnotations: any[] = [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const shapes: any[] = [];

  let globalWlMin = Infinity;
  let globalWlMax = -Infinity;

  // ── Compute wavelength range first (needed for feature marker filtering) ─
  for (const ps of preparedSpectra) {
    globalWlMin = Math.min(globalWlMin, ps.record.wavelength_min);
    globalWlMax = Math.max(globalWlMax, ps.record.wavelength_max);
  }

  if (isSingleMode) {
    // ── Single-spectrum mode ──────────────────────────────────────────
    const selectedPs = preparedSpectra.find(
      (ps) => ps.record.spectrum_id === selectedSpectrumId
    );

    if (selectedPs) {
      const spectrum = selectedPs.record;
      const customdata = spectrum.wavelengths.map((_, i) => [
        spectrum.flux_normalized[i].toFixed(3),
        selectedPs.epochLabel,
        spectrum.instrument,
      ]);

      traces.push({
        x: spectrum.wavelengths,
        y: spectrum.flux_normalized,
        type: 'scatter' as const,
        mode: 'lines' as const,
        name: selectedPs.epochLabel,
        line: { color: selectedPs.color, width: 1.5 },
        hovertemplate:
          '<b>%{x:.1f} nm</b><br>' +
          'Flux: %{customdata[0]}<br>' +
          'Epoch: %{customdata[1]}<br>' +
          'Instrument: %{customdata[2]}' +
          '<extra></extra>',
        customdata,
        showlegend: false,
      });
    }

    const selectedFlux = selectedPs?.record.flux_normalized ?? [0, 1];
    const fluxMin = Math.min(...selectedFlux);
    const fluxMax = Math.max(...selectedFlux);
    const fluxPadding = (fluxMax - fluxMin) * 0.08;
    const selectedWlMin = selectedPs?.record.wavelength_min ?? globalWlMin;
    const selectedWlMax = selectedPs?.record.wavelength_max ?? globalWlMax;
    const singleWlPadding = (selectedWlMax - selectedWlMin) * 0.02;

    // ── Feature markers (single-spectrum mode) ──────────────────────
    // In log mode the hover trace y-values must be positive (Plotly
    // drops non-positive points on a log axis).  Match the visible
    // axis range so the invisible hover line spans the full plot.
    const defaultYMin = logFluxY
      ? Math.max(fluxMin, 0.001)
      : fluxMin - fluxPadding;
    const defaultYMax = logFluxY
      ? fluxMax * 1.1
      : fluxMax + fluxPadding;
    const effectiveWlMin = userXRange ? userXRange[0] : selectedWlMin;
    const effectiveWlMax = userXRange ? userXRange[1] : selectedWlMax;
    const effectiveYMin = userYRange ? userYRange[0] : defaultYMin;
    const effectiveYMax = userYRange ? userYRange[1] : defaultYMax;
    addFeatureMarkers(
      activeFeatureGroups, traces, shapes,
      effectiveWlMin, effectiveWlMax,
      effectiveYMin, effectiveYMax,
    );

    const defaultXRange: [number, number] = [selectedWlMin - singleWlPadding, selectedWlMax + singleWlPadding];
    const defaultYRange: [number, number] = logFluxY
      ? [Math.log10(Math.max(fluxMin, 0.001)), Math.log10(fluxMax * 1.1)]
      : [fluxMin - fluxPadding, fluxMax + fluxPadding];

    const layout = {
      xaxis: {
        title: { text: 'Wavelength (nm)', font: { size: 12, color: 'var(--color-text-secondary)', family: 'DM Sans, sans-serif' } },
        range: userXRange ?? defaultXRange,
        autorange: !userXRange,
        gridcolor: 'var(--color-border-subtle)',
        zerolinecolor: 'var(--color-border-subtle)',
        tickfont: { size: 10, color: 'var(--color-text-tertiary)', family: 'DM Mono, monospace' },
      },
      yaxis: {
        title: { text: 'Normalized Flux', font: { size: 12, color: 'var(--color-text-secondary)', family: 'DM Sans, sans-serif' } },
        showticklabels: true,
        type: logFluxY ? 'log' as const : 'linear' as const,
        gridcolor: 'var(--color-border-subtle)',
        zerolinecolor: 'var(--color-border-subtle)',
        tickfont: { size: 10, color: 'var(--color-text-tertiary)', family: 'DM Mono, monospace' },
        range: userYRange ?? defaultYRange,
        autorange: !userYRange,
      },
      annotations: allAnnotations,
      shapes,
      margin: { l: 60, r: 20, t: 30, b: 50 },
      plot_bgcolor: 'transparent',
      paper_bgcolor: 'transparent',
      hovermode: 'closest' as const,
      showlegend: false,
    };

    const config = {
      displayModeBar: 'hover' as const,
      responsive: true,
      // Use autoScale2d (live autorange) instead of resetScale2d (cached
      // _initialRange) — Plotly's _initialRange can retain stale waterfall
      // y-ranges after transitioning to single-spectrum mode, causing the
      // "Reset axes" button to shrink the spectrum to ~10% of the y-axis.
      modeBarButtonsToRemove: ['select2d', 'lasso2d', 'resetScale2d', 'toImage'] as const,
      displaylogo: false,
    };

    return { traces, layout, config, preparedSpectra };
  }

  // ── Waterfall mode — collision-aware packing ────────────────────────────

  const compressed = fluxScale === 'sqrt';
  const { baselines, globalScale, yMin: waterfallYMin, yMax: waterfallYMax } =
    computeWaterfallPacking(preparedSpectra, compressed, globalWlMin, globalWlMax);

  preparedSpectra.forEach((ps) => {
    const spectrum = ps.record;
    const baseline = baselines[ps.displayIndex];
    const yValues = spectrum.wavelengths.map((_, j) => {
      const f = compressed
        ? Math.sqrt(Math.max(spectrum.flux_normalized[j], 0))
        : spectrum.flux_normalized[j];
      return (baseline + f) * globalScale;
    });

    // Hover shows original (uncompressed) flux
    const customdata = spectrum.wavelengths.map((_, i) => [
      spectrum.flux_normalized[i].toFixed(3),
      ps.epochLabel,
      spectrum.instrument,
    ]);

    traces.push({
      x: spectrum.wavelengths,
      y: yValues,
      type: 'scatter' as const,
      mode: 'lines' as const,
      name: ps.epochLabel,
      line: { color: ps.color, width: 1.2 },
      hovertemplate:
        '<b>%{x:.1f} nm</b><br>' +
        'Flux: %{customdata[0]}<br>' +
        'Epoch: %{customdata[1]}<br>' +
        'Instrument: %{customdata[2]}' +
        '<extra></extra>',
      customdata,
      showlegend: false,
    });

    const displayFluxValues = spectrum.flux_normalized
      .map(f => compressed ? Math.sqrt(Math.max(f, 0)) : f)
      .filter(f => !isNaN(f) && isFinite(f));
    const sortedFlux = [...displayFluxValues].sort((a, b) => a - b);
    const medianFlux = sortedFlux[Math.floor(sortedFlux.length / 2)] ?? 0;

    allAnnotations.push({
      x: 1.0, y: (baseline + medianFlux) * globalScale,
      text: ps.epochLabel,
      xanchor: 'left', yanchor: 'middle',
      showarrow: false,
      font: { size: 10, color: 'var(--color-text-secondary)', family: 'DM Mono, monospace' },
      xref: 'paper', yref: 'y',
    });
  });

  // ── Feature markers (waterfall mode) ──────────────────────────────────
  const wfEffectiveWlMin = userXRange ? userXRange[0] : globalWlMin;
  const wfEffectiveWlMax = userXRange ? userXRange[1] : globalWlMax;
  const wfEffectiveYMin = userYRange ? userYRange[0] : waterfallYMin;
  const wfEffectiveYMax = userYRange ? userYRange[1] : waterfallYMax;
  addFeatureMarkers(
    activeFeatureGroups, traces, shapes,
    wfEffectiveWlMin, wfEffectiveWlMax,
    wfEffectiveYMin, wfEffectiveYMax,
  );

  const wlPadding = (globalWlMax - globalWlMin) * 0.02;
  const rightMargin = effectiveFormat === 'calendar' ? 110 : 80;
  const defaultWfXRange: [number, number] = [globalWlMin - wlPadding, globalWlMax + wlPadding];
  const defaultWfYRange: [number, number] = [waterfallYMin, waterfallYMax];

  const layout = {
    xaxis: {
      title: { text: 'Wavelength (nm)', font: { size: 12, color: 'var(--color-text-secondary)', family: 'DM Sans, sans-serif' } },
      range: userXRange ?? defaultWfXRange,
      gridcolor: 'var(--color-border-subtle)',
      zerolinecolor: 'var(--color-border-subtle)',
      tickfont: { size: 10, color: 'var(--color-text-tertiary)', family: 'DM Mono, monospace' },
    },
    yaxis: {
      showticklabels: false,
      gridcolor: 'var(--color-border-subtle)',
      zerolinecolor: 'var(--color-border-subtle)',
      range: userYRange ?? defaultWfYRange,
    },
    annotations: allAnnotations,
    shapes,
    margin: { l: 40, r: rightMargin, t: 20, b: 50 },
    plot_bgcolor: 'transparent',
    paper_bgcolor: 'transparent',
    hovermode: 'closest' as const,
    showlegend: false,
  };

  const config = {
    displayModeBar: 'hover' as const,
    responsive: true,
    modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d', 'toImage'] as const,
    displaylogo: false,
  };

  return { traces, layout, config, preparedSpectra };
}

// ── Feature marker builder ────────────────────────────────────────────────────
//
// ADR-013: "When a group is toggled on, full-height vertical dashed lines are
// drawn at each line's wavelength, colored by group."
//
// We use Plotly "shapes" for the visible vertical dashed lines and invisible
// scatter traces for hover tooltips (shapes don't natively support hover).
// Labels appear only on hover to avoid clutter when lines are close together.
//
// Lines whose wavelength falls outside the displayed wavelength range are
// filtered out — no point drawing markers for invisible regions.

function addFeatureMarkers(
  activeGroups: Set<FeatureGroup>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  traces: any[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  shapes: any[],
  wlMin: number,
  wlMax: number,
  yMin: number,
  yMax: number,
) {
  if (activeGroups.size === 0) return;

  // Filter to active groups (plus universal lines when any group is active)
  // and visible wavelength range
  const activeLines = SPECTRAL_LINES.filter(
    (line) =>
      (activeGroups.has(line.group) || (line.universal && activeGroups.size > 0)) &&
      line.wavelength_nm >= wlMin &&
      line.wavelength_nm <= wlMax
  );

  for (const line of activeLines) {
    const color = FEATURE_GROUP_COLORS[line.group];
    const hoverLabel = `${line.label} — ${line.wavelength_nm.toFixed(1)} nm`;

    // ── Vertical dashed line (Plotly shape) ───────────────────────────
    shapes.push({
      type: 'line',
      x0: line.wavelength_nm,
      x1: line.wavelength_nm,
      y0: 0,
      y1: 1,
      xref: 'x',
      yref: 'paper',
      line: {
        color,
        width: 1,
        dash: 'dash',
      },
      opacity: 0.6,
    });

    // ── Invisible hover trace ────────────────────────────────────────
    // With hovermode:'closest', Plotly measures distance to the nearest
    // data *point*, not interpolated positions along a line segment.
    // Two points at yMin/yMax would only trigger near the extremes, so
    // we densify to ~20 points so there is always a nearby hover target.
    const N_HOVER_POINTS = 20;
    const hoverY = Array.from(
      { length: N_HOVER_POINTS },
      (_, i) => yMin + (yMax - yMin) * i / (N_HOVER_POINTS - 1),
    );
    const hoverX = Array(N_HOVER_POINTS).fill(line.wavelength_nm);
    const hoverText = Array(N_HOVER_POINTS).fill(hoverLabel);

    traces.push({
      x: hoverX,
      y: hoverY,
      type: 'scatter' as const,
      mode: 'lines' as const,
      line: { color: 'rgba(0,0,0,0)', width: 10 },
      hoverinfo: 'text' as const,
      text: hoverText,
      showlegend: false,
      hoverlabel: {
        bgcolor: color,
        font: { color: 'white', family: 'DM Mono, monospace', size: 11 },
      },
    });
  }
}
