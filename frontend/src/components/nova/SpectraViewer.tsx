'use client';

/**
 * SpectraViewer — waterfall spectra plot for the nova page.
 *
 * Renders all validated spectra for a nova as a Plotly.js waterfall plot:
 * flux vs. wavelength, vertically offset by epoch. Oldest spectrum at the
 * bottom, most recent at the top.
 *
 * Design spec: ADR-013 (visualization design)
 * Data contract: ADR-014 (spectra artifact schema, SpectraArtifact type)
 * Visual tokens: ADR-012 (design system)
 *
 * Layer A delivers:
 *   - Basic waterfall rendering with Plotly.js
 *   - Vertical offset proportional to epoch (DPO or MJD fallback)
 *   - Okabe-Ito-derived blue → amber color ramp (sparse mode)
 *   - Maximally-distinct 10-color palette (dense mode)
 *   - Right-hand epoch labels (DPO as default)
 *   - Hover tooltip: wavelength, normalized flux, epoch, instrument
 *   - Automatic log/linear default scale selection (ADR-013 rules)
 *   - Dense mode representative subset (~8–10 spectra)
 *   - Zoom, pan, reset via Plotly built-ins
 *   - Empty state (LineChart icon + message)
 *   - Error state (CircleAlert icon + "Try again" ghost button)
 */

import { useMemo, useState } from 'react';
import dynamic from 'next/dynamic';
import { LineChart, CircleAlert } from 'lucide-react';
import type { SpectraArtifact, SpectrumRecord } from '@/types/nova';

// ── Dynamic Plotly import ─────────────────────────────────────────────────────
//
// Why dynamic()?
//   plotly.js is ~3.5 MB and accesses browser DOM APIs (window, document).
//   Next.js renders components on the server first ("SSR"), where there is no
//   DOM. `dynamic()` with `ssr: false` tells Next.js: "don't render this on the
//   server — load it only when the browser needs it." This also means the Plotly
//   JS bundle isn't included in the initial page download, improving load time.
//
//   The `loading` option shows a skeleton placeholder while Plotly downloads.

const Plot = dynamic(() => import('react-plotly.js'), {
  ssr: false,
  loading: () => <PlotSkeleton />,
});

// ── Types ─────────────────────────────────────────────────────────────────────

export interface SpectraViewerProps {
  /** The full spectra artifact, fetched by NovaPage and passed down. */
  data: SpectraArtifact;
  /** Called when the user clicks "Try again" on a render error. */
  onRetry?: () => void;
}

/** Internal representation of a spectrum with computed display properties. */
interface DisplaySpectrum {
  record: SpectrumRecord;
  /** Y-axis baseline position for this spectrum. */
  baseline: number;
  /** Color assigned from the palette. */
  color: string;
  /** Formatted epoch label for the right-hand Y axis. */
  epochLabel: string;
  /** Index into the sorted spectra array (for stable ordering). */
  sortIndex: number;
}

// ── Color palettes ────────────────────────────────────────────────────────────
//
// ADR-013: "Okabe-Ito" colorblind-safe palette requirement.
//
// Sparse mode (≤ 8 spectra): a sequential blue → amber ramp. The anchor colors
// come from the Okabe-Ito palette (#0072B2 blue and #E69F00 orange). The
// intermediate values are hand-tuned to produce a smooth perceptual gradient
// that remains distinguishable under deuteranopia and protanopia.
//
// Dense mode (> 8 spectra): the full Okabe-Ito 8-color palette extended with
// 2 additional colorblind-safe colors, assigned via golden-ratio hue stepping
// so no two temporally adjacent spectra share similar hues.

const SPARSE_RAMP = [
  '#0072B2', // Okabe-Ito blue (coolest / earliest)
  '#2E91C4', // blue → sky transition
  '#56B4E9', // Okabe-Ito sky blue
  '#5EBD98', // sky → green transition
  '#009E73', // Okabe-Ito bluish green (midpoint)
  '#8DB535', // green → yellow transition
  '#E69F00', // Okabe-Ito orange
  '#D55E00', // Okabe-Ito vermillion (warmest / latest)
];

const DENSE_PALETTE = [
  '#0072B2', // blue
  '#E69F00', // orange
  '#009E73', // bluish green
  '#CC79A7', // reddish purple
  '#56B4E9', // sky blue
  '#D55E00', // vermillion
  '#F0E442', // yellow
  '#000000', // black
  '#88CCEE', // light cyan (extended)
  '#AA4499', // purple (extended)
];

// ── Helpers: color assignment ─────────────────────────────────────────────────

/**
 * Assign a color to each spectrum from the appropriate palette.
 *
 * - Sparse mode: interpolate linearly across the blue → amber ramp based on
 *   each spectrum's position in the epoch-sorted list. The first spectrum
 *   gets the coolest color, the last gets the warmest.
 *
 * - Dense mode: use golden-ratio stepping through the distinct palette.
 *   Golden ratio (≈ 0.618) ensures maximum spread: spectrum 0 gets color 0,
 *   spectrum 1 gets color 6, spectrum 2 gets color 2, etc. This prevents
 *   temporally adjacent spectra from receiving similar hues.
 */
function assignColor(sortIndex: number, totalCount: number, isDense: boolean): string {
  if (isDense) {
    // Golden-ratio stepping through the 10-color palette.
    // Math: multiply index by golden ratio, take fractional part, scale to palette size.
    const GOLDEN_RATIO = 0.6180339887;
    const paletteIndex = Math.floor(
      ((sortIndex * GOLDEN_RATIO) % 1) * DENSE_PALETTE.length
    );
    return DENSE_PALETTE[paletteIndex];
  }

  // Sparse mode: interpolate position in the 8-color ramp.
  if (totalCount === 1) return SPARSE_RAMP[0];
  const t = sortIndex / (totalCount - 1); // 0 → 1
  const scaledIndex = t * (SPARSE_RAMP.length - 1);
  const lo = Math.floor(scaledIndex);
  const hi = Math.min(lo + 1, SPARSE_RAMP.length - 1);
  // Use the nearest ramp color (no sub-pixel interpolation — keeps colors crisp)
  return scaledIndex - lo < 0.5 ? SPARSE_RAMP[lo] : SPARSE_RAMP[hi];
}

// ── Helpers: temporal axis ────────────────────────────────────────────────────

/**
 * Determine whether the default temporal scale should be log or linear.
 * Implements the two-rule cascade from ADR-013:
 *
 * Rule 1 — gap_ratio check:
 *   gap_ratio = max_gap / total_span. If > 0.5, use log.
 *   This catches novae with a cluster of early spectra + one late spectrum.
 *
 * Rule 2 — count check (only if Rule 1 doesn't trigger):
 *   N ≤ 8 → linear. N > 8 → log.
 */
function computeDefaultScale(epochValues: number[]): 'log' | 'linear' {
  if (epochValues.length < 2) return 'linear';

  const sorted = [...epochValues].sort((a, b) => a - b);
  const totalSpan = sorted[sorted.length - 1] - sorted[0];
  if (totalSpan === 0) return 'linear';

  // Rule 1: check for temporal clustering
  let maxGap = 0;
  for (let i = 1; i < sorted.length; i++) {
    maxGap = Math.max(maxGap, sorted[i] - sorted[i - 1]);
  }
  if (maxGap / totalSpan > 0.5) return 'log';

  // Rule 2: count-based
  return epochValues.length > 8 ? 'log' : 'linear';
}

/**
 * Select a representative subset of ~8–10 spectra using log-sampling in time.
 *
 * ADR-013: "Spectra are log-sampled in time so that the displayed subset
 * represents the full temporal evolution." This means we divide the time axis
 * into log-spaced intervals and pick the spectrum closest to each interval
 * boundary. This preserves both early dense coverage and late sparse coverage.
 */
function selectRepresentativeSubset(
  spectra: SpectrumRecord[],
  epochKey: (s: SpectrumRecord) => number,
  targetCount: number = 10,
): SpectrumRecord[] {
  if (spectra.length <= targetCount) return spectra;

  const sorted = [...spectra].sort((a, b) => epochKey(a) - epochKey(b));
  const minEpoch = epochKey(sorted[0]);
  const maxEpoch = epochKey(sorted[sorted.length - 1]);

  // Create log-spaced target points between min and max epoch.
  // We add 1 to avoid log(0); this shifts all values but preserves relative spacing.
  const logMin = Math.log10(minEpoch - minEpoch + 1); // = log10(1) = 0
  const logMax = Math.log10(maxEpoch - minEpoch + 1);
  const targets: number[] = [];
  for (let i = 0; i < targetCount; i++) {
    const logVal = logMin + (i / (targetCount - 1)) * (logMax - logMin);
    targets.push(Math.pow(10, logVal) + minEpoch - 1);
  }

  // For each target, find the nearest spectrum (greedy, no duplicates).
  const selected = new Set<number>(); // indices into sorted array
  for (const target of targets) {
    let bestIdx = -1;
    let bestDist = Infinity;
    for (let i = 0; i < sorted.length; i++) {
      if (selected.has(i)) continue;
      const dist = Math.abs(epochKey(sorted[i]) - target);
      if (dist < bestDist) {
        bestDist = dist;
        bestIdx = i;
      }
    }
    if (bestIdx >= 0) selected.add(bestIdx);
  }

  // Always include first and last for full temporal coverage.
  selected.add(0);
  selected.add(sorted.length - 1);

  return [...selected].sort((a, b) => a - b).map((i) => sorted[i]);
}

/**
 * Format a DPO epoch label per ADR-013.
 *
 * - Normal case: "Day 34"
 * - Substituted outburst date: "Day 34*" (asterisk signals the substitution)
 *
 * When outburst_mjd is null (no outburst date at all), DPO labels are not
 * available and we fall back to MJD. That logic lives in the component.
 */
function formatDpoLabel(daysSinceOutburst: number): string {
  return `Day ${Math.round(daysSinceOutburst)}`;
}

/**
 * Format an MJD epoch label. Integer only, per ADR-013.
 */
function formatMjdLabel(mjd: number): string {
  return Math.round(mjd).toString();
}

// ── PlotSkeleton ──────────────────────────────────────────────────────────────
//
// Shown while Plotly.js is downloading via the dynamic import. Matches the
// approximate dimensions of the eventual plot so the page doesn't jump when
// the real plot renders (this is called "layout shift prevention").

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

// ── Empty state ───────────────────────────────────────────────────────────────
//
// ADR-012: centered icon (32px, text-tertiary) + heading (text-secondary, base,
// medium) + optional explanation (text-tertiary, sm). Uses the LineChart Lucide
// icon as specified in the ADR-012 iconography table.

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
      <LineChart
        size={32}
        className="text-[var(--color-text-tertiary)]"
        aria-hidden="true"
      />
      <div className="text-center px-4">
        <p className="text-base font-medium text-[var(--color-text-secondary)]">
          No spectra available
        </p>
        <p className="text-sm text-[var(--color-text-tertiary)] mt-1">
          No validated spectra have been ingested for this nova yet.
        </p>
      </div>
    </div>
  );
}

// ── Error state ───────────────────────────────────────────────────────────────
//
// ADR-012: inline error with CircleAlert icon in --color-status-error-fg,
// "Try again" ghost button. Scoped to the visualization region.

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
      <CircleAlert
        size={32}
        className="text-[var(--color-status-error-fg)]"
        aria-hidden="true"
      />
      <p className="text-sm font-medium text-[var(--color-text-primary)]">
        Failed to render spectra
      </p>
      <p className="text-sm text-[var(--color-text-secondary)]">
        An error occurred while building the waterfall plot.
      </p>
      {onRetry && (
        <button
          onClick={onRetry}
          className={[
            'text-sm font-medium',
            'text-[var(--color-text-secondary)]',
            'hover:text-[var(--color-interactive)]',
            'transition-colors',
            'focus-visible:outline-none focus-visible:ring-2',
            'focus-visible:ring-[var(--color-focus-ring)]',
          ].join(' ')}
        >
          Try again
        </button>
      )}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function SpectraViewer({ data, onRetry }: SpectraViewerProps) {
  // ── Render error boundary ─────────────────────────────────────────────────
  //
  // React doesn't have try/catch for render errors in function components
  // (class-based ErrorBoundary is the React way), but we can catch errors
  // during the data preparation phase using a state flag. If the useMemo
  // below throws, we catch it and show the error state.
  const [renderError, setRenderError] = useState(false);

  // ── Empty state ───────────────────────────────────────────────────────────
  if (data.spectra.length === 0) {
    return <EmptyState />;
  }

  // ── Compute all display data ──────────────────────────────────────────────
  //
  // useMemo() is a React hook that caches ("memoizes") the result of an
  // expensive computation. It only re-runs when its dependencies change
  // (here: data). Without useMemo, this calculation would re-run on every
  // render — including when the user hovers over the plot (which triggers
  // React re-renders). For a dataset with thousands of wavelength points,
  // that waste adds up.

  const plotData = useMemo(() => {
    try {
      return buildPlotData(data);
    } catch {
      setRenderError(true);
      return null;
    }
  }, [data]);

  if (renderError || plotData === null) {
    return <ErrorState onRetry={onRetry} />;
  }

  const { traces, layout, config } = plotData;

  // ── Render ────────────────────────────────────────────────────────────────
  //
  // The Plot component from react-plotly.js accepts:
  //   data   — array of trace objects (one per spectrum line)
  //   layout — axis configuration, margins, annotations
  //   config — toolbar options, responsiveness
  //
  // `useResizeHandler` tells Plotly to re-measure when the container resizes.
  // `style` sets the container to fill its parent width.

  return (
    <div className="rounded-md border border-[var(--color-border-subtle)] overflow-hidden bg-[var(--color-surface-primary)]">
      <Plot
        data={traces}
        layout={layout}
        config={config}
        useResizeHandler
        style={{ width: '100%', height: 480 }}
      />
    </div>
  );
}

// ── Plot data builder ─────────────────────────────────────────────────────────
//
// Extracted from the component so useMemo can cache it cleanly.
// This function does all the heavy lifting:
//   1. Sort spectra by epoch
//   2. Determine DPO availability and epoch values
//   3. Compute default scale (log vs linear)
//   4. Select representative subset if dense
//   5. Compute baseline positions and amplitude
//   6. Assign colors
//   7. Build Plotly trace and layout objects

function buildPlotData(data: SpectraArtifact) {
  const { spectra, outburst_mjd } = data;

  // Sort by epoch (ascending = oldest first = bottom of waterfall)
  const sorted = [...spectra].sort((a, b) => a.epoch_mjd - b.epoch_mjd);

  // ── Epoch value extraction ──────────────────────────────────────────────
  // DPO is the preferred epoch frame (ADR-013). Fall back to MJD when
  // outburst_mjd is null (no outburst date resolved).
  const hasDpo = outburst_mjd !== null;
  const epochKey = (s: SpectrumRecord): number =>
    hasDpo && s.days_since_outburst !== null
      ? s.days_since_outburst
      : s.epoch_mjd;

  const epochValues = sorted.map(epochKey);

  // ── Default scale and subset selection ──────────────────────────────────
  const defaultScale = computeDefaultScale(epochValues);
  const isDense = defaultScale === 'log' && sorted.length > 8;

  // In dense mode, show a representative subset. The full set remains
  // accessible via the spectrum selection panel (Layer C).
  const displaySpectra = isDense
    ? selectRepresentativeSubset(sorted, epochKey, 10)
    : sorted;

  // ── Baseline positions ──────────────────────────────────────────────────
  // Y-axis position is proportional to epoch. For log scale, we take
  // log10(epoch) — but we need to handle the offset so log(0) never occurs.
  //
  // For DPO: Day 0 = outburst. The earliest spectrum should be Day 1+.
  //   The backend guarantees days_since_outburst > 0 for all spectra.
  //
  // For MJD: values are large positive numbers (~50000+), so log is always safe.

  const displayEpochs = displaySpectra.map(epochKey);
  const baselines = displayEpochs.map((e) =>
    defaultScale === 'log' ? Math.log10(Math.max(e, 0.1)) : e,
  );

  // ── Amplitude calculation ───────────────────────────────────────────────
  // ADR-013: AMP = min_inter_spectrum_gap × 0.78
  //
  // Each spectrum's flux is then scaled so its peak reaches exactly AMP.
  // Since flux is median-normalized (ADR-014), peaks can be > 1.0, so we
  // divide by each spectrum's max flux value to normalize to unit height,
  // then multiply by AMP.

  let minGap = Infinity;
  const sortedBaselines = [...baselines].sort((a, b) => a - b);
  for (let i = 1; i < sortedBaselines.length; i++) {
    const gap = sortedBaselines[i] - sortedBaselines[i - 1];
    if (gap > 0) minGap = Math.min(minGap, gap);
  }
  // For a single spectrum, use a sensible default amplitude
  if (!isFinite(minGap) || minGap === 0) minGap = 1;
  const amp = minGap * 0.78;

  // ── Build Plotly traces ─────────────────────────────────────────────────
  //
  // Each spectrum becomes one Plotly "scatter" trace (rendered as a line).
  // The y-values are: baseline + (flux_normalized / max_flux) * AMP
  //
  // This means:
  //   - The line sits at y = baseline when flux = 0
  //   - The tallest peak reaches y = baseline + AMP (78% of the lane)
  //   - All spectra have comparable visual height regardless of their
  //     absolute flux scale (since backend median-normalizes each one)

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const traces: any[] = [];
  const epochAnnotations: Array<{
    x: number;
    y: number;
    text: string;
    xanchor: string;
    yanchor: string;
    showarrow: boolean;
    font: { size: number; color: string; family: string };
    xref: string;
    yref: string;
  }> = [];

  // Track the global wavelength range for the X axis
  let globalWlMin = Infinity;
  let globalWlMax = -Infinity;

  displaySpectra.forEach((spectrum, idx) => {
    const baseline = baselines[idx];
    const color = assignColor(idx, displaySpectra.length, isDense);

    // Find this spectrum's peak flux for per-spectrum amplitude scaling
    const maxFlux = Math.max(...spectrum.flux_normalized);
    const scale = maxFlux > 0 ? amp / maxFlux : amp;

    // Offset flux values by the baseline
    const yValues = spectrum.flux_normalized.map(
      (f) => baseline + f * scale,
    );

    // Update global wavelength range
    globalWlMin = Math.min(globalWlMin, spectrum.wavelength_min);
    globalWlMax = Math.max(globalWlMax, spectrum.wavelength_max);

    // Format the epoch label for this spectrum
    const epochLabel =
      hasDpo && spectrum.days_since_outburst !== null
        ? formatDpoLabel(spectrum.days_since_outburst)
        : formatMjdLabel(spectrum.epoch_mjd);

    // ── Hover template ──────────────────────────────────────────────────
    //
    // ADR-013: tooltip shows wavelength (1 decimal), normalized flux,
    // epoch (active format), and instrument.
    //
    // Plotly's `hovertemplate` uses %{x}, %{customdata[n]} placeholders.
    // We pack epoch label and instrument into `customdata` because they're
    // per-point metadata that Plotly can't derive from x/y alone.
    //
    // The <extra></extra> tag suppresses the default trace-name box that
    // Plotly adds to every tooltip.

    const customdata = spectrum.wavelengths.map((_, i) => [
      spectrum.flux_normalized[i].toFixed(3), // normalized flux
      epochLabel,
      spectrum.instrument,
    ]);

    traces.push({
      x: spectrum.wavelengths,
      y: yValues,
      type: 'scatter' as const,
      mode: 'lines' as const,
      name: epochLabel,
      line: { color, width: 1.2 },
      hovertemplate:
        '<b>%{x:.1f} nm</b><br>' +
        'Flux: %{customdata[0]}<br>' +
        'Epoch: %{customdata[1]}<br>' +
        'Instrument: %{customdata[2]}' +
        '<extra></extra>',
      customdata,
      // Don't show this trace in Plotly's built-in legend — we build our
      // own legend strip in Layer C
      showlegend: false,
    });

    // ── Epoch annotation (right-hand label) ─────────────────────────────
    //
    // ADR-013: epoch labels on the right-hand side of the plot, aligned to
    // each spectrum's baseline. We use Plotly annotations positioned at the
    // right edge of the plot area (xref: 'paper', x: 1.0).
    //
    // Why annotations instead of a second Y axis?
    //   Plotly's second Y axis would require keeping two axes in sync and
    //   doesn't support per-tick custom formatting as cleanly. Annotations
    //   give us full control over positioning and styling.

    epochAnnotations.push({
      x: 1.0,
      y: baseline,
      text: epochLabel,
      xanchor: 'left',
      yanchor: 'middle',
      showarrow: false,
      font: {
        size: 10,
        color: 'var(--color-text-secondary)',
        family: 'DM Mono, monospace',
      },
      xref: 'paper',
      yref: 'y',
    });
  });

  // ── Layout ──────────────────────────────────────────────────────────────
  //
  // Plotly layout controls axes, margins, background, and annotations.
  //
  // Key decisions:
  //   - plot_bgcolor / paper_bgcolor: transparent so the component's own
  //     bg-surface-primary shows through. This keeps us in the design system.
  //   - Right margin: 80px to make room for epoch labels (annotations).
  //   - Y axis: no title, no tick labels (the baselines have no physical
  //     meaning in waterfall mode — flux meaning is local to each lane).
  //   - X axis: wavelength in nm with a clean title.

  const wlPadding = (globalWlMax - globalWlMin) * 0.02;

  const layout = {
    xaxis: {
      title: {
        text: 'Wavelength (nm)',
        font: {
          size: 12,
          color: 'var(--color-text-secondary)',
          family: 'DM Sans, sans-serif',
        },
      },
      range: [globalWlMin - wlPadding, globalWlMax + wlPadding],
      gridcolor: 'var(--color-border-subtle)',
      zerolinecolor: 'var(--color-border-subtle)',
      tickfont: {
        size: 10,
        color: 'var(--color-text-tertiary)',
        family: 'DM Mono, monospace',
      },
    },
    yaxis: {
      // No title — the Y axis encodes epoch offset, not flux
      showticklabels: false,
      gridcolor: 'var(--color-border-subtle)',
      zerolinecolor: 'var(--color-border-subtle)',
      // Small padding above/below the waterfall
      range: [
        Math.min(...baselines) - amp * 0.5,
        Math.max(...baselines) + amp * 1.5,
      ],
    },
    annotations: epochAnnotations,
    margin: { l: 40, r: 80, t: 20, b: 50 },
    plot_bgcolor: 'transparent',
    paper_bgcolor: 'transparent',
    hovermode: 'closest' as const,
    // Prevent Plotly from adding its own title
    showlegend: false,
  };

  // ── Config ──────────────────────────────────────────────────────────────
  //
  // Plotly `config` controls the toolbar and interaction behavior.
  //   - displayModeBar: show toolbar only on hover (not permanently visible)
  //   - responsive: re-measure on container resize
  //   - modeBarButtonsToRemove: strip out buttons we don't need (lasso select,
  //     box select, download image, etc.) to keep the toolbar minimal
  //   - displaylogo: hide the Plotly logo

  const config = {
    displayModeBar: 'hover' as const,
    responsive: true,
    modeBarButtonsToRemove: [
      'select2d',
      'lasso2d',
      'autoScale2d',
      'toImage',
    ] as const,
    displaylogo: false,
  };

  return { traces, layout, config };
}
