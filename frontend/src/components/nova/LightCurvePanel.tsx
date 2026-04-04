'use client';

/**
 * LightCurvePanel — Layers A + B + C.
 *
 * A tabbed, multi-regime Plotly.js scatter plot consuming photometry.json,
 * with interactive controls for epoch format, time scale, Y-axis scale,
 * band visibility, and error bar display.
 *
 * Layer A (complete): scatter plot, band colors, offsets, upper limits, tooltips
 * Layer B (complete): regime tabs, per-regime axis config
 * Layer C (complete):
 *   - Epoch format toggle: DPO / MJD / Calendar Date
 *   - Time scale toggle: Log / Linear (hidden when MJD or Calendar active)
 *   - Y-axis scale toggle: Log / Linear (non-optical regimes only; optical
 *     magnitude is inherently logarithmic so log-of-magnitude is meaningless)
 *   - Band legend chips: clickable, colored, toggle band visibility
 *   - Error bar toggle: show/hide
 *   - Upper limits follow their band's visibility automatically
 *
 * ADR references:
 *   - ADR-013: Light curve panel spec (interactions, gap-ratio rule, per-regime axes)
 *   - ADR-014: photometry.json schema
 *   - ADR-012: Component patterns (toggles, buttons, empty/error states)
 */

import { useState, useMemo, useCallback } from 'react';
import dynamic from 'next/dynamic';
import type { PhotometryArtifact, ObservationRecord, RegimeRecord } from '@/types/photometry';
import {
  getObservationValue,
  assignBandColors,
  buildBandLookup,
  formatEpoch,
  getTimeValue,
  getTimeAxisLabel,
  shouldDefaultToLogTime,
  type EpochFormat,
} from '@/lib/photometry';

const Plot = dynamic(() => import('react-plotly.js'), { ssr: false });

// ── Types ─────────────────────────────────────────────────────────────────────

interface LightCurvePanelProps {
  data: PhotometryArtifact | null;
}

// ── Toggle button sub-component ───────────────────────────────────────────────

interface ToggleButtonProps {
  label: string;
  active: boolean;
  disabled?: boolean;
  onClick: () => void;
}

function ToggleButton({ label, active, disabled = false, onClick }: ToggleButtonProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      aria-pressed={active}
      className={[
        'px-2.5 py-1 text-xs font-medium rounded-md transition-colors',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus-ring)]',
        disabled
          ? 'text-[var(--color-text-disabled)] cursor-not-allowed'
          : active
            ? 'bg-[var(--color-interactive)] text-[var(--color-text-inverse)]'
            : [
                'text-[var(--color-text-secondary)]',
                'border border-[var(--color-border-default)]',
                'hover:border-[var(--color-interactive)] hover:text-[var(--color-interactive)]',
              ].join(' '),
      ].join(' ')}
    >
      {label}
    </button>
  );
}

// ── Regime tab bar ────────────────────────────────────────────────────────────

interface RegimeTabBarProps {
  regimes: RegimeRecord[];
  activeRegimeId: string;
  onSelect: (regimeId: string) => void;
}

function RegimeTabBar({ regimes, activeRegimeId, onSelect }: RegimeTabBarProps) {
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

// ── Band legend chip ──────────────────────────────────────────────────────────

interface BandChipProps {
  band: string;
  color: string;
  visible: boolean;
  onToggle: () => void;
}

function BandChip({ band, color, visible, onToggle }: BandChipProps) {
  return (
    <button
      onClick={onToggle}
      aria-pressed={visible}
      aria-label={`${visible ? 'Hide' : 'Show'} ${band} band`}
      className={[
        'inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full',
        'text-xs font-medium transition-all',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus-ring)]',
        visible
          ? 'text-[var(--color-text-primary)] hover:bg-[var(--color-surface-tertiary)]'
          : 'text-[var(--color-text-disabled)] opacity-50 hover:opacity-75',
      ].join(' ')}
    >
      <span
        className="w-2.5 h-2.5 rounded-full shrink-0"
        style={{ backgroundColor: color, opacity: visible ? 1 : 0.3 }}
        aria-hidden="true"
      />
      {band}
    </button>
  );
}

// ── Controls header ───────────────────────────────────────────────────────────

interface ControlsHeaderProps {
  epochFormat: EpochFormat;
  onEpochFormatChange: (fmt: EpochFormat) => void;
  hasDpo: boolean;
  timeScaleLog: boolean;
  onTimeScaleChange: (log: boolean) => void;
  showErrorBars: boolean;
  onErrorBarsChange: (show: boolean) => void;
  /** Whether the active regime is optical (hides Y-scale toggle). */
  isOptical: boolean;
  /** Whether Y-axis is currently log scale. */
  yScaleLog: boolean;
  onYScaleChange: (log: boolean) => void;
}

function ControlsHeader({
  epochFormat,
  onEpochFormatChange,
  hasDpo,
  timeScaleLog,
  onTimeScaleChange,
  showErrorBars,
  onErrorBarsChange,
  isOptical,
  yScaleLog,
  onYScaleChange,
}: ControlsHeaderProps) {
  const showLogToggle = epochFormat === 'dpo';

  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 px-1 py-2">
      {/* ── Epoch format ─────────────────────────────────────────────── */}
      <div className="flex items-center gap-1">
        <span className="text-xs text-[var(--color-text-tertiary)] mr-1">Epoch:</span>
        <ToggleButton
          label="DPO"
          active={epochFormat === 'dpo'}
          disabled={!hasDpo}
          onClick={() => onEpochFormatChange('dpo')}
        />
        <ToggleButton
          label="MJD"
          active={epochFormat === 'mjd'}
          onClick={() => onEpochFormatChange('mjd')}
        />
        <ToggleButton
          label="Date"
          active={epochFormat === 'calendar'}
          onClick={() => onEpochFormatChange('calendar')}
        />
      </div>

      {/* ── Time scale (X axis, hidden when not DPO) ─────────────────── */}
      {showLogToggle && (
        <div className="flex items-center gap-1">
          <span className="text-xs text-[var(--color-text-tertiary)] mr-1">Time:</span>
          <ToggleButton
            label="Linear"
            active={!timeScaleLog}
            onClick={() => onTimeScaleChange(false)}
          />
          <ToggleButton
            label="Log"
            active={timeScaleLog}
            onClick={() => onTimeScaleChange(true)}
          />
        </div>
      )}

      {/* ── Y-axis scale (non-optical only) ──────────────────────────── */}
      {/*
       * ADR-013 specifies "Linear or log" for X-ray and gamma-ray, and
       * "Log (default)" for radio. Optical magnitude is inherently
       * logarithmic — applying a log scale would produce double-log,
       * which is scientifically meaningless. So this toggle is hidden
       * (not disabled) for the optical regime.
       */}
      {!isOptical && (
        <div className="flex items-center gap-1">
          <span className="text-xs text-[var(--color-text-tertiary)] mr-1">Y axis:</span>
          <ToggleButton
            label="Linear"
            active={!yScaleLog}
            onClick={() => onYScaleChange(false)}
          />
          <ToggleButton
            label="Log"
            active={yScaleLog}
            onClick={() => onYScaleChange(true)}
          />
        </div>
      )}

      {/* ── Error bars ───────────────────────────────────────────────── */}
      <div className="flex items-center gap-1">
        <span className="text-xs text-[var(--color-text-tertiary)] mr-1">Errors:</span>
        <ToggleButton
          label={showErrorBars ? 'Shown' : 'Hidden'}
          active={showErrorBars}
          onClick={() => onErrorBarsChange(!showErrorBars)}
        />
      </div>
    </div>
  );
}

// ── Outer component ───────────────────────────────────────────────────────────

export default function LightCurvePanel({ data }: LightCurvePanelProps) {
  // ── Regime tab state ──────────────────────────────────────────────────────
  const [activeRegimeId, setActiveRegimeId] = useState<string>(
    () => data?.regimes[0]?.id ?? ''
  );

  const hasDpo = data?.outburst_mjd != null;

  // ── Epoch format state ────────────────────────────────────────────────────
  const [epochFormat, setEpochFormat] = useState<EpochFormat>(
    () => hasDpo ? 'dpo' : 'mjd'
  );

  // ── Time scale state (X axis) ─────────────────────────────────────────────
  const defaultLogForRegime = useMemo(() => {
    if (!data) return false;
    const regimeObs = data.observations.filter((o) => o.regime === activeRegimeId);
    const times = regimeObs
      .filter((o) => !o.is_upper_limit)
      .map((o) => {
        if (epochFormat === 'dpo' && o.days_since_outburst != null) {
          return o.days_since_outburst;
        }
        return o.epoch_mjd;
      })
      .sort((a, b) => a - b);
    return shouldDefaultToLogTime(times);
  }, [data, activeRegimeId, epochFormat]);

  const [timeScaleLog, setTimeScaleLog] = useState<boolean>(defaultLogForRegime);

  // ── Y-axis scale state (non-optical regimes only) ─────────────────────────
  /**
   * ADR-013 allows linear or log for X-ray, gamma, and radio Y axes.
   * Optical is always linear (magnitude is inherently logarithmic).
   * Initialised from the regime's y_axis_scale_default so radio starts
   * with log active and X-ray starts with linear.
   */
  const [yScaleLog, setYScaleLog] = useState<boolean>(
    () => data?.regimes[0]?.y_axis_scale_default === 'log'
  );

  // ── Band visibility state ─────────────────────────────────────────────────
  const currentRegimeBands = useMemo(() => {
    if (!data) return [] as string[];
    const regime = data.regimes.find((r) => r.id === activeRegimeId);
    return regime?.bands ?? [];
  }, [data, activeRegimeId]);

  const [visibleBands, setVisibleBands] = useState<Set<string>>(
    () => new Set(currentRegimeBands)
  );

  // ── Error bar state ───────────────────────────────────────────────────────
  const [showErrorBars, setShowErrorBars] = useState(true);

  // ── Regime switch handler ─────────────────────────────────────────────────
  const handleRegimeSwitch = useCallback((regimeId: string) => {
    setActiveRegimeId(regimeId);
    if (data) {
      const newRegime = data.regimes.find((r) => r.id === regimeId);
      if (newRegime) {
        setVisibleBands(new Set(newRegime.bands));
        // Reset Y-axis scale to the new regime's default
        setYScaleLog(newRegime.y_axis_scale_default === 'log');
      }
      const regimeObs = data.observations.filter((o) => o.regime === regimeId);
      const times = regimeObs
        .filter((o) => !o.is_upper_limit)
        .map((o) => {
          if (epochFormat === 'dpo' && o.days_since_outburst != null) {
            return o.days_since_outburst;
          }
          return o.epoch_mjd;
        })
        .sort((a, b) => a - b);
      setTimeScaleLog(shouldDefaultToLogTime(times));
    }
  }, [data, epochFormat]);

  // ── Band toggle handler ───────────────────────────────────────────────────
  const handleBandToggle = useCallback((bandId: string) => {
    setVisibleBands((prev) => {
      const next = new Set(prev);
      if (next.has(bandId)) {
        next.delete(bandId);
      } else {
        next.add(bandId);
      }
      return next;
    });
  }, []);

  // ── Epoch format change handler ───────────────────────────────────────────
  const handleEpochFormatChange = useCallback((fmt: EpochFormat) => {
    setEpochFormat(fmt);
    if (fmt !== 'dpo') {
      setTimeScaleLog(false);
    }
  }, []);

  // ── Guard: no data ────────────────────────────────────────────────────────
  if (!data || data.observations.length === 0) {
    return (
      <div
        className="flex items-center justify-center py-16 text-sm text-[var(--color-text-tertiary)]"
        aria-label="No photometry data available"
      >
        No photometry data available.
      </div>
    );
  }

  const showTabs = data.regimes.length > 1;
  const resolvedRegimeId = data.regimes.some((r) => r.id === activeRegimeId)
    ? activeRegimeId
    : data.regimes[0].id;

  const isOptical = resolvedRegimeId === 'optical';
  const bandColors = assignBandColors(currentRegimeBands);

  return (
    <div className="flex flex-col">
      {/* ── Regime tabs ────────────────────────────────────────────────── */}
      {showTabs && (
        <RegimeTabBar
          regimes={data.regimes}
          activeRegimeId={resolvedRegimeId}
          onSelect={handleRegimeSwitch}
        />
      )}

      {/* ── Controls header ────────────────────────────────────────────── */}
      <ControlsHeader
        epochFormat={epochFormat}
        onEpochFormatChange={handleEpochFormatChange}
        hasDpo={hasDpo}
        timeScaleLog={timeScaleLog}
        onTimeScaleChange={setTimeScaleLog}
        showErrorBars={showErrorBars}
        onErrorBarsChange={setShowErrorBars}
        isOptical={isOptical}
        yScaleLog={yScaleLog}
        onYScaleChange={setYScaleLog}
      />

      {/* ── Plot ───────────────────────────────────────────────────────── */}
      <div
        role="tabpanel"
        id={`tabpanel-${resolvedRegimeId}`}
        aria-label={`${data.regimes.find((r) => r.id === resolvedRegimeId)?.label ?? ''} light curve`}
      >
        <LightCurvePlot
          data={data}
          regimeId={resolvedRegimeId}
          epochFormat={epochFormat}
          timeScaleLog={timeScaleLog}
          visibleBands={visibleBands}
          showErrorBars={showErrorBars}
          yScaleLog={yScaleLog}
        />
      </div>

      {/* ── Band legend strip ──────────────────────────────────────────── */}
      <div className="flex flex-wrap gap-1 px-1 py-2">
        {currentRegimeBands.map((bandId) => (
          <BandChip
            key={bandId}
            band={bandId}
            color={bandColors.get(bandId) ?? '#78726A'}
            visible={visibleBands.has(bandId)}
            onToggle={() => handleBandToggle(bandId)}
          />
        ))}
      </div>
    </div>
  );
}

// ── Inner plot component ──────────────────────────────────────────────────────

interface LightCurvePlotProps {
  data: PhotometryArtifact;
  regimeId: string;
  epochFormat: EpochFormat;
  timeScaleLog: boolean;
  visibleBands: Set<string>;
  showErrorBars: boolean;
  yScaleLog: boolean;
}

function LightCurvePlot({
  data,
  regimeId,
  epochFormat,
  timeScaleLog,
  visibleBands,
  showErrorBars,
  yScaleLog,
}: LightCurvePlotProps) {
  const visibleBandsKey = useMemo(
    () => [...visibleBands].sort().join(','),
    [visibleBands],
  );

  const { traces, layout } = useMemo(() => {
    const regime = data.regimes.find((r) => r.id === regimeId);
    if (!regime) return { traces: [], layout: {} };

    const bandLookup = buildBandLookup(data.bands);
    const bandColors = assignBandColors(regime.bands);

    const regimeObs = data.observations.filter(
      (o) => o.regime === regimeId && visibleBands.has(o.band)
    );

    const bandGroups = new Map<
      string,
      { detections: ObservationRecord[]; upperLimits: ObservationRecord[] }
    >();

    for (const obs of regimeObs) {
      let group = bandGroups.get(obs.band);
      if (!group) {
        group = { detections: [], upperLimits: [] };
        bandGroups.set(obs.band, group);
      }
      if (obs.is_upper_limit) {
        group.upperLimits.push(obs);
      } else {
        group.detections.push(obs);
      }
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const plotTraces: any[] = [];

    for (const bandId of regime.bands) {
      if (!visibleBands.has(bandId)) continue;

      const group = bandGroups.get(bandId);
      if (!group) continue;

      const color = bandColors.get(bandId) ?? '#78726A';
      const bandMeta = bandLookup.get(bandId);
      const vOffset = bandMeta?.vertical_offset ?? 0;

      // ── Detections ──────────────────────────────────────────────────
      if (group.detections.length > 0) {
        const x: number[] = [];
        const y: number[] = [];
        const errorY: number[] = [];
        const hoverTexts: string[] = [];

        for (const obs of group.detections) {
          const timeVal = getTimeValue(obs, epochFormat);
          const { value, error } = getObservationValue(obs, regimeId);
          if (value == null) continue;

          x.push(timeVal);
          y.push(value + vOffset);
          errorY.push(error ?? 0);

          const timeLabel = formatEpoch(obs, epochFormat);
          const provider = obs.instrument !== 'unknown'
            ? `${obs.instrument} / ${obs.provider}`
            : obs.provider;
          const errorStr = error ? ` ± ${error.toFixed(3)}` : '';
          hoverTexts.push(
            `${timeLabel}<br>${regime.y_axis_label}: ${value.toFixed(2)}${errorStr}`
            + (vOffset !== 0 ? ` (offset ${vOffset > 0 ? '+' : ''}${vOffset.toFixed(1)})` : '')
            + `<br>Band: ${bandId}`
            + `<br>${provider}`
          );
        }

        plotTraces.push({
          x,
          y,
          type: 'scatter',
          mode: 'markers',
          name: bandId,
          marker: {
            color,
            size: 6,
            line: { color: 'rgba(255,255,255,0.8)', width: 0.5 },
          },
          error_y: {
            type: 'data',
            array: errorY,
            visible: showErrorBars,
            color,
            thickness: 1,
            width: 2,
          },
          hovertext: hoverTexts,
          hoverinfo: 'text',
          legendgroup: bandId,
          showlegend: false,
        });
      }

      // ── Upper limits ────────────────────────────────────────────────
      if (group.upperLimits.length > 0) {
        const x: number[] = [];
        const y: number[] = [];
        const hoverTexts: string[] = [];

        for (const obs of group.upperLimits) {
          const timeVal = getTimeValue(obs, epochFormat);
          const { value } = getObservationValue(obs, regimeId);
          if (value == null) continue;

          x.push(timeVal);
          y.push(value + vOffset);

          const timeLabel = formatEpoch(obs, epochFormat);
          const provider = obs.instrument !== 'unknown'
            ? `${obs.instrument} / ${obs.provider}`
            : obs.provider;
          hoverTexts.push(
            `${timeLabel}<br>${regime.y_axis_label}: ≤ ${value.toFixed(2)}`
            + (vOffset !== 0 ? ` (offset ${vOffset > 0 ? '+' : ''}${vOffset.toFixed(1)})` : '')
            + `<br>Band: ${bandId}`
            + `<br>${provider}`
          );
        }

        plotTraces.push({
          x,
          y,
          type: 'scatter',
          mode: 'markers',
          name: `${bandId} (upper limit)`,
          marker: {
            symbol: 'triangle-down',
            color,
            size: 8,
            opacity: 0.4,
            line: { color, width: 1 },
          },
          hovertext: hoverTexts,
          hoverinfo: 'text',
          legendgroup: bandId,
          showlegend: false,
        });
      }
    }

    // ── Layout ────────────────────────────────────────────────────────────
    /**
     * Y-axis scale logic:
     *   - Optical (y_axis_inverted = true): always linear. Magnitude is
     *     inherently logarithmic; log-of-magnitude is double-log and
     *     scientifically meaningless.
     *   - Non-optical: controlled by yScaleLog state, which defaults to
     *     the regime's y_axis_scale_default and can be toggled by the user.
     */
    const resolvedYType = regime.y_axis_inverted
      ? 'linear'
      : (yScaleLog ? 'log' : 'linear');

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const plotLayout: any = {
      paper_bgcolor: 'transparent',
      plot_bgcolor: 'transparent',
      margin: { l: 60, r: 20, t: 10, b: 50 },

      xaxis: {
        title: {
          text: getTimeAxisLabel(epochFormat),
          font: { family: 'DM Sans, sans-serif', size: 12, color: '#5A554F' },
        },
        type: timeScaleLog ? 'log' : 'linear',
        gridcolor: '#E2DED6',
        gridwidth: 1,
        griddash: 'dot',
        zeroline: false,
        tickfont: { family: 'DM Mono, monospace', size: 11, color: '#78726A' },
      },

      yaxis: {
        title: {
          text: regime.y_axis_label,
          font: { family: 'DM Sans, sans-serif', size: 12, color: '#5A554F' },
        },
        autorange: regime.y_axis_inverted ? 'reversed' : true,
        type: resolvedYType,
        gridcolor: '#E2DED6',
        gridwidth: 1,
        griddash: 'dot',
        zeroline: false,
        tickfont: { family: 'DM Mono, monospace', size: 11, color: '#78726A' },
      },

      showlegend: false,
      hovermode: 'closest',
      dragmode: 'zoom',
    };

    return { traces: plotTraces, layout: plotLayout };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, regimeId, epochFormat, timeScaleLog, visibleBandsKey, showErrorBars, yScaleLog]);

  return (
    <div className="w-full" aria-label="Light curve plot">
      <Plot
        data={traces}
        layout={layout}
        useResizeHandler
        style={{ width: '100%', height: '400px' }}
        config={{
          displaylogo: false,
          modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d'],
          responsive: true,
        }}
      />
    </div>
  );
}
