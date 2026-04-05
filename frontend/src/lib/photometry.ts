/**
 * Photometry utility functions for the light curve panel.
 *
 * Separated from the component to keep rendering code focused and to make
 * these utilities available to future components (e.g. catalog sparklines,
 * photometry table on nova page).
 *
 * Contents:
 *   - Okabe-Ito band color palette (colorblind-safe, per ADR-013)
 *   - Regime → observation field mapping
 *   - Epoch formatting (DPO / MJD / Calendar Date)
 *   - Gap-ratio default time scale selection
 */

import type { ObservationRecord, RegimeRecord, BandRecord } from '@/types/photometry';

// ── Band color palette ────────────────────────────────────────────────────────

/**
 * Okabe-Ito colorblind-safe palette, semantically anchored to photometric bands.
 *
 * ADR-013 specifies:
 *   U/UV = violet/purple, B = blue, V = green, R/r = orange-red,
 *   I/i = deep red, other = neutral gray.
 *
 * Johnson and Sloan bands get distinct colors even when covering similar
 * wavelength ranges — they are not interchangeable (ADR-013).
 *
 * The Okabe-Ito palette (https://jfly.uni-koeln.de/color/) is the project's
 * non-negotiable colorblind accessibility requirement. These 8 colors are
 * distinguishable under deuteranopia, protanopia, and tritanopia.
 */
const BAND_COLOR_MAP: Record<string, string> = {
  // Johnson system — primary anchors
  U:  '#CC79A7', // reddish purple — UV/violet anchor
  B:  '#0072B2', // blue
  V:  '#009E73', // bluish green — ADR-013 "green"
  R:  '#D55E00', // vermilion — orange-red anchor
  I:  '#E69F00', // orange — warm, distinct from R

  // Sloan system — must be visually distinct from Johnson equivalents
  "g'": '#56B4E9', // sky blue — distinct from B
  "r'": '#F0E442', // yellow — distinct from R
  "i'": '#CC79A7', // reddish purple (reused from U, unlikely to coexist)
  "u'": '#0072B2', // blue (reused, unlikely to coexist with Johnson B)
  "z'": '#D55E00', // vermilion (reused, unlikely to coexist)

  // Common aliases — same Johnson colors
  Rc: '#D55E00',
  Ic: '#E69F00',

  // UV bands (Swift/UVOT)
  UVW1: '#CC79A7',
  UVW2: '#CC79A7',
  UVM2: '#CC79A7',
};

/**
 * Fallback Okabe-Ito cycle for bands not in the anchor map.
 * Used when unknown band labels appear. Cycling ensures every band
 * gets a distinguishable color, even if the semantic anchor is unknown.
 */
const OKABE_ITO_CYCLE = [
  '#0072B2', // blue
  '#D55E00', // vermilion
  '#009E73', // bluish green
  '#E69F00', // orange
  '#56B4E9', // sky blue
  '#CC79A7', // reddish purple
  '#F0E442', // yellow
];

/** Neutral gray from the design system for unfiltered/other. */
const FALLBACK_GRAY = '#78726A'; // --primitive-stone-500

/**
 * Get the display color for a photometric band.
 *
 * Priority:
 *   1. Semantic anchor (BAND_COLOR_MAP) — covers all common nova bands
 *   2. Fallback cycle — for unknown bands, cycles through Okabe-Ito colors
 *   3. Neutral gray — for "unfiltered", "CV", "TG", and similar
 *
 * @param band - Band identifier from the bands array
 * @param fallbackIndex - Index into the fallback cycle for unknown bands
 */
export function getBandColor(band: string, fallbackIndex: number = 0): string {
  // Check for unfiltered/visual estimate bands
  const lower = band.toLowerCase();
  if (lower === 'unfiltered' || lower === 'cv' || lower === 'tg' || lower === 'vis') {
    return FALLBACK_GRAY;
  }

  // Check semantic anchors
  if (band in BAND_COLOR_MAP) {
    return BAND_COLOR_MAP[band];
  }

  // Fallback: cycle through the palette
  return OKABE_ITO_CYCLE[fallbackIndex % OKABE_ITO_CYCLE.length];
}

// ── Regime → field mapping ────────────────────────────────────────────────────

/**
 * Maps a regime ID to the observation field names for Y value and error.
 *
 * ADR-014: each observation has four nullable quantity fields. Exactly one
 * is non-null, determined by regime. This lookup makes the mapping explicit
 * so the component doesn't need a switch statement.
 */
interface RegimeFieldMapping {
  /** The observation record field holding the Y value. */
  valueField: keyof ObservationRecord;
  /** The observation record field holding the Y error. */
  errorField: keyof ObservationRecord;
}

const REGIME_FIELD_MAP: Record<string, RegimeFieldMapping> = {
  optical: { valueField: 'magnitude',    errorField: 'magnitude_error' },
  xray:    { valueField: 'count_rate',   errorField: 'count_rate_error' },
  gamma:   { valueField: 'photon_flux',  errorField: 'photon_flux_error' },
  radio:   { valueField: 'flux_density', errorField: 'flux_density_error' },
};

/**
 * Get the Y-value and error from an observation record for a given regime.
 *
 * Returns null for value if the expected field is null (shouldn't happen
 * in well-formed data, but we handle it gracefully).
 */
export function getObservationValue(
  obs: ObservationRecord,
  regimeId: string,
): { value: number | null; error: number | null } {
  const mapping = REGIME_FIELD_MAP[regimeId];
  if (!mapping) return { value: null, error: null };

  return {
    value: obs[mapping.valueField] as number | null,
    error: obs[mapping.errorField] as number | null,
  };
}

// ── Epoch formatting ──────────────────────────────────────────────────────────

/**
 * Epoch label format options, matching the three-way toggle from ADR-013.
 * Same format used by both the spectra viewer and light curve panel.
 */
export type EpochFormat = 'dpo' | 'mjd' | 'calendar';

/** Short month names for calendar date display (ADR-013: "2018 Nov 14"). */
const MONTH_ABBR = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
];

/**
 * MJD epoch offset: MJD 0 = 1858-11-17T00:00:00 UTC.
 * JavaScript Date uses milliseconds since 1970-01-01.
 * MJD 40587.0 = 1970-01-01T00:00:00 UTC.
 */
const MJD_UNIX_EPOCH = 40587.0;
const MS_PER_DAY = 86400000;

/**
 * Convert an MJD value to a JavaScript Date object.
 */
export function mjdToDate(mjd: number): Date {
  const unixMs = (mjd - MJD_UNIX_EPOCH) * MS_PER_DAY;
  return new Date(unixMs);
}

/**
 * Format a time value for display, given the active epoch format.
 *
 * @param obs - The observation record (carries both MJD and DPO)
 * @param format - Active epoch label format
 * @returns Formatted string suitable for axis tick or tooltip
 */
export function formatEpoch(
  obs: { epoch_mjd: number; days_since_outburst: number | null },
  format: EpochFormat,
): string {
  switch (format) {
    case 'dpo': {
      if (obs.days_since_outburst == null) return `MJD ${obs.epoch_mjd.toFixed(1)}`;
      const d = obs.days_since_outburst;
      // Show decimal for sub-day precision, integer otherwise
      return d % 1 === 0 ? `Day ${d}` : `Day ${d.toFixed(1)}`;
    }
    case 'mjd':
      return obs.epoch_mjd.toFixed(1);
    case 'calendar': {
      const date = mjdToDate(obs.epoch_mjd);
      const year = date.getUTCFullYear();
      const month = MONTH_ABBR[date.getUTCMonth()];
      const day = date.getUTCDate();
      return `${year} ${month} ${day}`;
    }
  }
}

/**
 * Get the numeric X-axis value for an observation under the active epoch format.
 *
 * - DPO: days_since_outburst (falls back to MJD if null)
 * - MJD: epoch_mjd
 * - Calendar: epoch_mjd (Plotly handles Date objects, but we use MJD for
 *   consistent numeric axes and format the tick labels ourselves)
 *
 * For the calendar format, we return MJD as the underlying numeric coordinate
 * and handle formatting in the tooltip / tick labels. This avoids mixing
 * Date objects and numbers on the same axis.
 */
export function getTimeValue(
  obs: { epoch_mjd: number; days_since_outburst: number | null },
  format: EpochFormat,
): number {
  if (format === 'dpo' && obs.days_since_outburst != null) {
    return obs.days_since_outburst;
  }
  return obs.epoch_mjd;
}

/**
 * Get the X-axis label for the active epoch format.
 */
export function getTimeAxisLabel(format: EpochFormat): string {
  switch (format) {
    case 'dpo': return 'Days since outburst';
    case 'mjd': return 'MJD';
    case 'calendar': return 'Date';
  }
}

// ── Default time scale selection ──────────────────────────────────────────────

/**
 * Compute the gap ratio for a sorted array of time values.
 *
 * ADR-013: "If the ratio of the largest time gap to the total time span
 * exceeds 0.5, default to log scale."
 *
 * This catches novae with dense early observations followed by sparse
 * late-epoch data, where linear scaling would compress the interesting
 * early evolution into an unreadable band.
 *
 * @param times - Sorted array of time values (ascending)
 * @returns Gap ratio in [0, 1], or 0 if fewer than 2 points
 */
export function computeGapRatio(times: number[]): number {
  if (times.length < 2) return 0;

  const totalSpan = times[times.length - 1] - times[0];
  if (totalSpan <= 0) return 0;

  let maxGap = 0;
  for (let i = 1; i < times.length; i++) {
    const gap = times[i] - times[i - 1];
    if (gap > maxGap) maxGap = gap;
  }

  return maxGap / totalSpan;
}

/**
 * Determine whether the default time axis scale should be logarithmic.
 * ADR-013: log if gap_ratio > 0.5.
 */
export function shouldDefaultToLogTime(times: number[]): boolean {
  return computeGapRatio(times) > 0.5;
}

// ── Band metadata helpers ─────────────────────────────────────────────────────

/**
 * Build a lookup from band ID to BandRecord for quick access during rendering.
 */
export function buildBandLookup(bands: BandRecord[]): Map<string, BandRecord> {
  const map = new Map<string, BandRecord>();
  for (const b of bands) {
    map.set(b.band, b);
  }
  return map;
}

/**
 * Assign colors to all bands in a regime, using semantic anchors where
 * available and cycling through the fallback palette for unknowns.
 *
 * Returns a Map of band ID → hex color string.
 */
export function assignBandColors(bandIds: string[]): Map<string, string> {
  const colors = new Map<string, string>();
  let fallbackIdx = 0;

  for (const band of bandIds) {
    // Check if this band has a semantic anchor
    if (band in BAND_COLOR_MAP || ['unfiltered', 'cv', 'tg', 'vis'].includes(band.toLowerCase())) {
      colors.set(band, getBandColor(band));
    } else {
      colors.set(band, getBandColor(band, fallbackIdx));
      fallbackIdx++;
    }
  }

  return colors;
}
