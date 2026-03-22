/**
 * TypeScript types for the photometry.json artifact schema (ADR-014).
 *
 * These interfaces mirror the exact JSON shapes that the backend generation
 * pipeline writes. Field names are part of the public contract — do not rename
 * them without a coordinated backend change and schema version bump.
 *
 * Key design note: each observation carries four nullable quantity fields
 * (magnitude, flux_density, count_rate, photon_flux). Exactly one is non-null
 * per observation, determined by the regime. The regime metadata record tells
 * the frontend which field to read via its axis configuration.
 */

// ── Regime metadata ───────────────────────────────────────────────────────────

/**
 * One entry in the top-level `regimes` array.
 *
 * This is the authoritative source for tab structure. The frontend iterates
 * this array directly to determine how many tabs to render and what to label
 * them — it never scans the observations array to figure this out.
 */
export interface RegimeRecord {
  /** Regime identifier; matches `regime` field on band and observation records. */
  id: string;
  /** Human-readable tab label (e.g. "Optical", "X-ray"). */
  label: string;
  /** Y axis label for this regime's plot (e.g. "Magnitude", "Count rate (cts/s)"). */
  y_axis_label: string;
  /** Whether the Y axis is inverted (true for optical: brighter = top). */
  y_axis_inverted: boolean;
  /** Default Y axis scale: "linear" or "log". */
  y_axis_scale_default: string;
  /** Band identifiers belonging to this regime. */
  bands: string[];
}

// ── Band metadata ─────────────────────────────────────────────────────────────

/**
 * One entry in the top-level `bands` array.
 *
 * Each band carries a pre-computed vertical offset from the backend.
 * For optical bands this separates overlapping magnitude curves visually.
 * The reference band has offset 0.0.
 */
export interface BandRecord {
  /** Band identifier (e.g. "V", "B", "UVW1", "0.3-10keV"). */
  band: string;
  /** Regime this band belongs to; matches `id` in the regimes array. */
  regime: string;
  /** Effective wavelength in nm; null for X-ray and gamma-ray bands. */
  wavelength_eff_nm: number | null;
  /** Pre-computed vertical offset for display; 0.0 for the reference band. */
  vertical_offset: number;
  /**
   * CSS design token name for this band's plot color per ADR-012.
   * Null for X-ray bands (colored by instrument instead).
   */
  display_color_token: string | null;
}

// ── Observation record ────────────────────────────────────────────────────────

/**
 * One photometric observation.
 *
 * The four nullable quantity fields (magnitude, flux_density, count_rate,
 * photon_flux) are mutually exclusive by regime:
 *   - optical  → magnitude
 *   - radio    → flux_density
 *   - xray     → count_rate
 *   - gamma    → photon_flux
 *
 * Each has a corresponding nullable error field.
 */
export interface ObservationRecord {
  /** Stable observation UUID. */
  observation_id: string;
  /** Observation epoch in MJD. */
  epoch_mjd: number;
  /** Days since outburst; null if outburst date unresolved. */
  days_since_outburst: number | null;
  /** Band identifier; matches a `band` value in the bands array. */
  band: string;
  /** Regime identifier; matches `id` in the regimes array. */
  regime: string;

  // ── Nullable quantity fields (exactly one non-null per observation) ──
  magnitude: number | null;
  magnitude_error: number | null;
  flux_density: number | null;
  flux_density_error: number | null;
  count_rate: number | null;
  count_rate_error: number | null;
  photon_flux: number | null;
  photon_flux_error: number | null;

  /** Whether this observation is a non-detection upper limit. */
  is_upper_limit: boolean;
  /** Data provider / archive source. */
  provider: string;
  /** Telescope name; "unknown" if not recorded. */
  telescope: string;
  /** Instrument name; "unknown" if not recorded. */
  instrument: string;
}

// ── Top-level artifact ────────────────────────────────────────────────────────

/** Full photometry artifact (photometry.json). ADR-014 schema version 1.0. */
export interface PhotometryArtifact {
  schema_version: string;
  generated_at: string;
  nova_id: string;
  /** Reference outburst MJD; null if unresolved. */
  outburst_mjd: number | null;
  /** Regime metadata records; one per regime present in the data. */
  regimes: RegimeRecord[];
  /** Band metadata records; one per photometric band present. */
  bands: BandRecord[];
  /** Individual photometric observation records. */
  observations: ObservationRecord[];
}
