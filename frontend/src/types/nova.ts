/**
 * TypeScript types for per-nova artifact schemas (ADR-014).
 *
 * These interfaces mirror the exact JSON shapes that the backend generation
 * pipeline writes. Field names are part of the public contract — do not rename
 * them without a coordinated backend change and schema version bump.
 */

// ── Nova metadata artifact (nova.json) ────────────────────────────────────────

/** Core object properties powering the nova page metadata region. */
export interface NovaMetadata {
  schema_version: string;
  generated_at: string;
  nova_id: string;
  primary_name: string;
  aliases: string[];
  ra: string;
  dec: string;
  /**
   * Discovery date in YYYY-MM-DD format.
   * Day is "00" when only month precision is available.
   * Month and day are "00" when only year precision is available.
   * Examples: "1901-02-22", "1901-02-00", "1901-00-00"
   */
  discovery_date: string;
  nova_type: string;
  spectra_count: number;
  photometry_count: number;
}

// ── References artifact (references.json) ─────────────────────────────────────

/** A single literature reference record. */
export interface Reference {
  bibcode: string;
  title: string;
  authors: string[];
  year: number;
  doi: string | null;
  arxiv_id: string | null;
  ads_url: string;
}

/** Full references artifact. Fetched independently of nova.json (ADR-014). */
export interface ReferencesArtifact {
  schema_version: string;
  generated_at: string;
  nova_id: string;
  references: Reference[];
}

// ── Observation record (per-DataProduct row for the observations table) ──────

/** One raw DataProduct file's metadata, before multi-arm merging. */
export interface ObservationRecord {
  data_product_id: string;
  instrument: string;
  telescope: string;
  epoch_mjd: number;
  wavelength_min: number;
  wavelength_max: number;
  provider: string;
  /** Median signal-to-noise ratio per pixel; undefined when not available. */
  snr?: number;
}

// ── Spectra artifact (spectra.json) ───────────────────────────────────────────

/** One spectrum's data and metadata, as pre-processed by the backend. */
export interface SpectrumRecord {
  spectrum_id: string;
  epoch_mjd: number;
  /** Null when outburst date is unresolved. */
  days_since_outburst: number | null;
  instrument: string;
  telescope: string;
  provider: string;
  wavelength_min: number;
  wavelength_max: number;
  /** Original flux unit prior to normalisation, e.g. "erg/cm2/s/A". */
  flux_unit: string;
  /** Median flux used for normalisation, in original flux units. */
  normalization_scale: number;
  /** Wavelength array in nm. */
  wavelengths: number[];
  /** Median-normalised flux array, parallel to wavelengths. */
  flux_normalized: number[];
}

/** Full spectra artifact. All data required for the waterfall plot (ADR-013). */
export interface SpectraArtifact {
  schema_version: string;
  generated_at: string;
  nova_id: string;
  /** Reference outburst MJD; null if unresolved. */
  outburst_mjd: number | null;
  /** Wavelength unit for all spectra in this artifact (always "nm"). */
  wavelength_unit: string;
  /** Total number of raw DataProduct files (may exceed spectra.length after merging). */
  total_data_products?: number;
  /** Per-DataProduct observation records (pre-merge), for the observations table. */
  observations?: ObservationRecord[];
  spectra: SpectrumRecord[];
}
