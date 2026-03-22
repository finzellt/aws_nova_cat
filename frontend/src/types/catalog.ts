/**
 * Types for the catalog.json static artifact.
 *
 * Schema version: 1.0 (ADR-014)
 * These types mirror the artifact schema exactly. Field renames are breaking
 * changes — any update here must be coordinated with the backend generation
 * pipeline.
 */

/** One entry in the `novae` array of catalog.json. */
export interface NovaSummary {
  /** Stable nova UUID. Used as the canonical identifier downstream. */
  nova_id: string;

  /** Primary designation (e.g. "GK Per"). Used as the display name and link target. */
  primary_name: string;

  /**
   * All known aliases. Used for client-side name/alias search.
   * The catalog table shows up to 2 aliases inline; the rest are on the nova page.
   */
  aliases: string[];

  /** Right ascension in HH:MM:SS.ss format. */
  ra: string;

  /** Declination in ±DD:MM:SS.s format. */
  dec: string;

  /** Four-digit discovery year. */
  discovery_year: number;

  /** Count of validated spectra. Default sort key (descending). */
  spectra_count: number;

  /**
   * Count of photometric observations.
   * Value is 0 (not null) when none are available; the table renders `—` for 0.
   */
  photometry_count: number;

  /** Count of associated literature references. */
  references_count: number;

  /**
   * Whether a sparkline SVG has been generated for this nova.
   * The light curve column is post-MVP; this flag is reserved for when it lands.
   */
  has_sparkline: boolean;
}

/** Aggregate statistics block for the homepage stats bar. */
export interface CatalogStats {
  nova_count: number;
  spectra_count: number;
  photometry_count: number;
}

/** Top-level structure of the catalog.json artifact. */
export interface CatalogData {
  schema_version: string;
  generated_at: string;
  stats: CatalogStats;
  novae: NovaSummary[];
}
