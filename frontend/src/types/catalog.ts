/**
 * Types for the catalog.json static artifact.
 *
 * Schema version: 1.2 (ADR-014, DESIGN-003 §11.9, §F9)
 * These types mirror the artifact schema exactly. Field renames are breaking
 * changes — any update here must be coordinated with the backend generation
 * pipeline.
 *
 * v1.1 changes:
 *   - discovery_year (number) replaced by discovery_date (string | null).
 *     Format: YYYY-MM-DD. Day is "00" when only month precision is available;
 *     month and day are "00" when only year precision is available.
 *     Null when no dated references exist for the nova.
 */

/** Archive-provider source (e.g. ESO, AAVSO). */
export interface ArchiveSource {
  type: 'archive';
  provider: string;
}

/** Bibcode-backed source from ticket ingestion. */
export interface BibcodeSource {
  type: 'bibcode';
  bibcode: string;
}

/** Discriminated union for data provenance entries. */
export type SourceEntry = ArchiveSource | BibcodeSource;

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

  /**
   * Discovery date in YYYY-MM-DD format, or null when no dated references exist.
   *
   * Day is "00" when only month precision is available (e.g. "1901-02-00").
   * Month and day are "00" when only year precision is available (e.g. "1901-00-00").
   *
   * Schema v1.1 (§11.9): replaces the former discovery_year (number) field.
   */
  discovery_date: string | null;

  /** Count of validated spectra. Default sort key (descending). */
  spectra_count: number;

  /** Count of distinct observation nights with spectra. */
  spectral_visits: number;

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

  /**
   * Data provenance entries. Each entry is either an archive provider
   * (e.g. "ESO") or a bibcode from ticket-ingested data.
   * Empty array when no validated spectra exist for this nova.
   *
   * Schema v1.2 (§F9).
   */
  sources: SourceEntry[];
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
