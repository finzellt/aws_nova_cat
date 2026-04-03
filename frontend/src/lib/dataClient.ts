/**
 * Data client for artifact fetching (DESIGN-003 §14.5).
 *
 * Centralizes all data-layer access behind three functions:
 *   - resolveRelease()  — discover the active release ID
 *   - getArtifactUrl()  — construct a full artifact URL
 *   - fetchArtifact()   — convenience: resolve + fetch + parse
 *
 * Environment modes:
 *   Production — NEXT_PUBLIC_DATA_URL is set to the CloudFront domain.
 *                resolveRelease() fetches current.json from CloudFront.
 *                URLs are: ${DATA_URL}/releases/${releaseId}/${path}
 *
 *   Development — NEXT_PUBLIC_DATA_URL is unset (or empty).
 *                 resolveRelease() returns "local" with no network call.
 *                 URLs are: /data/${path} (Next.js public directory).
 *
 * This is a plain TypeScript module — not a React hook. It works in both
 * server components and client components. No dependencies beyond the
 * browser/Node fetch API.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Shape of the current.json pointer file written by the release publisher
 * (§12.3). Only release_id is consumed; generated_at is informational.
 */
interface ReleasePointer {
  release_id: string;
  generated_at: string;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/** Sentinel release ID returned in development mode (§14.6). */
const LOCAL_RELEASE_ID = 'local';

/**
 * Returns the configured CloudFront data URL, or null if unset (dev mode).
 * Strips any trailing slash so callers can append paths directly.
 */
function getDataUrl(): string | null {
  const url = process.env.NEXT_PUBLIC_DATA_URL;
  if (!url) return null;
  return url.replace(/\/+$/, '');
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Discover the active release ID.
 *
 * In production, fetches `current.json` from CloudFront and returns the
 * release_id string. Throws on network error or missing pointer — the
 * caller's error boundary handles this as a "data unavailable" state (§14.9).
 *
 * In development (NEXT_PUBLIC_DATA_URL unset), returns "local" immediately
 * with no network call (§14.6).
 */
export async function resolveRelease(): Promise<string> {
  const dataUrl = getDataUrl();

  if (!dataUrl) {
    return LOCAL_RELEASE_ID;
  }

  const response = await fetch(`${dataUrl}/current.json`, { cache: 'no-store' });

  if (!response.ok) {
    throw new Error(
      `Failed to fetch release pointer: ${response.status} ${response.statusText}`,
    );
  }

  const pointer: ReleasePointer = (await response.json()) as ReleasePointer;
  return pointer.release_id;
}

/**
 * Construct the full URL for an artifact.
 *
 * Pure function — no network call. The caller supplies a release ID
 * (from resolveRelease()) and an ADR-014-style path such as
 * `catalog.json` or `nova/<id>/spectra.json`.
 *
 * In production: `${DATA_URL}/releases/${releaseId}/${path}`
 * In dev mode (releaseId === "local"): `/data/${path}` (§14.6)
 */
export function getArtifactUrl(releaseId: string, path: string): string {
  if (releaseId === LOCAL_RELEASE_ID) {
    return `/data/${path}`;
  }

  const dataUrl = getDataUrl();

  if (!dataUrl) {
    // Defensive: if someone passes a real release ID but DATA_URL is unset,
    // fall back to local paths rather than constructing a broken URL.
    return `/data/${path}`;
  }

  return `${dataUrl}/releases/${releaseId}/${path}`;
}

/**
 * Convenience wrapper: resolve the active release, fetch an artifact, and
 * parse the JSON response.
 *
 * Typed generically so callers get type-safe artifacts:
 *   const catalog = await fetchArtifact<CatalogData>('catalog.json');
 *   const spectra = await fetchArtifact<SpectraArtifact>(`nova/${id}/spectra.json`);
 *
 * Throws on network error, non-OK response, or JSON parse failure.
 * Callers handle errors per §14.9 (error boundary or component error state).
 */
export async function fetchArtifact<T>(path: string): Promise<T> {
  const releaseId = await resolveRelease();
  const url = getArtifactUrl(releaseId, path);

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(
      `Failed to fetch artifact "${path}": ${response.status} ${response.statusText}`,
    );
  }

  return (await response.json()) as T;
}
