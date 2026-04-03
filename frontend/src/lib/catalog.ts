/**
 * Server-side catalog data helpers.
 *
 * This module provides catalog.json data to Server Components. It is the
 * only module that should import from 'fs' — do not import this file into
 * client components (any file marked 'use client').
 *
 * Two modes (DESIGN-003 §14.6):
 *
 *   Development (NEXT_PUBLIC_DATA_URL unset):
 *     Reads catalog.json from the filesystem at public/data/catalog.json.
 *     No network call, no AWS credentials needed. `npm run dev` just works.
 *
 *   Production (NEXT_PUBLIC_DATA_URL set):
 *     Fetches the active release's catalog.json from CloudFront via the
 *     data client. Pages become dynamically rendered (SSR, not SSG) because
 *     the fetch uses cache: 'no-store' on the pointer resolution.
 */

import { readFile } from 'fs/promises';
import path from 'path';
import type { CatalogData } from '@/types/catalog';
import { fetchArtifact } from '@/lib/dataClient';

/** Fallback returned when catalog data is unavailable. */
const EMPTY_CATALOG: CatalogData = {
  schema_version: '1.1',
  generated_at: '',
  stats: {
    nova_count: 0,
    spectra_count: 0,
    photometry_count: 0,
  },
  novae: [],
};

/**
 * Load catalog.json for use in Server Components.
 *
 * Returns an empty catalog on any failure (missing file in dev, network
 * error in production). Callers render a graceful empty state in that case
 * — the homepage shows "Catalog data is not yet available", and the catalog
 * page shows a zero-row table.
 */
export async function getCatalogData(): Promise<CatalogData> {
  const dataUrl = process.env.NEXT_PUBLIC_DATA_URL;

  if (!dataUrl) {
    // Dev mode: read directly from the filesystem (§14.6).
    // Relative fetch URLs don't work in server components (no browser
    // context to resolve against), so we keep the fs read for local dev.
    try {
      const filePath = path.join(process.cwd(), 'public', 'data', 'catalog.json');
      const raw = await readFile(filePath, 'utf-8');
      return JSON.parse(raw) as CatalogData;
    } catch {
      return EMPTY_CATALOG;
    }
  }

  // Production: fetch from CloudFront via the data client.
  // fetchArtifact resolves the active release, constructs a full
  // CloudFront URL (absolute — works in server components), fetches,
  // and parses the JSON response.
  try {
    return await fetchArtifact<CatalogData>('catalog.json');
  } catch {
    return EMPTY_CATALOG;
  }
}
