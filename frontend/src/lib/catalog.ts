/**
 * Server-side catalog data helpers.
 *
 * This module reads catalog.json directly from the filesystem at build time.
 * It is intended for use in Next.js Server Components only — do not import
 * into client components (any file marked 'use client').
 *
 * The catalog artifact lives at public/data/catalog.json and is served as a
 * static file by Next.js. Reading via fs at build time means SSG pages contain
 * the catalog data inline with no client-side fetch on first load.
 */

import { readFile } from 'fs/promises';
import path from 'path';
import type { CatalogData } from '@/types/catalog';

/** Fallback returned when catalog.json is absent (e.g. during initial setup). */
const EMPTY_CATALOG: CatalogData = {
  schema_version: '1.0',
  generated_at: '',
  stats: {
    nova_count: 0,
    spectra_count: 0,
    photometry_count: 0,
  },
  novae: [],
};

/**
 * Reads and parses catalog.json from public/data/.
 * Returns an empty catalog if the file does not exist or cannot be parsed.
 */
export async function getCatalogData(): Promise<CatalogData> {
  try {
    const filePath = path.join(process.cwd(), 'public', 'data', 'catalog.json');
    const raw = await readFile(filePath, 'utf-8');
    return JSON.parse(raw) as CatalogData;
  } catch {
    // File absent during development or first-run — return a safe default.
    return EMPTY_CATALOG;
  }
}
