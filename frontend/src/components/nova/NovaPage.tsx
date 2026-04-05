'use client';

/**
 * NovaPage — the primary client component for /nova/[identifier].
 *
 * Responsibilities:
 *   - Resolve the active data release via the data client (§14.5).
 *   - Fetch all three per-nova artifacts in parallel.
 *   - References are fetched independently so the metadata region can render
 *     before the references table is populated (ADR-014 design intent).
 *   - Render the two-column layout defined in ADR-011 / ADR-012.
 *   - Provide per-section loading and error states.
 *
 * Why 'use client'?
 *   The nova page fetches data at runtime from paths that depend on the
 *   identifier. Making it a client component with useEffect is the simplest
 *   approach, and it gives us independent loading states for each artifact.
 */

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { ChevronLeft, CircleAlert } from 'lucide-react';
import type {
  NovaMetadata,
  ReferencesArtifact,
  SpectraArtifact,
} from '@/types/nova';
import type { CatalogData } from '@/types/catalog';
import { resolveRelease, getArtifactUrl } from '@/lib/dataClient';
import ObjectSummary from './ObjectSummary';
import ObservationsTable from './ObservationsTable';
import ReferencesTable from './ReferencesTable';
import VisualizationRegion from './VisualizationRegion';

// ── Types ─────────────────────────────────────────────────────────────────────

interface NovaPageProps {
  /**
   * URL identifier for this nova. Next.js decodes this from the URL
   * automatically — e.g. if the URL is /nova/GK%20Per, identifier is "GK Per".
   */
  identifier: string;
}

/**
 * A discriminated union tracking the lifecycle of an async data fetch.
 * Using a union with a `status` field lets TypeScript narrow the type
 * safely — e.g. `if (state.status === 'success') { state.data }`.
 */
type FetchState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; message: string };

// ── Component ─────────────────────────────────────────────────────────────────

export default function NovaPage({ identifier }: NovaPageProps) {
  // Next.js params are already decoded, so `identifier` is the human-readable
  // value. Re-encode it for use in fetch URLs so paths with spaces work.
  const displayName = decodeURIComponent(identifier);
  const encodedId = encodeURIComponent(identifier);

  // Release-aware base path for per-nova artifacts, set after resolveRelease()
  // completes. Passed to VisualizationRegion for its independent photometry
  // fetch. Empty string until release resolution; this is safe because
  // VisualizationRegion only fetches photometry when hasPhotometry is true,
  // which requires nova.json to have loaded first.
  const [basePath, setBasePath] = useState('');

  // Release ID, stored separately for bundle URL construction (§14.8).
  const [releaseId, setReleaseId] = useState('');

  const [novaState, setNovaState] = useState<FetchState<NovaMetadata>>({
    status: 'loading',
  });
  const [spectraState, setSpectraState] = useState<FetchState<SpectraArtifact>>({
    status: 'loading',
  });
  const [refsState, setRefsState] = useState<FetchState<ReferencesArtifact>>({
    status: 'loading',
  });

  // Resolve the active release, then fetch all three artifacts in parallel.
  // nova.json and spectra.json start immediately after release resolution;
  // references.json also starts immediately but renders into its own section
  // so a delayed response only affects the references table, not the page.
  useEffect(() => {
    let cancelled = false;

    // Reset to loading on identifier change (client-side navigation).
    setNovaState({ status: 'loading' });
    setSpectraState({ status: 'loading' });
    setRefsState({ status: 'loading' });

    async function loadAll() {
      // ── Step 1: resolve the active release (§14.3) ─────────────────
      // In dev mode this returns "local" instantly (no network call).
      // In production this fetches current.json from CloudFront.
      let resolved: string;
      try {
        resolved = await resolveRelease();
      } catch {
        if (!cancelled) {
          setNovaState({
            status: 'error',
            message: 'Data temporarily unavailable',
          });
        }
        return;
      }

      // In production, S3 artifacts live under the nova UUID, not the
      // display name. Fetch catalog.json to resolve name → nova_id.
      // In dev mode (resolved === "local"), local fixtures use name-based
      // paths, so skip the catalog lookup.
      let novaPathSegment = encodedId;
      if (resolved !== 'local') {
        try {
          const catalogUrl = getArtifactUrl(resolved, 'catalog.json');
          const catRes = await fetch(catalogUrl);
          if (!catRes.ok) throw new Error(`HTTP ${catRes.status}`);
          const catalog: CatalogData = await catRes.json() as CatalogData;
          // CatalogTable links use hyphens for spaces (e.g. "V1324-Sco"),
          // so normalize hyphens back to spaces for matching.
          const normalizedName = displayName.replace(/-/g, ' ');
          const entry = catalog.novae.find(
            (n) => n.primary_name === normalizedName || n.primary_name === displayName,
          );
          if (!entry) {
            if (!cancelled) {
              setNovaState({ status: 'error', message: 'Nova not found in catalog' });
            }
            return;
          }
          novaPathSegment = entry.nova_id;
        } catch {
          if (!cancelled) {
            setNovaState({ status: 'error', message: 'Data temporarily unavailable' });
          }
          return;
        }
      }

      // Construct the base path for this nova's artifacts.
      // Dev:  /data/nova/<displayName>
      // Prod: https://<cf-domain>/releases/<release>/nova/<uuid>
      const novaBasePath = getArtifactUrl(resolved, `nova/${novaPathSegment}`);

      if (!cancelled) {
        setReleaseId(resolved);
        setBasePath(novaBasePath);
      }

      // ── Step 2: fetch all three artifacts in parallel ──────────────
      async function fetchNova() {
        try {
          const res = await fetch(`${novaBasePath}/nova.json`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data: NovaMetadata = await res.json() as NovaMetadata;
          if (!cancelled) setNovaState({ status: 'success', data });
        } catch (err) {
          if (!cancelled) {
            setNovaState({
              status: 'error',
              message: err instanceof Error ? err.message : 'Unknown error',
            });
          }
        }
      }

      async function fetchSpectra() {
        try {
          const res = await fetch(`${novaBasePath}/spectra.json`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data: SpectraArtifact = await res.json() as SpectraArtifact;
          if (!cancelled) setSpectraState({ status: 'success', data });
        } catch (err) {
          if (!cancelled) {
            setSpectraState({
              status: 'error',
              message: err instanceof Error ? err.message : 'Unknown error',
            });
          }
        }
      }

      async function fetchReferences() {
        try {
          const res = await fetch(`${novaBasePath}/references.json`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data: ReferencesArtifact = await res.json() as ReferencesArtifact;
          if (!cancelled) setRefsState({ status: 'success', data });
        } catch (err) {
          if (!cancelled) {
            setRefsState({
              status: 'error',
              message: err instanceof Error ? err.message : 'Unknown error',
            });
          }
        }
      }

      void fetchNova();
      void fetchSpectra();
      void fetchReferences();
    }

    void loadAll();

    // Cleanup: prevent setState on unmounted component if the user
    // navigates away before fetches complete.
    return () => { cancelled = true; };
    // encodedId is derived from identifier; if identifier changes, re-fetch.
  }, [encodedId]);

  // ── Nova-not-found error state ─────────────────────────────────────────────
  // Only show this for nova.json failures. Spectra/refs errors are handled
  // inline within their sections.
  if (novaState.status === 'error') {
    return (
      <div className="flex flex-col items-center justify-center py-24 gap-4 text-center">
        <CircleAlert
          size={32}
          className="text-[var(--color-status-error-fg)]"
          aria-hidden="true"
        />
        <p className="text-sm font-semibold text-[var(--color-text-primary)]">
          Nova not found
        </p>
        <p className="text-sm text-[var(--color-text-secondary)]">
          <code className="font-mono text-xs">{displayName}</code> could not
          be located in the catalog.
        </p>
        <Link
          href="/catalog"
          className="text-sm font-medium text-[var(--color-interactive)] hover:underline"
        >
          ← Back to catalog
        </Link>
      </div>
    );
  }

  const nova = novaState.status === 'success' ? novaState.data : null;

  // Bundle download URL (§14.8, DESIGN-003 OQ-5 resolved).  The S3 key
  // is the stable name "bundle.zip"; the dated human-readable filename
  // is delivered via Content-Disposition on the response.
  const bundleHref = basePath ? `${basePath}/bundle.zip` : '#';

  return (
    <div className="py-8">
      {/* ── Back navigation ──────────────────────────────────────── */}
      <Link
        href="/catalog"
        className={[
          'inline-flex items-center gap-1 mb-6',
          'text-sm text-[var(--color-text-secondary)]',
          'hover:text-[var(--color-interactive)] transition-colors',
        ].join(' ')}
      >
        <ChevronLeft size={14} aria-hidden="true" />
        All novae
      </Link>

      {/*
       * ── Two-column layout (ADR-011 / ADR-012) ────────────────────
       *
       * On viewports > 1024px: visualization region ~60% left,
       * metadata region ~40% right (expressed as 3fr / 2fr).
       *
       * items-start prevents the shorter column from stretching to
       * match the taller one.
       *
       * On ≤ 1024px: single column; visualization region appears first
       * because it is the primary content (ADR-011).
       */}
      <div className="grid grid-cols-1 lg:grid-cols-[3fr_2fr] gap-8 lg:items-start">

        {/* ── Left: visualization region ──────────────────────────── */}
        <VisualizationRegion
          spectraData={spectraState.status === 'success' ? spectraState.data : null}
          spectraLoading={spectraState.status === 'loading'}
          spectraError={spectraState.status === 'error'}
          hasPhotometry={nova !== null && nova.photometry_count > 0}
          basePath={basePath}
        />

        {/* ── Right: metadata region ──────────────────────────────── */}
        <div className="flex flex-col gap-8">

          {/* Object summary */}
          {novaState.status === 'loading' ? (
            // Loading skeleton: two placeholder bars while nova.json arrives
            <div className="animate-pulse flex flex-col gap-3" aria-busy="true" aria-label="Loading nova metadata">
              <div className="h-9 bg-[var(--color-surface-tertiary)] rounded w-2/3" />
              <div className="h-4 bg-[var(--color-surface-tertiary)] rounded w-1/2" />
              <div className="h-4 bg-[var(--color-surface-tertiary)] rounded w-3/5 mt-2" />
            </div>
          ) : nova !== null ? (
            <ObjectSummary nova={nova} bundleHref={bundleHref} />
          ) : null}

          {/* Observations summary (derived from spectra.json at render time) */}
          <section aria-labelledby="observations-heading">
            <h2
              id="observations-heading"
              className="text-xs font-semibold uppercase tracking-wider text-[var(--color-text-secondary)] mb-3"
            >
              Observations
            </h2>
            <ObservationsTable
              spectra={
                spectraState.status === 'success'
                  ? spectraState.data.spectra
                  : []
              }
              loading={spectraState.status === 'loading'}
              error={spectraState.status === 'error'}
            />
          </section>

          {/* Literature references (fetched independently for lazy loading) */}
          <section aria-labelledby="references-heading">
            <h2
              id="references-heading"
              className="text-xs font-semibold uppercase tracking-wider text-[var(--color-text-secondary)] mb-3"
            >
              Literature References
            </h2>
            <ReferencesTable
              references={
                refsState.status === 'success'
                  ? refsState.data.references
                  : []
              }
              loading={refsState.status === 'loading'}
              error={refsState.status === 'error'}
            />
          </section>

        </div>
      </div>
    </div>
  );
}
