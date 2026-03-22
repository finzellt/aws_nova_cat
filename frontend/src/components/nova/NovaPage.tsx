'use client';

/**
 * NovaPage — the primary client component for /nova/[identifier].
 *
 * Responsibilities:
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
  const basePath = `/data/nova/${encodedId}`;

  const [novaState, setNovaState] = useState<FetchState<NovaMetadata>>({
    status: 'loading',
  });
  const [spectraState, setSpectraState] = useState<FetchState<SpectraArtifact>>({
    status: 'loading',
  });
  const [refsState, setRefsState] = useState<FetchState<ReferencesArtifact>>({
    status: 'loading',
  });

  // Fetch all three artifacts. nova.json and spectra.json start immediately;
  // references.json also starts immediately but renders into its own section
  // so a delayed response only affects the references table, not the page.
  useEffect(() => {
    async function fetchNova() {
      try {
        const res = await fetch(`${basePath}/nova.json`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data: NovaMetadata = await res.json() as NovaMetadata;
        setNovaState({ status: 'success', data });
      } catch (err) {
        setNovaState({
          status: 'error',
          message: err instanceof Error ? err.message : 'Unknown error',
        });
      }
    }

    async function fetchSpectra() {
      try {
        const res = await fetch(`${basePath}/spectra.json`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data: SpectraArtifact = await res.json() as SpectraArtifact;
        setSpectraState({ status: 'success', data });
      } catch (err) {
        setSpectraState({
          status: 'error',
          message: err instanceof Error ? err.message : 'Unknown error',
        });
      }
    }

    async function fetchReferences() {
      try {
        const res = await fetch(`${basePath}/references.json`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data: ReferencesArtifact = await res.json() as ReferencesArtifact;
        setRefsState({ status: 'success', data });
      } catch (err) {
        setRefsState({
          status: 'error',
          message: err instanceof Error ? err.message : 'Unknown error',
        });
      }
    }

    void fetchNova();
    void fetchSpectra();
    void fetchReferences();
    // basePath is derived from identifier; if identifier changes, re-fetch.
  }, [basePath]);

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

  // Placeholder bundle href. In production this will be the pre-generated
  // bundle path. Per instructions, use a placeholder for now.
  const bundleHref = nova
    ? `/data/nova/${encodedId}/${nova.primary_name.replace(/\s+/g, '-')}_bundle.zip`
    : '#';

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
          loading={novaState.status === 'loading'}
          hasSpectra={nova !== null && nova.spectra_count > 0}
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
