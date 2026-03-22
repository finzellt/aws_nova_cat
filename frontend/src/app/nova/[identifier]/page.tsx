/**
 * Nova page route: /nova/[identifier]
 *
 * This is a thin Next.js server component. Its only job is to:
 *   1. Extract the identifier from the URL params.
 *   2. Set the HTML <title> tag for this page.
 *   3. Render the NovaPage client component with the identifier.
 *
 * All data fetching and rendering happens inside NovaPage (a 'use client'
 * component), because that data varies per-user-visit and depends on
 * browser-relative fetch paths.
 *
 * Note on Next.js versions: `params` is a Promise in Next.js 15+, so we
 * `await` it here. This is backward-compatible with Next.js 14 as well.
 */
import type { Metadata } from 'next';
import NovaPage from '@/components/nova/NovaPage';

interface PageProps {
  params: Promise<{ identifier: string }>;
}

/**
 * Generate the HTML <title> tag dynamically.
 *
 * We use the URL identifier as the title before the full nova name loads,
 * which is sufficient for browser tabs and bookmarks.
 */
export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const { identifier } = await params;
  const displayName = decodeURIComponent(identifier);
  return {
    title: `${displayName} — Open Nova Catalog`,
    description: `Observational data, spectra, and references for ${displayName}.`,
  };
}

export default async function NovaPageRoute({ params }: PageProps) {
  const { identifier } = await params;
  return <NovaPage identifier={identifier} />;
}
