/**
 * Root — /
 *
 * Redirects to /catalog, which is the primary entry point for the site
 * (ADR-010 §"Site Entry Point", reinstated by ADR-011 Amendment:
 * Homepage Redirect).
 *
 * The dedicated splash page formerly at this route — hero explainer,
 * stats bar, top-10 preview table — has been removed so users land
 * directly in the catalog without an intermediate page. The stats bar
 * now lives on /catalog and /search via the extracted StatsBar
 * component (src/components/catalog/StatsBar.tsx).
 *
 * 307 (temporary) is used for simplicity. If this routing decision
 * becomes settled long-term, swap in `permanentRedirect` from
 * next/navigation for a 308.
 */

import { redirect } from 'next/navigation';

export default function HomePage(): never {
  redirect('/catalog');
}
