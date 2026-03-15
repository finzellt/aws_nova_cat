# ADR-011: Frontend Architecture and Design Specification

Status: Proposed
Date: 2026-03-15

---

## Context

ADR-008 defines the product vision and design principles for the Open Nova Catalog website. ADR-009 defines the MVP strategy, including the published artifact architecture. ADR-010 defines the catalog navigation model and site structure.

This ADR defines the concrete frontend architecture, technology stack, and visual design specification required to implement the website described in those documents. It is intended to serve as a self-contained implementation guide.

---

## Relationship to the Open Supernova Catalog

The Open Nova Catalog can be understood as a spiritual successor to the Open Supernova Catalog (Guillochon et al. 2017, ApJ 835:64). That project demonstrated the value of a client-side catalog architecture for an astronomical sub-field — delivering a full metadata catalog to the browser for instant search and filtering — and served as a proof of concept for the object-centric, open-data model that motivates this project.

The Open Nova Catalog adopts a similar philosophy: curated, object-centered data, delivered as static artifacts, rendered in the browser. The frontend design builds on those lessons while prioritizing a cleaner, more modern visual presentation.

---

## Technology Stack

### Framework: React with Next.js

The frontend will be built using **React**, with **Next.js** as the application framework.

**Rationale:**

- React's component model scales naturally from a simple static catalog to a richer post-MVP interface with bespoke query tools and interactive comparisons.
- Next.js supports both static site generation (appropriate for MVP) and server-side API routes (appropriate for post-MVP programmatic access and dynamic queries), allowing the codebase to evolve without a framework change.
- React has the largest ecosystem of relevant pre-built components, including sortable/filterable data table libraries and scientific visualization tools.
- AI-assisted development workflows (the primary development model for this project) are most effective with React.

### Visualization Library: Plotly.js (via react-plotly.js)

Interactive spectra and light curve plots will be rendered using **Plotly.js**.

**Rationale:**

- Plotly.js supports the interaction patterns required for scientific data (zoom, pan, hover tooltips with wavelength/flux values).
- It handles the rendering requirements for both spectra (line plots with hover) and light curves (scatter plots with multi-band support).
- It is actively maintained and has a well-supported React wrapper.
- It aligns with common astronomical Python tooling (Plotly is also widely used in the scientific Python ecosystem), which simplifies artifact format design.

### Data Table: TanStack Table (via @tanstack/react-table)

The catalog table will be implemented using **TanStack Table**.

**Rationale:**

- Provides sorting, filtering, and pagination as composable primitives, giving full control over rendering.
- Headless design means no visual opinions are imposed — all styling is owned by the project.
- Scales well for post-MVP features such as advanced column filtering and server-side pagination.

### Styling: Tailwind CSS

Visual styling will use **Tailwind CSS**.

**Rationale:**

- Utility-first approach works well with AI-assisted development.
- Avoids the overhead of a component library while maintaining design consistency.
- Easily customizable to the project's specific design language.

---

## Visual Design Specification

### Overall Aesthetic

The interface should feel like it was designed by someone with genuine frontend experience — polished and considered, but not corporate or template-driven. The target sits between the AAVSO homepage (warm, accessible, human) and a clinical academic tool. It should feel lightweight and clean without tipping into minimalism.

Busyness should be avoided. Whitespace is a feature. The interface should never feel like it is trying to show everything at once.

### Color Palette

- **Background:** Warm off-white (`#F8F7F4` or similar), not stark white. This softens the interface and avoids the cold feeling of pure white against dark text.
- **Primary text:** Near-black (`#1A1A1A`), not pure black.
- **Accent color:** Deep teal (`#2A7D7B` or similar). Used for interactive elements, links, hover states, and key UI elements. Should feel scientific and considered, not decorative.
- **Secondary accent / muted:** A desaturated version of the teal or a warm grey, for secondary text, borders, and table grid lines.
- **Data visualization palette:** A small, carefully chosen set of distinguishable colors for multi-band light curves and spectra. Colors should remain legible on the warm off-white background.

*Note: Exact hex values should be refined during implementation. The palette described here defines the direction, not the final values.*

### Typography

- A clean, modern sans-serif for body text and UI elements (e.g., Inter or similar).
- A monospace or tabular-figures font for coordinates, identifiers, and numerical table values to ensure alignment.
- Headings should feel substantial without being heavy.

### Density

The interface should have moderate information density — enough that a scientist feels they are looking at a real data tool, not a marketing page, but not so dense that it feels like a legacy astronomical archive. Tables should have comfortable row heights with adequate padding.

---

## Page Specifications

### Homepage (`/`)

Per ADR-010, the root URL redirects to `/catalog`. However, a dedicated homepage is implemented as the landing experience.

**Layout (top to bottom):**

1. **Site header / navigation bar**
   - Site name / logo (left)
   - Navigation links: Catalog | Search | Documentation | About (right)

2. **Hero / explainer section**
   - A concise statement of what the Open Nova Catalog is and why it exists.
   - Should be 2–4 sentences. Not a wall of text. Aimed at a researcher encountering the site for the first time.
   - No large decorative imagery. Let the content speak.

3. **Stats bar**
   - A single horizontal strip summarizing the catalog at a glance.
   - Example fields: number of novae, total spectra available, total photometric observations.
   - Values should be drawn from the catalog artifact and therefore always current.
   - Clean, minimal visual treatment — numbers and labels, not charts.

4. **Catalog preview table**
   - A non-paginated sample of the full catalog (e.g., the top 10 entries by default sort order).
   - Uses the same component and column configuration as the full catalog page.
   - Includes a prominent "View Full Catalog →" link below the table.
   - Allows users to immediately see real data and click through to a nova page without navigating away first.

5. **Footer**
   - Links to GitHub repository, documentation, and citation information.
   - Brief data provenance note.

### Catalog Page (`/catalog`)

The primary browsing interface. Displays all novae in a paginated, sortable table.

**Catalog Table Columns:**

| Column | Notes |
|---|---|
| Primary Name | Links to `/nova/<primary-name>`. |
| RA / Dec | Displayed in standard astronomical format (HH:MM:SS / DD:MM:SS). Monospace font. Selectable/copyable text. |
| Discovery Year | Four-digit year. When only year precision is available, displayed as-is. |
| Spectra | Count of available validated spectra. Used as default sort key (descending). |
| Photometry | Count of photometric observations. Displayed as `—` when none available. |
| References | Count of associated literature references. |
| Light Curve | Inline sparkline showing the photometric light curve. Displayed as a greyed-out placeholder (e.g., `No data`) when photometry is unavailable. |

**Default Sort:** Descending by spectra count, so the most data-rich novae appear first.

**Pagination:** 25 rows per page. Classic pagination controls (Previous / Next / page numbers).

**Search:** A search bar above the table filters rows by primary name or alias. Filtering occurs client-side.

**Column selectability:** Table cells containing coordinates, names, and numerical values should render as selectable text that can be copied directly. Table layout should not interfere with standard browser copy behavior.

### Search Page (`/search`)

A dedicated route at `/search`. Presents the same search input and filtered catalog table as the catalog page, oriented around name/alias lookup. May share underlying components with the catalog page.

Rationale: users who already know the nova they are looking for should have a clearly labeled entry point. See ADR-010.

### Nova Page (`/nova/<identifier>`)

Accessible at both `/nova/<nova-id-uuid>` and `/nova/<primary-name>`. Both routes resolve to the same page.

**Layout:**

The page is divided into two primary regions, as defined in ADR-010.

**1. Visualization Region (primary, upper)**

- Displays the spectra viewer as the primary visual element.
- The spectra viewer renders all available validated spectra for the nova using Plotly.js.
- Spectra are plotted as flux vs. wavelength, offset vertically by epoch so that temporal evolution is visible at a glance (waterfall / time-series layout, following the Open Supernova Catalog model).
- Hover tooltips show wavelength, flux, epoch (MJD and calendar date), and instrument.
- Basic interaction: zoom, pan, reset.
- When photometry data becomes available, a light curve panel will be added to this region (below or alongside the spectra viewer).
- If no observational data is available for the nova, the region displays a clear, graceful empty state.

**2. Metadata and Reference Region (secondary, lower or sidebar)**

Organized into clearly labeled sections:

- **Object summary:** Primary name, aliases, coordinates (RA/Dec, selectable text), discovery date, nova status.
- **Observations summary:** Table listing available data products — instrument, epoch, wavelength range, data source/provider. Each row links to the originating archive where applicable.
- **Literature references:** Table of associated references — author/year, title, bibcode (linked to ADS). Formatted for easy reading and copy.
- **Download:** A clearly labeled download button for the curated nova bundle. Bundle format and contents described nearby (e.g., "ZIP archive containing reduced spectra, photometry table, and metadata").

**Copy-friendliness:** All structured data in this region (coordinates, identifiers, reference bibcodes) should be rendered as selectable text. Tables should not use layout techniques that interfere with standard clipboard copy behavior.

---

## Artifact Consumption Model

Per ADR-009, the frontend consumes pre-built published artifacts. It does not interact with internal databases or ingestion systems.

The frontend expects the following artifact types (schemas to be defined in a separate document):

| Artifact | Purpose | Consumed by |
|---|---|---|
| `catalog.json` | Full catalog metadata for all novae | Homepage preview, catalog page, search page |
| `nova/<nova-id>.json` | Per-nova metadata, references, observation summary | Nova page metadata region |
| `nova/<nova-id>/spectra.json` | Plot-ready spectra data (downsampled, axis metadata) | Nova page spectra viewer |
| `nova/<nova-id>/photometry.json` | Plot-ready photometry time-series | Nova page light curve panel, catalog sparklines |
| `nova/<nova-id>/bundle.zip` | Curated downloadable data bundle | Nova page download button |

Artifact schemas are defined in `docs/architecture/frontend-artifact-schemas.md` (to be written).

---

## Post-MVP Compatibility Notes

The following post-MVP capabilities have influenced architectural decisions in this document:

- **Bespoke query interface:** TanStack Table's headless, composable model supports the addition of multi-column filtering, range queries, and eventually server-side query execution without replacing the table implementation.
- **Programmatic API access:** Next.js API routes can be introduced alongside the existing static frontend, enabling a public API without a separate backend service.
- **Cross-nova comparison tools:** React's component model supports the addition of multi-nova visualization panels as self-contained components.
- **Extension to other transient classes:** The object-centric page model and artifact schema design should not be nova-specific. Generalization to other transient types should require data additions, not architectural changes.

---

## Open Questions

The following decisions are not resolved in this ADR and should be addressed before or during implementation:

1. **Artifact schemas:** The exact JSON shape of each artifact type needs to be formally specified. This is a dependency for both backend (artifact generation) and frontend (artifact consumption) implementation.

2. **Spectra viewer interaction details:** The exact behavior of the time-series spectra viewer (e.g., whether individual spectra can be toggled on/off, whether a wavelength range selector is provided) is not specified here. These decisions should be made during implementation with reference to the Open Supernova Catalog's spectra viewer as a baseline.

3. **Hosting and deployment:** The static hosting layer (e.g., S3 + CloudFront, Vercel, or similar) is not specified here. This decision should be captured in a separate infrastructure ADR.

4. **Site name and branding:** This document refers to the site as the "Open Nova Catalog." Final naming and any associated logo or wordmark are not addressed here.

---

## Consequences

Adopting this specification implies:

- The frontend is a React/Next.js application deployable as a static site for MVP.
- Plotly.js is the visualization dependency; its bundle size and behavior should be evaluated during implementation.
- All catalog browsing and search occurs client-side in the MVP; this is appropriate for a catalog of ≤50 novae and remains viable well beyond that scale.
- The artifact schema document (`frontend-artifact-schemas.md`) is a required prerequisite for implementation and should be written before frontend development begins.
- Visual design decisions (exact colors, typography choices, spacing) are specified at the direction level here; final values should be established during a brief prototyping phase before full implementation.
