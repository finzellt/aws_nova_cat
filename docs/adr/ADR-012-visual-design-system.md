# ADR-012: Visual Design System

Status: Proposed
Date: 2026-03-16

> **⚠ Amended by ADR-031** (2026-03-31)
> ADR-031 (Data Layer Readiness for Artifact Generation, Decision 11) promotes the
> light curve sparkline column from Post-MVP to MVP. The sparkline generator is fully
> specified in DESIGN-003 §9 and included in Epic 3 scope. The catalog table column
> specification below reflects this change.
>
> See: `docs/adr/ADR-031-data-layer-readiness-for-artifact-generation.md`

---

## Context

ADR-011 defines the frontend technology stack and provides directional guidance on the overall
visual aesthetic, color palette, and typography. This ADR expands those directions into a
complete, implementation-ready visual design system.

The design system defined here governs all visual decisions across the Open Nova Catalog
frontend: color tokens, typography scale, spacing, layout, component-level design patterns,
interactive states, iconography, and accessibility. It is intended to be the single
authoritative reference for frontend visual implementation.

Visualization and plot design (spectra viewer, light curve panel, catalog sparklines) are
intentionally excluded from this ADR and will be addressed in a dedicated subsequent ADR.

---

## Design Philosophy

The Open Nova Catalog serves researchers. Its interface should feel like it was built by
someone who respects both good science and good design — not a generic SaaS product, not a
legacy academic tool.

The guiding aesthetic is **refined utility**: every visual decision should either aid
comprehension or get out of the way. Decoration for its own sake is avoided. Whitespace is
a feature. The interface should communicate quiet confidence — unhurried, clear, and precise.

This philosophy has three practical implications:

1. **Legibility over styling.** Scientific content — names, coordinates, dates, reference
   data — should always be the most readable thing on the page. Typography and contrast are
   the primary design tools, not color or ornamentation.

2. **Restraint in color.** Color is used purposefully: to signal interactivity, indicate
   status, or draw attention to a single key element per context. It is not used decoratively.

3. **Consistency over cleverness.** A predictable, consistent visual language reduces
   cognitive load for researchers who visit frequently. Surprises should come from the data,
   not from the interface.

---

## Scope and Exclusions

This ADR covers:

- Color token system
- Typography scale
- Spacing and layout system
- Component design patterns (buttons, links, badges, tables, navigation, empty states,
  loading states, error states)
- Interactive and focus states
- Iconography
- Catalog table column specification
- Accessibility requirements
- Dark mode token strategy (deferred implementation, designed now)

This ADR does **not** cover:

- Data visualization and plot design (deferred to ADR-013)
- Hosting, deployment, or build tooling
- Artifact schemas or data contracts

---

## Platform Target

The interface is designed primarily for **desktop use** by researchers. The assumed minimum
viewport width is **1024px**. Layouts are not required to be fully functional at mobile
widths in the MVP.

However, layouts should not actively break below 1024px. A graceful degradation floor —
readable content, no overlapping elements, no horizontal overflow — should be maintained
down to approximately **768px** without additional engineering effort.

Full responsive and mobile optimization is explicitly deferred post-MVP.

---

## Color System

### Token Architecture

All colors are implemented as CSS custom properties (design tokens), organized into two
layers:

- **Primitive tokens**: Raw named color values. These are never used directly in component
  styling.
- **Semantic tokens**: Tokens that express intent (e.g., `--color-surface-primary`,
  `--color-interactive`). These map to primitive tokens and are the values used throughout
  the codebase.

This two-layer architecture is what makes dark mode implementable later without touching
component code — only the semantic token mappings change.

### Primitive Color Palette

```css
/* Neutrals — warm stone */
--primitive-stone-50:  #F8F7F4;
--primitive-stone-100: #F0EEE9;
--primitive-stone-200: #E2DED6;
--primitive-stone-300: #C9C3B8;
--primitive-stone-400: #A09890;
--primitive-stone-500: #78726A;
--primitive-stone-600: #5A554F;
--primitive-stone-700: #3D3A35;
--primitive-stone-800: #2A2722;
--primitive-stone-900: #1A1816;

/* Teal — primary accent */
--primitive-teal-100: #D4EEEC;
--primitive-teal-200: #A8DDD9;
--primitive-teal-400: #4FA8A5;
--primitive-teal-500: #2A7D7B;
--primitive-teal-600: #1F5E5C;
--primitive-teal-700: #164342;

/* Status */
--primitive-amber-100: #FEF3C7;
--primitive-amber-500: #D97706;
--primitive-red-100:   #FEE2E2;
--primitive-red-500:   #DC2626;
--primitive-green-100: #DCFCE7;
--primitive-green-500: #16A34A;
```

### Semantic Token Mapping (Light Mode)

```css
:root {
  /* Surfaces */
  --color-surface-primary:   var(--primitive-stone-50);
  --color-surface-secondary: var(--primitive-stone-100);
  --color-surface-tertiary:  var(--primitive-stone-200);

  /* Borders */
  --color-border-subtle:  var(--primitive-stone-200);
  --color-border-default: var(--primitive-stone-300);
  --color-border-strong:  var(--primitive-stone-400);

  /* Text */
  --color-text-primary:   var(--primitive-stone-900);
  --color-text-secondary: var(--primitive-stone-600);
  --color-text-tertiary:  var(--primitive-stone-400);
  --color-text-disabled:  var(--primitive-stone-300);
  --color-text-inverse:   var(--primitive-stone-50);

  /* Interactive */
  --color-interactive:         var(--primitive-teal-500);
  --color-interactive-hover:   var(--primitive-teal-600);
  --color-interactive-active:  var(--primitive-teal-700);
  --color-interactive-subtle:  var(--primitive-teal-100);
  --color-interactive-muted:   var(--primitive-teal-200);
  --color-focus-ring:          var(--primitive-teal-400);

  /* Status */
  --color-status-warning-bg:  var(--primitive-amber-100);
  --color-status-warning-fg:  var(--primitive-amber-500);
  --color-status-error-bg:    var(--primitive-red-100);
  --color-status-error-fg:    var(--primitive-red-500);
  --color-status-success-bg:  var(--primitive-green-100);
  --color-status-success-fg:  var(--primitive-green-500);
}
```

### Dark Mode Token Mapping (Future)

When dark mode is implemented, a `[data-theme="dark"]` selector overrides the semantic
tokens. No dark mode toggle UI is required for MVP. The token architecture simply needs to
be correct from the start so that dark mode can be activated later by applying
`data-theme="dark"` to the root element and respecting `prefers-color-scheme`.

```css
[data-theme="dark"] {
  --color-surface-primary:   var(--primitive-stone-900);
  --color-surface-secondary: var(--primitive-stone-800);
  --color-surface-tertiary:  var(--primitive-stone-700);

  --color-border-subtle:  var(--primitive-stone-700);
  --color-border-default: var(--primitive-stone-600);
  --color-border-strong:  var(--primitive-stone-500);

  --color-text-primary:   var(--primitive-stone-50);
  --color-text-secondary: var(--primitive-stone-300);
  --color-text-tertiary:  var(--primitive-stone-500);
  --color-text-disabled:  var(--primitive-stone-600);
  --color-text-inverse:   var(--primitive-stone-900);

  --color-interactive:         var(--primitive-teal-400);
  --color-interactive-hover:   var(--primitive-teal-200);
  --color-interactive-active:  var(--primitive-teal-100);
  --color-interactive-subtle:  var(--primitive-teal-700);
  --color-interactive-muted:   var(--primitive-teal-600);
  --color-focus-ring:          var(--primitive-teal-400);

  --color-status-warning-bg:  #2D2200;
  --color-status-warning-fg:  #FCD34D;
  --color-status-error-bg:    #2D0000;
  --color-status-error-fg:    #FCA5A5;
  --color-status-success-bg:  #002D0A;
  --color-status-success-fg:  #86EFAC;
}
```

---

## Typography

### Fonts

**Body and UI font: [DM Sans](https://fonts.google.com/specimen/DM+Sans)**

DM Sans is a low-contrast geometric sans-serif designed for interfaces. It is optically
comfortable at small sizes, has a distinctive warmth that suits the off-white background
palette, and conveys quiet precision without feeling corporate.

ADR-011 cited Inter as a candidate. DM Sans is chosen here instead because it has more
character at display sizes (headings, stats bar) while remaining equally legible at body
sizes. Inter is the sensible default; DM Sans is the considered choice.

**Scientific / data font: [DM Mono](https://fonts.google.com/specimen/DM+Mono)**

DM Mono pairs deliberately with DM Sans. It is used for: coordinates (RA/Dec), nova
identifiers, bibcodes, numerical table values, and any data that requires character-level
alignment. The deliberate pairing creates visual coherence between prose UI and data fields.

Both fonts are loaded via Next.js's built-in font optimization (`next/font/google`):

```js
import { DM_Sans, DM_Mono } from 'next/font/google';

const dmSans = DM_Sans({ subsets: ['latin'], variable: '--font-sans' });
const dmMono = DM_Mono({
  subsets: ['latin'],
  variable: '--font-mono',
  weight: ['400', '500']
});
```

### Type Scale

```css
:root {
  --font-sans:  'DM Sans', ui-sans-serif, system-ui, sans-serif;
  --font-mono:  'DM Mono', ui-monospace, monospace;

  --text-xs:   0.75rem;    /* 12px — table labels, bibcodes, fine print */
  --text-sm:   0.875rem;   /* 14px — secondary UI, table body, nav links */
  --text-base: 1rem;       /* 16px — body, primary table content */
  --text-lg:   1.125rem;   /* 18px — section subheadings */
  --text-xl:   1.25rem;    /* 20px — page subheadings */
  --text-2xl:  1.5rem;     /* 24px — page headings */
  --text-3xl:  1.875rem;   /* 30px — hero / nova name display */
  --text-4xl:  2.25rem;    /* 36px — stats bar numerals */

  --font-normal:   400;
  --font-medium:   500;
  --font-semibold: 600;

  --leading-tight:   1.25;
  --leading-snug:    1.375;
  --leading-normal:  1.5;
  --leading-relaxed: 1.625;
}
```

### Usage Rules

| Context | Font | Size | Weight | Leading |
|---|---|---|---|---|
| Page heading (nova name) | Sans | `3xl` | Semibold | Tight |
| Section heading | Sans | `xl` | Semibold | Snug |
| Subsection heading | Sans | `lg` | Medium | Snug |
| Body / prose | Sans | `base` | Normal | Relaxed |
| Table body | Sans | `sm` | Normal | Normal |
| Table header label | Sans | `xs` | Semibold | Normal |
| Table primary name link | Sans | `sm` | Medium | Normal |
| Table alias line | Sans | `xs` | Normal | Snug (italic) |
| Navigation links | Sans | `sm` | Medium | — |
| Button label | Sans | `sm` | Medium | — |
| Coordinates / identifiers | Mono | `sm` | Normal | Normal |
| Bibcodes / catalog IDs | Mono | `xs` | Normal | Normal |
| Stats bar numeral | Sans | `4xl` | Semibold | Tight |
| Stats bar label | Sans | `sm` | Normal | — |

---

## Spacing System

Spacing uses a base-4 scale, consistent with Tailwind's default spacing tokens.

```
4px  — xs   (tight internal padding, icon gap)
8px  — sm   (component internal padding, small gap)
12px — md   (standard internal padding)
16px — lg   (standard component gap, section padding)
24px — xl   (section separation, card padding)
32px — 2xl  (major section breaks)
48px — 3xl  (page section separation)
64px — 4xl  (hero / large layout gaps)
```

### Layout Grid

- **Max content width:** 1280px, centered with auto margins.
- **Page horizontal padding:** 24px at the content edge.
- **Nova page layout:** Two-column on wide viewports — visualization region ~60% width,
  metadata region ~40%. Stacks vertically at ≤ 1024px.
- **Catalog / homepage layout:** Single-column content with max-width constraint.

---

## Component Design Patterns

### Navigation Bar

- Full-width, pinned to the top of the viewport. Height: 56px.
- Background: `--color-surface-secondary`. Bottom border: 1px `--color-border-subtle`.
- Site name / wordmark on the left: semibold, `--color-text-primary`.
- Navigation links on the right: `font-size: sm`, `font-weight: medium`.
  - Rest state: `--color-text-primary` at 70% opacity.
  - Hover state: `--color-text-primary` at full opacity.
  - Active (current page): `--color-interactive`, full opacity.
- No shadow. The bottom border provides sufficient separation.

### Footer

- Background: `--color-surface-secondary`. Top border: 1px `--color-border-subtle`.
- Text: `--color-text-tertiary`. Links: `--color-interactive`. Padding: 32px vertical.

### Buttons

**Primary** — single most important action per context (e.g., "Download bundle").
- Background: `--color-interactive`. Text: `--color-text-inverse`.
- Hover: `--color-interactive-hover`. Border-radius: 6px. Padding: 8px 16px.

**Secondary** — supporting actions.
- Background: transparent. Border: 1px `--color-border-default`.
- Text: `--color-text-primary`. Hover background: `--color-surface-secondary`.

**Ghost** — low-emphasis actions (e.g., "Reset filters").
- No background, no border. Text: `--color-text-secondary`.
- Hover text: `--color-interactive`.

All buttons: `font-size: sm`, `font-weight: medium`, 36px minimum height.
Disabled state: `--color-text-disabled`, `--color-border-subtle`, `cursor: not-allowed`.

### Links

- Inline text links: `--color-interactive`, no underline at rest, underline on hover.
- Visited state: same as rest (visited styling is not useful in a scientific catalog).
- External links: append a `ExternalLink` Lucide icon (16px).

### Badges / Status Chips

- Pill shape: `border-radius: 9999px`. Padding: 2px 8px.
- Font: `xs`, medium weight.
- Color pairings: semantic status tokens (`--color-status-*-bg` / `--color-status-*-fg`).
- Neutral badge: `--color-surface-tertiary` background, `--color-text-secondary` text.

### Tables

**Catalog table:**
- Header: `--color-surface-secondary` background, `--color-text-secondary` label text,
  `xs` / semibold. Sortable headers show sort direction icon; active sort column uses
  `--color-text-primary`.
- Body rows: alternating `--color-surface-primary` / `--color-surface-secondary` striping.
- Row hover: `--color-interactive-subtle` background.
- Row height: 48px (increases naturally when alias line is present).
- Cell padding: 12px horizontal.
- Row separator: 1px `--color-border-subtle` bottom border.
- **No vertical column dividers.** Column alignment and header labels provide sufficient
  separation; vertical rules add visual weight without aiding scannability.
- Numerical and coordinate cells: right-aligned, DM Mono.
- Text cells: left-aligned.

**Name cell treatment:**
- Primary name: teal link, `--color-interactive`, `font-weight: medium`.
- Aliases (up to 2): second line within the same cell, `font-size: xs`, italic,
  `--color-text-tertiary`. Additional aliases are visible on the nova page.

**Metadata tables** (observations summary, references on nova page):
- No row striping. Row height: 40px. Otherwise same as catalog table.

**Pagination controls:**
- Below the table, right-aligned. 25 rows per page.
- Page number buttons: ghost at rest, secondary for current page.
- Previous / Next: `ChevronLeft` / `ChevronRight` icon buttons with `aria-label`.

### Cards

- Background: `--color-surface-secondary`. Border: 1px `--color-border-subtle`.
- Border-radius: 8px. Padding: 24px. No shadow.
- Used sparingly — primarily for stats bar items on the homepage.

### Empty States

- Centered within region. Single Lucide icon (32px) in `--color-text-tertiary`.
- Heading: `--color-text-secondary`, `base`, medium.
- Optional explanation line: `--color-text-tertiary`, `sm`, normal.
- No decorative imagery.

### Loading States

- Skeleton loader: rectangular placeholder blocks in `--color-surface-tertiary` with a
  shimmer animation. Block shapes approximate the content they replace.
- No spinner icons.

### Error States

- Inline message in `--color-status-error-fg` with a `CircleAlert` icon.
- "Try again" ghost button where retry is meaningful.
- Partial failures are scoped to their region — they do not affect the rest of the page.

---

## Interactive and Focus States

- **Focus ring:** 2px solid `--color-focus-ring`, 2px offset from element boundary.
- `:focus-visible` suppresses focus rings for mouse interaction while preserving them for
  keyboard navigation. Global `outline: none` without a replacement is not permitted.
- Hover states must be visually distinct from rest state on all interactive elements.

---

## Iconography

**Icon library: [Lucide](https://lucide.dev/)** via `lucide-react`.

Chosen for its clean 2px-stroke style, extensive functional coverage, and first-class React
integration. Icons are used functionally only — never decoratively.

| Use case | Icon |
|---|---|
| External link | `ExternalLink` |
| Navigate back | `ChevronLeft` |
| Sort ascending | `ChevronUp` |
| Sort descending | `ChevronDown` |
| Sort inactive | `ChevronsUpDown` |
| Pagination previous | `ChevronLeft` |
| Pagination next | `ChevronRight` |
| Download | `Download` |
| Copy to clipboard | `Copy` |
| Copy confirmed | `Check` |
| Warning | `TriangleAlert` |
| Error | `CircleAlert` |
| Success | `CircleCheck` |
| Search | `Search` |
| Empty state — no spectra | `LineChart` |
| Empty state — no photometry | `Activity` |
| Empty state — no references | `BookOpen` |

Default inline size: 16px. Empty state icon: 32px. Icons must always be paired with a text
label or tooltip, except pagination chevrons which are conventional enough to stand alone.

---

## Catalog Table Column Specification

This section is the authoritative reference for catalog table columns. MVP columns are
present at launch. Post-MVP columns are specced here so the data model can anticipate them,
but their UI is not implemented until the relevant data pipeline is ready.

### MVP Columns

| Column | Type | Notes |
|---|---|---|
| Primary name | Link + alias line | Teal link, `font-weight: medium`. Up to 2 aliases on a second line: `xs`, italic, `--color-text-tertiary`. |
| RA / Dec | Text | Standard astronomical format (HH:MM:SS / ±DD:MM:SS). DM Mono, right-aligned. Selectable text. |
| Discovery year | Text | Four-digit year, right-aligned. Displayed as-is when only year precision is available. |
| Spectra | Count | Integer count of validated spectra. Default sort key (descending). Right-aligned, tabular figures. |
| Photometry | Count | Integer count of photometric observations. Displays `—` when none available. Right-aligned. |
| References | Count | Integer count of associated literature references. Right-aligned. |
| Light curve sparkline | SVG thumbnail | Inline pre-rendered SVG of the optical light curve (90×55px). Displays `—` placeholder when no photometry is available. Visual spec in ADR-013; generation spec in DESIGN-003 §9. |

**Default sort:** Descending by spectra count.
**Pagination:** 25 rows per page.
**Client-side search:** Filters by primary name or alias.

### Post-MVP Columns

| Column | Notes |
|---|---|
| Wavelength coverage | Compact icon strip indicating data availability across regimes: Radio, UV, Optical, X-ray, Gamma. Each regime shown as a small colored indicator — present or absent. Signals data richness at a glance. |
| Recurrent flag | Boolean badge. Indicates the nova has been observed to erupt more than once. Under consideration for the catalog table given its scientific significance. |
| Extragalactic flag | Boolean badge. Indicates the nova is located outside the Milky Way. Under consideration alongside the recurrent flag. |

### Deferred to Nova Page

The following were considered for catalog table columns but belong in the object summary
region of individual nova pages instead:

- Peak magnitude
- Spectroscopic class
- t₂ decay time
- Distance estimate
- Reddening E(B−V)

These fields involve continuous values with measurement uncertainties and often multiple
published estimates. Presenting them as sortable table columns implies a precision and
consensus that may not exist.

**Note on data sourcing:** Physical parameters (t₂, distance, reddening, peak magnitude)
are not automatically ingested. They require either manual curation or a reliable automated
sourcing mechanism before they can appear on nova pages. This is an open problem noted here
for future implementation planning.

---

## Accessibility

The interface must meet **WCAG 2.1 Level AA** as a baseline.

### Contrast

- Normal text (< 18px regular, < 14px bold): minimum 4.5:1 against background.
- Large text (≥ 18px regular, ≥ 14px bold): minimum 3:1.
- UI components and graphical elements: minimum 3:1 against adjacent colors.

Contrast must be re-verified when dark mode tokens are finalized.

### Keyboard Navigation

- All interactive elements reachable and operable via keyboard.
- Tab order follows logical reading order. Focus rings always visible.
- Catalog table: arrow keys navigate cells; Enter follows nova link; sortable headers
  are keyboard-activatable.

### Semantics

- Landmark elements: `<header>`, `<nav>`, `<main>`, `<footer>`.
- Tables: `<thead>`, `<tbody>`, `<th scope>` markup.
- Sortable column headers: `aria-sort` attribute.
- Status badges: `aria-label` when color alone conveys meaning.
- Icon-only interactive elements: `aria-label`.
- Empty state containers: `aria-label`.
- Page title updates on navigation to reflect current nova name or page section.

---

## Implementation Notes

- All design tokens live in a single `:root` block in `styles/tokens.css`.
- Token values are mapped into `tailwind.config.js` so Tailwind utilities reference
  semantic tokens rather than raw hex values.
- Component-level styles that cannot be expressed as Tailwind utilities use CSS Modules.

---

## Open Questions

1. **Exact primitive hex values:** The palette defined here is directional. Final values
   should be validated against WCAG contrast ratios during a prototyping pass before
   implementation begins.

2. **Dark mode trigger mechanism:** Whether to follow `prefers-color-scheme` automatically,
   provide a manual toggle, or both is not resolved here.

3. **Site name and wordmark:** Final naming and typographic treatment are not determined
   here (see ADR-011 open questions).

4. **Recurrent and extragalactic flags in catalog table:** Inclusion should be decided when
   the data pipeline for these fields is ready.

5. **Physical parameter sourcing:** A sourcing strategy for derived nova parameters must be
   defined before these fields can appear on nova pages.

6. **Alias display cap:** The 2-alias cap in the catalog table name cell may need adjustment
   based on real catalog data. Some novae have many aliases, and 2 may be insufficient for
   lookup purposes.

---

## Consequences

- All color usage is mediated through semantic design tokens. Raw hex values do not appear
  in component code.
- DM Sans and DM Mono are the only fonts used across the interface.
- Lucide is the only icon library dependency.
- Dark mode requires no component-level changes when implemented — only a token mapping
  update.
- WCAG 2.1 AA is the accessibility baseline, imposing contrast verification obligations
  during prototyping.
- The catalog table column specification defined here governs both MVP implementation and
  post-MVP data model planning. Deviations require a superseding ADR or amendment.
- Visualization and plot styling are explicitly out of scope and governed by ADR-013.
