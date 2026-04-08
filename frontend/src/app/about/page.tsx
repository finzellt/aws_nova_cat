/**
 * About page — /about
 *
 * Dual-audience page: astronomers learn what the catalog offers and how
 * to cite it; engineers and recruiters see the depth of the system design.
 * One narrative serves both — the same facts read differently depending
 * on who you are.
 *
 * Server Component. No client-side interactivity required.
 */

import Link from 'next/link';

export const metadata = {
  title: 'About — Open Nova Catalog',
  description:
    'What the Open Nova Catalog is, how it works, and what building it required.',
};

// ── Section wrapper ──────────────────────────────────────────────────────────

function Section({
  heading,
  children,
}: {
  heading: string;
  children: React.ReactNode;
}) {
  return (
    <section>
      <h2 className="text-xl font-semibold text-text-primary mb-4">
        {heading}
      </h2>
      {children}
    </section>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────────

export default function AboutPage() {
  return (
    <div className="py-12 flex flex-col gap-12 max-w-3xl">

      {/* ── Header ─────────────────────────────────────────────────── */}
      <div>
        <h1 className="text-2xl font-semibold text-text-primary mb-4">
          About
        </h1>
        <p className="text-base leading-relaxed text-text-secondary">
          The Open Nova Catalog is a curated, publicly accessible repository of
          observational data for classical novae. It aggregates spectroscopic and
          photometric records from public astronomical archives and published
          literature, structures them against international standards, and
          delivers them through an interactive web interface and downloadable
          research-grade data bundles. The project draws inspiration from the
          Open Supernova Catalog (Guillochon et al. 2017, ApJ 835:64).
        </p>
      </div>

      {/* ── Why it exists ──────────────────────────────────────────── */}
      <Section heading="Why it exists">
        <div className="flex flex-col gap-3 text-base leading-relaxed text-text-secondary">
          <p>
            Nova observational data is scattered across dozens of archives, each
            with its own formats, naming conventions, and access patterns.
            Assembling a multi-wavelength picture of a single nova — spectra from
            ESO, photometry from VizieR, references from ADS — requires manual
            work that most researchers repeat independently.
          </p>
          <p>
            The Open Nova Catalog consolidates this work once. It resolves nova
            identity across archives, normalizes observations against{' '}
            <a
              href="https://www.ivoa.net/documents/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-interactive no-underline hover:underline"
            >
              IVOA
            </a>
            {' '}(International Virtual Observatory Alliance)
            standards, validates data quality, and publishes curated datasets
            that are immediately usable for research. The goal is to make the
            observational record of every known classical nova discoverable,
            inspectable, and downloadable from a single location.
          </p>
        </div>
      </Section>

      {/* ── What you can do ────────────────────────────────────────── */}
      <Section heading="What you can do here">
        <div className="flex flex-col gap-3 text-base leading-relaxed text-text-secondary">
          <p>
            <span className="text-text-primary font-medium">Browse the catalog</span>{' '}
            — a sortable, searchable table of all novae with observational
            coverage at a glance. Each entry links to a dedicated nova page.
          </p>
          <p>
            <span className="text-text-primary font-medium">Inspect observations visually</span>{' '}
            — interactive spectra waterfall plots with epoch controls, spectral
            feature markers (Fe II, He/N, nebular), and single-spectrum isolation
            mode. Multi-regime light curves with per-band toggles, error bars,
            and upper limit markers.
          </p>
          <p>
            <span className="text-text-primary font-medium">Download data bundles</span>{' '}
            — per-nova ZIP archives containing IVOA-compliant FITS spectra, a
            consolidated photometry table, provenance records, and BibTeX
            references. Ready for direct use in analysis pipelines.
          </p>
          <p>
            For details on data formats and the decisions behind them, see
            the{' '}
            <Link
              href="/docs"
              className="text-interactive no-underline hover:underline"
            >
              Documentation
            </Link>
            .
          </p>
        </div>
      </Section>

      {/* ── How it's built ─────────────────────────────────────────── */}
      <Section heading="How it&apos;s built">
        <div className="flex flex-col gap-3 text-base leading-relaxed text-text-secondary">
          <p>
            The catalog is a complete data platform — not just a website or a
            service, but an end-to-end system spanning data ingestion,
            persistence, artifact generation, and interactive frontend delivery.
            Architecture, infrastructure, backend services, data modeling,
            frontend, and documentation are all the work of a single developer
            with domain expertise in nova astronomy.
          </p>
          <p>
            <span className="text-text-primary font-medium">Ingestion pipeline.</span>{' '}
            Seven Step Functions workflows orchestrate 17 Lambda functions (13
            zip-bundled, 4 container-based) to discover spectra from provider
            archives, acquire and fingerprint raw FITS files, validate them
            against instrument-specific profiles, resolve nova identity via
            coordinate-based deduplication, and persist normalized products with
            full provenance. A parallel ticket-driven path ingests photometry and
            spectra from hand-curated metadata files that completely describe
            each data file&apos;s structure.
          </p>
          <p>
            <span className="text-text-primary font-medium">Artifact regeneration.</span>{' '}
            A Fargate task (2 vCPU / 8 GB) transforms internal DynamoDB and S3
            state into seven published artifact types per nova — metadata JSON,
            references, plot-ready spectra and photometry, SVG sparklines, and
            research-grade data bundles. Artifacts are published through an
            immutable release model: all artifacts for a release are written to
            S3 before an atomic pointer update makes them visible. Rollback is a
            single JSON write.
          </p>
          <p>
            <span className="text-text-primary font-medium">Contract-first architecture.</span>{' '}
            Pydantic models define every inter-service boundary. JSON Schemas are
            auto-generated from contracts. Services are developed and tested
            against typed interfaces, not ad hoc payloads. Strict mypy checking
            enforces type safety across all service code.
          </p>
          <p>
            <span className="text-text-primary font-medium">Frontend.</span>{' '}
            A React/Next.js application consuming pre-built static JSON
            artifacts with zero runtime backend calls. Interactive Plotly.js
            visualizations for spectra (waterfall plots) and photometry
            (multi-regime tabbed light curves). A two-layer CSS design token
            system enabling dark mode without touching component code.
          </p>
          <p>
            <span className="text-text-primary font-medium">Documentation discipline.</span>{' '}
            Over 30 architectural decision records, 4 pre-ADR design documents,
            per-workflow operational docs, and formal schema specifications —
            written before implementation, not after. The ADR corpus governs
            everything from coordinate deduplication thresholds to photometry
            visualization algorithms.
          </p>
        </div>
      </Section>

      {/* ── By the numbers ─────────────────────────────────────────── */}
      <Section heading="By the numbers">
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
          {[
            { value: '313', label: 'Commits' },
            { value: '17', label: 'Lambda functions' },
            { value: '7', label: 'Step Functions workflows' },
            { value: '30+', label: 'Architecture decision records' },
            { value: '2', label: 'DynamoDB tables' },
            { value: '1', label: 'Fargate task definition' },
          ].map(({ value, label }) => (
            <div
              key={label}
              className="bg-surface-secondary border border-border-subtle rounded-lg p-4"
            >
              <div className="text-2xl font-semibold text-text-primary">
                {value}
              </div>
              <div className="text-sm text-text-secondary mt-1">{label}</div>
            </div>
          ))}
        </div>
      </Section>

      {/* ── Data sources ───────────────────────────────────────────── */}
      <Section heading="Data sources">
        <p className="text-base leading-relaxed text-text-secondary">
          The catalog currently draws from the{' '}
          <a
            href="https://archive.eso.org/scienceportal/home"
            target="_blank"
            rel="noopener noreferrer"
            className="text-interactive no-underline hover:underline"
          >
            European Southern Observatory (ESO) Science Archive
          </a>
          , the Center for Astrophysics | Harvard &
          Smithsonian (CfA) spectral archive,{' '}
          <a
            href="https://vizier.cds.unistra.fr/viz-bin/VizieR"
            target="_blank"
            rel="noopener noreferrer"
            className="text-interactive no-underline hover:underline"
          >
            VizieR
          </a>
          ,{' '}
          <a
            href="https://ui.adsabs.harvard.edu/"
            target="_blank"
            rel="noopener noreferrer"
            className="text-interactive no-underline hover:underline"
          >
            NASA&apos;s Astrophysics Data System (ADS)
          </a>
          {' '}for literature references, and SIMBAD/TNS for identity
          resolution. Additional archives will be integrated as the catalog
          grows.
        </p>
      </Section>

      {/* ── Citation guidance ──────────────────────────────────────── */}
      <Section heading="Citation">
        <div className="flex flex-col gap-3 text-base leading-relaxed text-text-secondary">
          <p>
            If you use data from the Open Nova Catalog in a publication, please
            cite the original data sources listed in the BibTeX file included
            with each data bundle. We also ask that you cite the Open Nova
            Catalog itself. A formal citation record will be published as the
            catalog matures; in the interim, a reference to the project URL and
            the data bundle generation date is sufficient.
          </p>
        </div>
      </Section>

      {/* ── Links ──────────────────────────────────────────────────── */}
      <Section heading="Links">
        <div className="flex flex-col gap-2 text-base text-text-secondary">
          <p>
            <a
              href="https://github.com/YOUR_USERNAME/nova-cat"
              target="_blank"
              rel="noopener noreferrer"
              className="text-interactive no-underline hover:underline"
            >
              GitHub repository ↗
            </a>
            {' '}— source code, infrastructure, documentation, and issue tracker.
          </p>
          <p>
            <Link
              href="/docs"
              className="text-interactive no-underline hover:underline"
            >
              Documentation
            </Link>
            {' '}— data products, formats, and design decisions.
          </p>
        </div>
      </Section>

    </div>
  );
}
