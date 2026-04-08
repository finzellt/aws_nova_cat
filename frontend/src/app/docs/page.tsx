/**
 * Documentation page — /docs
 *
 * Three sections:
 *   1. Understanding the Catalog — what the numbers mean, identity model,
 *      discovery dates, known limitations.
 *   2. Data Products — what you get on the website and in bundles.
 *   3. Design Decisions — the interesting engineering and scientific choices.
 *
 * Server Component. Content is static prose — no data fetching needed.
 */

import Link from 'next/link';

export const metadata = {
  title: 'Documentation — Open Nova Catalog',
  description:
    'Data products, catalog semantics, and design decisions behind the Open Nova Catalog.',
};

// ── Reusable components ──────────────────────────────────────────────────────

function Section({
  id,
  heading,
  children,
}: {
  id: string;
  heading: string;
  children: React.ReactNode;
}) {
  return (
    <section id={id}>
      <h2 className="text-xl font-semibold text-text-primary mb-4">
        {heading}
      </h2>
      {children}
    </section>
  );
}

function Subsection({
  id,
  heading,
  children,
}: {
  id: string;
  heading: string;
  children: React.ReactNode;
}) {
  return (
    <div id={id} className="flex flex-col gap-3">
      <h3 className="text-base font-semibold text-text-primary">{heading}</h3>
      {children}
    </div>
  );
}

function Prose({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-3 text-base leading-relaxed text-text-secondary">
      {children}
    </div>
  );
}

// ── Table of contents ────────────────────────────────────────────────────────

const TOC = [
  {
    label: 'Understanding the Catalog',
    href: '#understanding',
    children: [
      { label: 'What the spectra count means', href: '#spectra-count' },
      { label: 'Nova identity', href: '#identity' },
      { label: 'Discovery dates', href: '#discovery-dates' },
      { label: 'Recurrent novae', href: '#recurrent-novae' },
    ],
  },
  {
    label: 'Data Products',
    href: '#data-products',
    children: [
      { label: 'Published artifacts', href: '#artifacts' },
      { label: 'Data bundles', href: '#bundles' },
      { label: 'FITS files', href: '#fits' },
    ],
  },
  {
    label: 'Design Decisions',
    href: '#design-decisions',
    children: [
      { label: 'Coordinate-based deduplication', href: '#dedup' },
      { label: 'SHA-256 fingerprinting', href: '#fingerprinting' },
      { label: 'Profile-driven FITS validation', href: '#profile-validation' },
      { label: 'Quarantine semantics', href: '#quarantine' },
      { label: 'Multi-arm spectra merging', href: '#spectra-merging' },
      { label: 'Same-night display deduplication', href: '#same-night' },
      { label: 'Large photometry errors as upper limits', href: '#large-errors' },
      { label: 'Upper limit suppression', href: '#upper-limit-suppression' },
      { label: 'Downsampling algorithms', href: '#downsampling' },
      { label: 'Photometry band offsets', href: '#band-offsets' },
      { label: 'Immutable release model', href: '#release-model' },
    ],
  },
];

// ── Page ─────────────────────────────────────────────────────────────────────

export default function DocsPage() {
  return (
    <div className="py-12 flex flex-col gap-12 max-w-3xl">

      {/* ── Header ─────────────────────────────────────────────────── */}
      <div>
        <h1 className="text-2xl font-semibold text-text-primary mb-4">
          Documentation
        </h1>
        <p className="text-base leading-relaxed text-text-secondary">
          How the catalog works, what the data products contain, and the design
          decisions behind them. For a high-level overview of the project, see
          the{' '}
          <Link
            href="/about"
            className="text-interactive no-underline hover:underline"
          >
            About
          </Link>{' '}
          page.
        </p>
      </div>

      {/* ── Table of contents ──────────────────────────────────────── */}
      <nav
        aria-label="Documentation table of contents"
        className="bg-surface-secondary border border-border-subtle rounded-lg p-6"
      >
        <p className="text-sm font-semibold text-text-primary mb-3">
          Contents
        </p>
        <ol className="flex flex-col gap-2 text-sm">
          {TOC.map((section) => (
            <li key={section.href}>
              <a
                href={section.href}
                className="text-interactive no-underline hover:underline font-medium"
              >
                {section.label}
              </a>
              {section.children.length > 0 && (
                <ol className="mt-1 ml-4 flex flex-col gap-1">
                  {section.children.map((child) => (
                    <li key={child.href}>
                      <a
                        href={child.href}
                        className="text-interactive no-underline hover:underline"
                      >
                        {child.label}
                      </a>
                    </li>
                  ))}
                </ol>
              )}
            </li>
          ))}
        </ol>
      </nav>

      {/* ================================================================ */}
      {/* SECTION 1 — Understanding the Catalog                            */}
      {/* ================================================================ */}

      <Section id="understanding" heading="Understanding the Catalog">
        <Prose>
          <p>
            The Open Nova Catalog aggregates spectroscopic and photometric
            observations of classical novae from public astronomical archives
            ({' '}
            <a
              href="https://archive.eso.org/scienceportal/home"
              target="_blank"
              rel="noopener noreferrer"
              className="text-interactive no-underline hover:underline"
            >
              ESO
            </a>
            , CfA,{' '}
            <a
              href="https://vizier.cds.unistra.fr/viz-bin/VizieR"
              target="_blank"
              rel="noopener noreferrer"
              className="text-interactive no-underline hover:underline"
            >
              VizieR
            </a>
            ) and published literature. Each nova in the
            catalog has a dedicated page showing its observational history,
            interactive visualizations, and a downloadable data bundle.
          </p>
          <p>
            A few things are worth understanding about how the catalog
            represents its data.
          </p>
        </Prose>
      </Section>

      {/* ── Spectra count ──────────────────────────────────────────── */}
      <Subsection id="spectra-count" heading="What the spectra count means">
        <Prose>
          <p>
            The spectra count displayed on the catalog table and nova pages is
            the total number of validated spectra files in the catalog for that
            nova — not the number of unique observing nights. A single night of
            observation can produce multiple spectra files: different wavelength
            arms from the same instrument (e.g., X-SHOOTER&apos;s UVB, VIS, and
            NIR arms), separate grating settings, or observations taken at
            different times during the night.
          </p>
          <p>
            When displaying spectra in the waterfall plot, same-night
            observations from the same instrument are merged for visual clarity
            (see{' '}
            <a
              href="#spectra-merging"
              className="text-interactive no-underline hover:underline"
            >
              multi-arm spectra merging
            </a>
            ), so the number of traces in the plot may be smaller than the
            spectra count. The data bundle always contains every individual{' '}
            <strong>FITS</strong> (Flexible Image Transport System — the standard
            binary format for astronomical data) file.
          </p>
        </Prose>
      </Subsection>

      {/* ── Identity ───────────────────────────────────────────────── */}
      <Subsection id="identity" heading="Nova identity">
        <Prose>
          <p>
            Novae are known by many names across archives — a single object
            might appear as V1324 Sco, Nova Sco 2012, and PNV
            J17175531-3214245 depending on who reported it. The catalog resolves
            these into a single stable identity using coordinate-based
            deduplication (see{' '}
            <a
              href="#dedup"
              className="text-interactive no-underline hover:underline"
            >
              design decisions
            </a>
            ). All known aliases are listed on the nova page, and the catalog
            search matches against all of them.
          </p>
          <p>
            Internally, every nova is identified by a UUID. Names are resolved
            once during ingestion; all downstream operations — data
            persistence, artifact generation, frontend routing — use the UUID
            exclusively. This means renaming a nova or adding new aliases never
            breaks references to its data.
          </p>
        </Prose>
      </Subsection>

      {/* ── Discovery dates ────────────────────────────────────────── */}
      <Subsection id="discovery-dates" heading="Discovery dates">
        <Prose>
          <p>
            Discovery dates are derived from{' '}
            <a
              href="https://ui.adsabs.harvard.edu/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-interactive no-underline hover:underline"
            >
              ADS
            </a>
            {' '}(NASA&apos;s Astrophysics Data
            System). The catalog queries ADS for all references associated with
            a nova and selects the earliest publication date as the discovery
            date. Because ADS publication dates typically resolve to the month
            rather than the calendar date, most discovery dates in the catalog
            have month-level precision only. When the exact day is unavailable,
            it is stored explicitly as unknown rather than silently defaulted
            to the first of the month.
          </p>
          <p>
            The spectra and photometry viewers can display observation epochs as
            {' '}<strong>DPO</strong> (days post-outburst). When the discovery date has
            only month precision, the DPO axis uses the first of the month as
            the reference point. The outburst date is an approximation in either
            case — the actual outburst may precede the earliest published report
            by days or weeks.
          </p>
        </Prose>
      </Subsection>

      {/* ── Recurrent novae ────────────────────────────────────────── */}
      <Subsection id="recurrent-novae" heading="Recurrent novae">
        <Prose>
          <p>
            The catalog does not yet handle recurrent novae as a distinct class.
            A recurrent nova like RS Oph has multiple outbursts separated by
            years or decades, and ideally each outburst would be treated as an
            independent event with its own DPO axis, light curve, and data
            bundle. The current infrastructure treats each nova as a single
            entity with one discovery date, which works well for classical novae
            but conflates multiple outbursts for recurrents.
          </p>
          <p>
            For now, recurrent novae fall back to using the earliest observation
            in the catalog as the DPO reference point rather than the historical
            discovery date (which may refer to an outburst centuries ago). Full
            outburst segmentation — per-outburst identity, visualization, and
            data packaging — is planned but requires dedicated design work.
          </p>
        </Prose>
      </Subsection>

      {/* ── Divider ────────────────────────────────────────────────── */}
      <hr className="border-border-subtle" />

      {/* ================================================================ */}
      {/* SECTION 2 — Data Products                                        */}
      {/* ================================================================ */}

      <Section id="data-products" heading="Data Products">
        <Prose>
          <p>
            The catalog publishes two kinds of data products: frontend-ready
            artifacts that power the website, and research-grade data bundles
            for download. These are distinct products optimized for different
            purposes — the website shows pre-processed, subsampled, normalized
            data for interactive visualization, while the bundles contain
            original-resolution FITS files and full datasets.
          </p>
        </Prose>
      </Section>

      {/* ── Published artifacts ─────────────────────────────────────── */}
      <Subsection id="artifacts" heading="Published artifacts">
        <Prose>
          <p>
            Each nova in the catalog has up to seven published artifacts,
            generated by a Fargate-based artifact regeneration pipeline and
            served to browsers via S3 and CloudFront:
          </p>
        </Prose>
        <div className="overflow-x-auto mt-2">
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="bg-surface-secondary text-text-secondary text-left">
                <th className="px-3 py-2 font-semibold">Artifact</th>
                <th className="px-3 py-2 font-semibold">Contents</th>
              </tr>
            </thead>
            <tbody className="text-text-secondary">
              {[
                ['nova.json', 'Core metadata: name, aliases, coordinates, discovery date, observation counts.'],
                ['references.json', 'ADS literature references with bibcodes, titles, authors, and publication dates.'],
                ['spectra.json', 'Plot-ready spectra: wavelength/flux arrays, peak-flux normalized, with epoch and instrument metadata.'],
                ['photometry.json', 'Multi-regime photometry: per-band observations with magnitudes or flux densities, error bars, and upper limit flags.'],
                ['sparkline.svg', 'A 90×55px inline light curve for the catalog table.'],
                ['bundle.zip', 'Research-grade data bundle (see below).'],
                ['catalog.json', 'Global catalog summary: one entry per nova, aggregate statistics. Powers the homepage and catalog table.'],
              ].map(([artifact, desc]) => (
                <tr key={artifact} className="border-t border-border-subtle">
                  <td className="px-3 py-2 font-mono text-text-primary whitespace-nowrap">
                    {artifact}
                  </td>
                  <td className="px-3 py-2">{desc}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <Prose>
          <p>
            All scientific computation — normalization, subsampling, offset
            calculation, outburst date resolution — happens at artifact
            generation time. The frontend performs zero scientific computation
            and makes zero backend API calls.
          </p>
        </Prose>
      </Subsection>

      {/* ── Bundles ────────────────────────────────────────────────── */}
      <Subsection id="bundles" heading="Data bundles">
        <Prose>
          <p>
            Each nova has a downloadable ZIP bundle containing
            research-grade data:
          </p>
        </Prose>
        <div className="overflow-x-auto mt-2">
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="bg-surface-secondary text-text-secondary text-left">
                <th className="px-3 py-2 font-semibold">File</th>
                <th className="px-3 py-2 font-semibold">Contents</th>
              </tr>
            </thead>
            <tbody className="text-text-secondary">
              {[
                ['README.txt', 'Nova identity, bundle inventory, format descriptions, and citation guidance.'],
                ['<nova>_metadata.json', 'Nova properties: name, aliases, coordinates, discovery date.'],
                ['<nova>_sources.json', 'Provenance records: provider, archive, original identifiers, retrieval date.'],
                ['<nova>_references.bib', 'BibTeX file of all associated literature references.'],
                ['spectra/*.fits', 'Individual FITS spectra in IVOA Spectrum DM v1.2 format, original (non-normalized) flux units.'],
                ['<nova>_photometry.fits', 'Consolidated photometry BINTABLE: time, band, magnitude/flux, errors, upper limits, provenance.'],
              ].map(([file, desc]) => (
                <tr key={file} className="border-t border-border-subtle">
                  <td className="px-3 py-2 font-mono text-text-primary whitespace-nowrap">
                    {file}
                  </td>
                  <td className="px-3 py-2">{desc}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <Prose>
          <p>
            Bundle filenames follow a deterministic convention: FITS files are
            named{' '}
            <span className="font-mono text-sm text-text-primary">
              &lt;nova&gt;_spectrum_&lt;provider&gt;_&lt;telescope&gt;_&lt;instrument&gt;_&lt;epoch_mjd&gt;.fits
            </span>
            , where each segment is always present (unknown values use the
            explicit sentinel &ldquo;unknown&rdquo;) and the fractional{' '}
            <strong>MJD</strong> (Modified Julian Date — a standard astronomical
            time system) provides sub-night temporal uniqueness.
          </p>
        </Prose>
      </Subsection>

      {/* ── FITS ───────────────────────────────────────────────────── */}
      <Subsection id="fits" heading="FITS files">
        <Prose>
          <p>
            Spectrum FITS files conform to the{' '}
            <a
              href="https://www.ivoa.net/documents/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-interactive no-underline hover:underline"
            >
              <strong>IVOA</strong>
            </a>
            {' '}(International Virtual Observatory Alliance) Spectrum Data Model v1.2.
            Each file contains a BINTABLE extension with WAVELENGTH (nm) and
            FLUX columns in the original flux units reported by the instrument
            pipeline. Instrument, telescope, epoch, and provider metadata are
            recorded in the FITS header using standard IVOA keywords.
          </p>
          <p>
            Photometry FITS files conform to IVOA PhotDM 1.1 and contain a
            BINTABLE with columns for time (MJD), band identification,
            magnitude or flux density, uncertainties, upper limit flags, and
            provenance fields.
          </p>
        </Prose>
      </Subsection>

      {/* ── Divider ────────────────────────────────────────────────── */}
      <hr className="border-border-subtle" />

      {/* ================================================================ */}
      {/* SECTION 3 — Design Decisions                                     */}
      {/* ================================================================ */}

      <Section id="design-decisions" heading="Design Decisions">
        <Prose>
          <p>
            The catalog makes a number of deliberate choices about how data is
            ingested, validated, transformed, and displayed. These decisions are
            documented here both to help researchers understand and trust the
            data they download, and to illustrate the engineering reasoning
            behind the system. Each decision is backed by a formal architectural
            decision record (ADR) in the project&apos;s documentation corpus.
          </p>
        </Prose>
      </Section>

      {/* ── Coordinate dedup ───────────────────────────────────────── */}
      <Subsection id="dedup" heading="Coordinate-based deduplication">
        <Prose>
          <p>
            When a new nova name is encountered during ingestion, the system
            queries SIMBAD and TNS for its coordinates, then compares against
            all existing novae in the catalog using angular separation
            thresholds:
          </p>
        </Prose>
        <div className="overflow-x-auto mt-2">
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="bg-surface-secondary text-text-secondary text-left">
                <th className="px-3 py-2 font-semibold">Separation</th>
                <th className="px-3 py-2 font-semibold">Action</th>
              </tr>
            </thead>
            <tbody className="text-text-secondary">
              <tr className="border-t border-border-subtle">
                <td className="px-3 py-2 font-mono text-text-primary">&lt; 2″</td>
                <td className="px-3 py-2">Attach as alias to existing nova</td>
              </tr>
              <tr className="border-t border-border-subtle">
                <td className="px-3 py-2 font-mono text-text-primary">2–10″</td>
                <td className="px-3 py-2">Identity quarantine — too close to be independent, too far for confident alias</td>
              </tr>
              <tr className="border-t border-border-subtle">
                <td className="px-3 py-2 font-mono text-text-primary">&gt; 10″</td>
                <td className="px-3 py-2">Create new nova</td>
              </tr>
            </tbody>
          </table>
        </div>
        <Prose>
          <p>
            The 2-arcsecond threshold reflects the typical astrometric
            precision of discovery reports. The quarantine zone between 2 and 10
            arcseconds captures ambiguous cases for operator review rather than
            making a silent wrong decision. This is an instance of the
            catalog&apos;s general{' '}
            <a
              href="#quarantine"
              className="text-interactive no-underline hover:underline"
            >
              quarantine philosophy
            </a>
            .
          </p>
        </Prose>
      </Subsection>

      {/* ── SHA-256 fingerprinting ─────────────────────────────────── */}
      <Subsection id="fingerprinting" heading="SHA-256 fingerprinting">
        <Prose>
          <p>
            Every spectrum acquired from a provider archive is fingerprinted
            with a SHA-256 hash of its raw bytes at acquisition time. After
            validation, this fingerprint is checked against all existing
            validated spectra for the same nova. If a byte-level match is
            found, the new product is marked as a duplicate of the canonical
            product and is not counted as a separate spectrum.
          </p>
          <p>
            This catches the case where different archives — or different
            access paths within the same archive — expose the same underlying
            data product under different identifiers. Discovery-time metadata
            deduplication catches most of these, but SHA-256 provides a
            definitive second layer that operates on actual content rather than
            metadata.
          </p>
        </Prose>
      </Subsection>

      {/* ── Profile-driven validation ──────────────────────────────── */}
      <Subsection
        id="profile-validation"
        heading="Profile-driven FITS validation"
      >
        <Prose>
          <p>
            FITS files are not taken at face value. Each spectrum is validated
            against a per-instrument profile that defines expected wavelength
            ranges, flux units, header keywords, and normalization rules. The
            profile is selected automatically based on the data provider and
            header signature fields (INSTRUME, TELESCOP).
          </p>
          <p>
            Validation includes structural checks (spectral axis monotonicity,
            non-empty flux arrays, finite values, plausible wavelength ranges)
            and metadata checks (required IVOA-aligned header fields present
            and parseable). Files that fail validation or that match no known
            profile are quarantined — not silently dropped or silently accepted.
          </p>
        </Prose>
      </Subsection>

      {/* ── Quarantine ─────────────────────────────────────────────── */}
      <Subsection id="quarantine" heading="Quarantine semantics">
        <Prose>
          <p>
            A recurring design principle throughout the catalog: when the system
            encounters an irreconcilable conflict — identity ambiguity, validation
            failure, metadata inconsistency — it quarantines the item for
            operator review rather than making a guess. Quarantined items are
            persisted with diagnostic metadata explaining why they were
            quarantined, and an SNS notification alerts the operator.
          </p>
          <p>
            This means the catalog may have fewer items than a system that
            silently resolves conflicts, but the items it does have are ones
            the system is confident about. A quarantine count of zero is not
            the goal — a nonzero quarantine queue is evidence that the system
            is correctly detecting ambiguity.
          </p>
        </Prose>
      </Subsection>

      {/* ── Multi-arm merging ──────────────────────────────────────── */}
      <Subsection id="spectra-merging" heading="Multi-arm spectra merging">
        <Prose>
          <p>
            Instruments like VLT X-SHOOTER observe simultaneously in multiple
            wavelength arms (UVB, VIS, NIR), producing separate FITS files for
            each arm. The catalog detects these by grouping spectra from the
            same instrument taken within a tight time window (typically less
            than 0.01 days).
          </p>
          <p>
            When arms are detected, they are merged into a single continuous
            spectrum for display. In overlap regions between arms, the system
            blends the flux values; in gaps, NaN sentinels are inserted so the
            plot shows a visible break rather than a misleading interpolation.
            If the overlap between arms exceeds a threshold, the system selects
            the better arm (by wavelength range, then point count) rather than
            producing a confusing blend.
          </p>
          <p>
            This merging is a display-layer operation only — the data bundle
            always contains the individual per-arm FITS files.
          </p>
        </Prose>
      </Subsection>

      {/* ── Same-night dedup ───────────────────────────────────────── */}
      <Subsection
        id="same-night"
        heading="Same-night display deduplication"
      >
        <Prose>
          <p>
            When a nova has dense observational coverage — dozens of spectra
            from different nights — the waterfall plot can become cluttered.
            For display, the viewer collapses same-day observations into a
            single representative spectrum per day, selected by wavelength
            coverage (broader is better), then point count, then range.
          </p>
          <p>
            For novae with very long temporal baselines, an additional
            log-spaced thinning pass ensures that late-epoch observations
            (which may be separated by months) are not crowded out by dense
            early coverage. The goal is to preserve the shape of the nova&apos;s
            spectral evolution across its full timeline.
          </p>
        </Prose>
      </Subsection>

      {/* ── Large errors as limits ─────────────────────────────────── */}
      <Subsection
        id="large-errors"
        heading="Large photometry errors as upper limits"
      >
        <Prose>
          <p>
            Optical photometry observations with magnitude uncertainties
            greater than 1.0 mag are treated as upper limits for display
            purposes. An error of this size means the detection is not
            scientifically trustworthy as a measurement — it is better
            understood as a constraint on the nova&apos;s brightness.
          </p>
          <p>
            This is a display-layer decision. The underlying data in DynamoDB
            is not mutated — the original magnitude, error, and upper limit
            flag are preserved. The auto-flagging applies only to the optical
            regime (magnitude-based); non-optical regimes (X-ray count rates,
            radio flux densities) use different error scales and are not
            affected.
          </p>
        </Prose>
      </Subsection>

      {/* ── Upper limit suppression ────────────────────────────────── */}
      <Subsection
        id="upper-limit-suppression"
        heading="Upper limit suppression"
      >
        <Prose>
          <p>
            An upper limit is informative only if it constrains the data — that
            is, if it tells you the nova was fainter than something you
            couldn&apos;t otherwise infer from the detections. An upper limit
            that is brighter than the brightest actual detection in the same
            band provides no additional information and would only compress the
            y-axis dynamic range.
          </p>
          <p>
            The catalog drops non-constraining upper limits on a per-band
            basis during artifact generation. This filtering is applied
            per-band so that it remains correct when a user isolates a single
            band in the light curve viewer and the y-axis rescales.
          </p>
        </Prose>
      </Subsection>

      {/* ── Downsampling ───────────────────────────────────────────── */}
      <Subsection id="downsampling" heading="Downsampling algorithms">
        <Prose>
          <p>
            Raw photometry datasets can contain thousands of observations,
            which would produce an unreadable plot. The catalog uses two
            complementary downsampling strategies, both applied during artifact
            generation:
          </p>
          <p>
            <span className="text-text-primary font-medium">
              LTTB (Largest-Triangle-Three-Buckets).
            </span>{' '}
            A purpose-built time-series downsampling algorithm (Steinarsson
            2013) that preferentially preserves peaks, troughs, and inflection
            points. Used for sparklines (downsampled to 90 points) and for
            spectra display. LTTB divides the data into equal-count buckets
            and selects the point in each bucket that maximizes the triangle
            area with its neighbors — ensuring that the visual shape of the
            light curve is preserved even at aggressive reduction ratios.
          </p>
          <p>
            <span className="text-text-primary font-medium">
              Density-preserving log subsampling.
            </span>{' '}
            Used for photometry (capped at 500 points per wavelength regime).
            Observation budgets are allocated proportionally across bands, then
            within each band, log-spaced time intervals with dynamic boundary
            stretching select representative points. Detections are preferred
            over upper limits. This ensures that sparse late-epoch
            observations — which carry high scientific value — are never
            crowded out by dense early coverage.
          </p>
        </Prose>
      </Subsection>

      {/* ── Band offsets ───────────────────────────────────────────── */}
      <Subsection id="band-offsets" heading="Photometry band offsets">
        <Prose>
          <p>
            When multiple photometric bands occupy overlapping magnitude ranges,
            their data points can form an unreadable cluster. The catalog
            computes per-band vertical offsets to separate overlapping traces,
            using a multi-stage algorithm:
          </p>
          <p>
            First, each band&apos;s time-series is fit with a smoothing spline.
            A gap analysis then measures the pairwise overlap between bands
            using the splined curves — specifically, the fraction of their
            shared time domain where the curves are within a threshold
            magnitude of each other. Bands are partitioned into overlap
            clusters using a union-find structure, so that well-separated bands
            (like I-band at 12 mag when V and R are at ~10 mag) are never
            displaced. Within each cluster, an exhaustive search finds the
            ordering and offset magnitudes that best separate the traces.
          </p>
          <p>
            Offsets are rounded to half-magnitude increments (e.g., +0.5, +1.0)
            for clean visual presentation, and are displayed in the band legend
            so researchers always know a shift has been applied. Results are
            cached in DynamoDB and invalidated when band membership or
            observation density changes significantly.
          </p>
        </Prose>
      </Subsection>

      {/* ── Release model ──────────────────────────────────────────── */}
      <Subsection id="release-model" heading="Immutable release model">
        <Prose>
          <p>
            Published artifacts are delivered through an immutable release
            model. Each artifact generation sweep writes all artifacts — both
            for novae with new data and unchanged novae copied from the
            previous release — to a new, timestamped S3 prefix. Only after
            every artifact is in place does an atomic pointer update
            (<span className="font-mono text-sm text-text-primary">current.json</span>)
            make the new release visible to browsers via CloudFront.
          </p>
          <p>
            This means users never see a partially updated catalog. The
            previous release remains available until the new one is fully
            written. Rolling back to a previous release is a single JSON
            write — no per-artifact operations required. Old releases are
            automatically cleaned up by an S3 lifecycle rule after 7 days.
          </p>
          <p>
            The frontend makes zero backend API calls. It fetches static JSON
            artifacts from CloudFront and renders them entirely on the client.
            This architecture keeps operational costs near zero and eliminates
            an entire class of backend availability concerns.
          </p>
        </Prose>
      </Subsection>

    </div>
  );
}
