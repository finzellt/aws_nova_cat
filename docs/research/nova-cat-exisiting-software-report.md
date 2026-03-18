# NovaCat's ecosystem has tools but no turnkey pipeline

**Existing Python astronomical libraries collectively address roughly 40% of NovaCat's requirements out of the box — file I/O, unit conversions, filter metadata lookup, and UCD parsing — but leave genuine, significant gaps in filter alias resolution, string disambiguation, column normalization, and unified photometry data modeling.** The practical recommendation is Scenario B: adopt existing tools where they're strong and build bespoke components only for the well-defined gaps. No single package or combination of packages provides an end-to-end multi-wavelength photometry ingestion pipeline, and no tool anywhere in the ecosystem performs filter string alias resolution — the single most critical missing capability for NovaCat.

---

## 1. Candidate tools overview

| Tool | Version | Description | Sub-problems addressed | Offline? | Lambda-viable? |
|------|---------|-------------|----------------------|----------|----------------|
| **astropy.table** | 7.2.0 | Multi-format table I/O (CSV, VOTable, FITS, CDS, ECSV) | SP3 (partial), SP5 | ✅ | ✅ ~80 MB |
| **astropy.units** | 7.2.0 | Physical units with AB/ST mag, Jy, erg/s/cm² support | SP4 (partial) | ✅ | ✅ (bundled) |
| **astropy.io.votable** | 7.2.0 | VOTable parser with UCD preservation and validation | SP3 (partial), SP5 | ✅ | ✅ (bundled) |
| **synphot** | 1.4.0 | Synthetic photometry engine; 12 built-in optical/NIR filters | SP1 (minimal), SP4 (partial) | Partial¹ | ⚠️ needs Vega file |
| **stsynphot** | 1.4.x | HST/JWST extension with ~30+ ground-based systems | SP1 (partial) | ❌ multi-GB | ❌ exceeds 250 MB |
| **astroquery.svo_fps** | 0.4.11 | Python client for SVO Filter Profile Service (11,000+ filters) | SP1 (strong) | ❌ network | ⚠️ HTTP at query time |
| **svo_filters** | 0.4.3 | Standalone SVO wrapper with ~50 bundled filters | SP1 (minimal) | Partial | ⚠️ unmaintained |
| **speclite** | 0.20 | DESI filter library; ~100+ filters as ECSV; AB magnitudes | SP1 (partial) | ✅ | ✅ small |
| **pyphot** | 2.1.1 | 243-filter HDF5 library; AB/Vega/ST zero points; SVO integration | SP1 (good), SP4 (partial) | ✅ mostly | ⚠️ HDF5 native dep |
| **sedpy** | 0.3.x | ~150 bundled .par filter files; lightweight; MIT license | SP1 (partial) | ✅ | ✅ ~5–10 MB |
| **PyVO** | 1.7 | IVOA protocol client (TAP, SCS, Registry); pure Python | SP5 (partial) | ❌ network | ✅ lightweight |
| **astroquery.vizier** | 0.4.11 | VizieR catalog queries with UCD metadata | SP5 (partial) | ❌ network | ⚠️ HTTP at query time |

¹ synphot's code is offline but requires a downloaded Vega spectrum file (~1 MB) for VEGAMAG calculations.

---

## 2. Per-sub-problem assessment

### SP1: No tool provides alias resolution — the critical gap

The SVO Filter Profile Service is the authoritative filter registry for astronomy, containing **11,000+ filters** with standardized IDs in `Facility/Instrument.Band` format (e.g., `Generic/Johnson.V`, `2MASS/2MASS.Ks`, `Swift/UVOT.UVW2`). Each filter carries rich metadata: effective/pivot/mean wavelengths, FWHM, zero points in Jy, magnitude system (Vega/AB/ST), detector type, photometric system name, and full transmission curves. The `astroquery.svo_fps` module provides programmatic access.

**However, no tool maps real-world filter strings to SVO IDs.** When a data file says `"V"`, `"Johnson V"`, `"Vmag"`, or `"Bessell_V"`, no existing library resolves these to `Generic/Johnson.V`. Every tool — astroquery.svo_fps, pyphot, speclite, sedpy, synphot — requires the caller to already know the exact canonical filter name in that tool's specific naming convention. The naming conventions themselves are mutually incompatible: sedpy uses `bessell_V`, pyphot uses `GROUND_JOHNSON_V`, speclite uses `bessell-V`, and SVO uses `Generic/Johnson.V`. There is no cross-registry mapping.

**Regime coverage is UV-through-mid-IR only.** SVO FPS covers GALEX, Swift/UVOT, Johnson-Cousins, SDSS, 2MASS, WISE, JWST/NIRCam, JWST/MIRI, and Spitzer IRAC. X-ray instruments (Swift/XRT, Chandra, XMM-Newton) use effective area curves rather than photometric bandpasses and are absent from all filter databases. X-ray "bands" like `0.3–10 keV` are pseudo-filters requiring bespoke handling. Radio and gamma-ray regimes are similarly unrepresented.

**What must be built:** A custom alias registry mapping ~200–500 common filter strings (from AAVSO codes, literature conventions, and catalog column names) to canonical SVO FPS FilterIDs. This registry should also flag non-photometric modes: AAVSO `Vis.` (visual eye estimates, band code 0), `CV`/`CR` (unfiltered CCD with V/R zero points, codes 8/9), and `TG`/`TB`/`TR` (DSLR tri-color channels). A static JSON or SQLite lookup table is the right architecture for Lambda deployment.

### SP2: Disambiguation is entirely unsolved

No existing tool provides a disambiguation model for filter strings. The problem is real: `"K"` could mean 2MASS Ks (~2.16 μm), Johnson K (~2.2 μm), or radio K-band (~22 GHz). `"R"` could mean Cousins R, Johnson R, or SDSS r. `"U"` could mean Johnson U, Swift/UVOT U, or Strömgren u.

**There is no documented taxonomy of known ambiguities** in any Python package. The closest resource is the SVO FPS itself, where searching for a band letter reveals all matching filters across photometric systems — but this is a discovery tool, not a disambiguation engine.

**What must be built:** A context-aware resolver that uses signals like catalog provenance (an AAVSO export implies Johnson-Cousins), facility metadata (a Swift observation implies UVOT filters), wavelength-regime context (adjacent columns in NIR suggest 2MASS, not radio), and explicit photometric system declarations in file headers. The resolver should implement a strict escalation policy: unambiguous mappings resolve silently, ambiguous-but-resolvable cases log their reasoning, and genuinely unresolvable cases raise errors rather than guess. A decision-tree or rule-based approach is appropriate — this does not need ML.

### SP3: UCD parsing exists but column mapping does not

Astropy's `astropy.io.votable.ucd` module can parse and validate UCDs against the **UCD 1.6 vocabulary** (updated December 2024). When reading VOTable files, astropy preserves UCD annotations in `column.meta['ucd']`, making it possible to programmatically find columns tagged `phot.mag;em.opt.V` or `stat.error;phot.mag;em.opt.B`. VizieR exports are the best-annotated source, with UCDs on nearly all columns.

**The gap is that no tool acts on UCDs automatically.** There is no function like `find_column_by_role(table, "magnitude", band="V")`. The pipeline must implement its own semantic column discovery layer that inspects UCDs for VOTable inputs, uses header pattern matching for CSV inputs (case-insensitive regex against synonyms like `MAG`, `Magnitude`, `mag`, `m`, `Vmag`), and falls back to positional heuristics for minimal-metadata files.

**A critical UCD limitation:** UCDs cannot distinguish Vega from AB magnitudes. The UCD `phot.mag;em.opt.V` means "V-band magnitude" without specifying the zero-point system. Magnitude system information must come from separate metadata — PhotDM annotations, SVO FPS `MagSys` fields, or file-level context. For CSV files with no UCD metadata, column normalization is entirely a pattern-matching problem.

### SP4: PhotDM is the right standard but has no Python implementation

**IVOA PhotDM 1.1** (Recommendation, November 2022) defines exactly the data model NovaCat needs: `PhotometryFilter` (with spectral location, transmission curve, facility/instrument), `PhotometricSystem` (detector type, system name), `PhotCal` (binding filter + zero point + magnitude system), and three `ZeroPoint` subclasses — `PogsonZeroPoint` for standard magnitudes, `AsinhZeroPoint` for luptitudes, and `LinearFluxZeroPoint` for linear flux measurements. The standard explicitly addresses multi-regime coverage including X-ray energy bands treated as pseudo-filters.

**No Python package implements PhotDM classes.** The `ivoa-std/PhotDM` GitHub repo contains serialization examples and a Jupyter notebook demonstrating SED construction from annotated VOTables, but no installable library. PyVO does not implement PhotDM — the IVOA community has discussed this as future work, and the MIVOT (Model Instances in VOTable) annotation effort aims to bridge PhotDM into astropy/PyVO, but this remains in development.

Astropy's unit system provides strong building blocks: **`ABmag`, `STmag`, `Jy`, `mJy`, `erg/s/cm²/Hz`** are all native units with conversion equivalencies. Vega magnitudes require synphot (which carries the Vega reference spectrum). `SpectralCoord` handles wavelength↔frequency↔energy conversions. But these are primitives, not a schema.

**What must be built:** A Python dataclass or Pydantic model implementing a subset of PhotDM, representing a single photometric measurement with fields for: value, uncertainty, unit, is_upper_limit flag, filter_id (SVO FPS canonical ID), magnitude_system (Vega/AB/ST/instrumental/N-A), spectral_coordinate (with original and converted representations), epoch (MJD with original time system preserved), and provenance (bibcode, catalog name, telescope, instrument). This is perhaps **50–100 lines of well-designed code**, not a large effort.

### SP5: Format reading is solved; semantic ingestion is not

Astropy's unified I/O is excellent for raw file reading. **`Table.read()` handles CSV, FITS, VOTable, CDS-format, IPAC, ECSV, HDF5, and Parquet** with automatic format detection. VOTable parsing preserves full metadata including UCDs, units, and descriptions. The `QTable` variant automatically wraps unit-bearing columns as `Quantity` objects.

For catalog access, `astroquery.vizier` queries any of **20,000+ VizieR catalogs** with UCD-based column filtering, and `astroquery.mast` provides access to Pan-STARRS, HSC, GALEX, and JWST catalogs with AWS cloud-hosted data. Both return astropy Tables.

**AAVSO is the notable gap.** There is no `astroquery.aavso` module and no official AAVSO Python library. AAVSO provides HTTP REST APIs for the AID (International Database) and VSX (Variable Star Index), returning CSV, JSON, or delimited text. Their filter encoding uses a numeric + string code system (`0=Vis.`, `2=V`, `3=B`, `8=CV`, `26=K`, etc.) that is entirely AAVSO-specific. A lightweight custom HTTP client with CSV parsing is needed.

**The deeper gap is semantic ingestion** — mapping from raw table columns (with catalog-specific names, units, and conventions) into NovaCat's unified schema. No existing tool chains together: read file → identify columns by role → resolve filter identity → validate magnitude system → normalize units → populate schema. Each step exists in some tool, but the orchestration layer connecting them is entirely custom.

---

## 3. Recommendation: Scenario B — partial coverage with targeted bespoke work

Existing tools solve the infrastructure layers well. **The gaps are in the semantic/mapping layers that sit between raw I/O and a unified data model.** Here is a concrete build-vs-adopt breakdown:

**Adopt existing tools for:**
- **File I/O**: `astropy.table` for all format reading (CSV, VOTable, FITS, CDS). Zero custom code needed.
- **Filter metadata**: Pre-cache SVO FPS data at build time using `astroquery.svo_fps`, storing the ~500 filters relevant to nova observations as a static JSON or SQLite file deployed with the Lambda function. This eliminates runtime network dependency while leveraging the authoritative registry.
- **Unit conversions**: `astropy.units` for all flux/magnitude/spectral conversions. Use `synphot` (with a pre-bundled Vega spectrum file) for Vega magnitude calculations.
- **UCD parsing**: `astropy.io.votable.ucd` for parsing and validating UCDs from VOTable inputs.
- **Catalog queries** (in non-Lambda contexts): `astroquery.vizier` and `astroquery.mast` for data acquisition.

**Build bespoke components for:**
- **Filter alias registry** (~200–500 entries): A static lookup mapping common filter strings → canonical SVO FPS FilterIDs, plus a rejection list for non-photometric modes (Vis., CV, CR, TG). Estimated effort: **2–3 days**.
- **Context-aware disambiguator**: Rule-based resolver using file provenance, facility metadata, and wavelength-regime context. Estimated effort: **2–3 days**.
- **Column normalizer**: Pattern-matching engine for CSV/text headers plus UCD-based discovery for VOTables, mapping to a canonical column schema. Estimated effort: **3–5 days**.
- **Photometry measurement model**: PhotDM-inspired dataclass hierarchy with Pydantic validation. Estimated effort: **2–3 days**.
- **AAVSO ingestion adapter**: HTTP client + CSV parser + AAVSO filter code translator. Estimated effort: **1–2 days**.
- **Per-source ingestion adapters**: Thin adapters for each source format (canonical CSV, VizieR VOTable, AAVSO export, literature supplement) that use the column normalizer and feed the measurement model. Estimated effort: **3–5 days per format**.

---

## 4. Notable caveats and risks

### Unmaintained or fragile tools

**svo_filters** (v0.4.3) shows no new releases in 12+ months and has only ~194 weekly PyPI downloads. It should be avoided in favor of `astroquery.svo_fps` for SVO access or a custom pre-cached solution. **sedpy** is lightly maintained — functional but not actively developed; its filter library is a useful reference but not a dependency to take. **stsynphot** requires multi-GB CRDS reference data, making it categorically incompatible with Lambda's **250 MB deployment limit**.

### Regime coverage holes are fundamental

Every filter database in the Python ecosystem — SVO FPS, pyphot, sedpy, speclite, synphot — is built around photometric bandpass filters, which are a UV-through-mid-IR concept. **X-ray, radio, and gamma-ray observations use fundamentally different response characterizations** (effective area curves, energy band definitions, beam patterns). NovaCat's schema must treat these regimes as first-class citizens with energy-band or frequency-range identifiers rather than SVO FilterIDs. The X-ray pseudo-filter concept from PhotDM 1.1 provides a model, but implementation is entirely bespoke.

### Lambda deployment architecture

**Cold-start sensitivity** is the primary concern. A Lambda function loading `astropy` (~80 MB) plus `numpy` (~30 MB) faces cold starts of **3–8 seconds** depending on memory allocation. Adding `synphot` and `pyphot` increases this. The mitigation strategy should be: (1) use provisioned concurrency for latency-sensitive paths, (2) pre-compute and bundle all filter metadata as a static file (eliminating runtime SVO FPS queries), (3) avoid pyphot's HDF5 dependency (requires native `libhdf5` in the Lambda layer), and (4) consider packaging with Lambda SnapStart or container images if the deployment package exceeds 250 MB.

**Network access** is available in Lambda (unlike some serverless assumptions), but HTTP calls to external services like SVO FPS add **200–2000 ms latency** per query and introduce a runtime dependency on a Spanish academic server's availability. Pre-caching eliminates this risk entirely. `astroquery.svo_fps`'s default file-based caching writes to `~/.astropy/cache/`, which requires a writable filesystem — on Lambda, this must be redirected to `/tmp` (512 MB ephemeral storage, or up to 10 GB with configuration).

### The UCD vocabulary gap matters

UCDs cannot encode magnitude system (Vega vs. AB), specific photometric system (Johnson V vs. Bessell V), or observation quality flags. This means UCD-based column discovery from VOTables provides **necessary but insufficient** information — it identifies that a column is "a V-band magnitude" but not which V-band system or zero point. The pipeline must always cross-reference UCD discoveries against catalog-level metadata or the SVO FPS PhotCalID system. This is a design constraint, not a bug to fix.

### Licensing is clean

All recommended tools use permissive licenses: astropy (BSD-3), synphot (BSD-3), astroquery (BSD-3), pyphot (likely BSD/MIT — published in JOSS), sedpy (MIT), speclite (BSD-3), PyVO (BSD-3). No GPL contamination risk for NovaCat's codebase.

---

## Conclusion

The Python astronomical ecosystem provides a strong foundation of I/O primitives, unit systems, and filter metadata services, but **no tool addresses the core challenge of NovaCat: mapping messy, heterogeneous, real-world photometry data into a clean, unified, multi-regime schema**. The SVO Filter Profile Service is the closest thing to an authoritative filter registry, and PhotDM 1.1 is the correct data model standard — but both lack Python implementations that handle the alias resolution and column mapping that a practical ingestion pipeline demands. The recommended architecture layers bespoke semantic components (alias registry, disambiguator, column normalizer, PhotDM-lite schema) on top of proven infrastructure (astropy I/O, astropy.units, pre-cached SVO metadata), keeping the custom code surface small and well-bounded while avoiding dependency on unmaintained or Lambda-hostile packages.
