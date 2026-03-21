# SVO Filter Analysis Tools

Tools for harvesting the SVO Filter Profile Service database, analyzing
intra-band filter consistency, and quantifying photometric systematics
across commercially available amateur filter sets.

These tools support the NovaCat photometric band grouping rules
(`docs/specs/band_grouping_rules.md`) and the filter consistency research
report (`docs/research/filter_consistency_report.md`).

## Quick Start

```bash
# Install dependencies
pip install requests astropy numpy scipy matplotlib pyarrow pymupdf

# Harvest the SVO database (~2 hours, ~500 MB)
python svo_harvest.py

# Run the full analysis
python svo_analysis.py --db svo_fps.db --custom-dir ./custom_filters/ --custom-unit nm

# Run the amateur filter comparison
python amateur_comparison.py --custom-dir ./custom_filters/ --custom-unit nm --db svo_fps.db
```

## Database Location

The harvested SQLite database (`svo_fps.db`, ~428 MB) is too large for git.

- **Local:** `tools/svo-filter-analysis/svo_fps.db` (gitignored)
- **S3 (persistent):** `s3://<novacat-bucket>/reference-data/svo-fps/svo_fps.db`
- **Parquet (Athena):** `s3://<novacat-bucket>/reference-data/svo-fps/parquet/`

To sync to/from S3:

```bash
# Upload (also exports Parquet automatically)
python svo_aws.py sync-to-s3 --bucket <novacat-bucket> --prefix reference-data/svo-fps/

# Download to a new machine
python svo_aws.py pull-from-s3 --bucket <novacat-bucket> --prefix reference-data/svo-fps/
```

To regenerate from scratch:

```bash
python svo_harvest.py -o svo_fps.db
```

The database was last harvested on 2026-03-20 (10,664 filters, 0 failures).

## Scripts

| Script | Purpose |
|---|---|
| `svo_harvest.py` | Two-phase SVO FPS harvester with schema migration, checkpointing, and resume support. |
| `svo_query.py` | CLI and Python library for querying the harvested database (search, export, plot). |
| `svo_aws.py` | S3 sync, Parquet export, custom filter insertion, Lambda helper class. |
| `svo_diagnostic.py` | Database summary statistics and band coverage report. |
| `svo_analysis.py` | Track 1 (overlap computation) and Track 2 (synthetic photometry systematics). Supports `--exclude` for outlier removal. |
| `svo_band_diagnostic.py` | Identifies outlier filters within bands; produces exclusion lists and visual verification plots. |
| `amateur_comparison.py` | Head-to-head comparison of amateur filter sets (Astrodon, Baader, Chroma) with Bessell reference. |
| `digitize_curve.py` | Interactive plot digitizer for extracting transmission curves from PDF datasheets. |

## Custom Filters

Amateur filter transmission curves are stored in `custom_filters/`. Wavelengths are in
nanometers, transmission in percent (0–100). The analysis scripts handle unit conversion
and normalization automatically.

Naming convention: `Manufacturer_FilterName_Band.csv`

### Data Sources

**Astrodon Photometrics (B, V, Rc, Ic)**
- Source: Manufacturer PDF datasheets, digitized using `digitize_curve.py`
- URLs: https://astrodon.com/products/astrodon-photometrics-uvbri-filters/
  - B: https://carlostapia.es/curvas_filtros/Astrodon_B_Johnson_files/filter_lica_astrodon_b_johnson.pdf
  - V: https://www.carlostapia.es/curvas_filtros/Astrodon_V_Johnson_files/filter_lica_astrodon_v_johnson_2.pdf
  - Rc: https://carlostapia.es/curvas_filtros/Astrodon_Rc_Johnson_files/filter_lica_astrodon_rc_johnson.pdf
  - Ic: https://carlostapia.es/curvas_filtros/Astrodon_Ic_Johnson_files/filter_lica_astrodon_ic_johnson.pdf
- Note 1: Astrodon Ic is a Cousins Ic design (narrower, hard red cutoff), distinct from the
  Bessell I prescription used by other manufacturers.
- Note 2: WebPlotDigitizer (https://automeris.io/) was used to extract curves from PDFs.

**Baader Planetarium (U, B, V, R, I)**
- Source: Manufacturer-published transmission data (OD curves converted to linear transmission)
- URLs: https://www.baader-planetarium.com/en/baader-ubvri-bessel-filter-set-photometric.html?sku=2961750
  - U: https://www.baader-planetarium.com/en/downloads/dl/file/id/1753/baader-ubvri-u-filter-transmission-od-log-t.xlsx
  - B: https://www.baader-planetarium.com/en/downloads/dl/file/id/1754/baader-ubvri-b-filter-transmission-od-log-t.xlsx
  - V: https://www.baader-planetarium.com/en/downloads/dl/file/id/1755/baader-ubvri-v-filter-transmission-od-log-t.xlsx
  - R: https://www.baader-planetarium.com/en/downloads/dl/file/id/1756/baader-ubvri-r-filter-transmission-od-log-t.xlsx
  - I: https://www.baader-planetarium.com/en/downloads/dl/file/id/1757/baader-ubvri-i-filter-transmission-od-log-t.xlsx
- Note 1: Hybrid glass + interference design. The Baader V is an extremely close match to
  the Bessell V reference (overlap 0.997).
- Note 2: The raw Baader OD curves show secondary transmission peaks (red leaks) at
  ~1150 nm in some filters (notably B and I). These are real interference-coating
  harmonics, not measurement artifacts. However, they fall beyond the quantum efficiency
  cutoff of silicon CCDs (~1050 nm), so no CCD observer will record these photons. The
  CSV files in `custom_filters/` are truncated before the red leak onset, representing
  the effective filter+silicon-detector bandpass — which is the appropriate model for
  AAVSO CCD observers.

**Chroma Technology (U, B, V, R, I)**
- Source: Manufacturer-published downloadable transmission data
- URL: https://www.chroma.com/products/sets/27105-classic-ubvri-set/
- Note: Chroma publishes numerical transmission data directly (no digitization needed).
  Click on the ASCII link next to each filter to download.

**Reference: OAF/Bessell (U, B, V, R, I)**
- Source: SVO FPS database (filter IDs: OAF/Bessell.U, .B, .V, .R, .I)
- These represent the canonical Bessell glass prescription for the Johnson-Cousins system.

### Files

```
custom_filters/
├── raw_sources/                ← Original PDFs and OD spreadsheets as downloaded
│   ├── Astrodon_B_Johnson.pdf
│   ├── Astrodon_V_Johnson.pdf
│   ├── Astrodon_Rc_Johnson.pdf
│   ├── Astrodon_Ic_Johnson.pdf
│   ├── Baader_U_OD.xlsx
│   ├── Baader_B_OD.xlsx
│   ├── Baader_V_OD.xlsx
│   ├── Baader_R_OD.xlsx
│   └── Baader_I_OD.xlsx
├── Astrodon_B_B.csv
├── Astrodon_V_V.csv
├── Astrodon_Rc_R.csv
├── Astrodon_Ic_I.csv
├── Baader_U_U.csv
├── Baader_B_B.csv
├── Baader_V_V.csv
├── Baader_R_R.csv
├── Baader_I_I.csv
├── Chroma_U_U.csv
├── Chroma_B_B.csv
├── Chroma_V_V.csv
├── Chroma_R_R.csv
└── Chroma_I_I.csv
```

## Analysis Outputs

Outputs are written to `analysis_output/` (also gitignored). Key files:

| File | Description |
|---|---|
| `overlap_boxplots.png` | Box plot of pairwise overlap distributions per band |
| `heatmap_*.png` | Clustered overlap matrices per band |
| `overlap_cdf_histogram.png` | CDF and histogram of all pairwise overlaps |
| `overlap_summary.csv` | Per-band overlap statistics |
| `mean_overlap_by_filter.png` | Per-filter mean overlap dot plot (outlier identification) |
| `outlier_curves_*.png` | Transmission curves with outliers highlighted |
| `outliers.json` | Exclusion list for re-running clean analysis |
| `filter_diagnostics.csv` | Full per-filter diagnostic table |
| `systematics_spread.png` | Magnitude spread vs temperature per band |
| `systematics_detail_*.png` | Per-filter magnitude offsets per band |
| `systematics_summary.csv` | Per-band systematics at key temperatures |
| `amateur_transmission_curves.png` | Amateur filter curves by band |
| `amateur_magnitude_differences.png` | Pairwise Δmag vs temperature |
| `amateur_comparison_table.csv` | Summary table with overlaps and Δmag |

## Reproducing the Analysis

```bash
# 1. Harvest (or pull from S3)
python svo_harvest.py -o svo_fps.db

# 2. Run diagnostics to identify outliers
python svo_band_diagnostic.py --db svo_fps.db --threshold 0.60

# 3. Run main analysis with outlier exclusion
python svo_analysis.py --db svo_fps.db \
    --custom-dir ./custom_filters/ \
    --custom-unit nm \
    --exclude analysis_output/outliers.json

# 4. Run amateur head-to-head comparison
python amateur_comparison.py \
    --custom-dir ./custom_filters/ \
    --custom-unit nm \
    --db svo_fps.db
```

## Dependencies

- Python 3.11+
- `requests` — HTTP client for SVO API
- `astropy` — VOTable parsing (optional but recommended; falls back to raw XML)
- `numpy`, `scipy` — Numerical computation, interpolation, clustering
- `matplotlib` — Visualization
- `pyarrow` — Parquet export (only needed for `svo_aws.py`)
- `boto3` — AWS S3 sync (only needed for `svo_aws.py`)
- `pymupdf` — PDF rendering (only needed for `digitize_curve.py`)

## Related Documents

- `docs/specs/band_grouping_rules.md` — Tier 1 and Tier 2 band grouping rules
- `docs/research/filter_consistency_report.md` — Full research report
- `docs/adr/ADR-016-band-filter-resolution.md` — Band resolution strategy
- `docs/adr/ADR-017-*` — Band registry design (in progress)
