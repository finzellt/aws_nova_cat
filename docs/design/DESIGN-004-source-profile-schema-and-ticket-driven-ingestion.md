# DESIGN-004: Source Profile Schema and Ticket-Driven Ingestion

**Status:** Draft
**Date:** 2026-03-25
**Author:** TF
**Document class:** Design / Scoping (feeds implementation; does not itself constitute
binding architectural decisions)

**Relates to:**
- `DESIGN-001` — Photometry Ingestion System: Full Redesign *(predecessor; this document
  provides an alternative entry point that bypasses DESIGN-001's Layer 0 runtime
  heuristics)*
- `DESIGN-002` — Data Provenance and Derived Quantities *(provenance field definitions
  used by the photometry reader)*
- `ADR-016` — Band/Filter Resolution Strategy *(case-sensitive matching, excluded filter
  handling)*
- `ADR-017` — Band Registry Design *(band resolution for photometry ingestion; amended
  two-track band_id convention)*
- `ADR-019` — Photometry Table Model Revision *(`PhotometryRow` v2.0 schema)*
- `ADR-020` — Photometry Storage Format *(DynamoDB row-level persistence, key structure,
  envelope items)*
- `ADR-021` — Layer 0 Pre-Ingestion Normalization *(the heuristic path that this
  document's ticket-driven path bypasses)*

---

## 1. Executive Summary

The photometry ingestion pipeline (DESIGN-001, ADR-015 through ADR-021) has thorough
specifications for what to do *after* the system knows a file's schema, but significant
gaps in how the system *decodes* an unknown file. The current architecture relies on
runtime heuristics (synonym registry, column header scanning, wide-format detection)
that are underspecified and unvalidated against real data.

Meanwhile, we have ~100 real ticket/CSV pairs — hand-curated metadata files that
completely describe each data file's structure. These tickets contain column indices,
time systems, filter systems, telescope/observer metadata, bibcodes, and everything else
needed to mechanically ingest the data.

This design document formalizes the ticket-driven approach as the **Source Profile**
architecture. A Source Profile defines the parsing grammar for the ticket format; each
ticket provides per-file parameters; the data file is read using those parameters. All
three layers are fully deterministic — no runtime inference, no heuristics, no
disambiguation.

This is the **primary ingestion path for MVP**. The heuristic path (ADR-021 Layer 0)
remains as the fallback for files without tickets, not the primary path.

**Scope boundary.** This document covers the ticket-driven ingestion path only. It does
not replace ADR-021 (Layer 0 heuristics) or ADR-018 (disambiguation algorithm). It
provides an alternative entry point that bypasses Layer 0's runtime inference when a
Source Profile + ticket exists.

---

## 2. Architecture Overview

### 2.1 Three-Layer Deterministic Model

```
Source Profile (defines ticket grammar)
    → Ticket (per-file configuration parsed from the .txt file)
        → Data file (CSV, read using ticket parameters)
```

Each layer is fully deterministic:

- **Source Profile:** The schema that the Pydantic models implement. Two profile types
  exist — photometry and spectra — reflecting the two structurally distinct ticket
  formats. The profile defines which keys are expected, their types, and their
  semantics.

- **Ticket:** A hand-curated `.txt` file containing key-value pairs that completely
  describe one data file's structure. The ticket is parsed against its Source Profile
  into a typed Pydantic model.

- **Data file:** A CSV file (headerless for photometry, with headers for spectra
  metadata) read using the column indices and metadata supplied by the ticket.

### 2.2 Two Ticket Types

The two ticket types share a common header structure (key-value metadata about the nova
and source) but diverge completely in their column mapping sections:

- **Photometry tickets** map columns of the data CSV directly. One ticket describes one
  CSV file. The CSV is headerless; columns are identified by 0-based index.

- **Spectra tickets** map columns of an intermediary metadata CSV. Each row of the
  metadata CSV describes one spectrum data file. This is a two-hop indirection: ticket →
  metadata CSV → spectrum data files. The metadata CSV has headers; the individual
  spectrum CSVs are headerless two-column files (wavelength, flux).

### 2.3 Workflow Shape

The ticket-driven ingestion path is realized as a single Step Functions state machine
(`ingest_ticket`) with a shared preamble and a type-specific branch:

```
ticket.txt → ParseTicket → ResolveNova → TicketTypeBranch
                                              │
                                         ┌────┴────┐
                                         │         │
                                    photometry   spectra
                                         │         │
                                    CSV rows    metadata CSV
                                         │         │
                                    band reg    per-spectrum:
                                    resolve      CSV → FITS
                                         │         │
                                    DDB PutItem   S3 upload +
                                  (PhotometryRow)  DDB ref
```

The full workflow specification is in `docs/workflows/ingest-ticket.md`.

---

## 3. Ticket Models (Contract Layer)

**Module:** `contracts/models/tickets.py`

The two ticket types are modeled as a **discriminated union** rather than an inheritance
hierarchy. The structural divergence between photometry and spectra tickets (different
column index sets, different output paths, different indirection models) makes a union
the natural fit. Callers pattern-match on `ticket_type` and work with a fully typed,
specific model.

A shared `_TicketCommon` base class provides DRY field definitions for the fields common
to both ticket types. It is not exported; the public API consists of
`PhotometryTicket`, `SpectraTicket`, and the `Ticket` union type.

All models use `extra = "forbid"` to reject unknown fields, consistent with the rest of
the contracts layer.

### 3.1 Shared Fields

Both ticket types carry the following fields, defined on `_TicketCommon`:

| Ticket Key | Pydantic Field | Type | Description |
|---|---|---|---|
| `OBJECT NAME` | `object_name` | `str` | Nova name. Input to `initialize_nova` for UUID resolution. |
| `WAVELENGTH REGIME` | `wavelength_regime` | `str` | Lowercased. Controlled vocabulary: `optical`, `uv`, `nir`, `mir`, `radio`, `xray`, `gamma`. |
| `TIME SYSTEM` | `time_system` | `str` | Time system of temporal values: `JD`, `MJD`, `HJD`, or `BJD`. |
| `ASSUMED DATE OF OUTBURST` | `assumed_outburst_date` | `float \| None` | Assumed outburst date in the ticket's `time_system`. `NA` → `None`. |
| `REFERENCE` | `reference` | `str` | Human-readable citation string. |
| `BIBCODE` | `bibcode` | `str` | 19-character ADS bibcode. |
| `TICKET STATUS` | `ticket_status` | `str` | Curation status. Only `completed` tickets are processed. |

### 3.2 Photometry Ticket Fields

Beyond the shared base, the photometry ticket adds:

**Header-level defaults** (can be overridden per-row if a column index is provided):

| Ticket Key | Pydantic Field | Type | Description |
|---|---|---|---|
| `TIME UNITS` | `time_units` | `str` | Unit of the time column (e.g. `days`). |
| `FLUX UNITS` | `flux_units` | `str` | Unit of the flux column (e.g. `mags`). |
| `FLUX ERROR UNITS` | `flux_error_units` | `str` | Unit of the flux error column (e.g. `mags`). |
| `FILTER SYSTEM` | `filter_system` | `str \| None` | Default photometric system (e.g. `Johnson-Cousins`). Overridden per-row if `filter_system_col` is set. |
| `MAGNITUDE SYSTEM` | `magnitude_system` | `str \| None` | Magnitude system (e.g. `Vega`, `AB`). Applies to all rows. |
| `TELESCOPE` | `telescope` | `str \| None` | Default telescope. Overridden per-row if `telescope_col` is set. |
| `OBSERVER` | `observer` | `str \| None` | Default observer. Overridden per-row if `observer_col` is set. |
| `DATA FILENAME` | `data_filename` | `str` | Filename of the headerless CSV data file. |

**Column index mappings** (0-based indices into the headerless data CSV):

| Ticket Key | Pydantic Field | Type |
|---|---|---|
| `TIME COLUMN NUMBER` | `time_col` | `int` |
| `FLUX COLUMN NUMBER` | `flux_col` | `int` |
| `FLUX ERROR COLUMN NUMBER` | `flux_error_col` | `int \| None` |
| `FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER` | `filter_col` | `int \| None` |
| `UPPER LIMIT FLAG COLUMN NUMBER` | `upper_limit_flag_col` | `int \| None` |
| `TELESCOPE COLUMN NUMBER` | `telescope_col` | `int \| None` |
| `OBSERVER COLUMN NUMBER` | `observer_col` | `int \| None` |
| `FILTER SYSTEM COLUMN NUMBER` | `filter_system_col` | `int \| None` |

The override pattern: if a column index is present, the per-row value from the CSV is
used. If the column index is `None` (parsed from `NA`), the ticket-level default
applies to every row.

**Discriminator:** `ticket_type: Literal["photometry"] = "photometry"`

### 3.3 Spectra Ticket Fields

**Header-level fields:**

| Ticket Key | Pydantic Field | Type | Description |
|---|---|---|---|
| `FLUX UNITS` | `flux_units` | `str \| None` | Default flux units. `NA` → `None`. Overridden per-spectrum if `flux_units_col` is set. |
| `FLUX ERROR UNITS` | `flux_error_units` | `str \| None` | Default flux error units. |
| `DEREDDENED FLAG` | `dereddened` | `bool` | `True` if spectra have been dereddened by the source authors. |
| `METADATA FILENAME` | `metadata_filename` | `str` | Filename of the metadata CSV (has headers). |

**Column indices into the metadata CSV** (0-based). These point into the metadata CSV,
not the individual spectrum data files:

| Ticket Key | Pydantic Field | Type |
|---|---|---|
| `FILENAME COLUMN` | `filename_col` | `int` |
| `WAVELENGTH COLUMN` | `wavelength_col` | `int` |
| `FLUX COLUMN` | `flux_col` | `int` |
| `FLUX ERROR COLUMN` | `flux_error_col` | `int \| None` |
| `FLUX UNITS COLUMN` | `flux_units_col` | `int \| None` |
| `DATE COLUMN` | `date_col` | `int` |
| `TELESCOPE COLUMN` | `telescope_col` | `int \| None` |
| `INSTRUMENT COLUMN` | `instrument_col` | `int \| None` |
| `OBSERVER COLUMN` | `observer_col` | `int \| None` |
| `SNR COLUMN` | `snr_col` | `int \| None` |
| `DISPERSION COLUMN` | `dispersion_col` | `int \| None` |
| `RESOLUTION COLUMN` | `resolution_col` | `int \| None` |
| `WAVELENGTH RANGE COLUMN` | `wavelength_range_cols` | `tuple[int, int] \| None` |

The `WAVELENGTH RANGE COLUMN` field is the only multi-valued column index in either
ticket type. The raw ticket value `"10,11"` is parsed into a `tuple[int, int]`.

**Discriminator:** `ticket_type: Literal["spectra"] = "spectra"`

### 3.4 The Union Type

```
Ticket = Annotated[
    PhotometryTicket | SpectraTicket,
    Field(discriminator="ticket_type"),
]
```

The `ticket_type` field is derived during parsing from the presence of `DATA FILENAME`
(photometry) vs `METADATA FILENAME` (spectra) in the raw ticket. Neither present or
both present → parse error.

### 3.5 Spectra Two-Hop Indirection

The spectra ticket's column indices point into the **metadata CSV**, not the spectrum
data files. The metadata CSV carries per-spectrum column indices (e.g.
`WAVELENGTH COL NUM`, `FLUX COL NUM`) that point into each individual spectrum CSV.
These inner column indices are read from the metadata CSV at processing time — they are
not part of the ticket model.

```
Spectra Ticket               Metadata CSV                 Spectrum CSV
─────────────               ────────────                 ────────────
FILENAME COLUMN: 0    →     #FILENAME: GQMUSA...csv
WAVELENGTH COLUMN: 1  →     WAVELENGTH COL NUM: 0   →   col 0: wavelength
FLUX COLUMN: 2        →     FLUX COL NUM: 1         →   col 1: flux
DATE COLUMN: 5        →     DATE: 2.44732e+06
TELESCOPE COLUMN: 7   →     TELESCOPE: CTIO 1 m
```

The ticket tells you how to read the metadata CSV. The metadata CSV tells you how to
read each spectrum file. Clean two-layer separation.

---

## 4. Ticket Parser

**Module:** `services/ticket_parser/`

The parser reads a `.txt` ticket file and produces a validated `PhotometryTicket` or
`SpectraTicket`. It is a standalone service module that imports from
`contracts.models.tickets`.

### 4.1 Public API

Two functions:

- **`parse_ticket_file(path) → dict[str, str]`** — Stage 1: format-aware,
  schema-ignorant. Reads the `.txt` file and returns raw key-value pairs.

- **`validate_ticket(raw_dict) → PhotometryTicket | SpectraTicket`** — Stage 2:
  schema-aware. Discriminates ticket type, maps keys to Pydantic field names, coerces
  types, and constructs the validated model.

The two stages are separated so that Stage 1 can be unit-tested against malformed files
independently of schema validation. Consumers (the `ticket_parser` Lambda handler) call
both in sequence.

### 4.2 Stage 1: Raw Parse Rules

- Split each line on the **first** `:` only. Strip whitespace from both key and value.
- Empty lines and whitespace-only lines are skipped.
- `NA` values are preserved as the literal string `"NA"` — conversion to `None` happens
  in Stage 2.
- Duplicate keys raise `TicketParseError` immediately.
- Lines with no `:` delimiter raise `TicketParseError` with the line number.
- Output keys are the raw ticket key strings (e.g. `"OBJECT NAME"`,
  `"FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER"`).

### 4.3 Stage 2: Discrimination and Validation

**Discrimination logic:** Presence of `DATA FILENAME` in the raw dict → photometry.
Presence of `METADATA FILENAME` → spectra. Both present or neither present →
`TicketParseError`.

**Key mapping:** One static dict per ticket type mapping raw ticket keys to Pydantic
field names. These dicts are the single source of truth for the ticket-key-to-field-name
correspondence. Any raw key not present in the selected map is rejected — this enforces
strict schema conformance before Pydantic validation.

The two key mapping dicts are defined as module-level constants in the parser. They
enumerate every valid ticket key for each type and its corresponding Pydantic field
name. Example entries:

- `"FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER"` → `"filter_col"`
- `"WAVELENGTH RANGE COLUMN"` → `"wavelength_range_cols"`
- `"DEREDDENED FLAG"` → `"dereddened"`

The complete mapping tables are an implementation deliverable.

**Type coercion rules** (applied after key mapping, before Pydantic construction):

| Raw value pattern | Coerced type | Examples |
|---|---|---|
| `"NA"` (case-insensitive) | `None` | `FLUX ERROR COLUMN NUMBER: NA` → `None` |
| Digit string | `int` | `"0"` → `0` |
| Comma-separated digit pair | `tuple[int, int]` | `"10,11"` → `(10, 11)` |
| `"True"` / `"False"` (case-insensitive) | `bool` | `"False"` → `False` |
| Float-like string in date field | `float` | `"2452148.839"` → `2452148.839` |
| All other strings | `str`, stripped | Preserved as-is |

**Normalizations applied unconditionally:** `wavelength_regime` is lowercased;
`ticket_status` is lowercased. All other string values preserve their original casing.

### 4.4 Error Model

`TicketParseError` with three fields: `path` (str), `reason` (str),
`line_number` (int | None).

A bad ticket is an operator authoring error, not a data quality issue. No quarantine, no
retry. The operator fixes the ticket and reruns. Pydantic `ValidationError` is caught
and re-raised as `TicketParseError` with the path and a human-readable summary of the
validation failures.

### 4.5 Scope Boundary

The parser produces a validated ticket model. It does not read the data CSV, resolve
nova names, or interact with DynamoDB. It is a pure function from file path to typed
model.

---

## 5. Nova Resolution Strategy

**Task:** `ResolveNova` (Lambda handler: `nova_resolver_ticket`)

Nova resolution translates the ticket's `OBJECT NAME` (e.g. `V4739_Sgr`) into a
`nova_id` UUID plus identity fields (`primary_name`, `ra_deg`, `dec_deg`). This is
accomplished via Lambda-encapsulated polling against the existing `initialize_nova`
workflow. No modifications to `initialize_nova` are required.

### 5.1 Resolution Sequence

1. **Preflight check:** Query DynamoDB `NameMapping` partition
   (`PK = "NAME#<normalized_object_name>"`) for an existing `nova_id`.

2. **If found:** Return the existing `nova_id` immediately. Fetch `ra_deg`, `dec_deg`
   from the Nova item (`PK = <nova_id>`, `SK = "NOVA"`).

3. **If not found:** Fire `initialize_nova` via `sfn:StartExecution` with
   `candidate_name = object_name`. Poll `sfn:DescribeExecution` at 2-second intervals
   until the execution reaches a terminal state.

4. **On `CREATED_AND_LAUNCHED` or `EXISTS_AND_LAUNCHED`:** Extract `nova_id` from the
   execution output. Fetch coordinates from the Nova item.

5. **On `NOT_FOUND`:** Raise `QuarantineError` with reason
   `UNRESOLVABLE_OBJECT_NAME`.

6. **On `QUARANTINED`:** Raise `QuarantineError` with reason `IDENTITY_AMBIGUITY`.

7. **On failure:** Raise `TerminalError`.

### 5.2 Design Rationale

- **Zero modifications to `initialize_nova`.** The polling approach is entirely
  self-contained within the `ResolveNova` Lambda. `initialize_nova` is an Express
  workflow, which rules out the synchronous `.sync:2` invocation pattern
  (Standard-only). The task token pattern (`.waitForTaskToken`) was considered and
  rejected for MVP due to the coupling cost — it would require `initialize_nova`'s
  terminal handlers to call `sfn:SendTaskSuccess/Failure`.

- **One name per ticket.** Each ticket contains exactly one `OBJECT NAME`, so the
  orchestrator fires a single `initialize_nova` execution. The polling overhead is
  minimal — `initialize_nova` completes in 5–15 seconds for the happy path.

- **Preflight DDB check avoids unnecessary workflow executions.** For the common case
  (the nova already exists), resolution completes in a single DDB read with no
  workflow invocation.

---

## 6. Photometry Reader

**Task:** `IngestPhotometry` (Lambda handler: `ticket_ingestor`, photometry branch)

The photometry reader takes a parsed `PhotometryTicket`, a resolved `nova_id` with
identity fields, and the path to the data directory. It reads the CSV, constructs
`PhotometryRow` items, and writes them to DynamoDB.

### 6.1 Inputs

- Serialized `PhotometryTicket` (output of `ParseTicket`)
- `nova_id`, `primary_name`, `ra_deg`, `dec_deg` (output of `ResolveNova`)
- `data_dir` (from the original workflow input)
- `correlation_id`, `job_run_id` (threading context)

### 6.2 CSV Reading

The data file is a headerless CSV at `{data_dir}/{ticket.data_filename}`. Reading rules:

- Standard CSV parsing (commas, quoted fields for values containing commas such as
  observer names)
- Empty rows and trailing whitespace-only rows are skipped
- Each row is accessed by positional index, not by column name — there are no headers

### 6.3 Per-Row Field Extraction

For each row, fields are extracted using the ticket's column indices. If a column index
is present, the per-row CSV value is used. If the column index is `None`, the
ticket-level default applies.

| Target field | Source | Fallback |
|---|---|---|
| Time value | `time_col` (required, always per-row) | — |
| Magnitude | `flux_col` (required, always per-row) | — |
| Magnitude error | `flux_error_col` | `None` → no error reported |
| Filter string | `filter_col` | `None` → see §6.5 |
| Upper limit flag | `upper_limit_flag_col` | `None` → `False`. Coerce `"0"` → `False`, `"1"` → `True` |
| Telescope | `telescope_col` | `ticket.telescope` |
| Observer | `observer_col` | `ticket.observer` |
| Filter system | `filter_system_col` | `ticket.filter_system` |

### 6.4 Time Conversion

The ticket carries `time_system` (`JD`, `MJD`, `HJD`, `BJD`) and `time_units`
(always `"days"` in the current corpus). The `PhotometryRow` contract requires
`time_mjd` in Modified Julian Date.

| `time_system` | Conversion to MJD | `time_bary_corr` | `time_orig_sys` |
|---|---|---|---|
| `JD` | `value - 2400000.5` | `False` | `JD` |
| `MJD` | no conversion | `False` | `MJD` |
| `HJD` | `value - 2400000.5` | `False` | `HJD` |
| `BJD` | `value - 2400000.5` | `True` | `BJD` |

In all cases:

- `time_orig` preserves the raw value from the CSV.
- `time_orig_sys` records the ticket's `time_system`.
- HJD is heliocentric, not barycentric — `time_bary_corr` is `False`. The original
  time system is preserved in `time_orig_sys` for downstream consumers that need to
  distinguish heliocentric from uncorrected times.

### 6.5 Band Resolution

Band resolution uses the real band registry (ADR-017). For each row, the reader has:

- A **filter string** from the CSV (e.g. `"V"`)
- Optionally a **filter system** context signal from the ticket or per-row column
  (e.g. `"Johnson-Cousins"`)

**Resolution sequence:**

1. Look up the filter string in the band registry's alias index via
   `lookup_band_id(filter_string)`.
2. **Single match:** resolved. Populate `band_id`, `regime`, `svo_filter_id`,
   `spectral_coord_value` (from the registry entry's `lambda_eff`),
   `spectral_coord_type`, `spectral_coord_unit`, `bandpass_width` from the matched
   entry.
3. **Matched entry has `excluded: true`:** skip this row. Log it. Increment
   `rows_skipped_excluded` counter.
4. **No match:** unrecognized filter string. Record as a row-level failure. The row is
   not written to DynamoDB.

For MVP ticket-driven ingestion, direct alias lookup is the primary resolution
mechanism. The full three-stage disambiguation funnel (ADR-018) is not required because
the ticket corpus uses unambiguous filter strings that resolve to single registry
entries via direct alias match. If future tickets introduce ambiguous filter strings,
the filter system context signal is available for narrowing, but that logic should be
implemented as part of ADR-018, not as ad hoc disambiguation in the ticket reader.

### 6.6 PhotometryRow Construction

For each successfully resolved row, a `PhotometryRow` (ADR-019 v2.0) is constructed
from three sources:

**From ResolveNova:** `nova_id`, `primary_name`, `ra_deg`, `dec_deg`

**From the band registry (resolved entry):** `band_id`, `regime`, `svo_filter_id`,
`spectral_coord_type`, `spectral_coord_value`, `spectral_coord_unit`, `bandpass_width`

**From the ticket + CSV row:**

- `time_mjd`, `time_orig`, `time_orig_sys`, `time_bary_corr` (per §6.4)
- `magnitude`, `mag_err` (from flux/error columns when `flux_units = "mags"`)
- `is_upper_limit`, `limiting_value` (from upper limit flag column)
- `telescope`, `observer` (per-row or ticket default)
- `bibcode` (from ticket)

**Hardcoded/defaulted fields:**

| Field | Value | Rationale |
|---|---|---|
| `data_origin` | `"literature"` | All ticket-ingested data is from published literature |
| `band_resolution_type` | `"canonical"` or `"synonym"` | Depends on which alias matched |
| `band_resolution_confidence` | `"high"` | Ticket provides explicit filter system context |
| `sidecar_contributed` | `False` | No sidecar in the ticket-driven path |
| `data_rights` | `"public"` | Published literature data |
| `donor_attribution` | `None` | Not donor-submitted |
| `spectral_coord_type` | `"wavelength"` | All current tickets are optical/UV/NIR |

### 6.7 Row-Level Failure Handling

Rows that fail (unrecognized filter string, type coercion error, Pydantic validation
failure) are collected but do not abort the batch. The handler processes all rows and
returns a summary:

- `rows_written`: successful conditional PutItem
- `rows_skipped_duplicate`: `row_id` already existed in DDB (conditional write
  suppressed)
- `rows_skipped_excluded`: filter string resolved to an excluded band
- `rows_failed`: collected with row number and failure reason

Row failures are persisted to S3 diagnostics following the existing pattern:
`diagnostics/photometry/<nova_id>/row_failures/<ticket_filename_sha256>.json`.

---

## 7. Spectra Reader

**Task:** `IngestSpectra` (Lambda handler: `ticket_ingestor`, spectra branch)

The spectra reader takes a parsed `SpectraTicket`, a resolved `nova_id` with identity
fields, and the path to the data directory. It reads the metadata CSV, iterates over
each spectrum file, converts each to FITS, uploads to S3, and inserts DDB reference
items.

### 7.1 Inputs

- Serialized `SpectraTicket` (output of `ParseTicket`)
- `nova_id`, `primary_name`, `ra_deg`, `dec_deg` (output of `ResolveNova`)
- `data_dir` (from the original workflow input)
- `correlation_id`, `job_run_id` (threading context)

### 7.2 Metadata CSV Reading

The metadata CSV is at `{data_dir}/{ticket.metadata_filename}`. Unlike the photometry
data CSV, it **has headers** (e.g. `#FILENAME, WAVELENGTH COL NUM, FLUX COL NUM, ...`).

For each row, the reader extracts fields using the ticket's column indices:

| Target field | Column index | Notes |
|---|---|---|
| Spectrum filename | `filename_col` | Path to the individual spectrum CSV |
| Wavelength column (in spectrum CSV) | `wavelength_col` | 0-based index into the spectrum data file |
| Flux column (in spectrum CSV) | `flux_col` | 0-based index into the spectrum data file |
| Flux error column (in spectrum CSV) | `flux_error_col` | `None` → no error column |
| Flux units | `flux_units_col` | Per-spectrum; falls back to `ticket.flux_units` |
| Observation date | `date_col` | In the ticket's `time_system` |
| Telescope | `telescope_col` | `None` → not recorded |
| Instrument | `instrument_col` | `None` → not recorded |
| Observer | `observer_col` | `None` → not recorded |
| SNR | `snr_col` | `None` → not recorded |
| Dispersion | `dispersion_col` | Å/pixel |
| Resolution | `resolution_col` | `None` → not recorded |
| Wavelength range | `wavelength_range_cols` | Pair of columns: (start, end) in Å |

Note the two-hop indirection: the ticket's `wavelength_col` value (e.g. `1`) is the
column index in the **metadata CSV** that contains the wavelength column number for
that spectrum's **data file**. The metadata CSV row for GQMUSA says
`WAVELENGTH COL NUM: 0`, meaning column 0 of `GQMUSA_Williams_Optical_Spectra.csv` is
the wavelength column.

### 7.3 Per-Spectrum Processing

For each row in the metadata CSV, the reader:

1. **Reads the spectrum data CSV** at `{data_dir}/{spectrum_filename}`. The file is a
   headerless CSV. Column indices for wavelength, flux, and optionally flux error come
   from the metadata CSV row (not from the ticket directly).

2. **Extracts the data arrays:** wavelength array (Å), flux array, and optionally flux
   error array.

3. **Converts to FITS:**
   - Primary HDU: flux array as the data unit
   - Header keywords reconstructed from the ticket and metadata CSV fields:

   | FITS Keyword | Source |
   |---|---|
   | `OBJECT` | `ticket.object_name` |
   | `DATE-OBS` | Observation date from metadata CSV, converted to ISO 8601 |
   | `TELESCOP` | Telescope from metadata CSV |
   | `INSTRUME` | Instrument from metadata CSV |
   | `OBSERVER` | Observer from metadata CSV |
   | `CRVAL1` | First wavelength value (start of wavelength array) |
   | `CDELT1` | Dispersion from metadata CSV (Å/pixel) |
   | `CRPIX1` | `1.0` (reference pixel) |
   | `CTYPE1` | `WAVE` |
   | `CUNIT1` | `Angstrom` |
   | `NAXIS1` | Length of flux array |
   | `BUNIT` | Flux units from metadata CSV |
   | `BIBCODE` | `ticket.bibcode` |
   | `DEREDDEN` | `ticket.dereddened` |
   | `SNR` | SNR from metadata CSV (if available) |
   | `WAV_MIN` | Wavelength range start (if available) |
   | `WAV_MAX` | Wavelength range end (if available) |

4. **Uploads the FITS file to S3.** The S3 key must be compatible with the existing
   spectra file layout so that `generate_nova_bundle` can find it. The exact key
   structure is an open question (OQ-2).

5. **Inserts DDB reference items.** A `DataProduct` item (spectra type) and a
   `FileObject` item are created for each spectrum, linking the `nova_id` to the S3
   artifact. The `data_product_id` is derived deterministically from the spectrum's
   identity (bibcode + filename + nova_id).

### 7.4 Date Conversion

Observation dates in the metadata CSV are in the ticket's `time_system` (e.g. JD
values like `2.44732e+06`). These are converted to ISO 8601 strings for the
`DATE-OBS` FITS keyword using the same JD → calendar date conversion used elsewhere in
the pipeline.

### 7.5 Failure Handling

Per-spectrum failures (missing data file, malformed CSV, FITS construction error, S3
upload failure) are collected but do not abort the batch. The handler returns:

- `spectra_ingested`: successful FITS upload + DDB reference creation
- `spectra_failed`: collected with spectrum filename and failure reason

---

## 8. DynamoDB Write Strategy

### 8.1 Photometry Writes

PhotometryRow items are written using the key structure from ADR-020:

```
PK = "<nova_id>"
SK = "PHOT#<row_id>"
```

`row_id` is a stable UUID derived deterministically from the row's natural identity.
The exact derivation function is ADR-020 OQ-1 — it must be resolved before
implementation. The ticket-driven path and the heuristic path **must use the same
derivation** so that the same observation ingested via either path produces the same
`row_id` and collides cleanly.

**Conditional PutItem:** Each write uses a condition expression that suppresses the
write if an item with the same `row_id` already exists. This provides row-level
idempotency — re-running the same ticket produces no duplicate rows.

**Envelope update:** After all rows are written, the
`PRODUCT#PHOTOMETRY_TABLE` envelope item is updated with `last_ingestion_at`,
`last_ingestion_source`, `ingestion_count` increment, and updated `row_count`.

If the envelope item does not yet exist (edge case: `initialize_nova` was just
created and `ingest_new_nova` hasn't run yet), the ingestor creates it with an
"ensure exists" pattern (conditional PutItem that only writes if the item is absent).

### 8.2 Spectra Writes

Each ingested spectrum produces two DDB items:

- A `DataProduct` item for the spectrum (spectra type, with `data_product_id`,
  provider = `"ticket_ingestion"`, operational status, etc.)
- A `FileObject` item linking the `nova_id` to the S3 FITS file

The `data_product_id` is derived deterministically from
`hash(bibcode + spectrum_filename + nova_id)` to ensure idempotency.

---

## 9. Lambda Handlers

The workflow requires three new Lambda handlers:

| Handler | Module | Task States | Description |
|---|---|---|---|
| `ticket_parser` | `services/ticket_parser/` | ParseTicket | Reads `.txt` file, validates into typed Pydantic model |
| `nova_resolver_ticket` | `services/nova_resolver_ticket/` | ResolveNova | DDB NameMapping lookup + `initialize_nova` fire-and-poll |
| `ticket_ingestor` | `services/ticket_ingestor/` | IngestPhotometry, IngestSpectra | Dispatches on `ticket_type`; reads data, transforms, persists |

`ticket_ingestor` is a single Lambda with internal dispatch based on `ticket_type`.
The two ingestion paths share no processing logic but share the Lambda deployment
artifact for operational simplicity.

Existing shared handlers (`job_run_manager`, `idempotency_guard`, `quarantine_handler`)
are reused without modification.

---

## 10. Open Questions

| # | Question | Blocking? | Target |
|---|---|---|---|
| OQ-1 | Deterministic `row_id` derivation for `PhotometryRow` items. Must be identical to the derivation used by the heuristic path so that the same observation ingested via either path produces the same `row_id`. | Blocks photometry implementation | ADR-020 OQ-1 |
| OQ-2 | S3 key structure for ticket-ingested FITS files. Must be compatible with the existing spectra file layout so that `generate_nova_bundle` can find them. | Blocks spectra implementation | Spectra S3 layout spec |
| OQ-3 | Should `ingest_ticket` create `PRODUCT#PHOTOMETRY_TABLE` and `PRODUCT#COLOR_TABLE` envelope items if they don't exist, or require that `initialize_nova` → `ingest_new_nova` has already created them? Proposed resolution: "ensure exists" pattern in the ingestor. | Blocks photometry implementation | Implementation decision |
| OQ-4 | Are there tickets in the ~100-file corpus where the filter string is ambiguous (resolves to multiple registry entries) and the filter system context signal is needed for disambiguation? If so, minimal disambiguation logic may be needed in the photometry reader from day one. | Informs band resolution complexity | TF to audit |
| OQ-5 | For spectra tickets with `FLUX UNITS: NA` at the ticket level and per-spectrum flux units in the metadata CSV, should the FITS `BUNIT` keyword be omitted if both are NA, or should it be set to a sentinel? | Blocks spectra implementation | Implementation decision |

---

## 11. Relationship to Existing Architecture

### 11.1 What This Replaces (for MVP)

The ticket-driven path replaces the need for:

- Layer 0 runtime heuristics (wide-format detection, column header scanning, synonym
  registry lookups) — the ticket supplies all structural information explicitly.
- ADR-018 disambiguation algorithm — the ticket corpus uses unambiguous filter strings.
- Sidecar metadata files (DESIGN-002 §3) — the ticket is a richer, more structured
  alternative to the sidecar format.

### 11.2 What This Does Not Replace

- **ADR-021 (Layer 0):** Remains as the fallback path for files without tickets. Future
  donated data that arrives without tickets will use this path.
- **ADR-018 (Disambiguation):** Remains as the resolution strategy for ambiguous filter
  strings in the heuristic path.
- **Band registry (ADR-017):** Used directly by the photometry reader. The ticket-driven
  path is a consumer of the registry, not a replacement.
- **ADR-020 (Persistence):** The DDB key structure, conditional writes, and envelope
  items are shared between the ticket-driven and heuristic paths.

### 11.3 Forward Compatibility

The ticket-driven path is the MVP ingestion mechanism. Post-MVP, it coexists with:

- **The heuristic path (Layer 0 → adapter → persistence):** For files without tickets,
  including future donated data.
- **DESIGN-003 (donation workflow):** Donated data may arrive with a simplified ticket
  format or with sidecar metadata. The Source Profile architecture is extensible to
  new profile types if the donation workflow requires them.

---

## 12. Work Decomposition

### Implementation Chunks

**Chunk 1 — Contracts and Parser:**
- `contracts/models/tickets.py` (Pydantic models)
- `services/ticket_parser/` (parser service)
- Unit tests for both

**Chunk 2 — Nova Resolution Handler:**
- `services/nova_resolver_ticket/` (DDB lookup + fire-and-poll)
- Unit tests (mock DDB + mock SFN)

**Chunk 3 — Photometry Reader:**
- Photometry branch of `services/ticket_ingestor/`
- Band registry integration
- DDB write logic
- Unit tests (mock DDB, mock band registry)

**Chunk 4 — Spectra Reader:**
- Spectra branch of `services/ticket_ingestor/`
- CSV → FITS conversion
- S3 upload logic
- DDB reference item creation
- Unit tests (mock S3, mock DDB)

**Chunk 5 — ASL and CDK:**
- State machine definition (`ingest_ticket.asl.json`)
- CDK construct additions (Lambda handlers, IAM permissions, SFN state machine)
- Integration test

### Dependencies

Chunk 1 has no dependencies. Chunk 2 depends on the existing `initialize_nova`
infrastructure. Chunks 3 and 4 depend on Chunk 1 (contracts) and Chunk 2 (nova
resolution). Chunk 5 depends on all preceding chunks.

Chunk 3 additionally depends on the band registry artifact (`band_registry.json`)
being committed — this is an Epic A deliverable that may be completed in parallel.
