# Spectra FITS Profiles Specification

## Purpose

This document defines the profile-driven normalization and validation strategy
for spectroscopic FITS products ingested by Nova Cat.

Nova Cat aligns, to the greatest extent practical, with relevant
IVOA (International Virtual Observatory Alliance) standards, including:

- IVOA Spectrum Data Model
- ObsCore metadata conventions
- Common FITS header conventions used in VO-compliant archives

Where providers deviate from strict IVOA compliance,
FITS Profiles provide normalization rules that translate
provider-specific formats into Nova Cat’s canonical internal representation.

---

## Design Philosophy

1. Prefer IVOA-compliant interpretation when available.
2. Use provider-specific profiles only where necessary.
3. Keep Step Functions orchestration stable.
4. Keep normalization logic profile-driven and extensible.
5. Treat unknown or non-compliant formats conservatively (QUARANTINE).

---

## Architectural Context

Workflow involvement:

- `discover_spectra_products`
  - Persists provider, locator, and format hints.
- `acquire_and_validate_spectra`
  - Performs acquisition.
  - Applies FITS Profile.
  - Validates and normalizes into canonical internal model.

Profiles are applied during the `ValidateBytes` state.

---

## Canonical Internal Spectral Model (IVOA-Aligned)

The internal normalized representation aims to align with IVOA Spectrum Data Model concepts:

Required canonical fields:

- `spectral_axis` (wavelength/frequency/energy)
- `flux_axis`
- `flux_units`
- `spectral_units`
- `observation_time`
- `target_coordinates`
- `instrument`
- `exposure_time` (if available)
- `provider`
- `dataset_id`
- `nova_id`

Where possible:
- Use IVOA-recommended FITS keywords
- Preserve original header metadata for provenance

---

## Profile Selection Strategy

Profile selection occurs in this order:

1. Match by `provider`
2. If multiple profiles exist:
   - Match header signature fields
     (e.g., `INSTRUME`, `TELESCOP`, `ORIGIN`, VO compliance markers)
3. If no profile matches:
   - QUARANTINE dataset

Selection must be deterministic.

---

## FITS Profile Structure (Conceptual)

Each profile defines:

### 1. Identification

- provider
- optional header signature rules
- optional instrument/telescope identifiers

### 2. Data Location Rules

Defines where spectral data lives:

- HDU name or index
- table-based or image-based
- expected column names (with aliases)

Example:

- wavelength aliases:
  - WAVE
  - WAVELENGTH
  - LAMBDA
  - VO-compliant spectral axis columns
- flux aliases:
  - FLUX
  - F_LAMBDA
  - SPEC

---

### 3. Units Handling

- Acceptable input units
- Expected canonical internal units
- Conversion rules
- Unknown or missing units → QUARANTINE

Units should follow IVOA/VOUnits conventions when possible.

---

### 4. Header Normalization Mapping

Maps provider-specific keywords to canonical IVOA-aligned fields.

Examples:

- DATE-OBS → observation_time
- MJD-OBS → observation_mjd
- RA / DEC → target_coordinates
- EXPTIME → exposure_time
- INSTRUME → instrument

Missing required canonical fields → QUARANTINE.

---

### 5. Validation Rules

Lightweight domain sanity checks:

- Spectral axis monotonic
- Flux array non-empty
- Acceptable NaN/Inf fraction
- Plausible wavelength range
- Required metadata present

Failure classification:

- Structural corruption → QUARANTINE
- Deterministic schema violation → QUARANTINE
- Transient acquisition failure → RETRYABLE

---

## Packaging Handling

If file extension indicates ZIP:

1. Unpack
2. Identify FITS files
3. Select spectral file via profile rules

Packaging handling is acquisition logic, not profile logic.

---

## Failure Classification Summary

- Unknown profile → QUARANTINE
- Missing required IVOA-aligned metadata → QUARANTINE
- Checksum mismatch → QUARANTINE
- Invalid identifiers → TERMINAL
- Transient download failure → RETRYABLE

---

## Versioning and Evolution

Profiles may initially be defined in code (MVP).

Future improvements may include:

- Versioned JSON profile definitions
- Explicit IVOA compliance flags
- Instrument-level profiles
- Automated VO compliance detection

Profile evolution must not invalidate previously validated datasets.

---

## Invariants

- UUID identity is never modified by normalization.
- Validation is stronger than existence.
- correlation_id is propagated but profile-independent.
- idempotency keys are workflow-internal only.
