#!/usr/bin/env python3
"""
apply_photometry_contracts.py
─────────────────────────────
Assertion-based script that applies all `light-curve-ingestion` epic contract
changes to the two existing contracts files:

  contracts/models/entities.py
    - New enums: TimeOrigSys, PhotSystem, SpectralCoordType, SpectralCoordUnit,
                 MagSystem, FluxDensityUnit, QualityFlag, DataRights,
                 PhotometryQuarantineReasonCode
    - New model: PhotometryRow

  contracts/models/events.py
    - IngestPhotometryEvent: add raw_s3_key, raw_s3_bucket, file_sha256 fields

Each modification is guarded by:
  1. A pre-condition assertion (insertion anchor must exist; new code must NOT
     already exist — prevents double-application).
  2. The change itself.
  3. A post-condition assertion (new code is present after write).

Run from the repository root:
    python scripts/apply_photometry_contracts.py

Exit 0 on success.  Any assertion failure raises AssertionError with a
descriptive message; no file is written if a pre-condition fails.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ENTITIES_PATH = REPO_ROOT / "contracts" / "models" / "entities.py"
EVENTS_PATH = REPO_ROOT / "contracts" / "models" / "events.py"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    print(f"  ✓ Written: {path.relative_to(REPO_ROOT)}")


def _assert_present(content: str, marker: str, label: str) -> None:
    assert marker in content, (
        f"Pre-condition failed ({label}): expected marker not found.\n"
        f"  Marker: {marker!r}\n"
        f"  This may mean the file has drifted from the expected state."
    )


def _assert_absent(content: str, marker: str, label: str) -> None:
    assert marker not in content, (
        f"Pre-condition failed ({label}): marker already present — "
        f"script may have been applied already.\n"
        f"  Marker: {marker!r}"
    )


# ---------------------------------------------------------------------------
# Payload: entities.py additions
# ---------------------------------------------------------------------------

# Insertion anchor: append immediately after SpectraQuarantineReasonCode class.
# We locate the end of that class by finding its last member line.
_ENTITIES_ANCHOR = '    other = "OTHER"\n'

# New content is inserted after the FIRST occurrence of this anchor that follows
# "class SpectraQuarantineReasonCode".  We use a two-phase replace: split on the
# anchor inside the Spectra class, not any other enum's `other = "OTHER"` line.
_ENTITIES_ANCHOR_CONTEXT = (
    "class SpectraQuarantineReasonCode(str, Enum):\n"
    '    """\n'
    "    Quarantine reason codes for spectra DataProduct validation quarantine\n"
    "    (acquire_and_validate_spectra workflow).\n"
    '    """\n'
    "\n"
    '    unknown_profile = "UNKNOWN_PROFILE"\n'
    '    missing_critical_metadata = "MISSING_CRITICAL_METADATA"\n'
    '    checksum_mismatch = "CHECKSUM_MISMATCH"\n'
    '    coordinate_proximity = "COORDINATE_PROXIMITY"\n'
    '    other = "OTHER"'
)

_ENTITIES_IDEMPOTENCY_MARKER = "class PhotometryQuarantineReasonCode"

_ENTITIES_ADDITIONS = '''

class TimeOrigSys(str, Enum):
    """
    Time system of the original reported epoch (time_orig).

    Allowed values follow photometry_table_model.md §2.
    NULL (None) when time_orig is NULL.
    """

    mjd_utc = "MJD_UTC"
    mjd_tt = "MJD_TT"
    hjd_utc = "HJD_UTC"
    hjd_tt = "HJD_TT"
    jd_utc = "JD_UTC"
    jd_tt = "JD_TT"
    isot = "ISOT"
    other = "OTHER"


class PhotSystem(str, Enum):
    """
    Photometric system name.

    Allowed values follow photometry_table_model.md §3.
    """

    johnson_cousins = "Johnson-Cousins"
    sloan = "Sloan"
    swift_uvot = "Swift-UVOT"
    twomass = "2MASS"
    bessel = "Bessel"
    radio = "Radio"
    xray = "X-ray"
    other = "OTHER"


class SpectralCoordType(str, Enum):
    """
    Type of spectral coordinate for the band.

    Determines the unit of spectral_coord_value.
    Allowed values follow photometry_table_model.md §3.
    """

    wavelength = "wavelength"
    frequency = "frequency"
    energy = "energy"


class SpectralCoordUnit(str, Enum):
    """
    Unit of spectral_coord_value.

    Allowed values follow photometry_table_model.md §3.
    """

    angstrom = "Angstrom"
    nm = "nm"
    ghz = "GHz"
    mhz = "MHz"
    kev = "keV"


class MagSystem(str, Enum):
    """
    Magnitude zero-point system.

    NULL for radio and X-ray (where magnitudes are not used).
    Allowed values follow photometry_table_model.md §3.
    """

    vega = "Vega"
    ab = "AB"
    st = "ST"


class FluxDensityUnit(str, Enum):
    """
    Unit of flux_density.

    NULL when flux_density is NULL.
    Allowed values follow photometry_table_model.md §4.
    """

    jy = "Jy"
    mjy = "mJy"
    ujy = "uJy"
    erg_cm2_s_hz = "erg/cm2/s/Hz"
    erg_cm2_s_kev = "erg/cm2/s/keV"


class QualityFlag(int, Enum):
    """
    Data quality flag.

    0 = good, 1 = uncertain/marginal, 2 = poor/use with caution, 3 = bad/do not use.
    Allowed values follow photometry_table_model.md §4.
    """

    good = 0
    uncertain = 1
    poor = 2
    bad = 3


class DataRights(str, Enum):
    """
    Data rights / licence for a photometry row.

    Defaults to `public` for published literature data.
    Allowed values follow photometry_table_model.md §5.
    """

    public = "public"
    cc_by = "CC-BY"
    cc_by_sa = "CC-BY-SA"
    proprietary = "proprietary"
    other = "OTHER"


class PhotometryQuarantineReasonCode(str, Enum):
    """
    Quarantine reason codes for photometry ingestion
    (ingest_photometry workflow).

    See: ADR-015, Decisions 2 and 5.
    """

    file_too_large = "FILE_TOO_LARGE"
    missing_required_columns = "MISSING_REQUIRED_COLUMNS"
    coercion_failure_threshold_exceeded = "COERCION_FAILURE_THRESHOLD_EXCEEDED"
    other = "OTHER"


# ---------------------------------------------------------------------------
# PhotometryRow
# ---------------------------------------------------------------------------


class PhotometryRow(BaseModel):
    """
    A single photometric measurement in the NovaCat photometry table.

    Maps 1:1 to a row in ``photometry_table_model.md`` (v1.1).

    This is a storage-format-agnostic logical contract.  The canonical
    serialisation format (Parquet vs. alternatives) is an open question per
    ADR-015 Open Question 1; this model is the validation contract regardless
    of format.

    ``row_id`` is intentionally absent: it is auto-incremented at persistence
    time and is not carried in the in-memory contract.

    Identity fields (nova_id, primary_name, ra_deg, dec_deg) are injected by
    the workflow from the resolved Nova entity.  They are NOT expected to be
    present in source CSV files; the PhotometryAdapter receives them as
    explicit parameters and stamps them onto every row.

    Cross-field invariants (enforced by model_validator):
      - If is_upper_limit=False: at least one of magnitude, flux_density,
        or count_rate must be non-None.
      - If is_upper_limit=True: limiting_value must be non-None.
      - limiting_value / limiting_sigma must be None when is_upper_limit=False.
      - flux_density_unit is required when flux_density is non-None.
        (Also required when is_upper_limit=True and the limit is expressed as
        a flux density — i.e. when limiting_value is non-None and magnitude
        is None.  Adapters should populate it in that case.)
      - time_orig and time_orig_sys must both be present or both be None.
    """

    model_config = ConfigDict(extra="forbid")

    # --- Section 1: Source Identification --------------------------------
    # row_id is absent by design (see docstring).
    nova_id: UUID
    primary_name: str = Field(..., min_length=1, max_length=256)
    ra_deg: float = Field(..., ge=0.0, le=360.0)
    dec_deg: float = Field(..., ge=-90.0, le=90.0)

    # --- Section 2: Temporal Metadata ------------------------------------
    time_mjd: float = Field(
        ...,
        description="Epoch of the observation in MJD (TDB scale).",
    )
    time_bary_corr: bool = Field(
        default=False,
        description="TRUE if time_mjd has been corrected to the Solar System barycentre.",
    )
    time_orig: float | None = Field(
        default=None,
        description="Original reported time value before conversion to MJD.",
    )
    time_orig_sys: TimeOrigSys | None = Field(
        default=None,
        description="Time system of time_orig.  Required when time_orig is non-None.",
    )

    # --- Section 3: Spectral / Bandpass Metadata -------------------------
    svo_filter_id: str | None = Field(
        default=None,
        max_length=256,
        description="SVO Filter Profile Service identifier.  NULL for radio and X-ray.",
    )
    filter_name: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Human-readable filter or band label.  Always populated.",
    )
    phot_system: PhotSystem
    spectral_coord_type: SpectralCoordType
    spectral_coord_value: float = Field(
        ...,
        description="Central wavelength (Å), frequency (GHz), or energy (keV).",
    )
    spectral_coord_unit: SpectralCoordUnit
    bandpass_width: float | None = Field(
        default=None,
        description="Effective width of the bandpass in spectral_coord_unit.  NULL if unknown.",
    )
    mag_system: MagSystem | None = Field(
        default=None,
        description="Magnitude zero-point system.  NULL for radio and X-ray.",
    )
    zero_point_flux: float | None = Field(
        default=None,
        description="Zero-point flux density in Jy.  NULL if not applicable.",
    )

    # --- Section 4: Photometric Measurement ------------------------------
    magnitude: float | None = None
    mag_err: float | None = None
    flux_density: float | None = None
    flux_density_err: float | None = None
    flux_density_unit: FluxDensityUnit | None = None
    count_rate: float | None = None
    count_rate_err: float | None = None
    is_upper_limit: bool = Field(default=False)
    limiting_value: float | None = Field(
        default=None,
        description=(
            "Limiting magnitude or flux density for non-detection rows.  "
            "In the same units as magnitude or flux_density (whichever applies).  "
            "NULL when is_upper_limit=False."
        ),
    )
    limiting_sigma: float | None = Field(
        default=None,
        description="Confidence level of the upper limit in sigma.  NULL when is_upper_limit=False.",
    )
    quality_flag: QualityFlag = Field(default=QualityFlag.good)
    notes: str | None = Field(default=None, max_length=2048)

    # --- Section 5: Provenance -------------------------------------------
    bibcode: str | None = Field(
        default=None,
        min_length=19,
        max_length=19,
        description="19-character ADS bibcode.  Preferred over doi for journal articles.",
    )
    doi: str | None = Field(default=None, max_length=512)
    data_url: str | None = Field(default=None, max_length=2048)
    orig_catalog: str | None = Field(default=None, max_length=256)
    orig_table_ref: str | None = Field(default=None, max_length=256)
    telescope: str | None = Field(default=None, max_length=256)
    instrument: str | None = Field(default=None, max_length=256)
    observer: str | None = Field(default=None, max_length=256)
    data_rights: DataRights = Field(default=DataRights.public)

    # --- Cross-field invariants ------------------------------------------

    @model_validator(mode="after")
    def validate_photometry_row_invariants(self) -> "PhotometryRow":
        errors: list[str] = []

        # 1. Measurement-present rule
        if self.is_upper_limit:
            if self.limiting_value is None:
                errors.append(
                    "limiting_value must be non-None when is_upper_limit=True."
                )
        else:
            if (
                self.magnitude is None
                and self.flux_density is None
                and self.count_rate is None
            ):
                errors.append(
                    "At least one of magnitude, flux_density, or count_rate must be "
                    "non-None when is_upper_limit=False."
                )

        # 2. Upper-limit field consistency
        if not self.is_upper_limit:
            if self.limiting_value is not None:
                errors.append(
                    "limiting_value must be None when is_upper_limit=False."
                )
            if self.limiting_sigma is not None:
                errors.append(
                    "limiting_sigma must be None when is_upper_limit=False."
                )

        # 3. flux_density_unit required when flux_density is present
        if self.flux_density is not None and self.flux_density_unit is None:
            errors.append(
                "flux_density_unit is required when flux_density is non-None."
            )

        # 4. time_orig / time_orig_sys co-presence
        if self.time_orig is not None and self.time_orig_sys is None:
            errors.append(
                "time_orig_sys is required when time_orig is non-None."
            )
        if self.time_orig is None and self.time_orig_sys is not None:
            errors.append(
                "time_orig_sys must be None when time_orig is None."
            )

        if errors:
            raise ValueError("; ".join(errors))

        return self
'''

# ---------------------------------------------------------------------------
# Payload: events.py additions
# ---------------------------------------------------------------------------

# Anchor: the existing field block inside IngestPhotometryEvent, just before
# the @model_validator.
_EVENTS_ANCHOR = (
    "    source: str | None = Field(\n"
    '        default=None, description="Where this request came from (e.g., UI, ingest feed)."\n'
    "    )\n"
    "    attributes: dict[str, Any] = Field(default_factory=dict)\n"
    "\n"
    '    @model_validator(mode="after")\n'
    "    def validate_identifiers(self) -> IngestPhotometryEvent:"
)

_EVENTS_IDEMPOTENCY_MARKER = "raw_s3_key: str = Field("

_EVENTS_REPLACEMENT = (
    "    source: str | None = Field(\n"
    '        default=None, description="Where this request came from (e.g., UI, ingest feed)."\n'
    "    )\n"
    "\n"
    "    # --- ADR-015, Decision 1: S3 staging fields ----------------------\n"
    "    raw_s3_key: str = Field(\n"
    "        ...,\n"
    "        min_length=1,\n"
    "        max_length=2048,\n"
    "        description=(\n"
    '            "S3 key of the staged photometry source file within the private data bucket. "\n'
    '            "Convention: uploads/photometry/<nova_id>/<filename>."\n'
    "        ),\n"
    "    )\n"
    "    raw_s3_bucket: str | None = Field(\n"
    "        default=None,\n"
    "        max_length=256,\n"
    "        description=(\n"
    '            "S3 bucket containing the staged file. "\n'
    '            "Defaults to the NovaCat private data bucket when absent."\n'
    "        ),\n"
    "    )\n"
    "    # --- ADR-015, Decision 3: idempotency key = IngestPhotometry:{nova_id}:{file_sha256}\n"
    "    file_sha256: str | None = Field(\n"
    "        default=None,\n"
    "        min_length=64,\n"
    "        max_length=64,\n"
    "        description=(\n"
    '            "SHA-256 hex digest of the source file bytes. "\n'
    '            "If absent, the workflow computes it from the S3 object before the "\n'
    '            "idempotency check. "\n'
    '            "Idempotency key: IngestPhotometry:{nova_id}:{file_sha256}."\n'
    "        ),\n"
    "    )\n"
    "\n"
    "    attributes: dict[str, Any] = Field(default_factory=dict)\n"
    "\n"
    '    @model_validator(mode="after")\n'
    "    def validate_identifiers(self) -> IngestPhotometryEvent:"
)


# ---------------------------------------------------------------------------
# Application logic
# ---------------------------------------------------------------------------


def apply_entities(path: Path) -> None:
    print(f"\nPatching {path.relative_to(REPO_ROOT)} ...")
    content = _read(path)

    # --- Pre-conditions
    _assert_present(content, _ENTITIES_ANCHOR_CONTEXT, "SpectraQuarantineReasonCode block")
    _assert_absent(
        content, _ENTITIES_IDEMPOTENCY_MARKER, "PhotometryQuarantineReasonCode (already applied?)"
    )

    # --- Apply: insert after the SpectraQuarantineReasonCode closing line
    new_content = content.replace(
        _ENTITIES_ANCHOR_CONTEXT,
        _ENTITIES_ANCHOR_CONTEXT + _ENTITIES_ADDITIONS,
        1,  # replace only the first occurrence
    )

    # --- Post-condition
    assert _ENTITIES_IDEMPOTENCY_MARKER in new_content, (
        "Post-condition failed: PhotometryQuarantineReasonCode not found after patch."
    )
    assert "class PhotometryRow" in new_content, (
        "Post-condition failed: PhotometryRow not found after patch."
    )

    _write(path, new_content)


def apply_events(path: Path) -> None:
    print(f"\nPatching {path.relative_to(REPO_ROOT)} ...")
    content = _read(path)

    # --- Pre-conditions
    _assert_present(
        content, _EVENTS_ANCHOR, "IngestPhotometryEvent source/attributes/validator block"
    )
    _assert_absent(content, _EVENTS_IDEMPOTENCY_MARKER, "raw_s3_key (already applied?)")

    # --- Apply
    new_content = content.replace(_EVENTS_ANCHOR, _EVENTS_REPLACEMENT, 1)

    # --- Post-condition
    assert _EVENTS_IDEMPOTENCY_MARKER in new_content, (
        "Post-condition failed: raw_s3_key not found after patch."
    )
    assert "raw_s3_bucket" in new_content, (
        "Post-condition failed: raw_s3_bucket not found after patch."
    )
    assert "file_sha256" in new_content, "Post-condition failed: file_sha256 not found after patch."

    _write(path, new_content)


def main() -> int:
    print("apply_photometry_contracts.py — light-curve-ingestion epic")
    print("=" * 60)

    for path in (ENTITIES_PATH, EVENTS_PATH):
        if not path.exists():
            print(f"ERROR: {path} not found.  Run from the repository root.", file=sys.stderr)
            return 1

    try:
        apply_entities(ENTITIES_PATH)
        apply_events(EVENTS_PATH)
    except AssertionError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1

    print("\n✓ All patches applied successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
