"""
profiles/base.py — FITS Profile Protocol and canonical output types.

Defines the interface contract all concrete profiles must satisfy.
Contains no provider-specific logic and no AWS dependencies.

Public surface (imported by profiles/__init__.py):
    FitsProfile        — Protocol every profile must implement
    NormalizedSpectrum — canonical output shape (IVOA-aligned)
    ProfileResult      — structured return type from validate()

NOT imported directly by spectra_validator. The validator calls only:
    from profiles import validate_spectrum
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import numpy as np

# ---------------------------------------------------------------------------
# Canonical output shape
# ---------------------------------------------------------------------------


@dataclass
class NormalizedSpectrum:
    """
    Canonical internal representation of a validated spectrum (IVOA-aligned).
    Every profile must produce this shape — no profile-specific fields.
    """

    # ---- Data arrays (required) ----
    spectral_axis: np.ndarray  # wavelength / frequency / energy values
    flux_axis: np.ndarray  # flux values
    spectral_units: str  # e.g. "Angstrom", "nm", "Hz"
    flux_units: str  # e.g. "erg/s/cm2/Angstrom", "Jy"

    # ---- Required metadata ----
    observation_time: str  # ISO-8601 UTC; from DATE-OBS or derived from MJD-OBS
    provider: str  # e.g. "ESO"
    data_product_id: str  # stable UUID — never modified by normalization

    # ---- Optional metadata ----
    observation_mjd: float | None = None
    target_ra_deg: float | None = None
    target_dec_deg: float | None = None
    instrument: str | None = None  # from INSTRUME
    telescope: str | None = None  # from TELESCOP
    exposure_time_s: float | None = None  # from EXPTIME
    spectral_resolution: float | None = None  # R = λ/Δλ; from SPECRP if available
    snr: float | None = None  # median signal-to-noise per pixel; from SNR column if available

    # ---- Provenance ----
    raw_header: dict[str, Any] = field(default_factory=dict)
    normalization_notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Profile result
# ---------------------------------------------------------------------------


@dataclass
class ProfileResult:
    """
    Structured outcome from a FitsProfile.validate() call.

    success=True  → spectrum is populated; ValidateBytes records VALID.
    success=False → spectrum is None; ValidateBytes records QUARANTINE.

    ProfileResult covers only deterministic failures (missing columns,
    bad units, failed sanity checks). Transient I/O exceptions (OSError,
    MemoryError, truncated stream) are NOT caught by the profile — they
    propagate to ValidateBytes, which converts them to RetryableError.
    """

    success: bool

    # Populated on success
    spectrum: NormalizedSpectrum | None = None

    # Populated on failure
    quarantine_reason: str | None = None
    quarantine_reason_code: str | None = None
    # Valid values (from SpectraQuarantineReasonCode):
    #   "UNKNOWN_PROFILE"
    #   "MISSING_CRITICAL_METADATA"
    #   "CHECKSUM_MISMATCH"
    #   "COORDINATE_PROXIMITY"
    #   "OTHER"

    # Present regardless of success/failure
    normalization_notes: list[str] = field(default_factory=list)
    header_signature_hash: str | None = None
    profile_id: str | None = None
    # The profile that was selected and applied.
    # Set even on failure so operators know which profile was attempted.


# ---------------------------------------------------------------------------
# FitsProfile Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class FitsProfile(Protocol):
    """
    Protocol all concrete FITS profile implementations must satisfy.

    To add a new profile:
      1. Implement this Protocol in a new module (e.g. profiles/eso_uves.py).
      2. Instantiate and append to _PROFILE_REGISTRY in profiles/__init__.py.
      No other files need to change.
    """

    @property
    def profile_id(self) -> str:
        """
        Stable unique identifier for this profile.
        Persisted as fits_profile_id on the DataProduct record.
        Examples: "ESO_UVES", "ESO_XSHOOTER", "ESO_FALLBACK"
        """
        ...

    def matches(
        self,
        provider: str,
        hdulist: Any,  # astropy.io.fits.HDUList
    ) -> bool:
        """
        Return True if this profile should handle the given file.

        Called during selection in registry order; first match wins.
        Must be deterministic: same inputs always return the same result.
        May inspect any HDU in hdulist — not just HDU 0.

        Typical implementations:
          Instrument-specific: return provider == "ESO" and
                               hdulist[0].header.get("INSTRUME") == "UVES"
          Fallback:            return provider == "ESO"
        """
        ...

    def validate(
        self,
        hdulist: Any,  # astropy.io.fits.HDUList
        product_metadata: dict[str, Any],
    ) -> ProfileResult:
        """
        Validate and normalize a FITS HDUList into a NormalizedSpectrum.

        product_metadata fields available:
          data_product_id  — stable UUID (must be passed through unchanged)
          nova_id          — for logging/context only
          provider         — e.g. "ESO"
          hints            — dict from DataProduct.hints (collection, t_min_mjd, etc.)

        Contract:
          - Return ProfileResult(success=True, spectrum=...) on success.
          - Return ProfileResult(success=False, quarantine_reason_code=...) on
            any deterministic failure. NEVER raise on deterministic failures.
          - Let transient I/O exceptions (OSError, MemoryError, etc.) propagate
            uncaught — the caller converts them to RetryableError.

        Sanity checks every profile MUST enforce:
          1. Spectral axis is strictly monotonic (ascending or descending).
          2. Flux array is non-empty.
          3. Fraction of NaN/Inf in flux is below threshold (default 20%).
          4. Spectral axis values fall within a plausible range for declared units.
          5. Required metadata fields are present and non-null;
             missing required fields → quarantine_reason_code="MISSING_CRITICAL_METADATA"
        """
        ...
