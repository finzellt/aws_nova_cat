"""
profiles/__init__.py — Profile registry and public entry point.

Public API (imported by spectra_validator/handler.py):
    validate_spectrum(hdulist, *, provider, data_product_id, hints) -> ProfileResult

Registry order matters: more specific profiles MUST precede fallbacks.
Current order:
    1. EsoUvesProfile     — ESO + INSTRUME starts with "UVES"
    [2. EsoXShooterProfile — future]
    [3. EsoFallbackProfile — future]
"""

from __future__ import annotations

from typing import Any

from .base import FitsProfile, ProfileResult
from .eso_uves import EsoUvesProfile

_PROFILE_REGISTRY: list[FitsProfile] = [
    EsoUvesProfile(),
    # EsoXShooterProfile(),
    # EsoFallbackProfile(),
]


def _select_profile(provider: str, hdulist: Any) -> FitsProfile | None:
    for profile in _PROFILE_REGISTRY:
        if profile.matches(provider, hdulist):
            return profile
    return None


def validate_spectrum(
    hdulist: Any,
    *,
    provider: str,
    data_product_id: str,
    hints: dict[str, Any],
) -> ProfileResult:
    """
    Select a FITS profile and validate/normalize a spectrum.

    The only function imported by spectra_validator/handler.py.

    Returns ProfileResult(success=True, spectrum=...) on success.
    Returns ProfileResult(success=False, quarantine_reason_code="UNKNOWN_PROFILE")
        if no profile matches.
    Returns ProfileResult(success=False, ...) on deterministic validation failure.

    Transient I/O exceptions propagate uncaught — ValidateBytes converts them
    to RetryableError.
    """
    profile = _select_profile(provider, hdulist)

    if profile is None:
        try:
            instrume = str(hdulist[0].header.get("INSTRUME", "")).strip()
            telescop = str(hdulist[0].header.get("TELESCOP", "")).strip()
        except Exception:
            instrume = telescop = "unknown"

        return ProfileResult(
            success=False,
            quarantine_reason=(
                f"No FITS profile matched for provider={provider!r}. "
                f"INSTRUME={instrume!r} TELESCOP={telescop!r}. "
                "Register a new profile in profiles/_PROFILE_REGISTRY."
            ),
            quarantine_reason_code="UNKNOWN_PROFILE",
            normalization_notes=[],
            profile_id=None,
        )

    return profile.validate(
        hdulist=hdulist,
        product_metadata={
            "data_product_id": data_product_id,
            "provider": provider,
            "hints": hints,
        },
    )
