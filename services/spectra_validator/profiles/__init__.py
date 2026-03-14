"""
profiles/__init__.py — FITS profile registry and public entry point.

Public surface:
    validate_spectrum(hdulist, *, provider, data_product_id, hints) -> ProfileResult

Internal:
    _PROFILE_REGISTRY  — ordered list of FitsProfile instances
                         First match wins; more specific profiles must come
                         before any fallback.

Adding a new profile:
    1. Implement FitsProfile in a new module (e.g. profiles/eso_harps.py).
    2. Instantiate and insert into _PROFILE_REGISTRY below.
       Order matters: more specific profiles (instrument-level) before
       any catch-all fallback.
    No other files need to change.
"""

from __future__ import annotations

from typing import Any

from .base import FitsProfile, NormalizedSpectrum, ProfileResult
from .eso_fallback import EsoFallbackProfile
from .eso_uves import EsoUvesProfile
from .eso_xshooter import EsoXShooterProfile

# ---------------------------------------------------------------------------
# Profile registry
# ---------------------------------------------------------------------------
# Order is significant — first match wins.
# Instrument-specific profiles must appear before provider-level fallbacks.

_PROFILE_REGISTRY: list[FitsProfile] = [
    EsoUvesProfile(),
    EsoXShooterProfile(),
    EsoFallbackProfile(),  # catch-all — must be last
]

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def validate_spectrum(
    hdulist: Any,
    *,
    provider: str,
    data_product_id: str,
    hints: dict[str, Any] | None = None,
) -> ProfileResult:
    """
    Select a profile for the given FITS file and run validation.

    Profile selection: iterate _PROFILE_REGISTRY in order; call the first
    profile whose matches(provider, hdulist) returns True.

    Returns ProfileResult(success=False, quarantine_reason_code="UNKNOWN_PROFILE")
    if no registered profile matches. This is a deterministic failure —
    it does not raise.

    Transient I/O exceptions from inside a profile's validate() are NOT caught
    here — they propagate to ValidateBytes, which converts them to RetryableError.
    """
    product_metadata: dict[str, Any] = {
        "data_product_id": data_product_id,
        "provider": provider,
        "hints": hints or {},
    }

    for profile in _PROFILE_REGISTRY:
        if profile.matches(provider, hdulist):
            return profile.validate(hdulist, product_metadata)

    # No profile matched — quarantine with a clear operator-facing reason.
    collection = (hints or {}).get("collection", "")
    reason = (
        f"No registered profile matched provider={provider!r}"
        + (f", collection={collection!r}" if collection else "")
        + f". Registered profiles: {[p.profile_id for p in _PROFILE_REGISTRY]}"
    )
    return ProfileResult(
        success=False,
        quarantine_reason=reason,
        quarantine_reason_code="UNKNOWN_PROFILE",
        profile_id=None,
    )


__all__ = [
    "validate_spectrum",
    "FitsProfile",
    "NormalizedSpectrum",
    "ProfileResult",
]
