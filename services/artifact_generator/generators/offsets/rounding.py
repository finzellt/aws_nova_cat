"""Rounding and output assembly for the band offset algorithm.

This module performs the final two steps of the offset pipeline:

1. **Normalisation.** Raw offsets from the ordering search (Chunk 4)
   anchor the faintest band at 0 and accumulate upward.  The output
   contract (ADR-032 Decision 6) requires the brightest band at 0
   with all other offsets in the fainter direction.  The normalisation
   is ``output_δ_i = max_raw − raw_i``.

2. **Half-integer rounding.** Each non-zero normalised offset is
   rounded **up** to the nearest half-integer (0.5, 1.0, 1.5, …) per
   ADR-013 and ADR-032 Decision 3.  Rounding up (not to nearest)
   ensures the separation guarantee is never violated.

References
----------
- ADR-032 Decision 3: Separation Threshold (rounding convention)
- ADR-032 Decision 6: Output Contract
- ADR-013: half-integer rounding, legend display format
"""

from __future__ import annotations

import math

from .types import BandOffsetResult, OffsetDirection

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ZERO_THRESHOLD: float = 1e-9
"""Offsets smaller than this (in magnitudes) are treated as exactly zero.

Prevents floating-point dust (e.g., 2.2e-16 from subtraction cancellation)
from being rounded up to 0.5.
"""


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _round_up_half_integer(value: float) -> float:
    """Round a positive value up to the nearest half-integer.

    Examples: 0.1 → 0.5, 0.5 → 0.5, 0.51 → 1.0, 1.0 → 1.0, 1.3 → 1.5.
    """
    return math.ceil(value * 2.0) / 2.0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def round_and_assemble(
    raw_offsets: dict[str, float],
) -> list[BandOffsetResult]:
    """Normalise, round, and assemble the final per-band offset results.

    Converts the raw offsets produced by
    :func:`~.ordering.find_optimal_offsets` into the output contract
    defined in ADR-032 Decision 6.

    The transformation is:

    1. **Normalise:** ``output_δ_i = max(raw) − raw_i``.  This flips
       the anchor from the faintest band (raw convention) to the
       brightest band (output convention).
    2. **Threshold:** offsets below ``_ZERO_THRESHOLD`` are clamped to
       0.0 to avoid rounding floating-point noise up to 0.5.
    3. **Round:** non-zero offsets are rounded up to the nearest
       half-integer via ``ceil(δ × 2) / 2``.
    4. **Assemble:** each band is wrapped in a :class:`BandOffsetResult`
       with the appropriate :class:`OffsetDirection`.

    Parameters
    ----------
    raw_offsets:
        Mapping from band identifier to raw offset.  Values are
        non-negative with the faintest band at 0.0, as produced by
        :func:`~.ordering.find_optimal_offsets`.

    Returns
    -------
    list[BandOffsetResult]:
        One result per band, sorted by band identifier for
        deterministic output ordering.
    """
    if not raw_offsets:
        return []

    # --- Step 1: Normalise (brightest band → 0) ---
    max_raw = max(raw_offsets.values())
    normalised: dict[str, float] = {band: max_raw - raw for band, raw in raw_offsets.items()}

    # --- Steps 2–4: Threshold, round, assemble ---
    results: list[BandOffsetResult] = []

    for band_id in sorted(normalised):
        offset = normalised[band_id]

        if offset < _ZERO_THRESHOLD:
            results.append(
                BandOffsetResult(
                    band_id=band_id,
                    offset_mag=0.0,
                    offset_direction=OffsetDirection.none,
                )
            )
        else:
            rounded = _round_up_half_integer(offset)
            results.append(
                BandOffsetResult(
                    band_id=band_id,
                    offset_mag=rounded,
                    offset_direction=OffsetDirection.fainter,
                )
            )

    return results
