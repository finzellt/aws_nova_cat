"""Per-band photometric offset algorithm (ADR-032).

This subpackage implements the band offset computation and caching
specified in ADR-032 and DESIGN-003 §8.7.  It is consumed by the
photometry generator's ``_compute_band_offsets()`` function.

Public API
----------
Pipeline:
    compute_band_offsets
        Top-level entry point.  Accepts subsampled observations per band,
        returns offset results.

Types:
    BandObservations
        Input dataclass — subsampled (mjd, mag) arrays for one band.
    BandOffsetResult
        Output dataclass — offset magnitude and direction for one band.

Cache:
    read_offset_cache
        Read cached offsets from the main NovaCat DynamoDB table.
    write_offset_cache
        Write computed offsets to the cache.
    is_cache_valid
        Evaluate whether cached offsets can be reused.
    CachedOffsets
        Deserialized cache record dataclass.
"""

from .cache import (
    CachedOffsets,
    is_cache_valid,
    read_offset_cache,
    write_offset_cache,
)
from .pipeline import compute_band_offsets
from .types import BandObservations, BandOffsetResult

__all__ = [
    "BandObservations",
    "BandOffsetResult",
    "CachedOffsets",
    "compute_band_offsets",
    "is_cache_valid",
    "read_offset_cache",
    "write_offset_cache",
]
