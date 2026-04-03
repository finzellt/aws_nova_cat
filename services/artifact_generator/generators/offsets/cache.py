"""DynamoDB offset cache for the band offset algorithm.

The offset computation (especially spline fitting and exhaustive
ordering search) is potentially expensive for dense optical datasets.
Because the photometry artifact is regenerated whenever *any*
photometry changes for a nova, a caching layer avoids unnecessary
recomputation when the change does not materially affect the overlap
geometry.

The cache is stored as a DynamoDB item in the **main NovaCat table**
at ``PK = <nova_id>, SK = OFFSET_CACHE#<regime>``.

**Invalidation heuristic** (DESIGN-003 §8.7):

1. **Band set check.** SHA-256 of the sorted band label list.  If the
   set of bands has changed (added or removed), the cache is stale.
2. **Density stability check.** Per-band observation counts are
   compared against the cached counts.  If any band's count has
   shifted by more than the configured threshold (default: 20%
   relative, minimum 5 absolute), the cache is stale.

References
----------
- DESIGN-003 §8.7: Band Offset Computation and Caching
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from .types import (
    BandOffsetResult,
    OffsetDirection,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SK_PREFIX: str = "OFFSET_CACHE#"
"""Sort key prefix for offset cache items in the main NovaCat table."""

DEFAULT_DENSITY_THRESHOLD_RELATIVE: float = 0.20
"""Default relative change threshold for density stability check.

If a band's observation count has changed by more than this fraction
relative to the cached count, the cache is invalidated.
"""

DEFAULT_DENSITY_THRESHOLD_ABSOLUTE: int = 5
"""Default minimum absolute change for density stability check.

Changes smaller than this (in number of observations) are always
tolerated, regardless of the relative change.  This prevents tiny
bands (e.g., 3 observations) from invalidating the cache when a
single new point arrives.
"""


# ---------------------------------------------------------------------------
# Cache data type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CachedOffsets:
    """Deserialized offset cache record from DynamoDB.

    Attributes
    ----------
    band_offsets:
        Mapping from band display label to offset magnitude.
    band_observation_counts:
        Mapping from band display label to observation count in the
        subsampled dataset that produced these offsets.
    band_set_hash:
        SHA-256 hex digest of the sorted band label list.
    computed_at:
        ISO 8601 UTC timestamp of when the offsets were computed.
    """

    band_offsets: dict[str, float]
    band_observation_counts: dict[str, int]
    band_set_hash: str
    computed_at: str


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _compute_band_set_hash(band_labels: list[str]) -> str:
    """Compute the SHA-256 hex digest of sorted, comma-joined band labels."""
    payload = ",".join(sorted(band_labels))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _decimal_map_to_float(m: dict[str, Any]) -> dict[str, float]:
    """Convert a DynamoDB number map (Decimal values) to float."""
    return {k: float(v) for k, v in m.items()}


def _decimal_map_to_int(m: dict[str, Any]) -> dict[str, int]:
    """Convert a DynamoDB number map (Decimal values) to int."""
    return {k: int(v) for k, v in m.items()}


def _float_map_to_decimal(m: dict[str, float]) -> dict[str, Decimal]:
    """Convert a float map to Decimal for DynamoDB writes."""
    return {k: Decimal(str(v)) for k, v in m.items()}


def _int_map_to_decimal(m: dict[str, int]) -> dict[str, Decimal]:
    """Convert an int map to Decimal for DynamoDB writes."""
    return {k: Decimal(str(v)) for k, v in m.items()}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def read_offset_cache(
    table: Any,
    nova_id: str,
    regime: str,
) -> CachedOffsets | None:
    """Read cached offsets from DynamoDB.

    Parameters
    ----------
    table:
        boto3 DynamoDB Table resource for the main NovaCat table.
    nova_id:
        Nova identifier (partition key).
    regime:
        Wavelength regime (e.g., ``"optical"``).

    Returns
    -------
    CachedOffsets | None:
        The cached record, or ``None`` if no cache exists or the read
        fails.
    """
    try:
        response: dict[str, Any] = table.get_item(
            Key={"PK": nova_id, "SK": f"{_SK_PREFIX}{regime}"},
        )
    except Exception:
        logger.warning(
            "Failed to read offset cache for nova=%s regime=%s",
            nova_id,
            regime,
            exc_info=True,
        )
        return None

    item: dict[str, Any] | None = response.get("Item")
    if item is None:
        logger.debug(
            "No offset cache found for nova=%s regime=%s",
            nova_id,
            regime,
        )
        return None

    try:
        return CachedOffsets(
            band_offsets=_decimal_map_to_float(item.get("band_offsets", {})),
            band_observation_counts=_decimal_map_to_int(item.get("band_observation_counts", {})),
            band_set_hash=str(item.get("band_set_hash", "")),
            computed_at=str(item.get("computed_at", "")),
        )
    except (TypeError, ValueError):
        logger.warning(
            "Corrupt offset cache for nova=%s regime=%s; treating as miss",
            nova_id,
            regime,
            exc_info=True,
        )
        return None


def is_cache_valid(
    cached: CachedOffsets,
    current_band_counts: dict[str, int],
    *,
    density_threshold_relative: float = DEFAULT_DENSITY_THRESHOLD_RELATIVE,
    density_threshold_absolute: int = DEFAULT_DENSITY_THRESHOLD_ABSOLUTE,
) -> bool:
    """Evaluate whether cached offsets are still valid.

    Implements the two-step invalidation heuristic from DESIGN-003 §8.7.

    Parameters
    ----------
    cached:
        Previously cached offset data.
    current_band_counts:
        Current per-band observation counts in the subsampled dataset.
    density_threshold_relative:
        Maximum allowed relative change in any band's count (0.0–1.0).
    density_threshold_absolute:
        Minimum absolute change required to trigger invalidation.
        Changes smaller than this are always tolerated.

    Returns
    -------
    bool:
        ``True`` if the cached offsets can be reused; ``False`` if they
        should be recomputed.
    """
    # Step 1: Band set check.
    current_hash = _compute_band_set_hash(list(current_band_counts.keys()))
    if current_hash != cached.band_set_hash:
        logger.info(
            "Offset cache invalidated: band set changed (cached=%s, current=%s)",
            cached.band_set_hash[:12],
            current_hash[:12],
        )
        return False

    # Step 2: Density stability check.
    for band, current_count in current_band_counts.items():
        cached_count = cached.band_observation_counts.get(band, 0)
        abs_change = abs(current_count - cached_count)

        # Small absolute changes are always tolerated.
        if abs_change < density_threshold_absolute:
            continue

        # Guard against division by zero (new band — should have been
        # caught by the hash check, but defend in depth).
        if cached_count == 0:
            logger.info(
                "Offset cache invalidated: band %r has no cached count",
                band,
            )
            return False

        rel_change = abs_change / cached_count
        if rel_change > density_threshold_relative:
            logger.info(
                "Offset cache invalidated: band %r count changed by %.1f%% (%d → %d)",
                band,
                rel_change * 100,
                cached_count,
                current_count,
            )
            return False

    logger.debug("Offset cache is valid; reusing cached offsets")
    return True


def write_offset_cache(
    table: Any,
    nova_id: str,
    regime: str,
    offsets: list[BandOffsetResult],
    band_counts: dict[str, int],
) -> None:
    """Write computed offsets to the DynamoDB cache.

    Uses unconditional ``PutItem`` (last-write-wins), consistent with
    the single-writer model of the Fargate artifact generator.

    Parameters
    ----------
    table:
        boto3 DynamoDB Table resource for the main NovaCat table.
    nova_id:
        Nova identifier (partition key).
    regime:
        Wavelength regime (e.g., ``"optical"``).
    offsets:
        Computed offset results to cache.
    band_counts:
        Per-band observation counts in the subsampled dataset.
    """
    band_offsets = {r.band_id: r.offset_mag for r in offsets}
    band_set_hash = _compute_band_set_hash(list(band_counts.keys()))

    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": f"{_SK_PREFIX}{regime}",
        "band_offsets": _float_map_to_decimal(band_offsets),
        "band_observation_counts": _int_map_to_decimal(band_counts),
        "band_set_hash": band_set_hash,
        "computed_at": _now_iso(),
    }

    try:
        table.put_item(Item=item)
        logger.info(
            "Offset cache written for nova=%s regime=%s (%d bands)",
            nova_id,
            regime,
            len(offsets),
        )
    except Exception:
        # Cache write failure is non-fatal — the offsets were already
        # computed and will be used for this generation.  The next
        # generation will recompute them (cache miss).
        logger.warning(
            "Failed to write offset cache for nova=%s regime=%s",
            nova_id,
            regime,
            exc_info=True,
        )


def cached_to_results(cached: CachedOffsets) -> list[BandOffsetResult]:
    """Convert cached offsets back to a list of BandOffsetResult.

    This is used when the cache is valid and the full offset computation
    can be skipped.

    Parameters
    ----------
    cached:
        Valid cached offset data.

    Returns
    -------
    list[BandOffsetResult]:
        Reconstructed results, sorted by band identifier.

    Raises
    ------
    ValueError
        If any cached offset violates the half-integer constraint
        (indicates data corruption in the cache).
    """
    results: list[BandOffsetResult] = []
    for band_id, offset_mag in cached.band_offsets.items():
        direction = OffsetDirection.fainter if offset_mag > 0.0 else OffsetDirection.none
        results.append(
            BandOffsetResult(
                band_id=band_id,
                offset_mag=offset_mag,
                offset_direction=direction,
            )
        )
    return sorted(results, key=lambda r: r.band_id)
