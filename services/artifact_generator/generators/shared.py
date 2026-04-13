"""Shared utilities for artifact generators (DESIGN-003 Epic 3).

Pure-computation helpers consumed by multiple generators.  No AWS
dependencies — these are testable without moto.

Functions
---------
resolve_outburst_mjd
    §7.6 — Discovery date → MJD conversion with imprecise-date
    handling, recurrent-nova fallback, and earliest-observation
    estimation.

format_coordinates
    §5.3 — RA/DEC decimal degrees → sexagesimal strings via astropy.

generated_at_timestamp
    ISO 8601 UTC timestamp for artifact ``generated_at`` fields.

lttb
    §9.4 — Largest-Triangle-Three-Buckets downsampling for sparklines.

trim_dead_edges
    Remove detector rolloff dead edges from spectral arrays.

remove_interior_dead_runs
    Remove interior runs of consecutive near-zero flux (chip gaps).

reject_chip_gap_artifacts
    Remove interpolated chip gap artifacts from spectral arrays.

segment_aware_lttb
    LTTB downsampling wrapper for contiguous spectra.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from datetime import UTC, datetime

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

# ---------------------------------------------------------------------------
# §7.6 — Outburst MJD resolution
# ---------------------------------------------------------------------------


def resolve_outburst_mjd(
    discovery_date: str | None,
    nova_type: str | None,
    observation_epochs_mjd: Sequence[float],
    outburst_date: str | None = None,
) -> tuple[float | None, bool]:
    """Resolve the outburst reference MJD for a nova.

    This computation is performed once per nova in the Fargate per-nova
    loop, before any generator runs.  The result is passed to
    ``spectra.json``, ``photometry.json``, and ``sparkline.svg``
    generators.

    Parameters
    ----------
    discovery_date
        ``YYYY-MM-DD`` string from the Nova DDB item.  Uses the ``00``
        convention for missing precision: day ``00`` defaults to the 1st,
        month+day ``00-00`` defaults to January 1st.  ``None`` if no
        discovery date has been resolved.
    nova_type
        Nova classification from the Nova DDB item (e.g. ``"recurrent"``).
        ``None`` for unclassified novae.
    observation_epochs_mjd
        Combined MJD epoch values from both spectra
        (``observation_date_mjd``) and photometry (``time_mjd``) items.
        Pre-queried by the caller.
    outburst_date
        Operator-injected precise ``YYYY-MM-DD`` outburst date.  When
        present (and the nova is not recurrent), takes priority over
        *discovery_date*.  ``None`` by default.

    Returns
    -------
    tuple[float | None, bool]
        ``(outburst_mjd, outburst_mjd_is_estimated)``.  Returns
        ``(None, False)`` when no discovery date exists and no
        observations are available.
    """
    # Recurrent novae always use the earliest-observation fallback,
    # regardless of whether discovery_date or outburst_date is present (§7.6).
    if nova_type != "recurrent":
        # Priority 1: operator-injected outburst_date
        if outburst_date is not None:
            return _outburst_from_discovery_date(outburst_date), False
        # Priority 2: ADS-derived discovery_date
        if discovery_date is not None:
            return _outburst_from_discovery_date(discovery_date), False

    # Fallback — earliest observation minus 1 day.
    # Places the estimated outburst so the earliest observation becomes
    # approximately Day 1 on DPO axes (avoids Day 0 on log scales).
    if observation_epochs_mjd:
        min_epoch = min(observation_epochs_mjd)
        return min_epoch - 1.0, True

    # No discovery date and no observations — should not occur for a
    # nova with a WorkItem, but handled defensively.
    return None, False


def discovery_date_to_mjd(discovery_date: str) -> float:
    """Convert a ``YYYY-MM-DD`` discovery date string to MJD.

    Handles the ``00`` convention for imprecise dates (same as
    ``resolve_outburst_mjd``): day ``00`` → 1st, month+day ``00-00``
    → January 1st.
    """
    return _outburst_from_discovery_date(discovery_date)


def _outburst_from_discovery_date(discovery_date: str) -> float:
    """Parse a ``YYYY-MM-DD`` discovery date to MJD.

    Handles the ``00`` convention for imprecise dates:

    - Day component ``00`` → default to the 1st of the month.
    - Month and day components both ``00`` → default to January 1st.
    """
    parts = discovery_date.split("-")
    year = parts[0]
    month = parts[1] if len(parts) > 1 else "01"
    day = parts[2] if len(parts) > 2 else "01"

    # Handle imprecise dates per §7.6.
    if month == "00":
        month = "01"
        day = "01"
    elif day == "00":
        day = "01"

    t = Time(f"{year}-{month}-{day}T00:00:00", format="isot", scale="utc")
    return float(t.mjd)


# ---------------------------------------------------------------------------
# §5.3 — Coordinate formatting
# ---------------------------------------------------------------------------


def format_coordinates(ra_deg: float, dec_deg: float) -> tuple[str, str]:
    """Convert decimal-degree coordinates to sexagesimal strings.

    Parameters
    ----------
    ra_deg
        Right ascension in ICRS decimal degrees.
    dec_deg
        Declination in ICRS decimal degrees.

    Returns
    -------
    tuple[str, str]
        ``(ra_str, dec_str)`` where *ra_str* is ``HH:MM:SS.ss`` and
        *dec_str* is ``±DD:MM:SS.s``.
    """
    coord = SkyCoord(ra=ra_deg, dec=dec_deg, unit="deg", frame="icrs")
    ra_str: str = coord.ra.to_string(
        unit=u.hour,
        sep=":",
        precision=2,
        pad=True,
    )
    dec_str: str = coord.dec.to_string(
        unit=u.degree,
        sep=":",
        precision=1,
        pad=True,
        alwayssign=True,
    )
    return ra_str, dec_str


# ---------------------------------------------------------------------------
# generated_at timestamp
# ---------------------------------------------------------------------------


def generated_at_timestamp() -> str:
    """Return the current UTC time as an ISO 8601 string.

    Format: ``YYYY-MM-DDTHH:MM:SSZ`` (no fractional seconds).
    Used for the ``generated_at`` field on all artifact schemas.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# §9.4 — LTTB downsampling
# ---------------------------------------------------------------------------


def lttb(
    points: list[tuple[float, float]],
    threshold: int,
) -> list[tuple[float, float]]:
    """Largest-Triangle-Three-Buckets downsampling (Steinarsson 2013).

    Purpose-built downsampling for time-series visual fidelity.  Divides
    the data into *threshold* buckets and selects one point per bucket —
    the point that forms the largest triangle with the selected point
    from the previous bucket and the average point of the next bucket.
    This preferentially preserves peaks, troughs, and inflection points.

    The first and last data points are always retained.

    Parameters
    ----------
    points
        Time-ordered ``(x, y)`` pairs — typically ``(time_mjd, magnitude)``.
    threshold
        Target number of output points.  Must be ≥ 3 for downsampling
        to occur.

    Returns
    -------
    list[tuple[float, float]]
        Downsampled points.  If ``len(points) <= threshold`` or
        ``threshold < 3``, a copy of *points* is returned unchanged.
    """
    n = len(points)
    if n <= threshold or threshold < 3:
        return list(points)

    sampled: list[tuple[float, float]] = [points[0]]

    # Split the n-2 interior points into (threshold - 2) buckets.
    bucket_size = (n - 2) / (threshold - 2)

    for bucket_idx in range(threshold - 2):
        # Current bucket boundaries (indices into *points*).
        curr_start = int(math.floor(bucket_idx * bucket_size)) + 1
        curr_end = int(math.floor((bucket_idx + 1) * bucket_size)) + 1

        # Compute the average point of the *next* bucket (or the final
        # point for the last bucket).
        if bucket_idx < threshold - 3:
            next_start = curr_end
            next_end = int(math.floor((bucket_idx + 2) * bucket_size)) + 1
            next_end = min(next_end, n - 1)
            span = max(next_end - next_start, 1)
            avg_x = sum(points[j][0] for j in range(next_start, next_end)) / span
            avg_y = sum(points[j][1] for j in range(next_start, next_end)) / span
        else:
            avg_x = points[-1][0]
            avg_y = points[-1][1]

        # Select the point in the current bucket that maximises the
        # triangle area with the previously selected point and the next
        # bucket's average.  The 0.5 scale factor is omitted since we
        # only compare relative magnitudes.
        prev = sampled[-1]
        best_area = -1.0
        best_idx = curr_start

        for j in range(curr_start, min(curr_end, n - 1)):
            area = abs(
                (prev[0] - avg_x) * (points[j][1] - prev[1])
                - (prev[0] - points[j][0]) * (avg_y - prev[1])
            )
            if area > best_area:
                best_area = area
                best_idx = j

        sampled.append(points[best_idx])

    sampled.append(points[-1])
    return sampled


# ---------------------------------------------------------------------------
# Spectral cleaning utilities (extracted from spectra.py for ADR-033)
# ---------------------------------------------------------------------------

_logger = logging.getLogger(__name__)

RELATIVE_ZERO_FRACTION = 1e-6  # fraction of peak flux below which values are "dead"
CHIP_GAP_FLUX_FRACTION: float = 0.1  # fraction of median abs flux below which flux is "near zero"
GAP_FACTOR: float = 5.0  # wavelength step multiplier to detect chip gap isolation
LTTB_THRESHOLD = 2000  # max points per spectrum (DESIGN-003 §7.9, P-4)


def trim_dead_edges(
    wavelengths: list[float],
    fluxes: list[float],
    data_product_id: str,
) -> tuple[list[float], list[float]]:
    """Remove runs of >1 consecutive near-zero flux from each edge.

    Detector sensitivity roll-off produces dead edges where flux drops
    to zero and stays there for several nm.  A single zero at the edge
    is left alone — it could be legitimate signal.

    Both arrays are trimmed together to stay aligned.
    """
    n = len(fluxes)
    if n == 0:
        return wavelengths, fluxes

    peak = max(abs(f) for f in fluxes)
    if peak == 0.0:
        return [], []

    threshold = peak * RELATIVE_ZERO_FRACTION

    # --- blue (low-wavelength) edge ---
    blue_zeros = 0
    for f in fluxes:
        if abs(f) < threshold:
            blue_zeros += 1
        else:
            break
    blue_trim = blue_zeros if blue_zeros >= 1 else 0

    # --- red (high-wavelength) edge ---
    red_zeros = 0
    for f in reversed(fluxes):
        if abs(f) < threshold:
            red_zeros += 1
        else:
            break
    red_trim = red_zeros if red_zeros >= 1 else 0

    if blue_trim or red_trim:
        _logger.debug(
            "Trimmed dead spectral edges",
            extra={
                "data_product_id": data_product_id,
                "blue_points_removed": blue_trim,
                "red_points_removed": red_trim,
            },
        )

    end = n - red_trim if red_trim else n
    return wavelengths[blue_trim:end], fluxes[blue_trim:end]


def remove_interior_dead_runs(
    wavelengths: list[float],
    fluxes: list[float],
    data_product_id: str,
    min_run: int = 3,
) -> tuple[list[float], list[float]]:
    """Remove interior runs of consecutive near-zero flux (chip gaps)."""
    peak = max(abs(f) for f in fluxes)
    if peak == 0:
        return wavelengths, fluxes

    threshold = peak * RELATIVE_ZERO_FRACTION

    # Mark each point as dead or alive
    alive = [abs(f) >= threshold for f in fluxes]

    # Keep all points except interior dead runs of length >= min_run
    keep = [True] * len(fluxes)
    run_start = None
    for i, is_alive in enumerate(alive):
        if not is_alive:
            if run_start is None:
                run_start = i
        else:
            if run_start is not None:
                run_len = i - run_start
                # Only remove interior runs (not touching edges)
                if run_len >= min_run and run_start > 0:
                    for j in range(run_start, i):
                        keep[j] = False
                run_start = None

    # Filter
    out_wl = [w for w, k in zip(wavelengths, keep, strict=False) if k]
    out_fx = [f for f, k in zip(fluxes, keep, strict=False) if k]
    return out_wl, out_fx


def reject_chip_gap_artifacts(
    wavelengths: list[float],
    fluxes: list[float],
    data_product_id: str,
) -> tuple[list[float], list[float]]:
    """Remove interpolated chip gap artifacts.

    Chip gaps produce isolated points at irregular wavelength spacing
    with near-zero flux. These are reduction pipeline artifacts, not
    real spectral data.
    """
    n = len(wavelengths)
    if n < 3:
        return wavelengths, fluxes

    # Compute median wavelength step
    steps = [wavelengths[i + 1] - wavelengths[i] for i in range(n - 1)]
    median_step = sorted(steps)[len(steps) // 2]  # simple median

    # Compute median absolute flux (excluding zeros)
    abs_fluxes = [abs(f) for f in fluxes if f != 0.0]
    if not abs_fluxes:
        return wavelengths, fluxes
    median_flux = sorted(abs_fluxes)[len(abs_fluxes) // 2]

    flux_threshold = median_flux * CHIP_GAP_FLUX_FRACTION
    gap_threshold = median_step * GAP_FACTOR

    # Identify chip gap artifacts
    keep_wl: list[float] = []
    keep_fx: list[float] = []
    removed = 0

    for i in range(n):
        # Check wavelength isolation
        gap_left = (wavelengths[i] - wavelengths[i - 1]) if i > 0 else 0.0
        gap_right = (wavelengths[i + 1] - wavelengths[i]) if i < n - 1 else 0.0
        is_isolated = gap_left > gap_threshold or gap_right > gap_threshold

        # Check near-zero flux
        is_near_zero = abs(fluxes[i]) < flux_threshold

        if is_isolated and is_near_zero:
            removed += 1
            continue

        keep_wl.append(wavelengths[i])
        keep_fx.append(fluxes[i])

    if removed > 0:
        _logger.debug(
            "Removed chip gap artifacts",
            extra={
                "data_product_id": data_product_id,
                "points_removed": removed,
                "median_step_nm": round(median_step, 4),
                "gap_threshold_nm": round(gap_threshold, 4),
            },
        )

    return keep_wl, keep_fx


def segment_aware_lttb(
    wavelengths: list[float],
    fluxes: list[float],
) -> tuple[list[float], list[float]]:
    """Run single-pass LTTB downsampling on a contiguous spectrum."""
    if len(wavelengths) <= LTTB_THRESHOLD:
        return wavelengths, fluxes

    points = list(zip(wavelengths, fluxes, strict=True))
    downsampled = lttb(points, LTTB_THRESHOLD)
    return [p[0] for p in downsampled], [p[1] for p in downsampled]
