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
"""

from __future__ import annotations

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

    Returns
    -------
    tuple[float | None, bool]
        ``(outburst_mjd, outburst_mjd_is_estimated)``.  Returns
        ``(None, False)`` when no discovery date exists and no
        observations are available.
    """
    # Recurrent novae always use the earliest-observation fallback,
    # regardless of whether discovery_date is present (§7.6).
    if nova_type != "recurrent" and discovery_date is not None:
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
