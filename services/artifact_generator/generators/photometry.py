"""photometry.json artifact generator (DESIGN-003 §8).

Generates the per-nova photometry artifact consumed by the light curve
panel component.  Carries all data required to render the multi-regime,
multi-band light curve as defined in ADR-013, with all backend
computations pre-applied.  No heavy computation is deferred to the
frontend.

This is the most computationally intensive per-nova artifact generator.

Input sources (§8.2):
    Dedicated photometry table — PhotometryRow items (``PHOT#<row_id>``).
    Band registry — display labels and effective wavelengths.
    Per-nova context — ``outburst_mjd`` and ``outburst_mjd_is_estimated``.
    Main NovaCat table — offset cache read/write (§8.7).

Output:
    ADR-014 ``photometry.json`` schema (``schema_version "1.1"``).

Side effects on *nova_context*:
    ``photometry_count``        — int, observations in the published artifact.
    ``photometry_raw_items``    — list[dict], full unfiltered DDB items (for bundle §10).
    ``photometry_observations`` — list[dict], processed observation records (for sparkline §9).
    ``photometry_bands``        — list[dict], band metadata records (for sparkline §9).
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from decimal import Decimal
from typing import Any

import numpy as np
from boto3.dynamodb.conditions import Key

from generators.offsets import (
    BandObservations,
    compute_band_offsets,
    is_cache_valid,
    read_offset_cache,
    write_offset_cache,
)
from generators.shared import generated_at_timestamp

_logger = logging.getLogger("artifact_generator")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = "1.1"  # §8.12: outburst_mjd_is_estimated addition
_MAX_POINTS_PER_REGIME = 500  # ADR-013 subsampling cap
_LARGE_ERROR_THRESHOLD = 1.0  # mag_err above this → treat as upper limit

# Recognised DDB regimes and their mapping to ADR-014 output regimes.
_REGIME_MAP: dict[str, str] = {
    "optical": "optical",
    "uv": "optical",
    "nir": "optical",
    "mir": "optical",
    "xray": "xray",
    "gamma": "gamma",
    "radio": "radio",
}

# ADR-014 regime sort order for the observations array.
_REGIME_SORT_ORDER: dict[str, int] = {
    "optical": 0,
    "xray": 1,
    "gamma": 2,
    "radio": 3,
}

# ADR-014 regime definitions (§8.9).
_REGIME_DEFINITIONS: dict[str, dict[str, Any]] = {
    "optical": {
        "id": "optical",
        "label": "Optical",
        "y_axis_label": "Magnitude",
        "y_axis_inverted": True,
        "y_axis_scale_default": "linear",
    },
    "xray": {
        "id": "xray",
        "label": "X-ray",
        "y_axis_label": "Count rate (cts/s)",
        "y_axis_inverted": False,
        "y_axis_scale_default": "linear",
    },
    "gamma": {
        "id": "gamma",
        "label": "Gamma-ray",
        "y_axis_label": "Photon flux (ph/cm²/s)",
        "y_axis_inverted": False,
        "y_axis_scale_default": "linear",
    },
    "radio": {
        "id": "radio",
        "label": "Radio / Sub-mm",
        "y_axis_label": "Flux density (mJy)",
        "y_axis_inverted": False,
        "y_axis_scale_default": "log",
    },
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_photometry_json(
    nova_id: str,
    photometry_table: Any,
    main_table: Any,
    band_registry: dict[str, Any],
    nova_context: dict[str, Any],
) -> dict[str, Any]:
    """Generate the ``photometry.json`` artifact for a nova.

    Parameters
    ----------
    nova_id
        Nova UUID string.
    photometry_table
        boto3 DynamoDB Table resource for the **dedicated** photometry
        table (not the main NovaCat table).
    main_table
        boto3 DynamoDB Table resource for the main NovaCat table.
        Used for offset cache read/write (§8.7).
    band_registry
        Mapping from ``band_id`` to registry entry dict.  Each entry
        should have ``band_name`` (str | None) and ``lambda_eff``
        (float | None, in Ångströms).
    nova_context
        Mutable dict accumulating per-nova state.  Must already contain
        ``outburst_mjd`` and ``outburst_mjd_is_estimated``.

    Returns
    -------
    dict[str, Any]
        Complete ``photometry.json`` artifact conforming to ADR-014.
    """
    outburst_mjd: float | None = nova_context.get("outburst_mjd")
    outburst_mjd_is_estimated: bool = nova_context.get(
        "outburst_mjd_is_estimated",
        False,
    )

    # ------------------------------------------------------------------
    # Step 1 — Query all PhotometryRow items.
    # ------------------------------------------------------------------
    raw_items = _query_photometry_rows(nova_id, photometry_table)

    # Stash the full unfiltered dataset for the bundle generator (§10.2).
    nova_context["photometry_raw_items"] = raw_items

    if not raw_items:
        _logger.info(
            "No photometry data for nova",
            extra={"nova_id": nova_id, "phase": "generate_photometry"},
        )
        return _empty_artifact(nova_id, outburst_mjd, outburst_mjd_is_estimated, nova_context)

    # ------------------------------------------------------------------
    # Step 2 — Classify: map regimes, resolve bands, filter invalid rows.
    # ------------------------------------------------------------------
    classified = _classify_rows(raw_items, band_registry, nova_id)

    if not classified:
        return _empty_artifact(nova_id, outburst_mjd, outburst_mjd_is_estimated, nova_context)

    # ------------------------------------------------------------------
    # Step 3 — Per-regime processing pipeline.
    # ------------------------------------------------------------------
    # Group by output regime.
    by_regime: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in classified:
        by_regime[row["_output_regime"]].append(row)

    all_observations: list[dict[str, Any]] = []
    all_bands: dict[str, dict[str, Any]] = {}  # keyed by display label

    for regime_id in sorted(by_regime, key=lambda r: _REGIME_SORT_ORDER.get(r, 99)):
        regime_rows = by_regime[regime_id]

        # 3a-pre — Auto-flag large-error optical points as upper limits (P2).
        regime_rows = _auto_flag_large_errors(regime_rows, regime_id, nova_id)

        # 3a — Upper limit suppression (per band within this regime).
        after_suppression = _suppress_upper_limits(regime_rows)

        # 3b — Density-preserving log subsampling.
        after_subsampling = _subsample_regime(after_suppression)

        # 3c — Compute band offsets (ADR-032 / §8.7).
        band_offsets = _compute_band_offsets(
            after_subsampling,
            regime_id,
            nova_id,
            main_table,
        )

        # 3d — Build observation records and collect band metadata.
        for row in after_subsampling:
            display_label = row["_display_label"]
            obs = _build_observation_record(row, outburst_mjd, band_offsets)
            all_observations.append(obs)

            # Collect band metadata (first occurrence wins).
            if display_label not in all_bands:
                all_bands[display_label] = _build_band_record(
                    row,
                    band_offsets.get(display_label, 0.0),
                )

    # ------------------------------------------------------------------
    # Step 4 — Sort observations: regime order, then epoch ascending.
    # ------------------------------------------------------------------
    all_observations.sort(
        key=lambda o: (_REGIME_SORT_ORDER.get(o["regime"], 99), o["epoch_mjd"]),
    )

    # ------------------------------------------------------------------
    # Step 5 — Build regime metadata (only for regimes with data).
    # ------------------------------------------------------------------
    regime_records = _build_regime_records(all_bands)

    # ------------------------------------------------------------------
    # Step 6 — Sort bands: wavelength ascending within regime, null last.
    # ------------------------------------------------------------------
    band_records = sorted(
        all_bands.values(),
        key=lambda b: (
            _REGIME_SORT_ORDER.get(b["regime"], 99),
            b["wavelength_eff_nm"] if b["wavelength_eff_nm"] is not None else float("inf"),
            b["band"],
        ),
    )

    # ------------------------------------------------------------------
    # Step 7 — Update nova_context.
    # ------------------------------------------------------------------
    nova_context["photometry_count"] = len(all_observations)
    nova_context["photometry_observations"] = all_observations
    nova_context["photometry_bands"] = band_records

    _logger.info(
        "Generated photometry.json",
        extra={
            "nova_id": nova_id,
            "raw_rows": len(raw_items),
            "output_observations": len(all_observations),
            "regimes": list(by_regime.keys()),
            "phase": "generate_photometry",
        },
    )

    return {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at_timestamp(),
        "nova_id": nova_id,
        "outburst_mjd": outburst_mjd,
        "outburst_mjd_is_estimated": outburst_mjd_is_estimated,
        "regimes": regime_records,
        "bands": band_records,
        "observations": all_observations,
    }


# ---------------------------------------------------------------------------
# DynamoDB query
# ---------------------------------------------------------------------------


def _query_photometry_rows(
    nova_id: str,
    table: Any,
) -> list[dict[str, Any]]:
    """Query all ``PHOT#`` items for *nova_id* with pagination."""
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": (Key("PK").eq(nova_id) & Key("SK").begins_with("PHOT#")),
    }
    while True:
        response: dict[str, Any] = table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


# ---------------------------------------------------------------------------
# Classification: regime mapping, band resolution, row filtering
# ---------------------------------------------------------------------------


def _classify_rows(
    raw_items: list[dict[str, Any]],
    band_registry: dict[str, Any],
    nova_id: str,
) -> list[dict[str, Any]]:
    """Annotate each row with output regime, display label, and measurement routing.

    Rows with unrecognised regimes or missing measurement values are
    excluded.  Annotations are stored as underscore-prefixed keys on a
    *copy* of the original dict.
    """
    result: list[dict[str, Any]] = []

    for item in raw_items:
        ddb_regime: str = item.get("regime", "")
        output_regime = _REGIME_MAP.get(ddb_regime)

        if output_regime is None:
            _logger.warning(
                "Unrecognised regime — excluding observation",
                extra={
                    "nova_id": nova_id,
                    "row_id": item.get("row_id"),
                    "regime": ddb_regime,
                },
            )
            continue

        # Check that at least one measurement value is present.
        if not _has_measurement(item):
            _logger.warning(
                "Observation has no measurement value — excluding",
                extra={"nova_id": nova_id, "row_id": item.get("row_id")},
            )
            continue

        # Resolve band display label.
        band_id: str = item.get("band_id", "unknown")
        display_label, wavelength_eff_nm = _resolve_band(
            band_id,
            band_registry,
            nova_id,
        )

        # Prefer stored band_name when present (ADR-019 amendment).
        # Falls back to registry-derived label for pre-migration rows.
        stored_band_name = item.get("band_name")
        if stored_band_name:
            display_label = str(stored_band_name)

        # Annotate.
        row = dict(item)
        row["_output_regime"] = output_regime
        row["_display_label"] = display_label
        row["_wavelength_eff_nm"] = wavelength_eff_nm
        result.append(row)

    return result


def _has_measurement(item: dict[str, Any]) -> bool:
    """Return True if the item has at least one non-null measurement field."""
    for field in ("magnitude", "flux_density", "count_rate", "photon_flux", "limiting_value"):
        val = item.get(field)
        if val is not None:
            return True
    return False


def _resolve_band(
    band_id: str,
    band_registry: dict[str, Any],
    nova_id: str,
) -> tuple[str, float | None]:
    """Resolve *band_id* to a display label and effective wavelength.

    Returns ``(display_label, wavelength_eff_nm)``.  Falls back to the
    raw ``band_id`` string when the registry has no entry.

    Post ADR-019 amendment (2026-04-03), the display label returned here
    serves as a **fallback** for pre-migration rows that lack a stored
    ``band_name``.  Callers should prefer ``item['band_name']`` when present.
    """
    entry = band_registry.get(band_id)
    if entry is None:
        _logger.warning(
            "Band not in registry — using raw band_id as display label",
            extra={"nova_id": nova_id, "band_id": band_id},
        )
        return band_id, None

    # Registry entries may be dicts or pydantic-like objects.
    if isinstance(entry, dict):
        display = entry.get("band_name") or band_id
        lambda_eff = entry.get("lambda_eff")
    else:
        display = getattr(entry, "band_name", None) or band_id
        lambda_eff = getattr(entry, "lambda_eff", None)

    wavelength_nm: float | None = None
    if lambda_eff is not None:
        wavelength_nm = float(lambda_eff) / 10.0  # Ångströms → nm

    return str(display), wavelength_nm


# ---------------------------------------------------------------------------
# Auto-flag large photometry errors as upper limits (P2)
# ---------------------------------------------------------------------------


def _auto_flag_large_errors(
    rows: list[dict[str, Any]],
    regime_id: str,
    nova_id: str,
) -> list[dict[str, Any]]:
    """Flag optical observations with large mag_err as upper limits.

    Observations with ``mag_err > _LARGE_ERROR_THRESHOLD`` that are not
    already flagged as upper limits are treated as upper limits for
    display purposes.  This is a display-layer decision — the DynamoDB
    item is NOT mutated.

    Only applies to the ``"optical"`` output regime (magnitude-based).
    Non-optical regimes are returned unchanged because flux density
    errors are not in magnitudes.
    """
    if regime_id != "optical":
        return rows

    result: list[dict[str, Any]] = []
    for row in rows:
        mag_err = _to_float_or_none(row.get("mag_err"))
        if (
            mag_err is not None
            and mag_err > _LARGE_ERROR_THRESHOLD
            and not row.get("is_upper_limit", False)
        ):
            flagged = dict(row)
            flagged["is_upper_limit"] = True
            _logger.debug(
                "Auto-flagged large-error observation as upper limit",
                extra={
                    "nova_id": nova_id,
                    "band_id": row.get("band_id"),
                    "time_mjd": row.get("time_mjd"),
                    "mag_err": mag_err,
                },
            )
            result.append(flagged)
        else:
            result.append(row)

    return result


# ---------------------------------------------------------------------------
# Upper limit suppression (ADR-013)
# ---------------------------------------------------------------------------


def _suppress_upper_limits(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Remove non-constraining upper limits per band.

    An upper limit is dropped if its magnitude is numerically smaller
    (brighter) than the brightest detection in the same band.  Only
    applies to magnitude-based regimes (optical); for flux-based
    regimes, no suppression is applied.
    """
    # Group by display label.
    by_band: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_band[row["_display_label"]].append(row)

    result: list[dict[str, Any]] = []

    for _band_label, band_rows in by_band.items():
        detections = [
            r
            for r in band_rows
            if not r.get("is_upper_limit", False) and r.get("magnitude") is not None
        ]

        if not detections:
            # No detections — keep all upper limits (no threshold).
            result.extend(band_rows)
            continue

        brightest_mag = min(float(_dec(r["magnitude"])) for r in detections)

        for row in band_rows:
            if row.get("is_upper_limit", False) and row.get("magnitude") is not None:
                ul_mag = float(_dec(row["magnitude"]))
                if ul_mag < brightest_mag:
                    # Non-constraining — drop.
                    continue
            result.append(row)

    return result


# ---------------------------------------------------------------------------
# Density-preserving log subsampling (§8.6)
# ---------------------------------------------------------------------------


def _subsample_regime(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Subsample a regime's observations to the per-regime cap.

    Uses the density-preserving log sampler from §8.6: proportional
    budget allocation across bands, log-spaced time intervals with
    dynamic boundary stretching, midpoint-closest selection preferring
    detections over upper limits.
    """
    if len(rows) <= _MAX_POINTS_PER_REGIME:
        return rows

    # Group by band.
    by_band: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_band[row["_display_label"]].append(row)

    n_bands = len(by_band)
    if n_bands == 0:
        return rows

    # Proportional budget allocation (minimum 1 per band).
    total = len(rows)
    budget: dict[str, int] = {}
    allocated = 0
    for label, band_rows in by_band.items():
        share = max(1, round(len(band_rows) / total * _MAX_POINTS_PER_REGIME))
        budget[label] = share
        allocated += share

    # Adjust if over-allocated (can happen with rounding + minimum).
    while allocated > _MAX_POINTS_PER_REGIME and budget:
        # Reduce the largest band's allocation.
        largest = max(budget, key=lambda k: budget[k])
        if budget[largest] > 1:
            budget[largest] -= 1
            allocated -= 1
        else:
            break

    # Per-band subsampling.
    result: list[dict[str, Any]] = []
    for label, band_rows in by_band.items():
        band_budget = budget.get(label, 1)
        if len(band_rows) <= band_budget:
            result.extend(band_rows)
        else:
            result.extend(_log_subsample_band(band_rows, band_budget))

    return result


def _log_subsample_band(
    rows: list[dict[str, Any]],
    n_target: int,
) -> list[dict[str, Any]]:
    """Select *n_target* representative observations from a single band.

    Uses log-spaced time intervals with dynamic boundary stretching
    and midpoint-closest selection (§8.6).
    """
    if n_target <= 0:
        return []
    if len(rows) <= n_target:
        return list(rows)

    # Sort by time.
    sorted_rows = sorted(rows, key=lambda r: float(_dec(r.get("time_mjd", 0))))

    times = [float(_dec(r.get("time_mjd", 0))) for r in sorted_rows]
    t_min = times[0]
    t_max = times[-1]

    if t_min == t_max:
        # All same epoch — return first n_target.
        return sorted_rows[:n_target]

    # Build log-spaced interval boundaries.
    # log(t - t_min + 1) space to avoid log(0).
    log_min = 0.0  # log(t_min - t_min + 1) = log(1) = 0
    log_max = math.log(t_max - t_min + 1.0)
    boundaries = [
        t_min + math.exp(log_min + i * (log_max - log_min) / n_target) - 1.0
        for i in range(n_target + 1)
    ]
    boundaries[0] = t_min
    boundaries[-1] = t_max + 1e-10  # inclusive of last point

    # Assign rows to intervals.
    intervals: list[list[tuple[int, dict[str, Any]]]] = [[] for _ in range(n_target)]
    for idx, row in enumerate(sorted_rows):
        t = times[idx]
        for iv in range(n_target):
            if boundaries[iv] <= t < boundaries[iv + 1]:
                intervals[iv].append((idx, row))
                break
        else:
            # Edge case: falls exactly on last boundary.
            intervals[-1].append((idx, row))

    # Dynamic boundary stretching: merge empty intervals with nearest non-empty.
    # Then select one representative per non-empty interval.
    selected_indices: set[int] = set()

    for iv in range(n_target):
        candidates = intervals[iv]
        if not candidates:
            continue

        # Interval midpoint in time.
        mid_t = (boundaries[iv] + boundaries[iv + 1]) / 2.0

        # Prefer detections over upper limits.
        detections = [(idx, r) for idx, r in candidates if not r.get("is_upper_limit", False)]
        pool = detections if detections else candidates

        # Select closest to midpoint.
        best_idx, _ = min(pool, key=lambda p: abs(times[p[0]] - mid_t))
        selected_indices.add(best_idx)

    # If we have fewer than n_target due to empty intervals, fill from
    # remaining rows (closest to any unfilled interval midpoint).
    if len(selected_indices) < n_target:
        remaining = [(i, r) for i, r in enumerate(sorted_rows) if i not in selected_indices]
        remaining.sort(key=lambda p: times[p[0]])
        for idx, _ in remaining:
            if len(selected_indices) >= n_target:
                break
            selected_indices.add(idx)

    return [sorted_rows[i] for i in sorted(selected_indices)]


# ---------------------------------------------------------------------------
# Band offset computation (ADR-032 / §8.7)
# ---------------------------------------------------------------------------


def _regime_value(row: dict[str, Any], regime_id: str) -> float | None:
    """Extract the regime-appropriate measurement value from a row.

    Optical uses magnitude; all other regimes use flux_density.
    """
    if regime_id == "optical":
        return _to_float_or_none(row.get("magnitude"))
    return _to_float_or_none(row.get("flux_density"))


def _build_band_observations(
    rows: list[dict[str, Any]],
    regime_id: str,
) -> list[BandObservations]:
    """Build ``BandObservations`` input structs from subsampled rows.

    Groups rows by display label and constructs sorted (mjd, value)
    arrays for each band.  Rows with missing time or measurement
    values are silently skipped.
    """
    by_band: dict[str, list[tuple[float, float]]] = defaultdict(list)

    for row in rows:
        t = _to_float_or_none(row.get("time_mjd"))
        v = _regime_value(row, regime_id)
        if t is None or v is None:
            continue
        by_band[row["_display_label"]].append((t, v))

    result: list[BandObservations] = []
    for label, points in by_band.items():
        if not points:
            continue
        # Sort by time ascending.
        points.sort(key=lambda p: p[0])
        mjd_arr = np.array([p[0] for p in points], dtype=np.float64)
        mag_arr = np.array([p[1] for p in points], dtype=np.float64)
        result.append(BandObservations(band_id=label, mjd=mjd_arr, mag=mag_arr))

    return result


def _compute_band_offsets(
    rows: list[dict[str, Any]],
    regime_id: str,
    nova_id: str,
    main_table: Any,
) -> dict[str, float]:
    """Compute per-band vertical offsets for a regime.

    Implements the full ADR-032 pipeline: spline fitting → gap analysis
    → ordering search → half-integer rounding, with DynamoDB offset
    caching per DESIGN-003 §8.7.

    Parameters
    ----------
    rows
        Subsampled observation rows for one regime.  Each row carries
        ``_display_label``, ``time_mjd``, and regime-appropriate
        measurement fields.
    regime_id
        Output regime identifier (e.g., ``"optical"``).
    nova_id
        Nova UUID — partition key for the offset cache.
    main_table
        boto3 DynamoDB Table resource for the main NovaCat table
        (offset cache storage).

    Returns
    -------
    dict[str, float]
        Mapping from display label to offset magnitude.
    """
    # Collect all display labels present in the subsampled data.
    labels: set[str] = set()
    for row in rows:
        labels.add(row["_display_label"])

    zero_offsets: dict[str, float] = {label: 0.0 for label in labels}

    if not labels:
        return zero_offsets

    # --- Count observations per band (for cache validation) ---
    band_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        band_counts[row["_display_label"]] += 1

    # --- Check offset cache (§8.7) ---
    cached = read_offset_cache(main_table, nova_id, regime_id)
    if cached is not None and is_cache_valid(cached, dict(band_counts)):
        _logger.info(
            "Using cached band offsets",
            extra={
                "nova_id": nova_id,
                "regime": regime_id,
                "cached_at": cached.computed_at,
            },
        )
        # Return cached offsets, filling in zero for any band not in cache.
        return {label: cached.band_offsets.get(label, 0.0) for label in labels}

    # --- Build BandObservations for the offset pipeline ---
    band_obs = _build_band_observations(rows, regime_id)

    if len(band_obs) <= 1:
        # Single band or no fittable data — trivially zero offsets.
        _logger.debug(
            "≤1 band with data; skipping offset computation",
            extra={"nova_id": nova_id, "regime": regime_id},
        )
        return zero_offsets

    # --- Run the offset pipeline (ADR-032) ---
    try:
        results = compute_band_offsets(band_obs)
    except Exception:
        _logger.warning(
            "Offset computation failed; falling back to zero offsets",
            extra={"nova_id": nova_id, "regime": regime_id},
            exc_info=True,
        )
        return zero_offsets

    # --- Map results back to display labels ---
    # BandOffsetResult.band_id is the display label (we used it as band_id
    # when constructing BandObservations).
    offsets: dict[str, float] = {label: 0.0 for label in labels}
    for r in results:
        if r.band_id in offsets:
            offsets[r.band_id] = r.offset_mag

    # --- Write to cache ---
    try:
        write_offset_cache(
            main_table,
            nova_id,
            regime_id,
            results,
            dict(band_counts),
        )
    except Exception:
        # Cache write failure is non-fatal — log and continue.
        _logger.warning(
            "Failed to write offset cache; continuing without caching",
            extra={"nova_id": nova_id, "regime": regime_id},
            exc_info=True,
        )

    _logger.info(
        "Computed band offsets",
        extra={
            "nova_id": nova_id,
            "regime": regime_id,
            "bands": len(band_obs),
            "non_zero": sum(1 for v in offsets.values() if v > 0.0),
        },
    )

    return offsets


# ---------------------------------------------------------------------------
# Output record builders
# ---------------------------------------------------------------------------


def _build_observation_record(
    row: dict[str, Any],
    outburst_mjd: float | None,
    band_offsets: dict[str, float],
) -> dict[str, Any]:
    """Map a classified row to an ADR-014 observation record."""
    epoch_mjd = float(_dec(row.get("time_mjd", 0)))
    regime = row["_output_regime"]
    display_label = row["_display_label"]

    days_since_outburst: float | None = None
    if outburst_mjd is not None:
        days_since_outburst = round(epoch_mjd - outburst_mjd, 4)

    # Regime-specific value routing (§8.8).
    mag, mag_err = None, None
    flux, flux_err = None, None
    count, count_err = None, None
    photon, photon_err = None, None

    if regime == "optical":
        mag = _to_float_or_none(row.get("magnitude"))
        mag_err = _to_float_or_none(row.get("mag_err"))
    elif regime == "xray":
        count = _to_float_or_none(row.get("flux_density"))
        count_err = _to_float_or_none(row.get("flux_density_err"))
    elif regime == "gamma":
        photon = _to_float_or_none(row.get("flux_density"))
        photon_err = _to_float_or_none(row.get("flux_density_err"))
    elif regime == "radio":
        flux = _to_float_or_none(row.get("flux_density"))
        flux_err = _to_float_or_none(row.get("flux_density_err"))

    # Upper-limit fallback: populate the regime's primary field from
    # limiting_value when the normal source field is NULL (non-optical
    # upper limits store the value only in limiting_value).
    if row.get("is_upper_limit", False):
        lv = _to_float_or_none(row.get("limiting_value"))
        if lv is not None:
            if regime == "optical" and mag is None:
                mag = lv
            elif regime == "radio" and flux is None:
                flux = lv
            elif regime == "xray" and count is None:
                count = lv
            elif regime == "gamma" and photon is None:
                photon = lv

    # Provider fallback chain (§8.9): orig_catalog → bibcode → "unknown".
    provider = row.get("orig_catalog") or row.get("bibcode") or "unknown"

    return {
        "observation_id": str(row.get("row_id", "")),
        "epoch_mjd": epoch_mjd,
        "days_since_outburst": days_since_outburst,
        "band": display_label,
        "regime": regime,
        "magnitude": mag,
        "magnitude_error": mag_err,
        "flux_density": flux,
        "flux_density_error": flux_err,
        "count_rate": count,
        "count_rate_error": count_err,
        "photon_flux": photon,
        "photon_flux_error": photon_err,
        "is_upper_limit": bool(row.get("is_upper_limit", False)),
        "provider": str(provider),
        "telescope": str(row.get("telescope") or "unknown"),
        "instrument": str(row.get("instrument") or "unknown"),
    }


def _build_band_record(
    row: dict[str, Any],
    offset: float,
) -> dict[str, Any]:
    """Build an ADR-014 band metadata record from the first occurrence."""
    regime = row["_output_regime"]
    display_label = row["_display_label"]
    wavelength_eff_nm = row.get("_wavelength_eff_nm")

    # Color token: null for X-ray (colored by instrument per ADR-013).
    color_token: str | None = None
    if regime != "xray":
        color_token = f"--color-plot-band-{display_label}"

    return {
        "band": display_label,
        "regime": regime,
        "wavelength_eff_nm": wavelength_eff_nm,
        "vertical_offset": offset,
        "display_color_token": color_token,
    }


def _build_regime_records(
    all_bands: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build regime metadata records for regimes that have data."""
    regimes_present: dict[str, list[str]] = defaultdict(list)
    for label, band_rec in all_bands.items():
        regimes_present[band_rec["regime"]].append(label)

    records: list[dict[str, Any]] = []
    for regime_id in sorted(regimes_present, key=lambda r: _REGIME_SORT_ORDER.get(r, 99)):
        defn = _REGIME_DEFINITIONS.get(regime_id)
        if defn is None:
            continue
        rec = dict(defn)
        rec["bands"] = sorted(
            regimes_present[regime_id],
            key=lambda b: (
                all_bands[b]["wavelength_eff_nm"]
                if all_bands[b]["wavelength_eff_nm"] is not None
                else float("inf"),
                b,
            ),
        )
        records.append(rec)

    return records


# ---------------------------------------------------------------------------
# Empty artifact helper
# ---------------------------------------------------------------------------


def _empty_artifact(
    nova_id: str,
    outburst_mjd: float | None,
    outburst_mjd_is_estimated: bool,
    nova_context: dict[str, Any],
) -> dict[str, Any]:
    """Return a valid photometry.json with empty arrays."""
    nova_context["photometry_count"] = 0
    nova_context["photometry_observations"] = []
    nova_context["photometry_bands"] = []

    return {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at_timestamp(),
        "nova_id": nova_id,
        "outburst_mjd": outburst_mjd,
        "outburst_mjd_is_estimated": outburst_mjd_is_estimated,
        "regimes": [],
        "bands": [],
        "observations": [],
    }


# ---------------------------------------------------------------------------
# Decimal / float helpers
# ---------------------------------------------------------------------------


def _dec(value: Any) -> float:
    """Convert a DynamoDB Decimal (or other numeric) to float."""
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _to_float_or_none(value: Any) -> float | None:
    """Convert to float, returning None for None/missing."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)
