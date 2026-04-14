"""spectra.json artifact generator (DESIGN-003 §7).

Generates the per-nova spectra artifact consumed by the waterfall plot
component.  Carries all data required to render the spectra viewer as
defined in ADR-013, with no computation deferred to the frontend.

Input sources (§7.2):
    Main table — VALID SPECTRA DataProduct items.
    S3 (private bucket) — web-ready CSV files
        (``derived/spectra/<nova_id>/<data_product_id>/web_ready.csv``).
    Per-nova context — ``outburst_mjd`` and ``outburst_mjd_is_estimated``
        from the shared utility (§7.6).

Output:
    ADR-014 ``spectra.json`` schema (``schema_version "1.1"``).

Side effects on *nova_context*:
    ``spectra_count`` — int, number of spectra in the artifact.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import statistics
import time
import uuid
from collections import defaultdict
from decimal import Decimal
from typing import Any

import numpy as np
from boto3.dynamodb.conditions import Attr, Key
from nova_common.spectral import der_snr

from generators.compositing import cluster_by_night
from generators.shared import (
    generated_at_timestamp,
    reject_chip_gap_artifacts,
    remove_interior_dead_runs,
    segment_aware_lttb,
    trim_dead_edges,
)

_logger = logging.getLogger("artifact_generator")

_SCHEMA_VERSION = "1.4"  # ADR-035: xray/uv split, per-regime trimming
_WAVELENGTH_UNIT = "nm"

_FLUX_FLOOR = 1e-4  # minimum normalized flux; prevents log(0) in frontend
_TRIM_TOLERANCE = 1.001  # 0.1% beyond median before wavelength trim kicks in

_ARM_MJD_TOLERANCE = 0.333  # days (~8 hr) — grouping tolerance for arms
_ARM_OVERLAP_MAX_NM = 100.0  # nm — max overlap before we reject a merge

# SNR quality gating (epic/29-spectra-quality-and-docs).
# Absolute floor: spectra below this are excluded from the waterfall plot
# but remain in the observation table. Applied after DER_SNR fallback
# computation so all spectra have an effective SNR value.
_SNR_DISPLAY_FLOOR = 5.0

# ADR-035 spectra wavelength regime boundaries (nm).
# Assignment is by wavelength midpoint: (wavelength_min + wavelength_max) / 2.
_SPECTRA_REGIME_BOUNDARIES: list[tuple[str, float]] = [
    ("xray", 91.0),  # λ_mid < 91 nm  (Lyman limit)
    ("uv", 320.0),  # 91 ≤ λ_mid < 320 nm
    ("optical", 1000.0),  # 320 ≤ λ_mid < 1000 nm
    ("nir", 5000.0),  # 1000 ≤ λ_mid < 5000 nm
    # ("mir", ∞)        # λ_mid ≥ 5000 nm — fallback
]

_SPECTRA_REGIME_SORT_ORDER: dict[str, int] = {
    "xray": 0,
    "uv": 1,
    "optical": 2,
    "nir": 3,
    "mir": 4,
}

_SPECTRA_REGIME_DEFINITIONS: dict[str, dict[str, Any]] = {
    "xray": {
        "id": "xray",
        "label": "X-ray",
        "wavelength_range_nm": [0, 91],
    },
    "uv": {
        "id": "uv",
        "label": "Ultraviolet",
        "wavelength_range_nm": [91, 320],
    },
    "optical": {
        "id": "optical",
        "label": "Optical",
        "wavelength_range_nm": [320, 1000],
    },
    "nir": {
        "id": "nir",
        "label": "Near-IR",
        "wavelength_range_nm": [1000, 5000],
    },
    "mir": {
        "id": "mir",
        "label": "Mid-IR",
        "wavelength_range_nm": [5000, None],
    },
}

# ---------------------------------------------------------------------------
# SNR quality gating
# ---------------------------------------------------------------------------


def _compute_effective_snr(rec: dict[str, Any]) -> float:
    """Compute effective SNR for a parsed spectrum record.

    Priority:
      1. Top-level ``snr`` on the DDB DataProduct item (from FITS validation).
      2. ``hints.snr`` (from SSAP discovery metadata).
      3. DER_SNR computed from the cleaned flux array (Stoehr et al. 2008).

    Returns 0.0 if no SNR can be determined (degenerate spectrum).
    """
    product = rec["product"]

    # 1. Top-level SNR from validation
    snr_val = product.get("snr")
    if snr_val is not None:
        try:
            return float(snr_val)
        except (TypeError, ValueError):
            pass

    # 2. Hints SNR from discovery metadata
    hints = product.get("hints")
    if isinstance(hints, dict):
        hints_snr = hints.get("snr")
        if hints_snr is not None:
            try:
                return float(hints_snr)
            except (TypeError, ValueError):
                pass

    # 3. DER_SNR fallback from cleaned flux array
    fluxes = rec.get("fluxes", [])
    if fluxes:
        return der_snr(fluxes)

    return 0.0


# ADR-035: Cross-boundary spectrum splitting thresholds.
_SPLIT_FRACTION_THRESHOLD = 0.15  # minor side must be ≥15% of total span
_SPLIT_ABSOLUTE_MIN_NM = 45.0  # minor side must be ≥45 nm

# Ordered list of regime boundary wavelengths for splitting checks.
_REGIME_SPLIT_BOUNDARIES: list[float] = [91.0, 320.0, 1000.0, 5000.0]


# ---------------------------------------------------------------------------
# ADR-034: Regime assignment
# ---------------------------------------------------------------------------


def _assign_spectra_regime(wavelength_min: float, wavelength_max: float) -> str:
    """Assign a spectrum to a wavelength regime by midpoint (ADR-034 Decision 2)."""
    midpoint = (wavelength_min + wavelength_max) / 2.0
    for regime_id, upper_bound in _SPECTRA_REGIME_BOUNDARIES:
        if midpoint < upper_bound:
            return regime_id
    return "mir"


# ---------------------------------------------------------------------------
# ADR-035: Cross-boundary splitting and per-regime trimming
# ---------------------------------------------------------------------------


def _split_cross_boundary_spectrum(
    rec: dict[str, Any],
) -> list[dict[str, Any]]:
    """Split a parsed stage-1 record at regime boundaries (ADR-035 Decision 2).

    Returns a list of 1+ records. If no split is needed, returns a
    single-element list containing the original record (unmodified).
    When a split occurs, each fragment gets:
      - Its own sliced wavelength/flux arrays
      - A ``_regime`` key set by midpoint classification of the fragment
      - A ``_split_suffix`` key with the regime id (for spectrum_id construction)

    Boundary-point rule: a data point at exactly a boundary wavelength
    goes to the redder (longer-wavelength) regime.
    """
    wavelengths: list[float] = rec["wavelengths"]
    if not wavelengths:
        return [rec]

    wl_min = wavelengths[0]
    wl_max = wavelengths[-1]
    total_span = wl_max - wl_min

    if total_span <= 0:
        return [rec]

    # Find all boundaries that fall strictly inside the spectrum's range.
    # "Strictly inside" means wl_min < boundary < wl_max (a boundary at
    # the edge doesn't create a split).
    active_boundaries: list[float] = [b for b in _REGIME_SPLIT_BOUNDARIES if wl_min < b < wl_max]

    if not active_boundaries:
        return [rec]

    # Check each boundary for splitting eligibility.
    split_points: list[float] = []
    for boundary in sorted(active_boundaries):
        # Minor side is the smaller portion.
        left_span = boundary - wl_min
        right_span = wl_max - boundary
        minor_span = min(left_span, right_span)

        if (
            minor_span >= _SPLIT_ABSOLUTE_MIN_NM
            and minor_span / total_span >= _SPLIT_FRACTION_THRESHOLD
        ):
            split_points.append(boundary)

    if not split_points:
        return [rec]

    # Perform the split. Build ordered list of cut points including edges.
    cuts = [wl_min] + split_points + [wl_max + 1.0]  # +1 so last segment includes wl_max

    fragments: list[dict[str, Any]] = []
    for seg_idx in range(len(cuts) - 1):
        seg_min = cuts[seg_idx]
        seg_max = cuts[seg_idx + 1]

        seg_wl: list[float] = []
        seg_fx: list[float] = []
        for wl, fx in zip(wavelengths, rec["fluxes"], strict=True):
            # Boundary point goes to the redder regime: use >= for lower bound
            # on all segments except the first (which owns the original blue edge).
            if seg_idx == 0:
                in_segment = wl >= seg_min and wl < seg_max
            else:
                in_segment = wl >= seg_min and wl < seg_max
            if in_segment:
                seg_wl.append(wl)
                seg_fx.append(fx)

        if not seg_wl:
            continue

        frag_regime = _assign_spectra_regime(seg_wl[0], seg_wl[-1])
        fragment: dict[str, Any] = {
            "wavelengths": seg_wl,
            "fluxes": seg_fx,
            "product": rec["product"],
            "nova_id": rec["nova_id"],
            "_regime": frag_regime,
            "_split_suffix": frag_regime,
        }
        fragments.append(fragment)

    dp_id = rec["product"]["data_product_id"]
    _logger.info(
        "Split cross-boundary spectrum",
        extra={
            "data_product_id": dp_id,
            "nova_id": rec["nova_id"],
            "original_range_nm": [wl_min, wl_max],
            "split_points_nm": split_points,
            "fragment_count": len(fragments),
            "fragment_regimes": [f["_regime"] for f in fragments],
        },
    )

    return fragments if fragments else [rec]


def _assign_and_split_regimes(
    parsed: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Classify each parsed spectrum by regime and split cross-boundary spectra.

    ADR-035 Decision 4 steps 5–6. After this function:
      - Every record has a ``_regime`` key.
      - Records that were split have a ``_split_suffix`` key.
      - Records that were NOT split have ``_regime`` set and no ``_split_suffix``.
    """
    result: list[dict[str, Any]] = []

    for rec in parsed:
        wavelengths = rec["wavelengths"]
        if not wavelengths:
            continue

        # Try splitting first.
        fragments = _split_cross_boundary_spectrum(rec)

        if len(fragments) == 1 and "_regime" not in fragments[0]:
            # No split occurred — assign regime to the original record.
            wl_min = wavelengths[0]
            wl_max = wavelengths[-1]
            fragments[0]["_regime"] = _assign_spectra_regime(wl_min, wl_max)

        result.extend(fragments)

    return result


def _trim_per_regime(
    parsed: list[dict[str, Any]],
    nova_id: str,
) -> list[dict[str, Any]]:
    """Apply median-based wavelength trimming independently per regime group.

    ADR-035 Decision 3. This replaces the old global Step 2b trimming.
    For each regime group with ≥ 2 spectra, compute the median blue/red
    edges and trim outliers using the same tolerance as before.
    """
    by_regime: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in parsed:
        by_regime[rec["_regime"]].append(rec)

    result: list[dict[str, Any]] = []

    for regime_id, group in by_regime.items():
        if len(group) < 2:
            # Single spectrum or empty — no trimming, pass through.
            result.extend(group)
            continue

        wl_mins = [s["wavelengths"][0] for s in group if s["wavelengths"]]
        wl_maxes = [s["wavelengths"][-1] for s in group if s["wavelengths"]]

        if not wl_mins or not wl_maxes:
            result.extend(group)
            continue

        display_min = statistics.median(wl_mins)
        display_max = statistics.median(wl_maxes)

        # Warn if trim would affect >50% of spectra in this regime.
        trim_count_red = sum(1 for wmax in wl_maxes if wmax > display_max * _TRIM_TOLERANCE)
        if trim_count_red > len(group) / 2:
            _logger.warning(
                "Red-side wavelength trim affects >50%% of spectra in regime — data may be bimodal",
                extra={
                    "nova_id": nova_id,
                    "regime": regime_id,
                    "trim_count": trim_count_red,
                    "total": len(group),
                },
            )

        trim_count_blue = sum(1 for wmin in wl_mins if wmin < display_min / _TRIM_TOLERANCE)
        if trim_count_blue > len(group) / 2:
            _logger.warning(
                "Blue-side wavelength trim affects >50%% of spectra in regime — data may be bimodal",
                extra={
                    "nova_id": nova_id,
                    "regime": regime_id,
                    "trim_count": trim_count_blue,
                    "total": len(group),
                },
            )

        # Red-side trim.
        for rec in group:
            if not rec["wavelengths"]:
                continue
            if rec["wavelengths"][-1] > display_max * _TRIM_TOLERANCE:
                _trim_wavelength_range(rec, display_max)

        # Drop empties after red-side trim.
        for rec in group:
            if not rec["wavelengths"]:
                _logger.warning(
                    "Spectrum empty after red-side wavelength trim — dropping",
                    extra={
                        "nova_id": nova_id,
                        "regime": regime_id,
                        "data_product_id": rec["product"]["data_product_id"],
                    },
                )
        group = [rec for rec in group if rec["wavelengths"]]

        # Blue-side trim.
        for rec in group:
            if not rec["wavelengths"]:
                continue
            if rec["wavelengths"][0] < display_min / _TRIM_TOLERANCE:
                _trim_wavelength_range_min(rec, display_min)

        # Drop empties after blue-side trim.
        for rec in group:
            if not rec["wavelengths"]:
                _logger.warning(
                    "Spectrum empty after blue-side wavelength trim — dropping",
                    extra={
                        "nova_id": nova_id,
                        "regime": regime_id,
                        "data_product_id": rec["product"]["data_product_id"],
                    },
                )
        group = [rec for rec in group if rec["wavelengths"]]

        result.extend(group)

    return result


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_spectra_json(
    nova_id: str,
    table: Any,
    s3_client: Any,
    private_bucket: str,
    nova_context: dict[str, Any],
) -> dict[str, Any]:
    """Generate the ``spectra.json`` artifact for a nova.

    Parameters
    ----------
    nova_id
        Nova UUID string.
    table
        boto3 DynamoDB Table resource for the main NovaCat table.
    s3_client
        boto3 S3 client (``boto3.client("s3")``).
    private_bucket
        Name of the private S3 bucket containing web-ready CSVs.
    nova_context
        Mutable dict accumulating per-nova state across generators.
        Must already contain ``outburst_mjd`` and
        ``outburst_mjd_is_estimated`` from the shared utility.

    Returns
    -------
    dict[str, Any]
        Complete ``spectra.json`` artifact conforming to ADR-014.
    """
    outburst_mjd: float | None = nova_context.get("outburst_mjd")
    outburst_mjd_is_estimated: bool = nova_context.get("outburst_mjd_is_estimated", False)

    # Step 1 — Query VALID spectra DataProduct items.
    products = _query_valid_spectra(nova_id, table)

    # Capture individual (non-composite) products before filtering.
    # Used for the observation table, which shows all original spectra.
    individual_products = [p for p in products if "COMPOSITE" not in p.get("SK", "")]

    # Step 1b — Post-query filtering: composites replace their constituents.
    products = _filter_composites(products)

    # Step 2a — First pass: parse CSV + trim dead edges for each spectrum.
    parsed: list[dict[str, Any]] = []
    for product in products:
        stage1 = _process_spectrum_stage1(
            nova_id,
            product,
            s3_client,
            private_bucket,
        )
        if stage1 is not None:
            parsed.append(stage1)

    # Step 2a½ — Detect multi-arm groups and merge.
    parsed = _merge_multi_arm_spectra(parsed, nova_id, s3_client, private_bucket)

    # Step 2a¾ — Compute effective SNR and apply absolute display gate.
    # DER_SNR fallback ensures every spectrum gets an SNR value, even if
    # the archive/validation pipeline didn't provide one.  Spectra below
    # the display floor are excluded from the waterfall plot but remain
    # in the observation table (they are real DataProducts with real metadata).
    pre_gate_count = len(parsed)
    gated: list[dict[str, Any]] = []
    for rec in parsed:
        eff_snr = _compute_effective_snr(rec)
        rec["effective_snr"] = eff_snr
        if 0 < eff_snr < _SNR_DISPLAY_FLOOR:
            _logger.info(
                "Spectrum excluded by SNR display gate",
                extra={
                    "nova_id": nova_id,
                    "data_product_id": rec["product"]["data_product_id"],
                    "effective_snr": round(eff_snr, 2),
                    "snr_floor": _SNR_DISPLAY_FLOOR,
                },
            )
            continue
        gated.append(rec)
    if pre_gate_count != len(gated):
        _logger.info(
            "SNR display gate applied",
            extra={
                "nova_id": nova_id,
                "before": pre_gate_count,
                "after": len(gated),
                "excluded": pre_gate_count - len(gated),
            },
        )
    parsed = gated

    # Step 2b — Regime classification and cross-boundary splitting (ADR-035).
    parsed = _assign_and_split_regimes(parsed)

    # Step 2b½ — Per-regime median display range computation and trimming (ADR-035).
    parsed = _trim_per_regime(parsed, nova_id)

    # Step 2c — Second pass: LTTB downsampling + normalization.
    spectra: list[dict[str, Any]] = []
    for rec in parsed:
        record = _process_spectrum_stage2(rec, outburst_mjd)
        if record is not None:
            spectra.append(record)

    # --- ADR-034: Build regime metadata and sort by regime ---
    present_regimes: dict[str, dict[str, Any]] = {}
    for sp in spectra:
        rid = sp["regime"]
        if rid not in present_regimes:
            present_regimes[rid] = dict(_SPECTRA_REGIME_DEFINITIONS[rid])

    regime_records = sorted(
        present_regimes.values(),
        key=lambda r: _SPECTRA_REGIME_SORT_ORDER.get(r["id"], 99),
    )

    # Step 3 — Sort spectra: regime order first, then epoch_mjd within each regime.
    spectra.sort(
        key=lambda s: (
            _SPECTRA_REGIME_SORT_ORDER.get(s["regime"], 99),
            s["epoch_mjd"],
        )
    )

    # Step 4 — Build observations list from individual (pre-filter) products.
    # ADR-033 Decision 5: individual spectra remain visible in the observation
    # table even when replaced by composites in the waterfall plot.
    observations_list: list[dict[str, Any]] = []
    for product in individual_products:
        obs: dict[str, Any] = {
            "data_product_id": product["data_product_id"],
            "instrument": product.get("instrument") or "Unknown",
            "telescope": product.get("telescope") or "Unknown",
            "epoch_mjd": float(Decimal(str(product.get("observation_date_mjd", 0)))),
            "wavelength_min": float(
                Decimal(str(product.get("wavelength_min_nm") or product.get("wavelength_min") or 0))
            ),
            "wavelength_max": float(
                Decimal(str(product.get("wavelength_max_nm") or product.get("wavelength_max") or 0))
            ),
            "provider": product.get("provider", "Unknown"),
        }
        _snr = product.get("snr")
        if _snr is not None:
            obs["snr"] = float(Decimal(str(_snr)))
        observations_list.append(obs)

    # Sort by epoch ascending
    observations_list.sort(key=lambda o: o["epoch_mjd"])

    # Step 5 — Update context.
    nova_context["spectra_count"] = len(individual_products)

    # Count distinct observing nights using gap-based clustering
    # (same algorithm as the compositing pipeline — ADR-033 Decision 2).
    products_with_mjd = [
        p for p in individual_products if p.get("observation_date_mjd") is not None
    ]
    if products_with_mjd:
        night_groups = cluster_by_night(products_with_mjd)
        distinct_nights = len(night_groups)
    else:
        distinct_nights = 0
    nova_context["spectral_visits"] = distinct_nights

    _logger.info(
        "Generated spectra.json",
        extra={
            "nova_id": nova_id,
            "valid_products": len(individual_products),
            "spectra_output": len(spectra),
            "phase": "generate_spectra",
        },
    )

    artifact: dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at_timestamp(),
        "nova_id": nova_id,
        "outburst_mjd": outburst_mjd,
        "outburst_mjd_is_estimated": outburst_mjd_is_estimated,
        "wavelength_unit": _WAVELENGTH_UNIT,
        "regimes": regime_records,
        "total_data_products": len(individual_products),
        "observations": observations_list,
        "spectra": spectra,
    }
    return artifact


# ---------------------------------------------------------------------------
# DynamoDB query
# ---------------------------------------------------------------------------


def _query_valid_spectra(
    nova_id: str,
    table: Any,
) -> list[dict[str, Any]]:
    """Query all VALID spectra DataProduct items for *nova_id*."""
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
        ),
        "FilterExpression": Attr("validation_status").eq("VALID"),
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
# Composite filtering
# ---------------------------------------------------------------------------


def _filter_composites(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter products so composites replace their constituent/rejected spectra.

    Composites are identified by having ``"COMPOSITE"`` in their ``SK`` field.
    Their ``constituent_data_product_ids`` and ``rejected_data_product_ids``
    lists define a suppression set — individual spectra in that set are excluded
    from the display set.
    """
    composites = [p for p in products if "COMPOSITE" in p.get("SK", "")]
    if not composites:
        return products

    suppression_set: set[str] = set()
    for comp in composites:
        suppression_set.update(comp.get("constituent_data_product_ids", []))
        suppression_set.update(comp.get("rejected_data_product_ids", []))

    return [
        p
        for p in products
        if "COMPOSITE" in p.get("SK", "") or p["data_product_id"] not in suppression_set
    ]


# ---------------------------------------------------------------------------
# Per-spectrum processing
# ---------------------------------------------------------------------------


def _process_spectrum_stage1(
    nova_id: str,
    product: dict[str, Any],
    s3_client: Any,
    private_bucket: str,
) -> dict[str, Any] | None:
    """Stage 1: S3 read, CSV parse, and dead-edge trimming.

    Returns a mutable dict carrying raw wavelength/flux arrays and the
    original product metadata, or ``None`` to skip this spectrum.
    """
    data_product_id: str = product["data_product_id"]
    # Composites store their S3 key directly; individuals use the convention.
    if "COMPOSITE" in product.get("SK", ""):
        s3_key = product["web_ready_s3_key"]
    else:
        s3_key = f"derived/spectra/{nova_id}/{data_product_id}/web_ready.csv"

    # --- S3 read ---
    try:
        _s3_start = time.perf_counter()
        response = s3_client.get_object(Bucket=private_bucket, Key=s3_key)
        body: str = response["Body"].read().decode("utf-8")
        _s3_duration_ms = (time.perf_counter() - _s3_start) * 1000
        _logger.info(
            "Operation completed: s3_read_csv",
            extra={
                "operation": "s3_read_csv",
                "duration_ms": round(_s3_duration_ms, 1),
                "nova_id": nova_id,
                "data_product_id": data_product_id,
                "s3_key": s3_key,
            },
        )
    except Exception as exc:
        _logger.warning(
            "Missing or unreadable web-ready CSV — skipping spectrum",
            extra={
                "nova_id": nova_id,
                "data_product_id": data_product_id,
                "instrument": product.get("instrument", "unknown"),
                "provider": product.get("provider", "unknown"),
                "s3_key": s3_key,
                "error": str(exc),
            },
        )
        return None

    # --- CSV parse ---
    wavelengths, fluxes = _parse_web_ready_csv(body)

    if not wavelengths:
        _logger.warning(
            "Empty web-ready CSV — skipping spectrum",
            extra={
                "nova_id": nova_id,
                "data_product_id": data_product_id,
                "instrument": product.get("instrument", "unknown"),
                "provider": product.get("provider", "unknown"),
            },
        )
        return None

    # --- Edge trimming (strip detector rolloff artifacts) ---
    wavelengths, fluxes = trim_dead_edges(wavelengths, fluxes, data_product_id)

    if not wavelengths:
        _logger.warning(
            "All-zero spectrum after edge trimming — skipping",
            extra={
                "nova_id": nova_id,
                "data_product_id": data_product_id,
                "instrument": product.get("instrument", "unknown"),
                "provider": product.get("provider", "unknown"),
            },
        )
        return None

    # --- Interior dead runs (chip gaps) ---
    wavelengths, fluxes = remove_interior_dead_runs(wavelengths, fluxes, data_product_id)

    # --- Chip gap artifact rejection ---
    wavelengths, fluxes = reject_chip_gap_artifacts(wavelengths, fluxes, data_product_id)

    if not wavelengths:
        _logger.warning(
            "Empty spectrum after chip gap rejection — skipping",
            extra={
                "nova_id": nova_id,
                "data_product_id": data_product_id,
                "instrument": product.get("instrument", "unknown"),
                "provider": product.get("provider", "unknown"),
            },
        )
        return None

    return {
        "wavelengths": wavelengths,
        "fluxes": fluxes,
        "product": product,
        "nova_id": nova_id,
    }


def _trim_wavelength_range(
    rec: dict[str, Any],
    display_wavelength_max: float,
) -> None:
    """Trim a stage-1 record's arrays to the display wavelength range (in place).

    NaN sentinel rows (gap markers from multi-arm merge) are preserved
    if the surrounding wavelength falls within the display range.
    """
    wavelengths: list[float] = rec["wavelengths"]
    fluxes: list[float] = rec["fluxes"]
    data_product_id: str = rec["product"]["data_product_id"]
    original_max = wavelengths[-1]

    trimmed_wl: list[float] = []
    trimmed_fx: list[float] = []
    for wl, fx in zip(wavelengths, fluxes, strict=True):
        if wl <= display_wavelength_max:
            trimmed_wl.append(wl)
            trimmed_fx.append(fx)

    _logger.debug(
        "Trimmed spectrum wavelength range to display bounds",
        extra={
            "data_product_id": data_product_id,
            "original_wavelength_max": original_max,
            "trimmed_wavelength_max": trimmed_wl[-1] if trimmed_wl else 0.0,
            "display_wavelength_max": display_wavelength_max,
        },
    )

    rec["wavelengths"] = trimmed_wl
    rec["fluxes"] = trimmed_fx


def _trim_wavelength_range_min(
    rec: dict[str, Any],
    display_wavelength_min: float,
) -> None:
    """Trim the blue side of a stage-1 record to the display minimum (in place).

    NaN sentinel rows (gap markers from multi-arm merge) are preserved
    if the surrounding wavelength falls within the display range.
    """
    wavelengths: list[float] = rec["wavelengths"]
    fluxes: list[float] = rec["fluxes"]
    data_product_id: str = rec["product"]["data_product_id"]
    original_min = wavelengths[0]

    trimmed_wl: list[float] = []
    trimmed_fx: list[float] = []
    for wl, fx in zip(wavelengths, fluxes, strict=True):
        if wl >= display_wavelength_min:
            trimmed_wl.append(wl)
            trimmed_fx.append(fx)

    _logger.debug(
        "Trimmed spectrum blue-side wavelength range to display bounds",
        extra={
            "data_product_id": data_product_id,
            "original_wavelength_min": original_min,
            "trimmed_wavelength_min": trimmed_wl[0] if trimmed_wl else 0.0,
            "display_wavelength_min": display_wavelength_min,
        },
    )

    rec["wavelengths"] = trimmed_wl
    rec["fluxes"] = trimmed_fx


def _process_spectrum_stage2(
    rec: dict[str, Any],
    outburst_mjd: float | None,
) -> dict[str, Any] | None:
    """Stage 2: LTTB downsampling, normalization, and record assembly."""
    wavelengths: list[float] = rec["wavelengths"]
    fluxes: list[float] = rec["fluxes"]
    product: dict[str, Any] = rec["product"]
    nova_id: str = rec["nova_id"]
    data_product_id: str = product["data_product_id"]

    if not wavelengths:
        return None

    # --- LTTB downsampling (§7.9) — preserve peaks within point budget ---
    wavelengths, fluxes = segment_aware_lttb(wavelengths, fluxes)

    # --- Flux normalization (§7.3) ---
    flux_normalized, normalization_scale = _normalize_flux(fluxes)
    if normalization_scale is None:
        _logger.warning(
            "Zero peak flux — skipping spectrum",
            extra={
                "nova_id": nova_id,
                "data_product_id": data_product_id,
                "instrument": product.get("instrument", "unknown"),
                "provider": product.get("provider", "unknown"),
            },
        )
        return None

    # --- Metadata ---
    epoch_mjd = _to_float(product.get("observation_date_mjd", 0))

    days_since_outburst: float | None = None
    if outburst_mjd is not None:
        days_since_outburst = round(epoch_mjd - outburst_mjd, 4)

    return {
        "spectrum_id": f"{data_product_id}::{rec['_split_suffix']}"
        if "_split_suffix" in rec
        else data_product_id,
        "regime": rec["_regime"],
        "epoch_mjd": epoch_mjd,
        "days_since_outburst": days_since_outburst,
        "instrument": product.get("instrument", "unknown"),
        "telescope": product.get("telescope", "unknown"),
        "provider": product.get("provider", "unknown"),
        "wavelength_min": min(wavelengths),
        "wavelength_max": max(wavelengths),
        "flux_unit": product.get("flux_unit", "unknown"),
        "normalization_scale": normalization_scale,
        "wavelengths": wavelengths,
        "flux_normalized": flux_normalized,
    }


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def _parse_web_ready_csv(body: str) -> tuple[list[float], list[float]]:
    """Parse a web-ready CSV into parallel wavelength and flux arrays.

    The CSV has a header row (``wavelength_nm,flux``) followed by data
    rows.  Wavelengths are monotonically ordered by the ingestion
    pipeline.
    """
    reader = csv.reader(io.StringIO(body))
    next(reader, None)  # skip header

    wavelengths: list[float] = []
    fluxes: list[float] = []

    for row in reader:
        if len(row) >= 2:
            try:
                wavelengths.append(float(row[0]))
                fluxes.append(float(row[1]))
            except ValueError:
                continue  # skip malformed rows silently

    return wavelengths, fluxes


# ---------------------------------------------------------------------------
# Flux normalization (§7.3)
# ---------------------------------------------------------------------------


def _normalize_flux(
    fluxes: list[float],
) -> tuple[list[float], float | None]:
    """Peak-normalize a flux array and clamp to floor.

    Returns ``(normalized, scale)`` where *scale* is the peak absolute
    flux.  Returns ``([], None)`` when the peak is zero or the array
    is empty — the caller should skip the spectrum.

    After normalization, all values are clamped to ``_FLUX_FLOOR`` to
    prevent ``log(0)`` on the frontend log-scale toggle.
    """
    if not fluxes:
        return [], None

    peak = max(abs(f) for f in fluxes)

    if peak == 0.0:
        return [], None

    normalized: list[float] = [max(f / peak, _FLUX_FLOOR) for f in fluxes]
    return normalized, peak


# ---------------------------------------------------------------------------
# Multi-arm merge (S4)
# ---------------------------------------------------------------------------


def _merge_multi_arm_spectra(
    parsed: list[dict[str, Any]],
    nova_id: str,
    s3_client: Any,
    private_bucket: str,
) -> list[dict[str, Any]]:
    """Detect multi-arm instrument groups and merge them.

    Groups spectra by (instrument, observation_date_mjd) with a tolerance
    of ``_ARM_MJD_TOLERANCE`` days.  Groups of size ≥ 2 are merged into a
    single record with overlap blending or gap NaN sentinels.
    """
    if len(parsed) < 2:
        return parsed

    # --- Build groups keyed by (instrument, representative_mjd) ---
    groups: list[list[dict[str, Any]]] = []
    used: set[int] = set()

    for i, rec_i in enumerate(parsed):
        if i in used:
            continue
        instrument_i = rec_i["product"].get("instrument", "")
        mjd_i = _to_float(rec_i["product"].get("observation_date_mjd", 0))
        group = [rec_i]
        used.add(i)

        for j in range(i + 1, len(parsed)):
            if j in used:
                continue
            rec_j = parsed[j]
            instrument_j = rec_j["product"].get("instrument", "")
            mjd_j = _to_float(rec_j["product"].get("observation_date_mjd", 0))

            if instrument_i == instrument_j and abs(mjd_i - mjd_j) <= _ARM_MJD_TOLERANCE:
                group.append(rec_j)
                used.add(j)

        groups.append(group)

    # --- Process each group ---
    result: list[dict[str, Any]] = []
    for group in groups:
        if len(group) == 1:
            result.append(group[0])
            continue

        merged = _merge_arm_group(group, nova_id, s3_client, private_bucket)
        if merged is not None:
            result.append(merged)
        else:
            # Merge failed — keep arms as separate spectra.
            result.extend(group)

    return result


def _pick_arm_to_keep(
    rec_a: dict[str, Any],
    rec_b: dict[str, Any],
) -> dict[str, Any]:
    """Choose the better arm from a pair with excessive overlap.

    Tiebreaker priority:
    1. Wavelength range > 100nm preferred over ≤ 100nm.
    2. Point count ≥ 2000 preferred.
    3. Broadest wavelength coverage wins.
    """

    def _score(rec: dict[str, Any]) -> tuple[bool, bool, float]:
        wl = rec["wavelengths"]
        wl_range = wl[-1] - wl[0] if wl else 0.0
        return (
            wl_range > 100.0,
            len(wl) >= 2000,
            wl_range,
        )

    return rec_a if _score(rec_a) >= _score(rec_b) else rec_b


def _merge_arm_group(
    group: list[dict[str, Any]],
    nova_id: str,
    s3_client: Any,
    private_bucket: str,
) -> dict[str, Any] | None:
    """Merge a group of arm spectra into a single record.

    Returns ``None`` if the group fails validation (overlap too large),
    in which case the caller should keep the arms separate.
    """
    # Sort arms by wavelength_min ascending.
    arms = list(group)
    arms.sort(key=lambda rec: rec["wavelengths"][0])

    instrument = arms[0]["product"].get("instrument", "unknown")

    # --- Per-pair overlap rejection ---
    # Drop one arm from any adjacent pair with >100nm overlap, then re-check.
    changed = True
    while changed and len(arms) >= 2:
        changed = False
        for k in range(len(arms) - 1):
            overlap_nm = arms[k]["wavelengths"][-1] - arms[k + 1]["wavelengths"][0]
            if overlap_nm > _ARM_OVERLAP_MAX_NM:
                keep = _pick_arm_to_keep(arms[k], arms[k + 1])
                drop = arms[k + 1] if keep is arms[k] else arms[k]
                drop_id = drop["product"]["data_product_id"]
                _logger.info(
                    "Dropping arm due to excessive overlap (%.0fnm)",
                    overlap_nm,
                    extra={
                        "nova_id": nova_id,
                        "instrument": instrument,
                        "dropped_data_product_id": drop_id,
                        "overlap_nm": round(overlap_nm, 2),
                        "reason": f"adjacent pair overlap > {_ARM_OVERLAP_MAX_NM:.0f}nm",
                    },
                )
                arms.remove(drop)
                # Re-sort and restart validation after removal.
                arms.sort(key=lambda rec: rec["wavelengths"][0])
                changed = True
                break

    if len(arms) < 2:
        # Only one arm survived — no merge needed, pass through as-is.
        return arms[0] if arms else None

    arm_ids = [rec["product"]["data_product_id"] for rec in arms]

    # --- Check flux_unit consistency ---
    flux_units = {rec["product"].get("flux_unit", "unknown") for rec in arms}
    if len(flux_units) > 1:
        _logger.warning(
            "Arms have different flux_unit values — merging anyway",
            extra={
                "nova_id": nova_id,
                "instrument": instrument,
                "flux_units": sorted(flux_units),
            },
        )

    # --- Merge adjacent arms ---
    merged_wl: list[float] = list(arms[0]["wavelengths"])
    merged_fx: list[float] = list(arms[0]["fluxes"])
    blend_applied = False

    for k in range(1, len(arms)):
        arm_wl = arms[k]["wavelengths"]
        arm_fx = arms[k]["fluxes"]
        overlap_nm = merged_wl[-1] - arm_wl[0]

        if overlap_nm > 0:
            # Overlap blending.
            merged_wl, merged_fx = _blend_overlap(merged_wl, merged_fx, arm_wl, arm_fx)
            blend_applied = True
        else:
            # No overlap — simple concatenation (gap is acceptable at
            # waterfall display scale; raw per-arm FITS preserve true coverage).
            merged_wl.extend(arm_wl)
            merged_fx.extend(arm_fx)

    # --- Composite ID ---
    sorted_ids = sorted(arm_ids)
    composite_id = str(uuid.UUID(hashlib.md5("|".join(sorted_ids).encode()).hexdigest()))  # noqa: S324

    # --- Persist merged CSV to S3 ---
    _persist_merged_csv(nova_id, composite_id, merged_wl, merged_fx, s3_client, private_bucket)

    # --- Build merged record ---
    first = arms[0]["product"]

    merged_product: dict[str, Any] = {
        "data_product_id": composite_id,
        "instrument": instrument,
        "telescope": first.get("telescope", "unknown"),
        "provider": first.get("provider", "unknown"),
        "observation_date_mjd": first.get("observation_date_mjd", 0),
        "flux_unit": first.get("flux_unit", "unknown"),
        "wavelength_min": merged_wl[0],
        "wavelength_max": merged_wl[-1],
    }

    _logger.info(
        "Merged multi-arm spectra",
        extra={
            "nova_id": nova_id,
            "instrument": instrument,
            "arm_count": len(arms),
            "arm_ids": arm_ids,
            "composite_id": composite_id,
            "wavelength_min": merged_product["wavelength_min"],
            "wavelength_max": merged_product["wavelength_max"],
            "blend_applied": blend_applied,
        },
    )

    return {
        "wavelengths": merged_wl,
        "fluxes": merged_fx,
        "product": merged_product,
        "nova_id": nova_id,
    }


def _blend_overlap(
    wl_a: list[float],
    fx_a: list[float],
    wl_b: list[float],
    fx_b: list[float],
) -> tuple[list[float], list[float]]:
    """Blend the overlap region between two adjacent arms.

    Uses linear interpolation onto a shared grid in the overlap zone,
    then averages the flux values.
    """
    overlap_start = wl_b[0]
    overlap_end = wl_a[-1]

    # Portion of arm A before the overlap.
    pre_wl: list[float] = []
    pre_fx: list[float] = []
    for w, f in zip(wl_a, fx_a, strict=True):
        if w < overlap_start:
            pre_wl.append(w)
            pre_fx.append(f)

    # Portion of arm B after the overlap.
    post_wl: list[float] = []
    post_fx: list[float] = []
    for w, f in zip(wl_b, fx_b, strict=True):
        if w > overlap_end:
            post_wl.append(w)
            post_fx.append(f)

    # Build the shared wavelength grid from the denser arm in the overlap.
    overlap_a_wl = [w for w in wl_a if overlap_start <= w <= overlap_end]
    overlap_b_wl = [w for w in wl_b if overlap_start <= w <= overlap_end]
    grid = overlap_a_wl if len(overlap_a_wl) >= len(overlap_b_wl) else overlap_b_wl

    if not grid:
        # Degenerate: no points in the overlap zone; just concatenate.
        return pre_wl + wl_b, pre_fx + fx_b

    grid_arr = np.array(grid)
    interp_a = np.interp(grid_arr, np.array(wl_a), np.array(fx_a))
    interp_b = np.interp(grid_arr, np.array(wl_b), np.array(fx_b))
    blended = ((interp_a + interp_b) / 2.0).tolist()

    merged_wl = pre_wl + grid + post_wl
    merged_fx = pre_fx + blended + post_fx
    return merged_wl, merged_fx


def _persist_merged_csv(
    nova_id: str,
    composite_id: str,
    wavelengths: list[float],
    fluxes: list[float],
    s3_client: Any,
    private_bucket: str,
) -> None:
    """Write a merged web-ready CSV to S3 for documentation/debugging."""
    s3_key = f"derived/spectra/{nova_id}/{composite_id}/web_ready.csv"
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["wavelength_nm", "flux"])
    for wl, fx in zip(wavelengths, fluxes, strict=True):
        writer.writerow([wl, fx])
    try:
        s3_client.put_object(
            Bucket=private_bucket,
            Key=s3_key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        _logger.debug(
            "Persisted merged CSV",
            extra={"nova_id": nova_id, "composite_id": composite_id, "s3_key": s3_key},
        )
    except Exception:
        _logger.warning(
            "Failed to persist merged CSV — continuing without caching",
            extra={"nova_id": nova_id, "composite_id": composite_id, "s3_key": s3_key},
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_float(value: Any) -> float:
    """Convert a DynamoDB Decimal (or other numeric) to float."""
    if isinstance(value, Decimal):
        return float(value)
    return float(value)
