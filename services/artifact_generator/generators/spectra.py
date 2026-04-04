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
import io
import logging
import statistics
import time
from decimal import Decimal
from typing import Any

from boto3.dynamodb.conditions import Attr, Key

from generators.shared import generated_at_timestamp, lttb

_logger = logging.getLogger("artifact_generator")

_SCHEMA_VERSION = "1.1"  # §7.8: outburst_mjd_is_estimated addition
_WAVELENGTH_UNIT = "nm"

_FLUX_FLOOR = 1e-4  # minimum normalized flux; prevents log(0) in frontend
_ZERO_THRESHOLD = 1e-10  # absolute threshold for "effectively zero" flux

_LTTB_THRESHOLD = 2000  # max points per spectrum (DESIGN-003 §7.9, P-4)
_TRIM_TOLERANCE = 1.1  # 10% beyond median before wavelength trim kicks in


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

    # Step 2b — Compute display wavelength range from median bounds.
    display_wavelength_min: float | None = None
    display_wavelength_max: float | None = None

    if len(parsed) >= 2:
        wl_mins = [s["wavelengths"][0] for s in parsed]
        wl_maxes = [s["wavelengths"][-1] for s in parsed]
        display_wavelength_min = statistics.median(wl_mins)
        display_wavelength_max = statistics.median(wl_maxes)
        assert display_wavelength_min is not None  # nosec: narrowing for mypy
        assert display_wavelength_max is not None  # nosec: narrowing for mypy

        # Warn if trim would affect >50% of spectra (bimodal data).
        trim_count = sum(1 for wmax in wl_maxes if wmax > display_wavelength_max * _TRIM_TOLERANCE)
        if trim_count > len(parsed) / 2:
            _logger.warning(
                "Wavelength trim affects >50%% of spectra — data may be bimodal",
                extra={
                    "nova_id": nova_id,
                    "trim_count": trim_count,
                    "total": len(parsed),
                },
            )

        # Trim outlier spectra to the display range.
        for rec in parsed:
            if rec["wavelengths"][-1] > display_wavelength_max * _TRIM_TOLERANCE:
                _trim_wavelength_range(rec, display_wavelength_max)

    # Step 2c — Second pass: LTTB downsampling + normalization.
    spectra: list[dict[str, Any]] = []
    for rec in parsed:
        record = _process_spectrum_stage2(rec, outburst_mjd)
        if record is not None:
            spectra.append(record)

    # Step 3 — Sort by epoch ascending (oldest at bottom of waterfall).
    spectra.sort(key=lambda s: s["epoch_mjd"])

    # Step 4 — Update context.
    nova_context["spectra_count"] = len(spectra)

    _logger.info(
        "Generated spectra.json",
        extra={
            "nova_id": nova_id,
            "valid_products": len(products),
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
        "spectra": spectra,
    }
    if display_wavelength_min is not None:
        artifact["display_wavelength_min"] = display_wavelength_min
    if display_wavelength_max is not None:
        artifact["display_wavelength_max"] = display_wavelength_max

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
                "data_product_id": data_product_id,
            },
        )
    except Exception as exc:
        _logger.warning(
            "Missing or unreadable web-ready CSV — skipping spectrum",
            extra={
                "nova_id": nova_id,
                "data_product_id": data_product_id,
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
            extra={"nova_id": nova_id, "data_product_id": data_product_id},
        )
        return None

    # --- Edge trimming (strip detector rolloff artifacts) ---
    wavelengths, fluxes = _trim_dead_edges(wavelengths, fluxes, data_product_id)

    if not wavelengths:
        _logger.warning(
            "All-zero spectrum after edge trimming — skipping",
            extra={"nova_id": nova_id, "data_product_id": data_product_id},
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
    """Trim a stage-1 record's arrays to the display wavelength range (in place)."""
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
    if len(wavelengths) > _LTTB_THRESHOLD:
        points = list(zip(wavelengths, fluxes, strict=True))
        downsampled = lttb(points, _LTTB_THRESHOLD)
        wavelengths = [p[0] for p in downsampled]
        fluxes = [p[1] for p in downsampled]

    # --- Flux normalization (§7.3) ---
    flux_normalized, normalization_scale = _normalize_flux(fluxes)
    if normalization_scale is None:
        _logger.warning(
            "Zero peak flux — skipping spectrum",
            extra={"nova_id": nova_id, "data_product_id": data_product_id},
        )
        return None

    # --- Metadata ---
    epoch_mjd = _to_float(product.get("observation_date_mjd", 0))

    days_since_outburst: float | None = None
    if outburst_mjd is not None:
        days_since_outburst = round(epoch_mjd - outburst_mjd, 4)

    return {
        "spectrum_id": data_product_id,
        "epoch_mjd": epoch_mjd,
        "days_since_outburst": days_since_outburst,
        "instrument": product.get("instrument", "unknown"),
        "telescope": product.get("telescope", "unknown"),
        "provider": product.get("provider", "unknown"),
        "wavelength_min": wavelengths[0],
        "wavelength_max": wavelengths[-1],
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
# Edge trimming
# ---------------------------------------------------------------------------


def _trim_dead_edges(
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

    # --- blue (low-wavelength) edge ---
    blue_zeros = 0
    for f in fluxes:
        if abs(f) < _ZERO_THRESHOLD:
            blue_zeros += 1
        else:
            break
    blue_trim = blue_zeros if blue_zeros > 1 else 0

    # --- red (high-wavelength) edge ---
    red_zeros = 0
    for f in reversed(fluxes):
        if abs(f) < _ZERO_THRESHOLD:
            red_zeros += 1
        else:
            break
    red_trim = red_zeros if red_zeros > 1 else 0

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

    normalized = [max(f / peak, _FLUX_FLOOR) for f in fluxes]
    return normalized, peak


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_float(value: Any) -> float:
    """Convert a DynamoDB Decimal (or other numeric) to float."""
    if isinstance(value, Decimal):
        return float(value)
    return float(value)
