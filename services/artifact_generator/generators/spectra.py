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
from decimal import Decimal
from typing import Any

from boto3.dynamodb.conditions import Attr, Key  # type: ignore[import-untyped]

from generators.shared import generated_at_timestamp

_logger = logging.getLogger("artifact_generator")

_SCHEMA_VERSION = "1.1"  # §7.8: outburst_mjd_is_estimated addition
_WAVELENGTH_UNIT = "nm"


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

    # Step 2 — Process each spectrum (S3 read + normalize).
    spectra: list[dict[str, Any]] = []
    for product in products:
        record = _process_spectrum(
            nova_id,
            product,
            s3_client,
            private_bucket,
            outburst_mjd,
        )
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

    return {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at_timestamp(),
        "nova_id": nova_id,
        "outburst_mjd": outburst_mjd,
        "outburst_mjd_is_estimated": outburst_mjd_is_estimated,
        "wavelength_unit": _WAVELENGTH_UNIT,
        "spectra": spectra,
    }


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


def _process_spectrum(
    nova_id: str,
    product: dict[str, Any],
    s3_client: Any,
    private_bucket: str,
    outburst_mjd: float | None,
) -> dict[str, Any] | None:
    """Read the web-ready CSV, normalize, and build a spectrum record.

    Returns ``None`` if the CSV is missing, empty, or corrupt — the
    spectrum is skipped and does not count toward ``spectra_count``.
    """
    data_product_id: str = product["data_product_id"]
    s3_key = f"derived/spectra/{nova_id}/{data_product_id}/web_ready.csv"

    # --- S3 read ---
    try:
        response = s3_client.get_object(Bucket=private_bucket, Key=s3_key)
        body: str = response["Body"].read().decode("utf-8")
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
# Flux normalization (§7.3)
# ---------------------------------------------------------------------------


def _normalize_flux(
    fluxes: list[float],
) -> tuple[list[float], float | None]:
    """Peak-normalize a flux array.

    Returns ``(normalized, scale)`` where *scale* is the peak absolute
    flux.  Returns ``([], None)`` when the peak is zero or the array
    is empty — the caller should skip the spectrum.
    """
    if not fluxes:
        return [], None

    peak = max(abs(f) for f in fluxes)

    if peak == 0.0:
        return [], None

    normalized = [f / peak for f in fluxes]
    return normalized, peak


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_float(value: Any) -> float:
    """Convert a DynamoDB Decimal (or other numeric) to float."""
    if isinstance(value, Decimal):
        return float(value)
    return float(value)
