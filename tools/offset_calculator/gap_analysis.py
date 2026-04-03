"""artifact_generator — Fargate task entry point (DESIGN-003 §4.4).

Entry point for the ECS Fargate container launched by the
``regenerate_artifacts`` Step Functions workflow.  Receives a
``plan_id`` via the ``PLAN_ID`` environment variable (set by the
RunTask container override).

Execution steps (§4.4):
  1. Load the band registry (once per Fargate execution).
  2. Load the ``RegenBatchPlan`` from DynamoDB.
  3. For each nova in the plan:
     a. Load the Nova DDB item (GetItem).
     b. Pre-query observation epochs from spectra DataProducts and
        PhotometryRow items for outburst MJD resolution (§7.6).
     c. Generate all artifacts specified in its manifest in dependency
        order (``GENERATION_ORDER``), uploading each to S3.
  4. Track per-nova success/failure — a single nova's failure does
     not abort the batch.
  5. After all novae, generate ``catalog.json`` (stub for Epic 4).
  6. Write ``nova_results`` back to the batch plan in DynamoDB.
  7. Exit with code 0 (success) or 1 (all novae failed).

Environment variables:
    PLAN_ID                        — batch plan UUID (required)
    NOVA_CAT_TABLE_NAME            — DynamoDB main table name (required)
    NOVA_CAT_PHOTOMETRY_TABLE_NAME — DynamoDB photometry table name (required)
    NOVA_CAT_PRIVATE_BUCKET        — S3 private data bucket name (required)
    NOVA_CAT_PUBLIC_SITE_BUCKET    — S3 public site bucket name (required)
    LOG_LEVEL                      — logging level (default INFO)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key
from generators.bundle import generate_bundle_zip
from generators.nova import generate_nova_json
from generators.photometry import generate_photometry_json
from generators.references import generate_references_json
from generators.shared import resolve_outburst_mjd
from generators.sparkline import generate_sparkline_svg
from generators.spectra import generate_spectra_json

from contracts.models.regeneration import (
    GENERATION_ORDER,
    ArtifactType,
    NovaResult,
)

# ---------------------------------------------------------------------------
# Logging — Fargate tasks don't use Powertools (no Lambda context), so we
# configure stdlib logging with JSON-structured output.
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(message)s",
    level=os.environ.get("LOG_LEVEL", "INFO"),
    stream=sys.stdout,
)
_logger = logging.getLogger("artifact_generator")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_PHOTOMETRY_TABLE_NAME = os.environ["NOVA_CAT_PHOTOMETRY_TABLE_NAME"]
_PRIVATE_BUCKET = os.environ["NOVA_CAT_PRIVATE_BUCKET"]
_PUBLIC_BUCKET = os.environ["NOVA_CAT_PUBLIC_SITE_BUCKET"]
_PLAN_ID = os.environ["PLAN_ID"]
_REGEN_PLAN_PK = "REGEN_PLAN"

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

_dynamodb = boto3.resource("dynamodb")
_s3_client = boto3.client("s3")
_table = _dynamodb.Table(_TABLE_NAME)
_photometry_table = _dynamodb.Table(_PHOTOMETRY_TABLE_NAME)


# ---------------------------------------------------------------------------
# Band registry (loaded once per Fargate execution, not per-nova)
# ---------------------------------------------------------------------------


def _load_band_registry() -> Any:
    """Load the photometry band registry from bundled JSON.

    The ``band_registry`` package is copied from
    ``services/photometry_ingestor/band_registry/`` into the Docker
    image at build time.  Returns the ``(entry_index, alias_index)``
    tuple consumed by
    :func:`generators.photometry.generate_photometry_json`.
    """
    # Deferred import — band_registry is available only inside the
    # Fargate container image, not in the unit-test Python path.
    from band_registry.registry import (  # type: ignore[import-not-found]
        _REGISTRY_PATH,
        _load_registry,
    )

    return _load_registry(_REGISTRY_PATH)  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# Per-nova setup helpers
# ---------------------------------------------------------------------------


def _load_nova_item(nova_id: str) -> dict[str, Any]:
    """Load the Nova DDB item via GetItem.

    Returns the raw item dict.  Raises ``ValueError`` if the item
    does not exist — this is a fatal error for the nova (not the batch).
    """
    response: dict[str, Any] = _table.get_item(
        Key={"PK": nova_id, "SK": "NOVA"},
    )
    item: dict[str, Any] | None = response.get("Item")
    if item is None:
        msg = f"Nova item not found: PK={nova_id}, SK=NOVA"
        raise ValueError(msg)
    return dict(item)


def _query_observation_epochs(nova_id: str) -> list[float]:
    """Pre-query observation epoch MJD values for outburst resolution.

    Queries both the main table (spectra DataProducts) and the
    dedicated photometry table to collect all observation timestamps.
    Used by :func:`generators.shared.resolve_outburst_mjd` (§7.6).

    Uses ``ProjectionExpression`` to minimize read capacity — only the
    epoch field is returned from each item.
    """
    epochs: list[float] = []

    # --- Spectra DataProduct observation dates (main table) ---
    spectra_kwargs: dict[str, Any] = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
        ),
        "FilterExpression": Attr("validation_status").eq("VALID"),
        "ProjectionExpression": "observation_date_mjd",
    }
    while True:
        response: dict[str, Any] = _table.query(**spectra_kwargs)
        for item in response.get("Items", []):
            mjd: Any = item.get("observation_date_mjd")
            if mjd is not None:
                epochs.append(float(mjd))
        last_key: dict[str, Any] | None = response.get("LastEvaluatedKey")
        if last_key is None:
            break
        spectra_kwargs["ExclusiveStartKey"] = last_key

    # --- PhotometryRow timestamps (dedicated photometry table) ---
    phot_kwargs: dict[str, Any] = {
        "KeyConditionExpression": (Key("PK").eq(nova_id) & Key("SK").begins_with("PHOT#")),
        "ProjectionExpression": "time_mjd",
    }
    while True:
        phot_response: dict[str, Any] = _photometry_table.query(**phot_kwargs)
        for item in phot_response.get("Items", []):
            mjd = item.get("time_mjd")
            if mjd is not None:
                epochs.append(float(mjd))
        phot_last_key: dict[str, Any] | None = phot_response.get("LastEvaluatedKey")
        if phot_last_key is None:
            break
        phot_kwargs["ExclusiveStartKey"] = phot_last_key

    return epochs


# ---------------------------------------------------------------------------
# Artifact dispatch and S3 upload
# ---------------------------------------------------------------------------


def _json_default(obj: object) -> int | float:
    """JSON serializer fallback for DynamoDB ``Decimal`` values."""
    if isinstance(obj, Decimal):
        if obj == int(obj):
            return int(obj)
        return float(obj)
    msg = f"Object of type {type(obj).__name__} is not JSON serializable"
    raise TypeError(msg)


def _upload_json_artifact(
    nova_id: str,
    filename: str,
    data: dict[str, Any],
) -> None:
    """Serialize and upload a JSON artifact to the public S3 bucket."""
    key = f"nova/{nova_id}/{filename}"
    _s3_client.put_object(
        Bucket=_PUBLIC_BUCKET,
        Key=key,
        Body=json.dumps(data, default=_json_default, ensure_ascii=False),
        ContentType="application/json",
    )
    _logger.info(
        "Uploaded artifact",
        extra={"nova_id": nova_id, "artifact": filename, "s3_key": key},
    )


def _upload_svg_artifact(
    nova_id: str,
    filename: str,
    svg: str,
) -> None:
    """Upload an SVG artifact to the public S3 bucket."""
    key = f"nova/{nova_id}/{filename}"
    _s3_client.put_object(
        Bucket=_PUBLIC_BUCKET,
        Key=key,
        Body=svg.encode("utf-8"),
        ContentType="image/svg+xml",
    )
    _logger.info(
        "Uploaded artifact",
        extra={"nova_id": nova_id, "artifact": filename, "s3_key": key},
    )


def _dispatch_generator(
    nova_id: str,
    artifact: ArtifactType,
    nova_context: dict[str, Any],
    *,
    band_registry: Any,
) -> None:
    """Route to the appropriate generator and upload the result.

    Each generator updates *nova_context* as a side effect (observation
    counts, intermediate data for downstream generators).  JSON and SVG
    artifacts are uploaded to the public S3 bucket.  The bundle generator
    handles its own S3 upload internally.
    """
    if artifact == ArtifactType.references_json:
        result = generate_references_json(
            nova_id,
            _dynamodb,
            _TABLE_NAME,
            nova_context,
        )
        _upload_json_artifact(nova_id, "references.json", result)

    elif artifact == ArtifactType.spectra_json:
        result = generate_spectra_json(
            nova_id,
            _table,
            _s3_client,
            _PRIVATE_BUCKET,
            nova_context,
        )
        _upload_json_artifact(nova_id, "spectra.json", result)

    elif artifact == ArtifactType.photometry_json:
        result = generate_photometry_json(
            nova_id,
            _photometry_table,
            band_registry,
            nova_context,
        )
        _upload_json_artifact(nova_id, "photometry.json", result)

    elif artifact == ArtifactType.sparkline_svg:
        svg = generate_sparkline_svg(nova_id, nova_context)
        if svg is not None:
            _upload_svg_artifact(nova_id, "sparkline.svg", svg)

    elif artifact == ArtifactType.nova_json:
        result = generate_nova_json(nova_id, nova_context)
        _upload_json_artifact(nova_id, "nova.json", result)

    elif artifact == ArtifactType.bundle_zip:
        # Bundle generator writes the ZIP to S3 internally.
        generate_bundle_zip(
            nova_id,
            _table,
            _s3_client,
            _PRIVATE_BUCKET,
            _PUBLIC_BUCKET,
            nova_context,
        )


def _generate_catalog_stub() -> None:
    """No-op catalog.json generator stub (Epic 2).

    In Epic 4, this generates catalog.json from the accumulated
    in-memory sweep results merged with a DDB Scan of all ACTIVE novae
    (§11).
    """


# ---------------------------------------------------------------------------
# Plan loading
# ---------------------------------------------------------------------------


def _load_batch_plan() -> dict[str, Any]:
    """Load the RegenBatchPlan from DynamoDB by ``plan_id``.

    Queries the ``REGEN_PLAN`` partition and filters by ``plan_id``
    attribute.  Returns the raw DDB item dict.

    Raises ``SystemExit`` if the plan is not found — this is a fatal
    error indicating the coordinator and Fargate task are out of sync.
    """
    response = _table.query(
        KeyConditionExpression=Key("PK").eq(_REGEN_PLAN_PK),
        FilterExpression=Attr("plan_id").eq(_PLAN_ID),
    )
    items = response.get("Items", [])
    if not items:
        _logger.error("Batch plan not found", extra={"plan_id": _PLAN_ID})
        sys.exit(1)
    return dict(items[0])


# ---------------------------------------------------------------------------
# Per-nova processing
# ---------------------------------------------------------------------------


def _process_nova(
    nova_id: str,
    manifest: dict[str, Any],
    *,
    band_registry: Any,
) -> NovaResult:
    """Process a single nova: run generators in dependency order.

    Before generators run, loads the Nova item from DDB, pre-queries
    observation epochs from both tables, and resolves the outburst MJD
    (§7.6).  These values are placed in ``nova_context`` for all
    downstream generators.

    Returns a ``NovaResult`` with success/failure and observation counts.
    Any exception from a generator marks the entire nova as failed — its
    WorkItems are retained for the next sweep.
    """
    artifacts_to_generate = manifest.get("artifacts", [])
    nova_context: dict[str, Any] = {}

    start = time.monotonic()
    try:
        # --- Per-nova setup (§7.6, Chunk 8) ---

        # (a) Load the Nova item from DDB via GetItem.
        nova_item = _load_nova_item(nova_id)
        nova_context["nova_item"] = nova_item

        # (b) Pre-query observation epochs from both tables for
        #     resolve_outburst_mjd().
        observation_epochs = _query_observation_epochs(nova_id)

        # (c) Populate nova_context with outburst MJD.
        outburst_mjd, outburst_mjd_is_estimated = resolve_outburst_mjd(
            nova_item.get("discovery_date"),
            nova_item.get("nova_type"),
            observation_epochs,
        )
        nova_context["outburst_mjd"] = outburst_mjd
        nova_context["outburst_mjd_is_estimated"] = outburst_mjd_is_estimated

        _logger.info(
            "Nova setup complete",
            extra={
                "nova_id": nova_id,
                "outburst_mjd": outburst_mjd,
                "outburst_mjd_is_estimated": outburst_mjd_is_estimated,
                "observation_epochs_count": len(observation_epochs),
                "phase": "setup",
            },
        )

        # --- Run generators in dependency order ---
        for artifact_type in GENERATION_ORDER:
            if artifact_type.value in artifacts_to_generate:
                _logger.info(
                    "Generating artifact",
                    extra={
                        "nova_id": nova_id,
                        "artifact": artifact_type.value,
                        "phase": "generate",
                    },
                )
                _dispatch_generator(
                    nova_id,
                    artifact_type,
                    nova_context,
                    band_registry=band_registry,
                )
    except Exception as exc:
        duration_ms = int((time.monotonic() - start) * 1000)
        _logger.error(
            "Nova processing failed",
            extra={
                "nova_id": nova_id,
                "error": str(exc),
                "duration_ms": duration_ms,
            },
        )
        return NovaResult(
            nova_id=nova_id,
            success=False,
            error=str(exc),
        )

    duration_ms = int((time.monotonic() - start) * 1000)
    _logger.info(
        "Nova processing succeeded",
        extra={"nova_id": nova_id, "duration_ms": duration_ms},
    )

    return NovaResult(
        nova_id=nova_id,
        success=True,
        spectra_count=nova_context.get("spectra_count", 0),
        photometry_count=nova_context.get("photometry_count", 0),
        references_count=nova_context.get("references_count", 0),
        has_sparkline=nova_context.get("has_sparkline", False),
    )


# ---------------------------------------------------------------------------
# Result writeback
# ---------------------------------------------------------------------------


def _write_results_to_plan(
    plan_item: dict[str, Any],
    nova_results: list[NovaResult],
) -> None:
    """Write ``nova_results`` back to the batch plan in DynamoDB.

    The Finalize Lambda reads these results to decide which WorkItems
    to delete and which observation counts to persist.
    """
    results_ddb = [result.model_dump(mode="json") for result in nova_results]
    _table.update_item(
        Key={"PK": _REGEN_PLAN_PK, "SK": plan_item["SK"]},
        UpdateExpression="SET nova_results = :results",
        ExpressionAttributeValues={":results": results_ddb},
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Fargate task entry point."""
    _logger.info(
        "Artifact generator started",
        extra={"plan_id": _PLAN_ID, "workflow_name": "artifact_generator"},
    )

    # Step 0 — Load band registry (once per Fargate execution, not per-nova)
    band_registry = _load_band_registry()
    _logger.info("Band registry loaded", extra={"phase": "init"})

    # Step 1 — Load the batch plan
    plan = _load_batch_plan()
    nova_manifests: dict[str, Any] = plan.get("nova_manifests", {})
    nova_count = len(nova_manifests)

    _logger.info(
        "Batch plan loaded",
        extra={"plan_id": _PLAN_ID, "nova_count": nova_count},
    )

    # Steps 2–3 — Process novae sequentially
    nova_results: list[NovaResult] = []
    succeeded = 0
    failed = 0

    for nova_id, manifest in nova_manifests.items():
        _logger.info(
            "Processing nova",
            extra={
                "nova_id": nova_id,
                "artifacts": manifest.get("artifacts", []),
                "progress": f"{succeeded + failed + 1}/{nova_count}",
            },
        )
        result = _process_nova(
            nova_id,
            manifest,
            band_registry=band_registry,
        )
        nova_results.append(result)
        if result.success:
            succeeded += 1
        else:
            failed += 1

    # Step 4 — Generate catalog.json (stub)
    _logger.info("Generating catalog.json (stub)", extra={"phase": "catalog"})
    _generate_catalog_stub()

    # Step 5 — Write results back to the plan
    _write_results_to_plan(plan, nova_results)

    _logger.info(
        "Artifact generator completed",
        extra={
            "plan_id": _PLAN_ID,
            "nova_count": nova_count,
            "succeeded": succeeded,
            "failed": failed,
        },
    )

    # Step 6 — Exit code
    if succeeded == 0 and nova_count > 0:
        _logger.error("All novae failed — exiting with error")
        sys.exit(1)


if __name__ == "__main__":
    main()
