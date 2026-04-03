"""artifact_generator — Fargate task entry point (DESIGN-003 §4.4).

Entry point for the ECS Fargate container launched by the
``regenerate_artifacts`` Step Functions workflow.  Receives a
``plan_id`` via the ``PLAN_ID`` environment variable (set by the
RunTask container override).

Execution steps (§4.4, amended by Epic 4):
  1. Load the ``RegenBatchPlan`` from DynamoDB.
  2. Initialize the release publisher and read previous pointer (§12).
  3. For each nova in the plan:
     a. Load the Nova item from DDB.
     b. Collect observation epochs and resolve outburst MJD.
     c. Generate and publish all artifacts specified in its manifest
        in dependency order (``GENERATION_ORDER``).
  4. Phase 2 — Copy forward unchanged ACTIVE novae from previous
     release (§12.5).
  5. Phase 3 — Generate ``catalog.json`` and write to release (§11).
  6. Phase 4 — Update ``current.json`` pointer (§12.5).
  7. Write ``nova_results`` back to the batch plan in DynamoDB.
  8. Exit with code 0 (success) or 1 (all novae failed / publication
     failed).

Environment variables:
    PLAN_ID                         — batch plan UUID (required)
    NOVA_CAT_TABLE_NAME             — main DynamoDB table name (required)
    NOVA_CAT_PHOTOMETRY_TABLE_NAME  — dedicated photometry table (required)
    NOVA_CAT_PRIVATE_BUCKET         — private S3 bucket (required)
    NOVA_CAT_PUBLIC_SITE_BUCKET     — public site S3 bucket (required)
    BAND_REGISTRY_PATH              — path to band_registry.json (default: ./band_registry.json)
    LOG_LEVEL                       — logging level (default INFO)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key
from generators.bundle import generate_bundle_zip
from generators.catalog import generate_catalog_json
from generators.nova import generate_nova_json
from generators.photometry import generate_photometry_json
from generators.references import generate_references_json
from generators.shared import resolve_outburst_mjd
from generators.sparkline import generate_sparkline_svg
from generators.spectra import generate_spectra_json
from release_publisher import ReleasePublisher

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
_PLAN_ID = os.environ["PLAN_ID"]
_REGEN_PLAN_PK = "REGEN_PLAN"

_PHOTOMETRY_TABLE_NAME = os.environ.get("NOVA_CAT_PHOTOMETRY_TABLE_NAME", "")
_PRIVATE_BUCKET = os.environ.get("NOVA_CAT_PRIVATE_BUCKET", "")
_PUBLIC_BUCKET = os.environ.get("NOVA_CAT_PUBLIC_SITE_BUCKET", "")
_REGISTRY_PATH = os.environ.get("BAND_REGISTRY_PATH", "./band_registry.json")

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)
_s3 = boto3.client("s3")

# Photometry table — may be empty string in tests that don't exercise
# the photometry generator.  The reference is safe to create; it only
# fails when an actual DDB call is made against a non-existent table.
_photometry_table = _dynamodb.Table(_PHOTOMETRY_TABLE_NAME) if _PHOTOMETRY_TABLE_NAME else None

# ---------------------------------------------------------------------------
# Band registry — loaded once per Fargate execution (§8.2).
#
# Populated by _load_band_registry() in main().  Tests can assign
# directly: ``mod._band_registry = {...}``.
# ---------------------------------------------------------------------------

_band_registry: dict[str, Any] = {}


def _load_band_registry() -> dict[str, Any]:
    """Load the band registry JSON and return a band_id → entry dict.

    The registry is loaded from *_REGISTRY_PATH* (default
    ``./band_registry.json``, bundled in the container image by the
    Dockerfile).  Each entry is a plain dict with at minimum
    ``band_name`` and ``lambda_eff`` fields.

    Returns an empty dict (with a warning) if the file is not found —
    the photometry generator handles missing registry entries gracefully
    by falling back to the raw ``band_id`` as the display label.
    """
    path = Path(_REGISTRY_PATH)
    if not path.exists():
        _logger.warning(
            "Band registry not found at %s — using empty registry",
            _REGISTRY_PATH,
        )
        return {}

    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    bands: list[dict[str, Any]] = raw.get("bands", [])

    registry: dict[str, Any] = {}
    for entry in bands:
        band_id = entry.get("band_id", "")
        if band_id:
            registry[band_id] = entry

    _logger.info("Band registry loaded: %d entries", len(registry))
    return registry


# ---------------------------------------------------------------------------
# Per-nova setup helpers
# ---------------------------------------------------------------------------


def _load_nova_item(nova_id: str) -> dict[str, Any] | None:
    """Load the Nova DDB item for *nova_id*.

    Returns ``None`` if the item does not exist or the nova is not
    ACTIVE.
    """
    response = _table.get_item(Key={"PK": nova_id, "SK": "NOVA"})
    item: dict[str, Any] | None = response.get("Item")

    if item is None:
        _logger.warning("Nova item not found", extra={"nova_id": nova_id})
        return None

    status = item.get("status", "")
    if status != "ACTIVE":
        _logger.warning(
            "Nova is not ACTIVE — skipping",
            extra={"nova_id": nova_id, "status": status},
        )
        return None

    return dict(item)


def _collect_observation_epochs(nova_id: str) -> list[float]:
    """Collect MJD epochs from both spectra and photometry for outburst resolution.

    Queries VALID spectra DataProducts (``observation_date_mjd``) and
    PhotometryRows (``time_mjd``).  Used by ``resolve_outburst_mjd()``
    for the earliest-observation fallback.
    """
    epochs: list[float] = []

    # Spectra epochs from the main table.
    spectra_kwargs: dict[str, Any] = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
        ),
        "FilterExpression": Attr("validation_status").eq("VALID"),
        "ProjectionExpression": "observation_date_mjd",
    }
    while True:
        resp = _table.query(**spectra_kwargs)
        for item in resp.get("Items", []):
            mjd = item.get("observation_date_mjd")
            if mjd is not None:
                epochs.append(float(Decimal(str(mjd))))
        if resp.get("LastEvaluatedKey") is None:
            break
        spectra_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    # Photometry epochs from the dedicated table.
    if _photometry_table is not None:
        phot_kwargs: dict[str, Any] = {
            "KeyConditionExpression": (Key("PK").eq(nova_id) & Key("SK").begins_with("PHOT#")),
            "ProjectionExpression": "time_mjd",
        }
        while True:
            resp = _photometry_table.query(**phot_kwargs)
            for item in resp.get("Items", []):
                mjd = item.get("time_mjd")
                if mjd is not None:
                    epochs.append(float(Decimal(str(mjd))))
            if resp.get("LastEvaluatedKey") is None:
                break
            phot_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    return epochs


# ---------------------------------------------------------------------------
# Artifact dispatch + publication (§12.5 Phase 1)
# ---------------------------------------------------------------------------


def _generate_and_publish(
    nova_id: str,
    artifact: ArtifactType,
    nova_context: dict[str, Any],
    publisher: ReleasePublisher,
) -> None:
    """Generate an artifact and publish it to the release prefix.

    Each generator reads from *nova_context* and/or AWS resources,
    produces an artifact dict (or SVG string), and updates
    *nova_context* with its outputs (counts, data for downstream
    generators).

    After generation, the artifact is uploaded to S3 under the current
    release prefix via the publisher (§12.5 Phase 1).
    """
    if artifact == ArtifactType.references_json:
        result = generate_references_json(nova_id, _table, _dynamodb, nova_context)
        publisher.upload_json_artifact(nova_id, "references.json", result)

    elif artifact == ArtifactType.spectra_json:
        result = generate_spectra_json(
            nova_id,
            _table,
            _s3,
            _PRIVATE_BUCKET,
            nova_context,
        )
        publisher.upload_json_artifact(nova_id, "spectra.json", result)

    elif artifact == ArtifactType.photometry_json:
        if _photometry_table is None:
            _logger.error(
                "Photometry table not configured — skipping photometry generator",
                extra={"nova_id": nova_id},
            )
            nova_context["photometry_count"] = 0
            nova_context["photometry_raw_items"] = []
            nova_context["photometry_observations"] = []
            nova_context["photometry_bands"] = []
            return
        result = generate_photometry_json(
            nova_id,
            _photometry_table,
            _table,
            _band_registry,
            nova_context,
        )
        publisher.upload_json_artifact(nova_id, "photometry.json", result)

    elif artifact == ArtifactType.sparkline_svg:
        svg = generate_sparkline_svg(nova_id, nova_context)
        if svg is not None:
            publisher.upload_svg_artifact(nova_id, "sparkline.svg", svg)

    elif artifact == ArtifactType.nova_json:
        result = generate_nova_json(nova_id, nova_context)
        publisher.upload_json_artifact(nova_id, "nova.json", result)

    elif artifact == ArtifactType.bundle_zip:
        generate_bundle_zip(
            nova_id,
            _table,
            _s3,
            _PRIVATE_BUCKET,
            _PUBLIC_BUCKET,
            nova_context,
            s3_key_prefix=f"releases/{publisher.release_id}/",
        )
        # The bundle generator writes to a flat S3 key internally.
        # Copy the bundle to the release prefix so it's part of the
        # immutable release.  The flat-key copy is cleaned up by the
        # S3 lifecycle rule.
        # TODO(epic-4): Update bundle.py to write directly to the
        # release prefix, eliminating this copy step.


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
    publisher: ReleasePublisher,
) -> NovaResult:
    """Process a single nova: generate and publish artifacts.

    Before generators run, loads the Nova item from DDB, pre-queries
    observation epochs from both tables, and resolves the outburst MJD
    (§7.6).  These values are placed in ``nova_context`` for all
    downstream generators.

    Each artifact is generated in dependency order and immediately
    published to S3 under the release prefix (Phase 1).

    Returns a ``NovaResult`` with success/failure and observation counts.
    Any exception from a generator marks the entire nova as failed — its
    WorkItems are retained for the next sweep.
    """
    artifacts_to_generate = manifest.get("artifacts", [])
    nova_context: dict[str, Any] = {}

    start = time.monotonic()
    try:
        # --- Per-nova setup (§4.4 step 2a–2b) ---
        nova_item = _load_nova_item(nova_id)
        if nova_item is None:
            raise ValueError(f"Nova {nova_id} not found or not ACTIVE")

        nova_context["nova_item"] = nova_item

        # Resolve outburst MJD (§7.6).
        observation_epochs = _collect_observation_epochs(nova_id)
        outburst_mjd, is_estimated = resolve_outburst_mjd(
            nova_item.get("discovery_date"),
            nova_item.get("nova_type"),
            observation_epochs,
        )
        nova_context["outburst_mjd"] = outburst_mjd
        nova_context["outburst_mjd_is_estimated"] = is_estimated

        # --- Generate and publish artifacts in dependency order ---
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
                _generate_and_publish(nova_id, artifact_type, nova_context, publisher)

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
    global _band_registry  # noqa: PLW0603

    _logger.info(
        "Artifact generator started",
        extra={"plan_id": _PLAN_ID, "workflow_name": "artifact_generator"},
    )

    # Load band registry once per Fargate execution (§8.2).
    _band_registry = _load_band_registry()

    # Step 1 — Load the batch plan.
    plan = _load_batch_plan()
    nova_manifests: dict[str, Any] = plan.get("nova_manifests", {})
    nova_count = len(nova_manifests)

    _logger.info(
        "Batch plan loaded",
        extra={"plan_id": _PLAN_ID, "nova_count": nova_count},
    )

    # Step 2 — Initialize publication and read previous pointer (§12).
    publisher = ReleasePublisher(_s3, _PUBLIC_BUCKET)
    publisher.read_previous_pointer()

    _logger.info(
        "Release initialized",
        extra={
            "release_id": publisher.release_id,
            "previous_release_id": publisher.previous_release_id,
            "phase": "publication",
        },
    )

    # Steps 3–4 — Process novae sequentially (Phase 1).
    nova_results: list[NovaResult] = []
    succeeded = 0
    failed = 0
    swept_nova_ids: set[str] = set()

    for nova_id, manifest in nova_manifests.items():
        swept_nova_ids.add(nova_id)
        _logger.info(
            "Processing nova",
            extra={
                "nova_id": nova_id,
                "artifacts": manifest.get("artifacts", []),
                "progress": f"{succeeded + failed + 1}/{nova_count}",
            },
        )
        result = _process_nova(nova_id, manifest, publisher)
        nova_results.append(result)
        if result.success:
            succeeded += 1
        else:
            failed += 1

    # Step 5 — Phase 2: copy forward unchanged ACTIVE novae (§12.5).
    #
    # The catalog generator's DDB Scan and the copy-forward step both
    # need the set of ACTIVE nova IDs.  Generate catalog first (it does
    # the Scan internally), then compute the copy set from its output.
    #
    # Ordering: catalog generation produces the artifact dict but does
    # NOT write to S3 yet.  Phase 2 copies forward, then Phase 3 writes
    # catalog.json, then Phase 4 updates the pointer.
    _logger.info("Generating catalog.json", extra={"phase": "catalog"})
    catalog_data = generate_catalog_json(nova_results, _table)

    # Compute the set of ACTIVE novae to copy forward: all novae in the
    # catalog minus those in the current sweep batch.
    active_nova_ids = {n["nova_id"] for n in catalog_data["novae"]}
    novae_to_copy = active_nova_ids - swept_nova_ids

    _logger.info(
        "Phase 2: copy-forward",
        extra={
            "active_novae": len(active_nova_ids),
            "swept_novae": len(swept_nova_ids),
            "novae_to_copy": len(novae_to_copy),
            "phase": "publication",
        },
    )
    copy_ok = publisher.copy_forward_unchanged_novae(novae_to_copy)

    # Step 6 — Phase 3: write catalog.json to release prefix.
    if copy_ok:
        _logger.info("Phase 3: writing catalog.json", extra={"phase": "publication"})
        publisher.write_catalog(catalog_data)
    else:
        _logger.error(
            "Skipping Phase 3–4: copy-forward had failures",
            extra={"phase": "publication"},
        )

    # Step 7 — Phase 4: update pointer (§12.5).
    publication_ok = False
    if copy_ok:
        try:
            _logger.info("Phase 4: updating pointer", extra={"phase": "publication"})
            publisher.update_pointer()
            publication_ok = True
        except Exception:
            _logger.exception(
                "Pointer update failed — release exists but is unreferenced",
                extra={"release_id": publisher.release_id, "phase": "publication"},
            )

    # Step 8 — Write results back to the plan.
    _write_results_to_plan(plan, nova_results)

    _logger.info(
        "Artifact generator completed",
        extra={
            "plan_id": _PLAN_ID,
            "release_id": publisher.release_id,
            "nova_count": nova_count,
            "succeeded": succeeded,
            "failed": failed,
            "publication_ok": publication_ok,
        },
    )

    # Step 9 — Exit code.
    if (succeeded == 0 and nova_count > 0) or not publication_ok:
        _logger.error(
            "Exiting with error",
            extra={
                "reason": "all novae failed" if succeeded == 0 else "publication failed",
            },
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
