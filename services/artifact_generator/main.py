"""artifact_generator — Fargate task scaffold (DESIGN-003 §4.4).

Entry point for the ECS Fargate container launched by the
``regenerate_artifacts`` Step Functions workflow.  Receives a
``plan_id`` via the ``PLAN_ID`` environment variable (set by the
RunTask container override).

Execution steps (§4.4):
  1. Load the ``RegenBatchPlan`` from DynamoDB.
  2. For each nova in the plan, generate all artifacts specified in
     its manifest in dependency order (``GENERATION_ORDER``).
  3. Track per-nova success/failure — a single nova's failure does
     not abort the batch.
  4. After all novae, generate ``catalog.json`` (stub for Epic 2).
  5. Write ``nova_results`` back to the batch plan in DynamoDB.
  6. Exit with code 0 (success) or 1 (all novae failed).

**Epic 2 scope:** All generators are no-op stubs that return dummy
counts.  The scaffold proves the control flow — sequential nova
processing, per-nova error isolation, and result payload assembly.
Epic 3 replaces the stubs with real generators.

Environment variables:
    PLAN_ID                — batch plan UUID (required)
    NOVA_CAT_TABLE_NAME    — DynamoDB table name (required)
    LOG_LEVEL              — logging level (default INFO)
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key

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

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)


# ---------------------------------------------------------------------------
# Generator stubs (Epic 2 — no-ops, replaced in Epic 3)
# ---------------------------------------------------------------------------


def _generate_artifact_stub(
    nova_id: str,
    artifact: ArtifactType,
    nova_context: dict[str, Any],
) -> None:
    """No-op generator stub.

    In Epic 3, this dispatch function will route to real generators.
    For now, it populates dummy observation counts in the nova context
    to exercise the result payload assembly.

    The *nova_context* dict accumulates per-nova state across generators
    within a single Fargate execution — this is the "state continuity"
    benefit described in §4.4.
    """
    if artifact == ArtifactType.spectra_json:
        nova_context["spectra_count"] = 0
    elif artifact == ArtifactType.photometry_json:
        nova_context["photometry_count"] = 0
    elif artifact == ArtifactType.references_json:
        nova_context["references_count"] = 0
    elif artifact == ArtifactType.sparkline_svg:
        nova_context["has_sparkline"] = False


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
) -> NovaResult:
    """Process a single nova: run generators in dependency order.

    Returns a ``NovaResult`` with success/failure and observation counts.
    Any exception from a generator marks the entire nova as failed — its
    WorkItems are retained for the next sweep.
    """
    artifacts_to_generate = manifest.get("artifacts", [])
    nova_context: dict[str, Any] = {}

    start = time.monotonic()
    try:
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
                _generate_artifact_stub(nova_id, artifact_type, nova_context)
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
        result = _process_nova(nova_id, manifest)
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
