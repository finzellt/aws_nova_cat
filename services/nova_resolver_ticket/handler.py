"""
nova_resolver_ticket Lambda handler

Resolves the ticket's OBJECT NAME to a stable nova identity for the
ingest_ticket workflow (DESIGN-004, §5).

Single task: ResolveNova.

Resolution sequence (DESIGN-004 §5.1 / ingest-ticket.md):
  1. Normalize object_name (strip → lowercase → collapse whitespace).
  2. Preflight DDB query: PK = "NAME#<normalized>" — NameMapping lookup.
     If a NameMapping item is found, return nova_id immediately and fetch
     ra_deg / dec_deg from the Nova item (PK = <nova_id>, SK = "NOVA").
  3. If not found: fire initialize_nova via sfn:StartSyncExecution (Express
     workflow — StartExecution + DescribeExecution is unsupported).
  4. On CREATED_AND_LAUNCHED / EXISTS_AND_LAUNCHED: extract nova_id from
     $.finalize in the execution output; fetch coordinates from the Nova item.
  5. On NOT_FOUND: raise QuarantineError("UNRESOLVABLE_OBJECT_NAME").
  6. On QUARANTINED (coordinate ambiguity): raise QuarantineError("IDENTITY_AMBIGUITY").
  7. On any other terminal failure: raise TerminalError.

Handler output (placed at ResolveNova ResultPath by Step Functions):
  {
      "nova_id":      str,
      "primary_name": str,
      "ra_deg":       float | None,
      "dec_deg":      float | None,
  }

primary_name is set to the ticket's OBJECT NAME (the operator-supplied name
that was used as candidate_name for resolution). The nova_id is the canonical
identifier for all downstream operations.

Environment variables:
  NOVA_CAT_TABLE_NAME               — DynamoDB table name
  INITIALIZE_NOVA_STATE_MACHINE_ARN — ARN of the initialize_nova Express
                                      workflow
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, cast

import boto3
from boto3.dynamodb.conditions import Key
from nova_common.errors import TerminalError
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_SFN_ARN = os.environ["INITIALIZE_NOVA_STATE_MACHINE_ARN"]

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)
_sfn = boto3.client("stepfunctions")

# initialize_nova outcomes that confirm a nova_id was assigned.
_RESOLVED_OUTCOMES: frozenset[str] = frozenset({"CREATED_AND_LAUNCHED", "EXISTS_AND_LAUNCHED"})


# Exception class names matched by the ingest_ticket ASL Catch blocks.
# Step Functions matches on the Python exception class __name__, so these
# classes must be named exactly as they appear in ErrorEquals.
class UNRESOLVABLE_OBJECT_NAME(Exception):  # noqa: N818
    """Object name not found in any archive — ticket cannot be ingested."""


class IDENTITY_AMBIGUITY(Exception):  # noqa: N818
    """Coordinate ambiguity prevents unambiguous nova identification."""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    if task_name != "ResolveNova":
        raise TerminalError(f"Unknown task_name: {task_name!r}")
    return _resolve_nova(event)


# ---------------------------------------------------------------------------
# Task implementation
# ---------------------------------------------------------------------------


@tracer.capture_method
def _resolve_nova(event: dict[str, Any]) -> dict[str, Any]:
    """
    Resolve the ticket OBJECT NAME to a stable nova_id with coordinates.

    Implements the two-phase strategy from DESIGN-004 §5.1:
      Phase 1 — cheap preflight DDB read (hits the common case).
      Phase 2 — start_sync_execution against initialize_nova (new/unknown names).
    """
    object_name: str = event["object_name"]
    normalized = _normalize(object_name)

    logger.info(
        "Resolving nova from ticket",
        extra={"object_name": object_name, "normalized": normalized},
    )

    # ── Phase 1: preflight DDB lookup ──────────────────────────────────────
    resp = _table.query(
        KeyConditionExpression=Key("PK").eq(f"NAME#{normalized}"),
        Limit=1,
    )
    items: list[dict[str, Any]] = resp.get("Items", [])

    if items:
        nova_id: str = items[0]["nova_id"]
        logger.info("Preflight hit — nova already resolved", extra={"nova_id": nova_id})
        ra_deg, dec_deg = _fetch_coordinates(nova_id)
        return _build_result(nova_id, object_name, ra_deg, dec_deg)

    # ── Phase 2: fire initialize_nova synchronously ─────────────────────────
    # initialize_nova is an Express workflow; start_sync_execution is the only
    # supported API (describe_execution is not available for Express workflows).
    logger.info(
        "Preflight miss — firing initialize_nova",
        extra={"object_name": object_name},
    )
    start_resp = _sfn.start_sync_execution(
        stateMachineArn=_SFN_ARN,
        input=json.dumps(
            {
                "candidate_name": object_name,
                "source": "ingest_ticket",
                "correlation_id": event.get("correlation_id"),
            }
        ),
    )

    execution_arn: str = start_resp["executionArn"]
    status: str = start_resp["status"]
    logger.info(
        "initialize_nova completed", extra={"executionArn": execution_arn, "status": status}
    )

    if status != "SUCCEEDED":
        raise TerminalError(
            f"initialize_nova execution terminated with status={status!r} arn={execution_arn!r}"
        )

    raw_output: str = start_resp.get("output", "{}")
    output = cast(dict[str, Any], json.loads(raw_output))
    nova_id = _extract_nova_id(output, execution_arn)

    ra_deg, dec_deg = _fetch_coordinates(nova_id)
    return _build_result(nova_id, object_name, ra_deg, dec_deg)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize(name: str) -> str:
    """Strip, lowercase, replace underscores with spaces, and collapse internal whitespace — identical to nova_resolver."""
    return re.sub(r"\s+", " ", name.replace("_", " ").strip().lower())


def _extract_nova_id(output: dict[str, Any], execution_arn: str) -> str:
    """
    Parse initialize_nova terminal output and return nova_id.

    Outcome mapping (DESIGN-004 §5.1):
      CREATED_AND_LAUNCHED / EXISTS_AND_LAUNCHED
          → nova_id from $.finalize.nova_id
      NOT_FOUND
          → QuarantineError("UNRESOLVABLE_OBJECT_NAME")
      absent / unrecognized (coordinate-ambiguity quarantine path)
          → QuarantineError("IDENTITY_AMBIGUITY")
      nova_id missing despite a resolved outcome
          → TerminalError (invariant violation)

    The coordinate-ambiguity quarantine path ends via FinalizeJobRunQuarantined,
    not FinalizeJobRunSuccess, so $.finalize.outcome is absent in that case.

    Raises:
        QuarantineError: Object name unresolvable, or coordinate ambiguity.
        TerminalError:   Unexpected output structure — operator investigation
                         required.
    """
    finalize: dict[str, Any] = output.get("finalize", {})
    outcome: str | None = finalize.get("outcome")

    if outcome in _RESOLVED_OUTCOMES:
        # nova_id is written to $.launch.nova_id (or $.upsert.nova_id) by
        # initialize_nova — it is NOT in $.finalize.
        nova_id: str | None = (
            output.get("launch", {}).get("nova_id")
            or output.get("upsert", {}).get("nova_id")
            or output.get("nova_creation", {}).get("nova_id")
        )
        if not nova_id:
            raise TerminalError(
                f"initialize_nova returned outcome={outcome!r} but nova_id is absent "
                f"from execution output; arn={execution_arn!r}"
            )
        logger.info(
            "initialize_nova resolved nova",
            extra={"outcome": outcome, "nova_id": nova_id},
        )
        return nova_id

    if outcome == "NOT_FOUND":
        logger.warning(
            "initialize_nova returned NOT_FOUND — object name unresolvable",
            extra={"executionArn": execution_arn},
        )
        raise UNRESOLVABLE_OBJECT_NAME(f"Object name unresolvable; arn={execution_arn!r}")

    if outcome == "NOT_A_CLASSICAL_NOVA":
        logger.warning(
            "initialize_nova returned NOT_A_CLASSICAL_NOVA — ticket targets a non-classical nova",
            extra={"executionArn": execution_arn},
        )
        raise TerminalError(
            f"Object resolved successfully but is not a classical nova; "
            f"ticket should be removed or reclassified. arn={execution_arn!r}"
        )

    # Execution SUCCEEDED but $.finalize.outcome is absent or unrecognized.
    # This is the coordinate-ambiguity quarantine branch: initialize_nova ends
    # via FinalizeJobRunQuarantined, which does not populate $.finalize.outcome.
    logger.warning(
        "initialize_nova completed without a recognized outcome — treating as quarantined",
        extra={"outcome": outcome, "executionArn": execution_arn},
    )
    raise IDENTITY_AMBIGUITY(f"Coordinate ambiguity; arn={execution_arn!r}")


def _fetch_coordinates(nova_id: str) -> tuple[float | None, float | None]:
    """
    Fetch ra_deg and dec_deg from the Nova item (PK=<nova_id>, SK="NOVA").

    Coordinates are stored as Decimal by nova_resolver; converted to float
    here. Returns (None, None) if the Nova item has no coordinate fields —
    this is legitimate for novae resolved via TNS without coordinate data.

    Raises:
        TerminalError: If the Nova item does not exist in DDB. This should
                       never happen immediately after a successful resolution,
                       and indicates an infrastructure invariant violation.
    """
    resp = _table.get_item(Key={"PK": nova_id, "SK": "NOVA"})
    item: dict[str, Any] | None = resp.get("Item")
    if item is None:
        raise TerminalError(
            f"Nova item absent from DDB immediately after resolution — nova_id={nova_id!r}"
        )

    ra_raw = item.get("ra_deg")
    dec_raw = item.get("dec_deg")

    ra_deg: float | None = float(ra_raw) if ra_raw is not None else None
    dec_deg: float | None = float(dec_raw) if dec_raw is not None else None
    return ra_deg, dec_deg


def _build_result(
    nova_id: str,
    primary_name: str,
    ra_deg: float | None,
    dec_deg: float | None,
) -> dict[str, Any]:
    """Assemble the ResolveNova task output dict."""
    return {
        "nova_id": nova_id,
        "primary_name": primary_name,
        "ra_deg": ra_deg,
        "dec_deg": dec_deg,
    }
