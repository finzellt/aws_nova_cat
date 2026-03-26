"""ticket_ingestor Lambda handler.

Two tasks:

  IngestPhotometry — reads the photometry CSV described by the parsed
    PhotometryTicket, resolves each row's filter string against the band
    registry, constructs PhotometryRow objects, writes them to DynamoDB,
    persists any row-level failures to S3, and updates the
    PRODUCT#PHOTOMETRY_TABLE envelope item.

  IngestSpectra — stub only; implemented in Chunk 4.

Input event fields (both tasks):
  task_name       — "IngestPhotometry" | "IngestSpectra" (enforced)
  ticket          — serialised PhotometryTicket / SpectraTicket dict
  nova_id         — UUID string (output of ResolveNova)
  primary_name    — resolved primary name (output of ResolveNova)
  ra_deg          — right ascension in decimal degrees
  dec_deg         — declination in decimal degrees
  data_dir        — filesystem path to the directory containing data files
  correlation_id  — request-scoped correlation identifier (for logging)
  job_run_id      — JobRun UUID (for logging)

Output shape (IngestPhotometry):
  {
      "rows_produced":          <int>,
      "rows_written":           <int>,
      "rows_skipped_duplicate": <int>,
      "failures":               <int>,
  }

Failure classification:
  Wrong task_name            → TerminalError
  Malformed event payload    → TerminalError  (ticket already validated in
                               ParseTicket; a bad payload here is an SFN
                               wiring error, not an operator authoring error)
  IngestSpectra (stub)       → TerminalError  (not yet implemented)

Environment variables:
  PHOTOMETRY_TABLE_NAME — dedicated photometry DynamoDB table
  NOVA_CAT_TABLE_NAME   — main NovaCat DynamoDB table (envelope items)
  DIAGNOSTICS_BUCKET    — S3 bucket for row failure diagnostics
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, cast

import boto3
from nova_common.errors import TerminalError
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

# ---------------------------------------------------------------------------
# Band registry — loaded once at module initialisation.
# The registry module is a singleton; importing it here causes it to load
# band_registry.json and build the alias index exactly once per Lambda
# cold start.  The module's public functions satisfy BandRegistryProtocol
# structurally, so we wrap it in a thin adapter below.
# ---------------------------------------------------------------------------
from photometry_ingestor.band_registry import (  # type: ignore[import-not-found]
    registry as _registry_module,
)
from pydantic import ValidationError

from contracts.models.tickets import PhotometryTicket
from ticket_ingestor.ddb_writer import (
    persist_row_failures,
    upsert_envelope_item,
    write_photometry_rows,
)
from ticket_ingestor.photometry_reader import BandRegistryProtocol, read_photometry_csv

# ---------------------------------------------------------------------------
# Module-level AWS clients — created once per cold start.
# All three env vars are required; missing vars raise at import time so that
# misconfigured deployments fail fast on the first invocation rather than at
# write time.
# ---------------------------------------------------------------------------
_PHOTOMETRY_TABLE_NAME = os.environ["PHOTOMETRY_TABLE_NAME"]
_NOVA_CAT_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_DIAGNOSTICS_BUCKET = os.environ["DIAGNOSTICS_BUCKET"]

_dynamodb = boto3.resource("dynamodb")
_photometry_table = _dynamodb.Table(_PHOTOMETRY_TABLE_NAME)
_nova_cat_table = _dynamodb.Table(_NOVA_CAT_TABLE_NAME)
_s3 = boto3.client("s3")


# ---------------------------------------------------------------------------
# Band registry adapter
# ---------------------------------------------------------------------------


class _RegistryAdapter:
    """Thin adapter that presents the registry module as BandRegistryProtocol."""

    def lookup_band_id(self, alias: str) -> str | None:
        return cast("str | None", _registry_module.lookup_band_id(alias))

    def get_entry(self, band_id: str) -> Any:
        return _registry_module.get_entry(band_id)

    def is_excluded(self, band_id: str) -> bool:
        return cast(bool, _registry_module.is_excluded(band_id))


_REGISTRY: BandRegistryProtocol = _RegistryAdapter()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    if task_name == "IngestPhotometry":
        return _ingest_photometry(event)
    if task_name == "IngestSpectra":
        return _ingest_spectra_stub()
    raise TerminalError(f"Unknown task_name: {task_name!r}")


# ---------------------------------------------------------------------------
# IngestPhotometry
# ---------------------------------------------------------------------------


@tracer.capture_method
def _ingest_photometry(event: dict[str, Any]) -> dict[str, Any]:
    """Transform, write, and summarise one photometry ticket.

    Stages:
      1. Deserialise and validate the PhotometryTicket from the event.
      2. Extract identity / routing fields from the event.
      3. Delegate to photometry_reader for pure CSV → PhotometryRow transform.
      4. Write successful rows to the photometry DDB table (ddb_writer).
      5. Persist row failures to S3 diagnostics (no-op if none).
      6. Upsert the PRODUCT#PHOTOMETRY_TABLE envelope item.
      7. Return a summary dict consumed by the Step Functions state machine.
    """
    # --- 1. Deserialise ticket --------------------------------------------
    raw_ticket = event.get("ticket")
    if not isinstance(raw_ticket, dict):
        raise TerminalError("Event field 'ticket' is missing or not a dict — SFN wiring error.")
    try:
        ticket = PhotometryTicket.model_validate(raw_ticket)
    except ValidationError as exc:
        raise TerminalError(
            f"PhotometryTicket validation failed — SFN wiring error: {exc}"
        ) from exc

    # --- 2. Extract identity + routing fields -----------------------------
    try:
        nova_id = uuid.UUID(str(event["nova_id"]))
        primary_name: str = str(event["primary_name"])
        ra_deg: float = float(event["ra_deg"])
        dec_deg: float = float(event["dec_deg"])
        data_dir: str = str(event["data_dir"])
    except (KeyError, ValueError, TypeError) as exc:
        raise TerminalError(
            f"Malformed event — missing or invalid identity/routing field: {exc}"
        ) from exc

    csv_path = Path(data_dir) / ticket.data_filename

    logger.info(
        "Starting photometry ingest",
        extra={
            "nova_id": str(nova_id),
            "primary_name": primary_name,
            "csv_path": str(csv_path),
            "data_filename": ticket.data_filename,
            "correlation_id": event.get("correlation_id"),
            "job_run_id": event.get("job_run_id"),
        },
    )

    # --- 3. Pure transform -----------------------------------------------
    result = read_photometry_csv(
        csv_path=csv_path,
        ticket=ticket,
        nova_id=nova_id,
        primary_name=primary_name,
        ra_deg=ra_deg,
        dec_deg=dec_deg,
        registry=_REGISTRY,
    )

    # --- 4. Write rows to DDB --------------------------------------------
    write_result = write_photometry_rows(
        rows=result.rows,
        nova_id=nova_id,
        table_name=_PHOTOMETRY_TABLE_NAME,
        table=_photometry_table,
    )

    # --- 5. Persist row failures to S3 (no-op if empty) ------------------
    persist_row_failures(
        failures=result.failures,
        nova_id=nova_id,
        filename=ticket.data_filename,
        bucket=_DIAGNOSTICS_BUCKET,
        s3=_s3,
    )

    # --- 6. Upsert envelope item -----------------------------------------
    upsert_envelope_item(
        nova_id=nova_id,
        rows_written=write_result.rows_written,
        table=_nova_cat_table,
    )

    logger.info(
        "Photometry ingest complete",
        extra={
            "nova_id": str(nova_id),
            "rows_produced": len(result.rows),
            "rows_written": write_result.rows_written,
            "rows_skipped_duplicate": write_result.rows_skipped_duplicate,
            "failures": len(result.failures),
        },
    )

    # --- 7. Return summary -----------------------------------------------
    return {
        "rows_produced": len(result.rows),
        "rows_written": write_result.rows_written,
        "rows_skipped_duplicate": write_result.rows_skipped_duplicate,
        "failures": len(result.failures),
    }


# ---------------------------------------------------------------------------
# IngestSpectra (stub — implemented in Chunk 4)
# ---------------------------------------------------------------------------


def _ingest_spectra_stub() -> dict[str, Any]:
    """Placeholder for the spectra ingestion branch.

    Raises TerminalError so that a premature IngestSpectra invocation is
    immediately visible in the SFN execution history as a named, intentional
    failure rather than an opaque routing miss.  This function is replaced
    wholesale in Chunk 4.
    """
    raise TerminalError("IngestSpectra is not yet implemented")
