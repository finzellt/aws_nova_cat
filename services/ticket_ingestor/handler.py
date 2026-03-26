"""ticket_ingestor Lambda handler.

Two tasks:

  IngestPhotometry — reads the photometry CSV described by the parsed
    PhotometryTicket, resolves each row's filter string against the band
    registry, constructs PhotometryRow objects, and returns a transform
    summary.  DDB writes are handled in Chunk 3b (ddb_writer.py).

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
      "rows_produced": <int>,
      "failures":      <int>,
  }

Failure classification:
  Wrong task_name            → TerminalError
  Malformed event payload    → TerminalError  (ticket already validated in
                               ParseTicket; a bad payload here is an SFN
                               wiring error, not an operator authoring error)
  IngestSpectra (stub)       → TerminalError  (not yet implemented)
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, cast

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
from ticket_ingestor.photometry_reader import BandRegistryProtocol, read_photometry_csv


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
    """
    Transform branch for the photometry ticket type.

    Deserialises the ticket, constructs the CSV path, delegates to the
    pure-transform photometry_reader, and returns a count summary.
    DDB writes are not performed in Chunk 3a — they land in ddb_writer.py
    (Chunk 3b).
    """
    # --- Deserialise ticket -----------------------------------------------
    raw_ticket = event.get("ticket")
    if not isinstance(raw_ticket, dict):
        raise TerminalError("Event field 'ticket' is missing or not a dict — SFN wiring error.")
    try:
        ticket = PhotometryTicket.model_validate(raw_ticket)
    except ValidationError as exc:
        raise TerminalError(
            f"PhotometryTicket validation failed — SFN wiring error: {exc}"
        ) from exc

    # --- Extract identity + routing fields --------------------------------
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
        "Starting photometry transform",
        extra={
            "nova_id": str(nova_id),
            "primary_name": primary_name,
            "csv_path": str(csv_path),
            "data_filename": ticket.data_filename,
            "correlation_id": event.get("correlation_id"),
            "job_run_id": event.get("job_run_id"),
        },
    )

    # --- Pure transform ---------------------------------------------------
    result = read_photometry_csv(
        csv_path=csv_path,
        ticket=ticket,
        nova_id=nova_id,
        primary_name=primary_name,
        ra_deg=ra_deg,
        dec_deg=dec_deg,
        registry=_REGISTRY,
    )

    logger.info(
        "Photometry transform complete",
        extra={
            "nova_id": str(nova_id),
            "rows_produced": len(result.rows),
            "failures": len(result.failures),
        },
    )

    # 3b will consume result.rows and result.failures for DDB writes.
    return {
        "rows_produced": len(result.rows),
        "failures": len(result.failures),
    }


# ---------------------------------------------------------------------------
# IngestSpectra (stub — implemented in Chunk 4)
# ---------------------------------------------------------------------------


def _ingest_spectra_stub() -> dict[str, Any]:
    """
    Placeholder for the spectra ingestion branch.

    Raises TerminalError so that a premature IngestSpectra invocation is
    immediately visible in the SFN execution history as a named, intentional
    failure rather than an opaque routing miss.  This function is replaced
    wholesale in Chunk 4.
    """
    raise TerminalError("IngestSpectra is not yet implemented")
