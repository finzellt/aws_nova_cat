"""ticket_ingestor Lambda handler.

Two tasks:

  IngestPhotometry — reads the photometry CSV described by the parsed
    PhotometryTicket, resolves each row's filter string against the band
    registry, constructs PhotometryRow objects, writes them to the dedicated
    photometry DDB table, and returns a transform summary.

  IngestSpectra — reads the spectra metadata CSV described by the parsed
    SpectraTicket, converts each referenced spectrum CSV to a FITS file,
    uploads the FITS to the Public S3 bucket, and writes DataProduct +
    FileObject reference items to the main NovaCat DDB table.

Input event fields (both tasks):
  task_name       — "IngestPhotometry" | "IngestSpectra" (enforced)
  ticket          — serialised PhotometryTicket / SpectraTicket dict
  nova_id         — UUID string (output of ResolveNova)
  primary_name    — resolved primary name (output of ResolveNova)
  ra_deg          — right ascension in decimal degrees
  dec_deg         — declination in decimal degrees
  data_dir        — bare S3 prefix or local filesystem path to the directory
                    containing data files.  In the deployed stack this is a
                    bare S3 prefix resolved against NOVA_CAT_PRIVATE_BUCKET;
                    in integration tests it is a local path and is used as-is.
  correlation_id  — request-scoped correlation identifier (for logging)
  job_run_id      — JobRun UUID (for logging and FileObject provenance)

Output shape (IngestPhotometry):
  {
      "rows_produced": <int>,
      "failures":      <int>,
  }

Output shape (IngestSpectra):
  {
      "spectra_ingested": <int>,
      "spectra_failed":   <int>,
  }

Failure classification:
  Wrong task_name            → TerminalError
  Malformed event payload    → TerminalError  (ticket already validated in
                               ParseTicket; a bad payload here is an SFN
                               wiring error, not an operator authoring error)
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
from nova_common.errors import TerminalError
from nova_common.file_io import resolve_dir
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

# ---------------------------------------------------------------------------
# Band registry — loaded once at module initialisation.
# ---------------------------------------------------------------------------
from photometry_ingestor.band_registry import (
    registry as _registry_module,
)
from pydantic import ValidationError

from contracts.models.tickets import PhotometryTicket, SpectraTicket
from ticket_ingestor.ddb_writer import (
    persist_row_failures,
    upsert_envelope_item,
    write_photometry_rows,
)
from ticket_ingestor.photometry_reader import BandRegistryProtocol, read_photometry_csv
from ticket_ingestor.spectra_reader import read_spectra
from ticket_ingestor.spectra_writer import write_spectrum

# ---------------------------------------------------------------------------
# AWS clients — module-level so moto patches them on fresh import in tests.
# ---------------------------------------------------------------------------

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_PHOTOMETRY_TABLE_NAME = os.environ["PHOTOMETRY_TABLE_NAME"]
_PUBLIC_BUCKET_NAME = os.environ["NOVA_CAT_PUBLIC_SITE_BUCKET"]
_DIAGNOSTICS_BUCKET_NAME = os.environ["DIAGNOSTICS_BUCKET"]
_PRIVATE_BUCKET = os.environ["NOVA_CAT_PRIVATE_BUCKET"]

_dynamodb = boto3.resource("dynamodb")
_TABLE = _dynamodb.Table(_TABLE_NAME)
_PHOTOMETRY_TABLE = _dynamodb.Table(_PHOTOMETRY_TABLE_NAME)
_s3 = boto3.client("s3")


# ---------------------------------------------------------------------------
# Band registry adapter
# ---------------------------------------------------------------------------


class _RegistryAdapter:
    """Thin adapter that presents the registry module as BandRegistryProtocol."""

    def lookup_band_id(self, alias: str) -> str | None:
        return _registry_module.lookup_band_id(alias)

    def get_entry(self, band_id: str) -> Any:
        return _registry_module.get_entry(band_id)

    def is_excluded(self, band_id: str) -> bool:
        return _registry_module.is_excluded(band_id)


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
        return _ingest_spectra(event)
    raise TerminalError(f"Unknown task_name: {task_name!r}")


# ---------------------------------------------------------------------------
# IngestPhotometry
# ---------------------------------------------------------------------------


@tracer.capture_method
def _ingest_photometry(event: dict[str, Any]) -> dict[str, Any]:
    """
    Transform branch for the photometry ticket type.

    Resolves ``data_dir`` to a local directory via ``resolve_dir`` (S3
    download when deployed; pass-through for local paths in integration
    tests), then reads the photometry CSV, runs the pure transform, and
    performs DDB writes.
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
        data_dir_spec: str = str(event["data_dir"])
    except (KeyError, ValueError, TypeError) as exc:
        raise TerminalError(
            f"Malformed event — missing or invalid identity/routing field: {exc}"
        ) from exc

    # --- Resolve data directory (S3 prefix → /tmp in deployed Lambda) -----
    local_data_dir = resolve_dir(
        data_dir_spec,
        s3_client=_s3,
        bucket=_PRIVATE_BUCKET,
    )

    csv_path = local_data_dir / ticket.data_filename

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

    # --- DDB writes -------------------------------------------------------
    write_result = write_photometry_rows(
        rows=result.rows,
        nova_id=nova_id,
        table_name=_PHOTOMETRY_TABLE_NAME,
        table=_PHOTOMETRY_TABLE,
    )
    persist_row_failures(
        failures=result.failures,
        nova_id=nova_id,
        filename=ticket.data_filename,
        bucket=_DIAGNOSTICS_BUCKET_NAME,
        s3=_s3,
    )
    upsert_envelope_item(
        nova_id=nova_id,
        rows_written=write_result.rows_written,
        table=_TABLE,
    )

    return {
        "rows_produced": len(result.rows),
        "failures": len(result.failures),
    }


# ---------------------------------------------------------------------------
# IngestSpectra
# ---------------------------------------------------------------------------


@tracer.capture_method
def _ingest_spectra(event: dict[str, Any]) -> dict[str, Any]:
    """
    Ingestion branch for the spectra ticket type.

    Resolves ``data_dir`` to a local directory via ``resolve_dir`` (S3
    download when deployed; pass-through for local paths in integration
    tests).  The resolved local directory is passed to both ``read_spectra``
    (for the metadata CSV and spectrum data CSVs) and the per-spectrum write
    loop.

    Per-spectrum failures collected by read_spectra (missing data file,
    malformed CSV, FITS construction error) are counted and returned without
    aborting the batch.  S3/DDB write failures for individual spectra are
    also caught and counted — a single broken spectrum should not abort the
    rest of the batch.
    """
    # --- Deserialise ticket -----------------------------------------------
    raw_ticket = event.get("ticket")
    if not isinstance(raw_ticket, dict):
        raise TerminalError("Event field 'ticket' is missing or not a dict — SFN wiring error.")
    try:
        ticket = SpectraTicket.model_validate(raw_ticket)
    except ValidationError as exc:
        raise TerminalError(f"SpectraTicket validation failed — SFN wiring error: {exc}") from exc

    # --- Extract identity + routing fields --------------------------------
    try:
        nova_id = uuid.UUID(str(event["nova_id"]))
        data_dir_spec: str = str(event["data_dir"])
        job_run_id: str = str(event.get("job_run_id", "unknown"))
    except (KeyError, ValueError, TypeError) as exc:
        raise TerminalError(
            f"Malformed event — missing or invalid identity/routing field: {exc}"
        ) from exc

    # --- Resolve data directory (S3 prefix → /tmp in deployed Lambda) -----
    local_data_dir = resolve_dir(
        data_dir_spec,
        s3_client=_s3,
        bucket=_PRIVATE_BUCKET,
    )

    metadata_csv_path = local_data_dir / ticket.metadata_filename

    logger.info(
        "Starting spectra ingest",
        extra={
            "nova_id": str(nova_id),
            "metadata_filename": ticket.metadata_filename,
            "metadata_csv_path": str(metadata_csv_path),
            "correlation_id": event.get("correlation_id"),
            "job_run_id": job_run_id,
        },
    )

    # --- Pure transform (no S3/DDB) ---------------------------------------
    read_result = read_spectra(
        metadata_csv_path=metadata_csv_path,
        data_dir=local_data_dir,
        ticket=ticket,
        nova_id=nova_id,
    )

    if read_result.failures:
        logger.warning(
            "Per-spectrum read failures — batch continues",
            extra={
                "nova_id": str(nova_id),
                "failure_count": len(read_result.failures),
                "failures": [
                    {"filename": f.spectrum_filename, "reason": f.reason}
                    for f in read_result.failures
                ],
            },
        )

    # --- S3 upload + DDB writes (per spectrum) ----------------------------
    spectra_ingested = 0
    write_failures = len(read_result.failures)

    for result in read_result.results:
        try:
            write_spectrum(
                result=result,
                nova_id=nova_id,
                job_run_id=job_run_id,
                bucket=_PUBLIC_BUCKET_NAME,
                s3=_s3,
                table=_TABLE,
            )
            spectra_ingested += 1
        except Exception as exc:  # noqa: BLE001
            write_failures += 1
            logger.error(
                "Failed to write spectrum — skipping",
                extra={
                    "nova_id": str(nova_id),
                    "spectrum_filename": result.spectrum_filename,
                    "data_product_id": str(result.data_product_id),
                    "error": str(exc),
                },
            )

    logger.info(
        "Spectra ingest complete",
        extra={
            "nova_id": str(nova_id),
            "spectra_ingested": spectra_ingested,
            "spectra_failed": write_failures,
        },
    )

    return {
        "spectra_ingested": spectra_ingested,
        "spectra_failed": write_failures,
    }
