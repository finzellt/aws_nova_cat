"""spectra_writer — S3 upload and DDB reference item creation.

Consumes a SpectrumResult produced by spectra_reader and writes two
artefacts:

  1. The raw FITS bytes to the Public S3 bucket at the result's s3_key.
  2. A SPECTRA DataProduct item in the main NovaCat DynamoDB table.
  3. A SPECTRA_RAW_FITS FileObject item in the same table.

All boto3 objects are injected by the caller (handler.py).  This module
contains no module-level boto3 clients and no environment variable reads.

Idempotency
-----------
DataProduct writes are unconditional PutItem.  The data_product_id is
derived deterministically in spectra_reader (UUID5), so a re-run of the
same ticket overwrites the same item cleanly without accumulating
duplicates.

FileObject writes use a fresh uuid4() for file_id on each call.  This
means a re-run creates a new FileObject rather than overwriting the
previous one.  That is acceptable: FileObject records are a lightweight
provenance index, not a uniqueness boundary.

Public API
----------
write_spectrum(result, nova_id, job_run_id, bucket, s3, table, private_bucket) -> None
"""

from __future__ import annotations

import enum
import io
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import astropy.io.fits as fits
from nova_common.web_ready_csv import build_web_ready_csv, write_web_ready_csv_to_s3

from ticket_ingestor.spectra_reader import SpectrumResult

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = "1.0.0"
_ENTITY_TYPE_PRODUCT = "DataProduct"
_ENTITY_TYPE_FILE = "FileObject"
_PROVIDER = "ticket_ingestion"
_CONTENT_TYPE = "application/fits"

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with 'Z' suffix."""
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _coerce_for_ddb(value: Any) -> Any:
    """Recursively coerce a value to a DynamoDB-safe type.

    Rules (consistent with ddb_writer._coerce_for_ddb):
      float  → Decimal(str(v))
      UUID   → str(v)
      Enum   → v.value
      dict   → recursively coerced
      list   → recursively coerced
      None   → callers should exclude None before calling; returned as-is
               to preserve explicitness in item construction.
      other  → returned unchanged (int, str, bool, Decimal all accepted)
    """
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _coerce_for_ddb(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_coerce_for_ddb(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def write_spectrum(
    result: SpectrumResult,
    nova_id: uuid.UUID,
    job_run_id: str,
    bucket: str,
    s3: Any,  # boto3 S3 client
    table: Any,  # boto3 DynamoDB Table resource
    private_bucket: str | None = None,  # private S3 bucket for derived artifacts
) -> None:
    """Upload FITS bytes to S3 and write DataProduct + FileObject items to DDB.

    Parameters
    ----------
    result:
        SpectrumResult produced by spectra_reader.read_spectra for one
        spectrum.  Carries fits_bytes, s3_key, data_product_id,
        locator_identity, and ADR-031 enrichment fields (instrument,
        telescope, observation_date_mjd, flux_unit).
    nova_id:
        Resolved UUID for the nova; used as the DynamoDB partition key and
        embedded in FileObject attributes.
    job_run_id:
        JobRun UUID string threaded from the workflow event; written into
        the FileObject created_by field for traceability.
    bucket:
        Name of the Public S3 bucket to upload FITS bytes to.
    s3:
        Injected boto3 S3 client.
    table:
        Injected boto3 DynamoDB Table resource (main NovaCat table).

    Raises
    ------
    botocore.exceptions.ClientError
        Propagated unchanged for both S3 and DynamoDB failures.  The caller
        (handler._ingest_spectra) is responsible for deciding whether to
        surface as TerminalError or collect as a per-spectrum failure.
    """
    now = _now_iso()
    nova_id_str = str(nova_id)
    data_product_id_str = str(result.data_product_id)

    # ── 1. S3 upload ────────────────────────────────────────────────────────
    s3.put_object(
        Bucket=bucket,
        Key=result.s3_key,
        Body=result.fits_bytes,
        ContentType=_CONTENT_TYPE,
    )

    # ── 2. DataProduct PutItem ───────────────────────────────────────────────
    #
    # Lifecycle state for ticket-ingested spectra: the FITS file is produced
    # directly by this pipeline (not acquired from an external provider), so
    # it is immediately ACQUIRED + VALID with no further eligibility.
    #
    # GSI1 attributes (EligibilityIndex) are intentionally omitted: with
    # eligibility=NONE the product must not appear in acquisition queries.
    #
    # SK pattern: PRODUCT#SPECTRA#<provider>#<data_product_id>
    # (consistent with docs/storage/dynamodb-item-model.md §3.2)
    dp_sk = f"PRODUCT#SPECTRA#{_PROVIDER}#{data_product_id_str}"

    dp_item: dict[str, Any] = {
        "PK": nova_id_str,
        "SK": dp_sk,
        "entity_type": _ENTITY_TYPE_PRODUCT,
        "schema_version": _SCHEMA_VERSION,
        "data_product_id": data_product_id_str,
        "nova_id": nova_id_str,
        "product_type": "SPECTRA",
        "provider": _PROVIDER,
        "locator_identity": result.locator_identity,
        "acquisition_status": "ACQUIRED",
        "validation_status": "VALID",
        "eligibility": "NONE",
        "attempt_count": 1,
        "last_attempt_at": now,
        "last_attempt_outcome": "SUCCESS",
        "raw_s3_bucket": bucket,
        "raw_s3_key": result.s3_key,
        "byte_length": len(result.fits_bytes),
        "created_at": now,
        "updated_at": now,
    }

    # --- ADR-031 enrichment fields (Decisions 2, 3, 5) ---
    # Only include when non-None — null-means-absent contract.
    # observation_date_mjd is coerced to Decimal for DDB Number type.
    if result.instrument is not None:
        dp_item["instrument"] = result.instrument

    if result.telescope is not None:
        dp_item["telescope"] = result.telescope

    if result.observation_date_mjd is not None:
        dp_item["observation_date_mjd"] = _coerce_for_ddb(result.observation_date_mjd)

    if result.flux_unit is not None:
        dp_item["flux_unit"] = result.flux_unit

    # --- S21: ingestion-time SNR ---
    if result.snr is not None:
        dp_item["snr"] = _coerce_for_ddb(result.snr)
    if result.snr_provenance is not None:
        dp_item["snr_provenance"] = result.snr_provenance

    table.put_item(Item=dp_item)

    # ── 2b. Web-ready CSV (ADR-031 P-4) ────────────────────────────────
    #
    # Best-effort: a failed write logs a warning but does not fail the
    # spectrum write.  The ticket path has the FITS bytes in memory, so
    # we re-open them to extract the validated arrays and spectral_units.
    if private_bucket is not None:
        try:
            with fits.open(io.BytesIO(result.fits_bytes), memmap=False) as hdul:
                primary_data = hdul[0].data
                spectral_units = hdul[0].header.get("CUNIT1", "Angstrom")
                # Flux is in the primary HDU data array; wavelength must be
                # reconstructed from WCS keywords (CRVAL1 + CDELT1 * index).
                crval1 = float(hdul[0].header["CRVAL1"])
                cdelt1 = float(hdul[0].header.get("CDELT1", 1.0))
                crpix1 = float(hdul[0].header.get("CRPIX1", 1.0))
                import numpy as np

                n_pix = len(primary_data)
                wavelength = crval1 + cdelt1 * (np.arange(n_pix) - (crpix1 - 1.0))
                flux = np.asarray(primary_data, dtype=np.float64)

            csv_content = build_web_ready_csv(
                wavelength=wavelength,
                flux=flux,
                spectral_units=spectral_units,
            )
            write_web_ready_csv_to_s3(
                csv_content=csv_content,
                nova_id=nova_id_str,
                data_product_id=data_product_id_str,
                s3=s3,
                bucket=private_bucket,
            )
        except Exception:  # noqa: BLE001
            import logging

            logging.getLogger(__name__).warning(
                "Failed to write web-ready CSV for ticket spectrum — continuing",
                exc_info=True,
                extra={
                    "data_product_id": data_product_id_str,
                    "nova_id": nova_id_str,
                },
            )

    # ── 3. FileObject PutItem ────────────────────────────────────────────────
    #
    # A fresh file_id is minted on each call.  Re-runs therefore create a new
    # FileObject rather than overwriting the previous one — acceptable because
    # FileObject is a provenance index, not a uniqueness boundary.
    #
    # SK pattern: FILE#SPECTRA_RAW_FITS#NOVA#<nova_id>#ID#<file_id>
    # (consistent with docs/storage/dynamodb-item-model.md §5)
    file_id = uuid.uuid4()
    file_id_str = str(file_id)
    fo_sk = f"FILE#SPECTRA_RAW_FITS#NOVA#{nova_id_str}#ID#{file_id_str}"

    fo_item: dict[str, Any] = {
        "PK": nova_id_str,
        "SK": fo_sk,
        "entity_type": _ENTITY_TYPE_FILE,
        "schema_version": _SCHEMA_VERSION,
        "file_id": file_id_str,
        "nova_id": nova_id_str,
        "data_product_id": data_product_id_str,
        "role": "SPECTRA_RAW_FITS",
        "bucket": bucket,
        "key": result.s3_key,
        "content_type": _CONTENT_TYPE,
        "byte_length": len(result.fits_bytes),
        "created_by": f"ticket_ingestor:{job_run_id}",
        "created_at": now,
        "updated_at": now,
    }

    table.put_item(Item=fo_item)
