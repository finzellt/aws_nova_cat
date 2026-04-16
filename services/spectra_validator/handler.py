"""
spectra_validator — Lambda handler

Description: Operational status check, FITS profile-driven validation,
             duplicate fingerprint detection, validation result persistence.
Workflows:   acquire_and_validate_spectra
Tasks:       CheckOperationalStatus, ValidateBytes,
             RecordValidationResult, RecordDuplicateLinkage

Task responsibilities:

  CheckOperationalStatus:
    Loads the DataProduct from DynamoDB and returns the fields required by the
    ASL Choice states (AlreadyValidated?, CooldownActive?, IsQuarantined?) plus
    the full product record for downstream tasks. Collapses the spec's
    LoadDataProductMetadata + CheckOperationalStatus into a single Lambda call
    for efficiency — retry policies on the task cover both concerns.

  ValidateBytes:
    Reads raw FITS bytes from S3. Selects a profile via the profile registry.
    Delegates all HDU parsing, normalization, and sanity checks to the profile.
    Performs sha256 duplicate detection against existing VALID products in the
    nova partition (query + in-memory filter; safe because N << 500 per nova).
    Returns a flat validation result dict for ASL Choice branching.

    Profile adapter contract (see profiles/base.py):
      - ProfileResult(success=True)  → VALID path
      - ProfileResult(success=False) → QUARANTINE path
      - Unexpected exception from profile (OSError, MemoryError, etc.) → RETRYABLE

  RecordValidationResult:
    Persists the final lifecycle state for a validated (or quarantined/terminal)
    product. Clears eligibility (eligibility=NONE) and removes GSI1PK/GSI1SK
    so the product is no longer visible in the EligibilityIndex.

    On the VALID path, also persists enrichment fields extracted from the
    validated spectrum (ADR-031 Decisions 2, 3, 5): instrument, telescope,
    observation_date_mjd, flux_unit. These are consumed by artifact generators
    (DESIGN-003 §7) and must not require FITS header reads at generation time.

  ValidateBytes also writes a web-ready CSV (ADR-031 Decision 4) to
    derived/spectra/<nova_id>/<data_product_id>/web_ready.csv in the private
    S3 bucket after successful validation. This is best-effort: a failed
    write does not affect the validation outcome. Skipped for duplicates.

  RecordDuplicateLinkage:
    Marks a byte-level duplicate product. Sets duplicate_of_data_product_id,
    acquisition_status=SKIPPED_DUPLICATE, clears eligibility and GSI1 attributes.

Environment variables (injected by CDK):
    NOVA_CAT_TABLE_NAME           — DynamoDB table name
    NOVA_CAT_PRIVATE_BUCKET       — private data S3 bucket name
    NOVA_CAT_PUBLIC_SITE_BUCKET   — public site S3 bucket name (unused here)
    NOVA_CAT_QUARANTINE_TOPIC_ARN — quarantine notifications SNS topic ARN (unused here)
    LOG_LEVEL                     — logging level (default INFO)
    POWERTOOLS_SERVICE_NAME       — AWS Lambda Powertools service name
"""

from __future__ import annotations

import contextlib
import io
import os
import pathlib
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import boto3
import numpy as np
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from nova_common.errors import RetryableError
from nova_common.logging import configure_logging, logger
from nova_common.spectral import der_snr
from nova_common.timing import log_duration
from nova_common.tracing import tracer
from nova_common.web_ready_csv import build_web_ready_csv, write_web_ready_csv_to_s3
from nova_common.work_item import DirtyType, write_work_item
from profiles import validate_spectrum


def _bootstrap_astropy(base: str = "/tmp") -> None:
    """Redirect astropy/astroquery cache dirs to /tmp for Lambda compatibility."""
    os.environ.setdefault("ASTROPY_CONFIGDIR", f"{base}/astropy/config")
    os.environ.setdefault("ASTROPY_CACHE_DIR", f"{base}/astropy/cache")
    os.environ.setdefault("XDG_CACHE_HOME", f"{base}/.cache")
    os.environ.setdefault("HOME", base)
    for p in (
        os.environ["ASTROPY_CONFIGDIR"],
        os.environ["ASTROPY_CACHE_DIR"],
        os.environ["XDG_CACHE_HOME"],
    ):
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)


# Must run before astropy import
_bootstrap_astropy()

import astropy.io.fits as fits  # noqa: E402

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_PRIVATE_BUCKET = os.environ["NOVA_CAT_PRIVATE_BUCKET"]
_SCHEMA_VERSION = "1"

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)
_s3 = boto3.client("s3")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    if not task_name:
        raise ValueError("Missing required field: task_name")
    handler_fn = _TASK_HANDLERS.get(task_name)
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}. Known tasks: {list(_TASK_HANDLERS)}")
    logger.info("Task started", extra={"task_name": task_name})
    with log_duration(f"task:{task_name}"):
        result = handler_fn(event, context)
    return result


# ---------------------------------------------------------------------------
# Task: CheckOperationalStatus
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_check_operational_status(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Load the DataProduct from DynamoDB and compute the operational decision flags
    used by the ASL Choice states (AlreadyValidated?, CooldownActive?, IsQuarantined?).

    Also returns the full product record so downstream tasks (AcquireArtifact,
    ValidateBytes) can access locators, hints, and cooldown metadata without
    additional DynamoDB reads.

    Decision flags:
      already_validated — validation_status == VALID; triggers SKIPPED_DUPLICATE path
      cooldown_active   — now < next_eligible_attempt_at; triggers SKIPPED_BACKOFF path
      is_quarantined    — validation_status == QUARANTINED and no manual clearance;
                          triggers SKIPPED_BACKOFF path (re-acquiring quarantined
                          items requires operator intervention via manual_review_status)

    Raises ValueError (terminal) if the product record does not exist.
    Raises RetryableError on DynamoDB transient failures.

    Returns:
      already_validated — bool
      cooldown_active   — bool
      is_quarantined    — bool
      data_product      — full DynamoDB item dict for downstream use
    """
    nova_id: str = event["nova_id"]
    provider: str = event["provider"]
    data_product_id: str = event["data_product_id"]

    sk = f"PRODUCT#SPECTRA#{provider}#{data_product_id}"

    try:
        resp = _table.get_item(Key={"PK": nova_id, "SK": sk})
    except ClientError as exc:
        raise RetryableError(
            f"DynamoDB get_item failed for DataProduct "
            f"nova_id={nova_id!r} data_product_id={data_product_id!r}: {exc}"
        ) from exc

    item = resp.get("Item")
    if not item:
        raise ValueError(
            f"DataProduct not found: nova_id={nova_id!r} "
            f"provider={provider!r} data_product_id={data_product_id!r}. "
            "Cannot acquire a product that has not been discovered."
        )

    validation_status: str = str(item.get("validation_status", "UNVALIDATED"))
    _mrs = item.get("manual_review_status")
    manual_review_status: str | None = str(_mrs) if _mrs is not None else None
    _nea = item.get("next_eligible_attempt_at")
    next_eligible_attempt_at: str | None = str(_nea) if _nea is not None else None

    already_validated = validation_status == "VALID"

    now = _now()
    cooldown_active = next_eligible_attempt_at is not None and next_eligible_attempt_at > now

    # A QUARANTINED product without explicit operator clearance is blocked.
    # Clearance signals: CLEARED_RETRY_APPROVED or CLEARED_TERMINAL.
    is_quarantined = validation_status == "QUARANTINED" and manual_review_status not in (
        "CLEARED_RETRY_APPROVED",
        "CLEARED_TERMINAL",
    )

    logger.info(
        "CheckOperationalStatus complete",
        extra={
            "data_product_id": data_product_id,
            "validation_status": validation_status,
            "already_validated": already_validated,
            "cooldown_active": cooldown_active,
            "is_quarantined": is_quarantined,
            "next_eligible_attempt_at": next_eligible_attempt_at,
        },
    )

    return {
        "already_validated": already_validated,
        "cooldown_active": cooldown_active,
        "is_quarantined": is_quarantined,
        "data_product": dict(item),  # full record for downstream tasks
    }


# ---------------------------------------------------------------------------
# Task: ValidateBytes
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_validate_bytes(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Read raw FITS bytes from S3, select a profile, run profile-driven validation,
    and check for byte-level duplicates (sha256 collision against VALID products).

    Profile selection:
      select_profile(provider, primary_header) → FitsProfile | None
      None → QUARANTINE with reason_code UNKNOWN_PROFILE

    Error classification:
      ProfileResult(success=False) → QUARANTINE (deterministic failure)
      Unexpected exception from profile → RETRYABLE (transient I/O)
        (astropy OSError, MemoryError, truncated stream, etc.)

    Duplicate detection:
      After successful profile validation, query all PRODUCT#SPECTRA# items for
      the nova and check if any VALID product shares the same sha256. Safe for
      MVP because N << 500 spectra per nova per provider. If a duplicate is found,
      is_duplicate=True is returned so the ASL DuplicateByFingerprint? Choice can
      route to RecordDuplicateLinkage.

    Event inputs:
      nova_id, provider, data_product_id — product identity
      data_product                        — from CheckOperationalStatus
      acquisition                         — from AcquireArtifact
        .raw_s3_bucket, .raw_s3_key       — S3 location of raw FITS bytes
        .sha256                           — expected content fingerprint

    Returns:
      validation_outcome        — "VALID" | "QUARANTINED" | "TERMINAL_INVALID"
      is_duplicate              — bool (True if sha256 matches existing VALID product)
      duplicate_of_data_product_id — str | None
      fits_profile_id           — profile selected (or None)
      profile_selection_inputs  — dict of inputs used for selection
      header_signature_hash     — str | None
      normalization_notes       — list[str]
      quarantine_reason         — str | None
      quarantine_reason_code    — str | None
      instrument                — str | None  (ADR-031 P-2; VALID path only)
      telescope                 — str | None  (ADR-031 P-2; VALID path only)
      observation_date_mjd      — float | None (ADR-031 P-3; VALID path only)
      flux_unit                 — str | None  (ADR-031 P-5; VALID path only)
    """
    nova_id: str = event["nova_id"]
    provider: str = event["provider"]
    data_product_id: str = event["data_product_id"]
    data_product: dict[str, Any] = event["data_product"]
    acquisition: dict[str, Any] = event["acquisition"]

    raw_s3_bucket: str = acquisition["raw_s3_bucket"]
    raw_s3_key: str = acquisition["raw_s3_key"]
    sha256: str = acquisition["sha256"]

    # --- Read raw bytes from S3 ---
    try:
        s3_obj = _s3.get_object(Bucket=raw_s3_bucket, Key=raw_s3_key)
        raw_bytes: bytes = s3_obj["Body"].read()
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in {"NoSuchKey", "NoSuchBucket"}:
            raise ValueError(
                f"Raw FITS object not found at s3://{raw_s3_bucket}/{raw_s3_key}. "
                "AcquireArtifact must have failed to write or the key is wrong."
            ) from exc
        raise RetryableError(f"S3 get_object transient failure reading raw FITS: {exc}") from exc

    # --- Open FITS ---
    # Unexpected exceptions from astropy (OSError, truncated stream, MemoryError)
    # are intentionally not caught here — they propagate to the Step Functions
    # retry handler as RetryableError via the outer try/except below.
    try:
        hdu_list = fits.open(io.BytesIO(raw_bytes), memmap=False)
        primary_header = dict(hdu_list[0].header)
    except Exception as exc:
        # Any exception opening the FITS file is treated as RetryableError.
        # If the file is genuinely corrupt, the profile will catch it deterministically
        # once the retry happens — but an unexpected failure here may be transient
        # (e.g. Lambda memory pressure, incomplete S3 read).
        raise RetryableError(
            f"Failed to open FITS for data_product_id={data_product_id!r}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    profile_selection_inputs = {
        "provider": provider,
        "instrume": primary_header.get("INSTRUME"),
        "telescop": primary_header.get("TELESCOP"),
        "origin": primary_header.get("ORIGIN"),
    }

    # --- Profile selection + validation ---
    # validate_spectrum selects internally and delegates to profile.validate().
    # ProfileResult(success=False) → QUARANTINE (deterministic failure)
    # Unexpected exception from profile internals → propagate → RETRYABLE
    try:
        with log_duration("fits_validation", data_product_id=data_product_id, provider=provider):
            result = validate_spectrum(
                hdu_list,
                provider=provider,
                data_product_id=data_product_id,
                hints=data_product.get("hints", {}),
            )
    except Exception as exc:
        raise RetryableError(
            f"Unexpected exception during profile validation "
            f"for data_product_id={data_product_id!r}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    finally:
        with contextlib.suppress(Exception):
            hdu_list.close()

    if not result.success:
        logger.warning(
            "Profile validation failed — quarantining",
            extra={
                "data_product_id": data_product_id,
                "profile_id": result.profile_id,
                "quarantine_reason_code": result.quarantine_reason_code,
                "quarantine_reason": result.quarantine_reason,
            },
        )
        logger.info(
            "Decision point: DuplicateByFingerprint",
            extra={
                "validation_outcome": "QUARANTINED",
                "is_duplicate": False,
                "data_product_id": data_product_id,
            },
        )
        return {
            "validation_outcome": "QUARANTINED",
            "is_duplicate": False,
            "duplicate_of_data_product_id": None,
            "fits_profile_id": result.profile_id,
            "profile_selection_inputs": profile_selection_inputs,
            "header_signature_hash": result.header_signature_hash,
            "normalization_notes": result.normalization_notes,
            "quarantine_reason": result.quarantine_reason,
            "quarantine_reason_code": result.quarantine_reason_code,
        }

    # --- Duplicate detection (byte-level, post-validation) ---
    # Only check for duplicates once we know the file is valid FITS.
    # Query all SPECTRA products for this nova and look for a VALID product
    # with the same sha256. N << 500 per provider in MVP: in-memory filter is safe.
    duplicate_id = _find_duplicate_by_sha256(
        nova_id=nova_id,
        sha256=sha256,
        current_data_product_id=data_product_id,
    )

    if duplicate_id:
        logger.info(
            "Byte-level duplicate detected",
            extra={
                "data_product_id": data_product_id,
                "duplicate_of_data_product_id": duplicate_id,
                "sha256_prefix": sha256[:16],
            },
        )

    # --- Extract enrichment fields from validated spectrum (ADR-031 P-2/P-3/P-5) ---
    # These flow through $.validation in the ASL state and are persisted on the
    # DataProduct item by RecordValidationResult, so artifact generators can read
    # them directly from DDB without parsing FITS headers at generation time.
    spectrum = result.spectrum
    instrument: str | None = spectrum.instrument if spectrum else None
    telescope: str | None = spectrum.telescope if spectrum else None
    observation_date_mjd: float | None = spectrum.observation_mjd if spectrum else None
    # Normalize empty string to None: ADR-031 contract is null-means-absent.
    raw_flux_unit: str | None = spectrum.flux_units if spectrum else None
    flux_unit: str | None = raw_flux_unit if raw_flux_unit else None

    # Wavelength range in nm (canonical unit)
    wavelength_min_nm: float | None = None
    wavelength_max_nm: float | None = None
    snr_median: float | None = None
    snr_provenance: str | None = None

    if spectrum:
        axis = spectrum.spectral_axis
        units = spectrum.spectral_units.lower()
        finite_wl = axis[np.isfinite(axis)]
        if len(finite_wl) > 0:
            wl_min = float(np.min(finite_wl))
            wl_max = float(np.max(finite_wl))
            # Convert to nm
            if units in ("angstrom", "aa", "å"):
                wl_min /= 10.0
                wl_max /= 10.0
            elif units in ("um", "micron"):
                wl_min *= 1000.0
                wl_max *= 1000.0
            # else: assume already nm
            wavelength_min_nm = round(wl_min, 4)
            wavelength_max_nm = round(wl_max, 4)

        # SNR: prefer profile-extracted source value, fall back to DER_SNR.
        if spectrum.snr is not None:
            snr_median = spectrum.snr
            snr_provenance = "source"
        else:
            flux = spectrum.flux_axis
            if len(flux) > 0:
                estimated = der_snr(flux.tolist())
                if estimated > 0.0:
                    snr_median = round(estimated, 4)
                    snr_provenance = "estimated_der_snr"

    # --- Write web-ready CSV (ADR-031 P-4) ---
    # Best-effort: a failed write logs a warning but does not fail the
    # validation.  The CSV is a derived artifact that can be regenerated
    # from the raw FITS via the backfill script.
    # Skipped for duplicates — the canonical product's CSV already exists.
    if not duplicate_id and spectrum:
        try:
            csv_content = build_web_ready_csv(
                wavelength=spectrum.spectral_axis,
                flux=spectrum.flux_axis,
                spectral_units=spectrum.spectral_units,
            )
            write_web_ready_csv_to_s3(
                csv_content=csv_content,
                nova_id=nova_id,
                data_product_id=data_product_id,
                s3=_s3,
                bucket=_PRIVATE_BUCKET,
            )
        except Exception:
            logger.warning(
                "Failed to write web-ready CSV — validation result unaffected",
                extra={
                    "data_product_id": data_product_id,
                    "nova_id": nova_id,
                    "primary_name": event.get("primary_name", "unknown"),
                },
                exc_info=True,
            )

    logger.info(
        "ValidateBytes complete",
        extra={
            "data_product_id": data_product_id,
            "profile_id": result.profile_id,
            "is_duplicate": duplicate_id is not None,
            "normalization_notes_count": len(result.normalization_notes),
            "instrument": instrument,
            "telescope": telescope,
            "observation_date_mjd": observation_date_mjd,
            "flux_unit": flux_unit,
        },
    )

    return {
        "validation_outcome": "VALID",
        "is_duplicate": duplicate_id is not None,
        "duplicate_of_data_product_id": duplicate_id,
        "fits_profile_id": result.profile_id,
        "profile_selection_inputs": profile_selection_inputs,
        "header_signature_hash": result.header_signature_hash,
        "normalization_notes": result.normalization_notes,
        "quarantine_reason": None,
        "quarantine_reason_code": None,
        "instrument": instrument,
        "telescope": telescope,
        "observation_date_mjd": observation_date_mjd,
        "flux_unit": flux_unit,
        "wavelength_min_nm": wavelength_min_nm,
        "wavelength_max_nm": wavelength_max_nm,
        "snr": snr_median,
        "snr_provenance": snr_provenance,
    }


# ---------------------------------------------------------------------------
# Task: RecordValidationResult
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_record_validation_result(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Persist the final lifecycle state for a validated, quarantined, or
    terminal-invalid DataProduct.

    In all cases:
      - eligibility is set to NONE
      - GSI1PK and GSI1SK are REMOVED (product drops off EligibilityIndex)
      - Acquisition metadata (sha256, byte_length, raw S3 pointer) is persisted
      - last_attempt_outcome is set

    VALID:
      validation_status=VALID, acquisition_status=ACQUIRED,
      fits_profile_id, header_signature_hash, normalization_notes persisted.
      ADR-031 enrichment fields (instrument, telescope, observation_date_mjd,
      flux_unit) are persisted when present in the validation payload.

    QUARANTINED:
      validation_status=QUARANTINED, acquisition_status=ACQUIRED,
      quarantine_reason_code persisted.
      All acquisition metadata is still stored for operator diagnosis.

    TERMINAL_INVALID:
      validation_status=TERMINAL_INVALID, acquisition_status=ACQUIRED.

    Event inputs:
      nova_id, provider, data_product_id
      acquisition  — {sha256, byte_length, etag, raw_s3_bucket, raw_s3_key}
      validation   — {validation_outcome, fits_profile_id, header_signature_hash,
                      normalization_notes, quarantine_reason_code, quarantine_reason,
                      profile_selection_inputs,
                      instrument, telescope, observation_date_mjd, flux_unit}

    Returns:
      persisted_outcome — the validation_outcome that was written
    """
    nova_id: str = event["nova_id"]
    provider: str = event["provider"]
    data_product_id: str = event["data_product_id"]
    acquisition: dict[str, Any] = event["acquisition"]
    validation: dict[str, Any] = event["validation"]

    validation_outcome: str = validation["validation_outcome"]

    # Map outcome → scientific status pair
    _STATUS_MAP = {
        "VALID": ("VALID", "ACQUIRED", "SUCCESS"),
        "QUARANTINED": ("QUARANTINED", "ACQUIRED", "QUARANTINE"),
        "TERMINAL_INVALID": ("TERMINAL_INVALID", "ACQUIRED", "TERMINAL_FAILURE"),
    }
    if validation_outcome not in _STATUS_MAP:
        raise ValueError(
            f"Unrecognised validation_outcome={validation_outcome!r} "
            f"for data_product_id={data_product_id!r}"
        )
    validation_status, acquisition_status, last_attempt_outcome = _STATUS_MAP[validation_outcome]

    now = _now()
    sk = f"PRODUCT#SPECTRA#{provider}#{data_product_id}"

    # Build UpdateExpression. SET and REMOVE are combined in a single call
    # so the transition from ACQUIRE to NONE is atomic.
    set_expr_parts = [
        "validation_status = :vs",
        "acquisition_status = :acq",
        "eligibility = :elig",
        "last_attempt_outcome = :outcome",
        "sha256 = :sha256",
        "byte_length = :byte_length",
        "etag = :etag",
        "raw_s3_bucket = :raw_s3_bucket",
        "raw_s3_key = :raw_s3_key",
        "profile_selection_inputs = :psi",
        "normalization_notes = :notes",
        "updated_at = :now",
    ]
    values: dict[str, Any] = {
        ":vs": validation_status,
        ":acq": acquisition_status,
        ":elig": "NONE",
        ":outcome": last_attempt_outcome,
        ":sha256": acquisition["sha256"],
        ":byte_length": acquisition["byte_length"],
        ":etag": acquisition.get("etag", ""),
        ":raw_s3_bucket": acquisition["raw_s3_bucket"],
        ":raw_s3_key": acquisition["raw_s3_key"],
        ":psi": validation.get("profile_selection_inputs", {}),
        ":notes": validation.get("normalization_notes", []),
        ":now": now,
    }

    # Optional fields — only SET if present
    if validation.get("fits_profile_id"):
        set_expr_parts.append("fits_profile_id = :profile_id")
        values[":profile_id"] = validation["fits_profile_id"]

    if validation.get("header_signature_hash"):
        set_expr_parts.append("header_signature_hash = :hsh")
        values[":hsh"] = validation["header_signature_hash"]

    if validation.get("quarantine_reason_code"):
        set_expr_parts.append("quarantine_reason_code = :qrc")
        values[":qrc"] = validation["quarantine_reason_code"]

    # --- ADR-031 enrichment fields (Decisions 2, 3, 5) ---
    # Written only on the VALID path — the QUARANTINED/TERMINAL_INVALID payloads
    # do not carry these fields, so the guards below are no-ops for those paths.
    if validation.get("instrument") is not None:
        set_expr_parts.append("instrument = :instrument")
        values[":instrument"] = validation["instrument"]

    if validation.get("telescope") is not None:
        set_expr_parts.append("telescope = :telescope")
        values[":telescope"] = validation["telescope"]

    if validation.get("observation_date_mjd") is not None:
        set_expr_parts.append("observation_date_mjd = :obs_mjd")
        values[":obs_mjd"] = Decimal(str(validation["observation_date_mjd"]))

    if validation.get("flux_unit") is not None:
        set_expr_parts.append("flux_unit = :flux_unit")
        values[":flux_unit"] = validation["flux_unit"]

    if validation.get("wavelength_min_nm") is not None:
        set_expr_parts.append("wavelength_min_nm = :wl_min")
        values[":wl_min"] = Decimal(str(validation["wavelength_min_nm"]))

    if validation.get("wavelength_max_nm") is not None:
        set_expr_parts.append("wavelength_max_nm = :wl_max")
        values[":wl_max"] = Decimal(str(validation["wavelength_max_nm"]))

    if validation.get("snr") is not None:
        set_expr_parts.append("snr = :snr")
        values[":snr"] = Decimal(str(validation["snr"]))

    if validation.get("snr_provenance") is not None:
        set_expr_parts.append("snr_provenance = :snr_prov")
        values[":snr_prov"] = validation["snr_provenance"]

    update_expression = "SET " + ", ".join(set_expr_parts) + " REMOVE GSI1PK, GSI1SK"

    try:
        _table.update_item(
            Key={"PK": nova_id, "SK": sk},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=values,
        )
    except ClientError as exc:
        raise RetryableError(
            f"DynamoDB update_item failed in RecordValidationResult "
            f"for data_product_id={data_product_id!r}: {exc}"
        ) from exc

    logger.info(
        "RecordValidationResult complete",
        extra={
            "data_product_id": data_product_id,
            "validation_status": validation_status,
            "acquisition_status": acquisition_status,
            "last_attempt_outcome": last_attempt_outcome,
        },
    )

    # --- ADR-031 Decision 7: WorkItem for the regeneration pipeline ---
    # Best-effort; only on the VALID path (QUARANTINED/TERMINAL don't
    # produce data that needs artifact regeneration).
    if validation_outcome == "VALID":
        write_work_item(
            _table,
            nova_id=nova_id,
            dirty_type=DirtyType.spectra,
            source_workflow="acquire_and_validate_spectra",
            job_run_id=event.get("job_run_id", event.get("correlation_id", "unknown")),
            correlation_id=event.get("correlation_id", "unknown"),
        )

    return {"persisted_outcome": validation_outcome}


# ---------------------------------------------------------------------------
# Task: RecordDuplicateLinkage
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_record_duplicate_linkage(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Mark the current DataProduct as a byte-level duplicate of an existing
    VALID product.

    Per the workflow spec and ADR-003:
      - The current product is marked with duplicate_of_data_product_id.
      - acquisition_status = SKIPPED_DUPLICATE.
      - validation_status is NOT set to VALID — the duplicate is not canonical.
      - eligibility = NONE; GSI1PK / GSI1SK are REMOVED.
      - last_attempt_outcome = SUCCESS (acquisition succeeded; the product just
        happens to be a known duplicate — this is an operational success).

    Note: This task does NOT add the duplicate's locator to the canonical product's
    locator list. That is a future enhancement (see workflow spec). For MVP,
    recording the linkage is sufficient.

    Event inputs:
      nova_id, provider, data_product_id
      acquisition  — {sha256, byte_length, etag, raw_s3_bucket, raw_s3_key}
      validation   — {duplicate_of_data_product_id}

    Returns:
      canonical_data_product_id — the stable UUID of the canonical (VALID) product
    """
    nova_id: str = event["nova_id"]
    provider: str = event["provider"]
    data_product_id: str = event["data_product_id"]
    acquisition: dict[str, Any] = event["acquisition"]
    validation: dict[str, Any] = event["validation"]

    canonical_id: str = validation["duplicate_of_data_product_id"]

    now = _now()
    sk = f"PRODUCT#SPECTRA#{provider}#{data_product_id}"

    try:
        _table.update_item(
            Key={"PK": nova_id, "SK": sk},
            UpdateExpression=(
                "SET acquisition_status = :acq, "
                "eligibility = :elig, "
                "duplicate_of_data_product_id = :dup_id, "
                "last_attempt_outcome = :outcome, "
                "sha256 = :sha256, "
                "byte_length = :byte_length, "
                "raw_s3_bucket = :raw_s3_bucket, "
                "raw_s3_key = :raw_s3_key, "
                "updated_at = :now "
                "REMOVE GSI1PK, GSI1SK"
            ),
            ExpressionAttributeValues={
                ":acq": "SKIPPED_DUPLICATE",
                ":elig": "NONE",
                ":dup_id": canonical_id,
                ":outcome": "SUCCESS",
                ":sha256": acquisition["sha256"],
                ":byte_length": acquisition["byte_length"],
                ":raw_s3_bucket": acquisition["raw_s3_bucket"],
                ":raw_s3_key": acquisition["raw_s3_key"],
                ":now": now,
            },
        )
    except ClientError as exc:
        raise RetryableError(
            f"DynamoDB update_item failed in RecordDuplicateLinkage "
            f"for data_product_id={data_product_id!r}: {exc}"
        ) from exc

    logger.info(
        "RecordDuplicateLinkage complete",
        extra={
            "data_product_id": data_product_id,
            "canonical_data_product_id": canonical_id,
        },
    )

    return {"canonical_data_product_id": canonical_id}


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------


def _find_duplicate_by_sha256(
    *,
    nova_id: str,
    sha256: str,
    current_data_product_id: str,
) -> str | None:
    """
    Query all PRODUCT#SPECTRA# items for the nova and return the data_product_id
    of any VALID product that shares the given sha256 (excluding the current product).

    This is a Query + in-memory filter. Safe for MVP because the maximum realistic
    spectra count per nova per provider is ~200, well within DynamoDB query limits
    and Lambda memory. See docs/storage/dynamodb-access-patterns.md for rationale.

    Raises RetryableError on DynamoDB transient failures.
    Returns None if no duplicate is found.
    """
    try:
        resp = _table.query(
            KeyConditionExpression=(
                Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
            ),
            ProjectionExpression="data_product_id, sha256, validation_status",
        )
    except ClientError as exc:
        raise RetryableError(
            f"DynamoDB query failed during sha256 duplicate check for nova_id={nova_id!r}: {exc}"
        ) from exc

    for item in resp.get("Items", []):
        if (
            item.get("sha256") == sha256
            and item.get("validation_status") == "VALID"
            and str(item.get("data_product_id", "")) != current_data_product_id
        ):
            return str(item["data_product_id"])
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "CheckOperationalStatus": _handle_check_operational_status,
    "ValidateBytes": _handle_validate_bytes,
    "RecordValidationResult": _handle_record_validation_result,
    "RecordDuplicateLinkage": _handle_record_duplicate_linkage,
}
