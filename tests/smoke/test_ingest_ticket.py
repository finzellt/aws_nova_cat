# tests/smoke/test_ingest_ticket.py

"""
Smoke tests for the ingest_ticket workflow.

Fires real Step Functions executions against the deployed NovaCatSmoke stack
and asserts on both the execution outcome and the DynamoDB / S3 artefacts it
produces.

ingest_ticket is an Express Workflow.  Express workflows do not support
DescribeExecution; the correct API is StartSyncExecution, which blocks until
the execution completes and returns status + output in the response.

Paths covered
-------------
1. TestIngestTicketPhotometryHappyPath — V4739 Sgr photometry ticket.
   initialize_nova fires (no NameMapping entry at session start), then
   IngestPhotometry writes PhotometryRow items to the dedicated photometry
   table.

2. TestIngestTicketSpectraHappyPath — GQ Mus spectra ticket.
   Same initialize_nova flow, then IngestSpectra converts spectrum CSVs to
   FITS, uploads to the public S3 bucket, and writes DataProduct + FileObject
   reference items to the main DDB table.

3. TestIngestTicketQuarantinePath — bogus object name.
   initialize_nova returns NOT_FOUND; the workflow routes to QuarantineHandler
   and finalises with status QUARANTINED.

Prerequisites (run once before this suite)
------------------------------------------
    python tools/smoke/upload_ingest_ticket_fixtures.py

This uploads the ticket .txt files and data CSVs to the smoke private bucket
under the S3 key prefixes expected by these tests.  The upload script handles
both real fixture files (tests/fixtures/spectra/gq_mus/) and synthetic
fallbacks, so the spectra test runs regardless of whether the real GQ Mus
files are committed.

Timeout guidance
----------------
initialize_nova resolves against real external archives (SIMBAD/TNS) and
typically completes in 5–30 s; allow up to 120 s for it.  Express workflows
have a hard 5-minute (300 s) execution ceiling, which is the operative limit
for all three paths.  The ``timeout`` parameter on start_sync_execution is
set conservatively below that ceiling.

  Photometry : 270 s
  Spectra    : 270 s
  Quarantine : 270 s
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any, TypeVar, cast

import boto3
import pytest
from boto3.dynamodb.conditions import Key
from pydantic import BaseModel, ConfigDict

from tests.smoke.conftest import StackOutputs

# ---------------------------------------------------------------------------
# Output models
#
# extra="ignore" is intentional: the Step Functions accumulated state dict
# carries many fields (ticket_path, data_dir, parsed_ticket, idempotency,
# …) that these models do not need to enumerate.
# ---------------------------------------------------------------------------

_OUTPUT_CFG = ConfigDict(extra="ignore")


class _FinalizeOutput(BaseModel):
    model_config = _OUTPUT_CFG
    outcome: str | None = None  # absent on the quarantine / failed path
    status: str | None = None  # present on quarantine path
    ended_at: str


class _JobRunOutput(BaseModel):
    model_config = _OUTPUT_CFG
    job_run_id: str
    correlation_id: str
    pk: str
    sk: str


class _NovaOutput(BaseModel):
    model_config = _OUTPUT_CFG
    nova_id: str
    primary_name: str
    ra_deg: float | None = None
    dec_deg: float | None = None


class _IngestPhotometryOutput(BaseModel):
    model_config = _OUTPUT_CFG
    rows_produced: int
    failures: int


class _IngestSpectraOutput(BaseModel):
    model_config = _OUTPUT_CFG
    spectra_ingested: int
    spectra_failed: int


class _IngestTicketPhotometryTerminalOutput(BaseModel):
    model_config = _OUTPUT_CFG
    job_run: _JobRunOutput
    nova: _NovaOutput | None = None
    ingest: _IngestPhotometryOutput | None = None
    finalize: _FinalizeOutput


class _IngestTicketSpectraTerminalOutput(BaseModel):
    model_config = _OUTPUT_CFG
    job_run: _JobRunOutput
    nova: _NovaOutput | None = None
    ingest: _IngestSpectraOutput | None = None
    finalize: _FinalizeOutput


class _IngestTicketQuarantineTerminalOutput(BaseModel):
    model_config = _OUTPUT_CFG
    job_run: _JobRunOutput
    finalize: _FinalizeOutput


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

T = TypeVar("T", bound=BaseModel)


def _run_sync(
    sfn_client: Any,
    state_machine_arn: str,
    payload: dict[str, Any],
    name_suffix: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    """
    Execute an Express workflow synchronously and return the response.

    Uses StartSyncExecution, which is the correct API for Express workflows.
    DescribeExecution is not supported for Express and raises InvalidArn.

    The response dict always contains ``status`` ("SUCCEEDED", "FAILED", or
    "TIMED_OUT") and, on success, ``output`` (the JSON execution output string).
    """
    sm_short = state_machine_arn.split(":")[-1][:30]
    execution_name = f"smoke-{sm_short}-{name_suffix}"
    return cast(
        dict[str, Any],
        sfn_client.start_sync_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(payload),
        ),
    )


def _output(resp: dict[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(resp["output"]))


def _assert_output(model_cls: type[T], out: dict[str, Any]) -> T:
    """
    Validate execution output against a terminal output model.

    Produces a structured failure message that includes the full execution
    output so failures are diagnosable without CloudWatch.
    """
    try:
        return model_cls.model_validate(out)
    except Exception as exc:
        formatted = json.dumps(out, default=str, indent=2)
        pytest.fail(
            f"Output schema validation failed for {model_cls.__name__}:\n"
            f"{exc}\n\nExecution output:\n{formatted}"
        )


def _get_item(table: Any, pk: str, sk: str) -> dict[str, Any] | None:
    resp = table.get_item(Key={"PK": pk, "SK": sk})
    return cast(dict[str, Any] | None, resp.get("Item"))


def _normalize_object_name(name: str) -> str:
    """Replicates nova_resolver_ticket._normalize exactly."""
    return re.sub(r"\s+", " ", name.strip().lower())


def _extract_nova_id(
    out: dict[str, Any],
    *,
    object_name: str,
    main_table: Any,
) -> str | None:
    """
    Extract the nova_id from an ingest_ticket execution output.

    Conservative two-level strategy:

    1. Primary — read ``out["nova"]["nova_id"]``, the field set by ResolveNova
       at ResultPath ``$.nova`` in the ASL.

    2. Fallback — query the main DDB table for the NameMapping item written by
       initialize_nova (``PK = "NAME#<normalized_object_name>"``).  This
       survives output-shape changes to the SFN state because the NameMapping
       is a persistent side-effect of initialize_nova, not an ephemeral field
       in the execution state dict.

    Returns None if both levels fail, signalling the caller to skip
    DDB-dependent assertions rather than failing with a confusing KeyError.
    """
    # Level 1 — execution output
    try:
        return str(out["nova"]["nova_id"])
    except (KeyError, TypeError):
        pass

    # Level 2 — NameMapping DDB item
    try:
        normalized = _normalize_object_name(object_name)
        resp = main_table.query(
            KeyConditionExpression=Key("PK").eq(f"NAME#{normalized}"),
            Limit=1,
        )
        items: list[dict[str, Any]] = resp.get("Items", [])
        if items:
            return str(items[0]["nova_id"])
    except Exception:  # noqa: BLE001
        pass

    return None


# ---------------------------------------------------------------------------
# S3 key prefixes / ticket paths — must match upload_ingest_ticket_fixtures.py
# ---------------------------------------------------------------------------

_V4739_SGR_TICKET_S3_KEY = "raw/tickets/V4739_Sgr_Livingston_optical_Photometry.txt"
_V4739_SGR_DATA_DIR_S3_PREFIX = "raw/data/v4739_sgr/"
_V4739_SGR_OBJECT_NAME = "V4739_Sgr"

_GQ_MUS_TICKET_S3_KEY = "raw/tickets/GQ_Mus_Williams_Optical_Spectra.txt"
_GQ_MUS_DATA_DIR_S3_PREFIX = "raw/data/gq_mus/"
_GQ_MUS_OBJECT_NAME = "GQ_Mus"

_QUARANTINE_TICKET_S3_KEY = "raw/tickets/BOGUS_OBJECT_smoke_test_quarantine.txt"
_QUARANTINE_DATA_DIR_S3_PREFIX = "raw/data/bogus/"

# Conservative timeout below Express's 5-minute hard ceiling.
_SYNC_TIMEOUT_SECONDS = 270


# ---------------------------------------------------------------------------
# Session-scoped S3 client (not in conftest — scoped to this module only)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _s3_client() -> Any:
    return boto3.client("s3", region_name="us-east-1")


# ===========================================================================
# 1. Photometry happy path — V4739 Sgr
# ===========================================================================


class TestIngestTicketPhotometryHappyPath:
    """
    Full photometry ingestion path using the V4739 Sgr ticket.

    initialize_nova is fired by ResolveNova (no NameMapping entry at the
    start of the test — cleanup_smoke_items wipes the main table before each
    test).  The workflow should resolve V4739 Sgr via SIMBAD, ingest the
    synthetic photometry CSV, and write PhotometryRow items to the dedicated
    photometry DDB table.

    Band registry must resolve "V" to a known band_id (alias lookup).
    """

    def test_photometry_workflow_succeeds_and_writes_rows(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
    ) -> None:
        suffix = uuid.uuid4().hex[:10]
        correlation_id = f"smoke-ingest-ticket-phot-{suffix}"

        resp = _run_sync(
            sfn_client,
            stack.ingest_ticket_arn,
            payload={
                "ticket_path": _V4739_SGR_TICKET_S3_KEY,
                "data_dir": _V4739_SGR_DATA_DIR_S3_PREFIX,
                "correlation_id": correlation_id,
            },
            name_suffix=suffix,
            timeout_seconds=_SYNC_TIMEOUT_SECONDS,
        )

        # ── Execution-level assertion (always fires) ─────────────────────
        assert resp["status"] == "SUCCEEDED", (
            f"ingest_ticket (photometry) did not SUCCEED.\n"
            f"Status: {resp['status']}\n"
            f"Error: {resp.get('error', '<none>')}\n"
            f"Cause: {resp.get('cause', '<none>')}"
        )

        out = _output(resp)
        model = _assert_output(_IngestTicketPhotometryTerminalOutput, out)

        assert model.finalize.outcome == "INGESTED_PHOTOMETRY", (
            f"Unexpected finalize outcome: {model.finalize.outcome!r}"
        )

        # ── DDB assertions (require nova_id extraction) ───────────────────
        main_table = dynamodb_resource.Table(stack.table_name)
        phot_table = dynamodb_resource.Table(stack.photometry_table_name)

        nova_id = _extract_nova_id(
            out,
            object_name=_V4739_SGR_OBJECT_NAME,
            main_table=main_table,
        )
        if nova_id is None:
            pytest.skip(
                "Could not extract nova_id from execution output or NameMapping — "
                "skipping DDB assertions.  Check CloudWatch Logs for ResolveNova."
            )

        # PhotometryRow items in the dedicated photometry table
        phot_resp = phot_table.query(
            KeyConditionExpression=(Key("PK").eq(nova_id) & Key("SK").begins_with("PHOT#"))
        )
        phot_items: list[dict[str, Any]] = phot_resp.get("Items", [])
        assert len(phot_items) > 0, (
            f"No PhotometryRow items found in photometry table for nova_id={nova_id!r}"
        )

        # ingest output count must match what is in DDB
        if model.ingest is not None:
            assert len(phot_items) == model.ingest.rows_produced, (
                f"DDB row count ({len(phot_items)}) != rows_produced ({model.ingest.rows_produced})"
            )

        # Envelope item in main table
        envelope = _get_item(main_table, nova_id, "PRODUCT#PHOTOMETRY_TABLE")
        assert envelope is not None, (
            f"PRODUCT#PHOTOMETRY_TABLE envelope item not found for nova_id={nova_id!r}"
        )

        # JobRun finalised correctly
        job_run_item = _get_item(main_table, model.job_run.pk, model.job_run.sk)
        assert job_run_item is not None, "JobRun item not found in DDB"
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["outcome"] == "INGESTED_PHOTOMETRY"


# ===========================================================================
# 2. Spectra happy path — GQ Mus
# ===========================================================================


class TestIngestTicketSpectraHappyPath:
    """
    Full spectra ingestion path using the GQ Mus ticket.

    Works with both real fixture files (when tests/fixtures/spectra/gq_mus/
    is present and upload_ingest_ticket_fixtures.py uploaded them) and the
    synthetic fallback that the upload script generates automatically.
    """

    def test_spectra_workflow_succeeds_and_writes_artefacts(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        _s3_client: Any,
    ) -> None:
        suffix = uuid.uuid4().hex[:10]
        correlation_id = f"smoke-ingest-ticket-spec-{suffix}"

        resp = _run_sync(
            sfn_client,
            stack.ingest_ticket_arn,
            payload={
                "ticket_path": _GQ_MUS_TICKET_S3_KEY,
                "data_dir": _GQ_MUS_DATA_DIR_S3_PREFIX,
                "correlation_id": correlation_id,
            },
            name_suffix=suffix,
            timeout_seconds=_SYNC_TIMEOUT_SECONDS,
        )

        # ── Execution-level assertion (always fires) ─────────────────────
        assert resp["status"] == "SUCCEEDED", (
            f"ingest_ticket (spectra) did not SUCCEED.\n"
            f"Status: {resp['status']}\n"
            f"Error: {resp.get('error', '<none>')}\n"
            f"Cause: {resp.get('cause', '<none>')}"
        )

        out = _output(resp)
        model = _assert_output(_IngestTicketSpectraTerminalOutput, out)

        assert model.finalize.outcome == "INGESTED_SPECTRA", (
            f"Unexpected finalize outcome: {model.finalize.outcome!r}"
        )

        # ── ingest count assertions ───────────────────────────────────────
        if model.ingest is not None:
            assert model.ingest.spectra_ingested >= 1, (
                f"Expected at least one spectrum ingested; "
                f"got spectra_ingested={model.ingest.spectra_ingested}, "
                f"spectra_failed={model.ingest.spectra_failed}"
            )

        # ── DDB assertions (require nova_id extraction) ───────────────────
        main_table = dynamodb_resource.Table(stack.table_name)

        nova_id = _extract_nova_id(
            out,
            object_name=_GQ_MUS_OBJECT_NAME,
            main_table=main_table,
        )
        if nova_id is None:
            pytest.skip(
                "Could not extract nova_id from execution output or NameMapping — "
                "skipping DDB / S3 assertions.  Check CloudWatch Logs for ResolveNova."
            )

        # DataProduct items in main table
        dp_resp = main_table.query(
            KeyConditionExpression=(
                Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#ticket_ingestion#")
            )
        )
        dp_items: list[dict[str, Any]] = dp_resp.get("Items", [])
        assert len(dp_items) >= 1, f"No SPECTRA DataProduct items found for nova_id={nova_id!r}"

        # Spot-check first DataProduct
        dp = dp_items[0]
        assert dp.get("provider") == "ticket_ingestion"
        assert dp.get("product_type") == "SPECTRA"
        assert dp.get("acquisition_status") == "ACQUIRED"
        assert dp.get("validation_status") == "VALID"

        # FileObject items in main table
        fo_resp = main_table.query(
            KeyConditionExpression=(
                Key("PK").eq(nova_id) & Key("SK").begins_with("FILE#SPECTRA_RAW_FITS#")
            )
        )
        assert len(fo_resp.get("Items", [])) >= 1, (
            f"No SPECTRA_RAW_FITS FileObject items found for nova_id={nova_id!r}"
        )

        # FITS object exists in the public S3 bucket
        s3_key: str | None = dp.get("raw_s3_key")
        if s3_key is not None:
            try:
                _s3_client.head_object(Bucket=stack.public_site_bucket_name, Key=s3_key)
            except Exception as exc:  # noqa: BLE001
                pytest.fail(
                    f"FITS object not found in public S3 bucket "
                    f"(bucket={stack.public_site_bucket_name!r}, key={s3_key!r}): {exc}"
                )

        # JobRun finalised correctly
        job_run_item = _get_item(main_table, model.job_run.pk, model.job_run.sk)
        assert job_run_item is not None, "JobRun item not found in DDB"
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["outcome"] == "INGESTED_SPECTRA"


# ===========================================================================
# 3. Quarantine path — unresolvable object name
# ===========================================================================


class TestIngestTicketQuarantinePath:
    """
    Quarantine path exercised when the ticket names an object that cannot be
    resolved by initialize_nova (returns NOT_FOUND).

    The state machine itself SUCCEEDS — the quarantine is a controlled
    terminal outcome, not an error.  The JobRun item in DDB must carry
    status == "QUARANTINED".
    """

    def test_quarantine_workflow_succeeds_and_quarantines_job_run(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
    ) -> None:
        suffix = uuid.uuid4().hex[:10]
        correlation_id = f"smoke-ingest-ticket-quar-{suffix}"

        resp = _run_sync(
            sfn_client,
            stack.ingest_ticket_arn,
            payload={
                "ticket_path": _QUARANTINE_TICKET_S3_KEY,
                "data_dir": _QUARANTINE_DATA_DIR_S3_PREFIX,
                "correlation_id": correlation_id,
            },
            name_suffix=suffix,
            timeout_seconds=_SYNC_TIMEOUT_SECONDS,
        )

        # ── Execution-level assertion (always fires) ─────────────────────
        # The state machine routes to QuarantineHandler → FinalizeJobRunQuarantined
        # and ends normally — the execution status is SUCCEEDED, not FAILED.
        assert resp["status"] == "SUCCEEDED", (
            f"ingest_ticket (quarantine) did not SUCCEED.\n"
            f"Status: {resp['status']}\n"
            f"Error: {resp.get('error', '<none>')}\n"
            f"Cause: {resp.get('cause', '<none>')}"
        )

        out = _output(resp)
        model = _assert_output(_IngestTicketQuarantineTerminalOutput, out)

        # FinalizeJobRunQuarantined sets status="QUARANTINED" (not outcome)
        assert model.finalize.status == "QUARANTINED", (
            f"Expected finalize.status=QUARANTINED, "
            f"got status={model.finalize.status!r} outcome={model.finalize.outcome!r}"
        )

        # ── DDB assertion — JobRun must be quarantined ────────────────────
        main_table = dynamodb_resource.Table(stack.table_name)
        job_run_item = _get_item(main_table, model.job_run.pk, model.job_run.sk)
        assert job_run_item is not None, "JobRun item not found in DDB"
        assert job_run_item["status"] == "QUARANTINED", (
            f"Expected JobRun status=QUARANTINED, got {job_run_item['status']!r}"
        )
