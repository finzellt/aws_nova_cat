# tests/integration/test_ingest_ticket_integration.py

"""
Integration tests for the ingest_ticket workflow.

Simulates the full Step Functions execution by calling each Lambda handler
directly in ASL order, sharing moto-backed DynamoDB tables and an S3 bucket.

Workflow execution order (per infra/workflows/ingest_ticket.asl.json):
    BeginJobRun → AcquireIdempotencyLock → ParseTicket → ResolveNova
      → [TicketTypeBranch]
          → IngestPhotometry → FinalizeJobRunSuccess_Photometry
          → IngestSpectra   → FinalizeJobRunSuccess_Spectra
      → (quarantine via ResolveNova) → QuarantineHandler
          → FinalizeJobRunQuarantined

Paths covered:
  1. Happy path — spectra ticket (synthetic GQ Mus–format files):
     ParseTicket → ResolveNova (NameMapping hit; _sfn stub-patched) →
     IngestSpectra → FinalizeJobRunSuccess_Spectra.
     Asserts: spectra_ingested > 0, DataProduct + FileObject items in main
     DDB table, FITS object present in S3.
     Skipped if tests/fixtures/spectra/gq_mus/ is absent; when the
     directory is present the real ticket file is used for ParseTicket while
     a minimal synthetic metadata + spectrum CSV provide the data layer.

  2. Happy path — photometry ticket (synthetic V4739 Sgr–format files):
     ParseTicket → ResolveNova (NameMapping hit) → IngestPhotometry →
     FinalizeJobRunSuccess_Photometry.
     Asserts: rows_produced > 0, PhotometryRow items present in the
     dedicated photometry DDB table.

  3. Quarantine path — unresolvable object name:
     ParseTicket → ResolveNova (no NameMapping entry; _sfn configured to
     return NOT_FOUND outcome) → QuarantineHandler →
     FinalizeJobRunQuarantined.
     Asserts: JobRun item status == "QUARANTINED".

nova_resolver_ticket's module-level _sfn client is patched in every test.
For the existing-nova cases the NameMapping preflight short-circuits before
any SFN call is made; the stub raises AssertionError on unexpected contact.
For the quarantine path _sfn is configured to return a SUCCEEDED execution
whose output encodes outcome="NOT_FOUND".
"""

from __future__ import annotations

import csv
import importlib
import json
import sys
import types
from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from boto3.dynamodb.conditions import Key
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REGION = "us-east-1"
_ACCOUNT = "123456789012"

_MAIN_TABLE = "NovaCat-IntegTest"
_PHOTOMETRY_TABLE = "NovaCat-Photometry-IntegTest"
_PUBLIC_BUCKET = "nova-cat-public-integ"
_DIAGNOSTICS_BUCKET = "nova-cat-diag-integ"
_QUARANTINE_TOPIC_ARN = f"arn:aws:sns:{_REGION}:{_ACCOUNT}:nova-cat-quarantine"
_INITIALIZE_NOVA_SM_ARN = (
    f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-initialize-nova"
)
_FAKE_EXEC_ARN = (
    f"arn:aws:states:{_REGION}:{_ACCOUNT}:execution:nova-cat-initialize-nova:integ-test-001"
)

_CORRELATION_ID = "integ-ingest-ticket-corr-001"

# GQ Mus — happy-path spectra case
_GQ_MUS_NOVA_ID = "22222222-0000-0000-0000-000000000001"
_GQ_MUS_PRIMARY_NAME = "GQ Mus"
_GQ_MUS_RA_DEG = 176.59
_GQ_MUS_DEC_DEG = -67.25
# _normalize("GQ_Mus") → "gq mus"  (underscore → space per I1 fix)
_GQ_MUS_NORMALIZED = "gq mus"

# V4739 Sgr — happy-path photometry case
_V4739_SGR_NOVA_ID = "11111111-0000-0000-0000-000000000001"
_V4739_SGR_PRIMARY_NAME = "V4739 Sgr"
_V4739_SGR_RA_DEG = 270.123
_V4739_SGR_DEC_DEG = -23.456
# _normalize("V4739_Sgr") → "v4739 sgr"  (underscore → space per I1 fix)
_V4739_SGR_NORMALIZED = "v4739 sgr"

# Real fixture directories — TestSpectraHappyPath is skipped if absent.
_GQ_MUS_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "spectra" / "gq_mus"
_GQ_MUS_REAL_TICKET = "GQ_Mus_Williams_Optical_Spectra.txt"
_GQ_MUS_METADATA_FILENAME = "GQ_Mus_Williams_Optical_Spectra_MetaData.csv"

# Synthetic spectrum filename placed in tmp_path for the spectra test.
_SYNTHETIC_SPECTRUM_CSV = "gq_mus_integ_spectrum.csv"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject all environment variables consumed by ingest_ticket handlers."""
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _MAIN_TABLE)
    monkeypatch.setenv("PHOTOMETRY_TABLE_NAME", _PHOTOMETRY_TABLE)
    monkeypatch.setenv("DIAGNOSTICS_BUCKET", _DIAGNOSTICS_BUCKET)
    monkeypatch.setenv("INITIALIZE_NOVA_STATE_MACHINE_ARN", _INITIALIZE_NOVA_SM_ARN)
    monkeypatch.setenv("NOVA_CAT_QUARANTINE_TOPIC_ARN", _QUARANTINE_TOPIC_ARN)
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", _DIAGNOSTICS_BUCKET)
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", _PUBLIC_BUCKET)
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-integ-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def aws_resources(aws_env: None) -> Generator[dict[str, Any], None, None]:
    """
    Provision all mocked AWS resources shared across the ingest_ticket workflow:
      - Main NovaCat DynamoDB table (PK/SK + EligibilityIndex GSI)
      - Dedicated photometry DynamoDB table (PK/SK only)
      - Public S3 bucket for FITS uploads
      - Private S3 bucket for diagnostics (row-failure JSON)
      - SNS topic for quarantine notifications (best-effort — handler swallows errors)

    The mock_aws() context remains active for the full duration of each test
    that receives this fixture (yield keeps the context manager open).  Test
    methods open a second with mock_aws(): block before calling _load_handlers()
    so that module-level boto3 clients are initialised inside the mock world.
    """
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)

        main_table = dynamodb.create_table(
            TableName=_MAIN_TABLE,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "EligibilityIndex",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )

        photometry_table = dynamodb.create_table(
            TableName=_PHOTOMETRY_TABLE,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        s3 = boto3.client("s3", region_name=_REGION)
        s3.create_bucket(Bucket=_PUBLIC_BUCKET)
        s3.create_bucket(Bucket=_DIAGNOSTICS_BUCKET)

        sns = boto3.client("sns", region_name=_REGION)
        sns.create_topic(Name="nova-cat-quarantine")

        yield {
            "main_table": main_table,
            "photometry_table": photometry_table,
            "s3": s3,
        }


# ---------------------------------------------------------------------------
# Handler loader
# ---------------------------------------------------------------------------


def _load_handlers() -> dict[str, types.ModuleType]:
    """
    Fresh import of every handler used by the ingest_ticket workflow.

    Module cache is cleared so that module-level boto3 clients (created at
    import time) are re-initialised inside the active moto mock context.
    """
    module_names = [
        "job_run_manager.handler",
        "idempotency_guard.handler",
        "quarantine_handler.handler",
        "ticket_parser.handler",
        "nova_resolver_ticket.handler",
        "ticket_ingestor.handler",
    ]
    for mod_name in module_names:
        sys.modules.pop(mod_name, None)
    return {
        "job_run_manager": importlib.import_module("job_run_manager.handler"),
        "idempotency_guard": importlib.import_module("idempotency_guard.handler"),
        "quarantine_handler": importlib.import_module("quarantine_handler.handler"),
        "ticket_parser": importlib.import_module("ticket_parser.handler"),
        "nova_resolver_ticket": importlib.import_module("nova_resolver_ticket.handler"),
        "ticket_ingestor": importlib.import_module("ticket_ingestor.handler"),
    }


# ---------------------------------------------------------------------------
# DDB seed helpers
# ---------------------------------------------------------------------------


def _seed_nova(
    table: Any,
    *,
    nova_id: str,
    primary_name: str,
    normalized_name: str,
    ra_deg: float,
    dec_deg: float,
) -> None:
    """
    Seed a Nova item and a corresponding NameMapping item in the main DDB table
    so that nova_resolver_ticket's NameMapping preflight query returns a hit.

    Coordinates are stored as Decimal — boto3 rejects plain float for DDB Number.
    """
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": "NOVA",
            "entity_type": "Nova",
            "schema_version": "1.0.0",
            "nova_id": nova_id,
            "primary_name": primary_name,
            "primary_name_normalized": normalized_name,
            "status": "ACTIVE",
            "ra_deg": Decimal(str(ra_deg)),
            "dec_deg": Decimal(str(dec_deg)),
        }
    )
    table.put_item(
        Item={
            "PK": f"NAME#{normalized_name}",
            "SK": f"NOVA#{nova_id}",
            "entity_type": "NameMapping",
            "nova_id": nova_id,
        }
    )


def _get_job_run(table: Any, state: dict[str, Any]) -> dict[str, Any]:
    """Fetch the JobRun item from DDB using the pk/sk stored in state."""
    job_run = state["job_run"]
    resp = table.get_item(Key={"PK": job_run["pk"], "SK": job_run["sk"]})
    item: dict[str, Any] | None = resp.get("Item")
    assert item is not None, f"JobRun item not found — pk={job_run['pk']!r} sk={job_run['sk']!r}"
    return item


# ---------------------------------------------------------------------------
# SFN mock factories
# ---------------------------------------------------------------------------


def _make_sfn_stub() -> MagicMock:
    """
    SFN mock that raises if unexpectedly called.

    Used for existing-nova tests where the NameMapping preflight should
    short-circuit before any SFN call.
    """
    mock_sfn = MagicMock()
    mock_sfn.start_sync_execution.side_effect = AssertionError(
        "_sfn.start_sync_execution was called unexpectedly — "
        "NameMapping preflight should have returned a hit."
    )
    return mock_sfn


def _make_sfn_not_found() -> MagicMock:
    """
    SFN mock that simulates initialize_nova returning outcome=NOT_FOUND.

    This exercises the quarantine path through nova_resolver_ticket._extract_nova_id.
    """
    mock_sfn = MagicMock()
    mock_sfn.start_sync_execution.return_value = {
        "executionArn": _FAKE_EXEC_ARN,
        "status": "SUCCEEDED",
        "output": json.dumps({"finalize": {"outcome": "NOT_FOUND"}}),
    }
    return mock_sfn


# ---------------------------------------------------------------------------
# Workflow runner helpers
# ---------------------------------------------------------------------------


def _run_prefix(
    h: dict[str, types.ModuleType],
    *,
    ticket_path: str,
    data_dir: str,
    correlation_id: str = _CORRELATION_ID,
) -> dict[str, Any]:
    """
    Execute the shared ingest_ticket preamble: BeginJobRun → AcquireIdempotencyLock.

    Mirrors the ASL states (ingest_ticket.asl.json §BeginJobRun and
    §AcquireIdempotencyLock).  Returns the accumulated state dict that later
    steps thread through.
    """
    job_run: dict[str, Any] = h["job_run_manager"].handle(
        {
            "task_name": "BeginJobRun",
            "workflow_name": "ingest_ticket",
            "ticket_path": ticket_path,
            "correlation_id": correlation_id,
        },
        None,
    )
    h["idempotency_guard"].handle(
        {
            "task_name": "AcquireIdempotencyLock",
            "workflow_name": "ingest_ticket",
            "ticket_path": ticket_path,
            "primary_id": ticket_path,
            "correlation_id": job_run["correlation_id"],
            "job_run_id": job_run["job_run_id"],
        },
        None,
    )
    return {
        "ticket_path": ticket_path,
        "data_dir": data_dir,
        "job_run": job_run,
    }


# ---------------------------------------------------------------------------
# Fixture-file builders
# ---------------------------------------------------------------------------

# GQ Mus spectra ticket text.
# Matches the real GQ_Mus_Williams_Optical_Spectra.txt (knowledge base §fixtures).
# METADATA FILENAME points to a file we create in tmp_path so the test is
# self-contained regardless of whether the real fixtures directory exists.
_GQ_MUS_TICKET_TEXT = """\
OBJECT NAME: GQ_Mus
FLUX UNITS: NA
FLUX ERROR UNITS: NA
WAVELENGTH REGIME: Optical
TIME SYSTEM: JD
ASSUMED DATE OF OUTBURST: NA
REFERENCE: Williams et al. (1992)
BIBCODE: 1992AJ....104..725W
DEREDDENED FLAG: False
METADATA FILENAME: {metadata_filename}
FILENAME COLUMN: 0
WAVELENGTH COLUMN: 1
FLUX COLUMN: 2
FLUX ERROR COLUMN: 3
FLUX UNITS COLUMN: 4
DATE COLUMN: 5
TELESCOPE COLUMN: 7
INSTRUMENT COLUMN: 8
OBSERVER COLUMN: 6
SNR COLUMN: NA
DISPERSION COLUMN: 9
RESOLUTION COLUMN: NA
WAVELENGTH RANGE COLUMN: 10,11
TICKET STATUS: Completed
"""

# V4739 Sgr photometry ticket text.
# Mirrors the parsed ticket used across the photometry-reader unit tests.
_V4739_SGR_TICKET_TEXT = """\
OBJECT NAME: V4739_Sgr
WAVELENGTH REGIME: Optical
TIME SYSTEM: JD
TIME UNITS: days
FLUX UNITS: mags
FLUX ERROR UNITS: mags
FILTER SYSTEM: Johnson-Cousins
MAGNITUDE SYSTEM: Vega
TELESCOPE: Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector
OBSERVER: Gilmore, A. C. & Kilmartin, P. M.
REFERENCE: Livingston et al. (2001)
BIBCODE: 2001IBVS.5172....1L
ASSUMED DATE OF OUTBURST: NA
DATA FILENAME: V4739_Sgr_Livingston_optical_Photometry.csv
TIME COLUMN NUMBER: 0
FLUX COLUMN NUMBER: 1
FLUX ERROR COLUMN NUMBER: 2
FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER: 3
UPPER LIMIT FLAG COLUMN NUMBER: 4
TELESCOPE COLUMN NUMBER: 5
OBSERVER COLUMN NUMBER: 6
FILTER SYSTEM COLUMN NUMBER: 7
TICKET STATUS: Completed
"""

# Quarantine ticket — object name will not be in NameMapping and SFN returns NOT_FOUND.
_QUARANTINE_TICKET_TEXT = """\
OBJECT NAME: BOGUS_NOVA_XXXX_999
WAVELENGTH REGIME: Optical
TIME SYSTEM: JD
TIME UNITS: days
FLUX UNITS: mags
FLUX ERROR UNITS: mags
FILTER SYSTEM: Johnson-Cousins
MAGNITUDE SYSTEM: Vega
TELESCOPE: Synthetic Telescope
OBSERVER: Test Observer
REFERENCE: Integ Test (2024)
BIBCODE: 2024INTEG.001....1T
ASSUMED DATE OF OUTBURST: NA
DATA FILENAME: bogus_photometry.csv
TIME COLUMN NUMBER: 0
FLUX COLUMN NUMBER: 1
FLUX ERROR COLUMN NUMBER: 2
FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER: 3
UPPER LIMIT FLAG COLUMN NUMBER: 4
TELESCOPE COLUMN NUMBER: 5
OBSERVER COLUMN NUMBER: 6
FILTER SYSTEM COLUMN NUMBER: 7
TICKET STATUS: Completed
"""


def _write_gq_mus_data_dir(tmp_path: Path, *, use_real_ticket: bool) -> tuple[Path, Path]:
    """
    Populate tmp_path with the files required for the GQ Mus spectra test.

    Returns (ticket_path, data_dir):
      ticket_path — the .txt ticket file fed to ParseTicket
      data_dir    — directory passed as data_dir to IngestSpectra; contains:
                    - GQ_Mus_Williams_Optical_Spectra_MetaData.csv (one row)
                    - gq_mus_integ_spectrum.csv (synthetic wavelength/flux data)

    When use_real_ticket=True, the ticket is copied from _GQ_MUS_FIXTURES_DIR
    so that ParseTicket exercises the real file.  The METADATA FILENAME field
    in the real ticket references _GQ_MUS_METADATA_FILENAME, so the metadata
    CSV written here uses that same name.

    When use_real_ticket=False, a synthetic ticket is written to tmp_path with
    METADATA FILENAME pointing to the same metadata file.
    """
    metadata_filename = _GQ_MUS_METADATA_FILENAME

    # --- Ticket file -------------------------------------------------------
    if use_real_ticket:
        real_ticket_path = _GQ_MUS_FIXTURES_DIR / _GQ_MUS_REAL_TICKET
        ticket_text = real_ticket_path.read_text(encoding="utf-8")
        ticket_path = tmp_path / _GQ_MUS_REAL_TICKET
        ticket_path.write_text(ticket_text, encoding="utf-8")
    else:
        ticket_text = _GQ_MUS_TICKET_TEXT.format(metadata_filename=metadata_filename)
        ticket_path = tmp_path / "GQ_Mus_Williams_Optical_Spectra.txt"
        ticket_path.write_text(ticket_text, encoding="utf-8")

    # --- Minimal synthetic metadata CSV (one spectrum row) -----------------
    # Column layout matches the ticket's *_COLUMN fields:
    #   0=FILENAME  1=WAVE_COL  2=FLUX_COL  3=FLUX_ERR_COL  4=FLUX_UNITS
    #   5=DATE      6=OBSERVER  7=TELESCOPE  8=INSTRUMENT  9=DISPERSION
    #   10=WAVE_RANGE_1  11=WAVE_RANGE_2
    metadata_path = tmp_path / metadata_filename
    with metadata_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "#FILENAME",
                "WAVELENGTH COL NUM",
                "FLUX COL NUM",
                "FLUX ERR COL NUM",
                "FLUX UNITS",
                "DATE",
                "OBSERVER",
                "TELESCOPE",
                "INSTRUMENT",
                "DISPERSION",
                "WAVELENGTH RANGE 1",
                "WAVELENGTH RANGE 2",
            ]
        )
        writer.writerow(
            [
                _SYNTHETIC_SPECTRUM_CSV,
                0,
                1,
                "NA",
                "ergs/cm^2/sec",
                "2.44732e+06",
                "Williams",
                "CTIO 1 m",
                "2D-Frutti",
                "3.0",
                "3100.0",
                "7450.0",
            ]
        )

    # --- Minimal synthetic spectrum CSV (wavelength col 0, flux col 1) -----
    spectrum_path = tmp_path / _SYNTHETIC_SPECTRUM_CSV
    with spectrum_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        for wl, fx in [
            (3100.0, 1.52e-13),
            (3103.0, 1.61e-13),
            (3106.0, 1.74e-13),
            (3109.0, 1.68e-13),
            (3112.0, 1.55e-13),
        ]:
            writer.writerow([wl, fx])

    return ticket_path, tmp_path


def _write_v4739_sgr_data_dir(tmp_path: Path) -> tuple[Path, Path]:
    """
    Populate tmp_path with a synthetic V4739 Sgr ticket and photometry CSV.

    Returns (ticket_path, data_dir).  Filter column uses "V" which must be
    resolvable via the band_registry (alias → Generic_V).
    """
    ticket_path = tmp_path / "V4739_Sgr_Livingston_optical_Photometry.txt"
    ticket_path.write_text(_V4739_SGR_TICKET_TEXT, encoding="utf-8")

    # Columns: 0=JD, 1=mag, 2=err, 3=filter, 4=upper_limit, 5=telescope, 6=observer, 7=filter_sys
    csv_path = tmp_path / "V4739_Sgr_Livingston_optical_Photometry.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "2452148.839",
                "7.46",
                "0.009",
                "V",
                "0",
                "Mt John Observatory",
                "Gilmore",
                "Johnson-Cousins",
            ]
        )
        writer.writerow(
            [
                "2452148.853",
                "7.51",
                "0.009",
                "V",
                "0",
                "Mt John Observatory",
                "Gilmore",
                "Johnson-Cousins",
            ]
        )
        writer.writerow(
            [
                "2452148.869",
                "7.58",
                "0.009",
                "V",
                "0",
                "Mt John Observatory",
                "Gilmore",
                "Johnson-Cousins",
            ]
        )

    return ticket_path, tmp_path


def _write_quarantine_data_dir(tmp_path: Path) -> tuple[Path, Path]:
    """Write a minimal photometry ticket with an unresolvable object name."""
    ticket_path = tmp_path / "bogus_ticket.txt"
    ticket_path.write_text(_QUARANTINE_TICKET_TEXT, encoding="utf-8")
    return ticket_path, tmp_path


# ===========================================================================
# Path 1 — Happy path: spectra (GQ Mus)
# ===========================================================================


@pytest.mark.skipif(
    not _GQ_MUS_FIXTURES_DIR.is_dir(),
    reason=(
        "GQ Mus fixture files not present — commit "
        "tests/fixtures/spectra/gq_mus/ to enable this test"
    ),
)
class TestSpectraHappyPath:
    """
    Full spectra ingestion path using the real GQ Mus ticket file for ParseTicket
    and a minimal synthetic data layer (metadata CSV + one spectrum CSV) so the
    test is reliable regardless of which spectrum data files are in the fixture dir.
    """

    def test_spectra_ingested_end_to_end(
        self, tmp_path: Path, aws_resources: dict[str, Any]
    ) -> None:
        ticket_path, data_dir = _write_gq_mus_data_dir(tmp_path, use_real_ticket=True)

        with mock_aws():
            h = _load_handlers()

            _seed_nova(
                aws_resources["main_table"],
                nova_id=_GQ_MUS_NOVA_ID,
                primary_name=_GQ_MUS_PRIMARY_NAME,
                normalized_name=_GQ_MUS_NORMALIZED,
                ra_deg=_GQ_MUS_RA_DEG,
                dec_deg=_GQ_MUS_DEC_DEG,
            )

            # Patch _sfn — NameMapping preflight should hit; SFN must not be called.
            with patch.object(h["nova_resolver_ticket"], "_sfn", _make_sfn_stub()):
                state = _run_prefix(h, ticket_path=str(ticket_path), data_dir=str(data_dir))

                # --- ParseTicket -------------------------------------------
                parsed: dict[str, Any] = h["ticket_parser"].handle(
                    {
                        "task_name": "ParseTicket",
                        "workflow_name": "ingest_ticket",
                        "ticket_path": str(ticket_path),
                        "data_dir": str(data_dir),
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert parsed["ticket_type"] == "spectra"

                # --- ResolveNova -------------------------------------------
                nova: dict[str, Any] = h["nova_resolver_ticket"].handle(
                    {
                        "task_name": "ResolveNova",
                        "workflow_name": "ingest_ticket",
                        "object_name": parsed["object_name"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert nova["nova_id"] == _GQ_MUS_NOVA_ID

                # --- IngestSpectra -----------------------------------------
                ingest_result: dict[str, Any] = h["ticket_ingestor"].handle(
                    {
                        "task_name": "IngestSpectra",
                        "workflow_name": "ingest_ticket",
                        "ticket": parsed["ticket"],
                        "nova_id": nova["nova_id"],
                        "data_dir": str(data_dir),
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert ingest_result["spectra_ingested"] > 0, (
                    f"Expected at least one spectrum ingested; got {ingest_result}"
                )

                # --- FinalizeJobRunSuccess_Spectra -------------------------
                h["job_run_manager"].handle(
                    {
                        "task_name": "FinalizeJobRunSuccess",
                        "workflow_name": "ingest_ticket",
                        "outcome": "INGESTED_SPECTRA",
                        "nova_id": nova["nova_id"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                        "job_run": state["job_run"],
                    },
                    None,
                )

        # ── Assertions ─────────────────────────────────────────────────────
        # DataProduct item in main DDB table
        dp_resp = aws_resources["main_table"].query(
            KeyConditionExpression=(
                Key("PK").eq(_GQ_MUS_NOVA_ID)
                & Key("SK").begins_with("PRODUCT#SPECTRA#ticket_ingestion#")
            )
        )
        assert len(dp_resp["Items"]) > 0, "DataProduct item not found in main DDB table"
        dp = dp_resp["Items"][0]
        assert dp["product_type"] == "SPECTRA"
        assert dp["provider"] == "ticket_ingestion"

        # FileObject item in main DDB table
        fo_resp = aws_resources["main_table"].query(
            KeyConditionExpression=(
                Key("PK").eq(_GQ_MUS_NOVA_ID) & Key("SK").begins_with("FILE#SPECTRA_RAW_FITS#")
            )
        )
        assert len(fo_resp["Items"]) > 0, "FileObject item not found in main DDB table"

        # FITS object in S3
        s3_key: str = dp["raw_s3_key"]
        s3_resp = aws_resources["s3"].get_object(Bucket=_PUBLIC_BUCKET, Key=s3_key)
        assert len(s3_resp["Body"].read()) > 0, "FITS object in S3 is empty"

        # JobRun status
        job_run_item = _get_job_run(aws_resources["main_table"], state)
        assert job_run_item["status"] == "SUCCEEDED"
        # ADR-031 Decision 7: WorkItem written for the regeneration pipeline
        wq_resp = aws_resources["main_table"].query(
            KeyConditionExpression=(
                Key("PK").eq("WORKQUEUE") & Key("SK").begins_with(f"{_GQ_MUS_NOVA_ID}#spectra#")
            ),
        )
        assert len(wq_resp["Items"]) >= 1, (
            "No WorkItem found in WORKQUEUE for spectra after ticket ingestion"
        )

        assert job_run_item["outcome"] == "INGESTED_SPECTRA"


# ===========================================================================
# Path 2 — Happy path: photometry (V4739 Sgr)
# ===========================================================================


class TestPhotometryHappyPath:
    """
    Full photometry ingestion path using synthetic V4739 Sgr ticket + CSV data.

    The band registry must resolve "V" to Generic_V (alias lookup).
    This is satisfied by the committed band_registry.json.
    """

    def test_photometry_rows_written_end_to_end(
        self, tmp_path: Path, aws_resources: dict[str, Any]
    ) -> None:
        ticket_path, data_dir = _write_v4739_sgr_data_dir(tmp_path)

        with mock_aws():
            h = _load_handlers()

            _seed_nova(
                aws_resources["main_table"],
                nova_id=_V4739_SGR_NOVA_ID,
                primary_name=_V4739_SGR_PRIMARY_NAME,
                normalized_name=_V4739_SGR_NORMALIZED,
                ra_deg=_V4739_SGR_RA_DEG,
                dec_deg=_V4739_SGR_DEC_DEG,
            )

            with patch.object(h["nova_resolver_ticket"], "_sfn", _make_sfn_stub()):
                state = _run_prefix(h, ticket_path=str(ticket_path), data_dir=str(data_dir))

                # --- ParseTicket -------------------------------------------
                parsed = h["ticket_parser"].handle(
                    {
                        "task_name": "ParseTicket",
                        "workflow_name": "ingest_ticket",
                        "ticket_path": str(ticket_path),
                        "data_dir": str(data_dir),
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert parsed["ticket_type"] == "photometry"

                # --- ResolveNova -------------------------------------------
                nova = h["nova_resolver_ticket"].handle(
                    {
                        "task_name": "ResolveNova",
                        "workflow_name": "ingest_ticket",
                        "object_name": parsed["object_name"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert nova["nova_id"] == _V4739_SGR_NOVA_ID

                # --- IngestPhotometry --------------------------------------
                # Mirrors ASL IngestPhotometry Parameters — nova coordinates
                # come from $.nova (output of ResolveNova at ResultPath $.nova).
                ingest_result = h["ticket_ingestor"].handle(
                    {
                        "task_name": "IngestPhotometry",
                        "workflow_name": "ingest_ticket",
                        "ticket": parsed["ticket"],
                        "nova_id": nova["nova_id"],
                        "primary_name": nova["primary_name"],
                        "ra_deg": nova["ra_deg"],
                        "dec_deg": nova["dec_deg"],
                        "data_dir": str(data_dir),
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )
                assert ingest_result["rows_produced"] > 0, (
                    f"Expected at least one row produced; got {ingest_result}"
                )

                # --- FinalizeJobRunSuccess_Photometry ----------------------
                h["job_run_manager"].handle(
                    {
                        "task_name": "FinalizeJobRunSuccess",
                        "workflow_name": "ingest_ticket",
                        "outcome": "INGESTED_PHOTOMETRY",
                        "nova_id": nova["nova_id"],
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                        "job_run": state["job_run"],
                    },
                    None,
                )

        # ── Assertions ─────────────────────────────────────────────────────
        # PhotometryRow items in the dedicated photometry DDB table
        phot_resp = aws_resources["photometry_table"].query(
            KeyConditionExpression=(
                Key("PK").eq(_V4739_SGR_NOVA_ID) & Key("SK").begins_with("PHOT#")
            )
        )
        assert len(phot_resp["Items"]) > 0, (
            "No PhotometryRow items found in the photometry DDB table"
        )
        assert len(phot_resp["Items"]) == ingest_result["rows_produced"]

        # Spot-check the first row's required fields
        row = phot_resp["Items"][0]
        assert row["nova_id"] == _V4739_SGR_NOVA_ID
        assert row["band_id"] == "Generic_V"

        # ADR-031 Decision 7: WorkItem written for the regeneration pipeline
        wq_resp = aws_resources["main_table"].query(
            KeyConditionExpression=(
                Key("PK").eq("WORKQUEUE")
                & Key("SK").begins_with(f"{_V4739_SGR_NOVA_ID}#photometry#")
            ),
        )
        assert len(wq_resp["Items"]) >= 1, (
            "No WorkItem found in WORKQUEUE for photometry after ticket ingestion"
        )

        # JobRun status
        job_run_item = _get_job_run(aws_resources["main_table"], state)
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["outcome"] == "INGESTED_PHOTOMETRY"


# ===========================================================================
# Path 3 — Quarantine path: unresolvable object name
# ===========================================================================


class TestQuarantinePath:
    """
    Quarantine path exercised when initialize_nova returns NOT_FOUND for an
    object name that has no NameMapping entry.

    The ASL catches the exception raised by nova_resolver_ticket
    (ErrorEquals: ["UNRESOLVABLE_OBJECT_NAME"]) and routes to QuarantineHandler →
    FinalizeJobRunQuarantined.  In this direct-call test we replicate that
    routing by catching the exception, extracting the reason code from the
    exception type name, and calling QuarantineHandler and
    FinalizeJobRunQuarantined explicitly.
    """

    def test_unresolvable_object_name_quarantines_job_run(
        self, tmp_path: Path, aws_resources: dict[str, Any]
    ) -> None:
        ticket_path, data_dir = _write_quarantine_data_dir(tmp_path)

        with mock_aws():
            h = _load_handlers()

            # No Nova or NameMapping is seeded — the object name is unknown.
            # _sfn is configured to report NOT_FOUND so that _extract_nova_id
            # raises the appropriate QuarantineError subclass.
            mock_sfn = _make_sfn_not_found()
            with patch.object(h["nova_resolver_ticket"], "_sfn", mock_sfn):
                state = _run_prefix(h, ticket_path=str(ticket_path), data_dir=str(data_dir))

                # --- ParseTicket -------------------------------------------
                parsed = h["ticket_parser"].handle(
                    {
                        "task_name": "ParseTicket",
                        "workflow_name": "ingest_ticket",
                        "ticket_path": str(ticket_path),
                        "data_dir": str(data_dir),
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                    },
                    None,
                )

                # --- ResolveNova — expect quarantine exception -------------
                # No NameMapping entry → SFN fires → NOT_FOUND → raises.
                # We replicate Step Functions' Catch routing by catching the
                # exception and reading its type name as the reason code
                # ($.quarantine.Error in ASL terms).
                with pytest.raises(Exception) as exc_info:
                    h["nova_resolver_ticket"].handle(
                        {
                            "task_name": "ResolveNova",
                            "workflow_name": "ingest_ticket",
                            "object_name": parsed["object_name"],
                            "correlation_id": state["job_run"]["correlation_id"],
                            "job_run_id": state["job_run"]["job_run_id"],
                        },
                        None,
                    )

                # The exception class name IS the reason code (Step Functions
                # ErrorEquals matching uses __class__.__name__).
                exc_type_name = type(exc_info.value).__name__
                # Guard: if nova_common uses a dynamically-named subclass the
                # type name is the reason code directly; if it raises the base
                # QuarantineError class, fall back to the first arg.
                reason_code = (
                    exc_type_name
                    if exc_type_name != "QuarantineError"
                    else str(exc_info.value.args[0])
                )
                assert reason_code == "UNRESOLVABLE_OBJECT_NAME", (
                    f"Unexpected quarantine reason code: {reason_code!r}"
                )

                # --- QuarantineHandler (ASL: $.quarantine.Error → reason code) -
                quarantine_result: dict[str, Any] = h["quarantine_handler"].handle(
                    {
                        "task_name": "QuarantineHandler",
                        "workflow_name": "ingest_ticket",
                        "quarantine_reason_code": reason_code,
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                        "job_run": state["job_run"],
                    },
                    None,
                )
                assert quarantine_result["quarantine_reason_code"] == reason_code
                assert "error_fingerprint" in quarantine_result

                # --- FinalizeJobRunQuarantined -----------------------------
                h["job_run_manager"].handle(
                    {
                        "task_name": "FinalizeJobRunQuarantined",
                        "workflow_name": "ingest_ticket",
                        "correlation_id": state["job_run"]["correlation_id"],
                        "job_run_id": state["job_run"]["job_run_id"],
                        "job_run": state["job_run"],
                        "quarantine": quarantine_result,
                    },
                    None,
                )

        # ── Assertions ─────────────────────────────────────────────────────
        job_run_item = _get_job_run(aws_resources["main_table"], state)
        assert job_run_item["status"] == "QUARANTINED", (
            f"Expected QUARANTINED, got {job_run_item['status']!r}"
        )
        assert job_run_item["quarantine_reason_code"] == "UNRESOLVABLE_OBJECT_NAME"
        assert "error_fingerprint" in job_run_item

        # Confirm SFN was actually invoked (the quarantine was not a NameMapping hit)
        mock_sfn.start_sync_execution.assert_called_once()
