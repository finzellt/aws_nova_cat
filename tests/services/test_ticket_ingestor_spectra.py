"""Unit tests for the ticket-ingestor spectra branch.

Coverage
--------
spectra_reader.read_spectra:
  - Two-hop column index indirection (ticket → metadata CSV → spectrum CSV)
  - FITS header keyword population from ticket + metadata CSV fields
  - DATE-OBS date conversion (JD → ISO 8601)
  - BUNIT handling (per-spectrum units present → set; both NA → empty string)
  - Per-spectrum failure collection (missing data file does not abort batch)

spectra_reader determinism:
  - Stable data_product_id across calls with identical inputs
  - S3 key structure matches raw/<nova_id>/ticket_ingestion/<data_product_id>.fits

spectra_writer.write_spectrum:
  - DataProduct item written to DDB with correct SK, provider, and lifecycle fields
  - FileObject item written to DDB with correct role SK and provenance fields

Moto backs all S3 and DynamoDB interactions; no real AWS calls are made.

Sample file conventions
-----------------------
Synthetic fixtures are built in tmp_path for each test.  Column indices and
data values mirror the real GQ Mus ticket corpus (GQ_Mus_Williams_Optical_Spectra.txt
and GQ_Mus_Williams_Optical_Spectra_MetaData.csv) so that the two-hop mapping
is exercised against realistic data.  The real sample files should be committed
to tests/fixtures/spectra/gq_mus/ for optional end-to-end reference.
"""

from __future__ import annotations

import io
import uuid
import warnings
from collections.abc import Generator
from pathlib import Path
from typing import Any

import boto3
import pytest
from astropy.io import fits
from astropy.time import Time
from boto3.dynamodb.conditions import Key
from moto import mock_aws
from ticket_ingestor.spectra_reader import read_spectra
from ticket_ingestor.spectra_writer import write_spectrum

from contracts.models.tickets import SpectraTicket

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REGION = "us-east-1"
_BUCKET_NAME = "test-public-bucket"
_TABLE_NAME = "NovaCat-Test"
_NOVA_ID = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
_JOB_RUN_ID = "bbbbbbbb-0000-0000-0000-000000000002"

# Path to checked-in sample files (tests/fixtures/spectra/gq_mus/).
# Tests that reference this directory are skipped when the directory is absent
# so that CI passes before the files are committed.
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "spectra" / "gq_mus"

# JD value used across several test fixtures — mirrors the GQMUSA metadata row.
_JD_GQMUSA = 2.44732e6

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject dummy AWS credentials so boto3 never calls STS."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture
def aws_resources(aws_credentials: None) -> Generator[dict[str, Any], None, None]:
    """Provision a moto-backed S3 bucket and DynamoDB table for writer tests.

    Yields a dict with keys ``s3`` (boto3 S3 client), ``table`` (boto3
    DynamoDB Table resource), and ``bucket`` (bucket name string).  Both
    resources share the same mock_aws context so calls between them are
    consistent.
    """
    with mock_aws():
        s3 = boto3.client("s3", region_name=_REGION)
        s3.create_bucket(Bucket=_BUCKET_NAME)

        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = dynamodb.create_table(
            TableName=_TABLE_NAME,
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
        yield {"s3": s3, "table": table, "bucket": _BUCKET_NAME}


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------


def _make_ticket(
    *,
    flux_units: str | None = None,
    metadata_filename: str = "metadata.csv",
) -> SpectraTicket:
    """Return a SpectraTicket whose column indices mirror the GQ Mus ticket.

    Column mapping (from GQ_Mus_Williams_Optical_Spectra.txt):

      Ticket field        Metadata CSV col  Value in sample
      ─────────────────   ────────────────  ───────────────
      filename_col     0  col 0             spectrum filename
      wavelength_col   1  col 1             WAVELENGTH COL NUM (inner idx)
      flux_col         2  col 2             FLUX COL NUM (inner idx)
      flux_error_col   3  col 3             FLUX ERR COL NUM (NA → None)
      flux_units_col   4  col 4             FLUX UNITS string
      date_col         5  col 5             DATE (JD)
      observer_col     6  col 6             OBSERVER
      telescope_col    7  col 7             TELESCOPE
      instrument_col   8  col 8             INSTRUMENT
      dispersion_col   9  col 9             DISPERSION (Å/pixel)
      wavelength_range (10, 11)             WAV_MIN, WAV_MAX
    """
    return SpectraTicket(
        object_name="GQ Mus",
        wavelength_regime="optical",
        time_system="JD",
        assumed_outburst_date=None,
        reference="Williams et al. (1992)",
        bibcode="1992AJ....104..725W",
        ticket_status="completed",
        flux_units=flux_units,
        flux_error_units=None,
        dereddened=False,
        metadata_filename=metadata_filename,
        filename_col=0,
        wavelength_col=1,
        flux_col=2,
        flux_error_col=3,
        flux_units_col=4,
        date_col=5,
        observer_col=6,
        telescope_col=7,
        instrument_col=8,
        dispersion_col=9,
        wavelength_range_cols=(10, 11),
    )


_METADATA_HEADER = (
    "#FILENAME,WAVELENGTH COL NUM,FLUX COL NUM,FLUX ERR COL NUM,"
    "FLUX UNITS,DATE,OBSERVER,TELESCOPE,INSTRUMENT,DISPERSION,"
    "WAVELENGTH RANGE 1,WAVELENGTH RANGE 2"
)


def _write_metadata_csv(tmp_path: Path, *, rows: list[str]) -> Path:
    """Write a metadata CSV with the standard GQ Mus column header and given rows."""
    path = tmp_path / "metadata.csv"
    path.write_text("\n".join([_METADATA_HEADER] + rows) + "\n")
    return path


def _write_spectrum_csv(
    tmp_path: Path,
    *,
    filename: str = "spectrum_a.csv",
    data: list[tuple[float, float]] | None = None,
) -> Path:
    """Write a minimal headerless two-column (wavelength, flux) spectrum CSV."""
    if data is None:
        data = [(3100.0, 1.45e-15), (3103.0, 1.67e-15), (3106.0, 1.54e-15)]
    path = tmp_path / filename
    path.write_text("\n".join(f"{w},{f}" for w, f in data) + "\n")
    return path


def _standard_metadata_row(
    spectrum_filename: str = "spectrum_a.csv",
    *,
    flux_units: str = "ergs/cm^2/sec",
    jd: float = _JD_GQMUSA,
) -> str:
    """Return a single metadata CSV row matching the GQMUSA sample structure."""
    return (
        f"{spectrum_filename},0,1,NA,{flux_units},{jd},"
        "Williams,CTIO 1 m,2D-Frutti,3.0,3100.0,7450.0"
    )


# ---------------------------------------------------------------------------
# Two-hop indirection
# ---------------------------------------------------------------------------


def test_read_spectra_two_hop_indirection(tmp_path: Path) -> None:
    """Ticket column indices → metadata CSV values → spectrum CSV column indices.

    The ticket's ``wavelength_col=1`` points to metadata CSV column 1
    (``WAVELENGTH COL NUM``), whose value ``"0"`` is used as the wavelength
    column index in the spectrum data CSV.  Likewise ``flux_col=2`` → metadata
    col 2 (``"1"``) → spectrum CSV column 1.  This test confirms the full
    two-hop resolution is working end-to-end.
    """
    _write_spectrum_csv(tmp_path)
    _write_metadata_csv(tmp_path, rows=[_standard_metadata_row()])
    ticket = _make_ticket()

    result = read_spectra(
        metadata_csv_path=tmp_path / "metadata.csv",
        data_dir=tmp_path,
        ticket=ticket,
        nova_id=_NOVA_ID,
    )

    assert result.failures == [], f"Unexpected read failures: {result.failures}"
    assert len(result.results) == 1

    sr = result.results[0]
    assert sr.spectrum_filename == "spectrum_a.csv"

    # FITS must be loadable and carry the correct number of flux values,
    # confirming the two-hop column resolution produced real data arrays.
    with fits.open(io.BytesIO(sr.fits_bytes)) as hdul:
        assert hdul[0].data is not None
        assert len(hdul[0].data) == 3


# ---------------------------------------------------------------------------
# FITS header keywords
# ---------------------------------------------------------------------------


def test_fits_header_keywords_populated(tmp_path: Path) -> None:
    """FITS primary header carries all expected keywords with correct values."""
    _write_spectrum_csv(tmp_path)
    _write_metadata_csv(tmp_path, rows=[_standard_metadata_row()])
    ticket = _make_ticket()

    result = read_spectra(
        metadata_csv_path=tmp_path / "metadata.csv",
        data_dir=tmp_path,
        ticket=ticket,
        nova_id=_NOVA_ID,
    )

    assert len(result.results) == 1
    with fits.open(io.BytesIO(result.results[0].fits_bytes)) as hdul:
        hdr = hdul[0].header

    # Identity
    assert hdr["OBJECT"] == "GQ Mus"
    assert hdr["BIBCODE"] == "1992AJ....104..725W"
    assert hdr["DEREDDEN"] == False  # noqa: E712  (FITS LOGICAL; == is intentional)

    # DATE-OBS: JD 2.44732e+06 → ISO 8601 date, computed via astropy for exactness.
    expected_date_obs = str(Time(_JD_GQMUSA, format="jd").to_value("iso", subfmt="date"))
    assert hdr["DATE-OBS"] == expected_date_obs

    # Provenance from metadata CSV
    assert hdr["TELESCOP"] == "CTIO 1 m"
    assert hdr["INSTRUME"] == "2D-Frutti"

    # WCS axis (wavelength)
    assert float(hdr["CRVAL1"]) == pytest.approx(3100.0)
    assert float(hdr["CDELT1"]) == pytest.approx(3.0)
    assert hdr["CTYPE1"] == "WAVE"
    assert hdr["CUNIT1"] == "Angstrom"

    # Flux units and wavelength range
    assert hdr["BUNIT"] == "ergs/cm^2/sec"
    assert float(hdr["WAV_MIN"]) == pytest.approx(3100.0)
    assert float(hdr["WAV_MAX"]) == pytest.approx(7450.0)


def test_bunit_empty_string_when_unavailable(tmp_path: Path) -> None:
    """BUNIT is '' when both per-spectrum flux_units_col and ticket.flux_units are NA.

    The FITS specification allows BUNIT='' to signal "unspecified units".
    astropy must not emit any warnings when opening such a file, confirming
    the empty-string convention is valid and not silently degraded.
    """
    _write_spectrum_csv(tmp_path)
    _write_metadata_csv(
        tmp_path,
        # flux_units column (col 4) is explicitly "NA"
        rows=[_standard_metadata_row(flux_units="NA")],
    )
    # ticket-level flux_units is also None — no fallback available
    ticket = _make_ticket(flux_units=None)

    result = read_spectra(
        metadata_csv_path=tmp_path / "metadata.csv",
        data_dir=tmp_path,
        ticket=ticket,
        nova_id=_NOVA_ID,
    )

    assert len(result.results) == 1
    fits_bytes = result.results[0].fits_bytes

    # Turn all warnings into errors inside this block so that any astropy
    # complaint about BUNIT='' causes an immediate, explicit test failure.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with fits.open(io.BytesIO(fits_bytes)) as hdul:
            bunit = hdul[0].header["BUNIT"]

    assert bunit == ""


# ---------------------------------------------------------------------------
# Deterministic identity
# ---------------------------------------------------------------------------


def test_deterministic_data_product_id(tmp_path: Path) -> None:
    """Two calls with identical inputs produce the same data_product_id."""
    _write_spectrum_csv(tmp_path)
    _write_metadata_csv(tmp_path, rows=[_standard_metadata_row()])
    ticket = _make_ticket()

    result_a = read_spectra(tmp_path / "metadata.csv", tmp_path, ticket, _NOVA_ID)
    result_b = read_spectra(tmp_path / "metadata.csv", tmp_path, ticket, _NOVA_ID)

    assert result_a.results[0].data_product_id == result_b.results[0].data_product_id


def test_s3_key_structure(tmp_path: Path) -> None:
    """S3 key matches raw/<nova_id>/ticket_ingestion/<data_product_id>.fits."""
    _write_spectrum_csv(tmp_path)
    _write_metadata_csv(tmp_path, rows=[_standard_metadata_row()])
    ticket = _make_ticket()

    result = read_spectra(tmp_path / "metadata.csv", tmp_path, ticket, _NOVA_ID)

    sr = result.results[0]
    expected = f"raw/{_NOVA_ID}/ticket_ingestion/{sr.data_product_id}.fits"
    assert sr.s3_key == expected


# ---------------------------------------------------------------------------
# spectra_writer — DDB item creation
# ---------------------------------------------------------------------------


def test_write_spectrum_creates_ddb_items(tmp_path: Path, aws_resources: dict[str, Any]) -> None:
    """write_spectrum writes a DataProduct item and a FileObject item to DDB.

    DataProduct is fetched by its deterministic SK.  FileObject is queried by
    SK prefix because file_id is a fresh uuid4() on each write_spectrum call.
    """
    _write_spectrum_csv(tmp_path)
    _write_metadata_csv(tmp_path, rows=[_standard_metadata_row()])
    ticket = _make_ticket()

    read_result = read_spectra(tmp_path / "metadata.csv", tmp_path, ticket, _NOVA_ID)
    assert len(read_result.results) == 1
    sr = read_result.results[0]

    table = aws_resources["table"]
    s3 = aws_resources["s3"]
    bucket = aws_resources["bucket"]

    write_spectrum(
        result=sr,
        nova_id=_NOVA_ID,
        job_run_id=_JOB_RUN_ID,
        bucket=bucket,
        s3=s3,
        table=table,
    )

    nova_id_str = str(_NOVA_ID)
    dp_id_str = str(sr.data_product_id)

    # ── DataProduct ─────────────────────────────────────────────────────────
    dp_response = table.get_item(
        Key={
            "PK": nova_id_str,
            "SK": f"PRODUCT#SPECTRA#ticket_ingestion#{dp_id_str}",
        }
    )
    assert "Item" in dp_response, "DataProduct item not found in DDB"
    dp = dp_response["Item"]
    assert dp["product_type"] == "SPECTRA"
    assert dp["provider"] == "ticket_ingestion"
    assert dp["acquisition_status"] == "ACQUIRED"
    assert dp["validation_status"] == "VALID"
    assert dp["eligibility"] == "NONE"
    assert dp["raw_s3_bucket"] == bucket
    assert dp["raw_s3_key"] == sr.s3_key

    # ── FileObject ──────────────────────────────────────────────────────────
    fo_response = table.query(
        KeyConditionExpression=(
            Key("PK").eq(nova_id_str)
            & Key("SK").begins_with(f"FILE#SPECTRA_RAW_FITS#NOVA#{nova_id_str}#ID#")
        )
    )
    assert len(fo_response["Items"]) == 1, "FileObject item not found in DDB"
    fo = fo_response["Items"][0]
    assert fo["role"] == "SPECTRA_RAW_FITS"
    assert fo["data_product_id"] == dp_id_str
    assert fo["bucket"] == bucket
    assert fo["key"] == sr.s3_key
    assert fo["content_type"] == "application/fits"
    assert fo["created_by"] == f"ticket_ingestor:{_JOB_RUN_ID}"


# ---------------------------------------------------------------------------
# Per-spectrum failure collection
# ---------------------------------------------------------------------------


def test_per_spectrum_failure_collection(tmp_path: Path) -> None:
    """A missing data file for one spectrum does not abort the batch.

    The metadata CSV references two spectra.  The first data file exists;
    the second does not.  read_spectra must return one SpectrumResult and
    one SpectrumFailure, with the failure attributed to the missing file.
    """
    _write_spectrum_csv(tmp_path, filename="spectrum_a.csv")
    # spectrum_missing.csv is intentionally not written.

    _write_metadata_csv(
        tmp_path,
        rows=[
            _standard_metadata_row("spectrum_a.csv"),
            _standard_metadata_row("spectrum_missing.csv", jd=2.44804e6),
        ],
    )
    ticket = _make_ticket()

    result = read_spectra(
        metadata_csv_path=tmp_path / "metadata.csv",
        data_dir=tmp_path,
        ticket=ticket,
        nova_id=_NOVA_ID,
    )

    assert len(result.results) == 1
    assert result.results[0].spectrum_filename == "spectrum_a.csv"

    assert len(result.failures) == 1
    assert result.failures[0].spectrum_filename == "spectrum_missing.csv"
