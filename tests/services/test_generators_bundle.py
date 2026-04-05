"""Unit tests for generators/bundle.py.

Uses moto to mock DynamoDB and S3 — no real AWS calls are made.

Covers:
  generate_bundle_zip (integration):
  - Happy path: spectra + photometry → ZIP on S3 with all files
  - Empty photometry: no photometry FITS in the archive
  - Missing spectrum FITS in S3: skipped, bundle still valid
  - All spectra FITS missing: no spectra/ dir, spectra_skipped counted
  - No spectra and no photometry: metadata-only bundle
  - S3 upload key is bundles/<nova_id>/full.zip
  - Return dict has correct counts and file list

  _build_bibtex:
  - Full entry with all fields
  - Missing optional fields omitted (not empty strings)
  - Empty references → empty string

  _build_photometry_fits:
  - BINTABLE columns present and correct
  - Decimal values converted to float
  - Nullable fields mapped to NaN

  _build_metadata:
  - Bundle counts (not web artifact counts)
  - Coordinates formatted as sexagesimal

  _aggregate_photometry_sources:
  - Groups by (bibcode, orig_catalog)
  - Band name derived from last segment of band_id

  _spectrum_filename:
  - ADR-014 naming convention
  - Missing fields → 'unknown' sentinel
  - Epoch MJD to 4 decimal places

  _hyphenate:
  - Spaces replaced with hyphens
"""

from __future__ import annotations

import io
import json
import zipfile
from collections.abc import Generator
from decimal import Decimal
from typing import Any

import boto3  # type: ignore[import-untyped]
import pytest
from astropy.io import fits  # type: ignore[import-untyped]
from generators.bundle import (  # type: ignore[import-untyped]
    _aggregate_photometry_sources,
    _build_bibtex,
    _build_metadata,
    _build_photometry_fits,
    _build_readme,
    _hyphenate,
    _spectrum_filename,
    generate_bundle_zip,
)
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REGION = "us-east-1"
_PRIVATE_BUCKET = "nova-cat-private-test"
_PUBLIC_BUCKET = "nova-cat-public-test"
_NOVA_ID = "aaaaaaaa-1111-2222-3333-444444444444"
_TABLE_NAME = "NovaCat-Test"

_NOVA_ITEM: dict[str, Any] = {
    "nova_id": _NOVA_ID,
    "primary_name": "GK Per",
    "aliases": ["Nova Persei 1901", "HD 21629"],
    "ra_deg": Decimal("52.7992583"),
    "dec_deg": Decimal("43.9046667"),
    "discovery_date": "1901-02-21",
    "nova_type": None,
    "status": "ACTIVE",
}

_DP_A_ID = "dp-aaaa-1111"
_DP_B_ID = "dp-bbbb-2222"

_FAKE_FITS = b"SIMPLE  = T" + b"\x00" * 2869  # minimal FITS-shaped bytes


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set AWS credential env vars for moto."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture()
def aws(_aws_env: None) -> Generator[tuple[Any, Any], None, None]:
    """Yield (table, s3_client) with mocked DDB table and S3 buckets."""
    with mock_aws():
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
        s3 = boto3.client("s3", region_name=_REGION)
        s3.create_bucket(Bucket=_PRIVATE_BUCKET)
        s3.create_bucket(Bucket=_PUBLIC_BUCKET)
        yield table, s3


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_data_product(
    table: Any,
    nova_id: str,
    dp_id: str,
    *,
    provider: str = "CfA",
    telescope: str = "FLWO15m",
    instrument: str = "FAST",
    epoch_mjd: Decimal | None = Decimal("46134.4471"),
    bibcode: str | None = "1992AJ....104..725W",
) -> None:
    """Write a VALID SPECTRA DataProduct item to DDB."""
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": f"PRODUCT#SPECTRA#{dp_id}",
            "entity_type": "DataProduct",
            "data_product_id": dp_id,
            "validation_status": "VALID",
            "provider": provider,
            "telescope": telescope,
            "instrument": instrument,
            "observation_date_mjd": epoch_mjd,
            "bibcode": bibcode,
        }
    )


def _upload_fits(s3: Any, nova_id: str, dp_id: str, body: bytes = _FAKE_FITS) -> None:
    """Upload a fake FITS file to the expected raw spectra S3 path."""
    s3.put_object(
        Bucket=_PRIVATE_BUCKET,
        Key=f"raw/spectra/{nova_id}/{dp_id}/primary.fits",
        Body=body,
    )


def _make_phot_row(
    *,
    row_id: str = "row-001",
    time_mjd: Decimal = Decimal("46134.5"),
    band_id: str = "HCT_HFOSC_Bessell_V",
    regime: str = "optical",
    magnitude: Decimal | None = Decimal("12.3"),
    mag_err: Decimal | None = Decimal("0.05"),
    bibcode: str | None = "2002MNRAS.334..699Z",
    orig_catalog: str | None = "SAAO",
    is_upper_limit: bool = False,
) -> dict[str, Any]:
    """Build a photometry raw item matching the nova_context contract."""
    return {
        "row_id": row_id,
        "time_mjd": time_mjd,
        "band_id": band_id,
        "regime": regime,
        "magnitude": magnitude,
        "mag_err": mag_err,
        "flux_density": None,
        "flux_density_err": None,
        "flux_density_unit": None,
        "is_upper_limit": is_upper_limit,
        "telescope": "SAAO1m",
        "instrument": "SITe",
        "observer": "Doe, J.",
        "orig_catalog": orig_catalog,
        "bibcode": bibcode,
        "band_res_type": "registry_match",
        "band_res_conf": "high",
    }


def _make_reference(
    *,
    bibcode: str = "1992AJ....104..725W",
    title: str | None = "Optical Spectra of Nova GQ Muscae",
    authors: list[str] | None = None,
    year: int | None = 1992,
    doi: str | None = "10.1086/116269",
) -> dict[str, Any]:
    """Build a reference record matching the nova_context contract."""
    return {
        "bibcode": bibcode,
        "title": title,
        "authors": authors if authors is not None else ["Williams, R. E.", "Phillips, M. M."],
        "year": year,
        "doi": doi,
        "arxiv_id": None,
        "ads_url": f"https://ui.adsabs.harvard.edu/abs/{bibcode}",
    }


def _base_nova_context(
    *,
    photometry_raw: list[dict[str, Any]] | None = None,
    references_output: list[dict[str, Any]] | None = None,
    references_count: int = 0,
    spectra_count: int = 0,
    photometry_count: int = 0,
    has_sparkline: bool = False,
) -> dict[str, Any]:
    """Build a minimal nova_context dict."""
    return {
        "nova_item": dict(_NOVA_ITEM),
        "outburst_mjd": 15042.0,
        "outburst_mjd_is_estimated": False,
        "references_count": references_count,
        "references_output": references_output or [],
        "spectra_count": spectra_count,
        "photometry_count": photometry_count,
        "photometry_raw_items": photometry_raw or [],
        "has_sparkline": has_sparkline,
    }


# ---------------------------------------------------------------------------
# generate_bundle_zip — integration tests
# ---------------------------------------------------------------------------


class TestGenerateBundleZip:
    """Integration tests for the main entry point."""

    def test_happy_path_produces_all_files(self, aws: tuple[Any, Any]) -> None:
        table, s3 = aws
        _seed_data_product(table, _NOVA_ID, _DP_A_ID)
        _upload_fits(s3, _NOVA_ID, _DP_A_ID)

        phot_rows = [
            _make_phot_row(),
            _make_phot_row(row_id="row-002", time_mjd=Decimal("46135")),
        ]
        refs = [_make_reference()]
        ctx = _base_nova_context(
            photometry_raw=phot_rows,
            references_output=refs,
            references_count=1,
        )

        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )

        assert result["spectra_included"] == 1
        assert result["spectra_skipped"] == 0
        assert result["photometry_rows"] == 2
        assert result["references_count"] == 1
        assert result["s3_key"] == f"nova/{_NOVA_ID}/bundle.zip"

        # Verify ZIP was uploaded and contains expected files
        obj = s3.get_object(Bucket=_PUBLIC_BUCKET, Key=result["s3_key"])
        zip_bytes = obj["Body"].read()
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            assert "README.txt" in names
            assert "GK-Per_metadata.json" in names
            assert "GK-Per_sources.json" in names
            assert "GK-Per_references.bib" in names
            assert "GK-Per_photometry.fits" in names
            # Spectrum file under spectra/
            spectra_files = [n for n in names if n.startswith("spectra/")]
            assert len(spectra_files) == 1
            assert "GK-Per_spectrum_CfA_FLWO15m_FAST_46134.4471.fits" in spectra_files[0]

    def test_empty_photometry_omits_fits(self, aws: tuple[Any, Any]) -> None:
        table, s3 = aws
        _seed_data_product(table, _NOVA_ID, _DP_A_ID)
        _upload_fits(s3, _NOVA_ID, _DP_A_ID)
        ctx = _base_nova_context(photometry_raw=[])

        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )

        assert result["photometry_rows"] == 0
        obj = s3.get_object(Bucket=_PUBLIC_BUCKET, Key=result["s3_key"])
        with zipfile.ZipFile(io.BytesIO(obj["Body"].read())) as zf:
            assert "GK-Per_photometry.fits" not in zf.namelist()

    def test_missing_spectrum_fits_is_skipped(self, aws: tuple[Any, Any]) -> None:
        table, s3 = aws
        # DDB has the DataProduct but S3 has no FITS file
        _seed_data_product(table, _NOVA_ID, _DP_A_ID)
        ctx = _base_nova_context()

        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )

        assert result["spectra_included"] == 0
        assert result["spectra_skipped"] == 1

    def test_partial_spectra_failure(self, aws: tuple[Any, Any]) -> None:
        """One spectrum present, one missing — bundle includes the one that works."""
        table, s3 = aws
        _seed_data_product(table, _NOVA_ID, _DP_A_ID, epoch_mjd=Decimal("46134.4471"))
        _seed_data_product(table, _NOVA_ID, _DP_B_ID, epoch_mjd=Decimal("46135.6832"))
        _upload_fits(s3, _NOVA_ID, _DP_A_ID)
        # _DP_B_ID has no FITS in S3

        ctx = _base_nova_context()
        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )

        assert result["spectra_included"] == 1
        assert result["spectra_skipped"] == 1

    def test_no_spectra_no_photometry_still_valid(self, aws: tuple[Any, Any]) -> None:
        """Metadata-only bundle when no scientific data exists."""
        table, s3 = aws
        ctx = _base_nova_context()

        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )

        assert result["spectra_included"] == 0
        assert result["photometry_rows"] == 0
        obj = s3.get_object(Bucket=_PUBLIC_BUCKET, Key=result["s3_key"])
        with zipfile.ZipFile(io.BytesIO(obj["Body"].read())) as zf:
            names = zf.namelist()
            assert "README.txt" in names
            assert "GK-Per_metadata.json" in names
            assert "GK-Per_sources.json" in names
            assert "GK-Per_references.bib" in names

    def test_s3_upload_key(self, aws: tuple[Any, Any]) -> None:
        table, s3 = aws
        ctx = _base_nova_context()
        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )
        assert result["s3_key"] == f"nova/{_NOVA_ID}/bundle.zip"

    def test_bundle_filename_includes_date(self, aws: tuple[Any, Any]) -> None:
        table, s3 = aws
        ctx = _base_nova_context()
        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )
        assert result["bundle_filename"].startswith("GK-Per_bundle_")
        assert result["bundle_filename"].endswith(".zip")

    def test_only_valid_data_products_included(self, aws: tuple[Any, Any]) -> None:
        """INVALID DataProducts should not appear in the bundle."""
        table, s3 = aws
        _seed_data_product(table, _NOVA_ID, _DP_A_ID)
        _upload_fits(s3, _NOVA_ID, _DP_A_ID)
        # Write an INVALID DataProduct directly
        table.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#{_DP_B_ID}",
                "entity_type": "DataProduct",
                "data_product_id": _DP_B_ID,
                "validation_status": "INVALID",
                "provider": "BadData",
            }
        )
        _upload_fits(s3, _NOVA_ID, _DP_B_ID)

        ctx = _base_nova_context()
        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )
        assert result["spectra_included"] == 1

    def test_metadata_json_has_bundle_counts(self, aws: tuple[Any, Any]) -> None:
        """metadata.json should reflect bundle counts, not web artifact counts."""
        table, s3 = aws
        _seed_data_product(table, _NOVA_ID, _DP_A_ID)
        _upload_fits(s3, _NOVA_ID, _DP_A_ID)

        phot = [_make_phot_row()]
        ctx = _base_nova_context(
            photometry_raw=phot,
            spectra_count=99,
            photometry_count=99,
        )

        result = generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )

        obj = s3.get_object(Bucket=_PUBLIC_BUCKET, Key=result["s3_key"])
        with zipfile.ZipFile(io.BytesIO(obj["Body"].read())) as zf:
            meta = json.loads(zf.read("GK-Per_metadata.json"))
            # Bundle counts: 1 spectrum included, 1 photometry row
            assert meta["spectra_count"] == 1
            assert meta["photometry_count"] == 1

    def test_tmp_file_cleaned_up(self, aws: tuple[Any, Any]) -> None:
        """Temp ZIP file should not persist after generation."""
        table, s3 = aws
        ctx = _base_nova_context()
        import glob

        before = set(glob.glob("/tmp/*.zip"))
        generate_bundle_zip(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_PRIVATE_BUCKET,
            public_bucket=_PUBLIC_BUCKET,
            nova_context=ctx,
        )
        after = set(glob.glob("/tmp/*.zip"))
        assert after == before


# ---------------------------------------------------------------------------
# _build_bibtex
# ---------------------------------------------------------------------------


class TestBuildBibtex:
    def test_full_entry(self) -> None:
        ref = _make_reference()
        bib = _build_bibtex([ref])
        assert "@article{1992AJ....104..725W," in bib
        assert "author  = {Williams, R. E. and Phillips, M. M.}," in bib
        assert "title   = {Optical Spectra of Nova GQ Muscae}," in bib
        assert "year    = {1992}," in bib
        assert "bibcode = {1992AJ....104..725W}," in bib
        assert "doi     = {10.1086/116269}" in bib

    def test_missing_optional_fields_omitted(self) -> None:
        ref = _make_reference(title=None, doi=None, authors=[], year=None)
        bib = _build_bibtex([ref])
        assert "author" not in bib
        assert "title" not in bib
        assert "year" not in bib
        assert "doi" not in bib
        # bibcode is always present
        assert "bibcode = {1992AJ....104..725W}" in bib

    def test_empty_references_returns_empty(self) -> None:
        assert _build_bibtex([]) == ""

    def test_multiple_entries_separated_by_blank_line(self) -> None:
        refs = [
            _make_reference(bibcode="2001A"),
            _make_reference(bibcode="2002B"),
        ]
        bib = _build_bibtex(refs)
        assert "@article{2001A," in bib
        assert "@article{2002B," in bib
        assert "\n\n" in bib


# ---------------------------------------------------------------------------
# _build_photometry_fits
# ---------------------------------------------------------------------------


class TestBuildPhotometryFits:
    def test_bintable_has_expected_columns(self) -> None:
        rows = [_make_phot_row()]
        fits_bytes = _build_photometry_fits(rows)

        hdul = fits.open(io.BytesIO(fits_bytes))
        table_hdu = hdul[1]
        col_names = [c.name for c in table_hdu.columns]

        expected = [
            "OBS_ID",
            "TIME_MJD",
            "BAND_ID",
            "BAND_NAME",
            "REGIME",
            "MAGNITUDE",
            "MAG_ERR",
            "FLUX_DENSITY",
            "FLUX_DENSITY_ERR",
            "FLUX_DENSITY_UNIT",
            "IS_UPPER_LIMIT",
            "TELESCOPE",
            "INSTRUMENT",
            "OBSERVER",
            "ORIG_CATALOG",
            "BIBCODE",
            "BAND_RES_TYPE",
            "BAND_RES_CONF",
        ]
        assert col_names == expected
        hdul.close()

    def test_decimal_values_converted(self) -> None:
        rows = [_make_phot_row(time_mjd=Decimal("46134.5"), magnitude=Decimal("12.3"))]
        fits_bytes = _build_photometry_fits(rows)

        hdul = fits.open(io.BytesIO(fits_bytes))
        data = hdul[1].data
        assert abs(float(data["TIME_MJD"][0]) - 46134.5) < 1e-6
        assert abs(float(data["MAGNITUDE"][0]) - 12.3) < 1e-6
        hdul.close()

    def test_nullable_numeric_is_nan(self) -> None:
        rows = [_make_phot_row(magnitude=None, mag_err=None)]
        fits_bytes = _build_photometry_fits(rows)

        hdul = fits.open(io.BytesIO(fits_bytes))
        data = hdul[1].data
        import math

        assert math.isnan(float(data["MAGNITUDE"][0]))
        assert math.isnan(float(data["MAG_ERR"][0]))
        hdul.close()

    def test_band_name_derived_from_band_id(self) -> None:
        rows = [_make_phot_row(band_id="HCT_HFOSC_Bessell_V")]
        fits_bytes = _build_photometry_fits(rows)

        hdul = fits.open(io.BytesIO(fits_bytes))
        data = hdul[1].data
        assert data["BAND_NAME"][0].strip() == "V"
        assert data["BAND_ID"][0].strip() == "HCT_HFOSC_Bessell_V"
        hdul.close()

    def test_extname_is_photometry(self) -> None:
        rows = [_make_phot_row()]
        fits_bytes = _build_photometry_fits(rows)

        hdul = fits.open(io.BytesIO(fits_bytes))
        assert hdul[1].header["EXTNAME"] == "PHOTOMETRY"
        hdul.close()

    def test_multiple_rows(self) -> None:
        rows = [
            _make_phot_row(row_id="r1", time_mjd=Decimal("100.0")),
            _make_phot_row(row_id="r2", time_mjd=Decimal("200.0")),
            _make_phot_row(row_id="r3", time_mjd=Decimal("300.0")),
        ]
        fits_bytes = _build_photometry_fits(rows)

        hdul = fits.open(io.BytesIO(fits_bytes))
        assert len(hdul[1].data) == 3
        hdul.close()


# ---------------------------------------------------------------------------
# _build_metadata
# ---------------------------------------------------------------------------


class TestBuildMetadata:
    def test_fields_present(self) -> None:
        meta = _build_metadata(
            nova_item=dict(_NOVA_ITEM),
            hyphenated="GK-Per",
            now="2026-04-01T00:00:00Z",
            spectra_count=5,
            photometry_count=100,
            references_count=3,
        )
        assert meta["nova_id"] == _NOVA_ID
        assert meta["primary_name"] == "GK Per"
        assert meta["spectra_count"] == 5
        assert meta["photometry_count"] == 100
        assert meta["references_count"] == 3
        assert meta["generated_at"] == "2026-04-01T00:00:00Z"
        assert meta["discovery_date"] == "1901-02-21"
        assert meta["nova_type"] is None

    def test_coordinates_formatted_sexagesimal(self) -> None:
        meta = _build_metadata(
            nova_item=dict(_NOVA_ITEM),
            hyphenated="GK-Per",
            now="2026-04-01T00:00:00Z",
            spectra_count=0,
            photometry_count=0,
            references_count=0,
        )
        # RA should be in HH:MM:SS format, Dec in ±DD:MM:SS format
        assert ":" in meta["ra"]
        assert ":" in meta["dec"]


# ---------------------------------------------------------------------------
# _aggregate_photometry_sources
# ---------------------------------------------------------------------------


class TestAggregatePhotometrySources:
    def test_groups_by_source(self) -> None:
        rows = [
            _make_phot_row(
                row_id="r1",
                bibcode="2002A",
                orig_catalog="SAAO",
                band_id="Generic_V",
                regime="optical",
            ),
            _make_phot_row(
                row_id="r2",
                bibcode="2002A",
                orig_catalog="SAAO",
                band_id="Generic_B",
                regime="optical",
            ),
            _make_phot_row(
                row_id="r3",
                bibcode="2005B",
                orig_catalog="AAVSO",
                band_id="Generic_V",
                regime="optical",
            ),
        ]
        result = _aggregate_photometry_sources(rows)
        assert len(result) == 2

        saao = next(r for r in result if r["orig_catalog"] == "SAAO")
        assert saao["observation_count"] == 2
        assert sorted(saao["bands"]) == ["B", "V"]
        assert saao["regimes"] == ["optical"]

        aavso = next(r for r in result if r["orig_catalog"] == "AAVSO")
        assert aavso["observation_count"] == 1

    def test_band_name_from_complex_band_id(self) -> None:
        rows = [
            _make_phot_row(band_id="HCT_HFOSC_Bessell_V"),
            _make_phot_row(row_id="r2", band_id="Swift_UVOT_UVW1"),
        ]
        result = _aggregate_photometry_sources(rows)
        # Both rows have the same source key, so one entry
        assert len(result) == 1
        assert "V" in result[0]["bands"]
        assert "UVW1" in result[0]["bands"]

    def test_empty_rows(self) -> None:
        assert _aggregate_photometry_sources([]) == []


# ---------------------------------------------------------------------------
# _spectrum_filename
# ---------------------------------------------------------------------------


class TestSpectrumFilename:
    def test_adr014_convention(self) -> None:
        dp: dict[str, Any] = {
            "provider": "CfA",
            "telescope": "FLWO15m",
            "instrument": "FAST",
            "observation_date_mjd": Decimal("46134.4471"),
        }
        name = _spectrum_filename("GK-Per", dp)
        assert name == "GK-Per_spectrum_CfA_FLWO15m_FAST_46134.4471.fits"

    def test_missing_telescope_uses_unknown(self) -> None:
        dp: dict[str, Any] = {
            "provider": "CfA",
            "telescope": None,
            "instrument": "FAST",
            "observation_date_mjd": Decimal("46134.4471"),
        }
        name = _spectrum_filename("GK-Per", dp)
        assert "_unknown_FAST_" in name

    def test_missing_instrument_uses_unknown(self) -> None:
        dp: dict[str, Any] = {
            "provider": "CfA",
            "telescope": "FLWO15m",
            "instrument": None,
            "observation_date_mjd": Decimal("46134.4471"),
        }
        name = _spectrum_filename("GK-Per", dp)
        assert "_FLWO15m_unknown_" in name

    def test_missing_epoch_uses_unknown(self) -> None:
        dp: dict[str, Any] = {
            "provider": "CfA",
            "telescope": "FLWO15m",
            "instrument": "FAST",
            "observation_date_mjd": None,
        }
        name = _spectrum_filename("GK-Per", dp)
        assert name.endswith("_unknown.fits")

    def test_epoch_mjd_four_decimal_places(self) -> None:
        dp: dict[str, Any] = {
            "provider": "X",
            "telescope": "T",
            "instrument": "I",
            "observation_date_mjd": Decimal("46134.5"),
        }
        name = _spectrum_filename("N", dp)
        assert "46134.5000.fits" in name


# ---------------------------------------------------------------------------
# _hyphenate
# ---------------------------------------------------------------------------


class TestHyphenate:
    def test_spaces_to_hyphens(self) -> None:
        assert _hyphenate("GK Per") == "GK-Per"

    def test_no_spaces(self) -> None:
        assert _hyphenate("V1324Sco") == "V1324Sco"

    def test_multiple_spaces(self) -> None:
        assert _hyphenate("Nova Persei 1901") == "Nova-Persei-1901"


# ---------------------------------------------------------------------------
# _build_readme
# ---------------------------------------------------------------------------


class TestBuildReadme:
    def test_contains_nova_identity(self) -> None:
        readme = _build_readme(
            nova_item=dict(_NOVA_ITEM),
            hyphenated="GK-Per",
            now="2026-04-01T00:00:00Z",
            data_products=[],
            photometry_count=0,
            references_count=0,
        )
        assert "GK Per" in readme
        assert "Nova Persei 1901" in readme
        assert "1901-02-21" in readme

    def test_contains_citation_guidance(self) -> None:
        readme = _build_readme(
            nova_item=dict(_NOVA_ITEM),
            hyphenated="GK-Per",
            now="2026-04-01T00:00:00Z",
            data_products=[],
            photometry_count=0,
            references_count=0,
        )
        assert "Citation Guidance" in readme
        assert "GK-Per_references.bib" in readme

    def test_contains_nova_page_link(self) -> None:
        readme = _build_readme(
            nova_item=dict(_NOVA_ITEM),
            hyphenated="GK-Per",
            now="2026-04-01T00:00:00Z",
            data_products=[],
            photometry_count=0,
            references_count=0,
        )
        assert "https://aws-nova-cat.vercel.app/nova/GK-Per" in readme

    def test_photometry_fits_listed_when_present(self) -> None:
        readme = _build_readme(
            nova_item=dict(_NOVA_ITEM),
            hyphenated="GK-Per",
            now="2026-04-01T00:00:00Z",
            data_products=[],
            photometry_count=10,
            references_count=0,
        )
        assert "GK-Per_photometry.fits" in readme

    def test_photometry_fits_not_listed_when_zero(self) -> None:
        readme = _build_readme(
            nova_item=dict(_NOVA_ITEM),
            hyphenated="GK-Per",
            now="2026-04-01T00:00:00Z",
            data_products=[],
            photometry_count=0,
            references_count=0,
        )
        assert "GK-Per_photometry.fits" not in readme

    def test_is_plain_text(self) -> None:
        readme = _build_readme(
            nova_item=dict(_NOVA_ITEM),
            hyphenated="GK-Per",
            now="2026-04-01T00:00:00Z",
            data_products=[],
            photometry_count=0,
            references_count=0,
        )
        # No Markdown indicators
        assert "```" not in readme
        assert "**" not in readme
        assert "##" not in readme
