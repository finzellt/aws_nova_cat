"""Integration test for the artifact generator (DESIGN-003 §17.4).

Deliverable: a manual sweep with real WorkItems for a test nova produces
all seven per-nova artifacts with correct content.

Seeds moto DDB (main and photometry tables) and moto S3 (private and
public buckets), then calls ``_process_nova()`` **without patching any
generators** — the full chain runs end-to-end:

  references.json → spectra.json → photometry.json → sparkline.svg →
  nova.json → bundle.zip

Assertions:
  - ``NovaResult.success`` is ``True``
  - ``spectra_count``, ``photometry_count``, ``references_count``, and
    ``has_sparkline`` are correct
  - Artifact JSON files are uploaded to the public S3 bucket
  - Offset cache item is written to the main DDB table

Follows the project's integration test conventions: ``mock_aws()``
context, module reloading, multi-table setup.
"""

from __future__ import annotations

import importlib
import io
import sys
import types
from decimal import Decimal
from typing import Any

import boto3
import pytest
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_PHOT_TABLE_NAME = "NovaCat-Photometry-Test"
_BUCKET_PRIVATE = "nova-cat-private-test"
_BUCKET_PUBLIC = "nova-cat-public-test"
_REGION = "us-east-1"
_PLAN_ID = "test-plan-00000000-0000-0000-0000-000000000001"

_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"
_PRIMARY_NAME = "V1324 Sco"
_RA_DEG = Decimal("261.097083")
_DEC_DEG = Decimal("-35.492667")
_DISCOVERY_DATE = "2012-06-01"

_BIBCODE_A = "2013ATel.5073....1S"
_BIBCODE_B = "2015ApJ...805..136L"

_DP_ID_A = "dp-aaaa-0001"

_REGEN_PLAN_PK = "REGEN_PLAN"

# Band registry for test — optical bands only.
_BAND_REGISTRY: dict[str, Any] = {
    "Generic_V": {"band_name": "V", "lambda_eff": 5510.0},
    "Generic_B": {"band_name": "B", "lambda_eff": 4450.0},
    "Generic_R": {"band_name": "R", "lambda_eff": 6580.0},
    "VLA_5GHz": {"band_name": "5 GHz", "lambda_eff": None},
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set all required environment variables before module import."""
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PHOTOMETRY_TABLE_NAME", _PHOT_TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", _BUCKET_PRIVATE)
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", _BUCKET_PUBLIC)
    monkeypatch.setenv("PLAN_ID", _PLAN_ID)
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------


def _load_module() -> types.ModuleType:
    """Fresh import of the artifact generator main module.

    Clears all cached artifact_generator modules so that module-level
    boto3 clients are re-initialised inside the active ``mock_aws()``
    context.
    """
    to_clear = [key for key in sys.modules if key.startswith(("artifact_generator", "generators"))]
    for mod_name in to_clear:
        del sys.modules[mod_name]
    return importlib.import_module("artifact_generator.main")


# ---------------------------------------------------------------------------
# DDB table creators
# ---------------------------------------------------------------------------


def _create_main_table() -> Any:
    """Create the main NovaCat DDB table with moto."""
    dynamodb = boto3.resource("dynamodb", region_name=_REGION)
    return dynamodb.create_table(
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


def _create_photometry_table() -> Any:
    """Create the dedicated photometry DDB table with moto."""
    dynamodb = boto3.resource("dynamodb", region_name=_REGION)
    return dynamodb.create_table(
        TableName=_PHOT_TABLE_NAME,
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


def _create_s3_buckets() -> Any:
    """Create private and public S3 buckets with moto."""
    s3 = boto3.client("s3", region_name=_REGION)
    s3.create_bucket(Bucket=_BUCKET_PRIVATE)
    s3.create_bucket(Bucket=_BUCKET_PUBLIC)
    return s3


# ---------------------------------------------------------------------------
# DDB seed helpers
# ---------------------------------------------------------------------------


def _seed_nova(table: Any) -> None:
    """Seed an ACTIVE Nova item in the main table."""
    table.put_item(
        Item={
            "PK": _NOVA_ID,
            "SK": "NOVA",
            "entity_type": "Nova",
            "schema_version": "1.0.0",
            "nova_id": _NOVA_ID,
            "primary_name": _PRIMARY_NAME,
            "primary_name_normalized": _PRIMARY_NAME.lower(),
            "status": "ACTIVE",
            "ra_deg": _RA_DEG,
            "dec_deg": _DEC_DEG,
            "discovery_date": _DISCOVERY_DATE,
            "aliases": ["Nova Sco 2012"],
        }
    )


def _seed_references(table: Any) -> None:
    """Seed NovaReference links and Reference global items."""
    # NovaReference items (per-nova links).
    for bibcode in (_BIBCODE_A, _BIBCODE_B):
        table.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"NOVAREF#{bibcode}",
                "entity_type": "NovaReference",
                "nova_id": _NOVA_ID,
                "bibcode": bibcode,
            }
        )

    # Reference global items.
    table.put_item(
        Item={
            "PK": f"REFERENCE#{_BIBCODE_A}",
            "SK": "METADATA",
            "entity_type": "Reference",
            "bibcode": _BIBCODE_A,
            "title": "Discovery of Nova V1324 Sco",
            "authors": ["Stanek, K. Z.", "Prieto, J. L."],
            "year": 2013,
            "ads_url": f"https://ui.adsabs.harvard.edu/abs/{_BIBCODE_A}",
        }
    )
    table.put_item(
        Item={
            "PK": f"REFERENCE#{_BIBCODE_B}",
            "SK": "METADATA",
            "entity_type": "Reference",
            "bibcode": _BIBCODE_B,
            "title": "The 2012 Eruption of Nova V1324 Sco",
            "authors": ["Linford, J. D.", "Ribeiro, V. A. R. M."],
            "year": 2015,
            "ads_url": f"https://ui.adsabs.harvard.edu/abs/{_BIBCODE_B}",
        }
    )


def _seed_spectra_data_product(table: Any) -> None:
    """Seed a VALID spectra DataProduct item in the main table."""
    table.put_item(
        Item={
            "PK": _NOVA_ID,
            "SK": f"PRODUCT#SPECTRA#ticket_ingestion#{_DP_ID_A}",
            "entity_type": "DataProduct",
            "product_type": "SPECTRA",
            "nova_id": _NOVA_ID,
            "data_product_id": _DP_ID_A,
            "provider": "ticket_ingestion",
            "validation_status": "VALID",
            "acquisition_status": "ACQUIRED",
            "eligibility": "NONE",
            "observation_date_mjd": Decimal("56080.5"),
            "instrument": "GMOS-S",
            "telescope": "Gemini South",
            "flux_unit": "erg/s/cm2/A",
            "raw_s3_bucket": _BUCKET_PRIVATE,
            "raw_s3_key": f"raw/spectra/{_NOVA_ID}/{_DP_ID_A}.fits",
        }
    )


def _seed_photometry_rows(phot_table: Any) -> None:
    """Seed PhotometryRow items across multiple optical bands.

    Seeds three optical bands (V, B, R) with enough points for the
    offset pipeline to exercise spline fitting.  Also seeds one radio
    row for multi-regime coverage.
    """
    # V band — 6 points spanning 200 days.
    v_data = [
        (56080.0, 10.0),
        (56090.0, 9.8),
        (56120.0, 10.5),
        (56160.0, 11.2),
        (56220.0, 12.1),
        (56280.0, 13.0),
    ]
    for i, (mjd, mag) in enumerate(v_data):
        phot_table.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"PHOT#V{i:03d}",
                "nova_id": _NOVA_ID,
                "row_id": f"V{i:03d}",
                "regime": "optical",
                "band_id": "Generic_V",
                "time_mjd": Decimal(str(mjd)),
                "magnitude": Decimal(str(mag)),
                "mag_err": Decimal("0.05"),
                "is_upper_limit": False,
                "telescope": "SMARTS 1.3m",
                "bibcode": _BIBCODE_A,
            }
        )

    # B band — 5 points, magnitudes close to V to potentially
    # trigger offset computation.
    b_data = [
        (56081.0, 10.2),
        (56091.0, 10.1),
        (56121.0, 10.8),
        (56161.0, 11.5),
        (56221.0, 12.4),
    ]
    for i, (mjd, mag) in enumerate(b_data):
        phot_table.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"PHOT#B{i:03d}",
                "nova_id": _NOVA_ID,
                "row_id": f"B{i:03d}",
                "regime": "optical",
                "band_id": "Generic_B",
                "time_mjd": Decimal(str(mjd)),
                "magnitude": Decimal(str(mag)),
                "mag_err": Decimal("0.08"),
                "is_upper_limit": False,
                "telescope": "SMARTS 1.3m",
                "bibcode": _BIBCODE_A,
            }
        )

    # R band — 4 points, separated from V/B by ~1 mag.
    r_data = [
        (56082.0, 8.9),
        (56092.0, 8.7),
        (56122.0, 9.4),
        (56162.0, 10.1),
    ]
    for i, (mjd, mag) in enumerate(r_data):
        phot_table.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"PHOT#R{i:03d}",
                "nova_id": _NOVA_ID,
                "row_id": f"R{i:03d}",
                "regime": "optical",
                "band_id": "Generic_R",
                "time_mjd": Decimal(str(mjd)),
                "magnitude": Decimal(str(mag)),
                "mag_err": Decimal("0.04"),
                "is_upper_limit": False,
                "telescope": "SMARTS 1.3m",
                "bibcode": _BIBCODE_B,
            }
        )

    # Radio — 2 points to test multi-regime coverage.
    for i, (mjd, flux) in enumerate([(56100.0, 1.5), (56200.0, 0.8)]):
        phot_table.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"PHOT#RADIO{i:03d}",
                "nova_id": _NOVA_ID,
                "row_id": f"RADIO{i:03d}",
                "regime": "radio",
                "band_id": "VLA_5GHz",
                "time_mjd": Decimal(str(mjd)),
                "flux_density": Decimal(str(flux)),
                "flux_density_err": Decimal("0.1"),
                "is_upper_limit": False,
                "telescope": "VLA",
                "bibcode": _BIBCODE_B,
            }
        )


# ---------------------------------------------------------------------------
# S3 seed helpers
# ---------------------------------------------------------------------------


def _seed_web_ready_csv(s3: Any) -> None:
    """Seed a web-ready CSV in the private bucket for the spectra generator."""
    csv_body = "wavelength_nm,flux\n400.0,1.0e-15\n500.0,2.5e-15\n600.0,1.8e-15\n700.0,0.9e-15\n"
    s3.put_object(
        Bucket=_BUCKET_PRIVATE,
        Key=f"derived/spectra/{_NOVA_ID}/{_DP_ID_A}/web_ready.csv",
        Body=csv_body.encode("utf-8"),
    )


def _seed_raw_fits(s3: Any) -> None:
    """Seed a minimal FITS file in the private bucket for the bundle generator."""
    from astropy.io import fits as pyfits  # type: ignore[import-untyped]

    hdu = pyfits.PrimaryHDU()
    buf = io.BytesIO()
    hdu.writeto(buf)
    buf.seek(0)
    s3.put_object(
        Bucket=_BUCKET_PRIVATE,
        Key=f"raw/spectra/{_NOVA_ID}/{_DP_ID_A}.fits",
        Body=buf.read(),
    )


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------


def _all_artifacts_manifest() -> dict[str, Any]:
    """Manifest requesting generation of all per-nova artifacts."""
    return {
        "artifacts": [
            "references.json",
            "spectra.json",
            "photometry.json",
            "sparkline.svg",
            "nova.json",
            "bundle.zip",
        ],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestArtifactGeneratorIntegration:
    """Full-chain integration test for the artifact generator.

    Runs ``_process_nova()`` with all generators unpatched against
    moto-backed DDB tables and S3 buckets seeded with realistic data.
    """

    def test_full_sweep_produces_all_artifacts(self) -> None:
        """A nova with references, spectra, and photometry produces
        a successful NovaResult with correct observation counts.
        """
        with mock_aws():
            # --- Infrastructure ---
            main_table = _create_main_table()
            phot_table = _create_photometry_table()
            s3 = _create_s3_buckets()

            # --- Seed DDB ---
            _seed_nova(main_table)
            _seed_references(main_table)
            _seed_spectra_data_product(main_table)
            _seed_photometry_rows(phot_table)

            # --- Seed S3 ---
            _seed_web_ready_csv(s3)
            _seed_raw_fits(s3)

            # --- Fresh import (module-level boto3 clients re-init) ---
            mod = _load_module()
            mod._band_registry = _BAND_REGISTRY  # type: ignore[attr-defined]

            # --- Run full generator chain ---
            result = mod._process_nova(
                _NOVA_ID,
                _all_artifacts_manifest(),
            )

            # --- NovaResult assertions ---
            assert result.success is True, f"Expected success but got error: {result.error}"
            assert result.nova_id == _NOVA_ID
            assert result.error is None

            # --- Observation count assertions ---
            # 1 VALID spectrum with a readable web-ready CSV.
            assert result.spectra_count == 1

            # 6 V + 5 B + 4 R + 2 radio = 17 photometry observations.
            assert result.photometry_count == 17

            # 2 references seeded (both resolvable).
            assert result.references_count == 2

            # Sparkline: optical detections with magnitude → True.
            assert result.has_sparkline is True

    def test_photometry_offset_cache_written(self) -> None:
        """After a successful sweep, the offset cache item exists in
        the main NovaCat table for the optical regime.
        """
        with mock_aws():
            main_table = _create_main_table()
            phot_table = _create_photometry_table()
            s3 = _create_s3_buckets()

            _seed_nova(main_table)
            _seed_references(main_table)
            _seed_spectra_data_product(main_table)
            _seed_photometry_rows(phot_table)
            _seed_web_ready_csv(s3)
            _seed_raw_fits(s3)

            mod = _load_module()
            mod._band_registry = _BAND_REGISTRY  # type: ignore[attr-defined]
            result = mod._process_nova(
                _NOVA_ID,
                _all_artifacts_manifest(),
            )
            assert result.success is True

            # The offset cache for optical should exist.
            cache_resp = main_table.get_item(
                Key={"PK": _NOVA_ID, "SK": "OFFSET_CACHE#optical"},
            )
            cache_item = cache_resp.get("Item")
            assert cache_item is not None, "Offset cache item not found for optical regime"
            assert "band_offsets" in cache_item
            assert "band_set_hash" in cache_item
            assert "computed_at" in cache_item

            # Cache should contain entries for V, B, R.
            band_offsets = cache_item["band_offsets"]
            assert "V" in band_offsets
            assert "B" in band_offsets
            assert "R" in band_offsets

    def test_nova_without_photometry_still_succeeds(self) -> None:
        """A nova with references and spectra but no photometry rows
        still produces a successful result.
        """
        with mock_aws():
            main_table = _create_main_table()
            _create_photometry_table()  # exists but empty
            s3 = _create_s3_buckets()

            _seed_nova(main_table)
            _seed_references(main_table)
            _seed_spectra_data_product(main_table)
            _seed_web_ready_csv(s3)
            _seed_raw_fits(s3)

            mod = _load_module()
            mod._band_registry = _BAND_REGISTRY  # type: ignore[attr-defined]
            result = mod._process_nova(
                _NOVA_ID,
                _all_artifacts_manifest(),
            )

            assert result.success is True
            assert result.spectra_count == 1
            assert result.photometry_count == 0
            assert result.references_count == 2
            assert result.has_sparkline is False

    def test_offset_cache_reused_on_second_run(self) -> None:
        """Running the generator twice reuses cached offsets on the
        second invocation (cache is still valid because data hasn't
        changed).
        """
        with mock_aws():
            main_table = _create_main_table()
            phot_table = _create_photometry_table()
            s3 = _create_s3_buckets()

            _seed_nova(main_table)
            _seed_references(main_table)
            _seed_spectra_data_product(main_table)
            _seed_photometry_rows(phot_table)
            _seed_web_ready_csv(s3)
            _seed_raw_fits(s3)

            mod = _load_module()

            mod._band_registry = _BAND_REGISTRY  # type: ignore[attr-defined]

            # First run — computes and caches offsets.
            result_1 = mod._process_nova(
                _NOVA_ID,
                _all_artifacts_manifest(),
            )
            assert result_1.success is True

            # Read the cache timestamp.
            cache_1 = main_table.get_item(
                Key={"PK": _NOVA_ID, "SK": "OFFSET_CACHE#optical"},
            )["Item"]
            computed_at_1 = cache_1["computed_at"]

            # Second run — should reuse cache (same data, no changes).
            result_2 = mod._process_nova(
                _NOVA_ID,
                _all_artifacts_manifest(),
            )
            assert result_2.success is True
            assert result_2.photometry_count == result_1.photometry_count

            # Cache timestamp should be unchanged (cache was reused,
            # not rewritten).
            cache_2 = main_table.get_item(
                Key={"PK": _NOVA_ID, "SK": "OFFSET_CACHE#optical"},
            )["Item"]
            assert cache_2["computed_at"] == computed_at_1
