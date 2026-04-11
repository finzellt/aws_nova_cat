"""Unit tests for composite filtering in generators/spectra.py.

Covers:
  - Composites replace their constituent spectra in the display set.
  - Rejected spectra from composites are suppressed.
  - Non-composited spectra pass through unchanged.
  - Composite web-ready S3 key is used instead of constructed path.
  - Mixed composites and individuals produce the correct display set.
"""

from __future__ import annotations

import csv
import io
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from generators.spectra import (
    _filter_composites,
    _process_spectrum_stage1,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_individual_product(
    data_product_id: str,
    provider: str = "ESO",
    mjd: float = 59000.0,
) -> dict[str, Any]:
    """Build a minimal individual spectra DataProduct item."""
    return {
        "PK": "nova-test",
        "SK": f"PRODUCT#SPECTRA#{provider}#{data_product_id}",
        "data_product_id": data_product_id,
        "validation_status": "VALID",
        "instrument": "X-Shooter",
        "telescope": "VLT",
        "provider": provider,
        "observation_date_mjd": Decimal(str(mjd)),
        "wavelength_min_nm": Decimal("350"),
        "wavelength_max_nm": Decimal("900"),
        "flux_unit": "erg/s/cm2/A",
    }


def _make_composite_product(
    composite_id: str,
    constituent_ids: list[str],
    rejected_ids: list[str] | None = None,
    provider: str = "ESO",
    mjd: float = 59000.0,
    web_ready_s3_key: str | None = None,
) -> dict[str, Any]:
    """Build a minimal composite spectra DataProduct item."""
    if web_ready_s3_key is None:
        web_ready_s3_key = f"derived/spectra/nova-test/composites/{composite_id}/web_ready.csv"
    return {
        "PK": "nova-test",
        "SK": f"PRODUCT#SPECTRA#{provider}#COMPOSITE#{composite_id}",
        "data_product_id": composite_id,
        "validation_status": "VALID",
        "instrument": "X-Shooter",
        "telescope": "VLT",
        "provider": provider,
        "observation_date_mjd": Decimal(str(mjd)),
        "wavelength_min_nm": Decimal("350"),
        "wavelength_max_nm": Decimal("900"),
        "flux_unit": "erg/s/cm2/A",
        "constituent_data_product_ids": constituent_ids,
        "rejected_data_product_ids": rejected_ids or [],
        "web_ready_s3_key": web_ready_s3_key,
    }


def _make_csv_body(n_points: int = 100) -> str:
    """Generate a simple web-ready CSV body."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["wavelength_nm", "flux"])
    for i in range(n_points):
        wl = 400.0 + i * 5.0
        fx = 1.0 + 0.5 * (i % 10)
        writer.writerow([wl, fx])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Test: _filter_composites
# ---------------------------------------------------------------------------


class TestFilterComposites:
    """Post-query filtering logic for composite spectra."""

    def test_composite_replaces_constituents(self) -> None:
        """Composite's constituent spectra are excluded from display set."""
        individual_a = _make_individual_product("dp-a")
        individual_b = _make_individual_product("dp-b")
        composite = _make_composite_product(
            "comp-1",
            constituent_ids=["dp-a", "dp-b"],
        )

        products = [individual_a, individual_b, composite]
        result = _filter_composites(products)

        result_ids = [p["data_product_id"] for p in result]
        assert "comp-1" in result_ids
        assert "dp-a" not in result_ids
        assert "dp-b" not in result_ids

    def test_rejected_spectra_suppressed(self) -> None:
        """Composite's rejected spectra are also excluded from display set."""
        individual_a = _make_individual_product("dp-a")
        individual_rejected = _make_individual_product("dp-rejected")
        individual_unrelated = _make_individual_product("dp-unrelated")
        composite = _make_composite_product(
            "comp-1",
            constituent_ids=["dp-a"],
            rejected_ids=["dp-rejected"],
        )

        products = [individual_a, individual_rejected, individual_unrelated, composite]
        result = _filter_composites(products)

        result_ids = [p["data_product_id"] for p in result]
        assert "comp-1" in result_ids
        assert "dp-unrelated" in result_ids
        assert "dp-a" not in result_ids
        assert "dp-rejected" not in result_ids

    def test_non_composited_spectra_pass_through(self) -> None:
        """Spectra not referenced by any composite remain unchanged."""
        individual_a = _make_individual_product("dp-a")
        individual_b = _make_individual_product("dp-b")
        individual_c = _make_individual_product("dp-c")

        products = [individual_a, individual_b, individual_c]
        result = _filter_composites(products)

        result_ids = [p["data_product_id"] for p in result]
        assert result_ids == ["dp-a", "dp-b", "dp-c"]

    def test_mixed_composites_and_individuals(self) -> None:
        """Nova with both composited and non-composited spectra."""
        # Night 1: dp-a + dp-b composited into comp-1
        individual_a = _make_individual_product("dp-a", mjd=59000.0)
        individual_b = _make_individual_product("dp-b", mjd=59000.0)
        composite_1 = _make_composite_product(
            "comp-1",
            constituent_ids=["dp-a", "dp-b"],
            mjd=59000.0,
        )
        # Night 2: dp-c stands alone (no composite)
        individual_c = _make_individual_product("dp-c", mjd=59001.0)
        # Night 3: dp-d composited, dp-e rejected
        individual_d = _make_individual_product("dp-d", mjd=59002.0)
        individual_e = _make_individual_product("dp-e", mjd=59002.0)
        composite_2 = _make_composite_product(
            "comp-2",
            constituent_ids=["dp-d"],
            rejected_ids=["dp-e"],
            mjd=59002.0,
        )

        products = [
            individual_a,
            individual_b,
            individual_c,
            individual_d,
            individual_e,
            composite_1,
            composite_2,
        ]
        result = _filter_composites(products)

        result_ids = [p["data_product_id"] for p in result]
        # Composites present
        assert "comp-1" in result_ids
        assert "comp-2" in result_ids
        # Non-composited individual present
        assert "dp-c" in result_ids
        # Suppressed individuals absent
        assert "dp-a" not in result_ids
        assert "dp-b" not in result_ids
        assert "dp-d" not in result_ids
        assert "dp-e" not in result_ids
        # Total: 2 composites + 1 individual = 3
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Test: Composite web-ready path
# ---------------------------------------------------------------------------


class TestCompositeWebReadyPath:
    """Composite items use their stored web_ready_s3_key for S3 reads."""

    def test_composite_uses_web_ready_s3_key(self) -> None:
        """_process_spectrum_stage1 reads from the composite's web_ready_s3_key."""
        composite_s3_key = "derived/spectra/nova-test/composites/comp-1/web_ready.csv"
        composite = _make_composite_product(
            "comp-1",
            constituent_ids=["dp-a", "dp-b"],
            web_ready_s3_key=composite_s3_key,
        )

        csv_body = _make_csv_body()
        s3_client = MagicMock()
        s3_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_body.encode("utf-8")))
        }

        result = _process_spectrum_stage1(
            "nova-test",
            composite,
            s3_client,
            "test-bucket",
        )

        # Verify S3 was called with the composite's key, not the constructed one
        s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket",
            Key=composite_s3_key,
        )
        assert result is not None
        assert result["product"]["data_product_id"] == "comp-1"

    def test_individual_uses_constructed_path(self) -> None:
        """Non-composite items still use the conventional S3 key construction."""
        individual = _make_individual_product("dp-a")

        csv_body = _make_csv_body()
        s3_client = MagicMock()
        s3_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_body.encode("utf-8")))
        }

        result = _process_spectrum_stage1(
            "nova-test",
            individual,
            s3_client,
            "test-bucket",
        )

        expected_key = "derived/spectra/nova-test/dp-a/web_ready.csv"
        s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket",
            Key=expected_key,
        )
        assert result is not None
