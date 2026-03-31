"""Unit tests for nova_common.web_ready_csv.

Coverage
--------
build_web_ready_csv:
  - Wavelength unit conversion (Angstrom → nm, nm passthrough, micron → nm)
  - CSV header and row format
  - No downsampling for arrays ≤ 2000 points
  - Downsampling to exactly 2000 points for larger arrays
  - Smart downsampling: arrays slightly over 2000 retain most points
  - Array length mismatch → ValueError
  - Empty arrays → ValueError

derive_web_ready_s3_key:
  - Canonical key format

write_web_ready_csv_to_s3:
  - Correct S3 put_object call (key, bucket, content type, body)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest
from nova_common.web_ready_csv import (
    _MAX_POINTS,
    build_web_ready_csv,
    derive_web_ready_s3_key,
    write_web_ready_csv_to_s3,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"
_DATA_PRODUCT_ID = "bbbbbbbb-1111-1111-1111-000000000001"


# ---------------------------------------------------------------------------
# derive_web_ready_s3_key
# ---------------------------------------------------------------------------


class TestDeriveWebReadyS3Key:
    def test_canonical_format(self) -> None:
        key = derive_web_ready_s3_key(_NOVA_ID, _DATA_PRODUCT_ID)
        assert key == f"derived/spectra/{_NOVA_ID}/{_DATA_PRODUCT_ID}/web_ready.csv"


# ---------------------------------------------------------------------------
# build_web_ready_csv — unit conversion
# ---------------------------------------------------------------------------


class TestBuildWebReadyCsvUnitConversion:
    def test_angstrom_to_nm(self) -> None:
        """Wavelengths in Angstrom are converted to nm (÷ 10)."""
        wave = np.array([3000.0, 4000.0, 5000.0])
        flux = np.array([1.0, 2.0, 3.0])

        csv = build_web_ready_csv(wave, flux, "Angstrom")
        lines = csv.strip().split("\n")

        assert lines[0] == "wavelength_nm,flux"
        # 3000 Å = 300 nm
        row1_wave = float(lines[1].split(",")[0])
        assert row1_wave == pytest.approx(300.0)

    def test_angstrom_lowercase(self) -> None:
        """astropy.units accepts 'angstrom' (lowercase) as well."""
        wave = np.array([5000.0])
        flux = np.array([1.5])

        csv = build_web_ready_csv(wave, flux, "angstrom")
        lines = csv.strip().split("\n")
        row_wave = float(lines[1].split(",")[0])
        assert row_wave == pytest.approx(500.0)

    def test_nm_passthrough(self) -> None:
        """Wavelengths already in nm are not modified."""
        wave = np.array([300.0, 400.0, 500.0])
        flux = np.array([1.0, 2.0, 3.0])

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")
        row1_wave = float(lines[1].split(",")[0])
        assert row1_wave == pytest.approx(300.0)

    def test_micron_to_nm(self) -> None:
        """Wavelengths in micron are converted to nm (× 1000)."""
        wave = np.array([0.5, 1.0, 2.0])
        flux = np.array([1.0, 2.0, 3.0])

        csv = build_web_ready_csv(wave, flux, "um")
        lines = csv.strip().split("\n")
        # 0.5 µm = 500 nm
        row1_wave = float(lines[1].split(",")[0])
        assert row1_wave == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# build_web_ready_csv — CSV format
# ---------------------------------------------------------------------------


class TestBuildWebReadyCsvFormat:
    def test_header_present(self) -> None:
        wave = np.array([300.0, 400.0])
        flux = np.array([1.0, 2.0])

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")
        assert lines[0] == "wavelength_nm,flux"

    def test_correct_row_count(self) -> None:
        n = 50
        wave = np.linspace(300.0, 700.0, n)
        flux = np.ones(n)

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")
        # Header + n data rows
        assert len(lines) == n + 1

    def test_flux_values_preserved(self) -> None:
        wave = np.array([300.0, 400.0, 500.0])
        flux = np.array([1.5e-16, 2.3e-16, 1.8e-16])

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")
        row1_flux = float(lines[1].split(",")[1])
        assert row1_flux == pytest.approx(1.5e-16)


# ---------------------------------------------------------------------------
# build_web_ready_csv — downsampling
# ---------------------------------------------------------------------------


class TestBuildWebReadyCsvDownsampling:
    def test_no_downsampling_under_limit(self) -> None:
        """Arrays with ≤ 2000 points are not downsampled."""
        n = 1500
        wave = np.linspace(300.0, 700.0, n)
        flux = np.ones(n)

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")
        assert len(lines) == n + 1  # header + all data rows

    def test_no_downsampling_at_limit(self) -> None:
        """Exactly 2000 points are preserved as-is."""
        n = _MAX_POINTS
        wave = np.linspace(300.0, 700.0, n)
        flux = np.ones(n)

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")
        assert len(lines) == n + 1

    def test_downsampling_large_array(self) -> None:
        """Arrays over 2000 points are downsampled to exactly 2000."""
        n = 100_000
        wave = np.linspace(300.0, 700.0, n)
        flux = np.ones(n)

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")
        assert len(lines) == _MAX_POINTS + 1

    def test_downsampling_slightly_over_limit(self) -> None:
        """An array of 2030 points downsamples to exactly 2000, not 1015."""
        n = 2030
        wave = np.linspace(300.0, 700.0, n)
        flux = np.ones(n)

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")
        assert len(lines) == _MAX_POINTS + 1

    def test_downsampled_endpoints_preserved(self) -> None:
        """First and last wavelength values survive downsampling."""
        n = 10_000
        wave = np.linspace(300.0, 700.0, n)
        flux = np.ones(n)

        csv = build_web_ready_csv(wave, flux, "nm")
        lines = csv.strip().split("\n")

        first_wave = float(lines[1].split(",")[0])
        last_wave = float(lines[-1].split(",")[0])
        assert first_wave == pytest.approx(300.0)
        assert last_wave == pytest.approx(700.0)


# ---------------------------------------------------------------------------
# build_web_ready_csv — error cases
# ---------------------------------------------------------------------------


class TestBuildWebReadyCsvErrors:
    def test_length_mismatch_raises(self) -> None:
        wave = np.array([300.0, 400.0])
        flux = np.array([1.0])

        with pytest.raises(ValueError, match="same length"):
            build_web_ready_csv(wave, flux, "nm")

    def test_empty_arrays_raises(self) -> None:
        wave = np.array([])
        flux = np.array([])

        with pytest.raises(ValueError, match="empty"):
            build_web_ready_csv(wave, flux, "nm")


# ---------------------------------------------------------------------------
# write_web_ready_csv_to_s3
# ---------------------------------------------------------------------------


class TestWriteWebReadyCsvToS3:
    def test_put_object_called_with_correct_params(self) -> None:
        mock_s3: Any = MagicMock()
        csv_content = "wavelength_nm,flux\n300.0,1.5e-16\n"
        bucket = "test-private-bucket"

        key = write_web_ready_csv_to_s3(
            csv_content=csv_content,
            nova_id=_NOVA_ID,
            data_product_id=_DATA_PRODUCT_ID,
            s3=mock_s3,
            bucket=bucket,
        )

        expected_key = f"derived/spectra/{_NOVA_ID}/{_DATA_PRODUCT_ID}/web_ready.csv"
        assert key == expected_key

        mock_s3.put_object.assert_called_once_with(
            Bucket=bucket,
            Key=expected_key,
            Body=csv_content.encode("utf-8"),
            ContentType="text/csv",
        )

    def test_returns_s3_key(self) -> None:
        mock_s3: Any = MagicMock()

        key = write_web_ready_csv_to_s3(
            csv_content="wavelength_nm,flux\n",
            nova_id=_NOVA_ID,
            data_product_id=_DATA_PRODUCT_ID,
            s3=mock_s3,
            bucket="bucket",
        )

        assert key.startswith("derived/spectra/")
        assert key.endswith("/web_ready.csv")
