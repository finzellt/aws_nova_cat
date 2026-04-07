#!/usr/bin/env python3
"""
Patch: Merge observation-list tests into test_generators_spectra.py.

Adds the wavelength field migration and SNR observation-list tests
(created during the wavelength/SNR enrichment work) into the existing
comprehensive spectra generator test file.

Also adds the required imports (MagicMock, patch) and helper functions
(_make_product, _run_generator) for the new tests.

Prerequisites:
  - The file must be the ORIGINAL test_generators_spectra.py (with edge
    trimming, flux floor, LTTB, and wavelength range tests).
  - The file must NOT already contain TestObservationsWavelengthFields.

Usage:
    python patch_merge_observation_tests.py \\
        tests/services/artifact_generator/test_generators_spectra.py
"""

from __future__ import annotations

import sys


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _require_absent(content: str, marker: str, label: str) -> None:
    if marker in content:
        print(f"PRECONDITION FAILED — {label!r} already present (patch may have been applied).")
        sys.exit(1)


_NEW_IMPORTS = """\
from unittest.mock import MagicMock, patch
"""

_NEW_HELPERS_AND_TESTS = '''

# ---------------------------------------------------------------------------
# Observation list helpers
# ---------------------------------------------------------------------------

_MODULE = "generators.spectra"


def _make_product(
    *,
    data_product_id: str = "dp-001",
    observation_date_mjd: float = 59000.0,
    instrument: str = "UVES",
    telescope: str = "VLT-UT2",
    provider: str = "ESO",
    wavelength_min_nm: float | None = None,
    wavelength_max_nm: float | None = None,
    wavelength_min: float | None = None,
    wavelength_max: float | None = None,
    snr: float | None = None,
) -> dict[str, Any]:
    """Build a minimal DataProduct dict for observation-list testing."""
    product: dict[str, Any] = {
        "data_product_id": data_product_id,
        "observation_date_mjd": Decimal(str(observation_date_mjd)),
        "instrument": instrument,
        "telescope": telescope,
        "provider": provider,
    }
    if wavelength_min_nm is not None:
        product["wavelength_min_nm"] = Decimal(str(wavelength_min_nm))
    if wavelength_max_nm is not None:
        product["wavelength_max_nm"] = Decimal(str(wavelength_max_nm))
    if wavelength_min is not None:
        product["wavelength_min"] = Decimal(str(wavelength_min))
    if wavelength_max is not None:
        product["wavelength_max"] = Decimal(str(wavelength_max))
    if snr is not None:
        product["snr"] = Decimal(str(snr))
    return product


def _run_generator(products: list[dict[str, Any]]) -> dict[str, Any]:
    """Run generate_spectra_json with mocked DDB query and S3 I/O.

    Returns the full artifact dict.  Stage-1 processing and multi-arm
    merging are bypassed — we only care about the observations list.
    """
    mock_table = MagicMock()
    mock_s3 = MagicMock()
    nova_context: dict[str, Any] = {
        "outburst_mjd": 59000.0,
        "outburst_mjd_is_estimated": False,
    }

    with (
        patch(f"{_MODULE}._query_valid_spectra", return_value=products),
        patch(f"{_MODULE}._process_spectrum_stage1", return_value=None),
        patch(f"{_MODULE}._merge_multi_arm_spectra", return_value=[]),
    ):
        return generate_spectra_json(
            nova_id="nova-001",
            table=mock_table,
            s3_client=mock_s3,
            private_bucket="test-bucket",
            nova_context=nova_context,
        )


# ---------------------------------------------------------------------------
# Observation list: wavelength field migration
# ---------------------------------------------------------------------------


class TestObservationsWavelengthFields:
    def test_observations_list_reads_wavelength_min_nm(self) -> None:
        """New-style DDB fields (wavelength_min_nm / wavelength_max_nm)."""
        products = [
            _make_product(
                wavelength_min_nm=350.0,
                wavelength_max_nm=950.0,
            ),
        ]
        artifact = _run_generator(products)
        obs = artifact["observations"]
        assert len(obs) == 1
        assert obs[0]["wavelength_min"] == pytest.approx(350.0)
        assert obs[0]["wavelength_max"] == pytest.approx(950.0)

    def test_observations_list_falls_back_to_old_wavelength_fields(self) -> None:
        """Old-style DDB fields (wavelength_min / wavelength_max) still work."""
        products = [
            _make_product(
                wavelength_min=300.0,
                wavelength_max=900.0,
            ),
        ]
        artifact = _run_generator(products)
        obs = artifact["observations"]
        assert len(obs) == 1
        assert obs[0]["wavelength_min"] == pytest.approx(300.0)
        assert obs[0]["wavelength_max"] == pytest.approx(900.0)


# ---------------------------------------------------------------------------
# Observation list: SNR
# ---------------------------------------------------------------------------


class TestObservationsSnr:
    def test_observations_list_includes_snr_when_present(self) -> None:
        products = [
            _make_product(snr=42.5, wavelength_min_nm=350.0, wavelength_max_nm=950.0),
        ]
        artifact = _run_generator(products)
        obs = artifact["observations"]
        assert len(obs) == 1
        assert obs[0]["snr"] == pytest.approx(42.5)

    def test_observations_list_omits_snr_when_absent(self) -> None:
        products = [
            _make_product(wavelength_min_nm=350.0, wavelength_max_nm=950.0),
        ]
        artifact = _run_generator(products)
        obs = artifact["observations"]
        assert len(obs) == 1
        assert "snr" not in obs[0]
'''


def main() -> None:
    if len(sys.argv) != 2:
        print(
            f"Usage: {sys.argv[0]} "
            "<path/to/tests/services/artifact_generator/test_generators_spectra.py>"
        )
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # ── Precondition checks ───────────────────────────────────────────────
    _require(src, "class TestTrimDeadEdges:", "original edge-trimming tests")
    _require(src, "class TestNormalizeFluxFloor:", "original flux floor tests")
    _require(src, "class TestLttbDownsampling:", "original LTTB tests")
    _require(src, "class TestWavelengthRangeTrim:", "original wavelength trim tests")
    _require(src, "from generators.spectra import", "correct import path")
    _require_absent(src, "TestObservationsWavelengthFields", "new tests already merged")
    _require_absent(src, "TestObservationsSnr", "new SNR tests already merged")

    print("All preconditions satisfied. Applying patches…")

    # ── Patch 1 — Add MagicMock/patch imports ─────────────────────────────
    # Insert after the existing pytest import
    if "from unittest.mock import" not in src:
        src = src.replace(
            "import pytest\n",
            "import pytest\n" + _NEW_IMPORTS,
            1,
        )
        print("  ✓ Added MagicMock/patch imports")
    else:
        print("  ⊘ MagicMock/patch imports already present — skipped")

    # ── Patch 2 — Append new helpers and test classes ─────────────────────
    src = src.rstrip() + "\n" + _NEW_HELPERS_AND_TESTS

    print("  ✓ Appended _make_product, _run_generator, and test classes")

    # ── Post-condition checks ─────────────────────────────────────────────
    checks = [
        ("class TestObservationsWavelengthFields:", "wavelength field tests"),
        ("class TestObservationsSnr:", "SNR tests"),
        ("def _make_product(", "_make_product helper"),
        ("def _run_generator(", "_run_generator helper"),
        ("from unittest.mock import MagicMock, patch", "mock imports"),
    ]

    failed = False
    for marker, label in checks:
        if marker not in src:
            print(f"POSTCONDITION FAILED — {label!r}")
            failed = True

    if failed:
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"\nPatched successfully: {path}")
    print()
    print("Next steps:")
    print("  1. Delete the duplicate: rm tests/services/test_generators_spectra.py")
    print(
        "  2. Run: python -m pytest tests/services/artifact_generator/test_generators_spectra.py -v"
    )
    print(
        "  3. Run: python -m ruff check tests/services/artifact_generator/test_generators_spectra.py"
    )


if __name__ == "__main__":
    main()
