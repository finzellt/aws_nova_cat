"""Integration tests for spectra SNR quality gates.

Covers two gates introduced in epic/29-spectra-quality-and-docs:

1. **Display gate** (``spectra.py:487``, constant ``_SNR_DISPLAY_FLOOR = 5.0``):
   Individual spectra with ``0 < effective_snr < 5.0`` are excluded from the
   waterfall plot output.  SNR exactly equal to 5.0 is *included* (strict
   less-than comparison).

2. **Composite relative gate** (``compositing.py:1055–1076``,
   constant ``_COMPOSITING_SNR_RELATIVE_THRESHOLD = 1/3``):
   Within a compositing group of ≥ 2 members, spectra with
   ``0 < snr < (group_median_snr × 1/3)`` are excluded from the composite
   but may still appear individually if they pass the display gate.

Gate implementations:
  - Display gate:    ``services/artifact_generator/generators/spectra.py`` line 487
  - Composite gate:  ``services/artifact_generator/generators/compositing.py`` line 1055
  - SNR estimator:   ``services/nova_common_layer/python/nova_common/spectral.py`` (``der_snr``)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import patch

import boto3
import numpy as np
from moto import mock_aws
from nova_common.spectral import der_snr
from numpy.typing import NDArray

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-SNRGate-Test"
_BUCKET = "nova-cat-snr-gate-test"
_REGION = "us-east-1"
_NOVA_ID = "snrgate-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# Synthetic flux helpers
# ---------------------------------------------------------------------------


def _make_high_snr_fluxes(n: int = 2000, seed: int = 42) -> list[float]:
    """Return a flux array whose DER_SNR estimate is well above 5.

    Constant signal = 1000 with low Gaussian noise (σ = 10) → SNR ≈ 100.
    """
    rng = np.random.default_rng(seed)
    result: list[float] = (1000.0 + rng.normal(0, 10, n)).tolist()
    return result


def _make_low_snr_fluxes(n: int = 2000, seed: int = 99) -> list[float]:
    """Return a flux array whose DER_SNR estimate is well below 5.

    Signal = 10 with high Gaussian noise (σ = 50) → SNR ≈ 0.2.
    """
    rng = np.random.default_rng(seed)
    fluxes = 10.0 + rng.normal(0, 50, n)
    # Ensure all values are positive so DER_SNR doesn't bail on negative median.
    result: list[float] = np.abs(fluxes).tolist()
    return result


def _make_csv(fluxes: list[float], wl_start: float = 400.0, wl_end: float = 700.0) -> str:
    """Build a ``wavelength_nm,flux`` CSV body from a flux array."""
    wavelengths = np.linspace(wl_start, wl_end, len(fluxes))
    lines = ["wavelength_nm,flux"]
    for w, f in zip(wavelengths, fluxes, strict=True):
        lines.append(f"{w:.6f},{f:.6f}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------


def _create_table(dynamodb: Any) -> Any:
    """Create a minimal NovaCat table in moto."""
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


def _seed_spectra_product(
    table: Any,
    data_product_id: str,
    *,
    snr: float | None = None,
    hints_snr: float | None = None,
    observation_date_mjd: float = 56083.0,
    instrument: str = "Goodman",
    provider: str = "TestProvider",
) -> None:
    """Seed a VALID spectra DataProduct item."""
    item: dict[str, Any] = {
        "PK": _NOVA_ID,
        "SK": f"PRODUCT#SPECTRA#{provider}#{data_product_id}",
        "entity_type": "DataProduct",
        "data_product_id": data_product_id,
        "nova_id": _NOVA_ID,
        "provider": provider,
        "product_type": "SPECTRA",
        "validation_status": "VALID",
        "observation_date_mjd": Decimal(str(observation_date_mjd)),
        "instrument": instrument,
        "telescope": "SOAR",
        "flux_unit": "erg/s/cm2/A",
        "wavelength_min_nm": Decimal("400.0"),
        "wavelength_max_nm": Decimal("700.0"),
    }
    if snr is not None:
        item["snr"] = Decimal(str(snr))
    if hints_snr is not None:
        item["hints"] = {"snr": Decimal(str(hints_snr))}
    table.put_item(Item=item)


def _seed_csv(s3: Any, data_product_id: str, csv_body: str) -> None:
    """Upload a web-ready CSV to moto S3."""
    s3.put_object(
        Bucket=_BUCKET,
        Key=f"derived/spectra/{_NOVA_ID}/{data_product_id}/web_ready.csv",
        Body=csv_body.encode(),
    )


# ---------------------------------------------------------------------------
# Display gate helper
# ---------------------------------------------------------------------------


def _spectra_ids_in_output(result: dict[str, Any]) -> set[str]:
    """Extract the set of spectrum_ids present in the spectra.json output.

    The output schema uses ``spectrum_id`` (which equals ``data_product_id``).
    Spectra appear in the top-level ``spectra`` list and optionally nested
    inside ``regimes[].spectra``.
    """
    ids: set[str] = set()
    for spectrum in result.get("spectra", []):
        sid = spectrum.get("spectrum_id")
        if sid:
            ids.add(sid)
    for regime in result.get("regimes", []):
        for spectrum in regime.get("spectra", []):
            sid = spectrum.get("spectrum_id")
            if sid:
                ids.add(sid)
    return ids


# ===================================================================
# Display gate tests
# ===================================================================


class TestDisplayGate:
    """Tests for the absolute SNR display gate in spectra.py.

    The gate excludes spectra with ``0 < effective_snr < 5.0`` from
    the waterfall output.
    """

    @staticmethod
    def _run_generate(
        table: Any,
        s3: Any,
    ) -> dict[str, Any]:
        """Import and call ``generate_spectra_json`` inside mock context."""
        from generators.spectra import generate_spectra_json

        nova_context: dict[str, Any] = {
            "outburst_mjd": 56080.0,
            "outburst_mjd_is_estimated": False,
        }
        return generate_spectra_json(
            nova_id=_NOVA_ID,
            table=table,
            s3_client=s3,
            private_bucket=_BUCKET,
            nova_context=nova_context,
        )

    def test_stored_snr_above_floor_included(self) -> None:
        """Spectrum with stored snr=10 passes the display gate."""
        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
            table = _create_table(dynamodb)
            s3 = boto3.client("s3", region_name=_REGION)
            s3.create_bucket(Bucket=_BUCKET)

            dp_id = "dp-high-snr-stored"
            _seed_spectra_product(table, dp_id, snr=10.0)
            _seed_csv(s3, dp_id, _make_csv(_make_high_snr_fluxes()))

            result = self._run_generate(table, s3)
            assert dp_id in _spectra_ids_in_output(result)

    def test_stored_snr_below_floor_excluded(self) -> None:
        """Spectrum with stored snr=2 is excluded by the display gate."""
        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
            table = _create_table(dynamodb)
            s3 = boto3.client("s3", region_name=_REGION)
            s3.create_bucket(Bucket=_BUCKET)

            dp_id = "dp-low-snr-stored"
            _seed_spectra_product(table, dp_id, snr=2.0)
            _seed_csv(s3, dp_id, _make_csv(_make_high_snr_fluxes()))

            result = self._run_generate(table, s3)
            assert dp_id not in _spectra_ids_in_output(result)

    def test_der_snr_fallback_above_floor_included(self) -> None:
        """Spectrum with no stored SNR but high DER_SNR estimate passes."""
        high_fluxes = _make_high_snr_fluxes()
        # Sanity-check the synthetic flux array actually has high SNR.
        estimated = der_snr(high_fluxes)
        assert estimated > 5.0, f"Expected DER_SNR >> 5, got {estimated:.2f}"

        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
            table = _create_table(dynamodb)
            s3 = boto3.client("s3", region_name=_REGION)
            s3.create_bucket(Bucket=_BUCKET)

            dp_id = "dp-high-snr-dersnr"
            _seed_spectra_product(table, dp_id)  # no snr field
            _seed_csv(s3, dp_id, _make_csv(high_fluxes))

            result = self._run_generate(table, s3)
            assert dp_id in _spectra_ids_in_output(result)

    def test_der_snr_fallback_below_floor_excluded(self) -> None:
        """Spectrum with no stored SNR and low DER_SNR estimate is excluded."""
        low_fluxes = _make_low_snr_fluxes()
        estimated = der_snr(low_fluxes)
        assert 0 < estimated < 5.0, f"Expected 0 < DER_SNR < 5, got {estimated:.2f}"

        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
            table = _create_table(dynamodb)
            s3 = boto3.client("s3", region_name=_REGION)
            s3.create_bucket(Bucket=_BUCKET)

            dp_id = "dp-low-snr-dersnr"
            _seed_spectra_product(table, dp_id)  # no snr field
            _seed_csv(s3, dp_id, _make_csv(low_fluxes))

            result = self._run_generate(table, s3)
            assert dp_id not in _spectra_ids_in_output(result)

    def test_boundary_snr_exactly_five_is_included(self) -> None:
        """Boundary: snr=5.0 is included (gate condition is strict less-than).

        Observed behavior: the gate checks ``0 < eff_snr < 5.0`` (spectra.py:487),
        so SNR exactly 5.0 does NOT satisfy the exclusion condition and is kept.
        """
        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
            table = _create_table(dynamodb)
            s3 = boto3.client("s3", region_name=_REGION)
            s3.create_bucket(Bucket=_BUCKET)

            dp_id = "dp-boundary-snr-5"
            _seed_spectra_product(table, dp_id, snr=5.0)
            _seed_csv(s3, dp_id, _make_csv(_make_high_snr_fluxes()))

            result = self._run_generate(table, s3)
            # SNR=5.0 is included — the condition ``0 < 5.0 < 5.0`` is False.
            assert dp_id in _spectra_ids_in_output(result)


# ===================================================================
# Composite relative gate tests
# ===================================================================


def _make_synthetic_fits_arrays(
    n: int = 3000,
) -> tuple[NDArray[np.float64], NDArray[np.float64]]:
    """Return (wavelengths, fluxes) arrays with enough points for compositing.

    MIN_POINTS_FOR_COMPOSITE is 2000; we use 3000 to be safe.
    """
    wavelengths = np.linspace(400.0, 700.0, n)
    fluxes = np.ones(n) * 100.0
    return wavelengths, fluxes


class TestCompositeRelativeGate:
    """Tests for the compositing relative SNR gate in compositing.py.

    The gate excludes spectra with ``0 < snr < (group_median × 1/3)``
    from the composite.  Only applies to groups with ≥ 2 members.
    """

    @staticmethod
    def _make_product(
        data_product_id: str,
        *,
        snr: float,
        observation_date_mjd: float = 56083.5,
        provider: str = "TestProvider",
        instrument: str = "Goodman",
        sha256: str = "aabbccdd",
    ) -> dict[str, Any]:
        """Build a minimal product dict for compositing."""
        return {
            "PK": _NOVA_ID,
            "SK": f"PRODUCT#SPECTRA#{provider}#{data_product_id}",
            "data_product_id": data_product_id,
            "nova_id": _NOVA_ID,
            "provider": provider,
            "instrument": instrument,
            "telescope": "SOAR",
            "observation_date_mjd": Decimal(str(observation_date_mjd)),
            "sha256": sha256,
            "snr": Decimal(str(snr)),
            "raw_s3_key": f"raw/spectra/{_NOVA_ID}/{data_product_id}.fits",
            "validation_status": "VALID",
            "product_type": "SPECTRA",
        }

    def _run_compositing_group(
        self,
        products: list[dict[str, Any]],
        instrument: str = "Goodman",
    ) -> dict[str, Any]:
        """Call ``_process_compositing_group`` and return the composite DDB item.

        Mocks ``read_fits_spectrum`` to return synthetic arrays and sets up
        moto DDB + S3 for the writes.
        """
        from generators.compositing import (
            CompositingGroup,
            CompositingSweepResult,
            _process_compositing_group,
        )

        group = CompositingGroup(instrument=instrument, products=products)
        result = CompositingSweepResult(groups_found=1, skipped=0, built=0, degenerate=0, errors=0)

        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
            table = _create_table(dynamodb)
            s3 = boto3.client("s3", region_name=_REGION)
            s3.create_bucket(Bucket=_BUCKET)

            synthetic = _make_synthetic_fits_arrays()
            with patch(
                "generators.fits_reader.read_fits_spectrum",
                return_value=synthetic,
            ):
                _process_compositing_group(
                    nova_id=_NOVA_ID,
                    group=group,
                    existing_by_fp={},
                    table=table,
                    s3_client=s3,
                    bucket=_BUCKET,
                    result=result,
                )

            # Read back all composite items written to DDB.
            resp = table.scan()
            composites = [
                item for item in resp.get("Items", []) if "COMPOSITE" in item.get("SK", "")
            ]
            assert len(composites) == 1, f"Expected 1 composite item, got {len(composites)}"
            composite: dict[str, Any] = composites[0]
            return composite

    def test_all_high_snr_all_included(self) -> None:
        """Group [30, 25, 20]: all above 1/3 × median(25) ≈ 8.33 → all included."""
        products = [
            self._make_product("dp-a", snr=30.0, sha256="aa"),
            self._make_product("dp-b", snr=25.0, sha256="bb"),
            self._make_product("dp-c", snr=20.0, sha256="cc"),
        ]
        composite = self._run_compositing_group(products)
        assert sorted(composite["constituent_data_product_ids"]) == ["dp-a", "dp-b", "dp-c"]
        assert composite["rejected_data_product_ids"] == []

    def test_low_snr_member_excluded(self) -> None:
        """Group [30, 25, 5]: snr=5 < 1/3 × median(25) ≈ 8.33 → excluded."""
        products = [
            self._make_product("dp-a", snr=30.0, sha256="aa"),
            self._make_product("dp-b", snr=25.0, sha256="bb"),
            self._make_product("dp-c", snr=5.0, sha256="cc"),
        ]
        composite = self._run_compositing_group(products)
        assert sorted(composite["constituent_data_product_ids"]) == ["dp-a", "dp-b"]
        assert sorted(composite["rejected_data_product_ids"]) == ["dp-c"]

    def test_single_spectrum_group_relative_gate_noop(self) -> None:
        """Single-spectrum group: relative gate requires ≥ 2 members, so it's a no-op.

        The function writes a degenerate composite pointing at the survivor.
        """
        from generators.compositing import (
            CompositingGroup,
            CompositingSweepResult,
            _process_compositing_group,
        )

        products = [self._make_product("dp-solo", snr=30.0, sha256="aa")]
        group = CompositingGroup(instrument="Goodman", products=products)
        result = CompositingSweepResult(groups_found=1, skipped=0, built=0, degenerate=0, errors=0)

        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=_REGION)
            table = _create_table(dynamodb)
            s3 = boto3.client("s3", region_name=_REGION)
            s3.create_bucket(Bucket=_BUCKET)

            synthetic = _make_synthetic_fits_arrays()
            with patch(
                "generators.fits_reader.read_fits_spectrum",
                return_value=synthetic,
            ):
                _process_compositing_group(
                    nova_id=_NOVA_ID,
                    group=group,
                    existing_by_fp={},
                    table=table,
                    s3_client=s3,
                    bucket=_BUCKET,
                    result=result,
                )

            # Degenerate composite: single survivor is included.
            assert result["degenerate"] == 1
            resp = table.scan()
            composites = [
                item for item in resp.get("Items", []) if "COMPOSITE" in item.get("SK", "")
            ]
            assert len(composites) == 1
            assert composites[0]["constituent_data_product_ids"] == ["dp-solo"]
            assert composites[0]["rejected_data_product_ids"] == []

    def test_interaction_display_gate_and_relative_gate(self) -> None:
        """Interaction: relative gate is computed over FITS-readable constituents.

        The relative SNR gate in ``_process_compositing_group`` operates on
        products that passed the MIN_POINTS_FOR_COMPOSITE check and had a
        successful FITS read.  It does NOT re-apply the display gate (that's
        a separate concern in ``generate_spectra_json``).

        Here we verify that within a group of [30, 25, 3], the snr=3 member
        is excluded by the relative gate (3 < 1/3 × 25 ≈ 8.33), and the
        relative gate threshold is computed over all three products (the
        display gate is not applied inside the compositing pipeline).
        """
        products = [
            self._make_product("dp-a", snr=30.0, sha256="aa"),
            self._make_product("dp-b", snr=25.0, sha256="bb"),
            self._make_product("dp-c", snr=3.0, sha256="cc"),
        ]
        composite = self._run_compositing_group(products)

        # snr=3 is excluded by relative gate (3 < 8.33).
        assert sorted(composite["constituent_data_product_ids"]) == ["dp-a", "dp-b"]
        assert sorted(composite["rejected_data_product_ids"]) == ["dp-c"]
