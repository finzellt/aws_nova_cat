"""Tests for compositing D1 — sweep orchestration.

End-to-end tests using moto for DDB and S3, with FITS reads and
cleaning functions mocked to return synthetic data.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import patch

import boto3
import numpy as np
from generators.compositing import (
    run_compositing_sweep,
)
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "TestNovaCat"
_BUCKET_NAME = "test-private-bucket"
_NOVA_ID = "aaaa-bbbb-cccc-dddd"
_REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _create_table(dynamodb: Any) -> Any:
    """Create a minimal DDB table."""
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
    table.wait_until_exists()
    return table


def _create_bucket(s3_client: Any) -> None:
    """Create a test S3 bucket."""
    s3_client.create_bucket(Bucket=_BUCKET_NAME)


def _put_spectrum(
    table: Any,
    dp_id: str,
    nova_id: str = _NOVA_ID,
    provider: str = "ESO",
    instrument: str = "UVES",
    observation_date_mjd: float = 60000.3,
    sha256: str = "sha_default",
    raw_s3_key: str | None = "raw/test.fits",
    validation_status: str = "VALID",
    snr: float = 30.0,
) -> None:
    """Write an individual spectra DataProduct item."""
    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": f"PRODUCT#SPECTRA#{provider}#{dp_id}",
        "entity_type": "DataProduct",
        "data_product_id": dp_id,
        "product_type": "SPECTRA",
        "provider": provider,
        "instrument": instrument,
        "validation_status": validation_status,
        "observation_date_mjd": Decimal(str(observation_date_mjd)),
        "sha256": sha256,
        "telescope": "VLT-UT2",
        "snr": Decimal(str(snr)),
    }
    if raw_s3_key is not None:
        item["raw_s3_key"] = raw_s3_key
    table.put_item(Item=item)


def _put_existing_composite(
    table: Any,
    composite_id: str,
    fingerprint: str,
    constituent_ids: list[str],
    rejected_ids: list[str] | None = None,
    nova_id: str = _NOVA_ID,
    provider: str = "ESO",
    instrument: str = "UVES",
) -> None:
    """Write an existing composite DataProduct item."""
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": f"PRODUCT#SPECTRA#{provider}#COMPOSITE#{composite_id}",
            "entity_type": "DataProduct",
            "data_product_id": composite_id,
            "product_type": "SPECTRA",
            "provider": provider,
            "instrument": instrument,
            "validation_status": "VALID",
            "constituent_data_product_ids": constituent_ids,
            "rejected_data_product_ids": rejected_ids or [],
            "composite_fingerprint": fingerprint,
            "observation_date_mjd": Decimal("60000.35"),
        }
    )


def _mock_fits_reader(n_points: int = 3000) -> Any:
    """Return a mock for read_fits_spectrum that produces synthetic arrays.

    The default n_points (3000) is above MIN_POINTS_FOR_COMPOSITE.
    """

    def _reader(
        s3_client: Any,
        bucket: str,
        raw_s3_key: str,
        data_product_id: str,
    ) -> tuple[np.ndarray, np.ndarray] | None:
        wl = np.linspace(400.0, 700.0, n_points)
        fx = np.sin(np.linspace(0, 10 * np.pi, n_points)) + 2.0
        return wl, fx

    return _reader


def _mock_fits_reader_by_id(
    point_counts: dict[str, int],
    default_points: int = 3000,
) -> Any:
    """Return a mock FITS reader that returns different point counts per dp_id."""

    def _reader(
        s3_client: Any,
        bucket: str,
        raw_s3_key: str,
        data_product_id: str,
    ) -> tuple[np.ndarray, np.ndarray] | None:
        n = point_counts.get(data_product_id, default_points)
        wl = np.linspace(400.0, 700.0, n)
        fx = np.ones(n) + 1.0
        return wl, fx

    return _reader


def _passthrough_clean(
    wl: list[float],
    fx: list[float],
    dp_id: str,
    **kwargs: object,
) -> tuple[list[float], list[float]]:
    """Cleaning function that passes through unchanged."""
    return wl, fx


# ---------------------------------------------------------------------------
# Patches applied to all sweep tests
# ---------------------------------------------------------------------------


def _sweep_patches() -> list[Any]:
    """Context manager stack for patching FITS reader + cleaning functions."""
    return [
        patch(
            "generators.fits_reader.read_fits_spectrum",
            side_effect=_mock_fits_reader(),
        ),
        patch("generators.shared.trim_dead_edges", side_effect=_passthrough_clean),
        patch("generators.shared.remove_interior_dead_runs", side_effect=_passthrough_clean),
        patch("generators.shared.reject_chip_gap_artifacts", side_effect=_passthrough_clean),
        patch("generators.shared.segment_aware_lttb", side_effect=lambda wl, fx: (wl, fx)),
    ]


# ===================================================================
# run_compositing_sweep
# ===================================================================


class TestRunCompositingSweep:
    """End-to-end sweep orchestration."""

    @mock_aws
    def test_no_individuals(self) -> None:
        """Nova with < 2 spectra returns immediately."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)

        _put_spectrum(table, "dp-001")  # only 1

        result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)
        assert result["groups_found"] == 0
        assert result["built"] == 0

    @mock_aws
    def test_no_groups_all_singletons(self) -> None:
        """Two spectra on different nights produce no compositing groups."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)

        _put_spectrum(table, "dp-001", observation_date_mjd=60000.3)
        _put_spectrum(table, "dp-002", observation_date_mjd=60001.3)

        result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)
        assert result["groups_found"] == 0

    @mock_aws
    def test_builds_real_composite(self) -> None:
        """Two same-night spectra above threshold produce a real composite."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        _put_spectrum(table, "dp-001", observation_date_mjd=60000.3, sha256="sha_1")
        _put_spectrum(table, "dp-002", observation_date_mjd=60000.4, sha256="sha_2")

        for p in _sweep_patches():
            p.start()
        try:
            result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)
        finally:
            patch.stopall()

        assert result["groups_found"] == 1
        assert result["built"] == 1
        assert result["skipped"] == 0

        # Verify composite DDB item was written.
        composites = table.query(
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key("PK").eq(_NOVA_ID)
                & boto3.dynamodb.conditions.Key("SK").begins_with("PRODUCT#SPECTRA#ESO#COMPOSITE#")
            ),
        )["Items"]
        assert len(composites) == 1
        comp = composites[0]
        assert sorted(comp["constituent_data_product_ids"]) == ["dp-001", "dp-002"]
        assert comp["composite_s3_key"] is not None
        assert comp["web_ready_s3_key"] is not None

    @mock_aws
    def test_fingerprint_match_skips(self) -> None:
        """Unchanged group is skipped when fingerprint matches."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        _put_spectrum(table, "dp-001", observation_date_mjd=60000.3, sha256="sha_1")
        _put_spectrum(table, "dp-002", observation_date_mjd=60000.4, sha256="sha_2")

        # Pre-compute the fingerprint that the sweep would generate.
        from generators.compositing import compute_composite_fingerprint

        expected_fp = compute_composite_fingerprint(
            ["dp-001", "dp-002"],
            {"dp-001": "sha_1", "dp-002": "sha_2"},
        )

        _put_existing_composite(
            table,
            "existing-comp",
            expected_fp,
            constituent_ids=["dp-001", "dp-002"],
        )

        result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)
        assert result["groups_found"] == 1
        assert result["skipped"] == 1
        assert result["built"] == 0

    @mock_aws
    def test_fingerprint_mismatch_rebuilds(self) -> None:
        """Changed sha256 causes fingerprint mismatch → rebuild."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        _put_spectrum(table, "dp-001", observation_date_mjd=60000.3, sha256="sha_CHANGED")
        _put_spectrum(table, "dp-002", observation_date_mjd=60000.4, sha256="sha_2")

        _put_existing_composite(
            table,
            "old-comp",
            "stale-fingerprint",
            constituent_ids=["dp-001", "dp-002"],
        )

        for p in _sweep_patches():
            p.start()
        try:
            result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)
        finally:
            patch.stopall()

        assert result["skipped"] == 0
        assert result["built"] == 1

    @mock_aws
    def test_degenerate_composite(self) -> None:
        """Group where only 1 spectrum passes threshold → degenerate composite."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        _put_spectrum(table, "dp-big", observation_date_mjd=60000.3, sha256="sha_big")
        _put_spectrum(table, "dp-small", observation_date_mjd=60000.4, sha256="sha_small")

        # dp-big has 3000 points (above threshold), dp-small has 500 (below).
        reader = _mock_fits_reader_by_id({"dp-big": 3000, "dp-small": 500})

        with (
            patch("generators.fits_reader.read_fits_spectrum", side_effect=reader),
            patch("generators.shared.trim_dead_edges", side_effect=_passthrough_clean),
            patch("generators.shared.remove_interior_dead_runs", side_effect=_passthrough_clean),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=_passthrough_clean),
            patch("generators.shared.segment_aware_lttb", side_effect=lambda wl, fx: (wl, fx)),
        ):
            result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)

        assert result["degenerate"] == 1
        assert result["built"] == 0

        # Verify degenerate DDB item.
        composites = table.query(
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key("PK").eq(_NOVA_ID)
                & boto3.dynamodb.conditions.Key("SK").begins_with("PRODUCT#SPECTRA#ESO#COMPOSITE#")
            ),
        )["Items"]
        assert len(composites) == 1
        comp = composites[0]
        assert comp["constituent_data_product_ids"] == ["dp-big"]
        assert "dp-small" in comp["rejected_data_product_ids"]
        assert "composite_s3_key" not in comp
        assert comp["web_ready_s3_key"] == f"derived/spectra/{_NOVA_ID}/dp-big/web_ready.csv"

    @mock_aws
    def test_all_rejected(self) -> None:
        """Group where all spectra are below threshold → no composite written."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        _put_spectrum(table, "dp-tiny1", observation_date_mjd=60000.3, sha256="sha_1")
        _put_spectrum(table, "dp-tiny2", observation_date_mjd=60000.4, sha256="sha_2")

        reader = _mock_fits_reader_by_id({"dp-tiny1": 100, "dp-tiny2": 200})

        with (
            patch("generators.fits_reader.read_fits_spectrum", side_effect=reader),
            patch("generators.shared.trim_dead_edges", side_effect=_passthrough_clean),
            patch("generators.shared.remove_interior_dead_runs", side_effect=_passthrough_clean),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=_passthrough_clean),
        ):
            result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)

        assert result["groups_found"] == 1
        assert result["built"] == 0
        assert result["degenerate"] == 0

        # No composite written.
        composites = table.query(
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key("PK").eq(_NOVA_ID)
                & boto3.dynamodb.conditions.Key("SK").begins_with("PRODUCT#SPECTRA#ESO#COMPOSITE#")
            ),
        )["Items"]
        assert len(composites) == 0

    @mock_aws
    def test_new_spectrum_breaks_fingerprint(self) -> None:
        """Adding a new spectrum to an existing group triggers a rebuild."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        # Existing composite was built from dp-001 + dp-002.
        from generators.compositing import compute_composite_fingerprint

        old_fp = compute_composite_fingerprint(
            ["dp-001", "dp-002"],
            {"dp-001": "sha_1", "dp-002": "sha_2"},
        )
        _put_existing_composite(
            table,
            "old-comp",
            old_fp,
            constituent_ids=["dp-001", "dp-002"],
        )

        # Same two spectra plus a new one on the same night.
        _put_spectrum(table, "dp-001", observation_date_mjd=60000.3, sha256="sha_1")
        _put_spectrum(table, "dp-002", observation_date_mjd=60000.4, sha256="sha_2")
        _put_spectrum(table, "dp-003", observation_date_mjd=60000.35, sha256="sha_3")

        for p in _sweep_patches():
            p.start()
        try:
            result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)
        finally:
            patch.stopall()

        assert result["skipped"] == 0
        assert result["built"] == 1

    @mock_aws
    def test_error_in_one_group_doesnt_crash_sweep(self) -> None:
        """An error in one group increments errors but processes others."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        # Group 1: night 1, UVES — will error (FITS reader returns None)
        _put_spectrum(
            table, "dp-err1", instrument="UVES", observation_date_mjd=60000.3, sha256="s1"
        )
        _put_spectrum(
            table, "dp-err2", instrument="UVES", observation_date_mjd=60000.4, sha256="s2"
        )

        # Group 2: night 1, XSHOOTER — will succeed
        _put_spectrum(
            table, "dp-ok1", instrument="XSHOOTER", observation_date_mjd=60000.3, sha256="s3"
        )
        _put_spectrum(
            table, "dp-ok2", instrument="XSHOOTER", observation_date_mjd=60000.4, sha256="s4"
        )

        call_count = 0

        def _flaky_reader(
            s3_client: Any,
            bucket: str,
            raw_s3_key: str,
            data_product_id: str,
        ) -> tuple[np.ndarray, np.ndarray] | None:
            nonlocal call_count
            call_count += 1
            if data_product_id.startswith("dp-err"):
                raise RuntimeError("Simulated FITS read failure")
            wl = np.linspace(400.0, 700.0, 3000)
            fx = np.ones(3000) + 1.0
            return wl, fx

        with (
            patch("generators.fits_reader.read_fits_spectrum", side_effect=_flaky_reader),
            patch("generators.shared.trim_dead_edges", side_effect=_passthrough_clean),
            patch("generators.shared.remove_interior_dead_runs", side_effect=_passthrough_clean),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=_passthrough_clean),
            patch("generators.shared.segment_aware_lttb", side_effect=lambda wl, fx: (wl, fx)),
        ):
            result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)

        assert result["groups_found"] == 2
        assert result["errors"] == 1
        assert result["built"] == 1

    @mock_aws
    def test_multiple_instruments_independent(self) -> None:
        """Groups from different instruments are processed independently."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        # UVES pair on night 1
        _put_spectrum(table, "dp-u1", instrument="UVES", observation_date_mjd=60000.3, sha256="su1")
        _put_spectrum(table, "dp-u2", instrument="UVES", observation_date_mjd=60000.4, sha256="su2")

        # XSHOOTER pair on night 1
        _put_spectrum(
            table, "dp-x1", instrument="XSHOOTER", observation_date_mjd=60000.3, sha256="sx1"
        )
        _put_spectrum(
            table, "dp-x2", instrument="XSHOOTER", observation_date_mjd=60000.4, sha256="sx2"
        )

        for p in _sweep_patches():
            p.start()
        try:
            result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)
        finally:
            patch.stopall()

        assert result["groups_found"] == 2
        assert result["built"] == 2

        # Verify two distinct composites written.
        composites = table.query(
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key("PK").eq(_NOVA_ID)
                & boto3.dynamodb.conditions.Key("SK").begins_with("PRODUCT#SPECTRA#")
            ),
        )["Items"]
        comp_items = [i for i in composites if "COMPOSITE" in str(i["SK"])]
        assert len(comp_items) == 2
        instruments = {c["instrument"] for c in comp_items}
        assert instruments == {"UVES", "XSHOOTER"}

    @mock_aws
    def test_no_raw_s3_key_rejects_spectrum(self) -> None:
        """Spectrum without raw_s3_key is treated as rejected."""
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        table = _create_table(dynamodb)
        s3 = boto3.client("s3", region_name=_REGION)
        _create_bucket(s3)

        _put_spectrum(
            table, "dp-good", observation_date_mjd=60000.3, sha256="s1", raw_s3_key="raw/good.fits"
        )
        _put_spectrum(table, "dp-nokey", observation_date_mjd=60000.4, sha256="s2", raw_s3_key=None)

        reader = _mock_fits_reader(n_points=3000)

        with (
            patch("generators.fits_reader.read_fits_spectrum", side_effect=reader),
            patch("generators.shared.trim_dead_edges", side_effect=_passthrough_clean),
            patch("generators.shared.remove_interior_dead_runs", side_effect=_passthrough_clean),
            patch("generators.shared.reject_chip_gap_artifacts", side_effect=_passthrough_clean),
            patch("generators.shared.segment_aware_lttb", side_effect=lambda wl, fx: (wl, fx)),
        ):
            result = run_compositing_sweep(_NOVA_ID, table, s3, _BUCKET_NAME)

        # Only 1 constituent → degenerate.
        assert result["degenerate"] == 1
        assert result["built"] == 0
