"""Unit tests for generators/catalog.py (DESIGN-003 §11).

The catalog generator is a pure function — it takes a DDB table resource
and a list of NovaResult objects, so no module-reload pattern is needed.
Moto provides the DDB table; NovaResult fixtures model the in-memory
sweep state.

Covers:
  Record assembly (§11.3):
  - DDB-only novae (not swept) use DDB counts
  - Swept + succeeded novae overlay in-memory counts
  - Swept + failed novae fall back to DDB counts
  - Newly ACTIVE novae never swept default to zero counts

  Output schema (§11.5, §11.9):
  - schema_version is "1.1"
  - discovery_date replaces discovery_year (string | null)
  - generated_at is ISO 8601 UTC

  Sort order (§11.6):
  - spectra_count descending
  - primary_name ascending as tiebreaker

  Stats block (§11.4):
  - nova_count, spectra_count, photometry_count aggregated correctly
  - references_count excluded from stats

  Edge cases (§11.7):
  - Zero ACTIVE novae → empty array, zero stats
  - Missing coordinates → excluded from catalog, not a fatal error
  - Missing discovery_date → null
  - Missing counts on DDB item → default to 0
  - Missing has_sparkline → default to False
  - DDB Decimal values handled correctly
"""

from __future__ import annotations

import re
from collections.abc import Generator
from decimal import Decimal
from typing import Any

import boto3
import pytest
from generators.catalog import generate_catalog_json
from moto import mock_aws

from contracts.models.regeneration import NovaResult

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"

_NOVA_A = "aaaaaaaa-0000-0000-0000-000000000001"
_NOVA_B = "bbbbbbbb-0000-0000-0000-000000000002"
_NOVA_C = "cccccccc-0000-0000-0000-000000000003"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal AWS env vars for moto."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture()
def table() -> Generator[Any, None, None]:
    """Create a moto-backed DDB table and yield it."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=_REGION)
        tbl = dynamodb.create_table(
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
        yield tbl


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_nova(
    table: Any,
    nova_id: str,
    *,
    primary_name: str = "Test Nova",
    aliases: list[str] | None = None,
    ra_deg: Decimal = Decimal("180.0"),
    dec_deg: Decimal = Decimal("45.0"),
    discovery_date: str | None = "2021-06-14",
    status: str = "ACTIVE",
    spectra_count: int | None = None,
    photometry_count: int | None = None,
    references_count: int | None = None,
    has_sparkline: bool | None = None,
) -> None:
    """Write a Nova item to the mocked table."""
    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": "NOVA",
        "nova_id": nova_id,
        "primary_name": primary_name,
        "aliases": aliases or [],
        "ra_deg": ra_deg,
        "dec_deg": dec_deg,
        "status": status,
    }
    if discovery_date is not None:
        item["discovery_date"] = discovery_date
    if spectra_count is not None:
        item["spectra_count"] = spectra_count
    if photometry_count is not None:
        item["photometry_count"] = photometry_count
    if references_count is not None:
        item["references_count"] = references_count
    if has_sparkline is not None:
        item["has_sparkline"] = has_sparkline
    table.put_item(Item=item)


def _nova_result(
    nova_id: str,
    *,
    success: bool = True,
    spectra_count: int | None = 5,
    photometry_count: int | None = 12,
    references_count: int | None = 3,
    has_sparkline: bool | None = True,
    error: str | None = None,
) -> NovaResult:
    """Build a NovaResult for the sweep overlay."""
    if not success:
        return NovaResult(
            nova_id=nova_id,
            success=False,
            error=error or "generator crashed",
        )
    return NovaResult(
        nova_id=nova_id,
        success=True,
        spectra_count=spectra_count,
        photometry_count=photometry_count,
        references_count=references_count,
        has_sparkline=has_sparkline,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_nova(catalog: dict[str, Any], nova_id: str) -> dict[str, Any]:
    """Find a nova record by ID in the catalog output."""
    for nova in catalog["novae"]:
        if nova["nova_id"] == nova_id:
            return dict(nova)
    raise AssertionError(f"Nova {nova_id} not found in catalog")


# ===========================================================================
# Output schema (§11.5, §11.9)
# ===========================================================================


class TestOutputSchema:
    """Top-level schema fields and versioning."""

    def test_schema_version_is_1_1(self, table: Any) -> None:
        """§11.9: schema_version bumped to '1.1'."""
        _seed_nova(table, _NOVA_A)
        result = generate_catalog_json([], table)
        assert result["schema_version"] == "1.1"

    def test_generated_at_is_iso_8601(self, table: Any) -> None:
        """generated_at matches YYYY-MM-DDTHH:MM:SSZ format."""
        _seed_nova(table, _NOVA_A)
        result = generate_catalog_json([], table)
        assert re.match(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
            result["generated_at"],
        )

    def test_has_stats_and_novae_keys(self, table: Any) -> None:
        _seed_nova(table, _NOVA_A)
        result = generate_catalog_json([], table)
        assert "stats" in result
        assert "novae" in result

    def test_discovery_date_replaces_discovery_year(self, table: Any) -> None:
        """§11.9: discovery_year is gone; discovery_date is present."""
        _seed_nova(table, _NOVA_A, discovery_date="2021-06-14")
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert "discovery_date" in nova
        assert "discovery_year" not in nova
        assert nova["discovery_date"] == "2021-06-14"

    def test_discovery_date_null_when_absent(self, table: Any) -> None:
        """§11.7: missing discovery_date emitted as null."""
        _seed_nova(table, _NOVA_A, discovery_date=None)
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert nova["discovery_date"] is None


# ===========================================================================
# Record assembly — DDB-only novae (§11.3)
# ===========================================================================


class TestDdbOnlyNovae:
    """Novae not in the sweep use DDB item values."""

    def test_uses_ddb_counts(self, table: Any) -> None:
        _seed_nova(
            table,
            _NOVA_A,
            spectra_count=10,
            photometry_count=200,
            references_count=4,
            has_sparkline=True,
        )
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert nova["spectra_count"] == 10
        assert nova["photometry_count"] == 200
        assert nova["references_count"] == 4
        assert nova["has_sparkline"] is True

    def test_missing_counts_default_to_zero(self, table: Any) -> None:
        """§11.3: never-swept novae with no count fields → 0/False."""
        _seed_nova(table, _NOVA_A)  # No count fields seeded.
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert nova["spectra_count"] == 0
        assert nova["photometry_count"] == 0
        assert nova["references_count"] == 0
        assert nova["has_sparkline"] is False

    def test_aliases_default_to_empty_list(self, table: Any) -> None:
        _seed_nova(table, _NOVA_A, aliases=[])
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert nova["aliases"] == []

    def test_aliases_passed_through(self, table: Any) -> None:
        _seed_nova(table, _NOVA_A, aliases=["Nova Her 2021", "V1674 Her"])
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert nova["aliases"] == ["Nova Her 2021", "V1674 Her"]


# ===========================================================================
# Record assembly — sweep overlay (§11.3)
# ===========================================================================


class TestSweepOverlay:
    """Succeeded swept novae overlay in-memory counts on DDB metadata."""

    def test_succeeded_nova_uses_in_memory_counts(self, table: Any) -> None:
        """In-memory counts replace DDB counts for succeeded novae."""
        _seed_nova(
            table,
            _NOVA_A,
            spectra_count=5,
            photometry_count=100,
            references_count=2,
            has_sparkline=False,
        )
        sweep = [
            _nova_result(
                _NOVA_A,
                spectra_count=8,
                photometry_count=150,
                references_count=4,
                has_sparkline=True,
            ),
        ]
        result = generate_catalog_json(sweep, table)
        nova = _find_nova(result, _NOVA_A)
        # In-memory values, not DDB values.
        assert nova["spectra_count"] == 8
        assert nova["photometry_count"] == 150
        assert nova["references_count"] == 4
        assert nova["has_sparkline"] is True

    def test_failed_nova_uses_ddb_counts(self, table: Any) -> None:
        """§11.3: failed swept novae fall back to DDB values."""
        _seed_nova(
            table,
            _NOVA_A,
            spectra_count=5,
            photometry_count=100,
            references_count=2,
            has_sparkline=False,
        )
        sweep = [_nova_result(_NOVA_A, success=False)]
        result = generate_catalog_json(sweep, table)
        nova = _find_nova(result, _NOVA_A)
        assert nova["spectra_count"] == 5
        assert nova["photometry_count"] == 100

    def test_metadata_always_from_ddb(self, table: Any) -> None:
        """Even when counts are overlaid, name/coords come from DDB."""
        _seed_nova(
            table,
            _NOVA_A,
            primary_name="V1674 Her",
            ra_deg=Decimal("284.379083"),
            dec_deg=Decimal("16.894333"),
        )
        sweep = [_nova_result(_NOVA_A, spectra_count=99)]
        result = generate_catalog_json(sweep, table)
        nova = _find_nova(result, _NOVA_A)
        assert nova["primary_name"] == "V1674 Her"
        # Coordinates should be sexagesimal, derived from the DDB degrees.
        assert nova["ra"].startswith("18:57:")
        assert nova["dec"].startswith("+16:53:")

    def test_partial_sweep_mixes_overlay_and_ddb(self, table: Any) -> None:
        """Two novae: one swept, one not. Each uses correct source."""
        _seed_nova(table, _NOVA_A, primary_name="Swept Nova", spectra_count=1)
        _seed_nova(table, _NOVA_B, primary_name="Unswepped Nova", spectra_count=10)
        sweep = [_nova_result(_NOVA_A, spectra_count=20)]
        result = generate_catalog_json(sweep, table)
        nova_a = _find_nova(result, _NOVA_A)
        nova_b = _find_nova(result, _NOVA_B)
        assert nova_a["spectra_count"] == 20  # overlay
        assert nova_b["spectra_count"] == 10  # DDB


# ===========================================================================
# Sort order (§11.6)
# ===========================================================================


class TestSortOrder:
    """spectra_count desc, primary_name asc tiebreaker."""

    def test_sorted_by_spectra_count_descending(self, table: Any) -> None:
        _seed_nova(table, _NOVA_A, primary_name="Low", spectra_count=2)
        _seed_nova(table, _NOVA_B, primary_name="High", spectra_count=20)
        _seed_nova(table, _NOVA_C, primary_name="Mid", spectra_count=10)
        result = generate_catalog_json([], table)
        names = [n["primary_name"] for n in result["novae"]]
        assert names == ["High", "Mid", "Low"]

    def test_tiebreaker_is_primary_name_ascending(self, table: Any) -> None:
        """Same spectra_count → alphabetical by primary_name."""
        _seed_nova(table, _NOVA_A, primary_name="Zeta Nova", spectra_count=5)
        _seed_nova(table, _NOVA_B, primary_name="Alpha Nova", spectra_count=5)
        _seed_nova(table, _NOVA_C, primary_name="Mu Nova", spectra_count=5)
        result = generate_catalog_json([], table)
        names = [n["primary_name"] for n in result["novae"]]
        assert names == ["Alpha Nova", "Mu Nova", "Zeta Nova"]

    def test_sort_stable_across_mixed_counts(self, table: Any) -> None:
        """Mixed: highest first, then tied pair in alpha order."""
        _seed_nova(table, _NOVA_A, primary_name="V1674 Her", spectra_count=31)
        _seed_nova(table, _NOVA_B, primary_name="RS Oph", spectra_count=5)
        _seed_nova(table, _NOVA_C, primary_name="GK Per", spectra_count=5)
        result = generate_catalog_json([], table)
        names = [n["primary_name"] for n in result["novae"]]
        assert names == ["V1674 Her", "GK Per", "RS Oph"]


# ===========================================================================
# Stats block (§11.4)
# ===========================================================================


class TestStatsBlock:
    """Aggregate counts for the homepage stats bar."""

    def test_stats_sums_across_all_novae(self, table: Any) -> None:
        _seed_nova(table, _NOVA_A, spectra_count=10, photometry_count=200)
        _seed_nova(table, _NOVA_B, spectra_count=5, photometry_count=100)
        result = generate_catalog_json([], table)
        stats = result["stats"]
        assert stats["nova_count"] == 2
        assert stats["spectra_count"] == 15
        assert stats["photometry_count"] == 300

    def test_stats_excludes_references_count(self, table: Any) -> None:
        """§11.4: references_count intentionally excluded from stats."""
        _seed_nova(table, _NOVA_A, references_count=10)
        result = generate_catalog_json([], table)
        assert "references_count" not in result["stats"]

    def test_stats_uses_overlay_for_swept_novae(self, table: Any) -> None:
        """Stats incorporate in-memory counts for succeeded novae."""
        _seed_nova(table, _NOVA_A, spectra_count=1, photometry_count=10)
        sweep = [_nova_result(_NOVA_A, spectra_count=50, photometry_count=500)]
        result = generate_catalog_json(sweep, table)
        assert result["stats"]["spectra_count"] == 50
        assert result["stats"]["photometry_count"] == 500

    def test_zero_novae_zero_stats(self, table: Any) -> None:
        """§11.7: empty catalog → zero-valued stats."""
        result = generate_catalog_json([], table)
        assert result["stats"]["nova_count"] == 0
        assert result["stats"]["spectra_count"] == 0
        assert result["stats"]["photometry_count"] == 0


# ===========================================================================
# Coordinate formatting (§5.3 via §11.5)
# ===========================================================================


class TestCoordinateFormatting:
    """RA/Dec converted from decimal degrees to sexagesimal."""

    def test_ra_format(self, table: Any) -> None:
        """RA in HH:MM:SS.ss format."""
        _seed_nova(table, _NOVA_A, ra_deg=Decimal("52.799083"), dec_deg=Decimal("43.904667"))
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert re.match(r"^\d{2}:\d{2}:\d{2}\.\d{2}$", nova["ra"])
        assert nova["ra"].startswith("03:31:")

    def test_dec_format(self, table: Any) -> None:
        """Dec in ±DD:MM:SS.s format."""
        _seed_nova(table, _NOVA_A, ra_deg=Decimal("52.799083"), dec_deg=Decimal("43.904667"))
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert re.match(r"^[+-]\d{2}:\d{2}:\d{2}\.\d$", nova["dec"])
        assert nova["dec"].startswith("+43:54:")


# ===========================================================================
# Edge cases (§11.7)
# ===========================================================================


class TestEdgeCases:
    """Error handling and boundary conditions."""

    def test_zero_active_novae(self, table: Any) -> None:
        """§11.7: empty catalog is valid."""
        result = generate_catalog_json([], table)
        assert result["novae"] == []
        assert result["stats"]["nova_count"] == 0

    def test_non_active_novae_excluded(self, table: Any) -> None:
        """Only ACTIVE novae appear; QUARANTINED/DEPRECATED are filtered."""
        _seed_nova(table, _NOVA_A, status="ACTIVE", primary_name="Active")
        _seed_nova(table, _NOVA_B, status="QUARANTINED", primary_name="Quarantined")
        result = generate_catalog_json([], table)
        assert len(result["novae"]) == 1
        assert result["novae"][0]["primary_name"] == "Active"

    def test_missing_coordinates_excluded(self, table: Any) -> None:
        """§11.7: nova with missing RA/DEC excluded, does not crash."""
        # Seed a normal nova and one with missing coords.
        _seed_nova(table, _NOVA_A, primary_name="Good Nova")
        # Write the bad nova manually (bypassing _seed_nova's defaults).
        table.put_item(
            Item={
                "PK": _NOVA_B,
                "SK": "NOVA",
                "nova_id": _NOVA_B,
                "primary_name": "Bad Nova",
                "aliases": [],
                "status": "ACTIVE",
                # No ra_deg or dec_deg.
            }
        )
        result = generate_catalog_json([], table)
        assert len(result["novae"]) == 1
        assert result["novae"][0]["primary_name"] == "Good Nova"

    def test_missing_coordinates_does_not_fail_entire_catalog(self, table: Any) -> None:
        """One bad nova doesn't suppress others."""
        _seed_nova(table, _NOVA_A, primary_name="Good A")
        _seed_nova(table, _NOVA_C, primary_name="Good C")
        table.put_item(
            Item={
                "PK": _NOVA_B,
                "SK": "NOVA",
                "nova_id": _NOVA_B,
                "primary_name": "Bad",
                "aliases": [],
                "status": "ACTIVE",
            }
        )
        result = generate_catalog_json([], table)
        assert len(result["novae"]) == 2
        ids = {n["nova_id"] for n in result["novae"]}
        assert _NOVA_A in ids
        assert _NOVA_C in ids
        assert _NOVA_B not in ids

    def test_decimal_values_handled(self, table: Any) -> None:
        """DDB returns Decimal for numeric fields — no crash."""
        table.put_item(
            Item={
                "PK": _NOVA_A,
                "SK": "NOVA",
                "nova_id": _NOVA_A,
                "primary_name": "Decimal Nova",
                "aliases": [],
                "ra_deg": Decimal("123.456"),
                "dec_deg": Decimal("-45.678"),
                "status": "ACTIVE",
                "spectra_count": Decimal("7"),
                "photometry_count": Decimal("42"),
                "references_count": Decimal("3"),
                "has_sparkline": True,
            }
        )
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert nova["spectra_count"] == 7
        assert nova["photometry_count"] == 42
        assert nova["references_count"] == 3

    def test_sweep_result_for_nova_not_in_ddb_ignored(self, table: Any) -> None:
        """A sweep result for a nova that isn't ACTIVE is harmless.

        This can happen if a nova was deactivated between plan creation
        and catalog generation.
        """
        _seed_nova(table, _NOVA_A, primary_name="Still Active")
        # Sweep result for NOVA_B which is not in DDB at all.
        sweep = [_nova_result(_NOVA_B, spectra_count=99)]
        result = generate_catalog_json(sweep, table)
        assert len(result["novae"]) == 1
        assert result["novae"][0]["nova_id"] == _NOVA_A


# ===========================================================================
# Nova record field completeness
# ===========================================================================


class TestRecordFields:
    """Every expected field is present on each nova record."""

    def test_all_fields_present(self, table: Any) -> None:
        _seed_nova(
            table,
            _NOVA_A,
            primary_name="V1674 Her",
            aliases=["Nova Her 2021"],
            ra_deg=Decimal("284.379083"),
            dec_deg=Decimal("16.894333"),
            discovery_date="2021-06-12",
            spectra_count=31,
            photometry_count=1200,
            references_count=8,
            has_sparkline=True,
        )
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)

        expected_keys = {
            "nova_id",
            "primary_name",
            "aliases",
            "ra",
            "dec",
            "discovery_date",
            "spectra_count",
            "photometry_count",
            "references_count",
            "has_sparkline",
        }
        assert set(nova.keys()) == expected_keys

    def test_no_extra_fields(self, table: Any) -> None:
        """Output does not leak DDB internal fields (PK, SK, status)."""
        _seed_nova(table, _NOVA_A)
        result = generate_catalog_json([], table)
        nova = _find_nova(result, _NOVA_A)
        assert "PK" not in nova
        assert "SK" not in nova
        assert "status" not in nova
        assert "ra_deg" not in nova
        assert "dec_deg" not in nova
