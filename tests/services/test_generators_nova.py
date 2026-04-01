"""Unit tests for generators/nova.py.

No AWS dependencies — the nova.json generator reads entirely from
nova_context.

Covers:
  Coordinate formatting:
  - RA/Dec converted to sexagesimal format
  - Format patterns match HH:MM:SS.ss and ±DD:MM:SS.s

  Discovery date:
  - Present date passed through unchanged
  - None passed through as null
  - Imprecise date (00 components) passed through as-is

  Nova type:
  - Absent → null
  - Present → passed through

  Observation counts:
  - Read from context, not computed
  - Default to 0 when absent

  Aliases:
  - Present list passed through
  - Missing → empty list

  Schema:
  - schema_version "1.0"
  - generated_at ISO timestamp
  - All required fields present
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from generators.nova import generate_nova_json

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _nova_item(
    *,
    primary_name: str = "GK Per",
    aliases: list[str] | None = None,
    ra_deg: float = 52.799083,
    dec_deg: float = 43.904667,
    discovery_date: str | None = "1901-02-21",
    nova_type: str | None = None,
) -> dict[str, Any]:
    """Build a minimal Nova DDB item dict."""
    item: dict[str, Any] = {
        "PK": "aaaaaaaa-0000-0000-0000-000000000001",
        "SK": "NOVA",
        "nova_id": "aaaaaaaa-0000-0000-0000-000000000001",
        "primary_name": primary_name,
        "ra_deg": Decimal(str(ra_deg)),
        "dec_deg": Decimal(str(dec_deg)),
        "status": "ACTIVE",
    }
    if aliases is not None:
        item["aliases"] = aliases
    if discovery_date is not None:
        item["discovery_date"] = discovery_date
    if nova_type is not None:
        item["nova_type"] = nova_type
    return item


def _make_context(
    nova: dict[str, Any],
    *,
    spectra_count: int = 24,
    photometry_count: int = 1840,
) -> dict[str, Any]:
    return {
        "nova_item": nova,
        "outburst_mjd": 51544.0,
        "outburst_mjd_is_estimated": False,
        "spectra_count": spectra_count,
        "photometry_count": photometry_count,
    }


_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# Coordinate formatting
# ---------------------------------------------------------------------------


class TestCoordinateFormatting:
    def test_ra_format(self) -> None:
        ctx = _make_context(_nova_item())
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert re.match(r"^\d{2}:\d{2}:\d{2}\.\d{2}$", artifact["ra"])

    def test_dec_format(self) -> None:
        ctx = _make_context(_nova_item())
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert re.match(r"^[+-]\d{2}:\d{2}:\d{2}\.\d$", artifact["dec"])

    def test_gk_per_ra_starts_with_03(self) -> None:
        """GK Per at RA ~52.8° → ~03h31m."""
        ctx = _make_context(_nova_item(ra_deg=52.799083))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["ra"].startswith("03:31:")

    def test_positive_dec(self) -> None:
        ctx = _make_context(_nova_item(dec_deg=43.904667))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["dec"].startswith("+43:")

    def test_negative_dec(self) -> None:
        ctx = _make_context(_nova_item(dec_deg=-30.5))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["dec"].startswith("-30:")


# ---------------------------------------------------------------------------
# Discovery date
# ---------------------------------------------------------------------------


class TestDiscoveryDate:
    def test_present_date_passed_through(self) -> None:
        ctx = _make_context(_nova_item(discovery_date="1901-02-21"))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["discovery_date"] == "1901-02-21"

    def test_none_passed_as_null(self) -> None:
        ctx = _make_context(_nova_item(discovery_date=None))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["discovery_date"] is None

    def test_imprecise_date_passed_as_is(self) -> None:
        """00 convention dates are not modified by the generator."""
        ctx = _make_context(_nova_item(discovery_date="1901-02-00"))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["discovery_date"] == "1901-02-00"


# ---------------------------------------------------------------------------
# Nova type
# ---------------------------------------------------------------------------


class TestNovaType:
    def test_absent_is_null(self) -> None:
        ctx = _make_context(_nova_item(nova_type=None))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["nova_type"] is None

    def test_present_passed_through(self) -> None:
        ctx = _make_context(_nova_item(nova_type="classical"))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["nova_type"] == "classical"


# ---------------------------------------------------------------------------
# Observation counts
# ---------------------------------------------------------------------------


class TestObservationCounts:
    def test_counts_from_context(self) -> None:
        ctx = _make_context(
            _nova_item(),
            spectra_count=24,
            photometry_count=1840,
        )
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["spectra_count"] == 24
        assert artifact["photometry_count"] == 1840

    def test_missing_counts_default_to_zero(self) -> None:
        ctx = {"nova_item": _nova_item()}
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["spectra_count"] == 0
        assert artifact["photometry_count"] == 0


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------


class TestAliases:
    def test_aliases_passed_through(self) -> None:
        ctx = _make_context(
            _nova_item(aliases=["Nova Per 1901", "V650 Per"]),
        )
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["aliases"] == ["Nova Per 1901", "V650 Per"]

    def test_missing_aliases_returns_empty_list(self) -> None:
        ctx = _make_context(_nova_item(aliases=None))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["aliases"] == []


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestSchema:
    def test_schema_version(self) -> None:
        ctx = _make_context(_nova_item())
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["schema_version"] == "1.0"

    def test_generated_at_format(self) -> None:
        ctx = _make_context(_nova_item())
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert re.match(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
            artifact["generated_at"],
        )

    def test_nova_id_in_output(self) -> None:
        ctx = _make_context(_nova_item())
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["nova_id"] == _NOVA_ID

    def test_primary_name_in_output(self) -> None:
        ctx = _make_context(_nova_item(primary_name="GK Per"))
        artifact = generate_nova_json(_NOVA_ID, ctx)
        assert artifact["primary_name"] == "GK Per"

    def test_all_required_fields_present(self) -> None:
        ctx = _make_context(_nova_item())
        artifact = generate_nova_json(_NOVA_ID, ctx)
        expected = {
            "schema_version",
            "generated_at",
            "nova_id",
            "primary_name",
            "aliases",
            "ra",
            "dec",
            "discovery_date",
            "nova_type",
            "spectra_count",
            "photometry_count",
        }
        assert set(artifact.keys()) == expected
