"""Unit tests for generators/references.py.

Uses moto to mock DynamoDB.  The generator module has no module-level
AWS calls, so it can be imported directly (no ``_load_module`` pattern
needed).

Covers:
  Happy path:
  - Two references resolved and returned in year-ascending order
  - ADS URL derived from bibcode
  - references_count set in nova_context
  - references_output set in nova_context for bundle generator

  Empty / missing:
  - Zero NovaReference items → empty references array, count = 0
  - Orphaned NovaReference (no matching Reference entity) → skipped
  - Missing optional fields (title, doi, arxiv_id) → null in output
  - Missing authors → empty list in output
  - Missing year → warning logged, reference sorted to end

  Sort order:
  - Year ascending with bibcode tiebreaker for same-year refs
  - Missing-year references sort after all dated references

  Schema:
  - Top-level schema_version, generated_at, nova_id present
  - generated_at is a valid ISO 8601 timestamp
"""

from __future__ import annotations

import re
from typing import Any

import boto3
import pytest
from generators.references import generate_references_json
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"

_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"

_BIBCODE_A = "1901MNRAS..61..337W"
_BIBCODE_B = "2013ATel.5297....1W"
_BIBCODE_C = "2020ApJ...901..123X"
_BIBCODE_ORPHAN = "9999FAKE..00..000Z"

_ADS_BASE = "https://ui.adsabs.harvard.edu/abs/"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set AWS env vars for boto3 in the moto context."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture()
def ddb(_aws_env: None) -> Any:
    """Create a mocked DynamoDB table and resource, yielded as a tuple."""
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
        yield table, dynamodb


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_nova_reference(table: Any, nova_id: str, bibcode: str) -> None:
    """Write a NovaReference link item."""
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": f"NOVAREF#{bibcode}",
            "entity_type": "NovaReference",
            "nova_id": nova_id,
            "bibcode": bibcode,
            "role": "OTHER",
        }
    )


def _seed_reference(
    table: Any,
    bibcode: str,
    *,
    year: int | None = 2020,
    title: str | None = "Test Title",
    authors: list[str] | None = None,
    doi: str | None = None,
    arxiv_id: str | None = None,
) -> None:
    """Write a Reference global item."""
    item: dict[str, Any] = {
        "PK": f"REFERENCE#{bibcode}",
        "SK": "METADATA",
        "entity_type": "Reference",
        "bibcode": bibcode,
    }
    if year is not None:
        item["year"] = year
    if title is not None:
        item["title"] = title
    if authors is not None:
        item["authors"] = authors
    if doi is not None:
        item["doi"] = doi
    if arxiv_id is not None:
        item["arxiv_id"] = arxiv_id
    table.put_item(Item=item)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_two_references_returned(self, ddb: Any) -> None:
        """Two linked references produce a two-element output array."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_B)
        _seed_reference(table, _BIBCODE_A, year=1901, title="Nova Persei")
        _seed_reference(table, _BIBCODE_B, year=2013, title="Nova Sco 2013")

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        assert len(artifact["references"]) == 2

    def test_ads_url_derived_from_bibcode(self, ddb: Any) -> None:
        """ads_url is constructed from the bibcode, not stored."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_reference(table, _BIBCODE_A, year=1901)

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        ref = artifact["references"][0]
        assert ref["ads_url"] == f"{_ADS_BASE}{_BIBCODE_A}"

    def test_references_count_in_context(self, ddb: Any) -> None:
        """references_count is set on nova_context."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_reference(table, _BIBCODE_A, year=1901)

        ctx: dict[str, Any] = {}
        generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        assert ctx["references_count"] == 1

    def test_references_output_in_context(self, ddb: Any) -> None:
        """references_output is stored for the bundle generator."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_reference(table, _BIBCODE_A, year=1901, title="Nova Persei")

        ctx: dict[str, Any] = {}
        generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        assert len(ctx["references_output"]) == 1
        assert ctx["references_output"][0]["bibcode"] == _BIBCODE_A

    def test_all_fields_mapped(self, ddb: Any) -> None:
        """All ADR-014 fields are present on the output record."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_reference(
            table,
            _BIBCODE_A,
            year=1901,
            title="Nova Persei",
            authors=["Williams, R."],
            doi="10.1093/mnras/61.5.337",
            arxiv_id="0001.0001",
        )

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        ref = artifact["references"][0]
        assert ref["bibcode"] == _BIBCODE_A
        assert ref["title"] == "Nova Persei"
        assert ref["authors"] == ["Williams, R."]
        assert ref["year"] == 1901
        assert ref["doi"] == "10.1093/mnras/61.5.337"
        assert ref["arxiv_id"] == "0001.0001"
        assert ref["ads_url"] == f"{_ADS_BASE}{_BIBCODE_A}"


# ---------------------------------------------------------------------------
# Empty / missing data
# ---------------------------------------------------------------------------


class TestEmptyAndMissing:
    def test_no_references_returns_empty_array(self, ddb: Any) -> None:
        """Nova with no NovaReference items → empty references list."""
        table, dynamodb = ddb
        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        assert artifact["references"] == []
        assert ctx["references_count"] == 0

    def test_orphaned_nova_reference_skipped(self, ddb: Any) -> None:
        """NovaReference without a matching Reference entity is omitted."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_ORPHAN)
        _seed_reference(table, _BIBCODE_A, year=1901)
        # No Reference entity for _BIBCODE_ORPHAN

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        assert len(artifact["references"]) == 1
        assert artifact["references"][0]["bibcode"] == _BIBCODE_A
        assert ctx["references_count"] == 1

    def test_missing_optional_fields_are_null(self, ddb: Any) -> None:
        """title, doi, arxiv_id default to None when absent."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        # Seed with no optional fields except year.
        _seed_reference(table, _BIBCODE_A, year=1901, title=None, authors=None)

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        ref = artifact["references"][0]
        assert ref["title"] is None
        assert ref["doi"] is None
        assert ref["arxiv_id"] is None

    def test_missing_authors_returns_empty_list(self, ddb: Any) -> None:
        """Missing authors field defaults to an empty list."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_reference(table, _BIBCODE_A, year=1901, authors=None)

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        assert artifact["references"][0]["authors"] == []

    def test_missing_year_is_null_in_output(self, ddb: Any) -> None:
        """Year is emitted as None (not 9999) when absent."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_reference(table, _BIBCODE_A, year=None)

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        assert artifact["references"][0]["year"] is None


# ---------------------------------------------------------------------------
# Sort order
# ---------------------------------------------------------------------------


class TestSortOrder:
    def test_year_ascending(self, ddb: Any) -> None:
        """References are sorted by year ascending."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_B)  # 2013
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)  # 1901
        _seed_reference(table, _BIBCODE_B, year=2013)
        _seed_reference(table, _BIBCODE_A, year=1901)

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        years = [r["year"] for r in artifact["references"]]
        assert years == [1901, 2013]

    def test_same_year_tiebreaker_by_bibcode(self, ddb: Any) -> None:
        """Same-year references use lexicographic bibcode tiebreaker."""
        table, dynamodb = ddb
        bc_z = "2020ZZZ...999..999Z"
        bc_a = "2020AAA...001..001A"
        _seed_nova_reference(table, _NOVA_ID, bc_z)
        _seed_nova_reference(table, _NOVA_ID, bc_a)
        _seed_reference(table, bc_z, year=2020)
        _seed_reference(table, bc_a, year=2020)

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        bibcodes = [r["bibcode"] for r in artifact["references"]]
        assert bibcodes == [bc_a, bc_z]

    def test_missing_year_sorts_to_end(self, ddb: Any) -> None:
        """References without a year appear after all dated references."""
        table, dynamodb = ddb
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_A)
        _seed_nova_reference(table, _NOVA_ID, _BIBCODE_B)
        _seed_reference(table, _BIBCODE_A, year=None)
        _seed_reference(table, _BIBCODE_B, year=2013)

        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        refs = artifact["references"]
        assert refs[0]["bibcode"] == _BIBCODE_B  # dated first
        assert refs[1]["bibcode"] == _BIBCODE_A  # no year → end


# ---------------------------------------------------------------------------
# Artifact schema
# ---------------------------------------------------------------------------


class TestArtifactSchema:
    def test_top_level_fields(self, ddb: Any) -> None:
        """Artifact has schema_version, generated_at, nova_id, references."""
        table, dynamodb = ddb
        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        assert artifact["schema_version"] == "1.0"
        assert artifact["nova_id"] == _NOVA_ID
        assert "generated_at" in artifact
        assert "references" in artifact

    def test_generated_at_is_iso_timestamp(self, ddb: Any) -> None:
        """generated_at matches YYYY-MM-DDTHH:MM:SSZ format."""
        table, dynamodb = ddb
        ctx: dict[str, Any] = {}
        artifact = generate_references_json(_NOVA_ID, table, dynamodb, ctx)

        ts = artifact["generated_at"]
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", ts)
