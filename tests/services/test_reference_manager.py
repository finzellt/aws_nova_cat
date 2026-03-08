"""
Unit tests for services/reference_manager/handler.py

Follows the same scaffold as all other NovaCat service unit tests:
  - aws_env fixture (autouse) sets env vars
  - table fixture creates the DDB table inside mock_aws()
  - _load_handler() deletes cached modules then re-imports fresh
  - All test methods wrap handler calls in `with mock_aws()`
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from collections.abc import Generator
from typing import Any, cast
from unittest.mock import MagicMock, patch

import boto3
import pytest
from boto3.dynamodb.conditions import Key
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_NOVA_ID = "aaaaaaaa-1111-2222-3333-444444444444"
_BIBCODE_A = "2013ATel.5073....1S"  # 2013-06-00 — later
_BIBCODE_B = "1992IAUC.5608....1W"  # 1992-01-00 — earlier
_BIBCODE_C = "2013ApJ...779..118M"  # no date in some tests
_ADS_SECRET_VALUE = json.dumps({"token": "test-ads-token-abc123"})

# Minimal raw ADS doc as it arrives from the API (date has time component)
_RAW_DOC_A = {
    "bibcode": _BIBCODE_A,
    "doctype": "telegram",
    "title": ["Discovery of Nova V1324 Sco"],
    "date": "2013-06-01T00:00:00Z",
    "author": ["Stanek, K. Z.", "Kochanek, C. S."],
    "doi": None,
    "identifier": ["arXiv:1307.0011"],
}

_RAW_DOC_B = {
    "bibcode": _BIBCODE_B,
    "doctype": "circular",
    "title": ["Nova V1324 Sco — IAUC notice"],
    "date": "1992-01-01T00:00:00Z",
    "author": ["Williams, R."],
    "doi": None,
    "identifier": [],
}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", "nova-cat-private-test")
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-public-test")
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:test-topic"
    )
    monkeypatch.setenv("ADS_SECRET_NAME", "ADSQueryToken")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def table(aws_env: None) -> Generator[Any, None, None]:
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb.create_table(
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
        yield ddb.Table(_TABLE_NAME)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_ads_secret() -> None:
    sm = boto3.client("secretsmanager", region_name="us-east-1")
    sm.create_secret(Name="ADSQueryToken", SecretString=_ADS_SECRET_VALUE)


def _load_handler() -> types.ModuleType:
    if "reference_manager.handler" in sys.modules:
        del sys.modules["reference_manager.handler"]
    return importlib.import_module("reference_manager.handler")


def _base_event(**kwargs: Any) -> dict[str, Any]:
    base = {
        "task_name": "FetchReferenceCandidates",
        "workflow_name": "refresh_references",
        "nova_id": _NOVA_ID,
        "correlation_id": "corr-ref-001",
    }
    base.update(kwargs)
    return base


def _seed_nova(table: Any, nova_id: str = _NOVA_ID, aliases: list[str] | None = None) -> None:
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": "NOVA",
            "entity_type": "Nova",
            "schema_version": "1.0.0",
            "nova_id": nova_id,
            "primary_name": "V1324 Sco",
            "primary_name_normalized": "v1324 sco",
            "status": "ACTIVE",
            "aliases": aliases if aliases is not None else ["NOVA Sco 2012", "Gaia DR3 1234567890"],
        }
    )


def _seed_reference(table: Any, bibcode: str, publication_date: str | None = None) -> None:
    item: dict[str, Any] = {
        "PK": f"REFERENCE#{bibcode}",
        "SK": "METADATA",
        "entity_type": "Reference",
        "schema_version": "1.0.0",
        "bibcode": bibcode,
        "reference_type": "journal_article",
        "authors": [],
        "created_at": "2024-01-01T00:00:00+00:00",
        "updated_at": "2024-01-01T00:00:00+00:00",
    }
    if publication_date is not None:
        item["publication_date"] = publication_date
    table.put_item(Item=item)


def _seed_novaref(table: Any, nova_id: str, bibcode: str) -> None:
    table.put_item(
        Item={
            "PK": nova_id,
            "SK": f"NOVAREF#{bibcode}",
            "entity_type": "NovaReference",
            "schema_version": "1.0.0",
            "nova_id": nova_id,
            "bibcode": bibcode,
            "role": "OTHER",
            "created_at": "2024-01-01T00:00:00+00:00",
            "updated_at": "2024-01-01T00:00:00+00:00",
        }
    )


def _mock_ads_response(docs: list[dict[str, Any]]) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"response": {"docs": docs}}
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


# ===========================================================================
# TestFetchReferenceCandidates
# ===========================================================================


class TestFetchReferenceCandidates:
    def test_returns_candidates_from_ads(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            _create_ads_secret()
            h = _load_handler()
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([_RAW_DOC_A])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                result = h.handle(_base_event(), None)
            assert result["nova_id"] == _NOVA_ID
            assert result["candidate_count"] == 1
            assert result["candidates"][0]["bibcode"] == _BIBCODE_A

    def test_query_includes_primary_name(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table, aliases=[])
            _create_ads_secret()
            h = _load_handler()
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                h.handle(_base_event(), None)
            url = mock_requests.get.call_args[0][0]
            assert "V1324" in url and "Sco" in url

    def test_query_includes_aliases(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table, aliases=["NOVA Sco 2012"])
            _create_ads_secret()
            h = _load_handler()
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                h.handle(_base_event(), None)
            url = mock_requests.get.call_args[0][0]
            assert "NOVA" in url and "Sco" in url and "2012" in url

    def test_query_includes_ads_name_hints(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table, aliases=[])
            _create_ads_secret()
            h = _load_handler()
            event = _base_event(attributes={"ads_name_hints": ["V1324Sco"]})
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                h.handle(event, None)
            url = mock_requests.get.call_args[0][0]
            assert "V1324Sco" in url

    def test_deduplicates_names_case_insensitively(self, table: Any) -> None:
        with mock_aws():
            # primary_name "V1324 Sco" and alias "v1324 sco" are the same normalized
            _seed_nova(table, aliases=["v1324 sco"])
            _create_ads_secret()
            h = _load_handler()
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                h.handle(_base_event(), None)
            url = mock_requests.get.call_args[0][0]
            # urlencode encodes spaces as + and quotes as %22
            # dedupe means "V1324 Sco" appears exactly once in either casing
            assert url.count("V1324") + url.count("v1324") == 1

    def test_uses_bearer_token_from_secret(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            _create_ads_secret()
            h = _load_handler()
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                h.handle(_base_event(), None)
            auth_header = mock_requests.get.call_args[1]["headers"]["Authorization"]
            assert auth_header == "Bearer test-ads-token-abc123"

    def test_ads_empty_response_returns_zero_candidates(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            _create_ads_secret()
            h = _load_handler()
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                result = h.handle(_base_event(), None)
            assert result["candidate_count"] == 0
            assert result["candidates"] == []

    def test_nova_not_found_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            _create_ads_secret()
            h = _load_handler()
            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="Nova not found"):
                h.handle(_base_event(), None)

    def test_http_429_raises_retryable_error(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            _create_ads_secret()
            h = _load_handler()
            from nova_common.errors import RetryableError

            mock_resp = MagicMock()
            mock_resp.status_code = 429
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = mock_resp
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                with pytest.raises(RetryableError, match="429"):
                    h.handle(_base_event(), None)

    def test_http_401_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            _create_ads_secret()
            h = _load_handler()
            from nova_common.errors import TerminalError

            mock_resp = MagicMock()
            mock_resp.status_code = 401
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = mock_resp
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                with pytest.raises(TerminalError, match="authentication failed"):
                    h.handle(_base_event(), None)

    def test_http_500_raises_retryable_error(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            _create_ads_secret()
            h = _load_handler()
            from nova_common.errors import RetryableError

            mock_resp = MagicMock()
            mock_resp.status_code = 500
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = mock_resp
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                with pytest.raises(RetryableError, match="server error"):
                    h.handle(_base_event(), None)

    def test_missing_nova_id_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            _create_ads_secret()
            h = _load_handler()
            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="nova_id"):
                h.handle(_base_event(nova_id=None), None)


# ===========================================================================
# TestNormalizeReference
# ===========================================================================


class TestNormalizeReference:
    def _run(self, table: Any, doc: dict[str, Any]) -> dict[str, Any]:
        with mock_aws():
            h = _load_handler()
            event = {**doc, "task_name": "NormalizeReference", "nova_id": _NOVA_ID}
            return cast(dict[str, Any], h.handle(event, None))

    def test_maps_telegram_doctype_to_atel(self, table: Any) -> None:
        result = self._run(table, _RAW_DOC_A)
        assert result["reference_type"] == "atel"

    def test_maps_circular_doctype_to_cbat(self, table: Any) -> None:
        result = self._run(table, _RAW_DOC_B)
        assert result["reference_type"] == "cbat_circular"

    @pytest.mark.parametrize(
        "doctype,expected",
        [
            ("article", "journal_article"),
            ("eprint", "arxiv_preprint"),
            ("inproceedings", "conference_abstract"),
            ("abstract", "conference_abstract"),
            ("catalog", "catalog"),
            ("software", "software"),
            ("unknown_thing", "other"),
            (None, "other"),
        ],
    )
    def test_doctype_mapping(self, table: Any, doctype: str | None, expected: str) -> None:
        doc = {**_RAW_DOC_A, "bibcode": "2000Test.0001....A", "doctype": doctype}
        result = self._run(table, doc)
        assert result["reference_type"] == expected

    def test_returns_bibcode(self, table: Any) -> None:
        result = self._run(table, _RAW_DOC_A)
        assert result["bibcode"] == _BIBCODE_A

    def test_passes_nova_id_through(self, table: Any) -> None:
        result = self._run(table, _RAW_DOC_A)
        assert result["nova_id"] == _NOVA_ID

    def test_title_extracted_from_list(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "title": ["First Title", "Second Title"]}
        result = self._run(table, doc)
        assert result["title"] == "First Title"

    def test_title_from_string(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "title": "Plain string title"}
        result = self._run(table, doc)
        assert result["title"] == "Plain string title"

    def test_title_none_when_missing(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "title": None}
        result = self._run(table, doc)
        assert result["title"] is None

    def test_doi_extracted_from_list(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "doi": ["10.1234/test.2013"]}
        result = self._run(table, doc)
        assert result["doi"] == "10.1234/test.2013"

    def test_doi_from_string(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "doi": "10.1234/test.2013"}
        result = self._run(table, doc)
        assert result["doi"] == "10.1234/test.2013"

    def test_doi_none_when_absent(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "doi": None}
        result = self._run(table, doc)
        assert result["doi"] is None

    def test_authors_preserved(self, table: Any) -> None:
        result = self._run(table, _RAW_DOC_A)
        assert result["authors"] == ["Stanek, K. Z.", "Kochanek, C. S."]

    def test_authors_empty_list_when_missing(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "author": None}
        result = self._run(table, doc)
        assert result["authors"] == []

    def test_publication_date_strips_time_and_day(self, table: Any) -> None:
        """ADS YYYY-MM-01T00:00:00Z → YYYY-MM-00."""
        doc = {**_RAW_DOC_A, "date": "2013-06-01T00:00:00Z"}
        result = self._run(table, doc)
        assert result["publication_date"] == "2013-06-00"

    def test_publication_date_full_date_also_discards_day(self, table: Any) -> None:
        """Any day value — not just 01 — is discarded."""
        doc = {**_RAW_DOC_A, "date": "2013-06-14T00:00:00Z"}
        result = self._run(table, doc)
        assert result["publication_date"] == "2013-06-00"

    def test_publication_date_bare_yyyy_mm_dd(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "date": "2013-06-14"}
        result = self._run(table, doc)
        assert result["publication_date"] == "2013-06-00"

    def test_publication_date_none_when_missing(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "date": None}
        result = self._run(table, doc)
        assert result["publication_date"] is None

    def test_year_derived_from_publication_date(self, table: Any) -> None:
        result = self._run(table, _RAW_DOC_A)
        assert result["year"] == 2013

    def test_year_none_when_no_date(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "date": None}
        result = self._run(table, doc)
        assert result["year"] is None

    def test_arxiv_id_stripped_of_prefix(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "identifier": ["arXiv:1307.0011"]}
        result = self._run(table, doc)
        assert result["arxiv_id"] == "1307.0011"

    def test_arxiv_id_case_insensitive_strip(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "identifier": ["ARXIV:1307.0011"]}
        result = self._run(table, doc)
        assert result["arxiv_id"] == "1307.0011"

    def test_arxiv_id_none_when_absent(self, table: Any) -> None:
        doc = {**_RAW_DOC_A, "identifier": []}
        result = self._run(table, doc)
        assert result["arxiv_id"] is None

    def test_missing_bibcode_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            from nova_common.errors import TerminalError

            event = {
                **_RAW_DOC_A,
                "bibcode": None,
                "task_name": "NormalizeReference",
                "nova_id": _NOVA_ID,
            }
            with pytest.raises(TerminalError, match="bibcode"):
                h.handle(event, None)


# ===========================================================================
# TestUpsertReferenceEntity
# ===========================================================================


class TestUpsertReferenceEntity:
    def _event(self, **overrides: Any) -> dict[str, Any]:
        base = {
            "task_name": "UpsertReferenceEntity",
            "workflow_name": "refresh_references",
            "nova_id": _NOVA_ID,
            "bibcode": _BIBCODE_A,
            "reference_type": "atel",
            "title": "Discovery of Nova V1324 Sco",
            "year": 2013,
            "publication_date": "2013-06-00",
            "authors": ["Stanek, K. Z."],
            "doi": None,
            "arxiv_id": "1307.0011",
        }
        base.update(overrides)
        return base

    def test_writes_reference_item_to_ddb(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            h.handle(self._event(), None)
            item = table.get_item(Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"})["Item"]
            assert item["bibcode"] == _BIBCODE_A
            assert item["reference_type"] == "atel"
            assert item["publication_date"] == "2013-06-00"
            assert item["year"] == 2013
            assert item["arxiv_id"] == "1307.0011"

    def test_sets_entity_type(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            h.handle(self._event(), None)
            item = table.get_item(Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"})["Item"]
            assert item["entity_type"] == "Reference"

    def test_sets_created_at_and_updated_at(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            h.handle(self._event(), None)
            item = table.get_item(Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"})["Item"]
            assert "created_at" in item
            assert "updated_at" in item

    def test_preserves_created_at_on_update(self, table: Any) -> None:
        with mock_aws():
            _seed_reference(table, _BIBCODE_A)
            original_created = table.get_item(
                Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"}
            )["Item"]["created_at"]
            h = _load_handler()
            h.handle(self._event(title="Updated Title"), None)
            item = table.get_item(Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"})["Item"]
            assert item["created_at"] == original_created
            assert item["title"] == "Updated Title"

    def test_omits_none_valued_optional_fields(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            h.handle(self._event(doi=None, arxiv_id=None), None)
            item = table.get_item(Key={"PK": f"REFERENCE#{_BIBCODE_A}", "SK": "METADATA"})["Item"]
            assert "doi" not in item
            assert "arxiv_id" not in item

    def test_returns_bibcode_and_publication_date(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["bibcode"] == _BIBCODE_A
            assert result["publication_date"] == "2013-06-00"

    def test_passes_nova_id_through(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["nova_id"] == _NOVA_ID

    def test_missing_bibcode_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="bibcode"):
                h.handle(self._event(bibcode=None), None)

    def test_idempotent_on_second_call(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            h.handle(self._event(), None)
            result = h.handle(self._event(), None)
            assert result["bibcode"] == _BIBCODE_A


# ===========================================================================
# TestLinkNovaReference
# ===========================================================================


class TestLinkNovaReference:
    def _event(self, **overrides: Any) -> dict[str, Any]:
        base = {
            "task_name": "LinkNovaReference",
            "workflow_name": "refresh_references",
            "nova_id": _NOVA_ID,
            "bibcode": _BIBCODE_A,
            "publication_date": "2013-06-00",
        }
        base.update(overrides)
        return base

    def test_writes_novaref_item(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            h.handle(self._event(), None)
            item = table.get_item(Key={"PK": _NOVA_ID, "SK": f"NOVAREF#{_BIBCODE_A}"})["Item"]
            assert item["nova_id"] == _NOVA_ID
            assert item["bibcode"] == _BIBCODE_A
            assert item["role"] == "OTHER"
            assert item["added_by_workflow"] == "refresh_references"

    def test_returns_linked_true(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["linked"] is True

    def test_returns_bibcode_and_publication_date(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["bibcode"] == _BIBCODE_A
            assert result["publication_date"] == "2013-06-00"

    def test_returns_nova_id(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["nova_id"] == _NOVA_ID

    def test_idempotent_second_call_does_not_raise(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            h.handle(self._event(), None)
            result = h.handle(self._event(), None)
            assert result["linked"] is True

    def test_idempotent_second_call_leaves_one_item(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            h.handle(self._event(), None)
            h.handle(self._event(), None)
            resp = table.query(
                KeyConditionExpression=Key("PK").eq(_NOVA_ID) & Key("SK").begins_with("NOVAREF#")
            )
            assert len(resp["Items"]) == 1

    def test_missing_nova_id_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="Missing required fields"):
                h.handle(self._event(nova_id=None), None)

    def test_missing_bibcode_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="Missing required fields"):
                h.handle(self._event(bibcode=None), None)


# ===========================================================================
# TestComputeDiscoveryDate
# ===========================================================================


class TestComputeDiscoveryDate:
    def _event(self, **overrides: Any) -> dict[str, Any]:
        base = {
            "task_name": "ComputeDiscoveryDate",
            "workflow_name": "refresh_references",
            "nova_id": _NOVA_ID,
        }
        base.update(overrides)
        return base

    def test_returns_none_when_no_references(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["earliest_bibcode"] is None
            assert result["earliest_publication_date"] is None

    def test_returns_single_reference(self, table: Any) -> None:
        with mock_aws():
            _seed_novaref(table, _NOVA_ID, _BIBCODE_A)
            _seed_reference(table, _BIBCODE_A, "2013-06-00")
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["earliest_bibcode"] == _BIBCODE_A
            assert result["earliest_publication_date"] == "2013-06-00"

    def test_picks_earlier_of_two_references(self, table: Any) -> None:
        with mock_aws():
            _seed_novaref(table, _NOVA_ID, _BIBCODE_A)
            _seed_novaref(table, _NOVA_ID, _BIBCODE_B)
            _seed_reference(table, _BIBCODE_A, "2013-06-00")
            _seed_reference(table, _BIBCODE_B, "1992-01-00")
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["earliest_bibcode"] == _BIBCODE_B
            assert result["earliest_publication_date"] == "1992-01-00"

    def test_tiebreaker_uses_lexicographically_smaller_bibcode(self, table: Any) -> None:
        bib_alpha = "2013ApJ.AAAA"
        bib_omega = "2013ApJ.ZZZZ"
        with mock_aws():
            _seed_novaref(table, _NOVA_ID, bib_alpha)
            _seed_novaref(table, _NOVA_ID, bib_omega)
            _seed_reference(table, bib_alpha, "2013-06-00")
            _seed_reference(table, bib_omega, "2013-06-00")
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["earliest_bibcode"] == bib_alpha

    def test_references_without_dates_are_excluded(self, table: Any) -> None:
        with mock_aws():
            _seed_novaref(table, _NOVA_ID, _BIBCODE_A)
            _seed_novaref(table, _NOVA_ID, _BIBCODE_C)
            _seed_reference(table, _BIBCODE_A, "2013-06-00")
            _seed_reference(table, _BIBCODE_C, None)  # no date
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["earliest_bibcode"] == _BIBCODE_A

    def test_all_undated_returns_none(self, table: Any) -> None:
        with mock_aws():
            _seed_novaref(table, _NOVA_ID, _BIBCODE_A)
            _seed_reference(table, _BIBCODE_A, None)
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["earliest_bibcode"] is None
            assert result["earliest_publication_date"] is None

    def test_returns_nova_id(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["nova_id"] == _NOVA_ID

    def test_missing_nova_id_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="nova_id"):
                h.handle(self._event(nova_id=None), None)


# ===========================================================================
# TestUpsertDiscoveryDateMetadata
# ===========================================================================


class TestUpsertDiscoveryDateMetadata:
    def _event(self, **overrides: Any) -> dict[str, Any]:
        base = {
            "task_name": "UpsertDiscoveryDateMetadata",
            "workflow_name": "refresh_references",
            "nova_id": _NOVA_ID,
            "earliest_bibcode": _BIBCODE_A,
            "earliest_publication_date": "2013-06-00",
        }
        base.update(overrides)
        return base

    def test_writes_discovery_date_to_nova(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            h = _load_handler()
            h.handle(self._event(), None)
            nova = table.get_item(Key={"PK": _NOVA_ID, "SK": "NOVA"})["Item"]
            assert nova["discovery_date"] == "2013-06-00"

    def test_returns_updated_true_when_written(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            h = _load_handler()
            result = h.handle(self._event(), None)
            assert result["updated"] is True
            assert result["discovery_date"] == "2013-06-00"

    def test_updates_when_new_date_is_earlier(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            table.update_item(
                Key={"PK": _NOVA_ID, "SK": "NOVA"},
                UpdateExpression="SET discovery_date = :d",
                ExpressionAttributeValues={":d": "2014-03-00"},
            )
            h = _load_handler()
            result = h.handle(self._event(earliest_publication_date="2013-06-00"), None)
            assert result["updated"] is True
            nova = table.get_item(Key={"PK": _NOVA_ID, "SK": "NOVA"})["Item"]
            assert nova["discovery_date"] == "2013-06-00"

    def test_noop_when_new_date_is_same(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            table.update_item(
                Key={"PK": _NOVA_ID, "SK": "NOVA"},
                UpdateExpression="SET discovery_date = :d",
                ExpressionAttributeValues={":d": "2013-06-00"},
            )
            h = _load_handler()
            result = h.handle(self._event(earliest_publication_date="2013-06-00"), None)
            assert result["updated"] is False
            assert result["discovery_date"] == "2013-06-00"

    def test_noop_when_new_date_is_later(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            table.update_item(
                Key={"PK": _NOVA_ID, "SK": "NOVA"},
                UpdateExpression="SET discovery_date = :d",
                ExpressionAttributeValues={":d": "2013-06-00"},
            )
            h = _load_handler()
            result = h.handle(self._event(earliest_publication_date="2015-01-00"), None)
            assert result["updated"] is False
            nova = table.get_item(Key={"PK": _NOVA_ID, "SK": "NOVA"})["Item"]
            assert nova["discovery_date"] == "2013-06-00"  # unchanged

    def test_noop_when_no_date_computed(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            h = _load_handler()
            result = h.handle(
                self._event(earliest_publication_date=None, earliest_bibcode=None),
                None,
            )
            assert result["updated"] is False
            assert result["discovery_date"] is None

    def test_records_old_date_in_result(self, table: Any) -> None:
        with mock_aws():
            _seed_nova(table)
            table.update_item(
                Key={"PK": _NOVA_ID, "SK": "NOVA"},
                UpdateExpression="SET discovery_date = :d",
                ExpressionAttributeValues={":d": "2014-03-00"},
            )
            h = _load_handler()
            result = h.handle(self._event(earliest_publication_date="2013-06-00"), None)
            assert result["discovery_date_old"] == "2014-03-00"

    def test_first_write_has_no_old_date_key(self, table: Any) -> None:
        """When there was no prior discovery_date, discovery_date_old should not appear."""
        with mock_aws():
            _seed_nova(table)
            h = _load_handler()
            result = h.handle(self._event(), None)
            # discovery_date_old is only set when overwriting an existing value
            assert result.get("discovery_date_old") is None

    def test_nova_not_found_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="Nova not found"):
                h.handle(self._event(), None)

    def test_missing_nova_id_raises_terminal_error(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            from nova_common.errors import TerminalError

            with pytest.raises(TerminalError, match="nova_id"):
                h.handle(self._event(nova_id=None), None)


# ===========================================================================
# TestPublicationDateNormalization
# ===========================================================================


class TestPublicationDateNormalization:
    @pytest.mark.parametrize(
        "ads_date,expected",
        [
            ("2013-06-01T00:00:00Z", "2013-06-00"),  # standard ADS format
            ("1992-01-01T00:00:00Z", "1992-01-00"),  # older record
            ("2013-06-14T00:00:00Z", "2013-06-00"),  # non-01 day still discarded
            ("2013-06-14", "2013-06-00"),  # bare date, no time component
            ("2013-06-00", "2013-06-00"),  # already canonical
            (None, None),
            ("", None),
            ("2013-00-01T00:00:00Z", None),  # month 00 is invalid
            ("bad-date", None),
        ],
    )
    def test_normalize_publication_date(
        self, table: Any, ads_date: str | None, expected: str | None
    ) -> None:
        with mock_aws():
            h = _load_handler()
            result = h._normalize_publication_date(ads_date)
            assert result == expected


# ===========================================================================
# TestArxivIdExtraction
# ===========================================================================


class TestArxivIdExtraction:
    @pytest.mark.parametrize(
        "identifiers,expected",
        [
            (["arXiv:1307.0011"], "1307.0011"),
            (["ARXIV:1307.0011v2"], "1307.0011v2"),
            (["arXiv:2101.12345.6789"], "2101.12345.6789"),
            (["10.1234/doi", "arXiv:1307.0011"], "1307.0011"),  # arxiv after doi
            (["10.1234/doi"], None),
            ([], None),
            (None, None),
        ],
    )
    def test_extract_arxiv_id(
        self, table: Any, identifiers: list[str] | None, expected: str | None
    ) -> None:
        with mock_aws():
            h = _load_handler()
            result = h._extract_arxiv_id(identifiers)
            assert result == expected


# ===========================================================================
# TestDispatch
# ===========================================================================


class TestDispatch:
    def test_unknown_task_raises_value_error(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            with pytest.raises(ValueError, match="Unknown task_name"):
                h.handle(_base_event(task_name="NonExistentTask"), None)

    def test_all_six_task_names_are_registered(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            expected = {
                "FetchReferenceCandidates",
                "NormalizeReference",
                "UpsertReferenceEntity",
                "LinkNovaReference",
                "ComputeDiscoveryDate",
                "UpsertDiscoveryDateMetadata",
            }
            assert set(h._TASK_HANDLERS.keys()) == expected

    def test_all_handlers_are_callable(self, table: Any) -> None:
        with mock_aws():
            h = _load_handler()
            for name, fn in h._TASK_HANDLERS.items():
                assert callable(fn), f"Handler for {name!r} is not callable"

    # ---------------------------------------------------------------------------
    # Additions to tests/services/test_reference_manager.py
    #
    # Add these two test methods to the existing TestFetchReferenceCandidates class.
    # ---------------------------------------------------------------------------

    def test_network_timeout_raises_retryable_error(self, table: Any) -> None:
        """
        A requests.exceptions.Timeout during the ADS call should raise
        RetryableError — it is not a terminal failure.
        """
        with mock_aws():
            _seed_nova(table)
            _create_ads_secret()
            h = _load_handler()
            from nova_common.errors import RetryableError

            with patch.object(h, "requests") as mock_requests:
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                mock_requests.get.side_effect = mock_requests.exceptions.Timeout("timed out")
                with pytest.raises(RetryableError, match="timed out"):
                    h.handle(_base_event(), None)

    def test_non_list_aliases_in_ddb_are_handled_gracefully(self, table: Any) -> None:
        """
        If the Nova item in DDB has a non-list value for `aliases` (e.g. due to
        a schema migration or manual edit), FetchReferenceCandidates should
        fall back to an empty alias list and still query ADS using the
        primary_name alone, rather than raising.
        """
        with mock_aws():
            # Seed nova with aliases as a string instead of a list
            table.put_item(
                Item={
                    "PK": _NOVA_ID,
                    "SK": "NOVA",
                    "entity_type": "Nova",
                    "schema_version": "1.0.0",
                    "nova_id": _NOVA_ID,
                    "primary_name": "V1324 Sco",
                    "primary_name_normalized": "v1324 sco",
                    "status": "ACTIVE",
                    "aliases": "not-a-list",  # malformed
                }
            )
            _create_ads_secret()
            h = _load_handler()
            with patch.object(h, "requests") as mock_requests:
                mock_requests.get.return_value = _mock_ads_response([])
                mock_requests.exceptions.Timeout = Exception
                mock_requests.exceptions.RequestException = Exception
                # Should not raise — falls back to primary_name only
                result = h.handle(_base_event(), None)
            assert result["candidate_count"] == 0
            # Confirm the ADS query was still made (using primary_name)
            url = mock_requests.get.call_args[0][0]
            assert "V1324" in url
