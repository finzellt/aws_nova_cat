"""
Unit tests for services/spectra_discoverer/handler.py

Uses moto to mock DynamoDB — no real AWS calls are made.
The adapters package is replaced with a fake module so handler tests
are isolated from ESO SSAP network calls.

Covers:
  - QueryProviderForProducts: returns raw_products, missing nova, missing coordinates
  - NormalizeProviderProducts: skips None from adapter, returns normalized list
  - DeduplicateAndAssignDataProductIds: uuid5 for NATIVE_ID/METADATA_KEY, uuid4 for WEAK,
    existing LocatorAlias reuses id, VALID product sets skip_acquisition
  - PersistDataProductMetadata: writes stub + alias for new products, skips VALID,
    alias-only write for existing non-VALID, ConditionalCheckFailed is a no-op
  - Unknown task_name raises ValueError
"""

from __future__ import annotations

import importlib
import sys
import types
import uuid
from collections.abc import Generator
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"
_NOVA_ID = "test-nova-id-0000-0000-0000-000000000001"
_PROVIDER = "ESO"

# Matches handler's _DATA_PRODUCT_ID_NAMESPACE
_ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", "nova-cat-private-test")
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-public-test")
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:quarantine"
    )
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def mock_adapter() -> MagicMock:
    adapter = MagicMock()
    adapter.provider = _PROVIDER
    adapter.query.return_value = []
    adapter.normalize.return_value = None
    return adapter


@pytest.fixture
def table(aws_env: None) -> Generator[Any, None, None]:
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
        # Seed a Nova item with coordinates
        tbl.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": "NOVA",
                "nova_id": _NOVA_ID,
                "ra_deg": Decimal("271.5755"),
                "dec_deg": Decimal("-30.6558"),
            }
        )
        yield tbl


def _load_handler(mock_adapter: MagicMock) -> types.ModuleType:
    """
    Import spectra_discoverer.handler fresh, injecting a fake 'adapters' module
    so that the real ESOAdapter and pyvo are never touched.

    The fake module exposes:
      - SpectraDiscoveryAdapter: the real @runtime_checkable Protocol so that
        _validate_adapters() (called at module load) passes its isinstance check
      - _PROVIDER_ADAPTERS: {"ESO": mock_adapter}
    """
    # Clear handler from sys.modules to force re-import
    mods_to_clear = [k for k in sys.modules if k == "spectra_discoverer.handler" or k == "adapters"]
    for mod in mods_to_clear:
        del sys.modules[mod]

    # MagicMock satisfies any @runtime_checkable Protocol because hasattr(mock, x)
    # is always True — we don't need the real SpectraDiscoveryAdapter imported here.
    # A trivial local Protocol is enough to pass _validate_adapters()'s isinstance check.
    from typing import Protocol, runtime_checkable

    @runtime_checkable
    class _StubProtocol(Protocol):
        pass

    fake_adapters = types.ModuleType("adapters")
    fake_adapters.SpectraDiscoveryAdapter = _StubProtocol  # type: ignore[attr-defined]
    fake_adapters._PROVIDER_ADAPTERS = {_PROVIDER: mock_adapter}  # type: ignore[attr-defined]
    sys.modules["adapters"] = fake_adapters

    return importlib.import_module("spectra_discoverer.handler")


def _base_query_event(**kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "QueryProviderForProducts",
        "provider": _PROVIDER,
        "nova_id": _NOVA_ID,
        "correlation_id": "corr-001",
        **kwargs,
    }


def _base_normalize_event(raw_products: list[dict], **kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "NormalizeProviderProducts",
        "provider": _PROVIDER,
        "nova_id": _NOVA_ID,
        "correlation_id": "corr-001",
        "raw_products": raw_products,
        **kwargs,
    }


def _base_dedup_event(normalized_products: list[dict], **kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "DeduplicateAndAssignDataProductIds",
        "provider": _PROVIDER,
        "nova_id": _NOVA_ID,
        "correlation_id": "corr-001",
        "normalized_products": normalized_products,
        **kwargs,
    }


def _base_persist_event(products_with_ids: list[dict], **kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "PersistDataProductMetadata",
        "provider": _PROVIDER,
        "nova_id": _NOVA_ID,
        "correlation_id": "corr-001",
        "products_with_ids": products_with_ids,
        **kwargs,
    }


def _native_id_product(**overrides: Any) -> dict[str, Any]:
    """A normalized product using the NATIVE_ID identity strategy."""
    return {
        "provider": _PROVIDER,
        "nova_id": _NOVA_ID,
        "provider_product_key": "eso:product-001",
        "locator_identity": "provider_product_id:eso:product-001",
        "identity_strategy": "NATIVE_ID",
        "locators": [{"kind": "URL", "role": "PRIMARY", "value": "http://archive.eso.org/spec1"}],
        "hints": {"collection": "UVES"},
        **overrides,
    }


def _metadata_key_product(**overrides: Any) -> dict[str, Any]:
    """A normalized product using the METADATA_KEY identity strategy."""
    return {
        "provider": _PROVIDER,
        "nova_id": _NOVA_ID,
        "provider_product_key": None,
        "locator_identity": "url:http://archive.eso.org/spec2",
        "identity_strategy": "METADATA_KEY",
        "locators": [{"kind": "URL", "role": "PRIMARY", "value": "http://archive.eso.org/spec2"}],
        "hints": {},
        **overrides,
    }


def _weak_product(**overrides: Any) -> dict[str, Any]:
    """A normalized product using the WEAK identity strategy."""
    return {
        "provider": _PROVIDER,
        "nova_id": _NOVA_ID,
        "provider_product_key": "eso:product-weak",
        "locator_identity": "provider_product_id:eso:product-weak",
        "identity_strategy": "WEAK",
        "locators": [],
        "hints": {},
        **overrides,
    }


# ---------------------------------------------------------------------------
# QueryProviderForProducts
# ---------------------------------------------------------------------------


class TestQueryProviderForProducts:
    def test_returns_raw_products_from_adapter(self, table: Any, mock_adapter: MagicMock) -> None:
        raw = [{"CREATORDID": "eso:product-001", "access_url": "http://example.com"}]
        mock_adapter.query.return_value = raw

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_query_event(), None)

        assert result["raw_products"] == raw

    def test_passes_coordinates_to_adapter(self, table: Any, mock_adapter: MagicMock) -> None:
        with mock_aws():
            handler = _load_handler(mock_adapter)
            handler.handle(_base_query_event(), None)

        mock_adapter.query.assert_called_once_with(
            nova_id=_NOVA_ID,
            ra_deg=pytest.approx(271.5755),
            dec_deg=pytest.approx(-30.6558),
            primary_name="unknown",
            aliases=[],
        )

    def test_raises_value_error_when_nova_not_found(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        with mock_aws():
            handler = _load_handler(mock_adapter)
            with pytest.raises(ValueError, match="Nova not found"):
                handler.handle(_base_query_event(nova_id="nonexistent-nova-id"), None)

    def test_raises_value_error_when_ra_missing(self, table: Any, mock_adapter: MagicMock) -> None:
        with mock_aws():
            # Seed a Nova item with no coordinates
            boto3.resource("dynamodb", region_name=_REGION).Table(_TABLE_NAME).put_item(
                Item={
                    "PK": "no-coords-nova",
                    "SK": "NOVA",
                    "nova_id": "no-coords-nova",
                }
            )
            handler = _load_handler(mock_adapter)
            with pytest.raises(ValueError, match="missing coordinates"):
                handler.handle(_base_query_event(nova_id="no-coords-nova"), None)

    def test_raises_value_error_for_unknown_provider(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        with mock_aws():
            handler = _load_handler(mock_adapter)
            with pytest.raises(ValueError, match="No adapter registered"):
                handler.handle(_base_query_event(provider="MAST"), None)


# ---------------------------------------------------------------------------
# NormalizeProviderProducts
# ---------------------------------------------------------------------------


class TestNormalizeProviderProducts:
    def test_returns_normalized_products(self, table: Any, mock_adapter: MagicMock) -> None:
        normalized = _native_id_product()
        mock_adapter.normalize.return_value = normalized

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_normalize_event([{"raw": "data"}]), None)

        assert result["normalized_products"] == [normalized]

    def test_skips_none_results_from_adapter(self, table: Any, mock_adapter: MagicMock) -> None:
        mock_adapter.normalize.return_value = None

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_normalize_event([{"raw": "data"}]), None)

        assert result["normalized_products"] == []

    def test_empty_raw_list_returns_empty(self, table: Any, mock_adapter: MagicMock) -> None:
        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_normalize_event([]), None)

        assert result["normalized_products"] == []

    def test_partial_normalize_failure_skips_failed_records(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        good = _native_id_product()
        mock_adapter.normalize.side_effect = [None, good]

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_normalize_event([{"raw": "bad"}, {"raw": "good"}]), None)

        assert len(result["normalized_products"]) == 1
        assert result["normalized_products"][0] == good


# ---------------------------------------------------------------------------
# DeduplicateAndAssignDataProductIds
# ---------------------------------------------------------------------------


class TestDeduplicateAndAssignDataProductIds:
    def test_native_id_generates_deterministic_uuid5(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        product = _native_id_product()
        expected_id = str(uuid.uuid5(_ID_NAMESPACE, f"ESO:{product['provider_product_key']}"))

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_dedup_event([product]), None)

        assert result["products_with_ids"][0]["data_product_id"] == expected_id

    def test_metadata_key_generates_deterministic_uuid5(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        product = _metadata_key_product()
        expected_id = str(uuid.uuid5(_ID_NAMESPACE, f"ESO:{product['locator_identity']}"))

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_dedup_event([product]), None)

        assert result["products_with_ids"][0]["data_product_id"] == expected_id

    def test_weak_generates_valid_uuid4(self, table: Any, mock_adapter: MagicMock) -> None:
        product = _weak_product()

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_dedup_event([product]), None)

        product_id = result["products_with_ids"][0]["data_product_id"]
        # Valid UUID4 — just verify format and version
        parsed = uuid.UUID(product_id)
        assert parsed.version == 4

    def test_new_product_flagged_as_is_new(self, table: Any, mock_adapter: MagicMock) -> None:
        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_dedup_event([_native_id_product()]), None)

        assert result["products_with_ids"][0]["is_new"] is True

    def test_existing_locator_alias_reuses_data_product_id(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        existing_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        product = _native_id_product()
        # Pre-seed a LocatorAlias
        boto3.resource("dynamodb", region_name=_REGION).Table(_TABLE_NAME).put_item(
            Item={
                "PK": f"LOCATOR#ESO#{product['locator_identity']}",
                "SK": f"DATA_PRODUCT#{existing_id}",
                "data_product_id": existing_id,
            }
        )

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_dedup_event([product]), None)

        enriched = result["products_with_ids"][0]
        assert enriched["data_product_id"] == existing_id
        assert enriched["is_new"] is False

    def test_existing_valid_product_sets_skip_acquisition(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        existing_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        product = _native_id_product()
        ddb = boto3.resource("dynamodb", region_name=_REGION).Table(_TABLE_NAME)
        ddb.put_item(
            Item={
                "PK": f"LOCATOR#ESO#{product['locator_identity']}",
                "SK": f"DATA_PRODUCT#{existing_id}",
                "data_product_id": existing_id,
            }
        )
        ddb.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#ESO#{existing_id}",
                "validation_status": "VALID",
            }
        )

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_dedup_event([product]), None)

        assert result["products_with_ids"][0]["skip_acquisition"] is True

    def test_existing_non_valid_product_does_not_skip(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        existing_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        product = _native_id_product()
        ddb = boto3.resource("dynamodb", region_name=_REGION).Table(_TABLE_NAME)
        ddb.put_item(
            Item={
                "PK": f"LOCATOR#ESO#{product['locator_identity']}",
                "SK": f"DATA_PRODUCT#{existing_id}",
                "data_product_id": existing_id,
            }
        )
        ddb.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#ESO#{existing_id}",
                "validation_status": "UNVALIDATED",
            }
        )

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_dedup_event([product]), None)

        assert result["products_with_ids"][0]["skip_acquisition"] is False


# ---------------------------------------------------------------------------
# PersistDataProductMetadata
# ---------------------------------------------------------------------------


class TestPersistDataProductMetadata:
    def _product_with_id(self, is_new: bool = True, skip: bool = False, **overrides: Any) -> dict:
        product = _native_id_product(**overrides)
        data_product_id = str(uuid.uuid5(_ID_NAMESPACE, f"ESO:{product['provider_product_key']}"))
        return {
            **product,
            "data_product_id": data_product_id,
            "is_new": is_new,
            "skip_acquisition": skip,
        }

    def test_writes_data_product_stub(self, table: Any, mock_adapter: MagicMock) -> None:
        product = self._product_with_id()

        with mock_aws():
            handler = _load_handler(mock_adapter)
            handler.handle(_base_persist_event([product]), None)

        item = table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{product['data_product_id']}",
            }
        ).get("Item")
        assert item is not None
        assert item["eligibility"] == "ACQUIRE"
        assert item["acquisition_status"] == "STUB"
        assert item["validation_status"] == "UNVALIDATED"

    def test_stub_writes_gsi1_attributes(self, table: Any, mock_adapter: MagicMock) -> None:
        product = self._product_with_id()

        with mock_aws():
            handler = _load_handler(mock_adapter)
            handler.handle(_base_persist_event([product]), None)

        item = table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{product['data_product_id']}",
            }
        ).get("Item")
        assert item is not None
        assert item["GSI1PK"] == _NOVA_ID
        assert item["GSI1SK"] == f"ELIG#ACQUIRE#SPECTRA#{_PROVIDER}#{product['data_product_id']}"

    def test_writes_locator_alias(self, table: Any, mock_adapter: MagicMock) -> None:
        product = self._product_with_id()

        with mock_aws():
            handler = _load_handler(mock_adapter)
            handler.handle(_base_persist_event([product]), None)

        alias = table.get_item(
            Key={
                "PK": f"LOCATOR#{_PROVIDER}#{product['locator_identity']}",
                "SK": f"DATA_PRODUCT#{product['data_product_id']}",
            }
        ).get("Item")
        assert alias is not None
        assert alias["data_product_id"] == product["data_product_id"]

    def test_skip_acquisition_skips_stub_and_alias_write(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        product = self._product_with_id(skip=True)

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_persist_event([product]), None)

        assert result["persisted_products"] == []
        item = table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{product['data_product_id']}",
            }
        ).get("Item")
        assert item is None

    def test_existing_stub_conditional_check_is_no_op(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        product = self._product_with_id()
        # Pre-seed the stub so conditional put fails
        table.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{product['data_product_id']}",
                "eligibility": "NONE",  # different from what handler would write
            }
        )

        with mock_aws():
            handler = _load_handler(mock_adapter)
            # Should not raise — ConditionalCheckFailed is a no-op
            handler.handle(_base_persist_event([product]), None)

        # Existing item is preserved
        item = table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{product['data_product_id']}",
            }
        ).get("Item")
        assert item["eligibility"] == "NONE"

    def test_existing_non_valid_product_queued_for_acquisition(
        self, table: Any, mock_adapter: MagicMock
    ) -> None:
        """Existing UNVALIDATED product is included in persisted_products so
        acquire_and_validate_spectra is launched for it on re-discovery."""
        product = self._product_with_id(is_new=False, skip=False)

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_persist_event([product]), None)

        # Added to persisted_products so acquisition is triggered
        assert len(result["persisted_products"]) == 1
        assert result["persisted_products"][0]["data_product_id"] == product["data_product_id"]
        # Alias still written
        alias = table.get_item(
            Key={
                "PK": f"LOCATOR#{_PROVIDER}#{product['locator_identity']}",
                "SK": f"DATA_PRODUCT#{product['data_product_id']}",
            }
        ).get("Item")
        assert alias is not None

    def test_returns_newly_persisted_products(self, table: Any, mock_adapter: MagicMock) -> None:
        product = self._product_with_id()

        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_persist_event([product]), None)

        assert len(result["persisted_products"]) == 1
        assert result["persisted_products"][0]["data_product_id"] == product["data_product_id"]
        assert result["persisted_products"][0]["provider"] == _PROVIDER
        assert result["persisted_products"][0]["nova_id"] == _NOVA_ID

    def test_empty_product_list_returns_empty(self, table: Any, mock_adapter: MagicMock) -> None:
        with mock_aws():
            handler = _load_handler(mock_adapter)
            result = handler.handle(_base_persist_event([]), None)

        assert result["persisted_products"] == []


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_unknown_task_name_raises(self, table: Any, mock_adapter: MagicMock) -> None:
        with mock_aws():
            handler = _load_handler(mock_adapter)
            with pytest.raises(ValueError, match="Unknown task_name"):
                handler.handle({"task_name": "NonExistentTask"}, None)

    def test_missing_task_name_raises(self, table: Any, mock_adapter: MagicMock) -> None:
        with mock_aws():
            handler = _load_handler(mock_adapter)
            with pytest.raises(ValueError, match="Missing required field"):
                handler.handle({}, None)
