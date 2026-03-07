"""
Integration tests for the discover_spectra_products workflow.

Simulates the full Step Functions execution by calling each Lambda handler
directly in ASL order, sharing a single mocked DynamoDB instance. No real
AWS or ESO SSAP calls are made — the ESO adapter's query() is patched, and
workflow_launcher's SFN calls are patched.

The spectra_discoverer handler is loaded with a fake 'adapters' module that
uses the real ESOAdapter.normalize() (so the full normalization pipeline runs)
but has query() controlled by the test. This mirrors the unit test pattern
while exercising more of the real code path.

Workflow order (per discover_spectra_products.asl.json):
  BeginJobRun → AcquireIdempotencyLock → PrepareProviderList →
  DiscoverAcrossProviders [Map]:
    QueryProviderForProducts → NormalizeProviderProducts →
    DeduplicateAndAssignDataProductIds → PersistDataProductMetadata →
    PublishAcquireAndValidateSpectraRequests
  → FinalizeJobRunSuccess | TerminalFailHandler

Paths covered:
  1. Happy path — ESO returns 2 products, both stubs written with correct
     DynamoDB shape, 2 acquire_and_validate_spectra executions started
  2. Empty results — provider returns no products, workflow completes
     successfully with zero stubs written
  3. Provider failure tolerated — adapter raises on query; Map's
     ToleratedFailurePercentage=100 is simulated by catching the error
     and still reaching FinalizeJobRunSuccess
  4. Idempotency — second run for the same nova re-enters the same products;
     ConditionalCheckFailed is a no-op, no duplicate stubs written, no
     duplicate acquisition events fired
  5. Already-VALID product — existing product with validation_status=VALID
     is skipped entirely; no acquisition event fired
  6. Existing non-VALID product — known LocatorAlias resolves to existing
     data_product_id; alias re-written (idempotent), no new stub, no
     acquisition event (is_new=False, skip_acquisition=False → alias only)
"""

from __future__ import annotations

import importlib
import sys
import types
import uuid
from collections.abc import Generator
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_NAME = "NovaCat-Test"
_REGION = "us-east-1"
_ACCOUNT = "123456789012"

_NOVA_ID = "bbbbbbbb-0000-0000-0000-000000000001"
_CORRELATION_ID = "integ-discover-corr-001"
_JOB_RUN_ID_PREFIX = "job-run-"

_PROVIDER = "ESO"

_SM_ARNS = {
    "ingest_new_nova": f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-ingest-new-nova",
    "refresh_references": f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-refresh-references",
    "discover_spectra_products": f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-discover-spectra-products",
    "acquire_and_validate_spectra": f"arn:aws:states:{_REGION}:{_ACCOUNT}:stateMachine:nova-cat-acquire-and-validate-spectra",
}
_FAKE_EXECUTION_ARN = (
    f"arn:aws:states:{_REGION}:{_ACCOUNT}:execution:nova-cat-acquire-and-validate-spectra:test"
)

# Fixed UUID5 namespace — must match handler._DATA_PRODUCT_ID_NAMESPACE
_ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv(
        "NOVA_CAT_QUARANTINE_TOPIC_ARN", f"arn:aws:sns:{_REGION}:{_ACCOUNT}:quarantine"
    )
    monkeypatch.setenv("INGEST_NEW_NOVA_STATE_MACHINE_ARN", _SM_ARNS["ingest_new_nova"])
    monkeypatch.setenv("REFRESH_REFERENCES_STATE_MACHINE_ARN", _SM_ARNS["refresh_references"])
    monkeypatch.setenv(
        "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN", _SM_ARNS["discover_spectra_products"]
    )
    monkeypatch.setenv(
        "ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN",
        _SM_ARNS["acquire_and_validate_spectra"],
    )
    monkeypatch.setenv("NOVA_CAT_PRIVATE_BUCKET", "nova-cat-private-test")
    monkeypatch.setenv("NOVA_CAT_PUBLIC_SITE_BUCKET", "nova-cat-public-test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def table(aws_env: None) -> Generator[Any, None, None]:
    """Shared DynamoDB table with a pre-seeded Nova item."""
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
        # Seed the Nova item with coordinates so QueryProviderForProducts can fetch them
        tbl.put_item(
            Item={
                "PK": _NOVA_ID,
                "SK": "NOVA",
                "entity_type": "Nova",
                "nova_id": _NOVA_ID,
                "status": "ACTIVE",
                "ra_deg": Decimal("271.5755"),
                "dec_deg": Decimal("-30.6558"),
            }
        )
        yield tbl


# ---------------------------------------------------------------------------
# Handler loader
# ---------------------------------------------------------------------------


def _load_handlers(raw_products: list[dict] | None = None) -> dict[str, types.ModuleType]:
    """
    Load all workflow handlers fresh inside the current moto context.

    spectra_discoverer is loaded with a fake 'adapters' module so that:
      - The real ESOAdapter.normalize() pipeline runs (full code coverage)
      - adapter.query() returns controlled raw_products (no real HTTP calls)

    If raw_products is None, query() returns an empty list.
    """
    for mod_name in [
        "job_run_manager.handler",
        "idempotency_guard.handler",
        "workflow_launcher.handler",
        "spectra_discoverer.handler",
        "adapters",
    ]:
        if mod_name in sys.modules:
            del sys.modules[mod_name]

    # Build a real ESOAdapter instance to get the real normalize() path,
    # then override query() with controlled output.
    # MagicMock satisfies any @runtime_checkable Protocol — no need to import
    # the real SpectraDiscoveryAdapter. A trivial local Protocol is enough to
    # pass _validate_adapters()'s isinstance check at handler module load time.
    from typing import Protocol, runtime_checkable

    from spectra_discoverer.adapters.eso import ESOAdapter  # type: ignore[import-not-found]

    @runtime_checkable
    class _StubProtocol(Protocol):
        pass

    real_adapter = ESOAdapter()
    mock_adapter = MagicMock(wraps=real_adapter)
    mock_adapter.provider = _PROVIDER
    mock_adapter.query.return_value = raw_products or []

    fake_adapters = types.ModuleType("adapters")
    fake_adapters.SpectraDiscoveryAdapter = _StubProtocol  # type: ignore[attr-defined]
    fake_adapters._PROVIDER_ADAPTERS = {_PROVIDER: mock_adapter}  # type: ignore[attr-defined]
    sys.modules["adapters"] = fake_adapters

    return {
        "job_run_manager": importlib.import_module("job_run_manager.handler"),
        "idempotency_guard": importlib.import_module("idempotency_guard.handler"),
        "workflow_launcher": importlib.import_module("workflow_launcher.handler"),
        "spectra_discoverer": importlib.import_module("spectra_discoverer.handler"),
    }


# ---------------------------------------------------------------------------
# Raw ESO record helpers
# ---------------------------------------------------------------------------


def _raw_record(
    creator_did: str | None = None,
    access_url: str | None = None,
    collection: str = "UVES",
) -> dict[str, Any]:
    """Build a minimal sanitized ESO SSAP raw record."""
    return {
        "COLLECTION": collection,
        "TARGETNAME": "V1324 Sco",
        "s_ra": 271.5755,
        "s_dec": -30.6558,
        "em_min": 3.0e-7,
        "em_max": 1.0e-6,
        "SPECRP": 40000.0,
        "SNR": 25.0,
        "t_min": 59000.0,
        "t_max": 59001.0,
        "CREATORDID": creator_did,
        "access_url": access_url,
    }


# ---------------------------------------------------------------------------
# Workflow runner helpers
# ---------------------------------------------------------------------------


def _run_prefix(h: dict[str, types.ModuleType]) -> dict[str, Any]:
    """
    Run the common prefix: BeginJobRun → AcquireIdempotencyLock.
    Returns accumulated state dict.
    """
    job_run = cast(
        dict[str, Any],
        h["job_run_manager"].handle(
            {
                "task_name": "BeginJobRun",
                "workflow_name": "discover_spectra_products",
                "nova_id": _NOVA_ID,
                "correlation_id": _CORRELATION_ID,
            },
            None,
        ),
    )

    h["idempotency_guard"].handle(
        {
            "task_name": "AcquireIdempotencyLock",
            "workflow_name": "discover_spectra_products",
            "primary_id": _NOVA_ID,
            "correlation_id": job_run["correlation_id"],
            "job_run_id": job_run["job_run_id"],
        },
        None,
    )

    return {"nova_id": _NOVA_ID, "job_run": job_run}


def _run_provider_iteration(
    h: dict[str, types.ModuleType],
    state: dict[str, Any],
    provider: str = _PROVIDER,
) -> dict[str, Any]:
    """
    Run the full Map iterator for a single provider, mirroring the ASL:
      QueryProviderForProducts → NormalizeProviderProducts →
      DeduplicateAndAssignDataProductIds → PersistDataProductMetadata →
      PublishAcquireAndValidateSpectraRequests

    Returns the publish_result from the final task.
    """
    correlation_id = state["job_run"]["correlation_id"]

    query_result = cast(
        dict[str, Any],
        h["spectra_discoverer"].handle(
            {
                "task_name": "QueryProviderForProducts",
                "provider": provider,
                "nova_id": _NOVA_ID,
                "correlation_id": correlation_id,
            },
            None,
        ),
    )

    normalize_result = cast(
        dict[str, Any],
        h["spectra_discoverer"].handle(
            {
                "task_name": "NormalizeProviderProducts",
                "provider": provider,
                "nova_id": _NOVA_ID,
                "correlation_id": correlation_id,
                "raw_products": query_result["raw_products"],
            },
            None,
        ),
    )

    dedup_result = cast(
        dict[str, Any],
        h["spectra_discoverer"].handle(
            {
                "task_name": "DeduplicateAndAssignDataProductIds",
                "provider": provider,
                "nova_id": _NOVA_ID,
                "correlation_id": correlation_id,
                "normalized_products": normalize_result["normalized_products"],
            },
            None,
        ),
    )

    persist_result = cast(
        dict[str, Any],
        h["spectra_discoverer"].handle(
            {
                "task_name": "PersistDataProductMetadata",
                "provider": provider,
                "nova_id": _NOVA_ID,
                "correlation_id": correlation_id,
                "products_with_ids": dedup_result["products_with_ids"],
            },
            None,
        ),
    )

    publish_result = cast(
        dict[str, Any],
        h["workflow_launcher"].handle(
            {
                "task_name": "PublishAcquireAndValidateSpectraRequests",
                "nova_id": _NOVA_ID,
                "correlation_id": correlation_id,
                "job_run": state["job_run"],
                "persisted_products": persist_result["persisted_products"],
            },
            None,
        ),
    )

    return {
        "query_result": query_result,
        "normalize_result": normalize_result,
        "dedup_result": dedup_result,
        "persist_result": persist_result,
        "publish_result": publish_result,
    }


def _finalize_success(h: dict[str, types.ModuleType], state: dict[str, Any]) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        h["job_run_manager"].handle(
            {
                "task_name": "FinalizeJobRunSuccess",
                "workflow_name": "discover_spectra_products",
                "outcome": "COMPLETED",
                "nova_id": _NOVA_ID,
                "correlation_id": state["job_run"]["correlation_id"],
                "job_run_id": state["job_run"]["job_run_id"],
                "job_run": state["job_run"],
            },
            None,
        ),
    )


def _finalize_failed(
    h: dict[str, types.ModuleType], state: dict[str, Any], error: dict[str, Any]
) -> dict[str, Any]:
    h["job_run_manager"].handle(
        {
            "task_name": "TerminalFailHandler",
            "workflow_name": "discover_spectra_products",
            "error": error,
            "correlation_id": state["job_run"]["correlation_id"],
            "job_run_id": state["job_run"]["job_run_id"],
            "job_run": state["job_run"],
        },
        None,
    )
    return cast(
        dict[str, Any],
        h["job_run_manager"].handle(
            {
                "task_name": "FinalizeJobRunFailed",
                "workflow_name": "discover_spectra_products",
                "error": error,
                "job_run": state["job_run"],
            },
            None,
        ),
    )


def _get_job_run(table: Any, state: dict[str, Any]) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        table.get_item(Key={"PK": state["job_run"]["pk"], "SK": state["job_run"]["sk"]})["Item"],
    )


def _get_data_product(table: Any, data_product_id: str) -> dict[str, Any] | None:
    return cast(
        dict[str, Any] | None,
        table.get_item(
            Key={
                "PK": _NOVA_ID,
                "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{data_product_id}",
            }
        ).get("Item"),
    )


def _get_locator_alias(
    table: Any, locator_identity: str, data_product_id: str
) -> dict[str, Any] | None:
    return cast(
        dict[str, Any] | None,
        table.get_item(
            Key={
                "PK": f"LOCATOR#{_PROVIDER}#{locator_identity}",
                "SK": f"DATA_PRODUCT#{data_product_id}",
            }
        ).get("Item"),
    )


# ---------------------------------------------------------------------------
# Path 1: Happy path — 2 new products discovered
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_two_products_stubs_written(self, table: Any) -> None:
        """
        ESO returns 2 products with CREATORDID (NATIVE_ID strategy).
        Both DataProduct stubs and LocatorAlias records are written.
        """
        raw = [
            _raw_record(creator_did="eso:prod-001", access_url="http://archive.eso.org/spec1"),
            _raw_record(
                creator_did="eso:prod-002",
                access_url="http://archive.eso.org/spec2",
                collection="HARPS",
            ),
        ]
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h)
                result = _run_provider_iteration(h, state)
                _finalize_success(h, state)

            # Two stubs written
            assert len(result["persist_result"]["persisted_products"]) == 2

            dp_id_1 = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-001"))
            dp_id_2 = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-002"))

            stub_1 = _get_data_product(table, dp_id_1)
            stub_2 = _get_data_product(table, dp_id_2)

            assert stub_1 is not None
            assert stub_2 is not None

    def test_stub_has_correct_initial_state(self, table: Any) -> None:
        """DataProduct stub fields match the initial-state spec."""
        raw = [_raw_record(creator_did="eso:prod-001", access_url="http://archive.eso.org/spec1")]
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h)
                _run_provider_iteration(h, state)
                _finalize_success(h, state)

            dp_id = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-001"))
            stub = _get_data_product(table, dp_id)

            assert stub is not None
            assert stub["eligibility"] == "ACQUIRE"
            assert stub["acquisition_status"] == "STUB"
            assert stub["validation_status"] == "UNVALIDATED"
            assert stub["attempt_count"] == 0
            assert stub["product_type"] == "SPECTRA"
            assert stub["provider"] == _PROVIDER
            assert stub["nova_id"] == _NOVA_ID
            assert stub["provider_product_key"] == "eso:prod-001"
            assert stub["identity_strategy"] == "NATIVE_ID"

    def test_stub_has_gsi1_attributes(self, table: Any) -> None:
        """GSI1 attributes are written so the product appears in the EligibilityIndex."""
        raw = [_raw_record(creator_did="eso:prod-001", access_url="http://archive.eso.org/spec1")]
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}
                state = _run_prefix(h)
                _run_provider_iteration(h, state)
                _finalize_success(h, state)

            dp_id = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-001"))
            stub = _get_data_product(table, dp_id)

            assert stub is not None
            assert stub["GSI1PK"] == _NOVA_ID
            assert stub["GSI1SK"] == f"ELIG#ACQUIRE#SPECTRA#{_PROVIDER}#{dp_id}"

    def test_locator_alias_written(self, table: Any) -> None:
        """LocatorAlias record is written with correct PK/SK and data_product_id."""
        raw = [_raw_record(creator_did="eso:prod-001", access_url="http://archive.eso.org/spec1")]
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}
                state = _run_prefix(h)
                _run_provider_iteration(h, state)
                _finalize_success(h, state)

            dp_id = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-001"))
            locator_identity = "provider_product_id:eso:prod-001"
            alias = _get_locator_alias(table, locator_identity, dp_id)

            assert alias is not None
            assert alias["data_product_id"] == dp_id
            assert alias["provider"] == _PROVIDER
            assert alias["nova_id"] == _NOVA_ID

    def test_two_acquire_executions_started(self, table: Any) -> None:
        """One sfn:StartExecution call is made per eligible data_product_id."""
        raw = [
            _raw_record(creator_did="eso:prod-001", access_url="http://archive.eso.org/spec1"),
            _raw_record(creator_did="eso:prod-002", access_url="http://archive.eso.org/spec2"),
        ]
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h)
                result = _run_provider_iteration(h, state)
                _finalize_success(h, state)

            assert mock_sfn.start_execution.call_count == 2
            assert result["publish_result"]["total"] == 2
            assert len(result["publish_result"]["launched"]) == 2
            assert result["publish_result"]["failed"] == []

    def test_acquire_execution_input_contains_required_fields(self, table: Any) -> None:
        """Continuation event passed to acquire_and_validate_spectra is complete."""
        import json

        raw = [_raw_record(creator_did="eso:prod-001", access_url="http://archive.eso.org/spec1")]
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}

                state = _run_prefix(h)
                _run_provider_iteration(h, state)
                _finalize_success(h, state)

            _, kwargs = mock_sfn.start_execution.call_args
            payload = json.loads(kwargs["input"])

        assert payload["nova_id"] == _NOVA_ID
        assert payload["provider"] == _PROVIDER
        assert "data_product_id" in payload
        assert "correlation_id" in payload
        assert kwargs["stateMachineArn"] == _SM_ARNS["acquire_and_validate_spectra"]

    def test_job_run_ends_succeeded(self, table: Any) -> None:
        raw = [_raw_record(creator_did="eso:prod-001", access_url="http://archive.eso.org/spec1")]
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}
                state = _run_prefix(h)
                _run_provider_iteration(h, state)
                _finalize_success(h, state)

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["outcome"] == "COMPLETED"

    def test_hints_persisted_on_stub(self, table: Any) -> None:
        """Hints extracted from the SSAP record (collection, specrp etc.) are stored."""
        raw = [
            _raw_record(
                creator_did="eso:prod-001",
                access_url="http://archive.eso.org/spec1",
                collection="UVES",
            )
        ]
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}
                state = _run_prefix(h)
                _run_provider_iteration(h, state)
                _finalize_success(h, state)

            dp_id = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-001"))
            stub = _get_data_product(table, dp_id)

        assert stub is not None
        assert stub["hints"]["collection"] == "UVES"
        assert "specrp" in stub["hints"]


# ---------------------------------------------------------------------------
# Path 2: Empty results — no products discovered
# ---------------------------------------------------------------------------


class TestEmptyResults:
    def test_empty_provider_results_no_stubs_written(self, table: Any) -> None:
        with mock_aws():
            h = _load_handlers(raw_products=[])
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                state = _run_prefix(h)
                result = _run_provider_iteration(h, state)
                _finalize_success(h, state)

            mock_sfn.start_execution.assert_not_called()

        assert result["persist_result"]["persisted_products"] == []
        assert result["publish_result"]["total"] == 0

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"

    def test_all_records_malformed_no_stubs_written(self, table: Any) -> None:
        """If all records normalize to None (malformed), result is same as empty."""
        raw = [{"CREATORDID": None, "access_url": None}]  # will normalize to None
        with mock_aws():
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                state = _run_prefix(h)
                result = _run_provider_iteration(h, state)
                _finalize_success(h, state)

            mock_sfn.start_execution.assert_not_called()

        assert result["normalize_result"]["normalized_products"] == []


# ---------------------------------------------------------------------------
# Path 3: Provider failure tolerated
# ---------------------------------------------------------------------------


class TestProviderFailureTolerated:
    def test_query_failure_does_not_prevent_finalize_success(self, table: Any) -> None:
        """
        If QueryProviderForProducts raises (e.g. ESO SSAP unreachable),
        the ASL Map state tolerates the failure (ToleratedFailurePercentage=100).
        The workflow still reaches FinalizeJobRunSuccess.

        Simulated here by making adapter.query() raise a RetryableError, then
        catching the error from the task call and proceeding directly to
        FinalizeJobRunSuccess, mirroring what the Map state does.
        """
        from nova_common.errors import RetryableError

        with mock_aws():
            h = _load_handlers(raw_products=[])

            # Reach into the fake adapters registry and make query() raise
            fake_adapter = sys.modules["adapters"]._PROVIDER_ADAPTERS[_PROVIDER]
            fake_adapter.query.side_effect = RetryableError("ESO SSAP unreachable")

            state = _run_prefix(h)

            # Map iteration fails — ASL tolerates it
            with pytest.raises(RetryableError):
                h["spectra_discoverer"].handle(
                    {
                        "task_name": "QueryProviderForProducts",
                        "provider": _PROVIDER,
                        "nova_id": _NOVA_ID,
                        "correlation_id": state["job_run"]["correlation_id"],
                    },
                    None,
                )

            # ASL proceeds to FinalizeJobRunSuccess despite iteration failure
            _finalize_success(h, state)

        job_run_item = _get_job_run(table, state)
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["outcome"] == "COMPLETED"


# ---------------------------------------------------------------------------
# Path 4: Idempotency — second run is a no-op
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_second_run_produces_no_duplicate_stubs(self, table: Any) -> None:
        """
        A second discover_spectra_products run for the same nova and same
        ESO products must not create duplicate DataProduct stubs or fire
        duplicate acquisition events.
        """
        raw = [_raw_record(creator_did="eso:prod-001", access_url="http://archive.eso.org/spec1")]
        dp_id = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-001"))

        with mock_aws():
            # First run
            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn.start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}
                state = _run_prefix(h)
                _run_provider_iteration(h, state)
                _finalize_success(h, state)

            first_run_stub = _get_data_product(table, dp_id)
            assert first_run_stub is not None
            first_created_at = first_run_stub["created_at"]

            # Second run — idempotency guard will block at a different hour,
            # but we bypass it here to simulate a manual re-trigger or retry.
            # Clear idempotency module to allow re-entry.
            for mod in [k for k in sys.modules if "idempotency_guard" in k]:
                del sys.modules[mod]

            h2 = _load_handlers(raw_products=raw)
            with patch.object(h2["workflow_launcher"], "_sfn") as mock_sfn2:
                mock_sfn2.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                mock_sfn2.start_execution.side_effect = Exception("ExecutionAlreadyExists")

                # Simulate second run's iteration — stubs already exist
                correlation_id_2 = "integ-discover-corr-002"
                job_run2 = h2["job_run_manager"].handle(
                    {
                        "task_name": "BeginJobRun",
                        "workflow_name": "discover_spectra_products",
                        "nova_id": _NOVA_ID,
                        "correlation_id": correlation_id_2,
                    },
                    None,
                )
                state2 = {"nova_id": _NOVA_ID, "job_run": job_run2}
                _run_provider_iteration(h2, state2)
                _finalize_success(h2, state2)

            # Stub still exists and created_at is unchanged (conditional put was no-op)
            second_run_stub = _get_data_product(table, dp_id)
            assert second_run_stub is not None
            assert second_run_stub["created_at"] == first_created_at


# ---------------------------------------------------------------------------
# Path 5: Already-VALID product — skipped entirely
# ---------------------------------------------------------------------------


class TestAlreadyValidProduct:
    def test_valid_product_not_re_acquired(self, table: Any) -> None:
        """
        If LocatorAlias resolves to an existing data_product_id whose
        validation_status is VALID, the product is skipped — no new stub
        write, no acquisition event fired.
        """
        dp_id = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-valid"))
        locator_identity = "provider_product_id:eso:prod-valid"

        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name=_REGION).Table(_TABLE_NAME)

            # Pre-seed LocatorAlias pointing at an existing VALID product
            ddb.put_item(
                Item={
                    "PK": f"LOCATOR#{_PROVIDER}#{locator_identity}",
                    "SK": f"DATA_PRODUCT#{dp_id}",
                    "entity_type": "LocatorAlias",
                    "data_product_id": dp_id,
                    "provider": _PROVIDER,
                    "nova_id": _NOVA_ID,
                }
            )
            ddb.put_item(
                Item={
                    "PK": _NOVA_ID,
                    "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{dp_id}",
                    "entity_type": "DataProduct",
                    "validation_status": "VALID",
                    "eligibility": "NONE",
                }
            )

            raw = [
                _raw_record(
                    creator_did="eso:prod-valid", access_url="http://archive.eso.org/spec-valid"
                )
            ]

            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                state = _run_prefix(h)
                result = _run_provider_iteration(h, state)
                _finalize_success(h, state)

            # No acquisition event fired
            mock_sfn.start_execution.assert_not_called()
            assert result["persist_result"]["persisted_products"] == []

            # Existing stub is unchanged (still VALID/NONE)
            stub = _get_data_product(table, dp_id)
            assert stub is not None
            assert stub["validation_status"] == "VALID"
            assert stub["eligibility"] == "NONE"


# ---------------------------------------------------------------------------
# Path 6: Existing non-VALID product — alias only
# ---------------------------------------------------------------------------


class TestExistingNonValidProduct:
    def test_existing_non_valid_writes_alias_only(self, table: Any) -> None:
        """
        If LocatorAlias resolves to an existing data_product_id that is NOT
        VALID, LocatorAlias is re-written (idempotent) but no new stub is
        inserted and no acquisition event is fired (is_new=False).

        The existing product's acquisition state is preserved — the workflow
        spec says: 'existing non-VALID product: LocatorAlias ensured, no stub
        re-write'.
        """
        dp_id = str(uuid.uuid5(_ID_NAMESPACE, "ESO:eso:prod-in-flight"))
        locator_identity = "provider_product_id:eso:prod-in-flight"

        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name=_REGION).Table(_TABLE_NAME)

            # Pre-seed LocatorAlias and an in-flight (UNVALIDATED) stub
            ddb.put_item(
                Item={
                    "PK": f"LOCATOR#{_PROVIDER}#{locator_identity}",
                    "SK": f"DATA_PRODUCT#{dp_id}",
                    "entity_type": "LocatorAlias",
                    "data_product_id": dp_id,
                    "provider": _PROVIDER,
                    "nova_id": _NOVA_ID,
                }
            )
            ddb.put_item(
                Item={
                    "PK": _NOVA_ID,
                    "SK": f"PRODUCT#SPECTRA#{_PROVIDER}#{dp_id}",
                    "entity_type": "DataProduct",
                    "validation_status": "UNVALIDATED",
                    "acquisition_status": "IN_PROGRESS",
                    "eligibility": "ACQUIRE",
                }
            )

            raw = [
                _raw_record(
                    creator_did="eso:prod-in-flight",
                    access_url="http://archive.eso.org/spec-inflight",
                )
            ]

            h = _load_handlers(raw_products=raw)
            with patch.object(h["workflow_launcher"], "_sfn") as mock_sfn:
                mock_sfn.exceptions.ExecutionAlreadyExists = type(
                    "ExecutionAlreadyExists", (Exception,), {}
                )
                state = _run_prefix(h)
                result = _run_provider_iteration(h, state)
                _finalize_success(h, state)

            # No new acquisition event
            mock_sfn.start_execution.assert_not_called()
            assert result["persist_result"]["persisted_products"] == []

            # Existing stub's acquisition state is preserved
            stub = _get_data_product(table, dp_id)
            assert stub is not None
            assert stub["acquisition_status"] == "IN_PROGRESS"

            # LocatorAlias still exists
            alias = _get_locator_alias(table, locator_identity, dp_id)
            assert alias is not None
