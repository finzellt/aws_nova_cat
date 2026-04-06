"""
Tier 2 smoke tests — per-workflow execution validation.

Starts each Express Step Functions state machine individually against the live
deployed stack via start_sync_execution, and asserts on:
  - Execution terminal status (SUCCEEDED / expected outcome)
  - Output payload schema (model_validate against contracts/models/outputs.py)
  - Output payload field presence per outcome path
  - DynamoDB side effects (records written with correct structure)

Design principles:
  - Fully isolated: each test mints a fresh nova_id (UUID4) and seeds only
    the minimum DynamoDB state it needs. No shared state between tests.
  - No cleanup by default: test UUIDs are clearly synthetic and won't conflict
    with real nova data. The dev stack is ephemeral; orphaned test records are
    harmless. The one exception is initialize_nova, which may create real
    NameMapping entries for V1324 Sco — idempotency handles re-runs cleanly.
  - Execution names are deterministic per test run (uuid4 suffix) so
    CloudWatch Logs are easy to correlate if a test fails.
  - Schema assertions use contracts.models.outputs terminal output models.
    _assert_output(ModelClass, out) is the single entry point; it wraps
    model_validate and formats a clear failure message on mismatch.

Workflows covered:
  TestInitializeNova           — CREATED_AND_LAUNCHED, EXISTS_AND_LAUNCHED,
                                  NOT_FOUND; per-path field presence
  TestIngestNewNova            — LAUNCHED; downstream list shape; idempotency
  TestRefreshReferences        — SUCCEEDED; per-field presence; no-ADS-hits path
  TestDiscoverSpectraProducts  — COMPLETED; providers field; per-provider
                                  iteration structure; stub/alias DDB checks
  TestAcquireAndValidateSpectra — SKIPPED_ALREADY_VALIDATED,
                                   SKIPPED_COOLDOWN_ACTIVE,
                                   SKIPPED_QUARANTINE_BLOCKED; skip-path field
                                   absence; status flag assertions

Timeouts (generous — Docker cold starts add latency on first invocation):
  initialize_nova              120s  (archive_resolver: SIMBAD + TNS, 90s timeout)
  ingest_new_nova               45s  (just acquires lock + fires StartExecution x2)
  refresh_references           120s  (reference_manager: ADS API, 90s timeout)
  discover_spectra_products     90s  (spectra_discoverer: ESO SSAP, 60s timeout)
  acquire_and_validate_spectra  45s  (skip paths only — no real acquisition)
"""

from __future__ import annotations

import json
import uuid
from decimal import Decimal
from typing import Any, TypeVar, cast

import pytest
from pydantic import BaseModel

from contracts.models.outputs import (
    AcquireAndValidateSpectraFinalizeOutput,
    AcquireAndValidateSpectraTerminalOutput,
    DiscoverSpectraProductsFinalizeOutput,
    DiscoverSpectraProductsTerminalOutput,
    FetchReferenceCandidatesOutput,
    IngestNewNovaFinalizeOutput,
    IngestNewNovaTerminalOutput,
    InitializeNovaFinalizeOutput,
    InitializeNovaTerminalOutput,
    RefreshReferencesFinalizeOutput,
    RefreshReferencesTerminalOutput,
)
from tests.smoke.conftest import StackOutputs

T = TypeVar("T", bound=BaseModel)

# ---------------------------------------------------------------------------
# Test nova — used wherever a real archive-resolvable name is needed.
# V1324 Sco is unambiguously a classical nova, well-documented in SIMBAD/TNS,
# and has two UVES spectra in the ESO archive (~June 2012).
#
# Coordinates must match the SIMBAD-precise position to within the ESO SSAP
# search cone radius (1.8 arcsec). The values below are derived from
# ATEL #4192 (Finzell et al. 2012): RA 17h 50m 54.46s, Dec -32° 37' 20.4".
#
# DO NOT round these to fewer decimal places. The ESO adapter uses a
# diameter=0.001° (3.6 arcsec) search cone. The previous values
# (267.7246, -32.6225) were ~7 arcsec off in RA — outside the cone —
# causing all ESO product discovery tests to skip with zero results.
# ---------------------------------------------------------------------------
_TEST_NOVA_NAME = "V1324 Sco"
_TEST_NOVA_RA = Decimal("267.72692")  # 17h 50m 54.46s  (ATEL #4192 / SIMBAD)
_TEST_NOVA_DEC = Decimal("-32.62233")  # -32° 37' 20.4"  (ATEL #4192 / SIMBAD)

# A name that SIMBAD will resolve as an object that is definitely not a
# classical nova (Crab Nebula = supernova remnant / pulsar wind nebula).
_NON_NOVA_NAME = "Crab Nebula"

# A name that no archive will resolve to any known object.
_UNKNOWN_NAME = "XXXXNOTAREALASTROPHYSICALOBJECTXXXX"

# A name that produces a valid SIMBAD hit but has no ADS bibliography.
# Used to exercise the zero-candidates path in refresh_references.
_NO_ADS_NOVA_NAME = "XXXXNOTAREALASTROPHYSICALOBJECTXXXX"

_REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def nova_id() -> str:
    """Fresh UUID4 nova_id — unique per test, clearly synthetic."""
    return str(uuid.uuid4())


@pytest.fixture
def correlation_id() -> str:
    return f"smoke-test-{uuid.uuid4()}"


@pytest.fixture
def execution_suffix() -> str:
    """Short suffix for Step Functions execution names (max 80 chars)."""
    return str(uuid.uuid4())[:8]


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------


def _seed_nova(
    table: Any,
    nova_id: str,
    *,
    status: str = "ACTIVE",
    primary_name: str = _TEST_NOVA_NAME,
    with_coordinates: bool = True,
    with_aliases: bool = False,
) -> None:
    """Seed a minimal Nova item sufficient for workflow execution."""
    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": "NOVA",
        "entity_type": "Nova",
        "nova_id": nova_id,
        "status": status,
        "primary_name": primary_name,
        "primary_name_normalized": primary_name.lower().replace(" ", ""),
        # Mark as smoke-test-seeded so cleanup_smoke_items / purge_smoke_items
        # catches this item in its correlation_id scan and deletes it after
        # each test. Without this, seeded Nova items outlive their test and
        # cause "Nova already existed" skips in subsequent runs.
        "correlation_id": "smoke-seeded",
    }
    if with_coordinates:
        item["ra_deg"] = _TEST_NOVA_RA
        item["dec_deg"] = _TEST_NOVA_DEC
    if with_aliases:
        item["aliases"] = [primary_name]
    table.put_item(Item=item)


def _seed_data_product(
    table: Any,
    nova_id: str,
    data_product_id: str,
    provider: str = "ESO",
    *,
    validation_status: str = "UNVALIDATED",
    acquisition_status: str = "STUB",
    eligibility: str = "ACQUIRE",
    next_eligible_attempt_at: str | None = None,
    add_gsi: bool = True,
) -> None:
    """Seed a minimal DataProduct stub for acquire_and_validate_spectra tests."""
    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": f"PRODUCT#SPECTRA#{provider}#{data_product_id}",
        "entity_type": "DataProduct",
        "nova_id": nova_id,
        "data_product_id": data_product_id,
        "provider": provider,
        "product_type": "SPECTRA",
        "validation_status": validation_status,
        "acquisition_status": acquisition_status,
        "eligibility": eligibility,
        "attempt_count": 0,
        "canonical_locator": f"https://archive.eso.org/test/{data_product_id}",
    }
    if next_eligible_attempt_at is not None:
        item["next_eligible_attempt_at"] = next_eligible_attempt_at
    if add_gsi and eligibility == "ACQUIRE":
        item["GSI1PK"] = nova_id
        item["GSI1SK"] = f"ELIG#ACQUIRE#SPECTRA#{provider}#{data_product_id}"
    table.put_item(Item=item)


def _get_item(table: Any, pk: str, sk: str) -> dict[str, Any] | None:
    resp = table.get_item(Key={"PK": pk, "SK": sk})
    return cast(dict[str, Any] | None, resp.get("Item"))


def _query_partition(table: Any, pk: str, sk_prefix: str) -> list[dict[str, Any]]:
    resp = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
        ExpressionAttributeValues={":pk": pk, ":prefix": sk_prefix},
    )
    return cast(list[dict[str, Any]], resp.get("Items", []))


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------


def _run_and_wait(
    sfn_client: Any,
    state_machine_arn: str,
    payload: dict[str, Any],
    suffix: str,
    timeout: int = 60,
) -> dict[str, Any]:
    """Start a synchronous Express execution and return the response.

    All NovaCat state machines are Express workflows. Express workflows use
    start_sync_execution which blocks until the execution completes and
    returns status + output directly — no polling required.
    """
    sm_short = state_machine_arn.split(":")[-1][:30]
    execution_name = f"smoke-{sm_short}-{suffix}"
    return cast(
        dict[str, Any],
        sfn_client.start_sync_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(payload),
        ),
    )


def _output(resp: dict[str, Any]) -> dict[str, Any]:
    """Parse the execution output JSON from a describe_execution response."""
    return cast(dict[str, Any], json.loads(resp["output"]))


def _assert_output(model_cls: type[T], out: dict[str, Any]) -> T:
    """
    Validate execution output against a terminal output model.

    Wraps model_validate with a human-readable failure message. Returns the
    validated model instance so callers can use typed attribute access for
    subsequent assertions without re-validating.

    Usage:
        model = _assert_output(RefreshReferencesTerminalOutput, out)
        assert model.finalize.outcome == "SUCCEEDED"
        assert model.discovery is not None
    """
    try:
        return model_cls.model_validate(out)
    except Exception as exc:
        formatted = json.dumps(out, default=str, indent=2)
        pytest.fail(
            f"Output schema validation failed for {model_cls.__name__}:\n"
            f"{exc}\n\n"
            f"Execution output:\n{formatted}"
        )


# ---------------------------------------------------------------------------
# TestInitializeNova
# ---------------------------------------------------------------------------


class TestInitializeNova:
    """
    initialize_nova is the system's front door — it calls real external
    archives (SIMBAD via archive_resolver). Tests use real nova names so
    the full resolution pipeline runs.
    """

    def test_created_or_exists_and_launched_v1324_sco(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        execution_suffix: str,
    ) -> None:
        """
        V1324 Sco resolves cleanly. On first run the outcome is
        CREATED_AND_LAUNCHED; on subsequent runs EXISTS_AND_LAUNCHED.
        Both are valid terminal successes for this test.
        """
        resp = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={
                "candidate_name": _TEST_NOVA_NAME,
                "correlation_id": f"smoke-init-{execution_suffix}",
                "source": "smoke_test",
            },
            suffix=execution_suffix,
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED", (
            f"initialize_nova FAILED. Check execution: {resp['executionArn']}"
        )

        out = _output(resp)
        model = _assert_output(InitializeNovaTerminalOutput, out)

        assert model.job_run.job_run_id is not None
        assert model.job_run.correlation_id is not None
        assert isinstance(model.finalize, InitializeNovaFinalizeOutput), (
            f"Expected finalize to be InitializeNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome in {"CREATED_AND_LAUNCHED", "EXISTS_AND_LAUNCHED"}, (
            f"Unexpected outcome: {model.finalize.outcome!r}"
        )

    def test_created_and_launched_writes_nova_to_dynamodb(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        execution_suffix: str,
    ) -> None:
        """
        When CREATED_AND_LAUNCHED, a Nova item must exist in DynamoDB with
        ACTIVE status and resolved coordinates.
        """
        resp = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={
                "candidate_name": _TEST_NOVA_NAME,
                "correlation_id": f"smoke-init-ddb-{execution_suffix}",
                "source": "smoke_test",
            },
            suffix=f"ddb-{execution_suffix}",
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(InitializeNovaTerminalOutput, out)

        assert isinstance(model.finalize, InitializeNovaFinalizeOutput), (
            f"Expected finalize to be InitializeNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        if model.finalize.outcome == "EXISTS_AND_LAUNCHED":
            pytest.skip("Nova already existed — CREATED path not exercised this run")
        assert model.finalize.outcome == "CREATED_AND_LAUNCHED", (
            f"Unexpected finalize outcome {model.finalize.outcome!r} — "
            f"expected CREATED_AND_LAUNCHED or EXISTS_AND_LAUNCHED"
        )

        assert model.nova_creation is not None, "nova_creation absent on CREATED path"
        nova_id = model.nova_creation.nova_id

        table = dynamodb_resource.Table(stack.table_name)
        nova_item = _get_item(table, nova_id, "NOVA")

        assert nova_item is not None, f"Nova item not found for nova_id={nova_id}"
        assert nova_item["status"] == "ACTIVE"
        assert "ra_deg" in nova_item
        assert "dec_deg" in nova_item
        assert nova_item["primary_name"] is not None

    def test_exists_and_launched_on_second_run(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        execution_suffix: str,
    ) -> None:
        """
        Running initialize_nova twice for the same name produces
        EXISTS_AND_LAUNCHED on the second call.
        """
        base_payload = {
            "candidate_name": _TEST_NOVA_NAME,
            "source": "smoke_test",
        }

        # First call — may be CREATED or EXISTS depending on prior runs
        _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={**base_payload, "correlation_id": f"smoke-init-1st-{execution_suffix}"},
            suffix=f"1st-{execution_suffix}",
            timeout=120,
        )

        # Second call — must be EXISTS_AND_LAUNCHED
        resp2 = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={**base_payload, "correlation_id": f"smoke-init-2nd-{execution_suffix}"},
            suffix=f"2nd-{execution_suffix}",
            timeout=120,
        )

        assert resp2["status"] == "SUCCEEDED"
        out = _output(resp2)
        model = _assert_output(InitializeNovaTerminalOutput, out)
        assert isinstance(model.finalize, InitializeNovaFinalizeOutput), (
            f"Expected finalize to be InitializeNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "EXISTS_AND_LAUNCHED"

    def test_not_found_outcome_for_unknown_name(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        execution_suffix: str,
    ) -> None:
        """
        A name that no archive resolves produces outcome NOT_FOUND.
        The workflow still SUCCEEDS — NOT_FOUND is a terminal success,
        not an error.
        """
        resp = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={
                "candidate_name": _UNKNOWN_NAME,
                "correlation_id": f"smoke-init-nf-{execution_suffix}",
                "source": "smoke_test",
            },
            suffix=f"nf-{execution_suffix}",
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(InitializeNovaTerminalOutput, out)
        assert isinstance(model.finalize, InitializeNovaFinalizeOutput), (
            f"Expected finalize to be InitializeNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "NOT_FOUND"

    def test_not_a_classical_nova_for_non_nova_object(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        execution_suffix: str,
    ) -> None:
        """
        A name that resolves to a known object that is not a nova at all
        produces NOT_FOUND. Crab Nebula is a supernova remnant — not a nova
        of any kind.
        """
        resp = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={
                "candidate_name": _NON_NOVA_NAME,
                "correlation_id": f"smoke-init-nc-{execution_suffix}",
                "source": "smoke_test",
            },
            suffix=f"nc-{execution_suffix}",
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(InitializeNovaTerminalOutput, out)
        assert isinstance(model.finalize, InitializeNovaFinalizeOutput), (
            f"Expected finalize to be InitializeNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "NOT_FOUND"

    def test_job_run_record_written_to_dynamodb(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        execution_suffix: str,
    ) -> None:
        """
        A JOBRUN record must be written and finalized for every execution,
        regardless of outcome.
        """
        resp = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={
                "candidate_name": _TEST_NOVA_NAME,
                "correlation_id": f"smoke-init-jr-{execution_suffix}",
                "source": "smoke_test",
            },
            suffix=f"jr-{execution_suffix}",
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(InitializeNovaTerminalOutput, out)

        table = dynamodb_resource.Table(stack.table_name)
        job_run_item = _get_item(table, model.job_run.pk, model.job_run.sk)

        assert job_run_item is not None
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["workflow_name"] == "initialize_nova"

    def test_output_fields_present_on_created_path(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        execution_suffix: str,
    ) -> None:
        """
        CREATED_AND_LAUNCHED path must populate: normalization, name_check,
        idempotency, resolution, nova_creation, upsert, launch.
        Fields that belong to other paths (coordinate_check, upsert_alias,
        quarantine, terminal_fail) must be absent.
        """
        resp = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={
                "candidate_name": _TEST_NOVA_NAME,
                "correlation_id": f"smoke-init-fields-{execution_suffix}",
                "source": "smoke_test",
            },
            suffix=f"fields-{execution_suffix}",
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(InitializeNovaTerminalOutput, out)

        assert isinstance(model.finalize, InitializeNovaFinalizeOutput), (
            f"Expected finalize to be InitializeNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        if model.finalize.outcome == "EXISTS_AND_LAUNCHED":
            pytest.skip("Nova already existed — CREATED field-presence path not exercised")
        assert model.finalize.outcome == "CREATED_AND_LAUNCHED", (
            f"Unexpected finalize outcome {model.finalize.outcome!r} — "
            f"expected CREATED_AND_LAUNCHED or EXISTS_AND_LAUNCHED"
        )

        # Fields present on CREATED_AND_LAUNCHED
        assert model.normalization is not None, "normalization absent on CREATED path"
        assert model.name_check is not None, "name_check absent on CREATED path"
        assert model.idempotency is not None, "idempotency absent on CREATED path"
        assert model.resolution is not None, "resolution absent on CREATED path"
        assert model.nova_creation is not None, "nova_creation absent on CREATED path"
        assert model.upsert is not None, "upsert absent on CREATED path"
        assert model.launch is not None, "launch absent on CREATED path"
        assert model.launch.nova_id == model.nova_creation.nova_id

        # Fields absent on CREATED_AND_LAUNCHED
        assert model.terminal_fail is None, "terminal_fail unexpectedly present"

    def test_output_fields_absent_on_not_found_path(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        execution_suffix: str,
    ) -> None:
        """
        NOT_FOUND path: resolution runs and returns is_nova=False; the workflow
        exits before CreateNovaId. launch, nova_creation, and upsert must all
        be absent.
        """
        resp = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={
                "candidate_name": _UNKNOWN_NAME,
                "correlation_id": f"smoke-init-nf-fields-{execution_suffix}",
                "source": "smoke_test",
            },
            suffix=f"nf-fields-{execution_suffix}",
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(InitializeNovaTerminalOutput, out)
        assert isinstance(model.finalize, InitializeNovaFinalizeOutput), (
            f"Expected finalize to be InitializeNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "NOT_FOUND"

        # Fields present on NOT_FOUND (ran up to and including resolution)
        assert model.normalization is not None, "normalization absent on NOT_FOUND path"
        assert model.name_check is not None, "name_check absent on NOT_FOUND path"
        assert model.idempotency is not None, "idempotency absent on NOT_FOUND path"
        assert model.resolution is not None, "resolution absent on NOT_FOUND path"
        assert model.resolution.is_nova is False

        # Fields absent on NOT_FOUND (workflow exited before these ran)
        assert model.nova_creation is None, "nova_creation unexpectedly present on NOT_FOUND"
        assert model.upsert is None, "upsert unexpectedly present on NOT_FOUND"
        assert model.launch is None, "launch unexpectedly present on NOT_FOUND"
        assert model.terminal_fail is None, "terminal_fail unexpectedly present"


# ---------------------------------------------------------------------------
# TestIngestNewNova
# ---------------------------------------------------------------------------


class TestIngestNewNova:
    """
    ingest_new_nova is always launched by initialize_nova, but we test it
    directly with a synthetic nova_id so we can control state precisely.
    """

    def test_happy_path_succeeds_and_launches_downstream(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        ingest_new_nova acquires an idempotency lock then fires
        refresh_references and discover_spectra_products concurrently.
        The workflow itself SUCCEEDS quickly after launching both — it does
        not wait for the sub-workflows.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id)

        resp = _run_and_wait(
            sfn_client,
            stack.ingest_new_nova_arn,
            payload={"nova_id": nova_id, "correlation_id": correlation_id},
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED", (
            f"ingest_new_nova FAILED. Check execution: {resp['executionArn']}"
        )

        out = _output(resp)
        model = _assert_output(IngestNewNovaTerminalOutput, out)

        assert model.job_run.correlation_id == correlation_id
        assert model.downstream is not None, (
            "downstream absent — Parallel state did not produce output"
        )

    def test_job_run_finalized_succeeded(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id)

        resp = _run_and_wait(
            sfn_client,
            stack.ingest_new_nova_arn,
            payload={"nova_id": nova_id, "correlation_id": correlation_id},
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(IngestNewNovaTerminalOutput, out)

        job_run_item = _get_item(table, model.job_run.pk, model.job_run.sk)
        assert job_run_item is not None
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["workflow_name"] == "ingest_new_nova"
        assert job_run_item["outcome"] == "LAUNCHED"

    def test_idempotency_lock_prevents_duplicate_run(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        execution_suffix: str,
    ) -> None:
        """
        Two executions for the same nova_id within the same time bucket:
        the second acquires an already-held idempotency lock and short-circuits
        to a SUCCEEDED outcome without re-launching downstream.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id)

        shared_correlation = f"smoke-idem-{execution_suffix}"

        resp1 = _run_and_wait(
            sfn_client,
            stack.ingest_new_nova_arn,
            payload={"nova_id": nova_id, "correlation_id": shared_correlation},
            suffix=f"idem-1-{execution_suffix}",
            timeout=45,
        )
        resp2 = _run_and_wait(
            sfn_client,
            stack.ingest_new_nova_arn,
            payload={"nova_id": nova_id, "correlation_id": shared_correlation},
            suffix=f"idem-2-{execution_suffix}",
            timeout=45,
        )

        # Both must succeed — second is a no-op, not an error
        assert resp1["status"] == "SUCCEEDED"
        assert resp2["status"] == "SUCCEEDED"

        _assert_output(IngestNewNovaTerminalOutput, _output(resp1))
        _assert_output(IngestNewNovaTerminalOutput, _output(resp2))

    def test_finalize_outcome_is_launched(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        finalize.outcome must be exactly "LAUNCHED" — the single valid success
        outcome for ingest_new_nova. Validates the Literal constraint in
        IngestNewNovaFinalizeOutput.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id)

        resp = _run_and_wait(
            sfn_client,
            stack.ingest_new_nova_arn,
            payload={"nova_id": nova_id, "correlation_id": correlation_id},
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED"
        model = _assert_output(IngestNewNovaTerminalOutput, _output(resp))
        assert isinstance(model.finalize, IngestNewNovaFinalizeOutput), (
            f"Expected finalize to be IngestNewNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert isinstance(model.finalize, IngestNewNovaFinalizeOutput), (
            f"Expected finalize to be IngestNewNovaFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "LAUNCHED"

    def test_downstream_has_two_branch_outputs(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        $.downstream is the Parallel state result: a list of exactly two
        branch outputs — index 0 is LaunchRefreshReferences, index 1 is
        LaunchDiscoverSpectraProducts. Each element must have nova_id.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id)

        resp = _run_and_wait(
            sfn_client,
            stack.ingest_new_nova_arn,
            payload={"nova_id": nova_id, "correlation_id": correlation_id},
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED"
        model = _assert_output(IngestNewNovaTerminalOutput, _output(resp))

        assert model.downstream is not None
        assert len(model.downstream) == 2, (
            f"Expected 2 Parallel branch outputs in $.downstream, got {len(model.downstream)}"
        )
        # Both branch outputs must identify the same nova
        for i, branch in enumerate(model.downstream):
            assert "nova_id" in branch, f"downstream[{i}] missing nova_id: {branch}"
            assert branch["nova_id"] == nova_id


# ---------------------------------------------------------------------------
# TestRefreshReferences
# ---------------------------------------------------------------------------


class TestRefreshReferences:
    """
    refresh_references queries ADS for a nova's bibliographic references.
    Tests use a seeded nova with V1324 Sco's name so ADS has real data to return.
    """

    def test_happy_path_succeeds(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_aliases=True)

        resp = _run_and_wait(
            sfn_client,
            stack.refresh_references_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "attributes": {},
            },
            suffix=execution_suffix,
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED", (
            f"refresh_references FAILED. Check execution: {resp['executionArn']}"
        )
        _assert_output(RefreshReferencesTerminalOutput, _output(resp))

    def test_references_linked_in_dynamodb(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        After a successful run, NOVAREF items must exist for the nova.
        Each NOVAREF SK is NOVAREF#<bibcode>.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_aliases=True)

        resp = _run_and_wait(
            sfn_client,
            stack.refresh_references_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "attributes": {},
            },
            suffix=execution_suffix,
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"

        novarefs = _query_partition(table, nova_id, "NOVAREF#")

        # V1324 Sco has well-documented references in ADS — expect at least one
        assert len(novarefs) >= 1, (
            "Expected at least one NOVAREF item after refresh_references — "
            "ADS returned no candidates for V1324 Sco"
        )
        for ref in novarefs:
            bibcode = ref["SK"].removeprefix("NOVAREF#")
            assert len(bibcode) > 0
            # Global Reference entity must also exist
            ref_entity = _get_item(table, f"REFERENCE#{bibcode}", "METADATA")
            assert ref_entity is not None, f"Global Reference entity missing for bibcode={bibcode}"

    def test_discovery_date_computed(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        discovery_date must be written to the Nova item after reference reconciliation.
        Format: YYYY-MM-DD (day may be 00 for month-precision dates per ADR-005).
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_aliases=True)

        resp = _run_and_wait(
            sfn_client,
            stack.refresh_references_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "attributes": {},
            },
            suffix=execution_suffix,
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"

        nova_item = _get_item(table, nova_id, "NOVA")
        assert nova_item is not None
        discovery_date = nova_item.get("discovery_date")
        assert discovery_date is not None, "discovery_date not written to Nova item"
        parts = discovery_date.split("-")
        assert len(parts) == 3, f"Unexpected discovery_date format: {discovery_date!r}"
        assert len(parts[0]) == 4  # YYYY

    def test_job_run_finalized_succeeded(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_aliases=True)

        resp = _run_and_wait(
            sfn_client,
            stack.refresh_references_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "attributes": {},
            },
            suffix=execution_suffix,
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(RefreshReferencesTerminalOutput, out)

        job_run_item = _get_item(
            dynamodb_resource.Table(stack.table_name),
            model.job_run.pk,
            model.job_run.sk,
        )
        assert job_run_item is not None
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["workflow_name"] == "refresh_references"

    def test_output_fields_all_present_on_happy_path(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        All six expected $ keys must be present on a successful run:
        job_run, idempotency, fetch, reconcile, discovery, discovery_upsert.
        finalize.outcome must be "SUCCEEDED".
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_aliases=True)

        resp = _run_and_wait(
            sfn_client,
            stack.refresh_references_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "attributes": {},
            },
            suffix=execution_suffix,
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED"
        model = _assert_output(RefreshReferencesTerminalOutput, _output(resp))

        assert isinstance(model.finalize, RefreshReferencesFinalizeOutput), (
            f"Expected finalize to be RefreshReferencesFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert isinstance(model.finalize, RefreshReferencesFinalizeOutput), (
            f"Expected finalize to be RefreshReferencesFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "SUCCEEDED"
        assert model.idempotency is not None, "idempotency absent on happy path"
        assert model.fetch is not None, "fetch absent on happy path"
        assert isinstance(model.fetch, FetchReferenceCandidatesOutput)
        assert model.fetch.candidate_count >= 0
        assert model.fetch.nova_id == nova_id
        assert model.reconcile is not None, "reconcile absent on happy path"
        assert isinstance(model.reconcile, list)
        assert model.discovery is not None, "discovery absent on happy path"
        assert model.discovery.nova_id == nova_id
        assert model.discovery_upsert is not None, "discovery_upsert absent on happy path"
        assert model.discovery_upsert.nova_id == nova_id
        assert model.terminal_fail is None

    def test_no_ads_hits_still_succeeds(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        A nova with a nonsense primary name returns zero ADS candidates.
        The Map state runs over an empty list, producing reconcile=[].
        ComputeDiscoveryDate still runs (returns earliest_bibcode=None).
        UpsertDiscoveryDateMetadata is a no-op (updated=False).
        The workflow SUCCEEDS — zero references is a valid steady state.
        """
        table = dynamodb_resource.Table(stack.table_name)
        # Seed with a nonsense name so ADS returns nothing; with_aliases=True
        # so the handler has something to query with.
        _seed_nova(
            table,
            nova_id,
            primary_name=_NO_ADS_NOVA_NAME,
            with_aliases=True,
        )

        resp = _run_and_wait(
            sfn_client,
            stack.refresh_references_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "attributes": {},
            },
            suffix=execution_suffix,
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED", (
            f"refresh_references FAILED for no-ADS-hits nova. "
            f"Check execution: {resp['executionArn']}"
        )

        model = _assert_output(RefreshReferencesTerminalOutput, _output(resp))
        assert isinstance(model.finalize, RefreshReferencesFinalizeOutput), (
            f"Expected finalize to be RefreshReferencesFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert isinstance(model.finalize, RefreshReferencesFinalizeOutput), (
            f"Expected finalize to be RefreshReferencesFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "SUCCEEDED"

        assert model.fetch is not None
        assert model.fetch.candidate_count == 0
        assert model.fetch.candidates == []

        assert model.reconcile is not None
        assert model.reconcile == [], (
            f"Expected empty reconcile list for zero-ADS-hit nova, got {model.reconcile}"
        )

        assert model.discovery is not None
        assert model.discovery.earliest_bibcode is None
        assert model.discovery.earliest_publication_date is None

        assert model.discovery_upsert is not None
        assert model.discovery_upsert.updated is False


# ---------------------------------------------------------------------------
# TestDiscoverSpectraProducts
# ---------------------------------------------------------------------------


class TestDiscoverSpectraProducts:
    """
    discover_spectra_products queries ESO SSAP with the nova's coordinates.
    Tests use V1324 Sco's real coordinates. ESO may or may not return spectra —
    both outcomes are valid; we assert structural correctness in both cases.
    """

    def test_happy_path_succeeds(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)

        resp = _run_and_wait(
            sfn_client,
            stack.discover_spectra_products_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "suppress_downstream": True,
            },
            suffix=execution_suffix,
            timeout=90,
        )

        assert resp["status"] == "SUCCEEDED", (
            f"discover_spectra_products FAILED. Check execution: {resp['executionArn']}"
        )
        _assert_output(DiscoverSpectraProductsTerminalOutput, _output(resp))

    def test_discovery_results_in_output(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        Output must contain discovery_results (Map output). Each entry is a
        per-provider result dict. A single ESO provider is configured for MVP.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)

        resp = _run_and_wait(
            sfn_client,
            stack.discover_spectra_products_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "suppress_downstream": True,
            },
            suffix=execution_suffix,
            timeout=90,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(DiscoverSpectraProductsTerminalOutput, out)

        assert model.discovery_results is not None
        assert isinstance(model.discovery_results, list)

    def test_stubs_written_if_products_discovered(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        If ESO returns products, DataProduct stubs must be written with the
        correct initial state (STUB / UNVALIDATED / ACQUIRE / GSI1 attributes).
        If ESO returns no results, this test passes trivially — we can't force
        ESO to have spectra for a synthetic nova_id.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)

        resp = _run_and_wait(
            sfn_client,
            stack.discover_spectra_products_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "suppress_downstream": True,
            },
            suffix=execution_suffix,
            timeout=90,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(DiscoverSpectraProductsTerminalOutput, out)

        # Distinguish "ESO found nothing" (legitimate skip) from "ESO found
        # products but stubs weren't written" (a real bug that must fail hard).
        eso_result = next(
            (r for r in (model.discovery_results or []) if r.get("provider") == "ESO"),
            None,
        )
        eso_new = eso_result.get("discover_result", {}).get("total_new", 0) if eso_result else 0

        stubs = _query_partition(table, nova_id, "PRODUCT#SPECTRA#")
        if not stubs:
            if eso_new == 0:
                pytest.skip("ESO returned no spectra products — stub validation skipped")
            else:
                pytest.fail(
                    f"ESO reported {eso_new} newly persisted product(s) in the workflow "
                    f"output, but no PRODUCT#SPECTRA# items were found in DynamoDB under "
                    f"nova_id={nova_id!r}. DiscoverAndPersistProducts likely failed silently."
                )

        _VALID_ACQUISITION_STATUSES = {
            "STUB",
            "ACQUIRING",
            "FAILED_RETRYABLE",
            "FAILED_TERMINAL",
            "ACQUIRED",
        }

        for stub in stubs:
            # acquisition_status may have advanced beyond STUB if acquire_and_validate_spectra
            # picked up the published request before this query ran — that is correct behaviour,
            # not a bug. Assert it is a known valid status rather than strictly "STUB".
            assert stub["acquisition_status"] in _VALID_ACQUISITION_STATUSES, (
                f"Unexpected acquisition_status {stub['acquisition_status']!r} "
                f"for data_product_id={stub.get('data_product_id')!r}"
            )
            assert stub["validation_status"] == "UNVALIDATED"
            assert stub["eligibility"] == "ACQUIRE"
            assert stub["attempt_count"] >= 0
            assert "data_product_id" in stub
            assert "GSI1PK" in stub
            assert "GSI1SK" in stub
            assert stub["GSI1SK"].startswith("ELIG#ACQUIRE#SPECTRA#")

    def test_locator_alias_written_for_each_stub(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        Each discovered product must have a corresponding LocatorAlias item
        at PK=LOCATOR#<provider>#<locator_identity>.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)

        resp = _run_and_wait(
            sfn_client,
            stack.discover_spectra_products_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "suppress_downstream": True,
            },
            suffix=execution_suffix,
            timeout=90,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(DiscoverSpectraProductsTerminalOutput, out)

        eso_result = next(
            (r for r in (model.discovery_results or []) if r.get("provider") == "ESO"),
            None,
        )
        eso_new = eso_result.get("discover_result", {}).get("total_new", 0) if eso_result else 0

        stubs = _query_partition(table, nova_id, "PRODUCT#SPECTRA#")
        if not stubs:
            if eso_new == 0:
                pytest.skip("ESO returned no spectra products — LocatorAlias validation skipped")
            else:
                pytest.fail(
                    f"ESO reported {eso_new} newly persisted product(s) in the workflow "
                    f"output, but no PRODUCT#SPECTRA# items were found in DynamoDB under "
                    f"nova_id={nova_id!r}. DiscoverAndPersistProducts likely failed silently."
                )

        for stub in stubs:
            dp_id = stub["data_product_id"]
            provider = stub["provider"]
            alias_items = table.query(
                KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
                ExpressionAttributeValues={
                    ":pk": f"LOCATOR#{provider}#{stub.get('locator_identity', '')}",
                    ":prefix": f"DATA_PRODUCT#{dp_id}",
                },
            ).get("Items", [])
            assert dp_id is not None
            assert len(dp_id) == 36  # UUID format
            assert len(alias_items) == 1, (
                f"Expected exactly 1 LocatorAlias item for data_product_id={dp_id!r}, "
                f"got {len(alias_items)}"
            )
            assert alias_items[0]["data_product_id"] == dp_id

    def test_job_run_finalized_succeeded(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)

        resp = _run_and_wait(
            sfn_client,
            stack.discover_spectra_products_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "suppress_downstream": True,
            },
            suffix=execution_suffix,
            timeout=90,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(DiscoverSpectraProductsTerminalOutput, out)

        job_run_item = _get_item(table, model.job_run.pk, model.job_run.sk)
        assert job_run_item is not None
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["outcome"] == "COMPLETED"

    def test_finalize_outcome_is_completed(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        finalize.outcome must be exactly "COMPLETED" — the single valid success
        outcome for discover_spectra_products (regardless of whether ESO
        returned any products).
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)

        resp = _run_and_wait(
            sfn_client,
            stack.discover_spectra_products_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "suppress_downstream": True,
            },
            suffix=execution_suffix,
            timeout=90,
        )

        assert resp["status"] == "SUCCEEDED"
        model = _assert_output(DiscoverSpectraProductsTerminalOutput, _output(resp))
        assert isinstance(model.finalize, DiscoverSpectraProductsFinalizeOutput), (
            f"Expected finalize to be DiscoverSpectraProductsFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert isinstance(model.finalize, DiscoverSpectraProductsFinalizeOutput), (
            f"Expected finalize to be DiscoverSpectraProductsFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "COMPLETED"

    def test_providers_field_is_eso_list(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        $.providers is written by the PrepareProviderList Pass state.
        For MVP it must be exactly [{"provider": "ESO"}]. This is the
        canonical onboarding check — adding a second provider requires this
        test to be updated.
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)

        resp = _run_and_wait(
            sfn_client,
            stack.discover_spectra_products_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "suppress_downstream": True,
            },
            suffix=execution_suffix,
            timeout=90,
        )

        assert resp["status"] == "SUCCEEDED"
        model = _assert_output(DiscoverSpectraProductsTerminalOutput, _output(resp))

        assert model.providers is not None, (
            "providers absent — PrepareProviderList Pass state did not run"
        )
        assert model.providers == [{"provider": "ESO"}], (
            f'Expected [{{"provider": "ESO"}}] (MVP provider list), got {model.providers!r}. '
            "Update this assertion if a new provider has been onboarded."
        )

    def test_discovery_results_per_provider_structure(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        Each element of $.discovery_results is the accumulated $ of one
        provider iteration. It must contain ``discover_result`` — the
        lightweight summary dict returned by the combined
        DiscoverAndPersistProducts task.
        For MVP, there is exactly one element (ESO).
        """
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)

        resp = _run_and_wait(
            sfn_client,
            stack.discover_spectra_products_arn,
            payload={
                "nova_id": nova_id,
                "correlation_id": correlation_id,
                "suppress_downstream": True,
            },
            suffix=execution_suffix,
            timeout=90,
        )

        assert resp["status"] == "SUCCEEDED"
        model = _assert_output(DiscoverSpectraProductsTerminalOutput, _output(resp))

        assert model.discovery_results is not None
        assert len(model.discovery_results) == 1, (
            f"Expected 1 provider iteration result (ESO), got {len(model.discovery_results)}. "
            "Update if additional providers have been onboarded."
        )

        eso_result = model.discovery_results[0]
        assert eso_result.get("provider") == "ESO"
        assert eso_result.get("nova_id") == nova_id

        # The combined DiscoverAndPersistProducts task writes a summary at
        # $.discover_result (ResultPath in the ASL). No individual task keys
        # (query_result, normalize_result, etc.) appear in the state output.
        assert "discover_result" in eso_result, (
            f"Expected key 'discover_result' in ESO provider iteration result, got keys: "
            f"{list(eso_result.keys())}"
        )

        summary = eso_result["discover_result"]
        for key in (
            "provider",
            "nova_id",
            "total_queried",
            "total_normalized",
            "total_new",
            "total_existing",
        ):
            assert key in summary, (
                f"Expected key '{key}' in discover_result summary, got keys: {list(summary.keys())}"
            )


# ---------------------------------------------------------------------------
# TestAcquireAndValidateSpectra
# ---------------------------------------------------------------------------


class TestAcquireAndValidateSpectra:
    """
    acquire_and_validate_spectra runs one execution per data_product_id.
    We test the three skip paths here (no real acquisition) by pre-seeding
    DataProduct state that triggers each short-circuit.

    Real acquisition (bytes download + FITS validation) is covered in
    test_e2e.py as part of the full pipeline.
    """

    def test_skip_already_validated(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        A DataProduct with validation_status=VALID triggers
        SKIPPED_ALREADY_VALIDATED. The execution SUCCEEDS immediately without
        touching the provider archive.
        """
        dp_id = str(uuid.uuid4())
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)
        _seed_data_product(
            table,
            nova_id,
            dp_id,
            validation_status="VALID",
            acquisition_status="ACQUIRED",
            eligibility="NONE",
            add_gsi=False,
        )

        resp = _run_and_wait(
            sfn_client,
            stack.acquire_and_validate_spectra_arn,
            payload={
                "nova_id": nova_id,
                "provider": "ESO",
                "data_product_id": dp_id,
                "correlation_id": correlation_id,
            },
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED", (
            f"acquire_and_validate_spectra FAILED. Check execution: {resp['executionArn']}"
        )

        out = _output(resp)
        model = _assert_output(AcquireAndValidateSpectraTerminalOutput, out)

        assert isinstance(model.finalize, AcquireAndValidateSpectraFinalizeOutput), (
            f"Expected finalize to be AcquireAndValidateSpectraFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert isinstance(model.finalize, AcquireAndValidateSpectraFinalizeOutput), (
            f"Expected finalize to be AcquireAndValidateSpectraFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "COMPLETED"
        assert model.status is not None
        assert model.status.already_validated is True
        assert model.skip_reason is not None
        assert model.skip_reason["outcome"] == "SKIPPED_ALREADY_VALIDATED"

    def test_skip_cooldown_active(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        A DataProduct with next_eligible_attempt_at in the future triggers
        SKIPPED_COOLDOWN_ACTIVE. The execution SUCCEEDS without acquiring bytes.
        """
        dp_id = str(uuid.uuid4())
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)
        _seed_data_product(
            table,
            nova_id,
            dp_id,
            validation_status="UNVALIDATED",
            acquisition_status="FAILED_RETRYABLE",
            eligibility="ACQUIRE",
            next_eligible_attempt_at="2099-01-01T00:00:00Z",
        )

        resp = _run_and_wait(
            sfn_client,
            stack.acquire_and_validate_spectra_arn,
            payload={
                "nova_id": nova_id,
                "provider": "ESO",
                "data_product_id": dp_id,
                "correlation_id": correlation_id,
            },
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(AcquireAndValidateSpectraTerminalOutput, out)

        assert isinstance(model.finalize, AcquireAndValidateSpectraFinalizeOutput), (
            f"Expected finalize to be AcquireAndValidateSpectraFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert isinstance(model.finalize, AcquireAndValidateSpectraFinalizeOutput), (
            f"Expected finalize to be AcquireAndValidateSpectraFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "COMPLETED"
        assert model.status is not None
        assert model.status.cooldown_active is True
        assert model.skip_reason is not None
        assert model.skip_reason["outcome"] == "SKIPPED_COOLDOWN_ACTIVE"

    def test_skip_quarantine_blocked(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        A DataProduct with validation_status=QUARANTINED (and no operator
        clearance) triggers SKIPPED_QUARANTINE_BLOCKED. Blocked until
        manual_review_status is updated by an operator.
        """
        dp_id = str(uuid.uuid4())
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)
        _seed_data_product(
            table,
            nova_id,
            dp_id,
            validation_status="QUARANTINED",
            acquisition_status="ACQUIRED",
            eligibility="ACQUIRE",
        )

        resp = _run_and_wait(
            sfn_client,
            stack.acquire_and_validate_spectra_arn,
            payload={
                "nova_id": nova_id,
                "provider": "ESO",
                "data_product_id": dp_id,
                "correlation_id": correlation_id,
            },
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(AcquireAndValidateSpectraTerminalOutput, out)

        assert isinstance(model.finalize, AcquireAndValidateSpectraFinalizeOutput), (
            f"Expected finalize to be AcquireAndValidateSpectraFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert isinstance(model.finalize, AcquireAndValidateSpectraFinalizeOutput), (
            f"Expected finalize to be AcquireAndValidateSpectraFinalizeOutput, "
            f"got {type(model.finalize).__name__} — workflow likely failed"
        )
        assert model.finalize.outcome == "COMPLETED"
        assert model.status is not None
        assert model.status.is_quarantined is True
        assert model.skip_reason is not None
        assert model.skip_reason["outcome"] == "SKIPPED_QUARANTINE_BLOCKED"

    def test_job_run_written_for_skip_path(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        Even on a skip path, a JOBRUN record must be written and finalized.
        Operational traceability is required for all executions.
        """
        dp_id = str(uuid.uuid4())
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)
        _seed_data_product(
            table,
            nova_id,
            dp_id,
            validation_status="VALID",
            acquisition_status="ACQUIRED",
            eligibility="NONE",
            add_gsi=False,
        )

        resp = _run_and_wait(
            sfn_client,
            stack.acquire_and_validate_spectra_arn,
            payload={
                "nova_id": nova_id,
                "provider": "ESO",
                "data_product_id": dp_id,
                "correlation_id": correlation_id,
            },
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED"
        out = _output(resp)
        model = _assert_output(AcquireAndValidateSpectraTerminalOutput, out)

        job_run_item = _get_item(table, model.job_run.pk, model.job_run.sk)
        assert job_run_item is not None
        assert job_run_item["status"] == "SUCCEEDED"
        assert job_run_item["workflow_name"] == "acquire_and_validate_spectra"

    @pytest.mark.parametrize(
        "seed_kwargs, expected_skip_reason",
        [
            (
                dict(
                    validation_status="VALID",
                    acquisition_status="ACQUIRED",
                    eligibility="NONE",
                    add_gsi=False,
                ),
                "SKIPPED_ALREADY_VALIDATED",
            ),
            (
                dict(
                    validation_status="UNVALIDATED",
                    acquisition_status="FAILED_RETRYABLE",
                    eligibility="ACQUIRE",
                    next_eligible_attempt_at="2099-01-01T00:00:00Z",
                ),
                "SKIPPED_COOLDOWN_ACTIVE",
            ),
            (
                dict(
                    validation_status="QUARANTINED",
                    acquisition_status="ACQUIRED",
                    eligibility="ACQUIRE",
                ),
                "SKIPPED_QUARANTINE_BLOCKED",
            ),
        ],
    )
    def test_skip_paths_have_no_acquisition_or_record_result(
        self,
        seed_kwargs: dict[str, Any],
        expected_skip_reason: str,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        On all three skip paths, the acquisition, validation, and record_result
        keys must be absent from the execution output. The workflow exits
        before AcquireArtifact runs; writing those keys would indicate a
        regression where the skip routing failed.
        """
        dp_id = str(uuid.uuid4())
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)
        _seed_data_product(table, nova_id, dp_id, **seed_kwargs)

        resp = _run_and_wait(
            sfn_client,
            stack.acquire_and_validate_spectra_arn,
            payload={
                "nova_id": nova_id,
                "provider": "ESO",
                "data_product_id": dp_id,
                "correlation_id": correlation_id,
            },
            suffix=f"{expected_skip_reason[:8].lower()}-{execution_suffix}",
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED"
        model = _assert_output(AcquireAndValidateSpectraTerminalOutput, _output(resp))

        assert model.skip_reason is not None
        assert model.skip_reason["outcome"] == expected_skip_reason

        assert model.acquisition is None, (
            f"acquisition unexpectedly present on {expected_skip_reason} path"
        )
        assert model.validation is None, (
            f"validation unexpectedly present on {expected_skip_reason} path"
        )
        assert model.record_result is None, (
            f"record_result unexpectedly present on {expected_skip_reason} path"
        )

    def test_status_flags_match_seeded_state(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
        nova_id: str,
        correlation_id: str,
        execution_suffix: str,
    ) -> None:
        """
        CheckOperationalStatus must correctly reflect the seeded DataProduct
        state in $.status. Verifies that the handler reads and maps the three
        decision flags correctly — not just that the Choice state routes correctly.

        Uses the VALID path (already_validated=True) as the canonical probe
        since it is the cleanest state to seed and verify.
        """
        dp_id = str(uuid.uuid4())
        table = dynamodb_resource.Table(stack.table_name)
        _seed_nova(table, nova_id, with_coordinates=True)
        _seed_data_product(
            table,
            nova_id,
            dp_id,
            validation_status="VALID",
            acquisition_status="ACQUIRED",
            eligibility="NONE",
            add_gsi=False,
        )

        resp = _run_and_wait(
            sfn_client,
            stack.acquire_and_validate_spectra_arn,
            payload={
                "nova_id": nova_id,
                "provider": "ESO",
                "data_product_id": dp_id,
                "correlation_id": correlation_id,
            },
            suffix=execution_suffix,
            timeout=45,
        )

        assert resp["status"] == "SUCCEEDED"
        model = _assert_output(AcquireAndValidateSpectraTerminalOutput, _output(resp))

        assert model.status is not None, (
            "status absent — CheckOperationalStatus did not write $.status"
        )
        assert model.status.already_validated is True
        assert model.status.cooldown_active is False
        assert model.status.is_quarantined is False
        assert model.status.data_product is not None, "data_product absent from $.status"
        assert model.status.data_product.get("data_product_id") == dp_id
