"""
End-to-end pipeline smoke test.

Runs the full NovaCat ingestion chain against NovaCatSmoke for V1324 Sco:

    initialize_nova
        → ingest_new_nova
            → refresh_references          (parallel)
            → discover_spectra_products   (parallel)
                → acquire_and_validate_spectra  (one per product)

Each workflow is polled to completion and asserted individually as it
finishes, giving streaming progress output. The final assertion is that
at least one ESO spectra product reaches validation_status = VALID.

Design notes:
  - Uses the real V1324 Sco name so the full archive resolution pipeline
    runs (SIMBAD → ESO SSAP → ESO download + FITS validation).
  - Does NOT pass suppress_downstream=True to discover_spectra_products —
    this is what distinguishes e2e from the per-workflow smoke tests.
  - Child executions are found by listing recent executions on each state
    machine and matching by nova_id in the input payload. This is
    intentionally simple: the smoke stack is low-traffic, so a recent
    execution for the right nova_id is unambiguous.
  - Acquire/validate executions may be many (one per product). We poll
    all of them to completion before asserting final DDB state.
  - Generous timeouts: ESO downloads add real latency.

Timeouts:
  initialize_nova                 120s
  ingest_new_nova                  45s
  refresh_references              120s
  discover_spectra_products        90s
  acquire_and_validate_spectra    180s per product (real FITS download)
  total wall-clock budget         ~15 min (generous; normal < 5 min)
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import pytest

from contracts.models.outputs import (
    AcquireAndValidateSpectraFinalizeOutput,
    AcquireAndValidateSpectraTerminalOutput,
    DiscoverSpectraProductsFinalizeOutput,
    DiscoverSpectraProductsTerminalOutput,
    IngestNewNovaFinalizeOutput,
    IngestNewNovaTerminalOutput,
    InitializeNovaFinalizeOutput,
    InitializeNovaTerminalOutput,
    RefreshReferencesFinalizeOutput,
    RefreshReferencesTerminalOutput,
)
from tests.smoke.conftest import StackOutputs, poll_execution
from tests.smoke.test_workflows import (
    _TEST_NOVA_NAME,
    _assert_output,
    _get_item,
    _output,
    _query_partition,
    _run_and_wait,
)

# ---------------------------------------------------------------------------
# E2E-specific constants
# ---------------------------------------------------------------------------

# How long to search backward when listing executions to find a child run.
_CHILD_SEARCH_WINDOW_SECONDS = 600  # 10 minutes

# How long to wait for all acquire_and_validate executions to finish.
_ACQUIRE_TIMEOUT_PER_PRODUCT = 180  # seconds

# How many seconds to sleep between polling acquire_and_validate executions.
_ACQUIRE_POLL_INTERVAL = 10  # seconds

# Maximum total time to wait for all acquire_and_validate executions.
_ACQUIRE_TOTAL_TIMEOUT = 900  # 15 minutes


# ---------------------------------------------------------------------------
# Child execution discovery helpers
# ---------------------------------------------------------------------------


def _find_child_executions(
    sfn_client: Any,
    state_machine_arn: str,
    nova_id: str,
    max_results: int = 20,
) -> list[str]:
    """
    List recent executions of a state machine and return ARNs whose input
    payload contains nova_id. Returns all matches, most recent first.

    We list the most recent `max_results` executions (both RUNNING and
    terminal) to avoid missing a fast-completing execution.
    """
    matching = []
    for status_filter in ("RUNNING", "SUCCEEDED", "FAILED"):
        try:
            resp = sfn_client.list_executions(
                stateMachineArn=state_machine_arn,
                statusFilter=status_filter,
                maxResults=max_results,
            )
            for execution in resp.get("executions", []):
                arn = execution["executionArn"]
                try:
                    detail = sfn_client.describe_execution(executionArn=arn)
                    payload = json.loads(detail.get("input", "{}"))
                    if payload.get("nova_id") == nova_id:
                        matching.append(arn)
                except Exception:
                    pass
        except Exception:
            pass

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique = []
    for arn in matching:
        if arn not in seen:
            seen.add(arn)
            unique.append(arn)
    return unique


def _wait_for_child(
    sfn_client: Any,
    state_machine_arn: str,
    nova_id: str,
    timeout: int,
    poll_interval: int = 5,
    label: str = "",
) -> str:
    """
    Wait until at least one child execution for nova_id appears on
    state_machine_arn, then poll it to completion. Returns the execution ARN.

    Raises AssertionError if no execution is found within timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        arns = _find_child_executions(sfn_client, state_machine_arn, nova_id)
        if arns:
            arn = arns[0]
            print(f"\n  ✓ Found {label} execution: {arn}")
            return arn
        print(f"  ... waiting for {label} execution to appear", end="\r", flush=True)
        time.sleep(poll_interval)

    pytest.fail(
        f"No {label} execution found for nova_id={nova_id!r} "
        f"within {timeout}s on {state_machine_arn}"
    )


def _poll_to_terminal(
    sfn_client: Any,
    arn: str,
    timeout: int,
    label: str = "",
) -> dict[str, Any]:
    """Poll an execution ARN to completion. Wraps conftest.poll_execution."""
    print(f"  Polling {label} ({arn.split(':')[-1][:40]})...")
    return poll_execution(sfn_client, arn, timeout_seconds=timeout)


# ---------------------------------------------------------------------------
# TestEndToEnd
# ---------------------------------------------------------------------------


class TestEndToEnd:
    """
    Full pipeline smoke test: initialize → ingest → refresh + discover → acquire.

    One test method per pipeline stage, ordered explicitly. Each stage
    depends on state written by the previous one (nova_id propagates via
    the `e2e_state` fixture).
    """

    # ── Shared mutable state fixture ──────────────────────────────────────────

    @pytest.fixture
    def e2e_state(self) -> dict[str, Any]:
        """
        Mutable dict passed between test stages within a single test.
        Keys populated progressively:
          nova_id, ingest_arn, refresh_arn, discover_arn,
          acquire_arns, acquire_results
        """
        return {}

    # ── Main e2e test ─────────────────────────────────────────────────────────

    def test_v1324_sco_full_pipeline(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
    ) -> None:
        """
        Full end-to-end pipeline for V1324 Sco.

        Asserts at each stage as it completes. A failure at any stage
        reports the exact workflow that broke and its execution ARN,
        so you know immediately which link in the chain is broken.
        """
        execution_suffix = str(uuid.uuid4())[:8]
        table = dynamodb_resource.Table(stack.table_name)

        # ── Stage 1: initialize_nova ──────────────────────────────────────────
        _section("Stage 1 — initialize_nova")

        resp = _run_and_wait(
            sfn_client,
            stack.initialize_nova_arn,
            payload={
                "candidate_name": _TEST_NOVA_NAME,
                "correlation_id": f"e2e-{execution_suffix}",
                "source": "e2e_smoke_test",
            },
            suffix=execution_suffix,
            timeout=120,
        )

        assert resp["status"] == "SUCCEEDED", (
            f"initialize_nova FAILED.\n"
            f"Execution: {resp['executionArn']}\n"
            f"Check CloudWatch logs for correlation_id=e2e-{execution_suffix}"
        )

        init_model = _assert_output(InitializeNovaTerminalOutput, _output(resp))

        assert isinstance(init_model.finalize, InitializeNovaFinalizeOutput), (
            f"initialize_nova finalize is not InitializeNovaFinalizeOutput — "
            f"workflow likely failed. Got: {type(init_model.finalize).__name__}"
        )
        assert init_model.finalize.outcome in {"CREATED_AND_LAUNCHED", "EXISTS_AND_LAUNCHED"}, (
            f"Unexpected initialize_nova outcome: {init_model.finalize.outcome!r}.\n"
            f"Expected CREATED_AND_LAUNCHED or EXISTS_AND_LAUNCHED."
        )
        assert init_model.launch is not None, (
            "initialize_nova: launch output absent — ingest_new_nova was not triggered"
        )

        nova_id = init_model.launch.nova_id
        assert nova_id, "initialize_nova: nova_id missing from launch output"

        print(f"\n  nova_id:  {nova_id}")
        print(f"  outcome:  {init_model.finalize.outcome}")
        _ok("initialize_nova")

        # ── DDB: Nova item ────────────────────────────────────────────────────
        nova_item = _get_item(table, nova_id, "NOVA")
        assert nova_item is not None, f"Nova item not found in DynamoDB for nova_id={nova_id}"
        assert nova_item["status"] == "ACTIVE", (
            f"Nova item status is {nova_item['status']!r}, expected ACTIVE"
        )
        assert "ra_deg" in nova_item and "dec_deg" in nova_item, (
            "Nova item missing coordinates — archive resolution may have failed"
        )
        print(f"  DDB Nova: ACTIVE | RA={nova_item['ra_deg']} Dec={nova_item['dec_deg']}")

        # ── Stage 2: ingest_new_nova ──────────────────────────────────────────
        _section("Stage 2 — ingest_new_nova")

        ingest_arn = _wait_for_child(
            sfn_client,
            stack.ingest_new_nova_arn,
            nova_id,
            timeout=60,
            label="ingest_new_nova",
        )
        ingest_resp = _poll_to_terminal(sfn_client, ingest_arn, timeout=45, label="ingest_new_nova")

        assert ingest_resp["status"] == "SUCCEEDED", (
            f"ingest_new_nova FAILED.\n"
            f"Execution: {ingest_arn}\n"
            f"Check CloudWatch logs for nova_id={nova_id}"
        )

        ingest_model = _assert_output(IngestNewNovaTerminalOutput, _output(ingest_resp))

        assert isinstance(ingest_model.finalize, IngestNewNovaFinalizeOutput), (
            f"ingest_new_nova finalize is not IngestNewNovaFinalizeOutput. "
            f"Got: {type(ingest_model.finalize).__name__}"
        )
        assert ingest_model.finalize.outcome == "LAUNCHED", (
            f"ingest_new_nova outcome={ingest_model.finalize.outcome!r}, expected LAUNCHED"
        )
        assert ingest_model.downstream is not None and len(ingest_model.downstream) == 2, (
            f"ingest_new_nova: downstream Parallel output malformed — "
            f"expected 2 branch outputs, got: {ingest_model.downstream}"
        )
        _ok("ingest_new_nova")

        # ── Stage 3: refresh_references ──────────────────────────────────────
        _section("Stage 3 — refresh_references")

        refresh_arn = _wait_for_child(
            sfn_client,
            stack.refresh_references_arn,
            nova_id,
            timeout=30,
            label="refresh_references",
        )
        refresh_resp = _poll_to_terminal(
            sfn_client, refresh_arn, timeout=120, label="refresh_references"
        )

        assert refresh_resp["status"] == "SUCCEEDED", (
            f"refresh_references FAILED.\n"
            f"Execution: {refresh_arn}\n"
            f"Check CloudWatch logs for nova_id={nova_id}"
        )

        refresh_model = _assert_output(RefreshReferencesTerminalOutput, _output(refresh_resp))

        assert isinstance(refresh_model.finalize, RefreshReferencesFinalizeOutput), (
            f"refresh_references finalize is not RefreshReferencesFinalizeOutput. "
            f"Got: {type(refresh_model.finalize).__name__}"
        )
        assert refresh_model.finalize.outcome == "SUCCEEDED"
        assert refresh_model.fetch is not None
        print(f"  ADS candidates fetched: {refresh_model.fetch.candidate_count}")

        # ── DDB: NOVAREF items ────────────────────────────────────────────────
        novarefs = _query_partition(table, nova_id, "NOVAREF#")
        assert len(novarefs) >= 1, (
            f"No NOVAREF items written for nova_id={nova_id} after refresh_references.\n"
            f"ADS returned {refresh_model.fetch.candidate_count} candidate(s) — "
            f"check reference_manager handler."
        )
        print(f"  NOVAREF items in DDB: {len(novarefs)}")

        # ── DDB: discovery_date ───────────────────────────────────────────────
        nova_item = _get_item(table, nova_id, "NOVA")
        discovery_date = (nova_item or {}).get("discovery_date")
        assert discovery_date is not None, (
            "discovery_date not written to Nova item after refresh_references"
        )
        print(f"  discovery_date: {discovery_date}")
        _ok("refresh_references")

        # ── Stage 4: discover_spectra_products ────────────────────────────────
        _section("Stage 4 — discover_spectra_products")

        discover_arn = _wait_for_child(
            sfn_client,
            stack.discover_spectra_products_arn,
            nova_id,
            timeout=30,
            label="discover_spectra_products",
        )
        discover_resp = _poll_to_terminal(
            sfn_client, discover_arn, timeout=90, label="discover_spectra_products"
        )

        assert discover_resp["status"] == "SUCCEEDED", (
            f"discover_spectra_products FAILED.\n"
            f"Execution: {discover_arn}\n"
            f"Check CloudWatch logs for nova_id={nova_id}"
        )

        discover_model = _assert_output(
            DiscoverSpectraProductsTerminalOutput, _output(discover_resp)
        )

        assert isinstance(discover_model.finalize, DiscoverSpectraProductsFinalizeOutput), (
            f"discover_spectra_products finalize is not DiscoverSpectraProductsFinalizeOutput. "
            f"Got: {type(discover_model.finalize).__name__}"
        )
        assert discover_model.finalize.outcome == "COMPLETED"
        assert discover_model.providers == [{"provider": "ESO"}], (
            f"Unexpected providers list: {discover_model.providers!r}"
        )

        # ── DDB: spectra stubs ────────────────────────────────────────────────
        stubs = _query_partition(table, nova_id, "PRODUCT#SPECTRA#")
        print(f"  ESO spectra stubs written: {len(stubs)}")

        if not stubs:
            pytest.skip(
                f"ESO returned no spectra products for V1324 Sco "
                f"(nova_id={nova_id}). "
                f"The cone search may have missed — check the coordinates "
                f"in the Nova item and the ESO adapter search radius."
            )

        for stub in stubs:
            assert stub["acquisition_status"] in {
                "STUB",
                "ACQUIRED",
                "FAILED_RETRYABLE",
            }, f"Unexpected acquisition_status on stub: {stub['acquisition_status']!r}"
            assert "data_product_id" in stub
            assert "GSI1PK" in stub or stub.get("eligibility") == "NONE", (
                f"Stub {stub['data_product_id']} has eligibility=ACQUIRE but GSI1PK is absent"
            )

        _ok("discover_spectra_products")

        # ── Stage 5: acquire_and_validate_spectra (all products) ─────────────
        _section("Stage 5 — acquire_and_validate_spectra")

        # Find all acquire_and_validate executions triggered for this nova.
        # There is one execution per data_product_id. We poll all of them.
        print(f"  Waiting for {len(stubs)} acquire_and_validate execution(s)...")

        acquire_arns = _collect_acquire_executions(
            sfn_client=sfn_client,
            state_machine_arn=stack.acquire_and_validate_spectra_arn,
            nova_id=nova_id,
            expected_count=len(stubs),
            timeout=_ACQUIRE_TOTAL_TIMEOUT,
            poll_interval=_ACQUIRE_POLL_INTERVAL,
        )

        assert len(acquire_arns) > 0, (
            f"No acquire_and_validate_spectra executions found for nova_id={nova_id}.\n"
            f"{len(stubs)} stub(s) were written — check that "
            f"discover_spectra_products published the acquisition events "
            f"and that the CDK stack wires the event to the state machine."
        )

        print(f"  Found {len(acquire_arns)} acquire_and_validate execution(s)")

        # Poll all executions to terminal state
        acquire_results = []
        for arn in acquire_arns:
            resp = _poll_to_terminal(
                sfn_client,
                arn,
                timeout=_ACQUIRE_TIMEOUT_PER_PRODUCT,
                label=f"acquire_and_validate ({arn.split(':')[-1][-12:]})",
            )
            assert resp["status"] == "SUCCEEDED", (
                f"acquire_and_validate_spectra FAILED.\n"
                f"Execution: {arn}\n"
                f"Check CloudWatch logs for nova_id={nova_id}"
            )
            model = _assert_output(AcquireAndValidateSpectraTerminalOutput, _output(resp))
            assert isinstance(model.finalize, AcquireAndValidateSpectraFinalizeOutput), (
                f"acquire_and_validate finalize type unexpected: {type(model.finalize).__name__}"
            )
            assert model.finalize.outcome == "COMPLETED"
            acquire_results.append(model)

        # ── Final DDB assertions ──────────────────────────────────────────────
        _section("Final DynamoDB assertions")

        final_stubs = _query_partition(table, nova_id, "PRODUCT#SPECTRA#")

        validation_statuses = [s.get("validation_status") for s in final_stubs]
        print(f"  Spectra validation statuses: {validation_statuses}")

        valid_count = validation_statuses.count("VALID")
        quarantined_count = validation_statuses.count("QUARANTINED")
        terminal_count = validation_statuses.count("TERMINAL_INVALID")

        print(f"  VALID:            {valid_count}")
        print(f"  QUARANTINED:      {quarantined_count}")
        print(f"  TERMINAL_INVALID: {terminal_count}")

        # At least one product must reach VALID for first light to be declared.
        assert valid_count >= 1, (
            f"No spectra products reached VALID for V1324 Sco (nova_id={nova_id}).\n"
            f"Statuses: {validation_statuses}\n"
            f"Check acquire_and_validate_spectra CloudWatch logs and the "
            f"ESO FITS profile registry. All products may be quarantined "
            f"(UNKNOWN_PROFILE) if the profile hasn't been registered."
        )

        # All validated products must have eligibility=NONE and no GSI1 attributes.
        for stub in final_stubs:
            vs = stub.get("validation_status")
            if vs in {"VALID", "QUARANTINED", "TERMINAL_INVALID"}:
                assert stub.get("eligibility") == "NONE", (
                    f"Product {stub['data_product_id']} has validation_status={vs!r} "
                    f"but eligibility={stub.get('eligibility')!r} — "
                    f"GSI1 was not cleared by RecordValidationResult"
                )
                assert "GSI1PK" not in stub, (
                    f"Product {stub['data_product_id']} has validation_status={vs!r} "
                    f"but GSI1PK is still present — EligibilityIndex was not cleared"
                )

        _ok(f"acquire_and_validate_spectra — {valid_count} VALID product(s)")
        print()
        print("  ══════════════════════════════════════════════════")
        print("  ✓  FIRST LIGHT — V1324 Sco ingested successfully")
        print(f"     nova_id:       {nova_id}")
        print(f"     discovery_date: {discovery_date}")
        print(f"     references:    {len(novarefs)}")
        print(f"     spectra valid: {valid_count} / {len(final_stubs)}")
        print("  ══════════════════════════════════════════════════")


# ---------------------------------------------------------------------------
# acquire_and_validate collection helper
# ---------------------------------------------------------------------------


def _collect_acquire_executions(
    sfn_client: Any,
    state_machine_arn: str,
    nova_id: str,
    expected_count: int,
    timeout: int,
    poll_interval: int,
) -> list[str]:
    """
    Collect acquire_and_validate_spectra execution ARNs for a given nova_id.

    Polls until `expected_count` executions have appeared or timeout is hit.
    Returns whatever has been found at timeout — the caller asserts on count.

    acquire_and_validate payloads include nova_id directly (unlike
    ingest_new_nova which has it too), so _find_child_executions works here.
    """
    deadline = time.time() + timeout
    found: set[str] = set()

    while time.time() < deadline:
        arns = _find_child_executions(sfn_client, state_machine_arn, nova_id, max_results=50)
        for arn in arns:
            found.add(arn)

        print(
            f"  ... {len(found)}/{expected_count} acquire executions found",
            end="\r",
            flush=True,
        )

        if len(found) >= expected_count:
            break

        time.sleep(poll_interval)

    print()  # clear the \r line
    return list(found)


# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------


def _section(title: str) -> None:
    print(f"\n  ── {title} {'─' * max(0, 52 - len(title))}")


def _ok(label: str) -> None:
    print(f"  ✓ {label} — PASSED")
