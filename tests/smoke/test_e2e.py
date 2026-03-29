"""
End-to-end pipeline smoke test (outcome-driven).

Runs the full NovaCat ingestion chain against NovaCatSmoke for V1324 Sco:

    initialize_nova
        → ingest_new_nova
            → refresh_references          (parallel)
            → discover_spectra_products   (parallel)
                → acquire_and_validate_spectra  (one per product)

Unlike the previous version that tracked downstream execution ARNs, this
test verifies **outcomes** by polling DynamoDB for the records each
workflow writes. The only workflow started directly is initialize_nova;
everything else is detected through its side-effects.

Stages and what they verify:
  Stage 1–2: Nova item exists in DDB
             (confirms initialize_nova + ingest_new_nova)
  Stage 3:   At least one NOVAREF# item exists
             (confirms refresh_references)
  Stage 4:   At least one PRODUCT#SPECTRA# stub exists
             (confirms discover_spectra_products)
  Stage 5:   At least one stub has validation_status != UNVALIDATED
             (confirms acquire_and_validate_spectra)

Design notes:
  - Uses the real V1324 Sco name so the full archive resolution pipeline
    runs (SIMBAD → ESO SSAP → ESO download + FITS validation).
  - Does NOT pass suppress_downstream to any workflow — downstream
    workflows fire naturally and their DDB side-effects are polled.
  - Each stage polls independently with its own timeout. Timeouts are
    generous to account for external archive latency (SIMBAD, ADS, ESO).
  - On timeout, pytest.fail includes the nova_id and what was/wasn't
    found in DynamoDB so failures are immediately diagnosable.

Timeouts:
  Stage 1–2 (Nova item)                120s  (SIMBAD + TNS resolution)
  Stage 3   (NOVAREF# items)           120s  (ADS API)
  Stage 4   (PRODUCT#SPECTRA# stubs)    90s  (ESO SSAP)
  Stage 5   (validation_status change)  180s  (ESO FITS download + validation)
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import pytest
from boto3.dynamodb.conditions import Key

from tests.smoke.conftest import StackOutputs

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# V1324 Sco — classical nova, unambiguous in SIMBAD/TNS, two UVES spectra
# in the ESO archive (~June 2012). Same test nova as test_workflows.py.
_TEST_NOVA_NAME = "V1324 Sco"

_POLL_INTERVAL = 5  # seconds — matches conftest._POLL_INTERVAL_SECONDS


# ---------------------------------------------------------------------------
# DDB polling helpers
# ---------------------------------------------------------------------------


def _get_item(table: Any, pk: str, sk: str) -> dict[str, Any] | None:
    """Fetch a single item by exact PK + SK."""
    resp = table.get_item(Key={"PK": pk, "SK": sk})
    return resp.get("Item")  # type: ignore[no-any-return]


def _query_prefix(table: Any, pk: str, sk_prefix: str) -> list[dict[str, Any]]:
    """Query all items under a PK with SK beginning with sk_prefix."""
    resp = table.query(
        KeyConditionExpression=(Key("PK").eq(pk) & Key("SK").begins_with(sk_prefix)),
    )
    items: list[dict[str, Any]] = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.query(
            KeyConditionExpression=(Key("PK").eq(pk) & Key("SK").begins_with(sk_prefix)),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))
    return items


def _poll_ddb(
    check_fn: Any,
    timeout_seconds: int,
    label: str,
    nova_id: str,
) -> Any:
    """Poll a DDB check function until it returns a truthy value or timeout.

    Parameters
    ----------
    check_fn:
        Callable returning a truthy result when the condition is met,
        or a falsy value to keep polling.
    timeout_seconds:
        Maximum wall-clock seconds to poll.
    label:
        Human-readable label for the stage (used in failure messages).
    nova_id:
        Nova UUID string (included in failure messages for diagnosis).

    Returns the truthy result from check_fn.
    Calls pytest.fail on timeout.
    """
    deadline = time.monotonic() + timeout_seconds
    result = None
    while time.monotonic() < deadline:
        result = check_fn()
        if result:
            return result
        time.sleep(_POLL_INTERVAL)

    pytest.fail(
        f"{label} not satisfied within {timeout_seconds}s for "
        f"nova_id={nova_id!r}. Last check returned: {result!r}"
    )


# ---------------------------------------------------------------------------
# Console output helpers
# ---------------------------------------------------------------------------


def _section(title: str) -> None:
    """Print a section header for streaming test output (-s flag)."""
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def _ok(msg: str) -> None:
    """Print a green-ish success marker."""
    print(f"  ✓ {msg}")


# ---------------------------------------------------------------------------
# TestEndToEnd
# ---------------------------------------------------------------------------


class TestEndToEnd:
    """
    Full pipeline smoke test: initialize → ingest → refresh + discover → acquire.

    Verifies outcomes via DynamoDB polling rather than tracking execution ARNs.
    The only workflow started directly is initialize_nova; all downstream
    workflows are confirmed by their DDB side-effects.
    """

    def test_v1324_sco_full_pipeline(
        self,
        stack: StackOutputs,
        sfn_client: Any,
        dynamodb_resource: Any,
    ) -> None:
        """
        Full end-to-end pipeline for V1324 Sco.

        Polls DynamoDB at each stage for the records written by the
        corresponding workflow. A timeout at any stage reports exactly
        what was expected and what was found.
        """
        execution_suffix = str(uuid.uuid4())[:8]
        correlation_id = f"e2e-{execution_suffix}"
        table = dynamodb_resource.Table(stack.table_name)

        # ══════════════════════════════════════════════════════════════════
        # Stage 0: Start initialize_nova (the only directly-started SFN)
        # ══════════════════════════════════════════════════════════════════
        _section("Stage 0 — start initialize_nova")

        # All NovaCat state machines are Express workflows.
        # start_sync_execution blocks until the execution completes and
        # returns status + output directly — no polling needed.
        init_resp = sfn_client.start_sync_execution(
            stateMachineArn=stack.initialize_nova_arn,
            name=f"e2e-{execution_suffix}",
            input=json.dumps(
                {
                    "candidate_name": _TEST_NOVA_NAME,
                    "correlation_id": correlation_id,
                    "source": "e2e_smoke_test",
                }
            ),
        )

        init_arn = init_resp.get("executionArn", "unknown")
        print(f"  Execution ARN: {init_arn}")
        print(f"  correlation_id: {correlation_id}")

        assert init_resp["status"] == "SUCCEEDED", (
            f"initialize_nova FAILED.\n"
            f"Execution: {init_arn}\n"
            f"Status: {init_resp['status']}\n"
            f"Check CloudWatch Logs for correlation_id={correlation_id}"
        )

        init_output = json.loads(init_resp["output"])

        # Extract nova_id — available on both CREATED_AND_LAUNCHED and
        # EXISTS_AND_LAUNCHED outcome paths.
        finalize = init_output.get("finalize", {})
        outcome = finalize.get("outcome", "")
        assert outcome in {"CREATED_AND_LAUNCHED", "EXISTS_AND_LAUNCHED"}, (
            f"Unexpected initialize_nova outcome: {outcome!r}"
        )

        launch = init_output.get("launch", {})
        nova_id: str = launch.get("nova_id", "")
        assert nova_id, (
            f"nova_id not found in initialize_nova output.\n"
            f"Output keys: {sorted(init_output.keys())}\n"
            f"launch keys: {sorted(launch.keys())}"
        )

        print(f"  outcome: {outcome}")
        print(f"  nova_id: {nova_id}")
        _ok("initialize_nova SUCCEEDED")

        # ══════════════════════════════════════════════════════════════════
        # Stage 1–2: Poll for Nova item in DDB
        # Confirms: initialize_nova wrote the Nova record, and
        # ingest_new_nova was triggered (it reads and potentially updates
        # the Nova item, and fires downstream workflows).
        # ══════════════════════════════════════════════════════════════════
        _section("Stage 1–2 — Poll for Nova item")

        def _check_nova_item() -> dict[str, Any] | None:
            item = _get_item(table, nova_id, "NOVA")
            if item and item.get("status") == "ACTIVE":
                return item
            return None

        nova_item = _poll_ddb(
            _check_nova_item,
            timeout_seconds=120,
            label="Nova item with status=ACTIVE",
            nova_id=nova_id,
        )

        primary_name = nova_item.get("primary_name", "")
        ra = nova_item.get("ra_deg")
        dec = nova_item.get("dec_deg")
        print(f"  primary_name: {primary_name}")
        print(f"  ra_deg: {ra}")
        print(f"  dec_deg: {dec}")

        assert ra is not None and dec is not None, (
            f"Nova item missing coordinates: ra_deg={ra}, dec_deg={dec}.\n"
            f"SIMBAD resolution may have failed silently."
        )
        _ok("Nova item exists with ACTIVE status and coordinates")

        # ══════════════════════════════════════════════════════════════════
        # Stage 3: Poll for NOVAREF# items
        # Confirms: refresh_references ran, queried ADS, and wrote at
        # least one NovaReference link item.
        # ══════════════════════════════════════════════════════════════════
        _section("Stage 3 — Poll for NOVAREF# items (refresh_references)")

        def _check_novarefs() -> list[dict[str, Any]] | None:
            items = _query_prefix(table, nova_id, "NOVAREF#")
            return items if items else None

        novarefs = _poll_ddb(
            _check_novarefs,
            timeout_seconds=120,
            label="At least one NOVAREF# item",
            nova_id=nova_id,
        )

        print(f"  NOVAREF# items found: {len(novarefs)}")

        # discovery_date must be derived from ADS references for V1324 Sco.
        nova_item_refreshed = _get_item(table, nova_id, "NOVA")
        discovery_date = (nova_item_refreshed or {}).get("discovery_date")
        assert discovery_date is not None, (
            f"discovery_date not written to Nova item after refresh_references.\n"
            f"nova_id={nova_id}, NOVAREF# count={len(novarefs)}.\n"
            f"ADS may have returned references without a derivable discovery date, "
            f"or the discovery_date extraction logic has a bug."
        )
        print(f"  discovery_date: {discovery_date}")

        _ok(f"refresh_references — {len(novarefs)} reference(s) found")

        # ══════════════════════════════════════════════════════════════════
        # Stage 4: Poll for PRODUCT#SPECTRA# stubs
        # Confirms: discover_spectra_products ran, queried the ESO SSAP
        # archive, and wrote DataProduct stubs for discovered spectra.
        # ══════════════════════════════════════════════════════════════════
        _section("Stage 4 — Poll for PRODUCT#SPECTRA# stubs (discover_spectra_products)")

        def _check_spectra_stubs() -> list[dict[str, Any]] | None:
            items = _query_prefix(table, nova_id, "PRODUCT#SPECTRA#")
            return items if items else None

        stubs = _poll_ddb(
            _check_spectra_stubs,
            timeout_seconds=90,
            label="At least one PRODUCT#SPECTRA# stub",
            nova_id=nova_id,
        )

        print(f"  PRODUCT#SPECTRA# stubs found: {len(stubs)}")
        for stub in stubs:
            dp_id = stub.get("data_product_id", "?")
            vs = stub.get("validation_status", "?")
            print(f"    {dp_id[:12]}…  validation_status={vs}")

        _ok(f"discover_spectra_products — {len(stubs)} stub(s) written")

        # ══════════════════════════════════════════════════════════════════
        # Stage 5: Poll for validation_status != UNVALIDATED
        # Confirms: acquire_and_validate_spectra ran for at least one
        # product and reached a terminal validation state (VALID,
        # QUARANTINED, or TERMINAL_INVALID).
        # ══════════════════════════════════════════════════════════════════
        _section("Stage 5 — Poll for spectra validation (acquire_and_validate_spectra)")

        expected_count = len(stubs)
        print(f"  Waiting for {expected_count} stub(s) to leave UNVALIDATED state…")

        def _check_validation_progress() -> list[dict[str, Any]] | None:
            current = _query_prefix(table, nova_id, "PRODUCT#SPECTRA#")
            resolved = [s for s in current if s.get("validation_status") != "UNVALIDATED"]
            if resolved:
                return resolved
            return None

        _poll_ddb(
            _check_validation_progress,
            timeout_seconds=180,
            label="At least one stub with validation_status != UNVALIDATED",
            nova_id=nova_id,
        )

        # Collect final state of all stubs (some may still be UNVALIDATED
        # if acquire_and_validate hasn't reached them yet — that's fine as
        # long as at least one has been processed).
        final_stubs = _query_prefix(table, nova_id, "PRODUCT#SPECTRA#")

        validation_statuses = [s.get("validation_status") for s in final_stubs]
        valid_count = validation_statuses.count("VALID")
        quarantined_count = validation_statuses.count("QUARANTINED")
        terminal_count = validation_statuses.count("TERMINAL_INVALID")
        unvalidated_count = validation_statuses.count("UNVALIDATED")

        print("  Final validation statuses:")
        print(f"    VALID:            {valid_count}")
        print(f"    QUARANTINED:      {quarantined_count}")
        print(f"    TERMINAL_INVALID: {terminal_count}")
        print(f"    UNVALIDATED:      {unvalidated_count}")

        # At least one product must reach VALID for first light.
        assert valid_count >= 1, (
            f"No spectra products reached VALID for V1324 Sco "
            f"(nova_id={nova_id}).\n"
            f"Statuses: {validation_statuses}\n"
            f"Check acquire_and_validate_spectra CloudWatch logs and the "
            f"ESO FITS profile registry. All products may be quarantined "
            f"(UNKNOWN_PROFILE) if the profile hasn't been registered."
        )

        # All resolved products must have eligibility=NONE and no GSI1 keys.
        for stub in final_stubs:
            vs = stub.get("validation_status")
            if vs in {"VALID", "QUARANTINED", "TERMINAL_INVALID"}:
                dp_id = stub.get("data_product_id", "?")
                assert stub.get("eligibility") == "NONE", (
                    f"Product {dp_id} has validation_status={vs!r} "
                    f"but eligibility={stub.get('eligibility')!r} — "
                    f"GSI1 was not cleared by RecordValidationResult"
                )
                assert "GSI1PK" not in stub, (
                    f"Product {dp_id} has validation_status={vs!r} "
                    f"but GSI1PK is still present — EligibilityIndex "
                    f"was not cleared"
                )

        _ok(f"acquire_and_validate_spectra — {valid_count} VALID product(s)")

        # ══════════════════════════════════════════════════════════════════
        # Summary
        # ══════════════════════════════════════════════════════════════════
        print()
        print("  ══════════════════════════════════════════════════")
        print("  ✓  FIRST LIGHT — V1324 Sco ingested successfully")
        print(f"     nova_id:        {nova_id}")
        print(f"     discovery_date: {discovery_date}")
        print(f"     references:     {len(novarefs)}")
        print(f"     spectra valid:  {valid_count} / {len(final_stubs)}")
        print("  ══════════════════════════════════════════════════")
