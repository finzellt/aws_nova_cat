"""
Unit tests for services/nova_resolver_ticket/handler.py

Strategy
--------
All three patchable module-level names are targeted by dotted string path so
that each test patches the name *where the handler uses it*, not where it is
defined:

  nova_resolver_ticket.handler._table  — DDB Table resource
  nova_resolver_ticket.handler._sfn    — SFN boto3 client
  nova_resolver_ticket.handler._sleep  — patchable sleep alias

No moto. No real AWS calls.  The handler module is evicted from sys.modules
and re-imported fresh for each test via the ``handler`` fixture so that
module-level globals that read os.environ at import time are always
initialised correctly against monkeypatched values.

Coverage
--------
 1. TestPreflightHitWithCoordinates      — NameMapping hit; Nova has coords
 2. TestPreflightHitCoordinatesAbsent    — NameMapping hit; Nova has no coords
 3. TestPreflightMissCreatedAndLaunched  — SFN → CREATED_AND_LAUNCHED
 4. TestPreflightMissExistsAndLaunched   — SFN → EXISTS_AND_LAUNCHED
 5. TestNotFoundOutcome                  — SFN → NOT_FOUND → QuarantineError
 6. TestQuarantinedPathAbsentOutcome     — SFN SUCCEEDS, outcome absent
 7. TestSfnExecutionFailed               — SFN status FAILED → TerminalError
 8. TestSfnExecutionTimedOut             — SFN status TIMED_OUT → TerminalError
 9. TestNovaItemAbsentAfterResolution    — get_item empty post-resolution
10. TestWrongTaskName                    — bad task_name → TerminalError
11. TestNormalization                    — whitespace/case → "NAME#gq mus"
12. TestPollLoop                         — RUNNING×2 then SUCCEEDED; sleep×2
"""

from __future__ import annotations

import importlib
import json
import sys
from decimal import Decimal
from typing import Any
from unittest.mock import call, patch

import pytest
from nova_common.errors import QuarantineError, TerminalError

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_MODULE = "nova_resolver_ticket.handler"
_TABLE_NAME = "NovaCat-Test"
_SFN_ARN = "arn:aws:states:us-east-1:123456789012:stateMachine:nova-cat-init-nova-test"
_EXEC_ARN = "arn:aws:states:us-east-1:123456789012:execution:nova-cat-init-nova-test:run-1"
_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"

# Decimal values mirror how nova_resolver stores coordinates in DDB.
# Used to exercise the Decimal → float conversion inside _fetch_coordinates.
_RA = Decimal("123.456")
_DEC = Decimal("-45.678")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Inject every env var the handler reads at module scope.

    autouse=True ensures these are set before the ``handler`` fixture imports
    the module, even though pytest resolves autouse fixtures first by
    convention.  The ``handler`` fixture also declares ``_env`` as an explicit
    parameter to make the dependency chain unambiguous.
    """
    monkeypatch.setenv("NOVA_CAT_TABLE_NAME", _TABLE_NAME)
    monkeypatch.setenv("INITIALIZE_NOVA_STATE_MACHINE_ARN", _SFN_ARN)
    monkeypatch.setenv("POWERTOOLS_SERVICE_NAME", "nova-cat-test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    # Satisfy boto3 region/credential checks at module import time.
    # The actual clients are patched in each test; these values are never
    # used to make real AWS calls.
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture()
def handler(_env: None) -> Any:
    """
    Return a freshly imported copy of the handler module.

    Evicts the module from sys.modules before importing so that the
    monkeypatched env vars are visible at module-scope import time.
    Typed as ``Any`` so mypy permits attribute access (handler.handle, etc.)
    without requiring a stub for a dynamically imported module.
    """
    sys.modules.pop(_MODULE, None)
    return importlib.import_module(_MODULE)


# ---------------------------------------------------------------------------
# SFN response helpers
# ---------------------------------------------------------------------------


def _sfn_output(outcome: str | None, nova_id: str | None = None) -> str:
    """
    Build the JSON string placed in a DescribeExecution response's ``output``
    field.

    ``outcome=None`` produces an empty ``finalize`` dict, simulating the
    coordinate-ambiguity quarantine path: initialize_nova ends via
    FinalizeJobRunQuarantined which does not populate $.finalize.outcome.
    """
    finalize: dict[str, Any] = {}
    if outcome is not None:
        finalize["outcome"] = outcome
    if nova_id is not None:
        finalize["nova_id"] = nova_id
    return json.dumps({"finalize": finalize})


def _sfn_describe(status: str, output: str | None = None) -> dict[str, Any]:
    """Minimal DescribeExecution response dict."""
    resp: dict[str, Any] = {"status": status}
    if output is not None:
        resp["output"] = output
    return resp


# ---------------------------------------------------------------------------
# DDB item helper
# ---------------------------------------------------------------------------


def _nova_item(
    *,
    ra: Decimal | None = _RA,
    dec: Decimal | None = _DEC,
) -> dict[str, Any]:
    """
    Simulate a successful DDB get_item response for a Nova record.

    Pass ``ra=None, dec=None`` to produce a Nova item that has no coordinate
    fields, which is legitimate for novae resolved via TNS without positional
    data.
    """
    item: dict[str, Any] = {
        "PK": _NOVA_ID,
        "SK": "NOVA",
        "nova_id": _NOVA_ID,
    }
    if ra is not None:
        item["ra_deg"] = ra
    if dec is not None:
        item["dec_deg"] = dec
    return {"Item": item}


# ===========================================================================
# 1. Preflight hit — NameMapping found, coordinates present
# ===========================================================================


class TestPreflightHitWithCoordinates:
    """DDB query returns a NameMapping item; Nova item carries ra_deg / dec_deg."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_returns_correct_fields(self, handler: Any) -> None:
        """
        Result has correct nova_id, primary_name, ra_deg, and dec_deg.

        Exercises the Decimal → float conversion: Decimal("123.456") stored
        in the mock Nova item must emerge as a Python float (not Decimal) in
        the returned dict.
        """
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn"),
        ):
            mock_table.query.return_value = {"Items": [{"nova_id": _NOVA_ID}]}
            mock_table.get_item.return_value = _nova_item(ra=_RA, dec=_DEC)

            result: dict[str, Any] = handler.handle(self._make_event(), None)

        assert result["nova_id"] == _NOVA_ID
        assert result["primary_name"] == "GQ Mus"
        assert result["ra_deg"] == float(_RA)
        assert result["dec_deg"] == float(_DEC)
        # Explicit type check: the handler must convert Decimal, not pass it through.
        assert isinstance(result["ra_deg"], float)
        assert isinstance(result["dec_deg"], float)

    def test_sfn_not_called(self, handler: Any) -> None:
        """start_execution is never invoked when the preflight DDB query hits."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
        ):
            mock_table.query.return_value = {"Items": [{"nova_id": _NOVA_ID}]}
            mock_table.get_item.return_value = _nova_item()

            handler.handle(self._make_event(), None)

            mock_sfn.start_execution.assert_not_called()


# ===========================================================================
# 2. Preflight hit — coordinates absent from Nova item
# ===========================================================================


class TestPreflightHitCoordinatesAbsent:
    """NameMapping exists but the Nova item has no ra_deg / dec_deg fields."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_coordinates_are_none(self, handler: Any) -> None:
        """ra_deg and dec_deg are None when the Nova item carries no coord fields."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn"),
        ):
            mock_table.query.return_value = {"Items": [{"nova_id": _NOVA_ID}]}
            mock_table.get_item.return_value = _nova_item(ra=None, dec=None)

            result: dict[str, Any] = handler.handle(self._make_event(), None)

        assert result["ra_deg"] is None
        assert result["dec_deg"] is None


# ===========================================================================
# 3. Preflight miss → CREATED_AND_LAUNCHED
# ===========================================================================


class TestPreflightMissCreatedAndLaunched:
    """DDB query returns nothing; SFN execution succeeds with CREATED_AND_LAUNCHED."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "V1500 Cyg"}

    def test_nova_id_extracted_from_sfn_output(self, handler: Any) -> None:
        """
        nova_id is taken from $.finalize.nova_id in the SFN execution output.
        primary_name is the original object_name, not the normalized form.
        """
        sfn_out = _sfn_output("CREATED_AND_LAUNCHED", _NOVA_ID)
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep"),
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.return_value = _sfn_describe("SUCCEEDED", sfn_out)
            mock_table.get_item.return_value = _nova_item()

            result: dict[str, Any] = handler.handle(self._make_event(), None)

        assert result["nova_id"] == _NOVA_ID
        assert result["primary_name"] == "V1500 Cyg"


# ===========================================================================
# 4. Preflight miss → EXISTS_AND_LAUNCHED
# ===========================================================================


class TestPreflightMissExistsAndLaunched:
    """DDB query returns nothing; SFN execution succeeds with EXISTS_AND_LAUNCHED."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "V1500 Cyg"}

    def test_nova_id_extracted_from_sfn_output(self, handler: Any) -> None:
        """
        EXISTS_AND_LAUNCHED is a member of _RESOLVED_OUTCOMES and must also
        yield a valid nova_id from $.finalize.nova_id.
        """
        sfn_out = _sfn_output("EXISTS_AND_LAUNCHED", _NOVA_ID)
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep"),
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.return_value = _sfn_describe("SUCCEEDED", sfn_out)
            mock_table.get_item.return_value = _nova_item()

            result: dict[str, Any] = handler.handle(self._make_event(), None)

        assert result["nova_id"] == _NOVA_ID


# ===========================================================================
# 5. NOT_FOUND outcome
# ===========================================================================


class TestNotFoundOutcome:
    """SFN execution SUCCEEDS but outcome is NOT_FOUND → QuarantineError."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "Mystery Object"}

    def test_raises_quarantine_error(self, handler: Any) -> None:
        """
        QuarantineError("UNRESOLVABLE_OBJECT_NAME") is raised.
        The object name was not found in any archive — the ticket cannot be
        ingested without operator intervention.
        """
        sfn_out = _sfn_output("NOT_FOUND")
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep"),
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.return_value = _sfn_describe("SUCCEEDED", sfn_out)

            with pytest.raises(QuarantineError, match="UNRESOLVABLE_OBJECT_NAME"):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 6. QUARANTINED path — $.finalize.outcome absent
# ===========================================================================


class TestQuarantinedPathAbsentOutcome:
    """
    SFN execution SUCCEEDS but $.finalize.outcome is absent.

    This is the coordinate-ambiguity quarantine branch: initialize_nova ends
    via FinalizeJobRunQuarantined, which does not write $.finalize.outcome.
    The handler must treat any SUCCEEDED execution without a recognised
    outcome as IDENTITY_AMBIGUITY.
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "Ambiguous Nova"}

    def test_raises_identity_ambiguity(self, handler: Any) -> None:
        """QuarantineError("IDENTITY_AMBIGUITY") raised when outcome is absent."""
        # _sfn_output(None) → {"finalize": {}} — no "outcome" key at all.
        sfn_out = _sfn_output(None)
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep"),
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.return_value = _sfn_describe("SUCCEEDED", sfn_out)

            with pytest.raises(QuarantineError, match="IDENTITY_AMBIGUITY"):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 7. SFN execution FAILED
# ===========================================================================


class TestSfnExecutionFailed:
    """DescribeExecution returns status FAILED → TerminalError."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_raises_terminal_error(self, handler: Any) -> None:
        """TerminalError raised when the SFN execution terminates with FAILED."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep"),
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.return_value = _sfn_describe("FAILED")

            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 8. SFN execution TIMED_OUT
# ===========================================================================


class TestSfnExecutionTimedOut:
    """DescribeExecution returns status TIMED_OUT → TerminalError."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_raises_terminal_error(self, handler: Any) -> None:
        """TerminalError raised when the SFN execution terminates with TIMED_OUT."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep"),
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.return_value = _sfn_describe("TIMED_OUT")

            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 9. Nova item absent after resolution
# ===========================================================================


class TestNovaItemAbsentAfterResolution:
    """
    SFN returns CREATED_AND_LAUNCHED with a valid nova_id, but the subsequent
    DDB get_item finds no Nova item.

    This represents an infrastructure invariant violation: a nova_id was
    assigned by initialize_nova but the Nova record does not exist in the
    table immediately afterward.
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_raises_terminal_error(self, handler: Any) -> None:
        """TerminalError raised when DDB get_item returns no Item post-resolution."""
        sfn_out = _sfn_output("CREATED_AND_LAUNCHED", _NOVA_ID)
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep"),
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.return_value = _sfn_describe("SUCCEEDED", sfn_out)
            # Simulate get_item returning a response with no "Item" key.
            mock_table.get_item.return_value = {}

            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 10. Wrong task_name
# ===========================================================================


class TestWrongTaskName:
    """task_name != "ResolveNova" → TerminalError before any AWS call is made."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "SomethingElse", "object_name": "GQ Mus"}

    def test_raises_terminal_error(self, handler: Any) -> None:
        """
        TerminalError raised immediately on task_name mismatch.
        No DDB query and no SFN start_execution call must be made.
        """
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
        ):
            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)

            mock_table.query.assert_not_called()
            mock_sfn.start_execution.assert_not_called()


# ===========================================================================
# 11. Normalization
# ===========================================================================


class TestNormalization:
    """
    object_name with mixed case and extra leading, trailing, and internal
    whitespace must be normalised before the DDB NameMapping query:
      "  GQ  Mus  " → strip → lowercase → collapse whitespace → "gq mus"
    The key expression must therefore be built from "NAME#gq mus".
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "  GQ  Mus  "}

    def test_query_key_is_normalized(self, handler: Any) -> None:
        """
        Patch nova_resolver_ticket.handler.Key and assert it was called with
        "PK" and that .eq() received "NAME#gq mus".

        Routing the preflight to a hit (mock query returns a NameMapping item)
        keeps the test focused on normalization without requiring SFN setup.
        """
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn"),
            patch(f"{_MODULE}.Key") as mock_key,
        ):
            mock_table.query.return_value = {"Items": [{"nova_id": _NOVA_ID}]}
            mock_table.get_item.return_value = _nova_item()

            handler.handle(self._make_event(), None)

        mock_key.assert_called_once_with("PK")
        mock_key.return_value.eq.assert_called_once_with("NAME#gq mus")


# ===========================================================================
# 12. Poll loop — RUNNING × 2 then SUCCEEDED
# ===========================================================================


class TestPollLoop:
    """
    _poll_until_terminal loops until a terminal SFN status.
    When describe_execution returns RUNNING twice before SUCCEEDED, the
    patchable _sleep alias must be called exactly twice, each time with the
    argument 2 (seconds).
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_sleep_called_twice_with_two_seconds(self, handler: Any) -> None:
        """
        _sleep(2) invoked exactly twice: once after each RUNNING poll response.

        Uses side_effect list on describe_execution to deliver the sequence:
          RUNNING → RUNNING → SUCCEEDED
        then asserts call_count == 2 and call_args_list == [call(2), call(2)].
        """
        sfn_out = _sfn_output("CREATED_AND_LAUNCHED", _NOVA_ID)
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep") as mock_sleep,
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.side_effect = [
                _sfn_describe("RUNNING"),
                _sfn_describe("RUNNING"),
                _sfn_describe("SUCCEEDED", sfn_out),
            ]
            mock_table.get_item.return_value = _nova_item()

            handler.handle(self._make_event(), None)

        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list == [call(2), call(2)]


# ===========================================================================
# 13. NOT_A_CLASSICAL_NOVA outcome
# ===========================================================================


class TestNotAClassicalNovaOutcome:
    """
    SFN execution SUCCEEDS with outcome NOT_A_CLASSICAL_NOVA → TerminalError.

    This is distinct from NOT_FOUND: the object was found and resolved by
    SIMBAD, but is not a classical nova (e.g. a recurrent nova).  The ticket
    should be removed or reclassified — this is a terminal condition, not a
    quarantine condition, because it reflects an operator data-quality error
    rather than an ambiguous identity.
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "RS Oph"}

    def test_raises_terminal_error(self, handler: Any) -> None:
        """TerminalError raised when initialize_nova returns NOT_A_CLASSICAL_NOVA."""
        sfn_out = _sfn_output("NOT_A_CLASSICAL_NOVA")
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep"),
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.return_value = _sfn_describe("SUCCEEDED", sfn_out)

            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 14. Poll loop exhausted — _MAX_POLL_ATTEMPTS reached
# ===========================================================================


class TestPollLoopExhausted:
    """
    _poll_until_terminal raises TerminalError after _MAX_POLL_ATTEMPTS
    consecutive non-terminal responses, rather than looping forever.

    The test drives the mock to return RUNNING for every describe_execution
    call (using side_effect=repeat) and asserts that:
      - TerminalError is raised
      - _sleep was called exactly _MAX_POLL_ATTEMPTS times (once per RUNNING
        response before the cap is hit and the loop exits without a final
        sleep)

    Note: the loop structure is ``for attempt in range(1, _MAX_POLL_ATTEMPTS + 1)``,
    calling _sleep inside the loop body when status is non-terminal, so
    _sleep is called _MAX_POLL_ATTEMPTS times total before the post-loop
    TerminalError is raised.
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_raises_terminal_error_after_max_attempts(self, handler: Any) -> None:
        """TerminalError raised when RUNNING persists for _MAX_POLL_ATTEMPTS calls."""
        from itertools import repeat

        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}._sleep") as mock_sleep,
            patch(f"{_MODULE}._MAX_POLL_ATTEMPTS", 3),  # keep the test fast
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_execution.return_value = {"executionArn": _EXEC_ARN}
            mock_sfn.describe_execution.side_effect = repeat(_sfn_describe("RUNNING"))

            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)

        # _sleep called once per loop iteration (3 iterations at patched cap).
        assert mock_sleep.call_count == 3
