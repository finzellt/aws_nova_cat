"""
Unit tests for services/nova_resolver_ticket/handler.py

Strategy
--------
All patchable module-level names are targeted by dotted string path so
that each test patches the name *where the handler uses it*, not where it is
defined:

  nova_resolver_ticket.handler._table  — DDB Table resource
  nova_resolver_ticket.handler._sfn    — SFN boto3 client

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
12. TestNotAClassicalNovaOutcome         — SFN → NOT_A_CLASSICAL_NOVA → TerminalError
"""

from __future__ import annotations

import importlib
import json
import sys
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from nova_common.errors import TerminalError

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_MODULE = "nova_resolver_ticket.handler"
_TABLE_NAME = "NovaCat-Test"
_SFN_ARN = "arn:aws:states:us-east-1:123456789012:stateMachine:nova-cat-init-nova-test"
_EXEC_ARN = "arn:aws:states:us-east-1:123456789012:express:nova-cat-init-nova-test:run-1"
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
    Build the JSON string placed in a start_sync_execution response's ``output``
    field, matching the actual initialize_nova output structure.

    nova_id is written to $.launch.nova_id (not $.finalize.nova_id).
    ``outcome=None`` produces an empty ``finalize`` dict, simulating the
    coordinate-ambiguity quarantine path: initialize_nova ends via
    FinalizeJobRunQuarantined which does not populate $.finalize.outcome.
    """
    finalize: dict[str, Any] = {}
    if outcome is not None:
        finalize["outcome"] = outcome
    output: dict[str, Any] = {"finalize": finalize}
    if nova_id is not None:
        output["launch"] = {"nova_id": nova_id}
    return json.dumps(output)


def _sfn_sync(status: str, output: str | None = None) -> dict[str, Any]:
    """Minimal start_sync_execution response dict."""
    resp: dict[str, Any] = {"status": status, "executionArn": _EXEC_ARN}
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
        """start_sync_execution is never invoked when the preflight DDB query hits."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
        ):
            mock_table.query.return_value = {"Items": [{"nova_id": _NOVA_ID}]}
            mock_table.get_item.return_value = _nova_item()

            handler.handle(self._make_event(), None)

            mock_sfn.start_sync_execution.assert_not_called()


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
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("SUCCEEDED", sfn_out)
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
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("SUCCEEDED", sfn_out)
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
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("SUCCEEDED", sfn_out)

            with pytest.raises(handler.UNRESOLVABLE_OBJECT_NAME):
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
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("SUCCEEDED", sfn_out)

            with pytest.raises(handler.IDENTITY_AMBIGUITY):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 7. SFN execution FAILED
# ===========================================================================


class TestSfnExecutionFailed:
    """start_sync_execution returns status FAILED → TerminalError."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_raises_terminal_error(self, handler: Any) -> None:
        """TerminalError raised when the SFN execution terminates with FAILED."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("FAILED")

            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 8. SFN execution TIMED_OUT
# ===========================================================================


class TestSfnExecutionTimedOut:
    """start_sync_execution returns status TIMED_OUT → TerminalError."""

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "GQ Mus"}

    def test_raises_terminal_error(self, handler: Any) -> None:
        """TerminalError raised when the SFN execution terminates with TIMED_OUT."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("TIMED_OUT")

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
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("SUCCEEDED", sfn_out)
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
        No DDB query and no SFN start_sync_execution call must be made.
        """
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
        ):
            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)

            mock_table.query.assert_not_called()
            mock_sfn.start_sync_execution.assert_not_called()


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
# 12. NOT_A_CLASSICAL_NOVA outcome
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
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("SUCCEEDED", sfn_out)

            with pytest.raises(TerminalError):
                handler.handle(self._make_event(), None)


# ===========================================================================
# 13. Preflight hit with underscore normalization — skip initialize_nova
# ===========================================================================


class TestPreflightHitUnderscoreNormalization:
    """
    NameMapping seeded for "v4739 sgr". Ticket arrives with object_name
    "V4739_Sgr". The underscore→space normalization ensures the preflight
    query hits, skipping initialize_nova entirely.
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "V4739_Sgr"}

    def test_skips_initialize_nova(self, handler: Any) -> None:
        """initialize_nova is NOT called when the NameMapping exists under normalized form."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
        ):
            mock_table.query.return_value = {"Items": [{"nova_id": _NOVA_ID}]}
            mock_table.get_item.return_value = _nova_item()

            result: dict[str, Any] = handler.handle(self._make_event(), None)

        assert result["nova_id"] == _NOVA_ID
        assert result["primary_name"] == "V4739_Sgr"
        mock_sfn.start_sync_execution.assert_not_called()

    def test_query_key_uses_underscore_normalized_form(self, handler: Any) -> None:
        """DDB query PK must be "NAME#v4739 sgr" (underscore replaced with space)."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn"),
            patch(f"{_MODULE}.Key") as mock_key,
        ):
            mock_table.query.return_value = {"Items": [{"nova_id": _NOVA_ID}]}
            mock_table.get_item.return_value = _nova_item()

            handler.handle(self._make_event(), None)

        mock_key.assert_called_once_with("PK")
        mock_key.return_value.eq.assert_called_once_with("NAME#v4739 sgr")


# ===========================================================================
# 14. Preflight miss — nova does not exist, proceed with initialize_nova
# ===========================================================================


class TestPreflightMissProceeds:
    """
    No NameMapping seeded. Preflight query returns empty. Handler must
    fire initialize_nova via start_sync_execution.
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "V4739 Sgr"}

    def test_calls_initialize_nova(self, handler: Any) -> None:
        """start_sync_execution IS called when the preflight DDB query misses."""
        sfn_out = _sfn_output("CREATED_AND_LAUNCHED", _NOVA_ID)
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
        ):
            mock_table.query.return_value = {"Items": []}
            mock_sfn.start_sync_execution.return_value = _sfn_sync("SUCCEEDED", sfn_out)
            mock_table.get_item.return_value = _nova_item()

            result: dict[str, Any] = handler.handle(self._make_event(), None)

        assert result["nova_id"] == _NOVA_ID
        mock_sfn.start_sync_execution.assert_called_once()


# ===========================================================================
# 15. Underscore normalization matches existing NameMapping (V1324_Sco)
# ===========================================================================


class TestUnderscoreNormalizationV1324Sco:
    """
    NameMapping seeded for "v1324 sco". Ticket has object_name "V1324_Sco".
    Verifies the I1 underscore→space normalization fix produces a match.
    """

    @staticmethod
    def _make_event() -> dict[str, Any]:
        return {"task_name": "ResolveNova", "object_name": "V1324_Sco"}

    def test_match_found(self, handler: Any) -> None:
        """Preflight hit: underscore in object_name does not prevent NameMapping match."""
        with (
            patch(f"{_MODULE}._table") as mock_table,
            patch(f"{_MODULE}._sfn") as mock_sfn,
            patch(f"{_MODULE}.Key") as mock_key,
        ):
            mock_table.query.return_value = {"Items": [{"nova_id": _NOVA_ID}]}
            mock_table.get_item.return_value = _nova_item()

            result: dict[str, Any] = handler.handle(self._make_event(), None)

        assert result["nova_id"] == _NOVA_ID
        mock_sfn.start_sync_execution.assert_not_called()
        mock_key.assert_called_once_with("PK")
        mock_key.return_value.eq.assert_called_once_with("NAME#v1324 sco")
