# tests/services/test_ticket_parser_handler.py
"""Unit tests for services/ticket_parser/handler.py.

Coverage:
  - Happy path photometry: valid ticket file → correct serialized output
    with ticket_type="photometry".
  - Happy path spectra: valid ticket file → ticket_type="spectra".
  - TicketParseError from parse_ticket_file → QuarantineError.
  - TicketParseError from validate_ticket → QuarantineError.
  - Wrong task_name → TerminalError.

Parser functions are patched at the handler module level
(ticket_parser.handler.parse_ticket_file / validate_ticket) so that no
real file I/O is performed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from nova_common.errors import QuarantineError, TerminalError
from ticket_parser.handler import handle
from ticket_parser.parser import TicketParseError

from contracts.models.tickets import PhotometryTicket, SpectraTicket

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# _TICKET_PATH = "/tmp/test_ticket.txt"
_TICKET_PATH = (
    Path(__file__).parent.parent
    / "fixtures"
    / "photometry"
    / "v4739_sgr"
    / "V4739_Sgr_Livingston_optical_Photometry.txt"
)
# A 19-character bibcode, as required by the Pydantic model.
_BIBCODE = "2012AJ....144...98W"

# Stub return value for parse_ticket_file — content is irrelevant because
# validate_ticket is always patched alongside it in the happy-path tests.
_RAW_STUB: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set AWS credentials and region unconditionally.

    boto3 clients may be created at module import time by nova_common
    dependencies; they require a region to be present before any patch()
    call is active.  Omitting these vars passes locally when a default
    profile exists but fails in CI.
    """
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")


# ---------------------------------------------------------------------------
# Helpers — minimal valid ticket models
# ---------------------------------------------------------------------------


def _make_photometry_ticket() -> PhotometryTicket:
    """Return a fully valid PhotometryTicket with only required fields set."""
    return PhotometryTicket(
        object_name="V4739_Sgr",
        wavelength_regime="optical",
        time_system="JD",
        assumed_outburst_date=None,
        reference="Livingston et al. (2003)",
        bibcode=_BIBCODE,
        ticket_status="completed",
        time_units="days",
        flux_units="mags",
        flux_error_units="mags",
        data_filename="V4739_Sgr_Livingston_optical_Photometry_Trimmed.csv",
        time_col=0,
        flux_col=1,
    )


def _make_spectra_ticket() -> SpectraTicket:
    """Return a fully valid SpectraTicket with only required fields set."""
    return SpectraTicket(
        object_name="GQ_Mus",
        wavelength_regime="optical",
        time_system="JD",
        assumed_outburst_date=None,
        reference="Williams et al. (1991)",
        bibcode=_BIBCODE,
        ticket_status="completed",
        dereddened=False,
        metadata_filename="GQ_Mus_Williams_Optical_Spectra_MetaData.csv",
        filename_col=0,
        wavelength_col=1,
        flux_col=2,
        date_col=5,
    )


def _base_event() -> dict[str, Any]:
    return {"task_name": "ParseTicket", "ticket_path": _TICKET_PATH}


# ---------------------------------------------------------------------------
# Happy path — photometry
# ---------------------------------------------------------------------------


class TestParseTicketPhotometry:
    def test_returns_serialized_photometry_ticket(self, _env: None) -> None:
        ticket = _make_photometry_ticket()
        with (
            patch(
                "ticket_parser.handler.parse_ticket_file",
                return_value=_RAW_STUB,
            ),
            patch(
                "ticket_parser.handler.validate_ticket",
                return_value=ticket,
            ),
        ):
            result = handle(_base_event(), None)

        expected = ticket.model_dump(mode="json")
        assert result["ticket_type"] == "photometry"
        assert result["object_name"] == expected.get("object_name")
        assert result["ticket"] == expected


# ---------------------------------------------------------------------------
# Happy path — spectra
# ---------------------------------------------------------------------------


class TestParseTicketSpectra:
    def test_returns_serialized_spectra_ticket(self, _env: None) -> None:
        ticket = _make_spectra_ticket()
        with (
            patch(
                "ticket_parser.handler.parse_ticket_file",
                return_value=_RAW_STUB,
            ),
            patch(
                "ticket_parser.handler.validate_ticket",
                return_value=ticket,
            ),
        ):
            result = handle(_base_event(), None)

        expected = ticket.model_dump(mode="json")
        assert result["ticket_type"] == "spectra"
        assert result["object_name"] == expected.get("object_name")
        assert result["ticket"] == expected


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestParseTicketErrors:
    def test_parse_file_error_raises_quarantine(self, _env: None) -> None:
        """TicketParseError from stage 1 (raw parse) → QuarantineError."""
        exc = TicketParseError(
            path=str(_TICKET_PATH),
            reason="No ':' delimiter found",
            line_number=3,
        )
        with (
            patch("ticket_parser.handler.parse_ticket_file", side_effect=exc),
            pytest.raises(QuarantineError),
        ):
            handle(_base_event(), None)

    def test_validate_ticket_error_raises_quarantine(self, _env: None) -> None:
        """TicketParseError from stage 2 (validation) → QuarantineError."""
        exc = TicketParseError(
            path=str(_TICKET_PATH),
            reason="Missing required field: time_col",
        )
        with (
            patch(
                "ticket_parser.handler.parse_ticket_file",
                return_value=_RAW_STUB,
            ),
            patch("ticket_parser.handler.validate_ticket", side_effect=exc),
            pytest.raises(QuarantineError),
        ):
            handle(_base_event(), None)

    def test_wrong_task_name_raises_terminal(self, _env: None) -> None:
        """Any task_name other than "ParseTicket" → TerminalError."""
        event: dict[str, Any] = {
            "task_name": "IngestPhotometry",
            "ticket_path": _TICKET_PATH,
        }
        with pytest.raises(TerminalError):
            handle(event, None)
