"""ticket_parser service — public API.

Exposes the two-stage parsing pipeline and the error class.
The Lambda entry point (handler.py) calls both stages in sequence.

Usage::

    from services.ticket_parser import (
        parse_ticket_file,
        validate_ticket,
        TicketParseError,
    )

    raw = parse_ticket_file("/tmp/V4739_Sgr_Livingston_optical_Photometry.txt")
    ticket = validate_ticket(raw, path=str(ticket_path))
"""

from ticket_parser.parser import (
    TicketParseError,
    parse_ticket_file,
    validate_ticket,
)

__all__ = [
    "TicketParseError",
    "parse_ticket_file",
    "validate_ticket",
]
