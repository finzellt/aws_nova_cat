"""ticket_parser Lambda handler.

Single task: ParseTicket.

Reads the .txt ticket file at the path supplied in the event, calls
``parse_ticket_file()`` followed by ``validate_ticket()``, and returns the
serialized ticket dict.  ``ticket_type`` is already a first-class field on
every ticket model, so ``model_dump(mode="json")`` produces the full output
shape that downstream states require — including the discriminator that
``TicketTypeBranch`` branches on.

Input event fields:
  task_name    — must equal "ParseTicket" (enforced)
  ticket_path  — filesystem or S3 path to the .txt ticket file (required)

Output shape (placed at ParseTicket ResultPath by Step Functions):
  {
      "ticket_type": "photometry" | "spectra",
      <all other ticket fields serialized as JSON-safe values>
  }

Failure classification (docs/workflows/ingest-ticket.md):
  TicketParseError  → QuarantineError  (malformed or schema-invalid ticket
                                        is an operator authoring error, not
                                        a retryable infrastructure failure)
  Wrong task_name   → TerminalError
"""

from __future__ import annotations

from typing import Any

from nova_common.errors import QuarantineError, TerminalError
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

from ticket_parser.parser import TicketParseError, parse_ticket_file, validate_ticket

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    if task_name != "ParseTicket":
        raise TerminalError(f"Unknown task_name: {task_name!r}")
    return _parse_ticket(event)


# ---------------------------------------------------------------------------
# Task implementation
# ---------------------------------------------------------------------------


@tracer.capture_method
def _parse_ticket(event: dict[str, Any]) -> dict[str, Any]:
    """
    Execute the two-stage parse pipeline and return the serialized ticket.

    Stage 1 (``parse_ticket_file``) is format-aware and schema-ignorant —
    it raises ``TicketParseError`` only for structural violations such as
    missing delimiters or duplicate keys.

    Stage 2 (``validate_ticket``) is schema-aware — it discriminates ticket
    type, maps raw keys to Pydantic fields, coerces types, and validates the
    model.  Any schema or coercion failure also surfaces as ``TicketParseError``.

    Either stage raising ``TicketParseError`` is unconditionally mapped to
    ``QuarantineError``: a bad ticket is an operator authoring error and must
    not be retried until the source file is corrected.
    """
    ticket_path: str = event["ticket_path"]

    logger.info("Parsing ticket", extra={"ticket_path": ticket_path})

    try:
        raw = parse_ticket_file(ticket_path)
    except TicketParseError as exc:
        logger.warning(
            "Ticket parse failed at stage 1 (raw parse)",
            extra={"ticket_path": ticket_path, "error": str(exc)},
        )
        raise QuarantineError(str(exc)) from exc

    try:
        ticket = validate_ticket(raw, path=ticket_path)
    except TicketParseError as exc:
        logger.warning(
            "Ticket parse failed at stage 2 (validation)",
            extra={"ticket_path": ticket_path, "error": str(exc)},
        )
        raise QuarantineError(str(exc)) from exc

    result: dict[str, Any] = ticket.model_dump(mode="json")
    logger.info(
        "Ticket parsed successfully",
        extra={
            "ticket_type": result["ticket_type"],
            "object_name": result.get("object_name"),
        },
    )
    return result
