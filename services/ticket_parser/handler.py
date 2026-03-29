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
  ticket_path  — bare S3 key or local filesystem path to the .txt ticket
                 file (required).  In the deployed stack this is a bare S3
                 key resolved against NOVA_CAT_PRIVATE_BUCKET; in
                 integration tests it is a local path and is used as-is.

Output shape (placed at ParseTicket ResultPath by Step Functions):
  {
      "ticket_type": "photometry" | "spectra",
      "object_name": "<object name string>",
      "ticket":      { <all ticket fields serialized as JSON-safe values> }
  }

Failure classification (docs/workflows/ingest-ticket.md):
  TicketParseError  → QuarantineError  (malformed or schema-invalid ticket
                                        is an operator authoring error, not
                                        a retryable infrastructure failure)
  Wrong task_name   → TerminalError
"""

from __future__ import annotations

import os
from typing import Any

import boto3
from nova_common.errors import QuarantineError, TerminalError
from nova_common.file_io import resolve_file
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

from ticket_parser.parser import TicketParseError, parse_ticket_file, validate_ticket

# ---------------------------------------------------------------------------
# Module-level AWS clients
# ---------------------------------------------------------------------------

_s3 = boto3.client("s3")

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

    ``ticket_path`` is resolved to a local filesystem path via
    ``resolve_file`` before being passed to ``parse_ticket_file``.  If the
    path already exists on the local filesystem (integration-test context)
    it is used as-is; otherwise it is treated as a bare S3 key and
    downloaded to /tmp from NOVA_CAT_PRIVATE_BUCKET.

    NOVA_CAT_PRIVATE_BUCKET is read inside this function rather than at
    module level so that the handler module can be imported in test
    environments that do not set this variable (e.g. unit tests that supply
    a local ticket_path and never exercise the S3 download path).

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
    ticket_path_spec: str = event["ticket_path"]

    logger.info("Resolving ticket path", extra={"ticket_path": ticket_path_spec})

    private_bucket: str = os.environ["NOVA_CAT_PRIVATE_BUCKET"]
    local_path = resolve_file(
        ticket_path_spec,
        s3_client=_s3,
        bucket=private_bucket,
    )

    logger.info("Parsing ticket", extra={"ticket_path": str(local_path)})

    try:
        raw = parse_ticket_file(local_path)
    except TicketParseError as exc:
        logger.warning(
            "Ticket parse failed at stage 1 (raw parse)",
            extra={"ticket_path": ticket_path_spec, "error": str(exc)},
        )
        raise QuarantineError(str(exc)) from exc

    try:
        ticket = validate_ticket(raw, path=str(local_path))
    except TicketParseError as exc:
        logger.warning(
            "Ticket parse failed at stage 2 (validation)",
            extra={"ticket_path": ticket_path_spec, "error": str(exc)},
        )
        raise QuarantineError(str(exc)) from exc

    ticket_dump: dict[str, Any] = ticket.model_dump(mode="json")
    logger.info(
        "Ticket parsed successfully",
        extra={
            "ticket_type": ticket_dump["ticket_type"],
            "object_name": ticket_dump.get("object_name"),
        },
    )
    return {
        "ticket_type": ticket_dump["ticket_type"],
        "object_name": ticket_dump.get("object_name"),
        "ticket": ticket_dump,
    }
