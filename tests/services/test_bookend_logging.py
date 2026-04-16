"""
Unit tests for bookend logging in all implemented Lambda handlers.

Verifies that each handler's ``handle()`` entry point emits:
  1. A "Task started" log line with ``task_name`` in extra
  2. An "Operation completed: task:<task_name>" log line from ``log_duration``

Each handler's ``_TASK_HANDLERS`` dispatch entry (or equivalent) is replaced
with a no-op stub so no real AWS calls are made. Logger assertions target the
specific module-level ``logger`` reference in each handler file and the shared
``nova_common.timing.logger`` used by ``log_duration``.
"""

from __future__ import annotations

import importlib
import sys
import types
from typing import Any, Protocol, runtime_checkable
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Environment variable fixtures
# ---------------------------------------------------------------------------

_COMMON_ENV = {
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "POWERTOOLS_SERVICE_NAME": "nova-cat-test",
    "LOG_LEVEL": "DEBUG",
    "NOVA_CAT_TABLE_NAME": "NovaCat-Test",
    "NOVA_CAT_PRIVATE_BUCKET": "nova-cat-private-test",
    "NOVA_CAT_PUBLIC_SITE_BUCKET": "nova-cat-public-test",
    "NOVA_CAT_QUARANTINE_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:test",
    "ADS_SECRET_NAME": "ADSQueryToken",
    "INGEST_NEW_NOVA_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:123456789012:stateMachine:test",
    "REFRESH_REFERENCES_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:123456789012:stateMachine:test",
    "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:123456789012:stateMachine:test",
    "ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:123456789012:stateMachine:test",
    "PHOTOMETRY_TABLE_NAME": "Photometry-Test",
    "DIAGNOSTICS_BUCKET": "diagnostics-test",
}


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _COMMON_ENV.items():
        monkeypatch.setenv(k, v)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    return {"status": "ok"}


def _assert_bookend_logs(
    handler_logger: MagicMock,
    timing_logger: MagicMock,
    task_name: str,
) -> None:
    """Assert both bookend log lines were emitted with the correct content."""
    # 1. "Task started" from the handler logger
    started_calls = [c for c in handler_logger.info.call_args_list if c[0][0] == "Task started"]
    assert len(started_calls) >= 1, (
        f"Expected 'Task started' log line, got: {handler_logger.info.call_args_list}"
    )
    assert started_calls[0][1]["extra"]["task_name"] == task_name

    # 2. "Operation completed: task:<task_name>" from log_duration (timing logger)
    completed_calls = [
        c for c in timing_logger.info.call_args_list if f"task:{task_name}" in c[0][0]
    ]
    assert len(completed_calls) >= 1, (
        f"Expected 'Operation completed: task:{task_name}' log line, "
        f"got: {timing_logger.info.call_args_list}"
    )
    extra = completed_calls[0][1]["extra"]
    assert extra["operation"] == f"task:{task_name}"
    assert isinstance(extra["duration_ms"], float)


# ---------------------------------------------------------------------------
# Standard dispatch-table handlers — patch each module's own logger
# ---------------------------------------------------------------------------

_STANDARD_HANDLERS: list[tuple[str, str, str]] = [
    ("nova_resolver.handler", "nova_resolver.handler.logger", "NormalizeCandidateName"),
    ("job_run_manager.handler", "job_run_manager.handler.logger", "BeginJobRun"),
    ("idempotency_guard.handler", "idempotency_guard.handler.logger", "AcquireIdempotencyLock"),
    ("workflow_launcher.handler", "workflow_launcher.handler.logger", "PublishIngestNewNova"),
    ("quarantine_handler.handler", "quarantine_handler.handler.logger", "QuarantineHandler"),
    ("artifact_finalizer.handler", "artifact_finalizer.handler.logger", "Finalize"),
    (
        "reference_manager.handler",
        "reference_manager.handler.logger",
        "FetchAndReconcileReferences",
    ),
    ("spectra_acquirer.handler", "spectra_acquirer.handler.logger", "AcquireArtifact"),
]


@pytest.mark.parametrize(
    "module_name,logger_path,task_name",
    _STANDARD_HANDLERS,
    ids=[t[2] for t in _STANDARD_HANDLERS],
)
def test_bookend_logging_standard_handlers(
    module_name: str,
    logger_path: str,
    task_name: str,
) -> None:
    """Standard dispatch-table handlers emit Task started + log_duration."""
    mod = importlib.import_module(module_name)
    original_fn = mod._TASK_HANDLERS[task_name]  # type: ignore[attr-defined]
    try:
        mod._TASK_HANDLERS[task_name] = _stub_handler  # type: ignore[attr-defined]
        with (
            patch(logger_path) as mock_handler_logger,
            patch("nova_common.timing.logger") as mock_timing_logger,
        ):
            result = mod.handle({"task_name": task_name}, None)

        assert result == {"status": "ok"}
        _assert_bookend_logs(mock_handler_logger, mock_timing_logger, task_name)
    finally:
        mod._TASK_HANDLERS[task_name] = original_fn  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# spectra_validator
# ---------------------------------------------------------------------------


def test_bookend_logging_spectra_validator() -> None:
    """spectra_validator emits bookend logs."""
    mod = importlib.import_module("spectra_validator.handler")
    task_name = "CheckOperationalStatus"
    original_fn = mod._TASK_HANDLERS[task_name]  # type: ignore[attr-defined]
    try:
        mod._TASK_HANDLERS[task_name] = _stub_handler  # type: ignore[attr-defined]
        with (
            patch("spectra_validator.handler.logger") as mock_handler_logger,
            patch("nova_common.timing.logger") as mock_timing_logger,
        ):
            result = mod.handle({"task_name": task_name}, None)

        assert result == {"status": "ok"}
        _assert_bookend_logs(mock_handler_logger, mock_timing_logger, task_name)
    finally:
        mod._TASK_HANDLERS[task_name] = original_fn  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# spectra_discoverer — needs fake adapters module injected
# ---------------------------------------------------------------------------


def test_bookend_logging_spectra_discoverer() -> None:
    """spectra_discoverer emits bookend logs."""

    @runtime_checkable
    class _StubProtocol(Protocol):
        pass

    fake_adapters = types.ModuleType("adapters")
    fake_adapters.SpectraDiscoveryAdapter = _StubProtocol  # type: ignore[attr-defined]
    fake_adapters._PROVIDER_ADAPTERS = {"ESO": MagicMock()}  # type: ignore[attr-defined]

    # Save originals for cleanup
    saved_adapters = sys.modules.get("adapters")
    saved_handler = sys.modules.get("spectra_discoverer.handler")

    sys.modules["adapters"] = fake_adapters
    # Remove cached handler so fresh import picks up fake adapters
    sys.modules.pop("spectra_discoverer.handler", None)

    try:
        mod = importlib.import_module("spectra_discoverer.handler")
        task_name = "DiscoverAndPersistProducts"
        _original_fn = mod._TASK_HANDLERS[task_name]  # type: ignore[attr-defined]
        mod._TASK_HANDLERS[task_name] = _stub_handler  # type: ignore[attr-defined]

        with (
            patch("spectra_discoverer.handler.logger") as mock_handler_logger,
            patch("nova_common.timing.logger") as mock_timing_logger,
        ):
            result = mod.handle({"task_name": task_name}, None)

        assert result == {"status": "ok"}
        _assert_bookend_logs(mock_handler_logger, mock_timing_logger, task_name)
    finally:
        # Restore original modules to avoid polluting other tests
        sys.modules.pop("spectra_discoverer.handler", None)
        if saved_handler is not None:
            sys.modules["spectra_discoverer.handler"] = saved_handler
        if saved_adapters is not None:
            sys.modules["adapters"] = saved_adapters
        else:
            sys.modules.pop("adapters", None)


# ---------------------------------------------------------------------------
# ticket_parser — single-task handler with hardcoded check
# ---------------------------------------------------------------------------


def test_bookend_logging_ticket_parser() -> None:
    """ticket_parser emits bookend logs for ParseTicket."""
    mod = importlib.import_module("ticket_parser.handler")
    original_fn = mod._parse_ticket  # type: ignore[attr-defined]
    try:
        mod._parse_ticket = lambda event: {"status": "ok"}  # type: ignore[attr-defined]
        with (
            patch("ticket_parser.handler.logger") as mock_handler_logger,
            patch("nova_common.timing.logger") as mock_timing_logger,
        ):
            result = mod.handle({"task_name": "ParseTicket"}, None)

        assert result == {"status": "ok"}
        _assert_bookend_logs(mock_handler_logger, mock_timing_logger, "ParseTicket")
    finally:
        mod._parse_ticket = original_fn  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# ticket_ingestor — if/elif dispatch with two tasks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("task_name", ["IngestPhotometry", "IngestSpectra"])
def test_bookend_logging_ticket_ingestor(task_name: str) -> None:
    """ticket_ingestor emits bookend logs for both task branches."""
    mod = importlib.import_module("ticket_ingestor.handler")
    original_photo = mod._ingest_photometry  # type: ignore[attr-defined]
    original_spectra = mod._ingest_spectra  # type: ignore[attr-defined]
    try:
        mod._ingest_photometry = lambda event: {"status": "ok"}  # type: ignore[attr-defined]
        mod._ingest_spectra = lambda event: {"status": "ok"}  # type: ignore[attr-defined]
        with (
            patch("ticket_ingestor.handler.logger") as mock_handler_logger,
            patch("nova_common.timing.logger") as mock_timing_logger,
        ):
            result = mod.handle({"task_name": task_name}, None)

        assert result == {"status": "ok"}
        _assert_bookend_logs(mock_handler_logger, mock_timing_logger, task_name)
    finally:
        mod._ingest_photometry = original_photo  # type: ignore[attr-defined]
        mod._ingest_spectra = original_spectra  # type: ignore[attr-defined]
