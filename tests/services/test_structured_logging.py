"""Unit tests for services/artifact_generator/structured_logging.py."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, cast

import pytest

# The structured_logging module lives inside the artifact_generator package.
# Add it to sys.path so we can import it directly.
_AG_DIR = str(Path(__file__).resolve().parents[2] / "services" / "artifact_generator")
if _AG_DIR not in sys.path:
    sys.path.insert(0, _AG_DIR)

from structured_logging import LogContext, StructuredJsonFormatter, configure_fargate_logging  # noqa: E402, I001

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(
    message: str = "test message",
    level: int = logging.INFO,
    extra: dict[str, Any] | None = None,
    exc_info: tuple[type[BaseException], BaseException, Any]
    | tuple[None, None, None]
    | None = None,
) -> logging.LogRecord:
    """Create a LogRecord, optionally with extra fields and exception info."""
    record = logging.LogRecord(
        name="artifact_generator",
        level=level,
        pathname="test.py",
        lineno=1,
        msg=message,
        args=(),
        exc_info=exc_info,
    )
    if extra:
        for key, value in extra.items():
            setattr(record, key, value)
    return record


def _format_and_parse(
    formatter: StructuredJsonFormatter,
    record: logging.LogRecord,
) -> dict[str, Any]:
    """Format a record and parse the resulting JSON."""
    line = formatter.format(record)
    return cast(dict[Any, Any], json.loads(line))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestStructuredJsonFormatter:
    """Tests for StructuredJsonFormatter."""

    def test_emits_valid_single_line_json(self) -> None:
        fmt = StructuredJsonFormatter()
        record = _make_record()
        line = fmt.format(record)
        # Must be valid JSON.
        parsed = json.loads(line)
        assert isinstance(parsed, dict)
        # Must be a single line.
        assert "\n" not in line

    def test_always_present_fields(self) -> None:
        fmt = StructuredJsonFormatter()
        record = _make_record("hello world", level=logging.WARNING)
        parsed = _format_and_parse(fmt, record)

        assert parsed["level"] == "WARNING"
        assert parsed["message"] == "hello world"
        assert parsed["service"] == "nova-cat"
        assert parsed["function_name"] == "artifact-generator"
        assert "timestamp" in parsed
        # Timestamp should be ISO 8601 with timezone.
        assert "T" in parsed["timestamp"]

    def test_persistent_context_fields_appear(self) -> None:
        ctx = LogContext()
        ctx.set_context(plan_id="plan-123", workflow_name="artifact_generator")
        fmt = StructuredJsonFormatter(ctx)
        record = _make_record()
        parsed = _format_and_parse(fmt, record)

        assert parsed["plan_id"] == "plan-123"
        assert parsed["workflow_name"] == "artifact_generator"

    def test_per_call_extra_fields_appear(self) -> None:
        fmt = StructuredJsonFormatter()
        record = _make_record(extra={"nova_id": "V1674_Her", "duration_ms": 42})
        parsed = _format_and_parse(fmt, record)

        assert parsed["nova_id"] == "V1674_Her"
        assert parsed["duration_ms"] == 42

    def test_per_call_extra_overrides_persistent_context(self) -> None:
        ctx = LogContext()
        ctx.set_context(nova_id="from_context", phase="generate")
        fmt = StructuredJsonFormatter(ctx)

        record = _make_record(extra={"nova_id": "from_extra"})
        parsed = _format_and_parse(fmt, record)

        # Per-call extra wins.
        assert parsed["nova_id"] == "from_extra"
        # Context-only fields still present.
        assert parsed["phase"] == "generate"

    def test_clear_nova_context_removes_nova_fields(self) -> None:
        ctx = LogContext()
        ctx.set_context(
            plan_id="plan-123",
            nova_id="V1674_Her",
            artifact="spectra_json",
            phase="generate",
        )
        ctx.clear_nova_context()
        fmt = StructuredJsonFormatter(ctx)
        record = _make_record()
        parsed = _format_and_parse(fmt, record)

        # Plan-level fields retained.
        assert parsed["plan_id"] == "plan-123"
        # Nova-level fields removed.
        assert "nova_id" not in parsed
        assert "artifact" not in parsed
        assert "phase" not in parsed

    def test_exception_captured(self) -> None:
        fmt = StructuredJsonFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = _make_record(exc_info=exc_info)
        parsed = _format_and_parse(fmt, record)

        assert "exception" in parsed
        assert "ValueError: boom" in parsed["exception"]
        assert "Traceback" in parsed["exception"]

    def test_no_exception_field_when_no_error(self) -> None:
        fmt = StructuredJsonFormatter()
        record = _make_record()
        parsed = _format_and_parse(fmt, record)

        assert "exception" not in parsed

    def test_stdlib_noise_excluded(self) -> None:
        """Stdlib internal fields should not leak into the JSON output."""
        fmt = StructuredJsonFormatter()
        record = _make_record()
        parsed = _format_and_parse(fmt, record)

        for noise_field in ("pathname", "lineno", "funcName", "processName", "threadName"):
            assert noise_field not in parsed


class TestLogContext:
    """Tests for LogContext."""

    def test_set_and_get(self) -> None:
        ctx = LogContext()
        ctx.set_context(plan_id="abc", release_id="rel-1")
        fields = ctx.get_fields()
        assert fields == {"plan_id": "abc", "release_id": "rel-1"}

    def test_update_overwrites(self) -> None:
        ctx = LogContext()
        ctx.set_context(phase="generate")
        ctx.set_context(phase="upload")
        assert ctx.get_fields()["phase"] == "upload"

    def test_clear_nova_context_selective(self) -> None:
        ctx = LogContext()
        ctx.set_context(plan_id="abc", nova_id="V1", artifact="x", phase="y")
        ctx.clear_nova_context()
        fields = ctx.get_fields()
        assert "plan_id" in fields
        assert "nova_id" not in fields
        assert "artifact" not in fields
        assert "phase" not in fields

    def test_get_fields_returns_copy(self) -> None:
        ctx = LogContext()
        ctx.set_context(a="1")
        fields = ctx.get_fields()
        fields["a"] = "mutated"
        assert ctx.get_fields()["a"] == "1"


class TestSequenceCounter:
    """Tests for the seq monotonic counter."""

    def test_seq_increments(self) -> None:
        fmt = StructuredJsonFormatter()
        r1 = _make_record("first")
        r2 = _make_record("second")
        p1 = _format_and_parse(fmt, r1)
        p2 = _format_and_parse(fmt, r2)
        assert isinstance(p1["seq"], int)
        assert isinstance(p2["seq"], int)
        assert p2["seq"] > p1["seq"]

    def test_seq_present_in_output(self) -> None:
        fmt = StructuredJsonFormatter()
        parsed = _format_and_parse(fmt, _make_record())
        assert "seq" in parsed


class TestConfigureFargateLogging:
    """Tests for configure_fargate_logging."""

    def test_returns_artifact_generator_logger(self) -> None:
        logger = configure_fargate_logging()
        assert logger.name == "artifact_generator"

    def test_logger_emits_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = LogContext()
        ctx.set_context(plan_id="test-plan")
        logger = configure_fargate_logging(ctx)
        logger.info("hello", extra={"nova_id": "V1"})
        captured = capsys.readouterr()
        parsed = json.loads(captured.out.strip())
        assert parsed["message"] == "hello"
        assert parsed["plan_id"] == "test-plan"
        assert parsed["nova_id"] == "V1"
        assert parsed["service"] == "nova-cat"

    def test_root_logger_emits_structured_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_fargate_logging()
        root = logging.getLogger()
        root.warning("library warning")
        captured = capsys.readouterr()
        parsed = json.loads(captured.out.strip())
        assert parsed["message"] == "library warning"
        assert parsed["level"] == "WARNING"
        assert "seq" in parsed

    def test_root_logger_suppresses_info(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_fargate_logging()
        root = logging.getLogger()
        root.info("should not appear")
        captured = capsys.readouterr()
        assert captured.out.strip() == ""

    def test_excepthook_installed(self) -> None:
        configure_fargate_logging()
        assert sys.excepthook is not sys.__excepthook__

    def test_excepthook_emits_structured_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_fargate_logging()
        try:
            raise RuntimeError("test")
        except RuntimeError:
            exc_type, exc_val, exc_tb = sys.exc_info()
            assert exc_type is not None
            assert exc_val is not None
            sys.excepthook(exc_type, exc_val, exc_tb)
        captured = capsys.readouterr()
        lines = captured.out.strip().splitlines()
        json_line = [line for line in lines if line.startswith("{")][0]
        parsed = json.loads(json_line)
        assert parsed["level"] == "CRITICAL"
        # assert "RuntimeError: kaboom" in parsed["exception"]
