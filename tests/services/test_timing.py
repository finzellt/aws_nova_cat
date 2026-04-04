"""Unit tests for nova_common.timing.log_duration."""

from __future__ import annotations

from unittest.mock import patch

from nova_common.timing import log_duration


def test_log_duration_logs_operation_and_duration_ms() -> None:
    """log_duration emits an INFO log with operation name and duration_ms as a float."""
    with patch("nova_common.timing.logger") as mock_logger, log_duration("test_op", foo="bar"):
        pass

    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args

    # Positional: message string contains the operation name
    message = call_args[0][0]
    assert "test_op" in message

    # Keyword extras
    extra = call_args[1]["extra"]
    assert extra["operation"] == "test_op"
    assert isinstance(extra["duration_ms"], float)
    assert extra["duration_ms"] >= 0
    assert extra["foo"] == "bar"


def test_log_duration_logs_even_on_exception() -> None:
    """Duration is logged even when the wrapped block raises."""
    with patch("nova_common.timing.logger") as mock_logger:
        try:
            with log_duration("failing_op"):
                raise ValueError("boom")
        except ValueError:
            pass

    mock_logger.info.assert_called_once()
    extra = mock_logger.info.call_args[1]["extra"]
    assert extra["operation"] == "failing_op"
    assert isinstance(extra["duration_ms"], float)
