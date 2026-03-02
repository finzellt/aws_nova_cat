"""
Unit tests for services/archive_resolver/handler.py

Uses unittest.mock to patch astroquery.simbad.Simbad — no real network
calls are made.

Covers:
  - Nova otype returns is_nova=True, is_classical_nova="true"
  - Recurrent nova otype returns is_nova=True, is_classical_nova="false"
  - Non-nova otype returns is_nova=False
  - No SIMBAD result, no TNS key → resolver_source="NONE"
  - SIMBAD network error raises RetryableError
  - SIMBAD and TNS conflict raises QuarantineError
  - _classify_otypes: nova type sets
  - _classify_otypes: recurrent type sets
  - _classify_otypes: empty set
"""

from __future__ import annotations

import importlib
import sys
import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


def _load_handler() -> types.ModuleType:
    # Remove cached module and any astroquery imports to allow clean patching
    for key in list(sys.modules.keys()):
        if "archive_resolver" in key:
            del sys.modules[key]
    # Patch astroquery before import so handler-level Simbad() instantiation is mocked
    with patch("astroquery.simbad.Simbad"):
        return importlib.import_module("archive_resolver.handler")


def _make_mock_table(otypes: list[str], ra: float = 267.56, dec: float = -32.55) -> MagicMock:
    """Build a mock astropy Table that mimics SIMBAD's multi-row response."""
    rows = []
    for otype in otypes:
        row = MagicMock()
        row.__getitem__ = lambda self, col, _o=otype, _ra=ra, _dec=dec: {
            "otypes.otype_txt": _o,
            "ra": _ra,
            "dec": _dec,
            "main_id": "V1324 Sco",
            "ids": "V1324 Sco|Nova Sco 2012",
        }[col]
        rows.append(row)

    tbl = MagicMock()
    tbl.__len__ = lambda self: len(rows)
    tbl.__iter__ = lambda self: iter(rows)
    tbl.__getitem__ = lambda self, idx: rows[idx]
    return tbl


def _resolve_event(**kwargs: Any) -> dict[str, Any]:
    return {
        "task_name": "ResolveCandidateAgainstPublicArchives",
        "candidate_name": "V1324 Sco",
        "normalized_candidate_name": "v1324 sco",
        "workflow_name": "initialize_nova",
        "job_run": {"correlation_id": "c1", "job_run_id": "j1"},
        **kwargs,
    }


# ---------------------------------------------------------------------------
# SIMBAD classification
# ---------------------------------------------------------------------------


class TestSimbadClassification:
    def test_nova_otype_is_nova(self) -> None:
        handler = _load_handler()
        with patch.object(
            handler._simbad, "query_object", return_value=_make_mock_table(["No*", "V*"])
        ):
            result = handler.handle(_resolve_event(), None)
        assert result["is_nova"] is True
        assert result["is_classical_nova"] == "true"
        assert result["resolver_source"] == "SIMBAD"

    def test_recurrent_nova_otype(self) -> None:
        handler = _load_handler()
        with patch.object(
            handler._simbad, "query_object", return_value=_make_mock_table(["RNe", "V*"])
        ):
            result = handler.handle(_resolve_event(), None)
        assert result["is_nova"] is True
        assert result["is_classical_nova"] == "false"

    def test_non_nova_otype(self) -> None:
        handler = _load_handler()
        with patch.object(
            handler._simbad, "query_object", return_value=_make_mock_table(["Star", "V*"])
        ):
            result = handler.handle(_resolve_event(), None)
        assert result["is_nova"] is False

    def test_coordinates_present_for_nova(self) -> None:
        handler = _load_handler()
        with patch.object(handler._simbad, "query_object", return_value=_make_mock_table(["No*"])):
            result = handler.handle(_resolve_event(), None)
        assert "resolved_ra" in result
        assert "resolved_dec" in result
        assert result["resolved_epoch"] == "J2000"

    def test_no_coordinates_for_non_nova(self) -> None:
        handler = _load_handler()
        with patch.object(handler._simbad, "query_object", return_value=_make_mock_table(["Star"])):
            result = handler.handle(_resolve_event(), None)
        assert "resolved_ra" not in result


# ---------------------------------------------------------------------------
# No result paths
# ---------------------------------------------------------------------------


class TestNoResult:
    def test_simbad_none_no_tns_key_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TNS_API_KEY", "")
        handler = _load_handler()
        with patch.object(handler._simbad, "query_object", return_value=None):
            result = handler.handle(_resolve_event(), None)
        assert result["is_nova"] is False
        assert result["resolver_source"] == "NONE"

    def test_simbad_empty_table_treated_as_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TNS_API_KEY", "")
        handler = _load_handler()
        empty = MagicMock()
        empty.__len__ = lambda self: 0
        with patch.object(handler._simbad, "query_object", return_value=empty):
            result = handler.handle(_resolve_event(), None)
        assert result["is_nova"] is False


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_simbad_timeout_raises_retryable(self) -> None:
        handler = _load_handler()
        with (
            patch.object(
                handler._simbad,
                "query_object",
                side_effect=Exception("timeout connecting to SIMBAD"),
            ),
            pytest.raises(handler.RetryableError),
        ):
            handler.handle(_resolve_event(), None)

    def test_simbad_tns_conflict_raises_quarantine(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TNS_API_KEY", "fake-key")
        handler = _load_handler()

        # SIMBAD says not a nova
        with patch.object(handler._simbad, "query_object", return_value=_make_mock_table(["Star"])):
            # TNS says it is a nova
            tns_result = {
                "is_nova": True,
                "is_classical_nova": "true",
                "resolved_ra": 267.56,
                "resolved_dec": -32.55,
                "resolved_epoch": "J2000",
            }
        with (
            patch.object(handler, "_query_tns", return_value=tns_result),
            patch.object(handler, "_query_simbad", return_value=None),
        ):
            simbad_result = {
                "is_nova": False,
                "is_classical_nova": "false",
            }
            with pytest.raises(handler.QuarantineError):
                handler._merge_results(simbad_result, tns_result)


# ---------------------------------------------------------------------------
# _classify_otypes unit tests
# ---------------------------------------------------------------------------


class TestClassifyOtypes:
    @pytest.mark.parametrize(
        "otypes,expected_nova,expected_classical",
        [
            ({"No*", "V*"}, True, "true"),
            ({"No?"}, True, "true"),
            ({"NL*", "Star"}, True, "true"),
            ({"RNe", "V*"}, True, "false"),
            ({"RN*"}, True, "false"),
            ({"Star", "V*"}, False, "false"),
            (set(), False, "false"),
        ],
    )
    def test_classification(
        self,
        otypes: set[str],
        expected_nova: bool,
        expected_classical: str,
    ) -> None:
        handler = _load_handler()
        is_nova, is_classical = handler._classify_otypes(otypes)
        assert is_nova is expected_nova
        assert is_classical == expected_classical
