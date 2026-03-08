"""
Unit tests for services/spectra_discoverer/adapters/eso.py

Pure unit tests — no AWS calls, no real HTTP requests.
pyvo is mocked to prevent network access.

Covers:
  - _sanitize_value: bytes decode, numpy scalar unwrap, nan/inf → None, passthrough
  - _normalize_url: scheme/host lowercasing, fragment stripping, None cases
  - _extract_hints: Decimal conversion, nan/inf dropped, missing keys produce empty dict
  - ESOAdapter.normalize: NATIVE_ID, METADATA_KEY, WEAK, and skip branches
  - ESOAdapter.query: mocked pyvo resultset, RetryableError on SSAP failure
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from spectra_discoverer.adapters.eso import (  # type: ignore[import-not-found]
    ESOAdapter,
    _extract_hints,
    _normalize_url,
    _sanitize_value,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeNumpyScalar:
    """Minimal stand-in for a numpy scalar (has .item() but is not a float)."""

    def __init__(self, value: object) -> None:
        self._value = value

    def item(self) -> object:
        return self._value


def _raw(**kwargs: object) -> dict:
    """Build a minimal raw ESO record with all SSAP fields defaulting to None."""
    defaults = {
        "COLLECTION": None,
        "TARGETNAME": None,
        "s_ra": None,
        "s_dec": None,
        "em_min": None,
        "em_max": None,
        "SPECRP": None,
        "SNR": None,
        "t_min": None,
        "t_max": None,
        "CREATORDID": None,
        "access_url": None,
    }
    return {**defaults, **kwargs}


# ---------------------------------------------------------------------------
# _sanitize_value
# ---------------------------------------------------------------------------


class TestSanitizeValue:
    def test_bytes_decoded_to_str(self) -> None:
        assert _sanitize_value(b"UVES") == "UVES"

    def test_bytes_stripped_of_whitespace(self) -> None:
        assert _sanitize_value(b"  HARPS  ") == "HARPS"

    def test_bytes_empty_after_strip_returns_none(self) -> None:
        assert _sanitize_value(b"   ") is None

    def test_numpy_scalar_unwrapped(self) -> None:
        scalar = _FakeNumpyScalar(3.14)
        assert _sanitize_value(scalar) == 3.14

    def test_nan_returns_none(self) -> None:
        assert _sanitize_value(float("nan")) is None

    def test_inf_returns_none(self) -> None:
        assert _sanitize_value(float("inf")) is None

    def test_neg_inf_returns_none(self) -> None:
        assert _sanitize_value(float("-inf")) is None

    def test_normal_float_passthrough(self) -> None:
        assert _sanitize_value(3.14) == 3.14

    def test_str_passthrough(self) -> None:
        assert _sanitize_value("hello") == "hello"

    def test_int_passthrough(self) -> None:
        assert _sanitize_value(42) == 42

    def test_none_passthrough(self) -> None:
        assert _sanitize_value(None) is None

    def test_numpy_scalar_nan_returns_none(self) -> None:
        """numpy scalar wrapping NaN should unwrap then return None."""
        scalar = _FakeNumpyScalar(float("nan"))
        assert _sanitize_value(scalar) is None


# ---------------------------------------------------------------------------
# _normalize_url
# ---------------------------------------------------------------------------


class TestNormalizeUrl:
    def test_lowercases_scheme(self) -> None:
        result = _normalize_url("HTTP://archive.eso.org/path")
        assert result is not None
        assert result.startswith("http://")

    def test_lowercases_host(self) -> None:
        result = _normalize_url("http://ARCHIVE.ESO.ORG/path")
        assert result is not None
        assert "archive.eso.org" in result

    def test_preserves_path(self) -> None:
        result = _normalize_url("http://archive.eso.org/ssap/query")
        assert result is not None
        assert "/ssap/query" in result

    def test_preserves_query_string(self) -> None:
        result = _normalize_url("http://archive.eso.org/ssap?REQUEST=getData&dp_id=ADP.123")
        assert result is not None
        assert "REQUEST=getData" in result

    def test_strips_fragment(self) -> None:
        result = _normalize_url("http://archive.eso.org/ssap#section")
        assert result is not None
        assert "#" not in result

    def test_empty_string_returns_none(self) -> None:
        assert _normalize_url("") is None

    def test_whitespace_only_returns_none(self) -> None:
        # urlparse("   ".strip()) = urlparse("") → empty result → None
        assert _normalize_url("   ") is None

    def test_strips_leading_whitespace_before_parsing(self) -> None:
        result = _normalize_url("  http://archive.eso.org/path  ")
        assert result is not None
        assert result.startswith("http://")


# ---------------------------------------------------------------------------
# _extract_hints
# ---------------------------------------------------------------------------


class TestExtractHints:
    def test_empty_raw_returns_empty_dict(self) -> None:
        assert _extract_hints(_raw()) == {}

    def test_collection_extracted_as_str(self) -> None:
        hints = _extract_hints(_raw(COLLECTION="UVES"))
        assert hints["collection"] == "UVES"

    def test_numeric_hints_converted_to_decimal(self) -> None:
        hints = _extract_hints(_raw(SPECRP=40000.0, SNR=25.5))
        assert isinstance(hints["specrp"], Decimal)
        assert isinstance(hints["snr"], Decimal)

    def test_decimal_values_are_correct(self) -> None:
        hints = _extract_hints(_raw(t_min=59000.123))
        assert hints["t_min_mjd"] == Decimal("59000.123")

    def test_nan_hint_dropped(self) -> None:
        hints = _extract_hints(_raw(SPECRP=float("nan")))
        assert "specrp" not in hints

    def test_inf_hint_dropped(self) -> None:
        hints = _extract_hints(_raw(SNR=float("inf")))
        assert "snr" not in hints

    def test_em_min_and_max_stored_with_metre_suffix(self) -> None:
        hints = _extract_hints(_raw(em_min=3e-7, em_max=1e-6))
        assert "em_min_m" in hints
        assert "em_max_m" in hints

    def test_all_hints_populated(self) -> None:
        hints = _extract_hints(
            _raw(
                COLLECTION="UVES",
                SPECRP=40000.0,
                SNR=25.0,
                t_min=59000.0,
                t_max=59001.0,
                em_min=3e-7,
                em_max=1e-6,
            )
        )
        assert set(hints.keys()) == {
            "collection",
            "specrp",
            "snr",
            "t_min_mjd",
            "t_max_mjd",
            "em_min_m",
            "em_max_m",
        }


# ---------------------------------------------------------------------------
# ESOAdapter.normalize
# ---------------------------------------------------------------------------


class TestNormalize:
    def setup_method(self) -> None:
        self.adapter = ESOAdapter()

    def test_provider_is_eso(self) -> None:
        raw = _raw(CREATORDID="eso:product-001", access_url="http://archive.eso.org/spec")
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["provider"] == "ESO"

    def test_native_id_strategy_when_creatordid_present(self) -> None:
        raw = _raw(CREATORDID="eso:product-001", access_url="http://archive.eso.org/spec")
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["identity_strategy"] == "NATIVE_ID"

    def test_native_id_locator_identity_prefixed(self) -> None:
        raw = _raw(CREATORDID="eso:product-001", access_url="http://archive.eso.org/spec")
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["locator_identity"] == "provider_product_id:eso:product-001"

    def test_native_id_sets_provider_product_key(self) -> None:
        raw = _raw(CREATORDID="eso:product-001", access_url="http://archive.eso.org/spec")
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["provider_product_key"] == "eso:product-001"

    def test_native_id_locators_contain_access_url(self) -> None:
        raw = _raw(CREATORDID="eso:product-001", access_url="http://archive.eso.org/spec")
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert len(result["locators"]) == 1
        assert result["locators"][0]["value"] == "http://archive.eso.org/spec"

    def test_metadata_key_strategy_when_only_access_url(self) -> None:
        raw = _raw(CREATORDID=None, access_url="http://archive.eso.org/spec")
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["identity_strategy"] == "METADATA_KEY"

    def test_metadata_key_locator_identity_uses_normalized_url(self) -> None:
        raw = _raw(CREATORDID=None, access_url="HTTP://ARCHIVE.ESO.ORG/spec")
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["locator_identity"].startswith("url:http://archive.eso.org")

    def test_metadata_key_provider_product_key_is_none(self) -> None:
        raw = _raw(CREATORDID=None, access_url="http://archive.eso.org/spec")
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["provider_product_key"] is None

    def test_weak_when_creatordid_present_but_no_access_url(self) -> None:
        raw = _raw(CREATORDID="eso:product-001", access_url=None)
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["identity_strategy"] == "WEAK"

    def test_weak_has_empty_locators(self) -> None:
        raw = _raw(CREATORDID="eso:product-001", access_url=None)
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["locators"] == []

    def test_returns_none_when_neither_creatordid_nor_access_url(self) -> None:
        raw = _raw(CREATORDID=None, access_url=None)
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is None

    def test_returns_none_when_url_cannot_be_normalized(self) -> None:
        raw = _raw(CREATORDID=None, access_url="http://example.com/spec")
        with patch("spectra_discoverer.adapters.eso._normalize_url", return_value=None):
            result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is None

    def test_nova_id_propagated_to_result(self) -> None:
        raw = _raw(CREATORDID="eso:product-001", access_url="http://archive.eso.org/spec")
        result = self.adapter.normalize(nova_id="nova-xyz", raw=raw)
        assert result is not None
        assert result["nova_id"] == "nova-xyz"

    def test_hints_populated_when_present(self) -> None:
        raw = _raw(
            CREATORDID="eso:product-001",
            access_url="http://archive.eso.org/spec",
            COLLECTION="UVES",
            SPECRP=40000.0,
        )
        result = self.adapter.normalize(nova_id="nova-001", raw=raw)
        assert result is not None
        assert result["hints"]["collection"] == "UVES"
        assert "specrp" in result["hints"]


# ---------------------------------------------------------------------------
# ESOAdapter.query
# ---------------------------------------------------------------------------


class TestQuery:
    def setup_method(self) -> None:
        self.adapter = ESOAdapter()

    def test_returns_list_of_raw_dicts(self) -> None:
        mock_row: dict[str, Any] = {
            field: None
            for field in [
                "COLLECTION",
                "TARGETNAME",
                "s_ra",
                "s_dec",
                "em_min",
                "em_max",
                "SPECRP",
                "SNR",
                "t_min",
                "t_max",
                "CREATORDID",
                "access_url",
            ]
        }
        mock_row["CREATORDID"] = "eso:product-001"
        mock_row["access_url"] = "http://archive.eso.org/spec"

        with patch("spectra_discoverer.adapters.eso.vo") as mock_vo:
            mock_vo.dal.SSAService.return_value.search.return_value = [mock_row]
            result = self.adapter.query(nova_id="nova-001", ra_deg=271.0, dec_deg=-30.0)

        assert len(result) == 1
        assert result[0]["CREATORDID"] == "eso:product-001"

    def test_empty_resultset_returns_empty_list(self) -> None:
        with patch("spectra_discoverer.adapters.eso.vo") as mock_vo:
            mock_vo.dal.SSAService.return_value.search.return_value = []
            result = self.adapter.query(nova_id="nova-001", ra_deg=271.0, dec_deg=-30.0)
        assert result == []

    def test_raises_retryable_error_on_ssap_failure(self) -> None:
        from nova_common.errors import RetryableError

        with patch("spectra_discoverer.adapters.eso.vo") as mock_vo:
            mock_vo.dal.SSAService.return_value.search.side_effect = ConnectionError("timeout")
            with pytest.raises(RetryableError, match="ESO SSAP query failed"):
                self.adapter.query(nova_id="nova-001", ra_deg=271.0, dec_deg=-30.0)

    def test_missing_field_on_row_set_to_none(self) -> None:
        """If a row is missing an expected field, it should be captured as None."""
        # An empty dict will raise KeyError on row[field] — which is caught → None
        with patch("spectra_discoverer.adapters.eso.vo") as mock_vo:
            mock_vo.dal.SSAService.return_value.search.return_value = [{}]
            result = self.adapter.query(nova_id="nova-001", ra_deg=271.0, dec_deg=-30.0)
        assert result[0]["CREATORDID"] is None
        assert result[0]["access_url"] is None
