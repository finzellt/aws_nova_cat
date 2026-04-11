"""
Unit tests for services/spectra_discoverer/adapters/mast.py

Pure unit tests — no AWS calls, no real HTTP requests.
astroquery.mast.Observations is mocked to prevent network access.

Covers:
  - _safe_float: finite conversion, nan/inf → None, missing → None
  - _safe_int: integer conversion, missing → None
  - _extract_hints: Decimal conversion, nan/inf dropped, missing keys graceful
  - _maybe_set_str: truthy check, whitespace stripping
  - _maybe_set_numeric: Decimal conversion, nan/inf dropped
  - MASTAdapter.normalize: NATIVE_ID identity, locator URL, hints, skip branches
  - MASTAdapter.query: name search, alias fallback, filtering, retries, error handling
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from astropy.table import Table  # type: ignore[import-untyped]
from spectra_discoverer.adapters.mast import (  # type: ignore[import-not-found]
    MASTAdapter,
    _extract_hints,
    _maybe_set_numeric,
    _maybe_set_str,
    _safe_float,
    _safe_int,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NOVA_ID = "test-nova-uuid-1234"
_PRIMARY_NAME = "V339 Del"
_ALIASES = ["Nova Del 2013", "NOVADEL2013"]
_RA_DEG = 305.878
_DEC_DEG = 20.768


# ---------------------------------------------------------------------------
# Helpers: build astropy Tables mimicking MAST query results
# ---------------------------------------------------------------------------


def _obs_table(rows: list[dict[str, Any]] | None = None) -> Table:
    """Build an astropy Table mimicking Observations.query_object() output."""
    if rows is None:
        rows = [_obs_row()]
    if not rows:
        return Table(
            names=[
                "obs_collection",
                "dataproduct_type",
                "project",
                "instrument_name",
                "target_name",
                "proposal_id",
                "obs_id",
                "obsid",
                "t_min",
                "t_max",
            ],
            dtype=["U16", "U16", "U16", "U16", "U32", "i4", "U64", "i8", "f8", "f8"],
        )
    return Table(rows=rows)


def _obs_row(**overrides: Any) -> dict[str, Any]:
    """Build a single observation row dict."""
    defaults: dict[str, Any] = {
        "obs_collection": "HST",
        "dataproduct_type": "spectrum",
        "project": "HASP",
        "instrument_name": "STIS",
        "target_name": "V339-DEL",
        "proposal_id": 13828,
        "obs_id": "hst_13828_stis_v339-del_e140m",
        "obsid": 99001,
        "t_min": 57153.9,
        "t_max": 57154.1,
    }
    return {**defaults, **overrides}


def _prod_table(rows: list[dict[str, Any]] | None = None) -> Table:
    """Build an astropy Table mimicking Observations.get_product_list() output."""
    if rows is None:
        rows = [_prod_row()]
    if not rows:
        return Table(
            names=["productFilename", "dataURI", "obsID", "size", "productSubGroupDescription"],
            dtype=["U128", "U128", "i8", "i8", "U16"],
        )
    return Table(rows=rows)


def _prod_row(**overrides: Any) -> dict[str, Any]:
    """Build a single product row dict."""
    defaults: dict[str, Any] = {
        "productFilename": "hst_13828_stis_v339-del_e140m_ocoj08_cspec.fits",
        "dataURI": "mast:HST/product/hst_13828_stis_v339-del_e140m_ocoj08_cspec.fits",
        "obsID": 99001,
        "size": 614400,
        "productSubGroupDescription": "CSPEC",
    }
    return {**defaults, **overrides}


def _raw(**overrides: Any) -> dict[str, Any]:
    """Build a complete raw product record as returned by query() for normalize tests."""
    defaults: dict[str, Any] = {
        "productFilename": "hst_13828_stis_v339-del_e140m_ocoj08_cspec.fits",
        "dataURI": "mast:HST/product/hst_13828_stis_v339-del_e140m_ocoj08_cspec.fits",
        "obsID": "99001",
        "size": 614400,
        "productSubGroupDescription": "CSPEC",
        "instrument_name": "STIS",
        "target_name": "V339-DEL",
        "proposal_id": "13828",
        "obs_id": "hst_13828_stis_v339-del_e140m",
        "t_min": 57153.9,
        "t_max": 57154.1,
    }
    return {**defaults, **overrides}


# ---------------------------------------------------------------------------
# _safe_float
# ---------------------------------------------------------------------------


class TestSafeFloat:
    def test_normal_float(self) -> None:
        assert _safe_float(3.14) == 3.14

    def test_int_converted(self) -> None:
        assert _safe_float(42) == 42.0

    def test_string_converted(self) -> None:
        assert _safe_float("3.14") == 3.14

    def test_nan_returns_none(self) -> None:
        assert _safe_float(float("nan")) is None

    def test_inf_returns_none(self) -> None:
        assert _safe_float(float("inf")) is None

    def test_neg_inf_returns_none(self) -> None:
        assert _safe_float(float("-inf")) is None

    def test_none_returns_none(self) -> None:
        assert _safe_float(None) is None

    def test_non_numeric_string_returns_none(self) -> None:
        assert _safe_float("not-a-number") is None


# ---------------------------------------------------------------------------
# _safe_int
# ---------------------------------------------------------------------------


class TestSafeInt:
    def test_normal_int(self) -> None:
        assert _safe_int(42) == 42

    def test_float_truncated(self) -> None:
        assert _safe_int(3.9) == 3

    def test_string_converted(self) -> None:
        assert _safe_int("99") == 99

    def test_none_returns_none(self) -> None:
        assert _safe_int(None) is None

    def test_non_numeric_returns_none(self) -> None:
        assert _safe_int("xyz") is None


# ---------------------------------------------------------------------------
# _maybe_set_str
# ---------------------------------------------------------------------------


class TestMaybeSetStr:
    def test_sets_value(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_str(h, "key", "value")
        assert h["key"] == "value"

    def test_strips_whitespace(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_str(h, "key", "  padded  ")
        assert h["key"] == "padded"

    def test_none_skipped(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_str(h, "key", None)
        assert "key" not in h

    def test_empty_string_skipped(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_str(h, "key", "")
        assert "key" not in h

    def test_whitespace_only_skipped(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_str(h, "key", "   ")
        assert "key" not in h


# ---------------------------------------------------------------------------
# _maybe_set_numeric
# ---------------------------------------------------------------------------


class TestMaybeSetNumeric:
    def test_converts_to_decimal(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_numeric(h, "key", 3.14)
        assert isinstance(h["key"], Decimal)

    def test_nan_dropped(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_numeric(h, "key", float("nan"))
        assert "key" not in h

    def test_inf_dropped(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_numeric(h, "key", float("inf"))
        assert "key" not in h

    def test_none_dropped(self) -> None:
        h: dict[str, Any] = {}
        _maybe_set_numeric(h, "key", None)
        assert "key" not in h


# ---------------------------------------------------------------------------
# _extract_hints
# ---------------------------------------------------------------------------


class TestExtractHints:
    def test_empty_raw_returns_empty_dict(self) -> None:
        assert _extract_hints({}) == {}

    def test_instrument_extracted(self) -> None:
        hints = _extract_hints(_raw())
        assert hints["instrument"] == "STIS"

    def test_target_name_extracted(self) -> None:
        hints = _extract_hints(_raw())
        assert hints["target_name"] == "V339-DEL"

    def test_proposal_id_extracted(self) -> None:
        hints = _extract_hints(_raw())
        assert hints["proposal_id"] == "13828"

    def test_numeric_hints_converted_to_decimal(self) -> None:
        hints = _extract_hints(_raw())
        assert isinstance(hints["t_min_mjd"], Decimal)
        assert isinstance(hints["t_max_mjd"], Decimal)

    def test_nan_times_dropped(self) -> None:
        hints = _extract_hints(_raw(t_min=float("nan"), t_max=float("inf")))
        assert "t_min_mjd" not in hints
        assert "t_max_mjd" not in hints

    def test_all_hints_populated(self) -> None:
        hints = _extract_hints(_raw())
        assert set(hints.keys()) == {
            "instrument",
            "target_name",
            "proposal_id",
            "obs_id",
            "t_min_mjd",
            "t_max_mjd",
        }


# ---------------------------------------------------------------------------
# MASTAdapter.normalize
# ---------------------------------------------------------------------------


class TestNormalize:
    def setup_method(self) -> None:
        self.adapter = MASTAdapter()

    def test_native_id_strategy(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw())
        assert result is not None
        assert result["identity_strategy"] == "NATIVE_ID"

    def test_provider_product_key_is_filename(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw())
        assert result is not None
        assert result["provider_product_key"] == ("hst_13828_stis_v339-del_e140m_ocoj08_cspec.fits")

    def test_locator_identity_uses_product_id_prefix(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw())
        assert result is not None
        assert result["locator_identity"].startswith("provider_product_id:")

    def test_locator_url_constructed_from_data_uri(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw())
        assert result is not None
        locators = result["locators"]
        assert len(locators) == 1
        assert locators[0]["kind"] == "URL"
        assert locators[0]["role"] == "PRIMARY"
        assert "mast.stsci.edu" in locators[0]["value"]
        assert "mast:HST/product/" in locators[0]["value"]

    def test_provider_is_mast(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw())
        assert result is not None
        assert result["provider"] == "MAST"

    def test_nova_id_propagated(self) -> None:
        result = self.adapter.normalize(nova_id="nova-xyz", raw=_raw())
        assert result is not None
        assert result["nova_id"] == "nova-xyz"

    def test_hints_populated(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw())
        assert result is not None
        assert result["hints"]["instrument"] == "STIS"
        assert "t_min_mjd" in result["hints"]

    def test_returns_none_when_product_filename_missing(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw(productFilename=None))
        assert result is None

    def test_returns_none_when_product_filename_empty(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw(productFilename=""))
        assert result is None

    def test_returns_none_when_data_uri_missing(self) -> None:
        result = self.adapter.normalize(nova_id=_NOVA_ID, raw=_raw(dataURI=None))
        assert result is None


# ---------------------------------------------------------------------------
# MASTAdapter.query
# ---------------------------------------------------------------------------


class TestQuery:
    def setup_method(self) -> None:
        self.adapter = MASTAdapter()

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_returns_cspec_products_for_primary_name(self, mock_sleep: Any, mock_obs: Any) -> None:
        mock_obs.query_object.return_value = _obs_table()
        mock_obs.get_product_list.return_value = _prod_table()

        result = self.adapter.query(
            nova_id=_NOVA_ID,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            primary_name=_PRIMARY_NAME,
            aliases=_ALIASES,
        )

        assert len(result) == 1
        assert result[0]["productFilename"].endswith("_cspec.fits")
        mock_obs.query_object.assert_called_once_with(_PRIMARY_NAME, radius="1 arcmin")

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_alias_fallback_when_primary_has_no_hasp(self, mock_sleep: Any, mock_obs: Any) -> None:
        """Primary name finds non-HASP obs; first alias finds HASP."""
        non_hasp = _obs_table([_obs_row(project="HST")])  # raw, not HASP
        hasp = _obs_table()

        mock_obs.query_object.side_effect = [non_hasp, hasp]
        mock_obs.get_product_list.return_value = _prod_table()

        result = self.adapter.query(
            nova_id=_NOVA_ID,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            primary_name=_PRIMARY_NAME,
            aliases=_ALIASES,
        )

        assert len(result) == 1
        assert mock_obs.query_object.call_count == 2
        mock_obs.query_object.assert_called_with(_ALIASES[0], radius="1 arcmin")

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_empty_results_from_all_names(self, mock_sleep: Any, mock_obs: Any) -> None:
        mock_obs.query_object.return_value = _obs_table([])

        result = self.adapter.query(
            nova_id=_NOVA_ID,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            primary_name=_PRIMARY_NAME,
            aliases=_ALIASES,
        )

        assert result == []
        # primary + 2 aliases = 3 calls
        assert mock_obs.query_object.call_count == 3

    def test_raises_value_error_when_no_primary_name(self) -> None:
        with pytest.raises(ValueError, match="primary_name"):
            self.adapter.query(
                nova_id=_NOVA_ID,
                ra_deg=_RA_DEG,
                dec_deg=_DEC_DEG,
                primary_name=None,
                aliases=_ALIASES,
            )

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_filters_non_hst_and_non_hasp(self, mock_sleep: Any, mock_obs: Any) -> None:
        """Only HST + spectrum + HASP observations pass the filter."""
        rows = [
            _obs_row(),  # valid
            _obs_row(obs_collection="JWST", obsid=99002),  # wrong collection
            _obs_row(dataproduct_type="image", obsid=99003),  # wrong type
            _obs_row(project="HST", obsid=99004),  # raw, not HASP
        ]
        mock_obs.query_object.return_value = _obs_table(rows)
        mock_obs.get_product_list.return_value = _prod_table()

        result = self.adapter.query(
            nova_id=_NOVA_ID,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            primary_name=_PRIMARY_NAME,
        )

        assert len(result) == 1

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_filters_non_cspec_products(self, mock_sleep: Any, mock_obs: Any) -> None:
        products = [
            _prod_row(),  # valid cspec
            _prod_row(
                productFilename="oc7r06010_x1d.fits",
                dataURI="mast:HST/product/oc7r06010_x1d.fits",
            ),  # raw x1d
        ]
        mock_obs.query_object.return_value = _obs_table()
        mock_obs.get_product_list.return_value = _prod_table(products)

        result = self.adapter.query(
            nova_id=_NOVA_ID,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            primary_name=_PRIMARY_NAME,
        )

        assert len(result) == 1
        assert result[0]["productFilename"].endswith("_cspec.fits")

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_retryable_error_after_all_attempts_fail(self, mock_sleep: Any, mock_obs: Any) -> None:
        from nova_common.errors import RetryableError

        mock_obs.query_object.side_effect = ConnectionError("MAST down")

        with pytest.raises(RetryableError, match="MAST query_object failed"):
            self.adapter.query(
                nova_id=_NOVA_ID,
                ra_deg=_RA_DEG,
                dec_deg=_DEC_DEG,
                primary_name=_PRIMARY_NAME,
            )

        assert mock_obs.query_object.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_retry_then_succeed(self, mock_sleep: Any, mock_obs: Any) -> None:
        """query_object fails on first attempt, succeeds on second."""
        mock_obs.query_object.side_effect = [
            ConnectionError("timeout"),
            _obs_table(),
        ]
        mock_obs.get_product_list.return_value = _prod_table()

        result = self.adapter.query(
            nova_id=_NOVA_ID,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            primary_name=_PRIMARY_NAME,
        )

        assert len(result) == 1
        assert mock_obs.query_object.call_count == 2
        assert any(c.args == (3,) for c in mock_sleep.call_args_list)

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_enriches_products_with_obs_metadata(self, mock_sleep: Any, mock_obs: Any) -> None:
        mock_obs.query_object.return_value = _obs_table(
            [
                _obs_row(instrument_name="COS/FUV", proposal_id=15890),
            ]
        )
        mock_obs.get_product_list.return_value = _prod_table()

        result = self.adapter.query(
            nova_id=_NOVA_ID,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            primary_name=_PRIMARY_NAME,
        )

        assert len(result) == 1
        assert result[0]["instrument_name"] == "COS/FUV"
        assert result[0]["proposal_id"] == "15890"

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_duplicate_alias_skipped(self, mock_sleep: Any, mock_obs: Any) -> None:
        """Alias identical to primary_name is not queried again."""
        mock_obs.query_object.return_value = _obs_table([])

        self.adapter.query(
            nova_id=_NOVA_ID,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            primary_name=_PRIMARY_NAME,
            aliases=[_PRIMARY_NAME, "Other Name"],
        )

        # primary + "Other Name" = 2 (duplicate skipped)
        assert mock_obs.query_object.call_count == 2

    @patch("spectra_discoverer.adapters.mast.Observations")
    @patch("spectra_discoverer.adapters.mast.time.sleep")
    def test_warning_logged_on_retry(
        self, mock_sleep: Any, mock_obs: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A WARNING log is emitted for each failed attempt that will be retried."""
        mock_obs.query_object.side_effect = [
            ConnectionError("oops"),
            _obs_table(),
        ]
        mock_obs.get_product_list.return_value = _prod_table()

        with caplog.at_level(logging.WARNING):
            self.adapter.query(
                nova_id=_NOVA_ID,
                ra_deg=_RA_DEG,
                dec_deg=_DEC_DEG,
                primary_name=_PRIMARY_NAME,
            )

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("attempt 1/3 failed" in msg for msg in warning_messages)


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


class TestProtocol:
    def test_satisfies_protocol(self) -> None:
        from spectra_discoverer.adapters.base import SpectraDiscoveryAdapter

        adapter = MASTAdapter()
        assert isinstance(adapter, SpectraDiscoveryAdapter)

    def test_provider_string(self) -> None:
        assert MASTAdapter().provider == "MAST"
