"""Tests for radio band alias matching and fuzzy frequency resolution.

Covers:
  - Exact alias match for format variants (space, no-space, underscore)
  - Case-insensitive note: alias lookup is case-sensitive by design;
    the fuzzy resolver handles case-insensitive frequency strings.
  - Nearest-frequency fuzzy matching within ±20% tolerance
  - Out-of-tolerance rejection
"""

from __future__ import annotations

from photometry_ingestor.band_registry.registry import (
    lookup_band_id,
    resolve_radio_frequency,
)

# ---------------------------------------------------------------------------
# Exact alias matching — format variants
# ---------------------------------------------------------------------------


class TestExactAliasMatch:
    """Alias index should match all format variants added to band_registry.json."""

    def test_space_format(self) -> None:
        assert lookup_band_id("4.9 GHz") == "Radio_4.9_GHz"

    def test_no_space_format(self) -> None:
        assert lookup_band_id("4.9GHz") == "Radio_4.9_GHz"

    def test_underscore_format(self) -> None:
        assert lookup_band_id("4.9_GHz") == "Radio_4.9_GHz"

    def test_band_id_itself(self) -> None:
        assert lookup_band_id("Radio_4.9_GHz") == "Radio_4.9_GHz"

    def test_band_name_alias(self) -> None:
        assert lookup_band_id("C band") == "Radio_4.9_GHz"

    def test_integer_freq_space(self) -> None:
        assert lookup_band_id("15 GHz") == "Radio_15_GHz"

    def test_integer_freq_no_space(self) -> None:
        assert lookup_band_id("15GHz") == "Radio_15_GHz"

    def test_integer_freq_underscore(self) -> None:
        assert lookup_band_id("15_GHz") == "Radio_15_GHz"


class TestExactAliasCaseSensitive:
    """Alias lookup is case-sensitive — lowercase 'ghz' should NOT match."""

    def test_lowercase_not_matched(self) -> None:
        assert lookup_band_id("4.9 ghz") is None

    def test_mixed_case_not_matched(self) -> None:
        assert lookup_band_id("4.9 Ghz") is None


# ---------------------------------------------------------------------------
# Fuzzy radio frequency resolution
# ---------------------------------------------------------------------------


class TestFuzzyRadioResolver:
    """resolve_radio_frequency should match frequency strings to the nearest
    registered radio band within ±20% tolerance."""

    def test_exact_center_frequency(self) -> None:
        assert resolve_radio_frequency("4.9 GHz") == "Radio_4.9_GHz"

    def test_near_frequency_ka_band(self) -> None:
        # 36.5 GHz is within 20% of 34.8 GHz (Ka band)
        # |36.5 - 34.8| / 34.8 = 4.9% < 20%
        assert resolve_radio_frequency("36.5 GHz") == "Radio_34.8_GHz"

    def test_near_frequency_c_band(self) -> None:
        # 5.0 GHz is within 20% of 4.9 GHz (C band)
        # |5.0 - 4.9| / 4.9 = 2.0% < 20%
        assert resolve_radio_frequency("5.0 GHz") == "Radio_4.9_GHz"

    def test_case_insensitive(self) -> None:
        assert resolve_radio_frequency("4.9 ghz") == "Radio_4.9_GHz"

    def test_case_insensitive_mixed(self) -> None:
        assert resolve_radio_frequency("4.9 Ghz") == "Radio_4.9_GHz"

    def test_no_space(self) -> None:
        assert resolve_radio_frequency("36.5GHz") == "Radio_34.8_GHz"

    def test_underscore_separator(self) -> None:
        assert resolve_radio_frequency("36.5_GHz") == "Radio_34.8_GHz"

    def test_mhz_conversion(self) -> None:
        # 1400 MHz = 1.4 GHz → Radio_1.4_GHz
        assert resolve_radio_frequency("1400 MHz") == "Radio_1.4_GHz"

    def test_out_of_tolerance_between_bands(self) -> None:
        # 100 GHz: nearest bands are 44 GHz and 230 GHz
        # |100 - 44| / 44 = 127% — way beyond 20%
        # |100 - 230| / 230 = 56.5% — also beyond 20%
        assert resolve_radio_frequency("100 GHz") is None

    def test_non_radio_string(self) -> None:
        assert resolve_radio_frequency("V") is None

    def test_empty_string(self) -> None:
        assert resolve_radio_frequency("") is None

    def test_negative_frequency(self) -> None:
        # Regex won't match negative numbers (no leading minus)
        assert resolve_radio_frequency("-5 GHz") is None

    def test_zero_frequency(self) -> None:
        assert resolve_radio_frequency("0 GHz") is None

    def test_whitespace_padding(self) -> None:
        assert resolve_radio_frequency("  4.9 GHz  ") == "Radio_4.9_GHz"
