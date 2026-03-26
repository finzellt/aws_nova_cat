"""Unit tests for services/ticket_parser/parser.py.

Coverage:
  Stage 1 — parse_ticket_file
    happy path (key-value parse, NA preservation, empty-line skipping,
    first-colon-only split, whitespace stripping, str/Path interop)
    error: no ':' delimiter  (TicketParseError with line_number)
    error: duplicate key     (TicketParseError with line_number)

  Stage 2 — validate_ticket
    photometry happy path  (grounded in real V4739 Sgr ticket)
    spectra happy path     (grounded in real GQ Mus ticket)
    discrimination errors  (both discriminators present; neither present)
    unknown key rejection  (photometry and spectra)
    _coerce_value branches (NA, bool, float, int-pair, int, string default)
    normalizations         (wavelength_regime and ticket_status lowercased)
    Pydantic failure re-raised as TicketParseError
"""

from __future__ import annotations

from pathlib import Path

import pytest

# _coerce_value is private but tested directly for branch coverage.
# mypy strict does not disallow importing underscore-prefixed names from
# regular (non-stub) modules; no noqa comment is required.
from ticket_parser.parser import (
    TicketParseError,
    _coerce_value,
    parse_ticket_file,
    validate_ticket,
)

from contracts.models.tickets import PhotometryTicket, SpectraTicket

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(tmp_path: Path, content: str) -> Path:
    """Write *content* verbatim to a tmp ticket file and return the path."""
    p = tmp_path / "ticket.txt"
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Constants: raw dicts mirroring the two real tickets exactly
#
# These are the key-value pairs that parse_ticket_file would return for each
# real file.  Keeping them as module-level constants means Stage 2 tests can
# be purely in-memory — no filesystem I/O required.
# ---------------------------------------------------------------------------

# Mirrors V4739_Sgr_Livingston_optical_Photometry.txt exactly.
_V4739_RAW: dict[str, str] = {
    "OBJECT NAME": "V4739_Sgr",
    "TIME UNITS": "days",
    "FLUX UNITS": "mags",
    "FLUX ERROR UNITS": "mags",
    "FILTER SYSTEM": "Johnson-Cousins",
    "MAGNITUDE SYSTEM": "Vega",
    "WAVELENGTH REGIME": "optical",
    "TIME SYSTEM": "JD",
    "ASSUMED DATE OF OUTBURST": "NA",
    "TELESCOPE": "Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector",
    "OBSERVER": "Gilmore, A. C. & Kilmartin, P. M.",
    "REFERENCE": "Livingston et al. (2001)",
    "BIBCODE": "2001IBVS.5172....1L",
    "DATA FILENAME": "V4739_Sgr_Livingston_optical_Photometry.csv",
    "TIME COLUMN NUMBER": "0",
    "FLUX COLUMN NUMBER": "1",
    "FLUX ERROR COLUMN NUMBER": "2",
    "FILTER/FREQUENCY/ENERGY RANGE COLUMN NUMBER": "3",
    "UPPER LIMIT FLAG COLUMN NUMBER": "4",
    "TELESCOPE COLUMN NUMBER": "5",
    "OBSERVER COLUMN NUMBER": "6",
    "FILTER SYSTEM COLUMN NUMBER": "7",
    "TICKET STATUS": "completed",
}

# Mirrors GQ_Mus_Williams_Optical_Spectra.txt exactly.
_GQMUS_RAW: dict[str, str] = {
    "OBJECT NAME": "GQ_Mus",
    "FLUX UNITS": "NA",
    "FLUX ERROR UNITS": "NA",
    "WAVELENGTH REGIME": "Optical",  # mixed-case — normalised to 'optical' by Stage 2
    "TIME SYSTEM": "JD",
    "ASSUMED DATE OF OUTBURST": "NA",
    "REFERENCE": "Williams et al. (1992)",
    "BIBCODE": "1992AJ....104..725W",
    "DEREDDENED FLAG": "False",
    "METADATA FILENAME": "GQ_Mus_Williams_Optical_Spectra_MetaData.csv",
    "FILENAME COLUMN": "0",
    "WAVELENGTH COLUMN": "1",
    "FLUX COLUMN": "2",
    "FLUX ERROR COLUMN": "3",
    "FLUX UNITS COLUMN": "4",
    "DATE COLUMN": "5",
    "TELESCOPE COLUMN": "7",
    "INSTRUMENT COLUMN": "8",
    "OBSERVER COLUMN": "6",
    "SNR COLUMN": "NA",
    "DISPERSION COLUMN": "9",
    "RESOLUTION COLUMN": "NA",
    "WAVELENGTH RANGE COLUMN": "10,11",
    "TICKET STATUS": "Completed",  # mixed-case — normalised to 'completed' by Stage 2
}


# ============================================================================
# Stage 1: parse_ticket_file
# ============================================================================


class TestParseTicketFileHappyPath:
    """Correct files return the expected raw dict."""

    def test_basic_key_value_pairs(self, tmp_path: Path) -> None:
        p = _write(
            tmp_path,
            "OBJECT NAME: V4739_Sgr\nFLUX UNITS: mags\nBIBCODE: 2001IBVS.5172....1L\n",
        )
        result = parse_ticket_file(p)
        assert result == {
            "OBJECT NAME": "V4739_Sgr",
            "FLUX UNITS": "mags",
            "BIBCODE": "2001IBVS.5172....1L",
        }

    def test_na_value_preserved_as_literal_string(self, tmp_path: Path) -> None:
        """NA must survive Stage 1 unchanged; Stage 2 converts it to None."""
        p = _write(tmp_path, "ASSUMED DATE OF OUTBURST: NA\n")
        result = parse_ticket_file(p)
        assert result["ASSUMED DATE OF OUTBURST"] == "NA"

    def test_empty_and_whitespace_only_lines_skipped(self, tmp_path: Path) -> None:
        p = _write(
            tmp_path,
            "OBJECT NAME: Nova\n\n   \nFLUX UNITS: mags\n",
        )
        result = parse_ticket_file(p)
        assert list(result.keys()) == ["OBJECT NAME", "FLUX UNITS"]

    def test_first_colon_only_split_preserves_colon_in_value(self, tmp_path: Path) -> None:
        """Values that themselves contain ':' must be captured in full."""
        p = _write(tmp_path, "TELESCOPE: ESO 3.6m:NTT\n")
        result = parse_ticket_file(p)
        assert result["TELESCOPE"] == "ESO 3.6m:NTT"

    def test_leading_and_trailing_whitespace_stripped(self, tmp_path: Path) -> None:
        p = _write(tmp_path, "  OBJECT NAME  :  V4739_Sgr  \n")
        result = parse_ticket_file(p)
        assert result == {"OBJECT NAME": "V4739_Sgr"}

    def test_accepts_str_path(self, tmp_path: Path) -> None:
        """Both str and Path arguments are accepted."""
        p = _write(tmp_path, "OBJECT NAME: Nova\n")
        assert parse_ticket_file(str(p)) == parse_ticket_file(p)


class TestParseTicketFileErrors:
    """Formatting violations raise TicketParseError with correct metadata."""

    def test_no_colon_raises_with_line_number(self, tmp_path: Path) -> None:
        p = _write(
            tmp_path,
            "OBJECT NAME: GQ_Mus\nBAD LINE NO COLON\n",
        )
        with pytest.raises(TicketParseError) as exc_info:
            parse_ticket_file(p)
        err = exc_info.value
        assert err.line_number == 2
        assert "No ':'" in err.reason

    def test_no_colon_path_included_in_str(self, tmp_path: Path) -> None:
        """The file path appears in the stringified error."""
        p = _write(tmp_path, "BAD\n")
        with pytest.raises(TicketParseError) as exc_info:
            parse_ticket_file(p)
        assert str(p) in str(exc_info.value)

    def test_duplicate_key_raises_at_second_occurrence(self, tmp_path: Path) -> None:
        p = _write(
            tmp_path,
            "OBJECT NAME: V4739_Sgr\nFLUX UNITS: mags\nOBJECT NAME: GQ_Mus\n",
        )
        with pytest.raises(TicketParseError) as exc_info:
            parse_ticket_file(p)
        err = exc_info.value
        assert err.line_number == 3
        assert "Duplicate key" in err.reason
        assert "'OBJECT NAME'" in err.reason

    def test_duplicate_key_on_line_two(self, tmp_path: Path) -> None:
        p = _write(tmp_path, "FLUX UNITS: mags\nFLUX UNITS: ergs\n")
        with pytest.raises(TicketParseError) as exc_info:
            parse_ticket_file(p)
        assert exc_info.value.line_number == 2


# ============================================================================
# Stage 2: photometry happy path (grounded in real V4739 Sgr ticket)
# ============================================================================


class TestValidateTicketPhotometryHappyPath:
    """validate_ticket on the V4739 Sgr raw dict produces a correct model."""

    def test_returns_photometry_ticket_instance(self) -> None:
        result = validate_ticket(_V4739_RAW, path="V4739_Sgr.txt")
        assert isinstance(result, PhotometryTicket)
        assert result.ticket_type == "photometry"

    def test_common_fields(self) -> None:
        result = validate_ticket(_V4739_RAW)
        assert result.object_name == "V4739_Sgr"
        assert result.wavelength_regime == "optical"
        assert result.time_system == "JD"
        assert result.assumed_outburst_date is None  # NA → None
        assert result.reference == "Livingston et al. (2001)"
        assert result.bibcode == "2001IBVS.5172....1L"
        assert result.ticket_status == "completed"

    def test_header_level_defaults(self) -> None:
        result = validate_ticket(_V4739_RAW)
        assert isinstance(result, PhotometryTicket)
        assert result.time_units == "days"
        assert result.flux_units == "mags"
        assert result.flux_error_units == "mags"
        assert result.filter_system == "Johnson-Cousins"
        assert result.magnitude_system == "Vega"
        assert result.telescope == ("Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector")
        assert result.observer == "Gilmore, A. C. & Kilmartin, P. M."
        assert result.data_filename == "V4739_Sgr_Livingston_optical_Photometry.csv"

    def test_column_indices(self) -> None:
        result = validate_ticket(_V4739_RAW)
        assert isinstance(result, PhotometryTicket)
        assert result.time_col == 0
        assert result.flux_col == 1
        assert result.flux_error_col == 2
        assert result.filter_col == 3
        assert result.upper_limit_flag_col == 4
        assert result.telescope_col == 5
        assert result.observer_col == 6
        assert result.filter_system_col == 7


# ============================================================================
# Stage 2: spectra happy path (grounded in real GQ Mus ticket)
# ============================================================================


class TestValidateTicketSpectraHappyPath:
    """validate_ticket on the GQ Mus raw dict produces a correct model."""

    def test_returns_spectra_ticket_instance(self) -> None:
        result = validate_ticket(_GQMUS_RAW, path="GQ_Mus.txt")
        assert isinstance(result, SpectraTicket)
        assert result.ticket_type == "spectra"

    def test_common_fields_with_normalization(self) -> None:
        """'Optical' → 'optical'; 'Completed' → 'completed'."""
        result = validate_ticket(_GQMUS_RAW)
        assert result.object_name == "GQ_Mus"
        assert result.wavelength_regime == "optical"  # 'Optical' normalised
        assert result.time_system == "JD"
        assert result.assumed_outburst_date is None
        assert result.bibcode == "1992AJ....104..725W"
        assert result.ticket_status == "completed"  # 'Completed' normalised

    def test_header_level_fields(self) -> None:
        result = validate_ticket(_GQMUS_RAW)
        assert isinstance(result, SpectraTicket)
        assert result.flux_units is None  # NA → None
        assert result.flux_error_units is None  # NA → None
        assert result.dereddened is False
        assert result.metadata_filename == "GQ_Mus_Williams_Optical_Spectra_MetaData.csv"

    def test_column_indices_including_nas_and_int_pair(self) -> None:
        result = validate_ticket(_GQMUS_RAW)
        assert isinstance(result, SpectraTicket)
        assert result.filename_col == 0
        assert result.wavelength_col == 1
        assert result.flux_col == 2
        assert result.flux_error_col == 3
        assert result.flux_units_col == 4
        assert result.date_col == 5
        assert result.observer_col == 6
        assert result.telescope_col == 7
        assert result.instrument_col == 8
        assert result.snr_col is None  # NA → None
        assert result.dispersion_col == 9
        assert result.resolution_col is None  # NA → None
        assert result.wavelength_range_cols == (10, 11)


# ============================================================================
# Stage 2: discrimination errors
# ============================================================================

# Minimal raw dict carrying only the fields common to both ticket types.
_COMMON_ONLY: dict[str, str] = {
    "OBJECT NAME": "Nova",
    "WAVELENGTH REGIME": "optical",
    "TIME SYSTEM": "JD",
    "ASSUMED DATE OF OUTBURST": "NA",
    "REFERENCE": "Author et al. (2000)",
    "BIBCODE": "2000ApJ...999..001A",
    "TICKET STATUS": "completed",
}


class TestDiscriminationErrors:
    def test_both_discriminators_raises(self) -> None:
        """Both DATA FILENAME and METADATA FILENAME present → ambiguous."""
        raw = {
            **_COMMON_ONLY,
            "DATA FILENAME": "data.csv",
            "METADATA FILENAME": "meta.csv",
        }
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(raw, path="ambiguous.txt")
        assert "both DATA FILENAME and METADATA FILENAME" in exc_info.value.reason

    def test_neither_discriminator_raises(self) -> None:
        """Neither discriminator key present → cannot determine type."""
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(_COMMON_ONLY, path="neither.txt")
        assert "neither DATA FILENAME nor METADATA FILENAME" in exc_info.value.reason


# ============================================================================
# Stage 2: unknown key rejection
# ============================================================================


class TestUnknownKeyRejection:
    def test_unknown_key_in_photometry_context(self) -> None:
        """A key not in the photometry map is rejected before Pydantic runs."""
        raw = {**_V4739_RAW, "ROGUE KEY": "bad_value"}
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(raw, path="phot.txt")
        err = exc_info.value
        assert "'ROGUE KEY'" in err.reason
        assert "photometry ticket" in err.reason

    def test_unknown_key_in_spectra_context(self) -> None:
        """A key not in the spectra map is rejected before Pydantic runs."""
        raw = {**_GQMUS_RAW, "EXTRA FIELD": "unexpected"}
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(raw, path="spec.txt")
        err = exc_info.value
        assert "'EXTRA FIELD'" in err.reason
        assert "spectra ticket" in err.reason


# ============================================================================
# _coerce_value: each branch
# ============================================================================


class TestCoerceValueNA:
    """Branch 1: any NA variant → None (universal, applied before type checks)."""

    def test_na_uppercase(self) -> None:
        assert _coerce_value("flux_error_col", "NA") is None

    def test_na_lowercase(self) -> None:
        assert _coerce_value("flux_error_col", "na") is None

    def test_na_mixed_case(self) -> None:
        assert _coerce_value("snr_col", "Na") is None

    def test_na_applies_to_bool_field(self) -> None:
        """NA short-circuits before the bool check — returns None for bool fields too."""
        assert _coerce_value("dereddened", "NA") is None

    def test_na_applies_to_float_field(self) -> None:
        assert _coerce_value("assumed_outburst_date", "NA") is None


class TestCoerceValueBool:
    """Branch 2: bool fields accept 'true'/'false' case-insensitively."""

    def test_true_capitalised(self) -> None:
        assert _coerce_value("dereddened", "True") is True

    def test_true_lowercase(self) -> None:
        assert _coerce_value("dereddened", "true") is True

    def test_true_uppercase(self) -> None:
        assert _coerce_value("dereddened", "TRUE") is True

    def test_false_capitalised(self) -> None:
        assert _coerce_value("dereddened", "False") is False

    def test_false_lowercase(self) -> None:
        assert _coerce_value("dereddened", "false") is False

    def test_invalid_bool_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Expected 'True' or 'False'"):
            _coerce_value("dereddened", "maybe")

    def test_invalid_bool_error_names_field(self) -> None:
        with pytest.raises(ValueError, match="'dereddened'"):
            _coerce_value("dereddened", "1")


class TestCoerceValueFloat:
    """Branch 3: float fields parsed from numeric strings."""

    def test_integer_string_accepted(self) -> None:
        result = _coerce_value("assumed_outburst_date", "2451545")
        assert isinstance(result, float)
        assert result == pytest.approx(2451545.0)

    def test_decimal_string_accepted(self) -> None:
        result = _coerce_value("assumed_outburst_date", "2451545.5")
        assert result == pytest.approx(2451545.5)

    def test_scientific_notation_accepted(self) -> None:
        result = _coerce_value("assumed_outburst_date", "2.44732e+06")
        assert result == pytest.approx(2_447_320.0)

    def test_non_numeric_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Expected a numeric value"):
            _coerce_value("assumed_outburst_date", "not_a_float")

    def test_invalid_float_error_names_field(self) -> None:
        with pytest.raises(ValueError, match="'assumed_outburst_date'"):
            _coerce_value("assumed_outburst_date", "abc")


class TestCoerceValueIntPair:
    """Branch 4: int-pair fields parse comma-separated 'N,M' → tuple[int, int]."""

    def test_valid_pair(self) -> None:
        assert _coerce_value("wavelength_range_cols", "10,11") == (10, 11)

    def test_spaces_around_comma_stripped(self) -> None:
        assert _coerce_value("wavelength_range_cols", "10, 11") == (10, 11)

    def test_zero_based_pair(self) -> None:
        assert _coerce_value("wavelength_range_cols", "0,1") == (0, 1)

    def test_too_many_parts_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected 'N,M'"):
            _coerce_value("wavelength_range_cols", "1,2,3")

    def test_single_value_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected 'N,M'"):
            _coerce_value("wavelength_range_cols", "10")

    def test_non_integer_parts_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected integer pair"):
            _coerce_value("wavelength_range_cols", "a,b")

    def test_float_parts_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected integer pair"):
            _coerce_value("wavelength_range_cols", "1.5,2.5")


class TestCoerceValueInt:
    """Branch 5: int fields coerced from digit strings."""

    def test_zero(self) -> None:
        assert _coerce_value("time_col", "0") == 0

    def test_positive(self) -> None:
        assert _coerce_value("flux_col", "7") == 7

    def test_larger_index(self) -> None:
        assert _coerce_value("date_col", "11") == 11

    def test_non_integer_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected an integer"):
            _coerce_value("time_col", "abc")

    def test_float_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected an integer"):
            _coerce_value("flux_col", "1.5")

    def test_invalid_int_error_names_field(self) -> None:
        with pytest.raises(ValueError, match="'time_col'"):
            _coerce_value("time_col", "bad")


class TestCoerceValueStringDefault:
    """Branch 6: all other field names → value returned as-is (already stripped)."""

    def test_object_name_preserved(self) -> None:
        assert _coerce_value("object_name", "V4739_Sgr") == "V4739_Sgr"

    def test_reference_with_punctuation_preserved(self) -> None:
        assert _coerce_value("reference", "Doe et al. (2001)") == "Doe et al. (2001)"

    def test_bibcode_preserved(self) -> None:
        assert _coerce_value("bibcode", "2001IBVS.5172....1L") == "2001IBVS.5172....1L"

    def test_filter_system_preserved(self) -> None:
        assert _coerce_value("filter_system", "Johnson-Cousins") == "Johnson-Cousins"

    def test_telescope_with_spaces_preserved(self) -> None:
        value = "Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector"
        assert _coerce_value("telescope", value) == value

    def test_observer_with_commas_preserved(self) -> None:
        value = "Gilmore, A. C. & Kilmartin, P. M."
        assert _coerce_value("observer", value) == value


# ============================================================================
# Stage 2: normalizations
# ============================================================================


class TestNormalizations:
    """wavelength_regime and ticket_status are lowercased unconditionally."""

    def test_wavelength_regime_uppercased_input_normalised(self) -> None:
        raw = {**_V4739_RAW, "WAVELENGTH REGIME": "OPTICAL"}
        result = validate_ticket(raw)
        assert result.wavelength_regime == "optical"

    def test_wavelength_regime_mixed_case_normalised(self) -> None:
        """Verifies the real GQ Mus ticket value 'Optical' → 'optical'."""
        result = validate_ticket(_GQMUS_RAW)
        assert result.wavelength_regime == "optical"

    def test_ticket_status_mixed_case_normalised(self) -> None:
        """Verifies the real GQ Mus ticket value 'Completed' → 'completed'."""
        result = validate_ticket(_GQMUS_RAW)
        assert result.ticket_status == "completed"

    def test_ticket_status_uppercase_normalised(self) -> None:
        raw = {**_V4739_RAW, "TICKET STATUS": "COMPLETED"}
        result = validate_ticket(raw)
        assert result.ticket_status == "completed"

    def test_already_lowercase_unchanged(self) -> None:
        """Values already lowercase pass through without error."""
        result = validate_ticket(_V4739_RAW)
        assert result.wavelength_regime == "optical"
        assert result.ticket_status == "completed"


# ============================================================================
# Stage 2: Pydantic validation failure re-raised as TicketParseError
# ============================================================================


class TestPydanticFailureReRaise:
    """Pydantic ValidationError is caught and re-raised as TicketParseError so
    callers never see a raw Pydantic error surface."""

    def test_missing_required_field_raises_ticket_parse_error(self) -> None:
        """Removing object_name (required) triggers Pydantic failure."""
        raw = {k: v for k, v in _V4739_RAW.items() if k != "OBJECT NAME"}
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(raw, path="test.txt")
        assert "Pydantic validation failed" in exc_info.value.reason

    def test_bibcode_too_short_raises_ticket_parse_error(self) -> None:
        """Bibcode shorter than 19 chars violates the min_length=19 constraint."""
        raw = {**_V4739_RAW, "BIBCODE": "2001IBVS"}  # 8 chars, not 19
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(raw, path="test.txt")
        assert "Pydantic validation failed" in exc_info.value.reason

    def test_bibcode_too_long_raises_ticket_parse_error(self) -> None:
        """Bibcode longer than 19 chars violates the max_length=19 constraint."""
        raw = {**_V4739_RAW, "BIBCODE": "2001IBVS.5172....1L_EXTRA"}
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(raw, path="test.txt")
        assert "Pydantic validation failed" in exc_info.value.reason

    def test_path_argument_propagated_into_error(self) -> None:
        """The path= argument is stored on the raised TicketParseError."""
        raw = {k: v for k, v in _V4739_RAW.items() if k != "OBJECT NAME"}
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(raw, path="/data/tickets/v4739.txt")
        assert exc_info.value.path == "/data/tickets/v4739.txt"

    def test_pydantic_error_is_chained(self) -> None:
        """The TicketParseError is raised *from* the original ValidationError."""
        raw = {k: v for k, v in _V4739_RAW.items() if k != "OBJECT NAME"}
        with pytest.raises(TicketParseError) as exc_info:
            validate_ticket(raw, path="test.txt")
        assert exc_info.value.__cause__ is not None
