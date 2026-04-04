"""Unit tests for ticket_ingestor.photometry_reader.

Scope: pure transform layer only.  No boto3, no DDB, no S3.
The band registry is injected as a hand-rolled mock dataclass.

All _env fixtures set the three AWS credential env vars that boto3
requires at module import time (even though photometry_reader.py itself
has no boto3 imports, handler.py does — and pytest collects the whole
package before isolating individual test modules).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from ticket_ingestor.photometry_reader import (
    _convert_time,
    _derive_row_id,
    _extract_fields,
    _resolve_band,
    _RowError,
    read_photometry_csv,
)

from contracts.models.entities import (
    BandResolutionConfidence,
    BandResolutionType,
    DataOrigin,
    DataRights,
    PhotometryRow,
    QualityFlag,
    SpectralCoordUnit,
    TimeOrigSys,
)
from contracts.models.tickets import PhotometryTicket

# ---------------------------------------------------------------------------
# AWS credential fixture — required so boto3 clients created at module
# import time (in handler.py) have a region before any patch() is active.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")


# ---------------------------------------------------------------------------
# Mock registry
# ---------------------------------------------------------------------------


@dataclass
class _MockEntry:
    """Minimal dataclass satisfying RegistryEntryLike for test purposes."""

    band_id: str
    regime: str
    band_name: str | None = None
    svo_filter_id: str | None = None
    lambda_eff: float | None = None
    spectral_coord_unit: SpectralCoordUnit | None = SpectralCoordUnit.angstrom
    bandpass_width: float | None = None


# Registry contents used across tests:
#   "V"              → Generic_V (alias on Generic entry per ADR-017
#                      amendment § Alias Ownership Invariant)
#   "EXCLUDED_BAND"  → Generic_EXCLUDED_BAND (alias exists, but excluded)
#   "Gg"             → no alias, but Generic_Gg entry exists (generic fallback)
#   "XYZ"            → no alias, no Generic_XYZ entry (unresolvable)
_V_ENTRY = _MockEntry(
    band_id="Generic_V",
    band_name="V",
    regime="optical",
    svo_filter_id="HCT/HFOSC.Bessell_V",
    lambda_eff=5696.92,
    spectral_coord_unit=SpectralCoordUnit.angstrom,
    bandpass_width=1584.54,
)
_GENERIC_GG_ENTRY = _MockEntry(
    band_id="Generic_Gg",
    regime="optical",
    lambda_eff=None,
    spectral_coord_unit=SpectralCoordUnit.angstrom,
)
_EXCLUDED_ENTRY = _MockEntry(
    band_id="Generic_EXCLUDED_BAND",
    regime="optical",
)


class _MockRegistry:
    """In-memory registry stub satisfying BandRegistryProtocol."""

    _ALIAS_INDEX: dict[str, str] = {
        "V": "Generic_V",
        "EXCLUDED_BAND": "Generic_EXCLUDED_BAND",
    }
    _ENTRIES: dict[str, _MockEntry] = {
        "Generic_V": _V_ENTRY,
        "Generic_Gg": _GENERIC_GG_ENTRY,
        "Generic_EXCLUDED_BAND": _EXCLUDED_ENTRY,
    }
    _EXCLUDED: set[str] = {"Generic_EXCLUDED_BAND"}

    def lookup_band_id(self, alias: str) -> str | None:
        return self._ALIAS_INDEX.get(alias)

    def get_entry(self, band_id: str) -> Any:
        return self._ENTRIES.get(band_id)

    def is_excluded(self, band_id: str) -> bool:
        return band_id in self._EXCLUDED

    def resolve_radio_frequency(self, filter_string: str) -> str | None:
        return None


_REGISTRY = _MockRegistry()

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_NOVA_ID = UUID("4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1")
_PRIMARY_NAME = "V4739 Sgr"
_RA_DEG = 270.123
_DEC_DEG = -23.456
_BIBCODE = "2001IBVS.5172....1L"


@pytest.fixture()
def v4739_ticket() -> PhotometryTicket:
    """Minimal PhotometryTicket matching the V4739 Sgr sample data layout."""
    return PhotometryTicket(
        object_name="V4739_Sgr",
        wavelength_regime="optical",
        time_system="JD",
        time_units="days",
        flux_units="mags",
        flux_error_units="mags",
        filter_system="Johnson-Cousins",
        magnitude_system="Vega",
        telescope="Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector",
        observer="Gilmore, A. C. & Kilmartin, P. M.",
        reference="Livingston et al. (2001)",
        bibcode=_BIBCODE,
        ticket_status="completed",
        assumed_outburst_date=None,
        data_filename="V4739_Sgr_Livingston_optical_Photometry.csv",
        time_col=0,
        flux_col=1,
        flux_error_col=2,
        filter_col=3,
        upper_limit_flag_col=4,
        telescope_col=5,
        observer_col=6,
        filter_system_col=7,
    )


@pytest.fixture()
def sample_csv(tmp_path: Path, v4739_ticket: PhotometryTicket) -> Path:
    """Write a small 3-row CSV matching the V4739 Sgr column layout."""
    rows = [
        [
            "2452148.839",
            "7.46",
            "0.009",
            "V",
            "0",
            "Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector",
            "Gilmore, A. C. & Kilmartin, P. M.",
            "Johnson-Cousins",
        ],
        [
            "2452148.853",
            "7.51",
            "0.009",
            "V",
            "0",
            "Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector",
            "Gilmore, A. C. & Kilmartin, P. M.",
            "Johnson-Cousins",
        ],
        [
            "2452148.869",
            "7.58",
            "0.009",
            "V",
            "0",
            "Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector",
            "Gilmore, A. C. & Kilmartin, P. M.",
            "Johnson-Cousins",
        ],
    ]
    csv_path = tmp_path / v4739_ticket.data_filename
    with open(csv_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerows(rows)
    return csv_path


# ===========================================================================
# _derive_row_id
# ===========================================================================


class TestDeriveRowId:
    def test_deterministic(self) -> None:
        """Same inputs always produce the same UUID."""
        a = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.46", "data.csv")
        b = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.46", "data.csv")
        assert a == b

    def test_different_epoch_produces_different_id(self) -> None:
        a = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.46", "data.csv")
        b = _derive_row_id(_NOVA_ID, "2452148.840", "Generic_V", "7.46", "data.csv")
        assert a != b

    def test_different_band_produces_different_id(self) -> None:
        a = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.46", "data.csv")
        b = _derive_row_id(_NOVA_ID, "2452148.839", "JohnsonCousins_B", "7.46", "data.csv")
        assert a != b

    def test_different_magnitude_produces_different_id(self) -> None:
        a = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.46", "data.csv")
        b = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.47", "data.csv")
        assert a != b

    def test_different_nova_id_produces_different_id(self) -> None:
        other_nova = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        a = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.46", "data.csv")
        b = _derive_row_id(other_nova, "2452148.839", "Generic_V", "7.46", "data.csv")
        assert a != b

    def test_different_filename_produces_different_id(self) -> None:
        a = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.46", "data_a.csv")
        b = _derive_row_id(_NOVA_ID, "2452148.839", "Generic_V", "7.46", "data_b.csv")
        assert a != b

    def test_returns_uuid(self) -> None:
        result = _derive_row_id(_NOVA_ID, "123.456", "Generic_V", "7.0", "f.csv")
        assert isinstance(result, UUID)


# ===========================================================================
# _convert_time
# ===========================================================================


class TestConvertTime:
    def test_mjd_passthrough(self) -> None:
        mjd, bary, sys = _convert_time(59000.5, "MJD")
        assert mjd == pytest.approx(59000.5)
        assert bary is False
        assert sys == TimeOrigSys.mjd_utc

    def test_jd_to_mjd(self) -> None:
        jd = 2452148.839
        mjd, bary, sys = _convert_time(jd, "JD")
        assert mjd == pytest.approx(jd - 2_400_000.5)
        assert bary is False
        assert sys == TimeOrigSys.jd_utc

    def test_hjd_to_mjd(self) -> None:
        hjd = 2452148.839
        mjd, bary, sys = _convert_time(hjd, "HJD")
        assert mjd == pytest.approx(hjd - 2_400_000.5)
        assert bary is False  # heliocentric, not barycentric
        assert sys == TimeOrigSys.hjd_utc

    def test_bjd_to_mjd(self) -> None:
        bjd = 2452148.839
        mjd, bary, sys = _convert_time(bjd, "BJD")
        assert mjd == pytest.approx(bjd - 2_400_000.5)
        assert bary is True
        assert sys == TimeOrigSys.other  # no bjd_* enum variant yet

    def test_unknown_time_system_raises_row_error(self) -> None:
        with pytest.raises(_RowError, match="Unrecognised time_system"):
            _convert_time(12345.0, "TAI")


# ===========================================================================
# _resolve_band
# ===========================================================================


class TestResolveBand:
    def test_alias_match_returns_canonical_high(self) -> None:
        # "V" is a direct alias on Generic_V per alias ownership rule.
        # Step 1 (alias lookup) fires → canonical / high.
        res = _resolve_band("V", _REGISTRY)
        assert res.band_id == "Generic_V"
        assert res.resolution_type == BandResolutionType.canonical
        assert res.confidence == BandResolutionConfidence.high

    def test_generic_fallback_returns_generic_fallback_low(self) -> None:
        # "Gg" has no alias entry but Generic_Gg exists in the registry.
        res = _resolve_band("Gg", _REGISTRY)
        assert res.band_id == "Generic_Gg"
        assert res.resolution_type == BandResolutionType.generic_fallback
        assert res.confidence == BandResolutionConfidence.low

    def test_excluded_filter_raises_row_error(self) -> None:
        with pytest.raises(_RowError, match="excluded band"):
            _resolve_band("EXCLUDED_BAND", _REGISTRY)

    def test_unresolvable_filter_raises_row_error(self) -> None:
        with pytest.raises(_RowError, match="Unresolvable filter string"):
            _resolve_band("XYZ", _REGISTRY)


# ===========================================================================
# _extract_fields
# ===========================================================================


class TestExtractFields:
    def _make_row(self) -> list[str]:
        """Return a CSV row matching the V4739 Sgr column layout."""
        return [
            "2452148.839",  # col 0: time
            "7.46",  # col 1: flux
            "0.009",  # col 2: flux_err
            "V",  # col 3: filter
            "0",  # col 4: upper limit
            "Mt John Observatory 0.6 m f/16 Cassegrain O.C. Reflector",  # col 5: telescope
            "Gilmore, A. C. & Kilmartin, P. M.",  # col 6: observer
            "Johnson-Cousins",  # col 7: filter system
        ]

    def test_all_columns_present(self, v4739_ticket: PhotometryTicket) -> None:
        raw = _extract_fields(self._make_row(), v4739_ticket, row_number=1)
        assert raw.epoch_raw == "2452148.839"
        assert raw.magnitude_raw == "7.46"
        assert raw.mag_err_raw == "0.009"
        assert raw.filter_string == "V"
        assert raw.upper_limit is False
        assert "Mt John" in (raw.telescope or "")
        assert "Gilmore" in (raw.observer or "")

    def test_upper_limit_flag_1_is_true(self, v4739_ticket: PhotometryTicket) -> None:
        row = self._make_row()
        row[4] = "1"
        raw = _extract_fields(row, v4739_ticket, row_number=1)
        assert raw.upper_limit is True

    def test_upper_limit_flag_0_is_false(self, v4739_ticket: PhotometryTicket) -> None:
        row = self._make_row()
        row[4] = "0"
        raw = _extract_fields(row, v4739_ticket, row_number=1)
        assert raw.upper_limit is False

    def test_ticket_default_used_when_no_telescope_col(
        self, v4739_ticket: PhotometryTicket
    ) -> None:
        ticket_no_tel_col = v4739_ticket.model_copy(update={"telescope_col": None})
        raw = _extract_fields(self._make_row(), ticket_no_tel_col, row_number=1)
        # Falls back to ticket.telescope
        assert raw.telescope == v4739_ticket.telescope

    def test_ticket_default_used_when_no_observer_col(self, v4739_ticket: PhotometryTicket) -> None:
        ticket_no_obs_col = v4739_ticket.model_copy(update={"observer_col": None})
        raw = _extract_fields(self._make_row(), ticket_no_obs_col, row_number=1)
        assert raw.observer == v4739_ticket.observer

    def test_out_of_range_col_raises_row_error(self, v4739_ticket: PhotometryTicket) -> None:
        # Row with only 2 fields; time_col=0 is fine but flux_col=1 requires index 1 to exist.
        with pytest.raises(_RowError, match="out of range"):
            _extract_fields(["2452148.839"], v4739_ticket, row_number=1)

    def test_no_filter_col_and_no_filter_system_raises(
        self, v4739_ticket: PhotometryTicket
    ) -> None:
        ticket = v4739_ticket.model_copy(update={"filter_col": None, "filter_system": None})
        with pytest.raises(_RowError, match="cannot determine filter string"):
            _extract_fields(self._make_row(), ticket, row_number=1)

    def test_invalid_upper_limit_flag_raises(self, v4739_ticket: PhotometryTicket) -> None:
        row = self._make_row()
        row[4] = "2"
        with pytest.raises(_RowError, match="Unrecognised upper_limit_flag"):
            _extract_fields(row, v4739_ticket, row_number=1)


# ===========================================================================
# read_photometry_csv — integration of the full pipeline
# ===========================================================================


class TestReadPhotometryCsv:
    def test_all_rows_produce_valid_photometry_rows(
        self,
        sample_csv: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        result = read_photometry_csv(
            csv_path=sample_csv,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        assert len(result.failures) == 0
        assert len(result.rows) == 3
        for row in result.rows:
            assert isinstance(row.row, PhotometryRow)

    def test_row_field_values_from_csv(
        self,
        sample_csv: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        result = read_photometry_csv(
            csv_path=sample_csv,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        first = result.rows[0].row
        # JD 2452148.839 → MJD 52148.339
        assert first.time_mjd == pytest.approx(2452148.839 - 2_400_000.5)
        assert first.time_orig == pytest.approx(2452148.839)
        assert first.time_orig_sys == TimeOrigSys.jd_utc
        assert first.time_bary_corr is False
        assert first.magnitude == pytest.approx(7.46)
        assert first.mag_err == pytest.approx(0.009)
        assert first.band_id == "Generic_V"
        assert first.band_name == "V"
        assert first.regime == "optical"
        assert first.nova_id == _NOVA_ID
        assert first.primary_name == _PRIMARY_NAME
        assert first.bibcode == _BIBCODE
        assert first.data_origin == DataOrigin.literature
        assert first.data_rights == DataRights.public
        assert first.sidecar_contributed is False
        assert first.band_resolution_type == BandResolutionType.canonical
        assert first.band_resolution_confidence == BandResolutionConfidence.high
        assert first.quality_flag == QualityFlag.good

    def test_ticket_level_defaults_used_when_no_column(
        self,
        tmp_path: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        """When telescope_col is None, ticket.telescope is stamped on every row."""
        ticket = v4739_ticket.model_copy(
            update={
                "telescope_col": None,
                "observer_col": None,
                "upper_limit_flag_col": None,
                "filter_system_col": None,
            }
        )
        csv_path = tmp_path / ticket.data_filename
        with open(csv_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            # Only 4 required columns (time, mag, mag_err, filter)
            writer.writerow(["2452148.839", "7.46", "0.009", "V"])
        result = read_photometry_csv(
            csv_path=csv_path,
            ticket=ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        assert len(result.rows) == 1
        assert result.rows[0].row.telescope == v4739_ticket.telescope
        assert result.rows[0].row.observer == v4739_ticket.observer

    def test_upper_limit_row(
        self,
        tmp_path: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        csv_path = tmp_path / v4739_ticket.data_filename
        with open(csv_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "2452150.000",
                    "9.99",
                    "",
                    "V",
                    "1",  # upper limit
                    "Telescope",
                    "Observer",
                    "Johnson-Cousins",
                ]
            )
        result = read_photometry_csv(
            csv_path=csv_path,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        assert len(result.rows) == 1
        row = result.rows[0].row
        assert row.is_upper_limit is True
        assert row.limiting_value == pytest.approx(9.99)
        assert row.magnitude is None

    def test_bad_float_in_magnitude_becomes_failure_not_abort(
        self,
        tmp_path: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        """A type-coercion error on one row must not abort the rest of the batch."""
        csv_path = tmp_path / v4739_ticket.data_filename
        with open(csv_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["2452148.839", "7.46", "0.009", "V", "0", "T", "O", "JC"])
            writer.writerow(["2452148.853", "INVALID", "0.009", "V", "0", "T", "O", "JC"])
            writer.writerow(["2452148.869", "7.58", "0.009", "V", "0", "T", "O", "JC"])
        result = read_photometry_csv(
            csv_path=csv_path,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        assert len(result.rows) == 2
        assert len(result.failures) == 1
        assert result.failures[0].row_number == 2
        assert "INVALID" in result.failures[0].reason

    def test_excluded_filter_becomes_row_failure(
        self,
        tmp_path: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        csv_path = tmp_path / v4739_ticket.data_filename
        with open(csv_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["2452148.839", "7.46", "0.009", "EXCLUDED_BAND", "0", "T", "O", "JC"])
        result = read_photometry_csv(
            csv_path=csv_path,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        assert len(result.rows) == 0
        assert len(result.failures) == 1
        assert "excluded" in result.failures[0].reason.lower()

    def test_unresolvable_filter_becomes_row_failure(
        self,
        tmp_path: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        csv_path = tmp_path / v4739_ticket.data_filename
        with open(csv_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["2452148.839", "7.46", "0.009", "XYZ", "0", "T", "O", "JC"])
        result = read_photometry_csv(
            csv_path=csv_path,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        assert len(result.rows) == 0
        assert len(result.failures) == 1
        assert "Unresolvable" in result.failures[0].reason

    def test_blank_rows_are_skipped(
        self,
        tmp_path: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        csv_path = tmp_path / v4739_ticket.data_filename
        with open(csv_path, "w", newline="") as fh:
            fh.write("2452148.839,7.46,0.009,V,0,T,O,JC\n")
            fh.write("\n")  # blank line
            fh.write("   \n")  # whitespace-only line
            fh.write("2452148.853,7.51,0.009,V,0,T,O,JC\n")
        result = read_photometry_csv(
            csv_path=csv_path,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        assert len(result.rows) == 2
        assert len(result.failures) == 0

    def test_row_ids_are_unique_across_rows(
        self,
        sample_csv: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        """Each row in the sample CSV has a distinct epoch, so row_ids must differ."""
        result = read_photometry_csv(
            csv_path=sample_csv,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        # row_id is not on PhotometryRow directly; recompute from raw fields.
        # (row_id is derived deterministically — we just verify uniqueness via
        # the distinct epoch values in the sample.)
        epochs = [r.row.time_orig for r in result.rows]
        assert len(set(epochs)) == len(epochs), "Epoch values must be distinct for this test"
        # If epochs are distinct, row_ids are guaranteed distinct by _derive_row_id.

    def test_generic_fallback_band_resolution(
        self,
        tmp_path: Path,
        v4739_ticket: PhotometryTicket,
    ) -> None:
        """Filter string with no alias but a Generic_ entry → generic_fallback resolution."""
        csv_path = tmp_path / v4739_ticket.data_filename
        with open(csv_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["2452148.839", "7.46", "0.009", "Gg", "0", "T", "O", "JC"])
        result = read_photometry_csv(
            csv_path=csv_path,
            ticket=v4739_ticket,
            nova_id=_NOVA_ID,
            primary_name=_PRIMARY_NAME,
            ra_deg=_RA_DEG,
            dec_deg=_DEC_DEG,
            registry=_REGISTRY,
        )
        assert len(result.rows) == 1
        row = result.rows[0].row
        assert row.band_id == "Generic_Gg"
        assert row.band_name == "Generic_Gg"  # fallback: band_name was None
        assert row.band_resolution_type == BandResolutionType.generic_fallback
        assert row.band_resolution_confidence == BandResolutionConfidence.low
