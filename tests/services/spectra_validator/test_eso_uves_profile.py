"""
tests/spectra_validator/test_eso_uves_profile.py

Unit tests for EsoUvesProfile and the validate_spectrum entry point.

All tests use synthetic astropy HDULists — no binary test fixtures on disk.
The _make_hdulist() helper produces a minimal but structurally valid UVES file
matching the confirmed real-file shape (1-row BinTable, fixed-length array columns).

Test groups:
  TestEsoUvesProfileMatches   — matches() accept/reject logic
  TestEsoUvesProfileHappyPath — successful validation; output shape and values
  TestEsoUvesProfileMissingColumns  — WAVE/FLUX column absence → quarantine
  TestEsoUvesProfileMissingMetadata — required header fields absent → quarantine
  TestEsoUvesProfileSanityChecks    — the five mandatory sanity checks
  TestValidateSpectrumEntryPoint    — registry dispatch and UNKNOWN_PROFILE
"""

from __future__ import annotations

import os
import pathlib

import numpy as np
import pytest


# Bootstrap astropy cache dirs before import (mirrors handler.py behaviour)
def _bootstrap() -> None:
    base = "/tmp/test_astropy"
    os.environ.setdefault("ASTROPY_CONFIGDIR", f"{base}/config")
    os.environ.setdefault("ASTROPY_CACHE_DIR", f"{base}/cache")
    os.environ.setdefault("XDG_CACHE_HOME", f"{base}/.cache")
    os.environ.setdefault("HOME", base)
    for p in (os.environ["ASTROPY_CONFIGDIR"], os.environ["ASTROPY_CACHE_DIR"]):
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)


_bootstrap()

import astropy.io.fits as fits  # noqa: E402
from spectra_validator.profiles import validate_spectrum  # noqa: E402
from spectra_validator.profiles.base import ProfileResult  # noqa: E402
from spectra_validator.profiles.eso_uves import EsoUvesProfile  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_N = 200  # synthetic spectrum length — enough to exercise all checks


def _make_primary_header(**overrides: object) -> fits.Header:
    """
    Build a minimal UVES primary header with all required fields present.
    Keyword overrides replace or add to the defaults.
    Use a value of None to delete a key (simulates missing header field).
    """
    defaults: dict[str, object] = {
        "SIMPLE": True,
        "BITPIX": 16,
        "NAXIS": 0,
        "INSTRUME": "UVES    ",  # padded as ESO pipeline produces it
        "TELESCOP": "ESO-VLT-U2",
        "MJD-OBS": 56082.05467768,
        "DATE-OBS": "2012-06-04",
        "RA": 267.725219,
        "DEC": -32.62309,
        "EXPTIME": 7199.9979,
        "SPEC_RES": 42310.0,
        "FLUXCAL": "ABSOLUTE",
        "ORIGIN": "ESO",
    }
    defaults.update(overrides)

    header = fits.Header()
    for key, val in defaults.items():
        if val is None:
            continue  # simulate missing keyword
        header[key] = val
    return header


def _make_wave(n: int = _N, descending: bool = False) -> np.ndarray:
    """Strictly monotonic wavelength array in Angstrom."""
    wave = np.linspace(5656.0, 9464.0, n)
    return wave[::-1] if descending else wave


def _make_flux(n: int = _N, nan_fraction: float = 0.0) -> np.ndarray:
    """Flux array with optional NaN injection."""
    rng = np.random.default_rng(42)
    flux = rng.uniform(0.1, 2.0, n)
    if nan_fraction > 0:
        n_nan = int(n * nan_fraction)
        flux[:n_nan] = np.nan
    return flux


def _make_spectrum_hdu(
    wave: np.ndarray | None = None,
    flux: np.ndarray | None = None,
    wave_col_name: str = "WAVE",
    flux_col_name: str = "FLUX",
    include_wave: bool = True,
    include_flux: bool = True,
    hdu_name: str = "SPECTRUM",
) -> fits.BinTableHDU:
    """
    Build a BinTableHDU matching the real UVES shape:
      1 row, fixed-length array columns (shape 1 × N).
    """
    if wave is None:
        wave = _make_wave()
    if flux is None:
        flux = _make_flux()

    n = len(wave)
    cols = []

    if include_wave:
        cols.append(
            fits.Column(
                name=wave_col_name,
                format=f"{n}D",
                unit="angstrom",
                array=wave.reshape(1, n),
            )
        )

    if include_flux:
        flux_unit = "10**(-16)erg.cm**(-2).s**(-1).angstrom**(-1)"
        cols.append(
            fits.Column(
                name=flux_col_name,
                format=f"{n}D",
                unit=flux_unit,
                array=flux.reshape(1, n),
            )
        )
        # ERR column — always present alongside FLUX in real files
        cols.append(
            fits.Column(
                name="ERR",
                format=f"{n}D",
                unit=flux_unit,
                array=(flux * 0.05).reshape(1, n),
            )
        )

    hdu = fits.BinTableHDU.from_columns(cols)
    hdu.name = hdu_name
    return hdu


def _make_hdulist(
    primary_header: fits.Header | None = None,
    spectrum_hdu: fits.BinTableHDU | None = None,
) -> fits.HDUList:
    """Assemble a minimal UVES HDUList from primary header and spectrum HDU."""
    if primary_header is None:
        primary_header = _make_primary_header()
    if spectrum_hdu is None:
        spectrum_hdu = _make_spectrum_hdu()
    primary = fits.PrimaryHDU(header=primary_header)
    return fits.HDUList([primary, spectrum_hdu])


def _product_metadata(
    data_product_id: str = "test-dpid-0001",
    provider: str = "ESO",
) -> dict:
    return {"data_product_id": data_product_id, "provider": provider, "hints": {}}


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def profile() -> EsoUvesProfile:
    return EsoUvesProfile()


# ---------------------------------------------------------------------------
# TestEsoUvesProfileMatches
# ---------------------------------------------------------------------------


class TestEsoUvesProfileMatches:
    def test_matches_eso_uves(self, profile: EsoUvesProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("ESO", hdulist) is True

    def test_matches_uves_with_padding(self, profile: EsoUvesProfile) -> None:
        """INSTRUME='UVES    ' (padded to 8 chars) must still match."""
        header = _make_primary_header(INSTRUME="UVES    ")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is True

    def test_no_match_wrong_provider(self, profile: EsoUvesProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("MAST", hdulist) is False

    def test_no_match_wrong_instrument(self, profile: EsoUvesProfile) -> None:
        header = _make_primary_header(INSTRUME="XSHOOTER")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is False

    def test_no_match_missing_instrume(self, profile: EsoUvesProfile) -> None:
        header = _make_primary_header(INSTRUME=None)
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is False

    def test_matches_uves_lowercase(self, profile: EsoUvesProfile) -> None:
        """INSTRUME comparison is case-insensitive."""
        header = _make_primary_header(INSTRUME="uves")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is True


# ---------------------------------------------------------------------------
# TestEsoUvesProfileHappyPath
# ---------------------------------------------------------------------------


class TestEsoUvesProfileHappyPath:
    @pytest.fixture
    def result(self, profile: EsoUvesProfile) -> ProfileResult:
        hdulist = _make_hdulist()
        return profile.validate(hdulist, _product_metadata())

    def test_success_is_true(self, result: ProfileResult) -> None:
        assert result.success is True

    def test_spectrum_is_populated(self, result: ProfileResult) -> None:
        assert result.spectrum is not None

    def test_profile_id(self, result: ProfileResult) -> None:
        assert result.profile_id == "ESO_UVES"

    def test_spectral_axis_shape(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.spectral_axis.ndim == 1
        assert len(result.spectrum.spectral_axis) == _N

    def test_flux_axis_shape(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.flux_axis.ndim == 1
        assert len(result.spectrum.flux_axis) == _N

    def test_spectral_units(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "angstrom"

    def test_flux_units_present(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.flux_units != ""

    def test_observation_time_iso8601(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        # MJD 56082.05467768 → 2012-06-04T...Z
        assert result.spectrum.observation_time.startswith("2012-06-04T")
        assert result.spectrum.observation_time.endswith("Z")

    def test_observation_mjd(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.observation_mjd is not None
        assert abs(result.spectrum.observation_mjd - 56082.05467768) < 1e-6

    def test_ra_deg(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.target_ra_deg is not None
        assert abs(result.spectrum.target_ra_deg - 267.725219) < 1e-4

    def test_dec_deg(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.target_dec_deg is not None
        assert abs(result.spectrum.target_dec_deg - (-32.62309)) < 1e-4

    def test_instrument(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.instrument == "UVES"

    def test_telescope(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.telescope == "ESO-VLT-U2"

    def test_exposure_time(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.exposure_time_s is not None
        assert abs(result.spectrum.exposure_time_s - 7199.9979) < 0.001

    def test_spectral_resolution(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.spectral_resolution == 42310.0

    def test_provider_passthrough(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.provider == "ESO"

    def test_data_product_id_passthrough(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.data_product_id == "test-dpid-0001"

    def test_header_signature_hash_present(self, result: ProfileResult) -> None:
        assert result.header_signature_hash is not None
        assert len(result.header_signature_hash) == 16  # truncated sha256

    def test_raw_header_populated(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert "INSTRUME" in result.spectrum.raw_header

    def test_no_quarantine_reason(self, result: ProfileResult) -> None:
        assert result.quarantine_reason is None
        assert result.quarantine_reason_code is None

    def test_descending_wave_succeeds(self, profile: EsoUvesProfile) -> None:
        """Descending spectral axis is valid — profile notes it but does not quarantine."""
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=_make_wave(descending=True)))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any("descending" in note.lower() for note in result.normalization_notes)

    def test_date_obs_fallback_when_no_mjd(self, profile: EsoUvesProfile) -> None:
        """When MJD-OBS is absent, DATE-OBS is used and a note is added."""
        header = _make_primary_header(**{"MJD-OBS": None})
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.observation_time.startswith("2012-06-04T00:00:00Z")
        assert any(
            "date-only" in note.lower() or "day-level" in note.lower()
            for note in result.normalization_notes
        )


# ---------------------------------------------------------------------------
# TestEsoUvesProfileMissingColumns
# ---------------------------------------------------------------------------


class TestEsoUvesProfileMissingColumns:
    def test_missing_wave_column_quarantines(self, profile: EsoUvesProfile) -> None:
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(include_wave=False))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"
        assert result.quarantine_reason is not None
        assert "wave" in result.quarantine_reason.lower()

    def test_missing_flux_column_quarantines(self, profile: EsoUvesProfile) -> None:
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(include_flux=False))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_no_spectrum_hdu_quarantines(self, profile: EsoUvesProfile) -> None:
        """HDUList with only a primary HDU and no BinTable → quarantine."""
        hdulist = fits.HDUList([fits.PrimaryHDU(header=_make_primary_header())])
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_wave_alias_accepted(self, profile: EsoUvesProfile) -> None:
        """WAVELENGTH column name is accepted as alias for WAVE."""
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave_col_name="WAVELENGTH"))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any(
            "alias" in note.lower() or "wavelength" in note.lower()
            for note in result.normalization_notes
        )


# ---------------------------------------------------------------------------
# TestEsoUvesProfileMissingMetadata
# ---------------------------------------------------------------------------


class TestEsoUvesProfileMissingMetadata:
    def test_missing_mjd_and_date_obs_quarantines(self, profile: EsoUvesProfile) -> None:
        header = _make_primary_header(**{"MJD-OBS": None, "DATE-OBS": None})
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"
        assert result.quarantine_reason is not None
        assert "observation_time" in result.quarantine_reason.lower()

    def test_missing_ra_dec_still_succeeds(self, profile: EsoUvesProfile) -> None:
        """RA and DEC are optional — their absence does not quarantine."""
        header = _make_primary_header(RA=None, DEC=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.target_ra_deg is None
        assert result.spectrum.target_dec_deg is None

    def test_missing_exptime_still_succeeds(self, profile: EsoUvesProfile) -> None:
        header = _make_primary_header(EXPTIME=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.exposure_time_s is None

    def test_missing_spec_res_still_succeeds(self, profile: EsoUvesProfile) -> None:
        header = _make_primary_header(SPEC_RES=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.spectral_resolution is None


# ---------------------------------------------------------------------------
# TestEsoUvesProfileSanityChecks
# ---------------------------------------------------------------------------


class TestEsoUvesProfileSanityChecks:
    def test_all_nan_flux_quarantines(self, profile: EsoUvesProfile) -> None:
        flux = np.full(_N, np.nan)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        # All-NaN hits the >20% NaN fraction check
        assert result.quarantine_reason_code is not None

    def test_high_nan_fraction_quarantines(self, profile: EsoUvesProfile) -> None:
        """25% NaN/Inf is above the 20% threshold → quarantine."""
        flux = _make_flux(nan_fraction=0.25)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "non-finite" in result.quarantine_reason.lower()

    def test_acceptable_nan_fraction_succeeds(self, profile: EsoUvesProfile) -> None:
        """10% NaN is below the threshold — note added but not quarantined."""
        flux = _make_flux(nan_fraction=0.10)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any(
            "nan" in note.lower() or "non-finite" in note.lower()
            for note in result.normalization_notes
        )

    def test_non_monotonic_wave_quarantines(self, profile: EsoUvesProfile) -> None:
        wave = _make_wave()
        # Introduce a non-monotonic step in the middle
        wave[_N // 2] = wave[_N // 2 - 5]
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "monoton" in result.quarantine_reason.lower()

    def test_all_zero_flux_quarantines(self, profile: EsoUvesProfile) -> None:
        flux = np.zeros(_N)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "zero" in result.quarantine_reason.lower()

    def test_wave_out_of_range_quarantines(self, profile: EsoUvesProfile) -> None:
        """Wavelength axis in metres (not Angstrom) looks out-of-range → quarantine."""
        wave = np.linspace(5.656e-7, 9.464e-7, _N)  # metres, not Angstrom
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "range" in result.quarantine_reason.lower()

    def test_empty_flux_quarantines(self, profile: EsoUvesProfile) -> None:
        wave = np.array([])
        flux = np.array([])
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave, flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False


# ---------------------------------------------------------------------------
# TestValidateSpectrumEntryPoint
# ---------------------------------------------------------------------------


class TestValidateSpectrumEntryPoint:
    def test_dispatches_to_uves_profile(self) -> None:
        hdulist = _make_hdulist()
        result = validate_spectrum(
            hdulist,
            provider="ESO",
            data_product_id="test-dpid-9999",
            hints={},
        )
        assert result.success is True
        assert result.profile_id == "ESO_UVES"

    def test_unknown_provider_returns_unknown_profile(self) -> None:
        hdulist = _make_hdulist()
        result = validate_spectrum(
            hdulist,
            provider="MAST",
            data_product_id="test-dpid-9999",
            hints={},
        )
        assert result.success is False
        assert result.quarantine_reason_code == "UNKNOWN_PROFILE"
        assert result.quarantine_reason is not None
        assert "MAST" in result.quarantine_reason

    def test_non_eso_provider_returns_unknown_profile(self) -> None:
        """Non-ESO provider has no registered profile → UNKNOWN_PROFILE.
        Note: ESO + any instrument is now covered by EsoFallbackProfile.
        """
        hdulist = _make_hdulist()
        result = validate_spectrum(
            hdulist,
            provider="MAST",
            data_product_id="test-dpid-9999",
            hints={},
        )
        assert result.success is False
        assert result.quarantine_reason_code == "UNKNOWN_PROFILE"

    def test_data_product_id_propagated(self) -> None:
        hdulist = _make_hdulist()
        dpid = "propagation-test-uuid"
        result = validate_spectrum(
            hdulist,
            provider="ESO",
            data_product_id=dpid,
            hints={},
        )
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.data_product_id == dpid


# ---------------------------------------------------------------------------
# TestEsoUvesProfileSnrExtraction
# ---------------------------------------------------------------------------


class TestEsoUvesProfileSnrExtraction:
    def test_snr_extracted_when_present(self, profile: EsoUvesProfile) -> None:
        """SNR column in BinTable → NormalizedSpectrum.snr is the median."""
        wave = _make_wave()
        flux = _make_flux()
        n = len(wave)
        snr_data = np.linspace(10.0, 50.0, n)
        expected_median = float(np.median(snr_data))

        cols = [
            fits.Column(name="WAVE", format=f"{n}D", unit="angstrom", array=wave.reshape(1, n)),
            fits.Column(
                name="FLUX",
                format=f"{n}D",
                unit="10**(-16)erg.cm**(-2).s**(-1).angstrom**(-1)",
                array=flux.reshape(1, n),
            ),
            fits.Column(name="ERR", format=f"{n}D", unit="", array=(flux * 0.05).reshape(1, n)),
            fits.Column(name="SNR", format=f"{n}D", unit="", array=snr_data.reshape(1, n)),
        ]
        hdu = fits.BinTableHDU.from_columns(cols)
        hdu.name = "SPECTRUM"
        hdulist = _make_hdulist(spectrum_hdu=hdu)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is not None
        assert abs(result.spectrum.snr - expected_median) < 0.01

    def test_snr_none_when_absent(self, profile: EsoUvesProfile) -> None:
        """No SNR column → NormalizedSpectrum.snr is None."""
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is None

    def test_snr_extracted_from_snr_reduced_column(self, profile: EsoUvesProfile) -> None:
        """BinTable has SNR_REDUCED but no SNR column → median of SNR_REDUCED."""
        wave = _make_wave()
        flux = _make_flux()
        n = len(wave)
        snr_data = np.linspace(10.0, 50.0, n)
        expected_median = float(np.median(snr_data))

        cols = [
            fits.Column(name="WAVE", format=f"{n}D", unit="angstrom", array=wave.reshape(1, n)),
            fits.Column(
                name="FLUX",
                format=f"{n}D",
                unit="10**(-16)erg.cm**(-2).s**(-1).angstrom**(-1)",
                array=flux.reshape(1, n),
            ),
            fits.Column(name="ERR", format=f"{n}D", unit="", array=(flux * 0.05).reshape(1, n)),
            fits.Column(name="SNR_REDUCED", format=f"{n}D", unit="", array=snr_data.reshape(1, n)),
        ]
        hdu = fits.BinTableHDU.from_columns(cols)
        hdu.name = "SPECTRUM"
        hdulist = _make_hdulist(spectrum_hdu=hdu)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is not None
        assert abs(result.spectrum.snr - expected_median) < 0.01

    def test_snr_header_fallback_when_no_column(self, profile: EsoUvesProfile) -> None:
        """No SNR column in BinTable, but HDU[0] header has SNR keyword."""
        header = _make_primary_header(SNR=42.5)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is not None
        assert abs(result.spectrum.snr - 42.5) < 0.01

    def test_snr_header_fallback_adds_normalization_note(self, profile: EsoUvesProfile) -> None:
        """Header SNR fallback must add a normalization note."""
        header = _make_primary_header(SNR=42.5)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any("HDU[0] header keyword" in note for note in result.normalization_notes)

    def test_snr_column_preferred_over_header(self, profile: EsoUvesProfile) -> None:
        """BinTable SNR column takes priority over HDU[0] header SNR keyword."""
        wave = _make_wave()
        flux = _make_flux()
        n = len(wave)
        snr_data = np.linspace(10.0, 50.0, n)
        expected_median = float(np.median(snr_data))

        cols = [
            fits.Column(name="WAVE", format=f"{n}D", unit="angstrom", array=wave.reshape(1, n)),
            fits.Column(
                name="FLUX",
                format=f"{n}D",
                unit="10**(-16)erg.cm**(-2).s**(-1).angstrom**(-1)",
                array=flux.reshape(1, n),
            ),
            fits.Column(name="ERR", format=f"{n}D", unit="", array=(flux * 0.05).reshape(1, n)),
            fits.Column(name="SNR", format=f"{n}D", unit="", array=snr_data.reshape(1, n)),
        ]
        hdu = fits.BinTableHDU.from_columns(cols)
        hdu.name = "SPECTRUM"
        header = _make_primary_header(SNR=999.0)
        hdulist = _make_hdulist(primary_header=header, spectrum_hdu=hdu)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is not None
        assert abs(result.spectrum.snr - expected_median) < 0.01
        assert result.spectrum.snr != 999.0

    def test_snr_none_when_no_column_and_no_header(self, profile: EsoUvesProfile) -> None:
        """No SNR column and no SNR header keyword → snr is None."""
        header = _make_primary_header()
        assert "SNR" not in header
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is None
