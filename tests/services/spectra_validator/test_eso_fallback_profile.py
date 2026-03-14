"""
tests/spectra_validator/test_eso_fallback_profile.py

Unit tests for EsoFallbackProfile and its integration with validate_spectrum.

All tests use synthetic astropy HDULists — no binary test fixtures on disk.
FEROS is used as the primary reference instrument (unit = 'angstrom', cols:
WAVE / FLUX / ERR), confirmed from a real archive product. FORS2-SPEC is
used as a secondary case.

Key behaviours under test beyond the standard profile contract:
  - Dynamic profile_id: result.profile_id = f"ESO_{INSTRUME}"
  - Wavelength unit detection from TUNIT (angstrom and nm)
  - Heuristic unit fallback when TUNIT is absent or unrecognised
  - SPECRP accepted as alias for SPEC_RES (FEROS uses SPECRP)
  - No FLUXCAL requirement (FEROS flux is in ADU, not absolute units)
  - Registry ordering: UVES and XSHOOTER must not be absorbed by fallback

Test groups:
  TestEsoFallbackProfileMatches       — matches() accept/reject logic
  TestEsoFallbackProfileHappyPathFeros   — full validation; FEROS-shaped input
  TestEsoFallbackProfileHappyPathFors2   — FORS2-SPEC variant
  TestEsoFallbackProfileDynamicProfileId — ESO_{INSTRUME} construction
  TestEsoFallbackProfileUnitDetection    — TUNIT reading and heuristic fallback
  TestEsoFallbackProfileMissingColumns   — WAVE/FLUX absence → quarantine
  TestEsoFallbackProfileMissingMetadata  — required header fields → quarantine
  TestEsoFallbackProfileSanityChecks     — five mandatory sanity checks
  TestValidateSpectrumDispatchesFallback — registry routing
"""

from __future__ import annotations

import os
import pathlib

import numpy as np
import pytest


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
from spectra_validator.profiles.eso_fallback import EsoFallbackProfile  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_N = 200

# Representative FEROS observation MJD
_MJD_OBS = 55000.5  # → 2009-09-17T12:00:00Z (approx)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_primary_header(**overrides: object) -> fits.Header:
    """
    Build a minimal ESO primary header representative of FEROS products.
    Use value=None to delete a keyword (simulate missing field).
    """
    defaults: dict[str, object] = {
        "SIMPLE": True,
        "BITPIX": 16,
        "NAXIS": 0,
        "INSTRUME": "FEROS",
        "TELESCOP": "ESO-2.2",
        "MJD-OBS": _MJD_OBS,
        "DATE-OBS": "2009-09-17T12:00:00.0",
        "RA": 100.2568,
        "DEC": -60.8532,
        "EXPTIME": 1800.0,
        "SPECRP": 48000.0,  # FEROS uses SPECRP, not SPEC_RES
        "ORIGIN": "ESO",
    }
    defaults.update(overrides)
    header = fits.Header()
    for key, val in defaults.items():
        if val is None:
            continue
        header[key] = val
    return header


def _make_wave_angstrom(n: int = _N, descending: bool = False) -> np.ndarray:
    wave = np.linspace(3530.0, 9200.0, n)  # FEROS optical range in Å
    return wave[::-1] if descending else wave


def _make_wave_nm(n: int = _N) -> np.ndarray:
    return np.linspace(353.0, 920.0, n)  # same range in nm


def _make_flux(n: int = _N, nan_fraction: float = 0.0) -> np.ndarray:
    rng = np.random.default_rng(0)
    flux = rng.uniform(100.0, 5000.0, n)  # ADU — not flux-calibrated like UVES
    if nan_fraction > 0:
        flux[: int(n * nan_fraction)] = np.nan
    return flux


def _make_spectrum_hdu(
    wave: np.ndarray | None = None,
    flux: np.ndarray | None = None,
    wave_unit: str = "angstrom",
    wave_col_name: str = "WAVE",
    flux_col_name: str = "FLUX",
    include_wave: bool = True,
    include_flux: bool = True,
    hdu_name: str = "SPECTRUM",
) -> fits.BinTableHDU:
    """
    Build a BinTableHDU matching the confirmed FEROS shape:
    1 row, fixed-length array columns, WAVE unit = 'angstrom'.
    """
    if wave is None:
        wave = _make_wave_angstrom()
    if flux is None:
        flux = _make_flux()

    n = len(wave)
    cols = []

    if include_wave:
        cols.append(
            fits.Column(
                name=wave_col_name,
                format=f"{n}D",
                unit=wave_unit,
                array=wave.reshape(1, n),
            )
        )

    if include_flux:
        cols.append(
            fits.Column(
                name=flux_col_name,
                format=f"{n}E",
                unit="adu",
                array=flux.reshape(1, n),
            )
        )
        cols.append(
            fits.Column(
                name="ERR",
                format=f"{n}E",
                unit="adu",
                array=(flux * 0.02).reshape(1, n),
            )
        )

    hdu = fits.BinTableHDU.from_columns(cols)
    hdu.name = hdu_name
    return hdu


def _make_hdulist(
    primary_header: fits.Header | None = None,
    spectrum_hdu: fits.BinTableHDU | None = None,
) -> fits.HDUList:
    if primary_header is None:
        primary_header = _make_primary_header()
    if spectrum_hdu is None:
        spectrum_hdu = _make_spectrum_hdu()
    return fits.HDUList([fits.PrimaryHDU(header=primary_header), spectrum_hdu])


def _product_metadata(
    data_product_id: str = "test-fallback-dpid-0001",
    provider: str = "ESO",
) -> dict:
    return {"data_product_id": data_product_id, "provider": provider, "hints": {}}


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def profile() -> EsoFallbackProfile:
    return EsoFallbackProfile()


# ---------------------------------------------------------------------------
# TestEsoFallbackProfileMatches
# ---------------------------------------------------------------------------


class TestEsoFallbackProfileMatches:
    def test_matches_eso_feros(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("ESO", hdulist) is True

    def test_matches_eso_fors2(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(instrume="FORS2")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is True

    def test_matches_any_eso_instrument(self, profile: EsoFallbackProfile) -> None:
        """Any ESO instrument not claimed upstream is accepted."""
        for instrume in ("HARPS", "ESPRESSO", "FLAMES", "CRIRES", "UNKNOWN_FUTURE"):
            header = _make_primary_header(instrume=instrume)
            hdulist = _make_hdulist(primary_header=header)
            assert profile.matches("ESO", hdulist) is True, f"Failed for {instrume}"

    def test_no_match_non_eso_provider(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("MAST", hdulist) is False

    def test_no_match_noirlab(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("NOIRLab", hdulist) is False


# ---------------------------------------------------------------------------
# TestEsoFallbackProfileHappyPathFeros
# ---------------------------------------------------------------------------


class TestEsoFallbackProfileHappyPathFeros:
    """FEROS-shaped input: angstrom WAVE, ADU flux, SPECRP resolving power."""

    @pytest.fixture
    def result(self, profile: EsoFallbackProfile) -> ProfileResult:
        return profile.validate(_make_hdulist(), _product_metadata())

    def test_success(self, result: ProfileResult) -> None:
        assert result.success is True

    def test_spectrum_populated(self, result: ProfileResult) -> None:
        assert result.spectrum is not None

    def test_profile_id_is_eso_feros(self, result: ProfileResult) -> None:
        assert result.profile_id == "ESO_FEROS"

    def test_spectral_axis_shape(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.spectral_axis.ndim == 1
        assert len(result.spectrum.spectral_axis) == _N

    def test_flux_axis_shape(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.flux_axis.ndim == 1
        assert len(result.spectrum.flux_axis) == _N

    def test_spectral_units_angstrom(self, result: ProfileResult) -> None:
        """FEROS WAVE unit is angstrom — must be detected from TUNIT."""
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "angstrom"

    def test_flux_units_adu(self, result: ProfileResult) -> None:
        """FEROS is not flux-calibrated — flux unit is ADU."""
        assert result.spectrum is not None
        assert result.spectrum.flux_units == "adu"

    def test_observation_time_from_mjd(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.observation_time.endswith("Z")
        assert result.spectrum.observation_mjd is not None
        assert abs(result.spectrum.observation_mjd - _MJD_OBS) < 1e-6

    def test_ra_dec(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.target_ra_deg is not None
        assert abs(result.spectrum.target_ra_deg - 100.2568) < 1e-4
        assert result.spectrum.target_dec_deg is not None
        assert abs(result.spectrum.target_dec_deg - (-60.8532)) < 1e-4

    def test_instrument(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.instrument == "FEROS"

    def test_telescope(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.telescope == "ESO-2.2"

    def test_exposure_time(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.exposure_time_s is not None
        assert abs(result.spectrum.exposure_time_s - 1800.0) < 0.001

    def test_spectral_resolution_from_specrp(self, result: ProfileResult) -> None:
        """FEROS uses SPECRP, not SPEC_RES — must be read as alias."""
        assert result.spectrum is not None
        assert result.spectrum.spectral_resolution == 48000.0

    def test_provider_passthrough(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.provider == "ESO"

    def test_data_product_id_passthrough(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.data_product_id == "test-fallback-dpid-0001"

    def test_header_signature_hash(self, result: ProfileResult) -> None:
        assert result.header_signature_hash is not None
        assert len(result.header_signature_hash) == 16

    def test_no_quarantine_reason(self, result: ProfileResult) -> None:
        assert result.quarantine_reason is None
        assert result.quarantine_reason_code is None

    def test_no_fluxcal_required(self, profile: EsoFallbackProfile) -> None:
        """FLUXCAL absent must not quarantine — FEROS has no absolute calibration."""
        header = _make_primary_header()
        # Confirm FLUXCAL is not in the header (it isn't in defaults)
        assert "FLUXCAL" not in header
        result = profile.validate(_make_hdulist(primary_header=header), _product_metadata())
        assert result.success is True

    def test_descending_wave_succeeds(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_angstrom(descending=True))
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any("descending" in note.lower() for note in result.normalization_notes)

    def test_hash_is_stable(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist()
        r1 = profile.validate(hdulist, _product_metadata())
        r2 = profile.validate(hdulist, _product_metadata())
        assert r1.header_signature_hash == r2.header_signature_hash


# ---------------------------------------------------------------------------
# TestEsoFallbackProfileHappyPathFors2
# ---------------------------------------------------------------------------


class TestEsoFallbackProfileHappyPathFors2:
    """FORS2-SPEC variant — same angstrom layout, different instrument."""

    def test_fors2_validates_successfully(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(instrume="FORS2")
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True

    def test_fors2_profile_id(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(instrume="FORS2")
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.profile_id == "ESO_FORS2"

    def test_fors2_instrument_field(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(instrume="FORS2")
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.instrument == "FORS2"


# ---------------------------------------------------------------------------
# TestEsoFallbackProfileDynamicProfileId
# ---------------------------------------------------------------------------


class TestEsoFallbackProfileDynamicProfileId:
    @pytest.mark.parametrize(
        "instrume,expected_id",
        [
            ("FEROS", "ESO_FEROS"),
            ("FORS2", "ESO_FORS2"),
            ("HARPS", "ESO_HARPS"),
            ("ESPRESSO", "ESO_ESPRESSO"),
            ("FLAMES", "ESO_FLAMES"),
        ],
    )
    def test_profile_id_from_instrume(
        self, profile: EsoFallbackProfile, instrume: str, expected_id: str
    ) -> None:
        header = _make_primary_header(instrume=instrume)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.profile_id == expected_id

    def test_missing_instrume_falls_back_to_eso_fallback(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(INSTRUME=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        # Even on failure (no INSTRUME for matches, etc.), profile_id is set
        assert result.profile_id == "ESO_FALLBACK"

    def test_class_profile_id_is_eso_fallback(self, profile: EsoFallbackProfile) -> None:
        """The class-level property is always ESO_FALLBACK regardless of instrument."""
        assert profile.profile_id == "ESO_FALLBACK"

    def test_instrume_with_hyphen_normalised(self, profile: EsoFallbackProfile) -> None:
        """INSTRUME values like 'FORS2-SPEC' should produce 'ESO_FORS2SPEC'."""
        header = _make_primary_header(instrume="FORS2-SPEC")
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.profile_id == "ESO_FORS2SPEC"


# ---------------------------------------------------------------------------
# TestEsoFallbackProfileUnitDetection
# ---------------------------------------------------------------------------


class TestEsoFallbackProfileUnitDetection:
    def test_tunit_angstrom_detected(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_angstrom(), wave_unit="angstrom")
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "angstrom"

    def test_tunit_nm_detected(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_nm(), wave_unit="nm")
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "nm"

    def test_tunit_aa_alias_for_angstrom(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_angstrom(), wave_unit="AA")
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "angstrom"

    def test_no_tunit_heuristic_angstrom(self, profile: EsoFallbackProfile) -> None:
        """Wave values > 100 with no TUNIT → heuristic assumes angstrom."""
        hdulist = _make_hdulist(
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_angstrom(), wave_unit="")
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "angstrom"
        assert any("heuristic" in note.lower() for note in result.normalization_notes)

    def test_no_tunit_heuristic_nm(self, profile: EsoFallbackProfile) -> None:
        """Wave values ≤ 100 with no TUNIT → heuristic assumes nm."""
        # Use a small nm range to trigger the ≤ 100 branch
        wave_small = np.linspace(0.5, 2.5, _N)  # micron-scale values
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave_small, wave_unit=""))
        result = profile.validate(hdulist, _product_metadata())
        # It'll either succeed (nm assumed, heuristic) or fail range check (um scale)
        # Either way, the heuristic note should be present
        assert any("heuristic" in note.lower() for note in result.normalization_notes)

    def test_unrecognised_tunit_falls_back_to_heuristic(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_angstrom(), wave_unit="furlongs")
        )
        result = profile.validate(hdulist, _product_metadata())
        assert any("unrecognised" in note.lower() for note in result.normalization_notes)


# ---------------------------------------------------------------------------
# TestEsoFallbackProfileMissingColumns
# ---------------------------------------------------------------------------


class TestEsoFallbackProfileMissingColumns:
    def test_missing_wave_quarantines(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(include_wave=False))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"
        assert result.quarantine_reason is not None
        assert "wave" in result.quarantine_reason.lower()

    def test_missing_flux_quarantines(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(include_flux=False))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_no_spectrum_hdu_quarantines(self, profile: EsoFallbackProfile) -> None:
        hdulist = fits.HDUList([fits.PrimaryHDU(header=_make_primary_header())])
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_wave_alias_wavelength_accepted(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave_col_name="WAVELENGTH"))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True

    def test_err_column_optional(self, profile: EsoFallbackProfile) -> None:
        """ERR is optional — only WAVE + FLUX required."""
        n = _N
        wave = _make_wave_angstrom()
        flux = _make_flux()
        cols = fits.ColDefs(
            [
                fits.Column(name="WAVE", format=f"{n}D", unit="angstrom", array=wave.reshape(1, n)),
                fits.Column(name="FLUX", format=f"{n}E", unit="adu", array=flux.reshape(1, n)),
                # No ERR column
            ]
        )
        hdu = fits.BinTableHDU.from_columns(cols)
        hdu.name = "SPECTRUM"
        hdulist = fits.HDUList([fits.PrimaryHDU(header=_make_primary_header()), hdu])
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True


# ---------------------------------------------------------------------------
# TestEsoFallbackProfileMissingMetadata
# ---------------------------------------------------------------------------


class TestEsoFallbackProfileMissingMetadata:
    def test_missing_mjd_and_date_obs_quarantines(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(**{"MJD-OBS": None, "DATE-OBS": None})
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"
        assert result.quarantine_reason is not None
        assert "observation_time" in result.quarantine_reason.lower()

    def test_date_obs_fallback_when_no_mjd(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(**{"MJD-OBS": None})
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert "2009-09-17T" in result.spectrum.observation_time

    def test_date_obs_dateonly_gets_midnight(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(**{"MJD-OBS": None, "DATE-OBS": "2009-09-17"})
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.observation_time == "2009-09-17T00:00:00Z"
        assert any(
            "day-level" in note.lower() or "date-only" in note.lower()
            for note in result.normalization_notes
        )

    def test_missing_ra_dec_succeeds(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(RA=None, DEC=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.target_ra_deg is None
        assert result.spectrum.target_dec_deg is None

    def test_missing_exptime_succeeds(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(EXPTIME=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.exposure_time_s is None

    def test_missing_specrp_and_spec_res_succeeds(self, profile: EsoFallbackProfile) -> None:
        header = _make_primary_header(SPECRP=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.spectral_resolution is None

    def test_spec_res_accepted_as_alias(self, profile: EsoFallbackProfile) -> None:
        """SPEC_RES (used by UVES/XSHOOTER) is also accepted."""
        header = _make_primary_header(SPECRP=None)
        header["SPEC_RES"] = 45000.0
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.spectral_resolution == 45000.0


# ---------------------------------------------------------------------------
# TestEsoFallbackProfileSanityChecks
# ---------------------------------------------------------------------------


class TestEsoFallbackProfileSanityChecks:
    def test_all_nan_flux_quarantines(self, profile: EsoFallbackProfile) -> None:
        flux = np.full(_N, np.nan)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False

    def test_high_nan_fraction_quarantines(self, profile: EsoFallbackProfile) -> None:
        flux = _make_flux(nan_fraction=0.25)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "non-finite" in result.quarantine_reason.lower()

    def test_acceptable_nan_fraction_succeeds(self, profile: EsoFallbackProfile) -> None:
        flux = _make_flux(nan_fraction=0.10)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any(
            "nan" in note.lower() or "non-finite" in note.lower()
            for note in result.normalization_notes
        )

    def test_non_monotonic_wave_quarantines(self, profile: EsoFallbackProfile) -> None:
        wave = _make_wave_angstrom()
        wave[_N // 2] = wave[_N // 2 - 5]
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "monoton" in result.quarantine_reason.lower()

    def test_all_zero_flux_quarantines(self, profile: EsoFallbackProfile) -> None:
        flux = np.zeros(_N)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "zero" in result.quarantine_reason.lower()

    def test_empty_arrays_quarantine(self, profile: EsoFallbackProfile) -> None:
        hdulist = _make_hdulist(
            spectrum_hdu=_make_spectrum_hdu(wave=np.array([]), flux=np.array([]))
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False

    def test_wave_in_metres_out_of_range(self, profile: EsoFallbackProfile) -> None:
        """Values in metres look like ~5e-7 angstrom — far below any plausible bound."""
        wave_metres = np.linspace(3.53e-7, 9.2e-7, _N)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave_metres))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "range" in result.quarantine_reason.lower()


# ---------------------------------------------------------------------------
# TestValidateSpectrumDispatchesFallback
# ---------------------------------------------------------------------------


class TestValidateSpectrumDispatchesFallback:
    def test_feros_dispatches_to_fallback(self) -> None:
        hdulist = _make_hdulist()
        result = validate_spectrum(
            hdulist,
            provider="ESO",
            data_product_id="feros-dispatch-001",
            hints={},
        )
        assert result.success is True
        assert result.profile_id == "ESO_FEROS"

    def test_fors2_dispatches_to_fallback(self) -> None:
        header = _make_primary_header(instrume="FORS2")
        hdulist = _make_hdulist(primary_header=header)
        result = validate_spectrum(
            hdulist,
            provider="ESO",
            data_product_id="fors2-dispatch-001",
            hints={},
        )
        assert result.success is True
        assert result.profile_id == "ESO_FORS2"

    def test_uves_not_absorbed_by_fallback(self) -> None:
        """UVES must still be routed to EsoUvesProfile, not the fallback."""
        header = fits.Header()
        header["INSTRUME"] = "UVES    "
        header["TELESCOP"] = "ESO-VLT-U2"
        header["MJD-OBS"] = 56082.05
        header["DATE-OBS"] = "2012-06-04"
        header["EXPTIME"] = 7200.0

        n = _N
        wave = np.linspace(3200.0, 10000.0, n)
        flux = np.ones(n) * 1.5e-16
        cols = fits.ColDefs(
            [
                fits.Column(name="WAVE", format=f"{n}D", unit="angstrom", array=wave.reshape(1, n)),
                fits.Column(
                    name="FLUX", format=f"{n}D", unit="erg/s/cm2/AA", array=flux.reshape(1, n)
                ),
            ]
        )
        hdu = fits.BinTableHDU.from_columns(cols)
        hdu.name = "SPECTRUM"
        hdulist = fits.HDUList([fits.PrimaryHDU(header=header), hdu])

        result = validate_spectrum(
            hdulist, provider="ESO", data_product_id="uves-not-fallback", hints={}
        )
        assert result.profile_id == "ESO_UVES"

    def test_xshooter_not_absorbed_by_fallback(self) -> None:
        """XSHOOTER must still be routed to EsoXShooterProfile, not the fallback."""
        header = fits.Header()
        header["INSTRUME"] = "XSHOOTER"
        header["TELESCOP"] = "ESO-VLT-U2"
        header["MJD-OBS"] = 56368.97
        header["DATE-OBS"] = "2013-03-17T23:18:01.234"
        header["EXPTIME"] = 1800.0
        header["DISPELEM"] = "NIR"

        n = _N
        wave = np.linspace(994.0, 2480.0, n)
        flux = np.ones(n) * 1.0e-16
        cols = fits.ColDefs(
            [
                fits.Column(name="WAVE", format=f"{n}D", unit="nm", array=wave.reshape(1, n)),
                fits.Column(
                    name="FLUX", format=f"{n}D", unit="erg/s/cm2/AA", array=flux.reshape(1, n)
                ),
            ]
        )
        hdu = fits.BinTableHDU.from_columns(cols)
        hdu.name = "SPECTRUM"
        hdulist = fits.HDUList([fits.PrimaryHDU(header=header), hdu])

        result = validate_spectrum(
            hdulist, provider="ESO", data_product_id="xshooter-not-fallback", hints={}
        )
        assert result.profile_id == "ESO_XSHOOTER"

    def test_non_eso_provider_unknown_profile(self) -> None:
        hdulist = _make_hdulist()
        result = validate_spectrum(
            hdulist,
            provider="MAST",
            data_product_id="mast-no-profile",
            hints={},
        )
        assert result.success is False
        assert result.quarantine_reason_code == "UNKNOWN_PROFILE"

    def test_data_product_id_propagated(self) -> None:
        hdulist = _make_hdulist()
        dpid = "fallback-propagation-uuid"
        result = validate_spectrum(hdulist, provider="ESO", data_product_id=dpid, hints={})
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.data_product_id == dpid
