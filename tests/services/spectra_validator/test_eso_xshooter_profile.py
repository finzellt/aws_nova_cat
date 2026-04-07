"""
tests/spectra_validator/test_eso_xshooter_profile.py

Unit tests for EsoXShooterProfile and its integration with the validate_spectrum
entry point.

All tests use synthetic astropy HDULists — no binary test fixtures on disk.
The _make_hdulist() helper produces a minimal but structurally valid X-Shooter
file matching the confirmed real-file shape (1-row BinTable, fixed-length array
columns, WAVE in nm).

Key differences from UVES that drive distinct test cases:
  - WAVE column unit is nm (not angstrom) — wavelength range bounds differ
  - DATE-OBS is full ISO-8601 (not date-only) — no fallback appending needed
  - DISPELEM header field identifies the arm (UVB / VIS / NIR)
  - INSTRUME = 'XSHOOTER' (no hyphen; padded to 8 chars by ESO pipeline)
  - Three arms cover different wavelength ranges:
      UVB: ~300–550 nm
      VIS: ~550–1020 nm
      NIR: ~994–2480 nm

Test groups:
  TestEsoXShooterProfileMatches      — matches() accept/reject logic
  TestEsoXShooterProfileHappyPath    — successful validation; output shape and values
  TestEsoXShooterProfileAllThreeArms — UVB, VIS, NIR each produce a valid result
  TestEsoXShooterProfileMissingColumns    — WAVE/FLUX column absence → quarantine
  TestEsoXShooterProfileMissingMetadata   — required header fields absent → quarantine
  TestEsoXShooterProfileSanityChecks      — the five mandatory sanity checks
  TestEsoXShooterProfileHeaderSignature   — DISPELEM is included in the hash
  TestValidateSpectrumDispatchesXShooter  — registry routes ESO+XSHOOTER correctly
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
from spectra_validator.profiles.eso_xshooter import EsoXShooterProfile  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_N = 200  # synthetic spectrum length

# Representative MJD from a real T-Pyx X-Shooter observation (NIR arm)
_MJD_OBS = 56368.97085495  # → 2013-03-17T23:18:01Z


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_primary_header(**overrides: object) -> fits.Header:
    """
    Build a minimal X-Shooter primary header with all required fields present.
    Use a value of None to delete a key (simulates missing header keyword).
    """
    defaults: dict[str, object] = {
        "SIMPLE": True,
        "BITPIX": 16,
        "NAXIS": 0,
        "INSTRUME": "XSHOOTER",  # confirmed no hyphen in real archive products
        "TELESCOP": "ESO-VLT-U2",
        "MJD-OBS": _MJD_OBS,
        "DATE-OBS": "2013-03-17T23:18:01.234",  # full ISO-8601 — unlike UVES
        "RA": 229.6713,
        "DEC": -32.4770,
        "EXPTIME": 1800.0,
        "SPEC_RES": 11000.0,
        "DISPELEM": "NIR",
        "FLUXCAL": "ABSOLUTE",
        "ORIGIN": "ESO",
    }
    defaults.update(overrides)

    header = fits.Header()
    for key, val in defaults.items():
        if val is None:
            continue
        header[key] = val
    return header


def _make_wave_nm(n: int = _N, arm: str = "NIR", descending: bool = False) -> np.ndarray:
    """Strictly monotonic wavelength array in nm for the given arm."""
    ranges = {
        "UVB": (300.0, 550.0),
        "VIS": (550.0, 1020.0),
        "NIR": (994.0, 2480.0),
    }
    lo, hi = ranges.get(arm, (994.0, 2480.0))
    wave = np.linspace(lo, hi, n)
    return wave[::-1] if descending else wave


def _make_flux(n: int = _N, nan_fraction: float = 0.0) -> np.ndarray:
    rng = np.random.default_rng(42)
    flux = rng.uniform(0.5e-16, 5.0e-16, n)
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
    Build a BinTableHDU matching the real X-Shooter shape:
      1 row, fixed-length array columns (shape 1 × N), WAVE in nm.
    """
    if wave is None:
        wave = _make_wave_nm()
    if flux is None:
        flux = _make_flux()

    n = len(wave)
    cols = []

    if include_wave:
        cols.append(
            fits.Column(
                name=wave_col_name,
                format=f"{n}D",
                unit="nm",
                array=wave.reshape(1, n),
            )
        )

    if include_flux:
        flux_unit = "erg cm**(-2) s**(-1) angstrom**(-1)"
        cols.append(
            fits.Column(
                name=flux_col_name,
                format=f"{n}D",
                unit=flux_unit,
                array=flux.reshape(1, n),
            )
        )
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
    if primary_header is None:
        primary_header = _make_primary_header()
    if spectrum_hdu is None:
        spectrum_hdu = _make_spectrum_hdu()
    return fits.HDUList([fits.PrimaryHDU(header=primary_header), spectrum_hdu])


def _product_metadata(
    data_product_id: str = "test-xshooter-dpid-0001",
    provider: str = "ESO",
) -> dict:
    return {"data_product_id": data_product_id, "provider": provider, "hints": {}}


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def profile() -> EsoXShooterProfile:
    return EsoXShooterProfile()


# ---------------------------------------------------------------------------
# TestEsoXShooterProfileMatches
# ---------------------------------------------------------------------------


class TestEsoXShooterProfileMatches:
    def test_matches_eso_xshooter(self, profile: EsoXShooterProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("ESO", hdulist) is True

    def test_matches_xshooter_with_padding(self, profile: EsoXShooterProfile) -> None:
        """INSTRUME='XSHOOTER' padded to 8 chars must still match."""
        header = _make_primary_header(INSTRUME="XSHOOTER")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is True

    def test_matches_xshooter_lowercase(self, profile: EsoXShooterProfile) -> None:
        """INSTRUME comparison is case-insensitive."""
        header = _make_primary_header(INSTRUME="xshooter")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is True

    def test_no_match_wrong_provider(self, profile: EsoXShooterProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("MAST", hdulist) is False

    def test_no_match_uves(self, profile: EsoXShooterProfile) -> None:
        """UVES files must not be claimed by the X-Shooter profile."""
        header = _make_primary_header(INSTRUME="UVES    ")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is False

    def test_no_match_missing_instrume(self, profile: EsoXShooterProfile) -> None:
        header = _make_primary_header(INSTRUME=None)
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is False

    def test_no_match_other_eso_instrument(self, profile: EsoXShooterProfile) -> None:
        header = _make_primary_header(INSTRUME="HARPS   ")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("ESO", hdulist) is False


# ---------------------------------------------------------------------------
# TestEsoXShooterProfileHappyPath
# ---------------------------------------------------------------------------


class TestEsoXShooterProfileHappyPath:
    @pytest.fixture
    def result(self, profile: EsoXShooterProfile) -> ProfileResult:
        hdulist = _make_hdulist()
        return profile.validate(hdulist, _product_metadata())

    def test_success_is_true(self, result: ProfileResult) -> None:
        assert result.success is True

    def test_spectrum_is_populated(self, result: ProfileResult) -> None:
        assert result.spectrum is not None

    def test_profile_id(self, result: ProfileResult) -> None:
        assert result.profile_id == "ESO_XSHOOTER"

    def test_spectral_axis_shape(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.spectral_axis.ndim == 1
        assert len(result.spectrum.spectral_axis) == _N

    def test_flux_axis_shape(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.flux_axis.ndim == 1
        assert len(result.spectrum.flux_axis) == _N

    def test_spectral_units_nm(self, result: ProfileResult) -> None:
        """X-Shooter WAVE is in nm — not angstrom like UVES."""
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "nm"

    def test_flux_units_present(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.flux_units != ""

    def test_observation_time_from_mjd(self, result: ProfileResult) -> None:
        """MJD-OBS is preferred over DATE-OBS for precision."""
        assert result.spectrum is not None
        # MJD 56368.97085495 → 2013-03-17T23:18:01Z
        assert result.spectrum.observation_time.startswith("2013-03-17T")
        assert result.spectrum.observation_time.endswith("Z")

    def test_observation_mjd(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.observation_mjd is not None
        assert abs(result.spectrum.observation_mjd - _MJD_OBS) < 1e-6

    def test_ra_deg(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.target_ra_deg is not None
        assert abs(result.spectrum.target_ra_deg - 229.6713) < 1e-4

    def test_dec_deg(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.target_dec_deg is not None
        assert abs(result.spectrum.target_dec_deg - (-32.4770)) < 1e-4

    def test_instrument(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.instrument == "XSHOOTER"

    def test_telescope(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.telescope == "ESO-VLT-U2"

    def test_exposure_time(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.exposure_time_s is not None
        assert abs(result.spectrum.exposure_time_s - 1800.0) < 0.001

    def test_spectral_resolution(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.spectral_resolution == 11000.0

    def test_provider_passthrough(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.provider == "ESO"

    def test_data_product_id_passthrough(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert result.spectrum.data_product_id == "test-xshooter-dpid-0001"

    def test_header_signature_hash_present(self, result: ProfileResult) -> None:
        assert result.header_signature_hash is not None
        assert len(result.header_signature_hash) == 16

    def test_raw_header_populated(self, result: ProfileResult) -> None:
        assert result.spectrum is not None
        assert "INSTRUME" in result.spectrum.raw_header

    def test_no_quarantine_reason(self, result: ProfileResult) -> None:
        assert result.quarantine_reason is None
        assert result.quarantine_reason_code is None

    def test_descending_wave_succeeds(self, profile: EsoXShooterProfile) -> None:
        """Descending spectral axis is valid — profile notes it but does not quarantine."""
        hdulist = _make_hdulist(
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_nm(descending=True))
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any("descending" in note.lower() for note in result.normalization_notes)

    def test_date_obs_fallback_when_no_mjd(self, profile: EsoXShooterProfile) -> None:
        """
        When MJD-OBS is absent, DATE-OBS is used directly.
        Unlike UVES, X-Shooter DATE-OBS includes a time component so no
        T00:00:00Z appending note should appear.
        """
        header = _make_primary_header(**{"MJD-OBS": None})
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.observation_time.startswith("2013-03-17T")

    def test_date_obs_dateonly_fallback_adds_note(self, profile: EsoXShooterProfile) -> None:
        """
        If DATE-OBS is unexpectedly date-only (defensive path), a note is added
        and validation still succeeds.
        """
        header = _make_primary_header(**{"MJD-OBS": None, "DATE-OBS": "2013-03-17"})
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.observation_time == "2013-03-17T00:00:00Z"
        assert any(
            "day-level" in note.lower() or "date-only" in note.lower()
            for note in result.normalization_notes
        )


# ---------------------------------------------------------------------------
# TestEsoXShooterProfileAllThreeArms
# ---------------------------------------------------------------------------


class TestEsoXShooterProfileAllThreeArms:
    """Each X-Shooter arm is a separate archive product — all three must validate."""

    @pytest.mark.parametrize("arm", ["UVB", "VIS", "NIR"])
    def test_arm_validates_successfully(self, profile: EsoXShooterProfile, arm: str) -> None:
        header = _make_primary_header(DISPELEM=arm)
        hdulist = _make_hdulist(
            primary_header=header,
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_nm(arm=arm)),
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True, f"Arm {arm!r} failed validation: {result.quarantine_reason}"
        assert result.profile_id == "ESO_XSHOOTER"

    @pytest.mark.parametrize("arm", ["UVB", "VIS", "NIR"])
    def test_arm_spectral_units_nm(self, profile: EsoXShooterProfile, arm: str) -> None:
        header = _make_primary_header(DISPELEM=arm)
        hdulist = _make_hdulist(
            primary_header=header,
            spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_nm(arm=arm)),
        )
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "nm"


# ---------------------------------------------------------------------------
# TestEsoXShooterProfileMissingColumns
# ---------------------------------------------------------------------------


class TestEsoXShooterProfileMissingColumns:
    def test_missing_wave_column_quarantines(self, profile: EsoXShooterProfile) -> None:
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(include_wave=False))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"
        assert result.quarantine_reason is not None
        assert "wave" in result.quarantine_reason.lower()

    def test_missing_flux_column_quarantines(self, profile: EsoXShooterProfile) -> None:
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(include_flux=False))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_no_spectrum_hdu_quarantines(self, profile: EsoXShooterProfile) -> None:
        """HDUList with only a primary HDU and no BinTable → quarantine."""
        hdulist = fits.HDUList([fits.PrimaryHDU(header=_make_primary_header())])
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_wave_alias_accepted(self, profile: EsoXShooterProfile) -> None:
        """WAVELENGTH column name is accepted as alias for WAVE."""
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave_col_name="WAVELENGTH"))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any(
            "alias" in note.lower() or "wavelength" in note.lower()
            for note in result.normalization_notes
        )


# ---------------------------------------------------------------------------
# TestEsoXShooterProfileMissingMetadata
# ---------------------------------------------------------------------------


class TestEsoXShooterProfileMissingMetadata:
    def test_missing_mjd_and_date_obs_quarantines(self, profile: EsoXShooterProfile) -> None:
        header = _make_primary_header(**{"MJD-OBS": None, "DATE-OBS": None})
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"
        assert result.quarantine_reason is not None
        assert "observation_time" in result.quarantine_reason.lower()

    def test_missing_ra_dec_still_succeeds(self, profile: EsoXShooterProfile) -> None:
        """RA and DEC are optional."""
        header = _make_primary_header(RA=None, DEC=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.target_ra_deg is None
        assert result.spectrum.target_dec_deg is None

    def test_missing_exptime_still_succeeds(self, profile: EsoXShooterProfile) -> None:
        header = _make_primary_header(EXPTIME=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.exposure_time_s is None

    def test_missing_spec_res_still_succeeds(self, profile: EsoXShooterProfile) -> None:
        header = _make_primary_header(SPEC_RES=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.spectral_resolution is None

    def test_missing_dispelem_still_succeeds(self, profile: EsoXShooterProfile) -> None:
        """DISPELEM is optional — its absence means the arm cannot be identified
        but must not quarantine the product."""
        header = _make_primary_header(DISPELEM=None)
        hdulist = _make_hdulist(primary_header=header)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True


# ---------------------------------------------------------------------------
# TestEsoXShooterProfileSanityChecks
# ---------------------------------------------------------------------------


class TestEsoXShooterProfileSanityChecks:
    def test_all_nan_flux_quarantines(self, profile: EsoXShooterProfile) -> None:
        flux = np.full(_N, np.nan)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code is not None

    def test_high_nan_fraction_quarantines(self, profile: EsoXShooterProfile) -> None:
        """25% NaN/Inf is above the 20% threshold → quarantine."""
        flux = _make_flux(nan_fraction=0.25)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "non-finite" in result.quarantine_reason.lower()

    def test_acceptable_nan_fraction_succeeds(self, profile: EsoXShooterProfile) -> None:
        """10% NaN is below the threshold — note added but not quarantined."""
        flux = _make_flux(nan_fraction=0.10)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any(
            "nan" in note.lower() or "non-finite" in note.lower()
            for note in result.normalization_notes
        )

    def test_non_monotonic_wave_quarantines(self, profile: EsoXShooterProfile) -> None:
        wave = _make_wave_nm()
        wave[_N // 2] = wave[_N // 2 - 5]
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "monoton" in result.quarantine_reason.lower()

    def test_all_zero_flux_quarantines(self, profile: EsoXShooterProfile) -> None:
        flux = np.zeros(_N)
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "zero" in result.quarantine_reason.lower()

    def test_wave_in_metres_looks_out_of_range(self, profile: EsoXShooterProfile) -> None:
        """
        A wavelength array in metres (e.g. 5e-7 to 2.5e-6) would appear as
        ~0.0000005–0.0000025 nm to the profile, far below any plausible nm
        lower bound → quarantine.
        """
        wave_metres = np.linspace(5e-7, 2.5e-6, _N)  # metres, not nm
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave_metres))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason is not None
        assert "range" in result.quarantine_reason.lower()

    def test_empty_flux_quarantines(self, profile: EsoXShooterProfile) -> None:
        wave = np.array([])
        flux = np.array([])
        hdulist = _make_hdulist(spectrum_hdu=_make_spectrum_hdu(wave=wave, flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False


# ---------------------------------------------------------------------------
# TestEsoXShooterProfileHeaderSignature
# ---------------------------------------------------------------------------


class TestEsoXShooterProfileHeaderSignature:
    def test_hash_differs_across_arms(self, profile: EsoXShooterProfile) -> None:
        """
        Two products from the same observation block but different arms must
        produce different header_signature_hash values because DISPELEM is
        included in the hash inputs.
        """
        results = {}
        for arm in ("UVB", "VIS", "NIR"):
            header = _make_primary_header(DISPELEM=arm)
            hdulist = _make_hdulist(
                primary_header=header,
                spectrum_hdu=_make_spectrum_hdu(wave=_make_wave_nm(arm=arm)),
            )
            result = profile.validate(hdulist, _product_metadata())
            assert result.success is True
            results[arm] = result.header_signature_hash

        assert results["UVB"] != results["VIS"]
        assert results["VIS"] != results["NIR"]
        assert results["UVB"] != results["NIR"]

    def test_hash_stable_for_same_inputs(self, profile: EsoXShooterProfile) -> None:
        """Same header fields must always produce the same hash (deterministic)."""
        hdulist = _make_hdulist()
        r1 = profile.validate(hdulist, _product_metadata())
        r2 = profile.validate(hdulist, _product_metadata())
        assert r1.header_signature_hash == r2.header_signature_hash

    def test_hash_length(self, profile: EsoXShooterProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.header_signature_hash is not None
        assert len(result.header_signature_hash) == 16


# ---------------------------------------------------------------------------
# TestValidateSpectrumDispatchesXShooter
# ---------------------------------------------------------------------------


class TestValidateSpectrumDispatchesXShooter:
    def test_dispatches_to_xshooter_profile(self) -> None:
        hdulist = _make_hdulist()
        result = validate_spectrum(
            hdulist,
            provider="ESO",
            data_product_id="test-xshooter-dispatch-001",
            hints={"collection": "XSHOOTER"},
        )
        assert result.success is True
        assert result.profile_id == "ESO_XSHOOTER"

    def test_uves_not_claimed_by_xshooter(self) -> None:
        """A UVES file must be routed to EsoUvesProfile, not EsoXShooterProfile."""

        uves_header = fits.Header()
        uves_header["INSTRUME"] = "UVES    "
        uves_header["TELESCOP"] = "ESO-VLT-U2"
        uves_header["MJD-OBS"] = 56082.05467768
        uves_header["DATE-OBS"] = "2012-06-04"
        uves_header["EXPTIME"] = 7200.0
        uves_header["SPEC_RES"] = 42000.0

        wave = np.linspace(3200.0, 10000.0, _N)
        flux = np.ones(_N) * 1.5e-16
        cols = fits.ColDefs(
            [
                fits.Column(
                    name="WAVE", format=f"{_N}D", unit="angstrom", array=wave.reshape(1, _N)
                ),
                fits.Column(
                    name="FLUX", format=f"{_N}D", unit="erg/s/cm2/AA", array=flux.reshape(1, _N)
                ),
            ]
        )
        spectrum_hdu = fits.BinTableHDU.from_columns(cols)
        spectrum_hdu.name = "SPECTRUM"
        hdulist = fits.HDUList([fits.PrimaryHDU(header=uves_header), spectrum_hdu])

        result = validate_spectrum(
            hdulist,
            provider="ESO",
            data_product_id="uves-routing-check",
            hints={"collection": "UVES"},
        )
        assert result.profile_id == "ESO_UVES"

    def test_data_product_id_propagated(self) -> None:
        hdulist = _make_hdulist()
        dpid = "xshooter-propagation-uuid"
        result = validate_spectrum(
            hdulist,
            provider="ESO",
            data_product_id=dpid,
            hints={},
        )
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.data_product_id == dpid

    def test_harps_routes_to_fallback_not_xshooter(self) -> None:
        """ESO + HARPS is not claimed by XSHOOTER — routes to EsoFallbackProfile."""
        header = _make_primary_header(INSTRUME="HARPS   ")
        hdulist = _make_hdulist(primary_header=header)
        result = validate_spectrum(
            hdulist,
            provider="ESO",
            data_product_id="harps-fallback",
            hints={"collection": "HARPS"},
        )
        assert result.success is True
        assert result.profile_id == "ESO_HARPS"


# ---------------------------------------------------------------------------
# TestEsoXShooterProfileSnrExtraction
# ---------------------------------------------------------------------------


class TestEsoXShooterProfileSnrExtraction:
    @pytest.fixture
    def profile(self) -> EsoXShooterProfile:
        return EsoXShooterProfile()

    def test_snr_extracted_when_present(self, profile: EsoXShooterProfile) -> None:
        """SNR column in BinTable → NormalizedSpectrum.snr is the median."""
        wave = _make_wave_nm()
        flux = _make_flux()
        n = len(wave)
        snr_data = np.linspace(5.0, 30.0, n)
        expected_median = float(np.median(snr_data))

        cols = [
            fits.Column(name="WAVE", format=f"{n}D", unit="nm", array=wave.reshape(1, n)),
            fits.Column(
                name="FLUX",
                format=f"{n}D",
                unit="erg cm**(-2) s**(-1) angstrom**(-1)",
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

    def test_snr_none_when_absent(self, profile: EsoXShooterProfile) -> None:
        """No SNR column → NormalizedSpectrum.snr is None."""
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is None
