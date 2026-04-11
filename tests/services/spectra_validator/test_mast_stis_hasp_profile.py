"""
tests/spectra_validator/test_mast_stis_hasp_profile.py

Unit tests for MastStisHaspProfile and validate_spectrum dispatch.

All tests use synthetic astropy HDULists — no binary test fixtures on disk.
The _make_hdulist() helper produces a minimal but structurally valid HASP
cspec file matching the confirmed real-file shape:
  HDU[0]: PrimaryHDU (metadata only)
  HDU[1]: BinTableHDU "SCI" (1 row, variable-length array columns)
  HDU[2]: BinTableHDU "PROVENANCE" (one row per constituent exposure)

Test groups:
  TestMastStisHaspMatches          — matches() accept/reject logic
  TestMastStisHaspHappyPath        — successful validation; output shape and values
  TestMastStisHaspMissingColumns   — WAVELENGTH/FLUX absence → quarantine
  TestMastStisHaspProvenance       — observation time, exposure time from PROVENANCE
  TestMastStisHaspSanityChecks     — mandatory sanity checks
  TestMastStisHaspSnr              — SNR extraction
  TestValidateSpectrumDispatch     — registry dispatch to MAST_STIS_HASP
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
from spectra_validator.profiles.mast_stis_hasp import MastStisHaspProfile  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_N = 200  # synthetic spectrum length


def _make_primary_header(**overrides: object) -> fits.Header:
    """
    Build a minimal HASP STIS primary header with all expected fields.
    Use a value of None to delete a key (simulates missing header field).
    """
    defaults: dict[str, object] = {
        "SIMPLE": True,
        "BITPIX": 16,
        "NAXIS": 0,
        "TELESCOP": "HST",
        "INSTRUME": "STIS",
        "DETECTOR": "FUV-MAMA",
        "APERTURE": "0.2X0.2",
        "TARGNAME": "V339-DEL",
        "PROPOSID": 13828,
        "CENTRWV": 1437.5,
        "MINWAVE": 1140.0,
        "MAXWAVE": 1735.0,
        "FILENAME": "hst_13828_stis_v339-del_e140m_ocoj08_cspec.fits",
        "HLSP_LVL": 1,
        "CAL_VER": "HSLA Cal 1.2.4",
        "ORIGIN": "Space Telescope Science Institute",
        "NUM_EXP": 1,
        "TARG_RA": 305.8778,
        "TARG_DEC": 20.7677,
    }
    defaults.update(overrides)

    header = fits.Header()
    for key, val in defaults.items():
        if val is None:
            continue
        header[key] = val
    return header


def _make_wave(n: int = _N) -> np.ndarray:
    """Strictly monotonic wavelength array in Angstrom (STIS FUV range)."""
    return np.linspace(1143.0, 1710.0, n)


def _make_flux(n: int = _N, nan_fraction: float = 0.0) -> np.ndarray:
    """Flux array with optional NaN injection."""
    rng = np.random.default_rng(42)
    flux = rng.uniform(1e-13, 5e-13, n)
    if nan_fraction > 0:
        n_nan = int(n * nan_fraction)
        flux[:n_nan] = np.nan
    return flux


def _make_snr(n: int = _N) -> np.ndarray:
    """SNR array with realistic values (mix of positive and negative)."""
    rng = np.random.default_rng(99)
    return rng.uniform(-5.0, 15.0, n)


def _make_sci_hdu(
    wave: np.ndarray | None = None,
    flux: np.ndarray | None = None,
    include_wave: bool = True,
    include_flux: bool = True,
    include_snr: bool = True,
    hdu_name: str = "SCI",
) -> fits.BinTableHDU:
    """
    Build a BinTableHDU matching the real HASP cspec SCI shape:
      1 row, variable-length array columns (shape 1 × N).
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
                name="WAVELENGTH",
                format=f"{n}E",
                unit="Angstrom",
                array=wave.reshape(1, n).astype(np.float32),
            )
        )

    if include_flux:
        flux_unit = "erg /s /cm**2 /Angstrom"
        cols.append(
            fits.Column(
                name="FLUX",
                format=f"{n}E",
                unit=flux_unit,
                array=flux.reshape(1, n).astype(np.float32),
            )
        )
        cols.append(
            fits.Column(
                name="ERROR",
                format=f"{n}E",
                unit=flux_unit,
                array=(flux * 0.1).reshape(1, n).astype(np.float32),
            )
        )

    if include_snr and include_flux:
        snr = _make_snr(n)
        cols.append(
            fits.Column(
                name="SNR",
                format=f"{n}E",
                unit="",
                array=snr.reshape(1, n).astype(np.float32),
            )
        )

    if not cols:
        # Need at least one column to create a BinTableHDU
        cols.append(fits.Column(name="DUMMY", format="1E", array=np.array([[0.0]])))

    hdu = fits.BinTableHDU.from_columns(cols)
    hdu.name = hdu_name
    return hdu


def _make_provenance_hdu(
    rows: list[dict] | None = None,
    hdu_name: str = "PROVENANCE",
) -> fits.BinTableHDU:
    """
    Build a PROVENANCE BinTableHDU matching real HASP structure.
    Each row represents one constituent x1d exposure.
    """
    if rows is None:
        rows = [_prov_row()]

    filenames = [r.get("FILENAME", "oc7r06010_x1d.fits") for r in rows]
    dispersers = [r.get("DISPERSER", "E140M") for r in rows]
    mjd_begs = [r.get("MJD_BEG", 57153.916) for r in rows]
    mjd_ends = [r.get("MJD_END", 57153.920) for r in rows]
    xposures = [r.get("XPOSURE", 2455.0) for r in rows]
    specress = [r.get("SPECRES", 45800.0) for r in rows]
    cenwaves = [r.get("CENWAVE", 1425) for r in rows]

    cols = [
        fits.Column(name="FILENAME", format="50A", array=filenames),
        fits.Column(name="DISPERSER", format="10A", array=dispersers),
        fits.Column(name="MJD_BEG", format="D", array=mjd_begs),
        fits.Column(name="MJD_END", format="D", array=mjd_ends),
        fits.Column(name="XPOSURE", format="D", array=xposures),
        fits.Column(name="SPECRES", format="D", array=specress),
        fits.Column(name="CENWAVE", format="J", array=cenwaves),
    ]

    hdu = fits.BinTableHDU.from_columns(cols)
    hdu.name = hdu_name
    return hdu


def _prov_row(**overrides: object) -> dict:
    """Build a single PROVENANCE row dict."""
    defaults = {
        "FILENAME": "ocoj08010_x1d.fits",
        "DISPERSER": "E140M",
        "MJD_BEG": 57153.916504,
        "MJD_END": 57153.930000,
        "XPOSURE": 2455.0,
        "SPECRES": 45800.0,
        "CENWAVE": 1425,
    }
    return {**defaults, **overrides}


def _make_hdulist(
    primary_header: fits.Header | None = None,
    sci_hdu: fits.BinTableHDU | None = None,
    provenance_hdu: fits.BinTableHDU | None = None,
) -> fits.HDUList:
    """Assemble a minimal HASP cspec HDUList."""
    if primary_header is None:
        primary_header = _make_primary_header()
    if sci_hdu is None:
        sci_hdu = _make_sci_hdu()
    if provenance_hdu is None:
        provenance_hdu = _make_provenance_hdu()

    primary = fits.PrimaryHDU(header=primary_header)
    return fits.HDUList([primary, sci_hdu, provenance_hdu])


def _product_metadata(
    data_product_id: str = "test-dpid-0001",
    provider: str = "MAST",
) -> dict:
    return {"data_product_id": data_product_id, "provider": provider, "hints": {}}


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def profile() -> MastStisHaspProfile:
    return MastStisHaspProfile()


# ---------------------------------------------------------------------------
# TestMastStisHaspMatches
# ---------------------------------------------------------------------------


class TestMastStisHaspMatches:
    def test_matches_mast_stis(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("MAST", hdulist) is True

    def test_no_match_wrong_provider(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        assert profile.matches("ESO", hdulist) is False

    def test_no_match_wrong_instrument(self, profile: MastStisHaspProfile) -> None:
        header = _make_primary_header(INSTRUME="COS")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("MAST", hdulist) is False

    def test_no_match_missing_origin(self, profile: MastStisHaspProfile) -> None:
        header = _make_primary_header(ORIGIN=None)
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("MAST", hdulist) is False

    def test_no_match_wrong_origin(self, profile: MastStisHaspProfile) -> None:
        header = _make_primary_header(ORIGIN="European Southern Observatory")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("MAST", hdulist) is False

    def test_matches_stis_with_whitespace(self, profile: MastStisHaspProfile) -> None:
        """INSTRUME='STIS    ' (padded) must still match."""
        header = _make_primary_header(INSTRUME="STIS    ")
        hdulist = _make_hdulist(primary_header=header)
        assert profile.matches("MAST", hdulist) is True


# ---------------------------------------------------------------------------
# TestMastStisHaspHappyPath
# ---------------------------------------------------------------------------


class TestMastStisHaspHappyPath:
    def test_valid_spectrum_succeeds(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.profile_id == "MAST_STIS_HASP"

    def test_spectrum_has_correct_units(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.spectral_units == "Angstrom"
        assert "erg" in result.spectrum.flux_units

    def test_spectrum_arrays_are_1d(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.spectral_axis.ndim == 1
        assert result.spectrum.flux_axis.ndim == 1
        assert len(result.spectrum.spectral_axis) == _N

    def test_data_product_id_propagated(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        dpid = "propagation-test-uuid"
        result = profile.validate(hdulist, _product_metadata(data_product_id=dpid))
        assert result.spectrum is not None
        assert result.spectrum.data_product_id == dpid

    def test_provider_propagated(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.provider == "MAST"

    def test_instrument_extracted(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.instrument == "STIS"

    def test_telescope_extracted(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.telescope == "HST"

    def test_coordinates_extracted(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.target_ra_deg == pytest.approx(305.8778, abs=0.001)
        assert result.spectrum.target_dec_deg == pytest.approx(20.7677, abs=0.001)

    def test_header_signature_hash_present(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.header_signature_hash is not None
        assert len(result.header_signature_hash) == 16  # 16-char hex prefix


# ---------------------------------------------------------------------------
# TestMastStisHaspMissingColumns
# ---------------------------------------------------------------------------


class TestMastStisHaspMissingColumns:
    def test_missing_wavelength_quarantines(self, profile: MastStisHaspProfile) -> None:
        sci_hdu = _make_sci_hdu(include_wave=False)
        hdulist = _make_hdulist(sci_hdu=sci_hdu)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_missing_flux_quarantines(self, profile: MastStisHaspProfile) -> None:
        sci_hdu = _make_sci_hdu(include_flux=False)
        hdulist = _make_hdulist(sci_hdu=sci_hdu)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_missing_sci_hdu_quarantines(self, profile: MastStisHaspProfile) -> None:
        """HDUList without a SCI extension → quarantine."""
        primary = fits.PrimaryHDU(header=_make_primary_header())
        provenance = _make_provenance_hdu()
        hdulist = fits.HDUList([primary, provenance])
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"

    def test_wrong_sci_hdu_name_still_found(self, profile: MastStisHaspProfile) -> None:
        """SCI HDU with non-standard name but correct columns is still found."""
        sci_hdu = _make_sci_hdu(hdu_name="SCIENCE")
        hdulist = _make_hdulist(sci_hdu=sci_hdu)
        result = profile.validate(hdulist, _product_metadata())
        # Should still validate — _find_sci_hdu falls back to column search
        assert result.success is True


# ---------------------------------------------------------------------------
# TestMastStisHaspProvenance
# ---------------------------------------------------------------------------


class TestMastStisHaspProvenance:
    def test_observation_time_from_min_mjd_beg(self, profile: MastStisHaspProfile) -> None:
        """Observation time = min(MJD_BEG) across provenance rows."""
        rows = [
            _prov_row(MJD_BEG=57153.916),
            _prov_row(MJD_BEG=57153.900, FILENAME="earlier_x1d.fits"),  # earlier
        ]
        hdulist = _make_hdulist(provenance_hdu=_make_provenance_hdu(rows))
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.observation_mjd == pytest.approx(57153.900, abs=0.001)

    def test_observation_time_is_iso8601(self, profile: MastStisHaspProfile) -> None:
        hdulist = _make_hdulist()
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert "T" in result.spectrum.observation_time
        assert result.spectrum.observation_time.endswith("Z")

    def test_exposure_time_summed_from_provenance(self, profile: MastStisHaspProfile) -> None:
        """Exposure time = sum(XPOSURE) across provenance rows."""
        rows = [
            _prov_row(XPOSURE=100.0),
            _prov_row(XPOSURE=200.0, FILENAME="second_x1d.fits"),
        ]
        hdulist = _make_hdulist(provenance_hdu=_make_provenance_hdu(rows))
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        assert result.spectrum.exposure_time_s == pytest.approx(300.0)

    def test_spectral_resolution_from_provenance(self, profile: MastStisHaspProfile) -> None:
        """Spectral resolution = median(SPECRES) from provenance."""
        rows = [
            _prov_row(SPECRES=45800.0),
            _prov_row(SPECRES=114000.0, FILENAME="second_x1d.fits"),
        ]
        hdulist = _make_hdulist(provenance_hdu=_make_provenance_hdu(rows))
        result = profile.validate(hdulist, _product_metadata())
        assert result.spectrum is not None
        expected = float(np.median([45800.0, 114000.0]))
        assert result.spectrum.spectral_resolution == pytest.approx(expected)

    def test_missing_provenance_quarantines(self, profile: MastStisHaspProfile) -> None:
        """No PROVENANCE HDU → quarantine (cannot determine observation time)."""
        primary = fits.PrimaryHDU(header=_make_primary_header())
        sci = _make_sci_hdu()
        hdulist = fits.HDUList([primary, sci])
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert result.quarantine_reason_code == "MISSING_CRITICAL_METADATA"
        assert "PROVENANCE" in (result.quarantine_reason or "")

    def test_empty_provenance_quarantines(self, profile: MastStisHaspProfile) -> None:
        """PROVENANCE HDU with zero rows → quarantine."""
        # Build with explicit empty arrays
        cols = [
            fits.Column(name="FILENAME", format="50A", array=[]),
            fits.Column(name="MJD_BEG", format="D", array=[]),
            fits.Column(name="MJD_END", format="D", array=[]),
            fits.Column(name="XPOSURE", format="D", array=[]),
            fits.Column(name="SPECRES", format="D", array=[]),
        ]
        empty_prov = fits.BinTableHDU.from_columns(cols)
        empty_prov.name = "PROVENANCE"
        hdulist = _make_hdulist(provenance_hdu=empty_prov)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False


# ---------------------------------------------------------------------------
# TestMastStisHaspSanityChecks
# ---------------------------------------------------------------------------


class TestMastStisHaspSanityChecks:
    def test_non_monotonic_quarantines(self, profile: MastStisHaspProfile) -> None:
        """Non-monotonic wavelength axis → quarantine."""
        wave = _make_wave()
        wave[50], wave[51] = wave[51], wave[50]  # swap two elements
        hdulist = _make_hdulist(sci_hdu=_make_sci_hdu(wave=wave))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert "monoton" in (result.quarantine_reason or "").lower()

    def test_descending_wave_accepted(self, profile: MastStisHaspProfile) -> None:
        """Monotonically decreasing wavelength is accepted with a note."""
        wave = _make_wave()[::-1]
        hdulist = _make_hdulist(sci_hdu=_make_sci_hdu(wave=wave))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert any("decreasing" in n.lower() for n in result.normalization_notes)

    def test_high_nan_fraction_quarantines(self, profile: MastStisHaspProfile) -> None:
        """Flux with >20% NaN → quarantine."""
        flux = _make_flux(nan_fraction=0.25)
        hdulist = _make_hdulist(sci_hdu=_make_sci_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert "nan" in (result.quarantine_reason or "").lower()

    def test_acceptable_nan_fraction_passes(self, profile: MastStisHaspProfile) -> None:
        """Flux with <20% NaN → passes."""
        flux = _make_flux(nan_fraction=0.10)
        hdulist = _make_hdulist(sci_hdu=_make_sci_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True

    def test_wave_out_of_range_quarantines(self, profile: MastStisHaspProfile) -> None:
        """Wavelength in nm instead of Angstrom looks out-of-range."""
        wave = np.linspace(114.3, 171.0, _N)  # nm, not Angstrom
        hdulist = _make_hdulist(sci_hdu=_make_sci_hdu(wave=wave))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False
        assert "range" in (result.quarantine_reason or "").lower()

    def test_all_nan_flux_quarantines(self, profile: MastStisHaspProfile) -> None:
        """Flux array with all NaN → quarantine."""
        flux = np.full(_N, np.nan)
        hdulist = _make_hdulist(sci_hdu=_make_sci_hdu(flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False

    def test_empty_arrays_quarantine(self, profile: MastStisHaspProfile) -> None:
        wave = np.array([])
        flux = np.array([])
        hdulist = _make_hdulist(sci_hdu=_make_sci_hdu(wave=wave, flux=flux))
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is False


# ---------------------------------------------------------------------------
# TestMastStisHaspSnr
# ---------------------------------------------------------------------------


class TestMastStisHaspSnr:
    def test_snr_extracted_as_median_positive(self, profile: MastStisHaspProfile) -> None:
        """SNR = median of finite, positive SNR values."""
        wave = _make_wave()
        flux = _make_flux()
        n = len(wave)
        snr_data = np.linspace(1.0, 20.0, n)
        expected_median = float(np.median(snr_data))

        cols = [
            fits.Column(
                name="WAVELENGTH",
                format=f"{n}E",
                unit="Angstrom",
                array=wave.reshape(1, n).astype(np.float32),
            ),
            fits.Column(
                name="FLUX",
                format=f"{n}E",
                unit="erg /s /cm**2 /Angstrom",
                array=flux.reshape(1, n).astype(np.float32),
            ),
            fits.Column(
                name="ERROR",
                format=f"{n}E",
                unit="erg /s /cm**2 /Angstrom",
                array=(flux * 0.1).reshape(1, n).astype(np.float32),
            ),
            fits.Column(
                name="SNR", format=f"{n}E", unit="", array=snr_data.reshape(1, n).astype(np.float32)
            ),
        ]
        sci = fits.BinTableHDU.from_columns(cols)
        sci.name = "SCI"
        hdulist = _make_hdulist(sci_hdu=sci)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is not None
        assert abs(result.spectrum.snr - expected_median) < 0.1

    def test_snr_none_when_column_absent(self, profile: MastStisHaspProfile) -> None:
        """No SNR column → snr is None."""
        sci = _make_sci_hdu(include_snr=False)
        hdulist = _make_hdulist(sci_hdu=sci)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr is None

    def test_snr_ignores_negative_values(self, profile: MastStisHaspProfile) -> None:
        """Negative SNR values are excluded from the median."""
        wave = _make_wave()
        flux = _make_flux()
        n = len(wave)
        snr_data = np.full(n, -10.0)
        snr_data[0:10] = 5.0  # only 10 positive values

        cols = [
            fits.Column(
                name="WAVELENGTH",
                format=f"{n}E",
                unit="Angstrom",
                array=wave.reshape(1, n).astype(np.float32),
            ),
            fits.Column(
                name="FLUX",
                format=f"{n}E",
                unit="erg /s /cm**2 /Angstrom",
                array=flux.reshape(1, n).astype(np.float32),
            ),
            fits.Column(
                name="ERROR",
                format=f"{n}E",
                unit="erg /s /cm**2 /Angstrom",
                array=(flux * 0.1).reshape(1, n).astype(np.float32),
            ),
            fits.Column(
                name="SNR", format=f"{n}E", unit="", array=snr_data.reshape(1, n).astype(np.float32)
            ),
        ]
        sci = fits.BinTableHDU.from_columns(cols)
        sci.name = "SCI"
        hdulist = _make_hdulist(sci_hdu=sci)
        result = profile.validate(hdulist, _product_metadata())
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.snr == pytest.approx(5.0, abs=0.1)


# ---------------------------------------------------------------------------
# TestValidateSpectrumDispatch
# ---------------------------------------------------------------------------


class TestValidateSpectrumDispatch:
    def test_dispatches_to_mast_stis_hasp(self) -> None:
        hdulist = _make_hdulist()
        result = validate_spectrum(
            hdulist,
            provider="MAST",
            data_product_id="test-dpid-9999",
            hints={},
        )
        assert result.success is True
        assert result.profile_id == "MAST_STIS_HASP"

    def test_data_product_id_propagated(self) -> None:
        hdulist = _make_hdulist()
        dpid = "propagation-test-uuid"
        result = validate_spectrum(
            hdulist,
            provider="MAST",
            data_product_id=dpid,
            hints={},
        )
        assert result.success is True
        assert result.spectrum is not None
        assert result.spectrum.data_product_id == dpid
