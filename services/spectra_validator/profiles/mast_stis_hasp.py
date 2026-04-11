"""
profiles/mast_stis_hasp.py — MAST STIS HASP FITS profile.

Validates HASP (Hubble Advanced Spectral Products) co-added spectra from
HST/STIS served via the MAST archive.

Validated against real data:
  hst_13388_stis_novadel2013_e140m_oc7r06_cspec.fits    (single grating, HLSP_LVL=1)
  hst_13388_stis_novadel2013_e140m-e230h_oc7r01_cspec.fits  (multi-grating, HLSP_LVL=2)
  hst_13828_stis_v339-del_e140m_ocoj08_cspec.fits       (single grating, HLSP_LVL=1)

FITS structure (confirmed from real data):
  HDU[0]: PrimaryHDU — NAXIS=0 (no data); observation metadata in header
  HDU[1]: BinTableHDU named "SCI" — 1 row, variable-length array columns
  HDU[2]: BinTableHDU named "PROVENANCE" — one row per constituent exposure

  Key HDU[0] header keywords:
    TELESCOP  = 'HST'
    INSTRUME  = 'STIS' (or 'COS' — this profile handles STIS only)
    DETECTOR  = 'FUV-MAMA' | 'NUV-MAMA' | 'CCD' | 'MULTI'
    APERTURE  = '0.2X0.2' (etc.)
    TARGNAME  = target name as used in HST proposal (e.g. 'NOVADEL2013')
    PROPOSID  = HST program number (integer)
    CENTRWV   = central wavelength (float, Angstrom)
    MINWAVE   = wavelength minimum (float, Angstrom)
    MAXWAVE   = wavelength maximum (float, Angstrom)
    FILENAME  = HASP product filename (e.g. 'hst_13388_stis_..._cspec.fits')
    HLSP_LVL  = 1 or 2 (HASP high-level science product level)
    CAL_VER   = HASP calibration version string
    ORIGIN    = 'Space Telescope Science Institute'
    NUM_EXP   = number of constituent exposures (integer)

    NOTE: No MJD-OBS or DATE-OBS with observation time — observation time
    must be derived from the PROVENANCE table (HDU[2]).

  HDU[1] "SCI" columns (all arrays of shape (1, N), float32):
    WAVELENGTH    Angstrom           spectral axis
    FLUX          erg/s/cm**2/Angstrom  absolute calibrated flux
    ERROR         erg/s/cm**2/Angstrom  1-sigma uncertainty
    SNR           dimensionless      signal-to-noise per pixel
    EFF_EXPTIME   seconds            effective exposure time per pixel

  HDU[2] "PROVENANCE" columns (one row per constituent x1d exposure):
    FILENAME, EXPNAME, PROPOSID, TELESCOPE, INSTRUMENT, DETECTOR,
    DISPERSER, CENWAVE, APERTURE, LIFE_ADJ, SPECRES, CAL_VER,
    MJD_BEG, MJD_MID, MJD_END, XPOSURE, MINWAVE, MAXWAVE

Observation time derivation:
  min(PROVENANCE.MJD_BEG) across all provenance rows → MJD → ISO-8601 UTC.
  This gives the start of the earliest constituent exposure, which is the
  correct epoch for a per-visit co-add in transient science.

Exposure time derivation:
  sum(PROVENANCE.XPOSURE) across all provenance rows — total on-source time.

SNR derivation:
  Median of finite, positive values in the SNR column of HDU[1].

Wavelength units:
  Always Angstrom for HASP STIS products (confirmed from TUNIT1).

Flux calibration:
  Always absolute (erg/s/cm²/Å) for HASP cspec products.
"""

from __future__ import annotations

import contextlib
import hashlib
from datetime import UTC, datetime
from typing import Any

import numpy as np

from .base import NormalizedSpectrum, ProfileResult

# ---------------------------------------------------------------------------
# Sanity check thresholds
# ---------------------------------------------------------------------------

_MAX_NAN_INF_FRACTION = 0.20  # >20% bad pixels → QUARANTINE
_WAVE_MIN_ANGSTROM = 800.0  # generous UV cutoff (Lyman limit ~912 Å)
_WAVE_MAX_ANGSTROM = 12_000.0  # STIS red limit ~10000 Å, generous


class MastStisHaspProfile:
    """
    FITS profile for MAST HASP co-added STIS spectra (_cspec.fits).

    Handles per-visit and per-visit-multi-grating co-adds (HLSP_LVL 1 and 2).
    """

    @property
    def profile_id(self) -> str:
        return "MAST_STIS_HASP"

    def matches(self, provider: str, hdulist: Any) -> bool:
        """
        Match MAST STIS HASP cspec files.

        Checks:
          - provider == "MAST"
          - INSTRUME == "STIS" (case-insensitive, whitespace-stripped)
          - ORIGIN contains "Space Telescope Science Institute"
        """
        if provider != "MAST":
            return False
        instrume = str(hdulist[0].header.get("INSTRUME", "")).strip().upper()
        if instrume != "STIS":
            return False
        origin = str(hdulist[0].header.get("ORIGIN", ""))
        return "Space Telescope Science Institute" in origin

    def validate(
        self,
        hdulist: Any,
        product_metadata: dict[str, Any],
    ) -> ProfileResult:
        """
        Validate and normalize a MAST STIS HASP FITS HDUList.

        Extracts WAVELENGTH and FLUX from the SCI BinTable (HDU[1]).
        Derives observation_time from PROVENANCE table (HDU[2]).
        Runs mandatory sanity checks.

        Returns ProfileResult(success=True) with a populated NormalizedSpectrum,
        or ProfileResult(success=False) with quarantine_reason_code on any
        deterministic failure.

        Transient I/O exceptions are NOT caught — they propagate to
        ValidateBytes which converts them to RetryableError.
        """
        data_product_id: str = product_metadata["data_product_id"]
        provider: str = product_metadata["provider"]
        notes: list[str] = []

        # ----------------------------------------------------------------
        # 1. Locate SCI extension
        # ----------------------------------------------------------------
        sci_hdu = _find_sci_hdu(hdulist)
        if sci_hdu is None:
            return ProfileResult(
                success=False,
                quarantine_reason=(
                    "No SCI BinTable HDU found. "
                    "Expected HDU named 'SCI' with WAVELENGTH and FLUX columns."
                ),
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=self.profile_id,
            )

        # ----------------------------------------------------------------
        # 2. Extract spectral and flux arrays
        # ----------------------------------------------------------------
        wave, flux, flux_units, extraction_notes = _extract_arrays(sci_hdu)
        notes.extend(extraction_notes)

        if wave is None or flux is None:
            return ProfileResult(
                success=False,
                quarantine_reason=("Could not extract WAVELENGTH or FLUX from SCI HDU."),
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=self.profile_id,
            )

        spectral_units = "Angstrom"

        # --- SNR extraction (best-effort) ---
        snr_value: float | None = None
        try:
            snr_col = np.asarray(sci_hdu.data["SNR"], dtype=np.float64).squeeze()
            finite_positive = snr_col[np.isfinite(snr_col) & (snr_col > 0)]
            if len(finite_positive) > 0:
                snr_value = float(np.median(finite_positive))
        except Exception:
            notes.append("SNR column not found or unreadable; SNR will be None.")

        # ----------------------------------------------------------------
        # 3. Extract observation metadata from PROVENANCE table
        # ----------------------------------------------------------------
        metadata_result = _extract_metadata(hdulist, notes)
        if not metadata_result["ok"]:
            return ProfileResult(
                success=False,
                quarantine_reason=metadata_result["reason"],
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=self.profile_id,
            )

        # ----------------------------------------------------------------
        # 4. Sanity checks
        # ----------------------------------------------------------------
        check_result = _run_sanity_checks(
            wave=wave,
            flux=flux,
            notes=notes,
        )
        if not check_result["ok"]:
            return ProfileResult(
                success=False,
                quarantine_reason=check_result["reason"],
                quarantine_reason_code="OTHER",
                normalization_notes=notes,
                profile_id=self.profile_id,
            )

        # ----------------------------------------------------------------
        # 5. Header signature hash
        # ----------------------------------------------------------------
        primary_header = hdulist[0].header
        sig_fields = "|".join(
            [
                provider,
                str(primary_header.get("INSTRUME", "")).strip(),
                str(primary_header.get("TELESCOP", "")).strip(),
                str(metadata_result["observation_mjd"]),
                str(primary_header.get("PROPOSID", "")),
                str(primary_header.get("FILENAME", "")),
            ]
        )
        header_signature_hash = hashlib.sha256(sig_fields.encode()).hexdigest()[:16]

        # ----------------------------------------------------------------
        # 6. Assemble NormalizedSpectrum
        # ----------------------------------------------------------------
        m = metadata_result
        spectrum = NormalizedSpectrum(
            spectral_axis=wave,
            flux_axis=flux,
            spectral_units=spectral_units,
            flux_units=flux_units,
            observation_time=m["observation_time"],
            observation_mjd=m["observation_mjd"],
            provider=provider,
            data_product_id=data_product_id,
            target_ra_deg=m["ra_deg"],
            target_dec_deg=m["dec_deg"],
            instrument=m["instrument"],
            telescope=m["telescope"],
            exposure_time_s=m["exposure_time_s"],
            spectral_resolution=m["spectral_resolution"],
            snr=snr_value,
            raw_header=dict(primary_header),
            normalization_notes=notes,
        )

        return ProfileResult(
            success=True,
            spectrum=spectrum,
            normalization_notes=notes,
            header_signature_hash=header_signature_hash,
            profile_id=self.profile_id,
        )


# ---------------------------------------------------------------------------
# HDU location
# ---------------------------------------------------------------------------


def _find_sci_hdu(hdulist: Any) -> Any | None:
    """
    Return the SCI BinTable HDU.

    Search order:
      1. HDU named 'SCI'
      2. First BinTableHDU with WAVELENGTH and FLUX columns
      3. None (caller quarantines)
    """
    from astropy.io.fits import BinTableHDU

    # Strategy 1: by name
    for hdu in hdulist:
        if hdu.name == "SCI" and isinstance(hdu, BinTableHDU):
            return hdu

    # Strategy 2: by column presence
    for hdu in hdulist:
        if isinstance(hdu, BinTableHDU) and hdu.columns is not None:
            col_names = [c.upper() for c in hdu.columns.names]
            if "WAVELENGTH" in col_names and "FLUX" in col_names:
                return hdu

    return None


# ---------------------------------------------------------------------------
# Array extraction
# ---------------------------------------------------------------------------


def _extract_arrays(
    sci_hdu: Any,
) -> tuple[Any | None, Any | None, str, list[str]]:
    """
    Extract WAVELENGTH and FLUX arrays from the SCI BinTable HDU.

    HASP cspec files have shape (1, N) — a single-row BinTable with
    variable-length array columns. Arrays are squeezed to 1D.

    Returns (wavelength, flux, flux_units, notes).
    wavelength and flux are None on failure.
    """
    notes: list[str] = []
    col_names = [c.upper() for c in sci_hdu.columns.names]

    if "WAVELENGTH" not in col_names:
        notes.append("WAVELENGTH column not found in SCI HDU.")
        return None, None, "", notes

    if "FLUX" not in col_names:
        notes.append("FLUX column not found in SCI HDU.")
        return None, None, "", notes

    # Read and flatten from (1, N) to (N,)
    wave = np.asarray(sci_hdu.data["WAVELENGTH"], dtype=np.float64).squeeze()
    flux = np.asarray(sci_hdu.data["FLUX"], dtype=np.float64).squeeze()

    if wave.ndim != 1:
        wave = wave.flatten()
        notes.append("WAVELENGTH array was not 1D after squeeze; flattened.")
    if flux.ndim != 1:
        flux = flux.flatten()
        notes.append("FLUX array was not 1D after squeeze; flattened.")

    if len(wave) != len(flux):
        notes.append(
            f"WAVELENGTH length ({len(wave)}) != FLUX length ({len(flux)}). "
            "Cannot produce aligned spectrum."
        )
        return None, None, "", notes

    if len(wave) == 0:
        notes.append("WAVELENGTH array is empty.")
        return None, None, "", notes

    # Flux unit from TUNIT of the FLUX column
    flux_col_idx = col_names.index("FLUX")
    tunit_key = f"TUNIT{flux_col_idx + 1}"
    flux_units = str(sci_hdu.header.get(tunit_key, "")).strip()
    if not flux_units:
        flux_units = "erg /s /cm**2 /Angstrom"
        notes.append(f"FLUX TUNIT ({tunit_key}) absent; assumed 'erg /s /cm**2 /Angstrom'.")

    return wave, flux, flux_units, notes


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------


def _extract_metadata(
    hdulist: Any,
    notes: list[str],
) -> dict[str, Any]:
    """
    Extract observation metadata from primary header and PROVENANCE table.

    Observation time is derived from min(MJD_BEG) in the PROVENANCE table
    (HDU[2]), NOT from the primary header (which lacks MJD-OBS/DATE-OBS
    with observation time for HASP products).

    Exposure time is sum(XPOSURE) from PROVENANCE.
    Spectral resolution is median(SPECRES) from PROVENANCE.

    Returns a dict with ok=True and populated fields, or ok=False with reason.
    """
    result: dict[str, Any] = {
        "ok": True,
        "reason": None,
        "observation_time": None,
        "observation_mjd": None,
        "ra_deg": None,
        "dec_deg": None,
        "instrument": None,
        "telescope": None,
        "exposure_time_s": None,
        "spectral_resolution": None,
    }

    primary_header = hdulist[0].header

    # --- Observation time from PROVENANCE (required) ---
    prov_hdu = _find_provenance_hdu(hdulist)
    if prov_hdu is None or prov_hdu.data is None or len(prov_hdu.data) == 0:
        result["ok"] = False
        result["reason"] = (
            "No PROVENANCE table found or it is empty. "
            "Cannot determine observation time for HASP cspec product."
        )
        return result

    prov_data = prov_hdu.data

    try:
        mjd_beg_values = np.asarray(prov_data["MJD_BEG"], dtype=np.float64)
        finite_mjd = mjd_beg_values[np.isfinite(mjd_beg_values)]
        if len(finite_mjd) == 0:
            result["ok"] = False
            result["reason"] = "All MJD_BEG values in PROVENANCE are NaN/Inf."
            return result
        observation_mjd = float(np.min(finite_mjd))
        result["observation_mjd"] = observation_mjd
        result["observation_time"] = _mjd_to_iso(observation_mjd)
    except Exception as exc:
        result["ok"] = False
        result["reason"] = f"Failed to extract MJD_BEG from PROVENANCE: {exc}"
        return result

    # --- Exposure time from PROVENANCE (optional) ---
    try:
        xposure_values = np.asarray(prov_data["XPOSURE"], dtype=np.float64)
        finite_xposure = xposure_values[np.isfinite(xposure_values)]
        if len(finite_xposure) > 0:
            result["exposure_time_s"] = float(np.sum(finite_xposure))
    except Exception:
        notes.append("XPOSURE column not found in PROVENANCE; exposure_time_s will be None.")

    # --- Spectral resolution from PROVENANCE (optional) ---
    try:
        specres_values = np.asarray(prov_data["SPECRES"], dtype=np.float64)
        finite_specres = specres_values[np.isfinite(specres_values) & (specres_values > 0)]
        if len(finite_specres) > 0:
            result["spectral_resolution"] = float(np.median(finite_specres))
    except Exception:
        notes.append("SPECRES column not found in PROVENANCE; spectral_resolution will be None.")

    # --- Standard metadata from primary header (optional) ---
    instrume = str(primary_header.get("INSTRUME", "")).strip() or None
    result["instrument"] = instrume

    telescop = str(primary_header.get("TELESCOP", "")).strip() or None
    result["telescope"] = telescop

    # RA/DEC — HASP uses TARG_RA / TARG_DEC (not RA_TARG / DEC_TARG)
    ra_targ = primary_header.get("TARG_RA")
    dec_targ = primary_header.get("TARG_DEC")
    if ra_targ is not None:
        with contextlib.suppress(TypeError, ValueError):
            result["ra_deg"] = float(ra_targ)
    if dec_targ is not None:
        with contextlib.suppress(TypeError, ValueError):
            result["dec_deg"] = float(dec_targ)

    return result


def _find_provenance_hdu(hdulist: Any) -> Any | None:
    """
    Return the PROVENANCE BinTable HDU.

    Search order:
      1. HDU named 'PROVENANCE'
      2. Third HDU (index 2) if it is a BinTableHDU with MJD_BEG column
      3. None
    """
    from astropy.io.fits import BinTableHDU

    for hdu in hdulist:
        if hdu.name == "PROVENANCE" and isinstance(hdu, BinTableHDU):
            return hdu

    if len(hdulist) > 2:
        hdu = hdulist[2]
        if isinstance(hdu, BinTableHDU) and hdu.columns is not None:
            col_names = [c.upper() for c in hdu.columns.names]
            if "MJD_BEG" in col_names:
                return hdu

    return None


# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------


def _run_sanity_checks(
    *,
    wave: Any,
    flux: Any,
    notes: list[str],
) -> dict[str, Any]:
    """
    Run mandatory sanity checks on extracted arrays.

    Checks:
      1. Wavelength monotonically increasing
      2. NaN/Inf fraction below threshold
      3. Wavelength within plausible range for STIS
      4. Flux array has at least some finite values

    Returns dict with ok=True or ok=False + reason.
    """
    result: dict[str, Any] = {"ok": True, "reason": None}

    # 1. Monotonicity
    finite_wave = wave[np.isfinite(wave)]
    if len(finite_wave) > 1:
        diffs = np.diff(finite_wave)
        if not np.all(diffs > 0):
            # Allow monotonically decreasing (reverse it)
            if np.all(diffs < 0):
                notes.append("Wavelength axis is monotonically decreasing; accepted.")
            else:
                result["ok"] = False
                result["reason"] = (
                    "Wavelength axis is not monotonic. "
                    f"Positive diffs: {np.sum(diffs > 0)}, "
                    f"negative diffs: {np.sum(diffs < 0)}, "
                    f"zero diffs: {np.sum(diffs == 0)}."
                )
                return result

    # 2. NaN/Inf fraction
    total = len(flux)
    bad_count = np.sum(~np.isfinite(flux))
    if total > 0:
        bad_frac = bad_count / total
        if bad_frac > _MAX_NAN_INF_FRACTION:
            result["ok"] = False
            result["reason"] = (
                f"NaN/Inf fraction in flux array is {bad_frac:.2%} "
                f"(threshold: {_MAX_NAN_INF_FRACTION:.0%}). "
                f"Bad pixels: {bad_count}/{total}."
            )
            return result

    # 3. Wavelength range
    if len(finite_wave) > 0:
        wl_min = float(np.min(finite_wave))
        wl_max = float(np.max(finite_wave))
        if wl_min < _WAVE_MIN_ANGSTROM or wl_max > _WAVE_MAX_ANGSTROM:
            result["ok"] = False
            result["reason"] = (
                f"Wavelength range [{wl_min:.1f}, {wl_max:.1f}] Å "
                f"outside plausible bounds [{_WAVE_MIN_ANGSTROM}, {_WAVE_MAX_ANGSTROM}] Å."
            )
            return result

    # 4. Flux has finite values
    finite_flux = flux[np.isfinite(flux)]
    if len(finite_flux) == 0:
        result["ok"] = False
        result["reason"] = "Flux array contains no finite values."
        return result

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mjd_to_iso(mjd: float) -> str:
    """
    Convert MJD to ISO-8601 UTC string.

    Uses the standard epoch: MJD 0 = 1858-11-17T00:00:00Z.
    """
    jd = mjd + 2_400_000.5
    # Astropy-free conversion: JD 2451545.0 = 2000-01-01T12:00:00Z
    delta_days = jd - 2_451_545.0
    epoch = datetime(2000, 1, 1, 12, 0, 0, tzinfo=UTC)
    from datetime import timedelta

    obs_dt = epoch + timedelta(days=delta_days)
    return obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
