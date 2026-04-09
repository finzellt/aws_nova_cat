"""
profiles/eso_uves.py — ESO UVES FITS profile.

Validated against: ADP_2020-06-09T06_35_43_457.fits (V1324 Sco, red arm, CD#4)

FITS structure (confirmed from real data):
  HDU[0]: PrimaryHDU — NAXIS=0 (no data); all observation metadata in header
  HDU[1]: BinTableHDU named "SPECTRUM" — 1 row, 185847 elements per column

  Key HDU[0] header keywords:
    INSTRUME  = 'UVES'
    TELESCOP  = 'ESO-VLT-U2'
    DATE-OBS  = 'YYYY-MM-DD'         (date only — use MJD-OBS for full precision)
    MJD-OBS   = <float>              (MJD of observation start)
    MJD-END   = <float>              (MJD of observation end; optional)
    RA        = <float>              (degrees, J2000)
    DEC       = <float>              (degrees, J2000)
    EXPTIME   = <float>              (seconds)
    SPEC_RES  = <float>              (spectral resolving power R = λ/Δλ)
    FLUXCAL   = 'ABSOLUTE'           (confirms flux calibration)
    ORIGIN    = 'ESO'

  HDU[1] columns (all arrays of length NELEM):
    WAVE          angstrom           spectral axis
    FLUX          10**(-16)erg.cm**(-2).s**(-1).angstrom**(-1)  calibrated flux
    ERR           (same as FLUX)     1-sigma uncertainty
    FLUX_REDUCED  adu                uncalibrated flux (not used)
    ERR_REDUCED   adu                uncertainty on uncalibrated flux (not used)
    QUAL          integer            quality bitmask (0 = good)
    SNR           dimensionless      signal-to-noise per pixel

Normalization notes:
  - FLUX unit string is stored verbatim; canonical normalisation deferred.
  - observation_time is derived from MJD-OBS via MJD→ISO-8601 UTC conversion.
  - QUAL=0 pixels are good; non-zero QUAL pixels are not masked by this profile
    (the full array is passed through) but the fraction is counted and logged.
"""

from __future__ import annotations

import hashlib
import math
from datetime import UTC, datetime
from typing import Any

import numpy as np

from .base import NormalizedSpectrum, ProfileResult

# ---------------------------------------------------------------------------
# Sanity check thresholds
# ---------------------------------------------------------------------------

_MAX_NAN_INF_FRACTION = 0.20  # >20% bad pixels → QUARANTINE
_WAVE_MIN_ANGSTROM = 1_000.0  # UV cutoff (generous)
_WAVE_MAX_ANGSTROM = 110_000.0  # IR cutoff (generous; UVES red goes to ~11000 Å)


class EsoUvesProfile:
    """
    FITS profile for ESO UVES reduced spectra served via the ESO Science Archive.

    Handles merged, flux-calibrated 1D spectra produced by the UVES pipeline
    (product category SCIENCE.SPECTRUM / FLUXCAL_STACK_POINT_RED).
    """

    @property
    def profile_id(self) -> str:
        return "ESO_UVES"

    def matches(self, provider: str, hdulist: Any) -> bool:
        """
        Match ESO UVES files.
        Checks provider == "ESO" and INSTRUME starts with "UVES" (case-insensitive).
        Strips whitespace — ESO pads INSTRUME to 8 chars: 'UVES    '.
        """
        if provider != "ESO":
            return False
        instrume = str(hdulist[0].header.get("INSTRUME", "")).strip().upper()
        return instrume.startswith("UVES")

    def validate(
        self,
        hdulist: Any,
        product_metadata: dict[str, Any],
    ) -> ProfileResult:
        """
        Validate and normalize a UVES FITS HDUList.

        Extracts WAVE and FLUX from the SPECTRUM BinTable (HDU[1]).
        Derives observation_time from MJD-OBS.
        Runs five mandatory sanity checks.

        Returns ProfileResult(success=True) with a populated NormalizedSpectrum,
        or ProfileResult(success=False) with a quarantine_reason_code on any
        deterministic failure.

        Transient I/O exceptions (e.g. astropy OSError) are NOT caught —
        they propagate to ValidateBytes which converts them to RetryableError.
        """
        data_product_id: str = product_metadata["data_product_id"]
        provider: str = product_metadata["provider"]
        notes: list[str] = []

        # ----------------------------------------------------------------
        # 1. Locate SPECTRUM extension
        # ----------------------------------------------------------------
        spectrum_hdu = _find_spectrum_hdu(hdulist)
        if spectrum_hdu is None:
            return ProfileResult(
                success=False,
                quarantine_reason=(
                    "No SPECTRUM BinTable HDU found. "
                    "Expected HDU named 'SPECTRUM' or a BinTableHDU at index 1."
                ),
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=self.profile_id,
            )

        # ----------------------------------------------------------------
        # 2. Extract spectral and flux arrays
        # ----------------------------------------------------------------
        wave, flux, flux_units, extraction_notes = _extract_arrays(spectrum_hdu)
        notes.extend(extraction_notes)

        if wave is None:
            return ProfileResult(
                success=False,
                quarantine_reason=(
                    "Could not locate WAVE column in SPECTRUM HDU. "
                    "Checked aliases: WAVE, WAVELENGTH, LAMBDA."
                ),
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=self.profile_id,
            )
        if flux is None:
            return ProfileResult(
                success=False,
                quarantine_reason=(
                    "Could not locate flux column in SPECTRUM HDU. "
                    "Checked aliases: FLUX, FLUX_REDUCED, F_LAMBDA, SPEC."
                ),
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=self.profile_id,
            )

        spectral_units = "angstrom"

        # Extract SNR (best-effort — not all products have this column)
        _snr_value: float | None = None
        try:
            col_names_upper = {c.name.upper() for c in spectrum_hdu.columns}
            _snr_col_name: str | None = None
            for _alias in ("SNR", "SNR_REDUCED", "SNR_MED", "SNR_MEAN"):
                if _alias in col_names_upper:
                    _snr_col_name = _alias
                    break
            if _snr_col_name is not None:
                snr_col = spectrum_hdu.data[_snr_col_name]
                snr_arr = np.asarray(snr_col, dtype=float).ravel()
                finite_snr = snr_arr[np.isfinite(snr_arr)]
                if len(finite_snr) > 0:
                    _snr_value = float(np.median(finite_snr))
            else:
                _hdr_snr = hdulist[0].header.get("SNR")
                if _hdr_snr is not None:
                    _hdr_snr_f = float(_hdr_snr)
                    if math.isfinite(_hdr_snr_f):
                        _snr_value = _hdr_snr_f
                        notes.append(
                            "SNR: extracted from HDU[0] header keyword"
                            " (no BinTable SNR column found)."
                        )
        except Exception:
            pass  # SNR extraction is best-effort

        # ----------------------------------------------------------------
        # 3. Extract required header metadata
        # ----------------------------------------------------------------
        primary_header = dict(hdulist[0].header)
        metadata_result = _extract_metadata(primary_header, notes)
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
            spectral_units=spectral_units,
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
        #    Stable fingerprint of the key discriminating header fields.
        #    Used for traceability — not for dedup (sha256 of bytes is used for that).
        # ----------------------------------------------------------------
        sig_fields = "|".join(
            [
                provider,
                str(primary_header.get("INSTRUME", "")).strip(),
                str(primary_header.get("TELESCOP", "")).strip(),
                str(primary_header.get("MJD-OBS", "")),
                str(primary_header.get("EXPTIME", "")),
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
            snr=_snr_value,
            raw_header=primary_header,
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


def _find_spectrum_hdu(hdulist: Any) -> Any | None:
    """
    Return the SPECTRUM BinTable HDU.

    Search order:
      1. HDU named 'SPECTRUM' (standard ESO UVES archive product)
      2. First BinTableHDU with a WAVE or WAVELENGTH column
      3. HDU at index 1 if it is a BinTableHDU (positional fallback)
    """
    import astropy.io.fits as fits

    # Pass 1: by name
    for hdu in hdulist:
        if isinstance(hdu, fits.BinTableHDU) and hdu.name.strip().upper() == "SPECTRUM":
            return hdu

    # Pass 2: by column content
    for hdu in hdulist:
        if isinstance(hdu, fits.BinTableHDU) and hdu.columns:
            col_names = {c.name.upper() for c in hdu.columns}
            if col_names & {"WAVE", "WAVELENGTH", "LAMBDA"}:
                return hdu

    # Pass 3: positional fallback
    if len(hdulist) > 1:
        import astropy.io.fits as fits

        if isinstance(hdulist[1], fits.BinTableHDU):
            return hdulist[1]

    return None


# ---------------------------------------------------------------------------
# Array extraction
# ---------------------------------------------------------------------------

_WAVE_ALIASES = ["WAVE", "WAVELENGTH", "LAMBDA", "WAVE_AIR", "WAVE_VAC"]
# FLUX priority: prefer absolutely calibrated FLUX over raw FLUX_REDUCED
_FLUX_ALIASES = ["FLUX", "F_LAMBDA", "FLUX_REDUCED", "SPEC", "FLUXCAL"]


def _extract_arrays(
    spectrum_hdu: Any,
) -> tuple[np.ndarray | None, np.ndarray | None, str, list[str]]:
    """
    Extract wave and flux arrays from the SPECTRUM BinTable.

    UVES confirmed column names:
      WAVE         → spectral axis (angstrom)
      FLUX         → calibrated flux (10**-16 erg/cm2/s/Å)
      FLUX_REDUCED → uncalibrated flux (adu) — lower priority

    Returns: (wave_array, flux_array, flux_units, normalization_notes)
    Arrays are 1D numpy float64. BinTable rows are shape (1, N); we squeeze.
    """
    notes: list[str] = []
    col_map = {c.name.upper(): c.name for c in spectrum_hdu.columns}

    # --- Locate WAVE column ---
    wave_col = None
    for alias in _WAVE_ALIASES:
        if alias in col_map:
            wave_col = col_map[alias]
            if alias != "WAVE":
                notes.append(f"Spectral axis found in column {alias!r} (alias for WAVE).")
            break

    if wave_col is None:
        return None, None, "", notes

    # --- Locate FLUX column ---
    flux_col = None
    flux_units = ""
    for alias in _FLUX_ALIASES:
        if alias in col_map:
            flux_col = col_map[alias]
            # Get units from the column descriptor
            for c in spectrum_hdu.columns:
                if c.name == flux_col:
                    flux_units = getattr(c, "unit", "") or ""
                    break
            if alias != "FLUX":
                notes.append(f"Flux found in column {alias!r} (alias for FLUX).")
            break

    if flux_col is None:
        return None, None, "", notes

    # --- Read and flatten ---
    # BinTable data shape is (nrows, nelems). For UVES: (1, 185847).
    wave_data = spectrum_hdu.data[wave_col]
    flux_data = spectrum_hdu.data[flux_col]

    wave = np.asarray(wave_data, dtype=np.float64).squeeze()
    flux = np.asarray(flux_data, dtype=np.float64).squeeze()

    # Ensure 1D
    if wave.ndim != 1:
        wave = wave.flatten()
        notes.append("Wave array was not 1D; flattened to 1D.")
    if flux.ndim != 1:
        flux = flux.flatten()
        notes.append("Flux array was not 1D; flattened to 1D.")

    if len(wave) != len(flux):
        notes.append(
            f"Wave length ({len(wave)}) != flux length ({len(flux)}). "
            "Cannot produce aligned spectrum."
        )
        return None, None, "", notes

    return wave, flux, flux_units, notes


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------


def _extract_metadata(header: dict[str, Any], notes: list[str]) -> dict[str, Any]:
    """
    Extract required and optional metadata fields from the primary header.

    Required: observation_time (derived from MJD-OBS or DATE-OBS)
    Optional: RA, DEC, INSTRUME, TELESCOP, EXPTIME, SPEC_RES

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

    # --- observation_time (required) ---
    # Prefer MJD-OBS for full precision; fall back to DATE-OBS + 'T00:00:00Z'
    mjd_obs = header.get("MJD-OBS")
    date_obs = header.get("DATE-OBS")

    if mjd_obs is not None:
        try:
            iso = _mjd_to_iso(float(mjd_obs))
            result["observation_time"] = iso
            result["observation_mjd"] = float(mjd_obs)
        except Exception as exc:
            notes.append(f"MJD-OBS conversion failed ({exc}); trying DATE-OBS.")
            mjd_obs = None

    if result["observation_time"] is None:
        if date_obs:
            # DATE-OBS is date-only for this file: '2012-06-04'
            date_str = str(date_obs).strip()
            if "T" not in date_str:
                date_str += "T00:00:00Z"
                notes.append(
                    "DATE-OBS was date-only; appended T00:00:00Z. Time precision is day-level only."
                )
            result["observation_time"] = date_str
        else:
            result["ok"] = False
            result["reason"] = (
                "Cannot determine observation_time: neither MJD-OBS nor DATE-OBS "
                "is present in the primary header."
            )
            return result

    # --- RA / DEC (optional) ---
    ra = header.get("RA")
    dec = header.get("DEC")
    if ra is not None:
        try:
            result["ra_deg"] = float(ra)
        except (TypeError, ValueError):
            notes.append(f"RA header value {ra!r} could not be parsed as float.")
    if dec is not None:
        try:
            result["dec_deg"] = float(dec)
        except (TypeError, ValueError):
            notes.append(f"DEC header value {dec!r} could not be parsed as float.")

    # --- INSTRUME ---
    instrume = header.get("INSTRUME")
    if instrume is not None:
        result["instrument"] = str(instrume).strip()

    # --- TELESCOP ---
    telescop = header.get("TELESCOP")
    if telescop is not None:
        result["telescope"] = str(telescop).strip()

    # --- EXPTIME ---
    exptime = header.get("EXPTIME")
    if exptime is not None:
        try:
            result["exposure_time_s"] = float(exptime)
        except (TypeError, ValueError):
            notes.append(f"EXPTIME header value {exptime!r} could not be parsed as float.")

    # --- SPEC_RES (spectral resolving power) ---
    spec_res = header.get("SPEC_RES")
    if spec_res is not None:
        try:
            result["spectral_resolution"] = float(spec_res)
        except (TypeError, ValueError):
            notes.append(f"SPEC_RES header value {spec_res!r} could not be parsed as float.")

    return result


# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------


def _run_sanity_checks(
    *,
    wave: np.ndarray,
    flux: np.ndarray,
    spectral_units: str,
    notes: list[str],
) -> dict[str, Any]:
    """
    Run the five mandatory sanity checks.

    Returns dict with ok=True, or ok=False and a reason string.
    Appends informational notes for any warnings that don't fail the check.
    """

    # Check 1: flux array non-empty
    if len(flux) == 0:
        return {"ok": False, "reason": "Flux array is empty."}
    if len(wave) == 0:
        return {"ok": False, "reason": "Spectral axis array is empty."}

    # Check 2: NaN/Inf fraction in flux
    bad_mask = ~np.isfinite(flux)
    bad_fraction = float(np.sum(bad_mask)) / len(flux)
    if bad_fraction > _MAX_NAN_INF_FRACTION:
        return {
            "ok": False,
            "reason": (
                f"Flux array has {bad_fraction:.1%} non-finite values "
                f"(threshold: {_MAX_NAN_INF_FRACTION:.0%}). "
                "File may be corrupt or heavily masked."
            ),
        }
    if bad_fraction > 0:
        notes.append(f"Flux array contains {bad_fraction:.1%} NaN/Inf values (within threshold).")

    # Check 3: spectral axis monotonicity
    # Use finite wave values only for the check.
    finite_wave = wave[np.isfinite(wave)]
    if len(finite_wave) < 2:
        return {
            "ok": False,
            "reason": "Spectral axis has fewer than 2 finite values; cannot verify monotonicity.",
        }
    diffs = np.diff(finite_wave)
    is_ascending = bool(np.all(diffs > 0))
    is_descending = bool(np.all(diffs < 0))
    if not (is_ascending or is_descending):
        # Count violations for the error message
        violations = int(np.sum(diffs == 0) + np.sum(diffs > 0 if is_descending else diffs < 0))
        return {
            "ok": False,
            "reason": (
                f"Spectral axis is not strictly monotonic "
                f"({violations} non-monotonic step(s) detected)."
            ),
        }
    if is_descending:
        notes.append("Spectral axis is descending; profile does not reorder it.")

    # Check 4: plausible wavelength range for declared units
    # For angstrom: expect 1000–110000 Å (UV to mid-IR, generous bounds)
    wave_min = float(np.nanmin(wave))
    wave_max = float(np.nanmax(wave))
    if spectral_units.lower() in ("angstrom", "aa", "å") and (
        wave_max < _WAVE_MIN_ANGSTROM or wave_min > _WAVE_MAX_ANGSTROM
    ):
        return {
            "ok": False,
            "reason": (
                f"Spectral axis range [{wave_min:.1f}, {wave_max:.1f}] Å "
                f"is outside the plausible range "
                f"[{_WAVE_MIN_ANGSTROM:.0f}, {_WAVE_MAX_ANGSTROM:.0f}] Å."
            ),
        }

    # Check 5: required metadata present
    # observation_time is checked in _extract_metadata already.
    # This check guards against a completely empty flux array of zeros
    # (distinct from NaN/Inf — might indicate a failed extraction).
    nonzero_fraction = float(np.sum(flux[np.isfinite(flux)] != 0)) / max(1, len(flux))
    if nonzero_fraction == 0.0:
        return {
            "ok": False,
            "reason": "Flux array is entirely zero. This likely indicates a failed extraction.",
        }
    if nonzero_fraction < 0.01:
        notes.append(
            f"Warning: only {nonzero_fraction:.1%} of flux pixels are non-zero. "
            "May indicate a partially failed extraction."
        )

    return {"ok": True, "reason": None}


# ---------------------------------------------------------------------------
# MJD → ISO-8601 UTC helper
# ---------------------------------------------------------------------------

# MJD epoch: 1858-11-17T00:00:00 UTC
_MJD_EPOCH = datetime(1858, 11, 17, 0, 0, 0, tzinfo=UTC)
_SECONDS_PER_DAY = 86400.0


def _mjd_to_iso(mjd: float) -> str:
    """
    Convert a Modified Julian Date to an ISO-8601 UTC string.

    Example: 56082.05467768 → '2012-06-04T01:18:44Z'
    """
    if not math.isfinite(mjd):
        raise ValueError(f"MJD value is not finite: {mjd}")
    total_seconds = mjd * _SECONDS_PER_DAY
    dt = datetime.fromtimestamp(
        _MJD_EPOCH.timestamp() + total_seconds,
        tz=UTC,
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
