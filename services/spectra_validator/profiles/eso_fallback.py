"""
profiles/eso_fallback.py — ESO fallback FITS profile.

Handles any ESO archive spectrum not claimed by a more specific profile
(UVES, X-Shooter, etc.). Designed to validate FEROS, FORS2-SPEC, HARPS,
ESPRESSO, and any future ESO instrument without requiring a dedicated profile.

Must be registered LAST in _PROFILE_REGISTRY — it accepts any ESO file and
acts as a catch-all after instrument-specific profiles have had first refusal.

FITS structure assumptions (common to ESO pipeline 1D spectra):
  HDU[0]: PrimaryHDU — NAXIS=0; observation metadata in header
  HDU[1]: BinTableHDU — 1 row, fixed-length array columns

  Required columns:
    WAVE    spectral axis   (unit read from TUNIT; see below)
    FLUX    flux values     (any unit; stored verbatim)

  Optional columns:
    ERR / SIGMA / NOISE     uncertainty
    QUAL / QUALITY          quality bitmask
    SNR                     signal-to-noise

  Required header keywords:
    INSTRUME    instrument name (used to construct dynamic profile_id)
    MJD-OBS or DATE-OBS     observation time

  Optional header keywords:
    TELESCOP, RA, DEC, EXPTIME, SPEC_RES / SPECRP

Wavelength unit detection:
  The fallback reads the TUNIT keyword of the WAVE column directly from the
  FITS BinTable column descriptor, which ESO pipeline products reliably populate.
  Confirmed for FEROS: unit = 'angstrom'.

  Supported units and their plausible wavelength bounds:
    angstrom / AA / Å   →  800 – 150 000 Å     (UV to mid-IR, generous)
    nm                  →  80  – 15 000 nm
    um / micron         →  0.08 – 15.0 μm

  If TUNIT is absent or unrecognised, a heuristic is applied:
    max(WAVE) > 100  → assumed angstrom  (adds normalization note)
    max(WAVE) ≤ 100  → assumed nm        (adds normalization note)

Dynamic profile_id:
  The ProfileResult.profile_id is set to f"ESO_{INSTRUME}" (e.g. "ESO_FEROS",
  "ESO_FORS2") so operators can tell from the DataProduct which instrument
  was matched. Falls back to "ESO_FALLBACK" if INSTRUME is absent.

Flux calibration:
  No FLUXCAL requirement — instruments like FEROS produce flux in ADU rather
  than absolute units. The flux unit string is stored verbatim from TUNIT.
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

# Plausible wavelength bounds per unit
_WAVE_BOUNDS: dict[str, tuple[float, float]] = {
    "angstrom": (800.0, 150_000.0),
    "nm": (80.0, 15_000.0),
    "um": (0.08, 15.0),
}

# Unit string normalisation map (lowercase input → canonical key in _WAVE_BOUNDS)
_UNIT_ALIASES: dict[str, str] = {
    "angstrom": "angstrom",
    "angstroms": "angstrom",
    "aa": "angstrom",
    "å": "angstrom",
    "nm": "nm",
    "nanometer": "nm",
    "nanometers": "nm",
    "nanometre": "nm",
    "nanometres": "nm",
    "um": "um",
    "micron": "um",
    "microns": "um",
    "micrometer": "um",
    "micrometers": "um",
    "micrometre": "um",
    "micrometres": "um",
}

# Column name aliases — same priority order as UVES profile
_WAVE_ALIASES = ["WAVE", "WAVELENGTH", "LAMBDA", "WAVE_AIR", "WAVE_VAC"]
_FLUX_ALIASES = ["FLUX", "F_LAMBDA", "FLUX_REDUCED", "SPEC", "FLUXCAL"]


class EsoFallbackProfile:
    """
    Catch-all FITS profile for ESO archive spectra without a dedicated profile.

    Registered last in _PROFILE_REGISTRY; matches any ESO file not claimed
    by EsoUvesProfile, EsoXShooterProfile, or any other specific profile.
    """

    @property
    def profile_id(self) -> str:
        # Stable class-level identifier. The per-result profile_id is set
        # dynamically to ESO_{INSTRUME} inside validate().
        return "ESO_FALLBACK"

    def matches(self, provider: str, hdulist: Any) -> bool:
        """
        Match any ESO file not already claimed by a more specific profile.
        Registry order ensures this is only reached for non-UVES, non-XSHOOTER files.
        """
        return provider == "ESO"

    def validate(
        self,
        hdulist: Any,
        product_metadata: dict[str, Any],
    ) -> ProfileResult:
        """
        Validate and normalize an ESO FITS HDUList using instrument-agnostic logic.

        Reads wavelength units from the WAVE column's TUNIT keyword.
        Constructs a dynamic profile_id from INSTRUME (e.g. "ESO_FEROS").

        Returns ProfileResult(success=True) with a populated NormalizedSpectrum,
        or ProfileResult(success=False) with a quarantine_reason_code on any
        deterministic failure.

        Transient I/O exceptions propagate uncaught to ValidateBytes.
        """
        data_product_id: str = product_metadata["data_product_id"]
        provider: str = product_metadata["provider"]
        notes: list[str] = []

        primary_header = dict(hdulist[0].header)

        # Dynamic profile_id from INSTRUME — "ESO_FEROS", "ESO_FORS2", etc.
        instrume_raw = str(primary_header.get("INSTRUME", "")).strip()
        instrume = instrume_raw.upper().replace("-", "").replace(" ", "")
        result_profile_id = f"ESO_{instrume}" if instrume else "ESO_FALLBACK"

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
                profile_id=result_profile_id,
            )

        # ----------------------------------------------------------------
        # 2. Extract spectral and flux arrays + detect wavelength units
        # ----------------------------------------------------------------
        wave, flux, spectral_units, flux_units, extraction_notes = _extract_arrays(
            spectrum_hdu, notes
        )
        notes.extend(extraction_notes)

        if wave is None:
            return ProfileResult(
                success=False,
                quarantine_reason=(
                    "Could not locate WAVE column in SPECTRUM HDU. "
                    "Checked aliases: WAVE, WAVELENGTH, LAMBDA, WAVE_AIR, WAVE_VAC."
                ),
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=result_profile_id,
            )
        if flux is None:
            return ProfileResult(
                success=False,
                quarantine_reason=(
                    "Could not locate flux column in SPECTRUM HDU. "
                    "Checked aliases: FLUX, F_LAMBDA, FLUX_REDUCED, SPEC, FLUXCAL."
                ),
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=result_profile_id,
            )

        # ----------------------------------------------------------------
        # 3. Extract required header metadata
        # ----------------------------------------------------------------
        metadata_result = _extract_metadata(primary_header, notes)
        if not metadata_result["ok"]:
            return ProfileResult(
                success=False,
                quarantine_reason=metadata_result["reason"],
                quarantine_reason_code="MISSING_CRITICAL_METADATA",
                normalization_notes=notes,
                profile_id=result_profile_id,
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
                profile_id=result_profile_id,
            )

        # ----------------------------------------------------------------
        # 5. Header signature hash
        # ----------------------------------------------------------------
        sig_fields = "|".join(
            [
                provider,
                instrume_raw,
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
            instrument=instrume_raw or None,
            telescope=m["telescope"],
            exposure_time_s=m["exposure_time_s"],
            spectral_resolution=m["spectral_resolution"],
            raw_header=primary_header,
            normalization_notes=notes,
        )

        return ProfileResult(
            success=True,
            spectrum=spectrum,
            normalization_notes=notes,
            header_signature_hash=header_signature_hash,
            profile_id=result_profile_id,
        )


# ---------------------------------------------------------------------------
# HDU location (identical to eso_uves._find_spectrum_hdu)
# ---------------------------------------------------------------------------


def _find_spectrum_hdu(hdulist: Any) -> Any | None:
    """
    Return the SPECTRUM BinTable HDU.

    Search order:
      1. HDU named 'SPECTRUM'
      2. First BinTableHDU with a WAVE or WAVELENGTH column
      3. BinTableHDU at index 1 (positional fallback)
    """
    import astropy.io.fits as fits

    for hdu in hdulist:
        if isinstance(hdu, fits.BinTableHDU) and hdu.name.strip().upper() == "SPECTRUM":
            return hdu

    for hdu in hdulist:
        if isinstance(hdu, fits.BinTableHDU) and hdu.columns:
            col_names = {c.name.upper() for c in hdu.columns}
            if col_names & {"WAVE", "WAVELENGTH", "LAMBDA"}:
                return hdu

    if len(hdulist) > 1 and isinstance(hdulist[1], fits.BinTableHDU):
        return hdulist[1]

    return None


# ---------------------------------------------------------------------------
# Unit detection
# ---------------------------------------------------------------------------


def _detect_wave_units(
    wave_column: Any,
    wave_array: np.ndarray,
    notes: list[str],
) -> str:
    """
    Determine the wavelength unit from the FITS column descriptor (TUNIT).

    If TUNIT is present and recognised, returns the canonical unit string
    ("angstrom", "nm", "um").

    If TUNIT is absent or unrecognised, applies a heuristic:
      max(WAVE) > 100  → "angstrom"
      max(WAVE) ≤ 100  → "nm"
    Adds a normalization note in either case.

    Returns the canonical unit string.
    """
    raw_unit = (getattr(wave_column, "unit", None) or "").strip()
    canonical = _UNIT_ALIASES.get(raw_unit.lower())

    if canonical:
        return canonical

    if raw_unit:
        notes.append(
            f"Unrecognised WAVE column unit {raw_unit!r}; "
            "falling back to heuristic range detection."
        )
    else:
        notes.append("WAVE column has no TUNIT; falling back to heuristic range detection.")

    # Heuristic: optical/UV spectra in angstrom are typically > 1000;
    # nm spectra are typically < 100 for the same wavelength range.
    finite = wave_array[np.isfinite(wave_array)]
    if len(finite) > 0 and float(np.max(finite)) > 100.0:
        notes.append("Heuristic: wavelength values > 100 — assumed angstrom.")
        return "angstrom"
    else:
        notes.append("Heuristic: wavelength values ≤ 100 — assumed nm.")
        return "nm"


# ---------------------------------------------------------------------------
# Array extraction
# ---------------------------------------------------------------------------


def _extract_arrays(
    spectrum_hdu: Any,
    notes: list[str],
) -> tuple[np.ndarray | None, np.ndarray | None, str, str, list[str]]:
    """
    Extract wave and flux arrays from the SPECTRUM BinTable.

    Reads wavelength units from the WAVE column's TUNIT descriptor.

    Returns: (wave, flux, spectral_units, flux_units, extraction_notes)
    Arrays are 1D numpy float64.
    """
    extraction_notes: list[str] = []
    col_map = {c.name.upper(): c for c in spectrum_hdu.columns}

    # --- Locate WAVE column ---
    wave_col_obj = None
    wave_col_name = None
    for alias in _WAVE_ALIASES:
        if alias in col_map:
            wave_col_obj = col_map[alias]
            wave_col_name = col_map[alias].name
            if alias != "WAVE":
                extraction_notes.append(
                    f"Spectral axis found in column {alias!r} (alias for WAVE)."
                )
            break

    if wave_col_obj is None:
        return None, None, "", "", extraction_notes

    # --- Locate FLUX column ---
    flux_col_name = None
    flux_units = ""
    for alias in _FLUX_ALIASES:
        if alias in col_map:
            flux_col_obj = col_map[alias]
            flux_col_name = flux_col_obj.name
            flux_units = getattr(flux_col_obj, "unit", "") or ""
            if alias != "FLUX":
                extraction_notes.append(f"Flux found in column {alias!r} (alias for FLUX).")
            break

    if flux_col_name is None:
        return None, None, "", "", extraction_notes

    # --- Read and flatten ---
    wave_data = spectrum_hdu.data[wave_col_name]
    flux_data = spectrum_hdu.data[flux_col_name]

    wave = np.asarray(wave_data, dtype=np.float64).squeeze()
    flux = np.asarray(flux_data, dtype=np.float64).squeeze()

    if wave.ndim != 1:
        wave = wave.flatten()
        extraction_notes.append("Wave array was not 1D; flattened.")
    if flux.ndim != 1:
        flux = flux.flatten()
        extraction_notes.append("Flux array was not 1D; flattened.")

    if len(wave) != len(flux):
        extraction_notes.append(
            f"Wave length ({len(wave)}) != flux length ({len(flux)}). "
            "Cannot produce aligned spectrum."
        )
        return None, None, "", "", extraction_notes

    # --- Detect wavelength units from TUNIT ---
    spectral_units = _detect_wave_units(wave_col_obj, wave, notes)

    return wave, flux, spectral_units, flux_units, extraction_notes


# ---------------------------------------------------------------------------
# Metadata extraction (identical contract to eso_uves._extract_metadata)
# ---------------------------------------------------------------------------


def _extract_metadata(header: dict[str, Any], notes: list[str]) -> dict[str, Any]:
    """
    Extract required and optional metadata from the primary header.

    Required: observation_time (MJD-OBS or DATE-OBS).
    Optional: RA, DEC, TELESCOP, EXPTIME, SPEC_RES / SPECRP.
    """
    result: dict[str, Any] = {
        "ok": True,
        "reason": None,
        "observation_time": None,
        "observation_mjd": None,
        "ra_deg": None,
        "dec_deg": None,
        "telescope": None,
        "exposure_time_s": None,
        "spectral_resolution": None,
    }

    # --- observation_time (required) ---
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
            date_str = str(date_obs).strip()
            if "T" not in date_str:
                date_str += "T00:00:00Z"
                notes.append(
                    "DATE-OBS was date-only; appended T00:00:00Z. Time precision is day-level only."
                )
            elif not date_str.endswith("Z"):
                date_str += "Z"
            result["observation_time"] = date_str
        else:
            result["ok"] = False
            result["reason"] = (
                "Cannot determine observation_time: neither MJD-OBS nor DATE-OBS "
                "is present in the primary header."
            )
            return result

    # --- RA / DEC (optional) ---
    for key, field in (("RA", "ra_deg"), ("DEC", "dec_deg")):
        val = header.get(key)
        if val is not None:
            try:
                result[field] = float(val)
            except (TypeError, ValueError):
                notes.append(f"{key} header value {val!r} could not be parsed as float.")

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

    # --- SPEC_RES / SPECRP (spectral resolving power) ---
    for key in ("SPEC_RES", "SPECRP"):
        val = header.get(key)
        if val is not None:
            try:
                result["spectral_resolution"] = float(val)
                break
            except (TypeError, ValueError):
                notes.append(f"{key} header value {val!r} could not be parsed as float.")

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

    Wavelength range bounds are looked up from _WAVE_BOUNDS using the
    detected spectral_units. Falls back to the angstrom bounds if the
    unit is not in _WAVE_BOUNDS (should not happen in practice).
    """
    # Check 1: non-empty arrays
    if len(flux) == 0:
        return {"ok": False, "reason": "Flux array is empty."}
    if len(wave) == 0:
        return {"ok": False, "reason": "Spectral axis array is empty."}

    # Check 2: NaN/Inf fraction
    bad_fraction = float(np.sum(~np.isfinite(flux))) / len(flux)
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

    # Check 3: strict monotonicity
    finite_wave = wave[np.isfinite(wave)]
    if len(finite_wave) < 2:
        return {
            "ok": False,
            "reason": "Spectral axis has fewer than 2 finite values.",
        }
    diffs = np.diff(finite_wave)
    is_ascending = bool(np.all(diffs > 0))
    is_descending = bool(np.all(diffs < 0))
    if not (is_ascending or is_descending):
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

    # Check 4: plausible wavelength range for detected units
    wave_min_bound, wave_max_bound = _WAVE_BOUNDS.get(spectral_units, _WAVE_BOUNDS["angstrom"])
    wave_min = float(np.nanmin(wave))
    wave_max = float(np.nanmax(wave))
    if wave_max < wave_min_bound or wave_min > wave_max_bound:
        return {
            "ok": False,
            "reason": (
                f"Spectral axis range [{wave_min:.4g}, {wave_max:.4g}] {spectral_units} "
                f"is outside the plausible range "
                f"[{wave_min_bound:.4g}, {wave_max_bound:.4g}] {spectral_units}."
            ),
        }

    # Check 5: not all-zero flux
    finite_flux = flux[np.isfinite(flux)]
    if len(finite_flux) > 0 and float(np.sum(finite_flux != 0)) / len(flux) == 0.0:
        return {
            "ok": False,
            "reason": "Flux array is entirely zero. Likely indicates a failed extraction.",
        }

    return {"ok": True, "reason": None}


# ---------------------------------------------------------------------------
# MJD → ISO-8601 UTC helper
# ---------------------------------------------------------------------------

_MJD_EPOCH = datetime(1858, 11, 17, 0, 0, 0, tzinfo=UTC)
_SECONDS_PER_DAY = 86400.0


def _mjd_to_iso(mjd: float) -> str:
    if not math.isfinite(mjd):
        raise ValueError(f"MJD value is not finite: {mjd}")
    dt = datetime.fromtimestamp(
        _MJD_EPOCH.timestamp() + mjd * _SECONDS_PER_DAY,
        tz=UTC,
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
