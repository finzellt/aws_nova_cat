# services/photometry_ingestor/adapters/canonical_csv.py
"""
CanonicalCsvAdapter — concrete MVP implementation of the PhotometryAdapter protocol.

Handles Tier 1 (canonical column headers) and Tier 2 (synonym registry) column
mapping per ADR-015, Decision 2.  Band and filter value resolution follows
ADR-016.

Public surface:
    CanonicalCsvAdapter   — the adapter class
    MissingRequiredColumnsError — raised on file-level structural failures
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from uuid import UUID

from photometry_ingestor.adapters.base import (
    AdaptationFailure,
    AdaptationResult,
)
from pydantic import ValidationError

from contracts.models.entities import PhotometryRow

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_ADAPTER_DIR = Path(__file__).parent

# Identity fields injected by the workflow; never sourced from the CSV.
_IDENTITY_FIELDS: frozenset[str] = frozenset({"nova_id", "primary_name", "ra_deg", "dec_deg"})

# Required fields that must be resolvable from the source file.
# Identity fields are excluded (injected by workflow).
# phot_system is excluded: _resolve_band() may derive it from filter_name.
# spectral_coord_type and spectral_coord_unit are excluded: inferred from
#   phot_system by _infer_spectral_coord_meta() when absent.
# time_mjd is excluded: derived from time_orig (JD/HJD/BJD) by _resolve_time()
#   when absent.
# spectral_coord_value IS required: cannot be inferred without a filter
#   library (ADR-016, Decision 6).  Must be present in the source file.
_REQUIRED_SOURCE_FIELDS: frozenset[str] = frozenset(
    {
        "filter_name",
        "spectral_coord_value",
    }
)

# Sentinel strings normalised to None before column resolution.
# Matching is case-insensitive against the stripped value.
_SENTINEL_VALUES: frozenset[str] = frozenset({"", "n/a", "na", "null", "none", "nan", "--", "-"})

# ---------------------------------------------------------------------------
# Band resolution lookups (ADR-016, Decisions 2–4)
#
# All filter string matching is CASE-SENSITIVE.  This is intentional:
# letter case carries semantic meaning in filter nomenclature (e.g. "V" is
# Johnson-Cousins; "v" would be unrecognized; "i" is Sloan; "I" is
# Johnson-Cousins).  See ADR-016, Decision 2.
#
# phot_system string values must match PhotSystem enum values exactly.
# ---------------------------------------------------------------------------

# Step 2: Combined value splitting.
# Maps exact source filter string → (canonical filter_name, phot_system value).
_COMBINED_BAND_LOOKUP: dict[str, tuple[str, str]] = {
    # Johnson-Cousins family — all common prefixes for the same system
    "Johnson V": ("V", "Johnson-Cousins"),
    "Johnson B": ("B", "Johnson-Cousins"),
    "Johnson U": ("U", "Johnson-Cousins"),
    "Johnson R": ("R", "Johnson-Cousins"),
    "Johnson I": ("I", "Johnson-Cousins"),
    "Cousins V": ("V", "Johnson-Cousins"),
    "Cousins B": ("B", "Johnson-Cousins"),
    "Cousins U": ("U", "Johnson-Cousins"),
    "Cousins R": ("R", "Johnson-Cousins"),
    "Cousins I": ("I", "Johnson-Cousins"),
    "Bessel V": ("V", "Johnson-Cousins"),
    "Bessel B": ("B", "Johnson-Cousins"),
    "Bessel U": ("U", "Johnson-Cousins"),
    "Bessel R": ("R", "Johnson-Cousins"),
    "Bessel I": ("I", "Johnson-Cousins"),
    # Sloan / SDSS family — both prime and bare notations
    "Sloan u'": ("u'", "Sloan"),
    "Sloan g'": ("g'", "Sloan"),
    "Sloan r'": ("r'", "Sloan"),
    "Sloan i'": ("i'", "Sloan"),
    "Sloan z'": ("z'", "Sloan"),
    "SDSS u": ("u", "Sloan"),
    "SDSS g": ("g", "Sloan"),
    "SDSS r": ("r", "Sloan"),
    "SDSS i": ("i", "Sloan"),
    "SDSS z": ("z", "Sloan"),
    # 2MASS family
    "2MASS J": ("J", "2MASS"),
    "2MASS H": ("H", "2MASS"),
    "2MASS K": ("K", "2MASS"),
    "2MASS Ks": ("Ks", "2MASS"),
    # Swift/UVOT family — both "Swift/UVOT X" and "UVOT X" prefixes
    "Swift/UVOT UVW2": ("UVW2", "Swift-UVOT"),
    "Swift/UVOT UVM2": ("UVM2", "Swift-UVOT"),
    "Swift/UVOT UVW1": ("UVW1", "Swift-UVOT"),
    "Swift/UVOT U": ("U", "Swift-UVOT"),
    "Swift/UVOT B": ("B", "Swift-UVOT"),
    "Swift/UVOT V": ("V", "Swift-UVOT"),
    "UVOT UVW2": ("UVW2", "Swift-UVOT"),
    "UVOT UVM2": ("UVM2", "Swift-UVOT"),
    "UVOT UVW1": ("UVW1", "Swift-UVOT"),
    "UVOT U": ("U", "Swift-UVOT"),
    "UVOT B": ("B", "Swift-UVOT"),
    "UVOT V": ("V", "Swift-UVOT"),
}

# Step 3: Conservative defaults for unambiguous short names.
# Maps exact filter string → phot_system value.
# "U" is intentionally absent: ambiguous between Johnson-Cousins and Swift/UVOT.
# "K" is intentionally absent: ambiguous between 2MASS and radio (see Decision 4).
_DEFAULT_PHOT_SYSTEM: dict[str, str] = {
    # Johnson-Cousins (uppercase = Bessel/Johnson convention)
    "V": "Johnson-Cousins",
    "B": "Johnson-Cousins",
    "R": "Johnson-Cousins",
    "I": "Johnson-Cousins",
    # Sloan (lowercase = SDSS convention; case-sensitive)
    "u": "Sloan",
    "g": "Sloan",
    "r": "Sloan",
    "i": "Sloan",
    "z": "Sloan",
    # Sloan prime notation
    "u'": "Sloan",
    "g'": "Sloan",
    "r'": "Sloan",
    "i'": "Sloan",
    "z'": "Sloan",
    # 2MASS NIR (unambiguous; "K" excluded — handled in _disambiguate_with_context)
    "J": "2MASS",
    "H": "2MASS",
    "Ks": "2MASS",
    # Swift/UVOT (unique designations; no ambiguity)
    "UVW2": "Swift-UVOT",
    "UVM2": "Swift-UVOT",
    "UVW1": "Swift-UVOT",
    # Unambiguous radio bands
    "Ku": "Radio",
    "Ka": "Radio",
}

# Spectral coordinate type + unit inferred from phot_system when absent
# in the source file.  Only type and unit are inferrable here; value
# requires a filter library (ADR-016, Decision 6).
_PHOT_SYSTEM_SPECTRAL_META: dict[str, tuple[str, str]] = {
    "Johnson-Cousins": ("wavelength", "Angstrom"),
    "Sloan": ("wavelength", "Angstrom"),
    "Swift-UVOT": ("wavelength", "Angstrom"),
    "2MASS": ("wavelength", "Angstrom"),
    "Bessel": ("wavelength", "Angstrom"),
    "Radio": ("frequency", "GHz"),
    "X-ray": ("energy", "keV"),
}

# Step 4: Context-aware disambiguation.
# Known radio telescopes — used to disambiguate "K" (radio vs. 2MASS).
_RADIO_TELESCOPES: frozenset[str] = frozenset(
    {
        "VLA",
        "JVLA",
        "ATCA",
        "MeerKAT",
        "WSRT",
        "AMI",
        "NOEMA",
        "SMA",
        "ALMA",
        "e-MERLIN",
        "EVN",
        "VLBA",
        "GBT",
        "Effelsberg",
    }
)

# Known NIR/optical telescopes — used to disambiguate "K" toward 2MASS.
_NIR_TELESCOPES: frozenset[str] = frozenset(
    {
        "CTIO",
        "2MASS",
        "UKIRT",
        "VISTA",
        "NTT",
        "VLT",
        "Keck",
        "Palomar",
        "IRTF",
        "TNG",
        "NOT",
        "SAAO",
    }
)


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class MissingRequiredColumnsError(Exception):
    """
    Raised by CanonicalCsvAdapter.adapt() when the source file is structurally
    missing columns that resolve to required PhotometryRow fields.

    This is a file-level failure — no rows are processed.  The caller should
    map it to PhotometryQuarantineReasonCode.missing_required_columns.

    Attributes
    ----------
    missing_fields:
        Canonical field names that could not be resolved from the source headers.
    """

    def __init__(self, missing_fields: list[str]) -> None:
        self.missing_fields = sorted(missing_fields)
        super().__init__(
            f"Source file is missing required columns for fields: {self.missing_fields}"
        )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class CanonicalCsvAdapter:
    """
    Concrete MVP implementation of the PhotometryAdapter protocol.

    Implements Tier 1 (canonical CSV headers) and Tier 2 (synonym registry)
    column name resolution per ADR-015, Decision 2.  Band and photometric
    system value resolution is handled by _resolve_band() per ADR-016.

    Parameters
    ----------
    synonyms:
        Optional synonym dict override, primarily for testing.  Keys are
        non-canonical column name strings (any case); values are canonical
        PhotometryRow field names.  If None, loaded from synonyms.json at
        construction time.
    excluded_filters:
        Optional excluded filter dict override, primarily for testing.  Keys
        are exact (case-sensitive) filter strings; values are human-readable
        reason strings.  If None, loaded from excluded_filters.json at
        construction time.
    """

    def __init__(
        self,
        synonyms: dict[str, str] | None = None,
        excluded_filters: dict[str, str] | None = None,
    ) -> None:
        # Canonical field names derived from the model — the authoritative set.
        self._canonical_fields: frozenset[str] = frozenset(PhotometryRow.model_fields.keys())

        # Synonym registry: stored with uppercase keys for O(1) case-insensitive lookup.
        if synonyms is not None:
            self._synonyms: dict[str, str] = {k.upper(): v for k, v in synonyms.items()}
        else:
            raw = json.loads((_ADAPTER_DIR / "synonyms.json").read_text())
            self._synonyms = {k.upper(): v for k, v in raw["synonyms"].items()}

        # Excluded filter registry: exact case-sensitive keys (ADR-016, Decision 2).
        if excluded_filters is not None:
            self._excluded_filters: dict[str, str] = excluded_filters
        else:
            raw = json.loads((_ADAPTER_DIR / "excluded_filters.json").read_text())
            self._excluded_filters = raw["excluded"]

    # ------------------------------------------------------------------
    # Public interface — PhotometryAdapter protocol
    # ------------------------------------------------------------------

    def adapt(
        self,
        raw_rows: Iterable[dict[str, Any]],
        nova_id: UUID,
        primary_name: str,
        ra_deg: float,
        dec_deg: float,
    ) -> AdaptationResult:
        """
        Adapt an iterable of raw CSV rows into validated PhotometryRow instances.

        Per-row pipeline (ADR-016, Decision 1):
          1. Normalise sentinel values → None
          2. Resolve column names (Tier 1 canonical, then Tier 2 synonym)
          3. [First row only] Check required columns are resolvable
          4. Resolve band: derive filter_name + phot_system via _resolve_band()
          5. Filter kwargs to canonical fields only
          6. Suppress source identity columns; inject workflow-provided values
          7. Construct and validate PhotometryRow via Pydantic

        Parameters
        ----------
        raw_rows:
            Raw parsed rows from the CSV.  Keys are source column names;
            values may be strings or native Python types.
        nova_id, primary_name, ra_deg, dec_deg:
            Identity fields from the resolved Nova entity.  Stamped onto
            every row; any source column mapping to these fields is ignored.

        Returns
        -------
        AdaptationResult

        Raises
        ------
        MissingRequiredColumnsError
            If the first row's resolved columns do not cover all required
            source fields.  Raised before any rows enter the failure list.
        """
        valid_rows: list[PhotometryRow] = []
        failures: list[AdaptationFailure] = []
        total = 0
        headers_checked = False

        for row_index, raw_row in enumerate(raw_rows):
            total += 1

            # Step 1: sentinel normalisation
            normalised = self._normalise_sentinels(raw_row)

            # Step 2: column name resolution
            resolved = self._resolve_columns(normalised)

            # Step 3: required-column guard (file-level; checked once)
            if not headers_checked:
                self._check_required_columns(resolved)
                headers_checked = True

            # Step 4: band resolution
            band_result = self._resolve_band(resolved)
            if isinstance(band_result, str):
                # band_result is a failure reason string
                failures.append(
                    AdaptationFailure(
                        row_index=row_index,
                        raw_row=raw_row,
                        error=band_result,
                    )
                )
                continue
            resolved["filter_name"], resolved["phot_system"] = band_result

            # Step 4a: infer spectral_coord_type / unit from phot_system
            self._infer_spectral_coord_meta(resolved)

            # Step 4b: derive time_mjd from time_orig (JD/HJD) if absent
            time_result = self._resolve_time(resolved)
            if time_result is not None:
                failures.append(
                    AdaptationFailure(
                        row_index=row_index,
                        raw_row=raw_row,
                        error=time_result,
                    )
                )
                continue

            # Step 5: restrict to canonical fields
            kwargs: dict[str, Any] = {
                k: v for k, v in resolved.items() if k in self._canonical_fields
            }

            # Step 6: suppress source identity columns; inject workflow values
            for field in _IDENTITY_FIELDS:
                kwargs.pop(field, None)
            kwargs["nova_id"] = nova_id
            kwargs["primary_name"] = primary_name
            kwargs["ra_deg"] = ra_deg
            kwargs["dec_deg"] = dec_deg

            # Step 7: Pydantic validation — final gate
            try:
                valid_rows.append(PhotometryRow(**kwargs))
            except ValidationError as exc:
                failures.append(
                    AdaptationFailure(
                        row_index=row_index,
                        raw_row=raw_row,
                        error=str(exc),
                    )
                )

        return AdaptationResult(
            valid_rows=valid_rows,
            failures=failures,
            total_row_count=total,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _normalise_sentinels(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Replace common null-sentinel strings with None.

        Matching is case-insensitive and applied after stripping whitespace.
        Non-string values are passed through unchanged.
        """
        result: dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, str) and v.strip().lower() in _SENTINEL_VALUES:
                result[k] = None
            else:
                result[k] = v
        return result

    def _resolve_column(self, col: str) -> str | None:
        """
        Resolve a single source column name to its canonical PhotometryRow field name.

        Tier 1 (exact canonical match, lowercased) takes precedence over
        Tier 2 (synonym registry, case-insensitive).  Returns None if the
        column cannot be resolved.
        """
        lower = col.lower()
        if lower in self._canonical_fields:
            return lower
        return self._synonyms.get(col.upper())

    def _resolve_columns(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Apply _resolve_column() to all keys in a normalised row dict.

        Unresolvable columns are silently dropped.  If two source columns
        resolve to the same canonical field, the later column (in dict
        iteration order) wins.
        """
        resolved: dict[str, Any] = {}
        for col, value in row.items():
            canonical = self._resolve_column(col)
            if canonical is not None:
                resolved[canonical] = value
        return resolved

    def _check_required_columns(self, resolved: dict[str, Any]) -> None:
        """
        Verify that required source fields are present in the resolved dict.

        phot_system is exempt: _resolve_band() may derive it from filter_name.
        All other fields in _REQUIRED_SOURCE_FIELDS must be present.

        Raises
        ------
        MissingRequiredColumnsError
            If any required fields are absent.
        """
        missing = [f for f in _REQUIRED_SOURCE_FIELDS if f not in resolved]
        if missing:
            raise MissingRequiredColumnsError(missing)

    def _infer_spectral_coord_meta(self, resolved: dict[str, Any]) -> None:
        """
        Fill in spectral_coord_type and spectral_coord_unit from phot_system
        when they are absent from the source file.

        Only fires when the field is missing or None; explicit source values
        always win.  phot_system must already be present in resolved (i.e.
        _resolve_band() has run).  Mutates resolved in place.
        """
        phot_system = resolved.get("phot_system")
        if phot_system not in _PHOT_SYSTEM_SPECTRAL_META:
            return
        inferred_type, inferred_unit = _PHOT_SYSTEM_SPECTRAL_META[phot_system]
        if not resolved.get("spectral_coord_type"):
            resolved["spectral_coord_type"] = inferred_type
        if not resolved.get("spectral_coord_unit"):
            resolved["spectral_coord_unit"] = inferred_unit

    def _resolve_time(self, resolved: dict[str, Any]) -> str | None:
        """
        Ensure time_mjd is present in resolved, converting from time_orig
        (JD, HJD, BJD) when needed.

        Resolution order:
          1. time_mjd already present → nothing to do.
          2. time_orig present + time_orig_sys is a JD/HJD variant
             → MJD = time_orig - 2400000.5.
          3. time_orig present + no time_orig_sys + value > 2400000.5
             → infer JD, convert, default time_orig_sys to JD_UTC.
          4. Neither resolvable → row failure.

        Mutates resolved in place on success.

        Returns
        -------
        None
            On success.
        str
            Human-readable failure reason on failure.
        """
        if resolved.get("time_mjd") is not None:
            return None

        time_orig = resolved.get("time_orig")
        if time_orig is None:
            return (
                "missing time: no source column resolved to time_mjd or a "
                "convertible time_orig (JD/HJD/BJD)"
            )

        try:
            time_orig_float = float(time_orig)
        except (ValueError, TypeError):
            return f"time conversion failed: could not parse time_orig '{time_orig}' as a number"

        time_orig_sys = resolved.get("time_orig_sys")

        _JD_SYSTEMS = {"JD_UTC", "JD_TT", "HJD_UTC", "HJD_TT"}
        _MJD_SYSTEMS = {"MJD_UTC", "MJD_TT"}

        if time_orig_sys in _MJD_SYSTEMS:
            # Source reported MJD in a time_orig column — use directly.
            resolved["time_mjd"] = time_orig_float
            return None

        if time_orig_sys in _JD_SYSTEMS or (
            time_orig_sys is None and time_orig_float > 2_400_000.5
        ):
            resolved["time_mjd"] = time_orig_float - 2_400_000.5
            # If time_orig_sys was absent, default to JD_UTC so the
            # time_orig / time_orig_sys pair invariant is satisfied.
            if time_orig_sys is None:
                resolved["time_orig_sys"] = "JD_UTC"
            return None

        return (
            f"time conversion failed: time_orig='{time_orig_float}' with "
            f"time_orig_sys='{time_orig_sys}' — cannot derive time_mjd"
        )

    def _resolve_band(self, resolved: dict[str, Any]) -> tuple[str, str] | str:
        """
        Resolve filter_name and phot_system from the resolved row dict.

        Implements the resolution procedure defined in ADR-016, Decisions 3–4:

          Step 1: Both filter_name and phot_system already present → pass through.
          Step 2: filter_name absent → row failure.
          Step 3: filter string in excluded set → row failure (known, rejected).
          Step 4: filter string in combined-value lookup → split and return.
          Step 5: filter string in conservative-default lookup → apply default.
          Step 6: context-aware disambiguation for genuinely ambiguous strings.
          Step 7: unrecognized → row failure.

        All filter string matching is case-sensitive (ADR-016, Decision 2).

        Returns
        -------
        tuple[str, str]
            ``(filter_name, phot_system)`` on success.
        str
            Human-readable failure reason on failure.
        """
        raw_filter = resolved.get("filter_name")
        raw_system = resolved.get("phot_system")

        # Step 1: both already present
        if raw_filter is not None and raw_system is not None:
            return str(raw_filter), str(raw_system)

        # Step 2: filter_name absent — nothing to work with
        if raw_filter is None:
            return "missing filter_name: no source column resolved to filter_name"

        filter_str = str(raw_filter)

        # Step 3: excluded filter (known, deliberately rejected)
        if filter_str in self._excluded_filters:
            reason = self._excluded_filters[filter_str]
            return f"excluded filter type: '{filter_str}' ({reason}) — row dropped"

        # Step 4: combined value splitting
        if filter_str in _COMBINED_BAND_LOOKUP:
            return _COMBINED_BAND_LOOKUP[filter_str]

        # Step 5: conservative default for unambiguous short names
        if filter_str in _DEFAULT_PHOT_SYSTEM:
            return filter_str, _DEFAULT_PHOT_SYSTEM[filter_str]

        # Step 6: context-aware disambiguation
        context_result = self._disambiguate_with_context(filter_str, resolved)
        if context_result is not None:
            return context_result

        # Step 7: unrecognized
        return (
            f"unrecognized filter: '{filter_str}' — not in combined-value lookup, "
            f"short-name defaults, or excluded set; add to excluded_filters.json or "
            f"synonyms.json if encountered repeatedly"
        )

    def _disambiguate_with_context(
        self,
        filter_str: str,
        resolved: dict[str, Any],
    ) -> tuple[str, str] | None:
        """
        Attempt context-aware disambiguation for genuinely ambiguous filter strings.

        Currently handles: ``"K"`` (2MASS K-band ~2.2 μm vs. radio K-band ~22 GHz).

        Context signals are read from the already-resolved kwargs dict; column
        name resolution has run across all source columns before this is called,
        so there is no ordering dependency (ADR-016, Decision 4).

        Returns
        -------
        tuple[str, str]
            ``(filter_name, phot_system)`` if context is sufficient.
        None
            If context is insufficient; the caller will record a row failure.
        """
        if filter_str != "K":
            return None

        phot_system = resolved.get("phot_system")
        telescope = str(resolved.get("telescope") or "").strip()
        coord_type = resolved.get("spectral_coord_type")
        coord_unit = resolved.get("spectral_coord_unit")

        # Explicit phot_system wins outright
        if phot_system == "2MASS":
            return "K", "2MASS"
        if phot_system == "Radio":
            return "K", "Radio"

        # Telescope hints — substring match to handle e.g. "VLA D-array"
        if telescope:
            if any(rt in telescope for rt in _RADIO_TELESCOPES):
                return "K", "Radio"
            if any(nt in telescope for nt in _NIR_TELESCOPES):
                return "K", "2MASS"

        # spectral_coord_type hints
        if coord_type == "frequency":
            return "K", "Radio"
        if coord_type == "wavelength":
            return "K", "2MASS"

        # spectral_coord_unit hints
        if coord_unit in ("GHz", "MHz"):
            return "K", "Radio"
        if coord_unit in ("Angstrom", "nm"):
            return "K", "2MASS"

        # Insufficient context — caller records a descriptive row failure
        return None
