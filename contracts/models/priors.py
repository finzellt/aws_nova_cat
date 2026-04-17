"""Nova priors contract model (ADR-036).

A ``NovaPriorsEntry`` is a single row of the operator-curated nova priors
artifact bundled with ``nova_resolver``.  The canonical source of truth for
these entries is ``tools/catalog-expansion/nova_candidates_final_full_year.csv``;
``tools/catalog-expansion/build_nova_priors.py`` transforms that CSV into the
bundled ``services/nova_resolver/nova_priors/nova_priors.json`` and validates
every row through this model at build time (ADR-036 Decision 4).

Validation locus
----------------
This model is **the** schema contract.  Validation runs:

  1. In ``build_nova_priors.py`` — one ``NovaPriorsEntry`` constructed per CSV
     row.  Build fails loudly on any Pydantic error, and no JSON is emitted.
  2. In ``validate_nova_priors.py`` (CI) — re-parses the committed JSON through
     this model as a belt-and-suspenders check against hand-edits.

The runtime reader in ``services/nova_resolver/nova_priors/reader.py``
does **not** re-validate entries on every Lambda cold start; it performs only
cheap top-level checks (schema version, alias collisions) and trusts the
build-time-validated bundle (ADR-036 Decision 4 rationale).

The model is frozen because the priors artifact is read-only at runtime
(ADR-036 Decision 10), mirroring the ``BandRegistryEntry`` pattern from
ADR-017.
"""

from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Discovery-date format validator — PRIORS-LOCAL, stricter than the one in
# entities.py.
#
# Nova.discovery_date and Reference.publication_date both accept day=00
# (``entities._DISCOVERY_DATE_RE``) because ADS pubdates are month-precision
# at best, and ``refresh_references`` therefore often produces month-only
# discovery dates that must be storable.
#
# Priors, by contrast, exist precisely to ship precise curated dates into
# ``initialize_nova``.  Accepting day=00 here would defeat the purpose — the
# file would be carrying the same month-only imprecision that
# ``refresh_references`` already produces downstream.  Priors are allowed to
# omit a discovery date entirely (the field is ``str | None``), but when
# present it MUST resolve to a specific day.
#
# Day range is 01-31; month range is 01-12.  No day-00 or month-00.
_PRIORS_DISCOVERY_DATE_RE = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$")


class NovaPriorsEntry(BaseModel):
    """A single entry in the nova priors artifact (ADR-036 Decision 3).

    Frozen at construction time; treat as immutable.

    Field-to-CSV mapping is documented in ADR-036 Decision 3.  The CSV's
    ``Input_Name`` column is intentionally dropped at build time and does not
    appear here.

    Co-field invariants enforced by ``_validate_peak_mag_cofields``:

    - ``peak_mag_band`` is non-null iff ``peak_mag`` is non-null.  A band
      without a magnitude, or a magnitude without a band, is rejected.
    - ``peak_mag_uncertain == True`` requires ``peak_mag`` to be non-null.
      An uncertainty flag on a missing measurement is rejected.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    primary_name: str = Field(
        ...,
        min_length=1,
        description=(
            "Display name of the nova, verbatim from the curated CSV.  "
            "Normalization for lookup is applied by the reader at runtime "
            "(ADR-036 Decision 2); the stored string preserves display "
            "casing and whitespace."
        ),
    )
    simbad_main_id: str | None = Field(
        default=None,
        description=(
            "SIMBAD main_id for this nova, or None when not yet resolved "
            "(typical for very recent discoveries)."
        ),
    )
    aliases: list[str] = Field(
        default_factory=list,
        description=(
            "All known alias strings for this nova, drawn from SIMBAD ids plus "
            "operator curation.  Pipe-delimited in the source CSV; list-valued "
            "here.  Whitespace-stripped and deduped at build time; order is "
            "preserved for reviewability."
        ),
    )
    discovery_date: str | None = Field(
        default=None,
        description=(
            "Discovery date in YYYY-MM-DD format with a SPECIFIC day (01-31). "
            "None when no discovery date has been curated.  Unlike "
            "Nova.discovery_date and Reference.publication_date, priors do "
            "NOT accept day=00 — the priors file exists to ship precise "
            "curated dates, not month-only imprecision.  If a precise day "
            "cannot be determined, leave the field empty in the source CSV "
            "so this entry serializes to None."
        ),
    )
    otypes: list[str] = Field(
        default_factory=list,
        description=(
            "SIMBAD object-type tokens (e.g. 'CV*', 'No*', 'V*').  Pipe-"
            "delimited in the source CSV; list-valued here.  Carried for "
            "future use by F10 (nova-type-as-list) and classification "
            "display; not consumed by initialize_nova at this time."
        ),
    )
    is_nova: bool = Field(
        ...,
        description=(
            "True if this entry represents a confirmed classical nova.  "
            "False for entries retained in the priors file as known "
            "non-novae (e.g. mis-identified candidates, BH X-ray binaries, "
            "FU Orionis variables).  False entries drive the front-door "
            "rejection flow in initialize_nova (ADR-036 Decision 9, "
            "wiring in item-3 follow-up)."
        ),
    )
    is_recurrent: bool = Field(
        ...,
        description=(
            "True if this nova is a recurrent nova (multiple recorded "
            "outbursts).  Recurrent-nova infrastructure is not yet built "
            "(tracked under D2), so the item-3 consumer will mark any "
            "is_recurrent=True nova as INACTIVE at creation time to keep "
            "it out of the catalog until multi-outburst support lands.  "
            "See ADR-036 §7 and Open Item 3 for the open question of "
            "whether this signal is stored on the Nova DDB item as a "
            "boolean or folded into the existing nova_type string."
        ),
    )
    peak_mag: float | None = Field(
        default=None,
        description=(
            "Peak apparent magnitude observed during outburst, as curated "
            "from AAVSO / literature sources.  None when no peak magnitude "
            "has been recorded.  Band is carried in peak_mag_band; "
            "uncertainty flag in peak_mag_uncertain."
        ),
    )
    peak_mag_band: str | None = Field(
        default=None,
        description=(
            "Band in which the peak magnitude was measured (e.g. 'V', 'G', "
            "'J', 'r').  Required if peak_mag is non-None and forbidden "
            "otherwise; see _validate_peak_mag_cofields."
        ),
    )
    peak_mag_uncertain: bool = Field(
        default=False,
        description=(
            "True when the peak magnitude value carries significant "
            "uncertainty (typically because observations near maximum were "
            "sparse or the source was already declining when first "
            "detected).  Requires peak_mag to be non-None; see "
            "_validate_peak_mag_cofields."
        ),
    )

    # ------------------------------------------------------------------
    # Field-level validators
    # ------------------------------------------------------------------

    @field_validator("primary_name")
    @classmethod
    def _reject_blank_primary_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("primary_name cannot be blank.")
        return v

    @field_validator("aliases", "otypes")
    @classmethod
    def _reject_blank_list_entries(cls, v: list[str]) -> list[str]:
        for item in v:
            if not item.strip():
                raise ValueError(
                    "aliases and otypes entries cannot be blank.  "
                    "Strip and filter empty tokens at build time."
                )
        return v

    @field_validator("discovery_date")
    @classmethod
    def _validate_discovery_date_format(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not _PRIORS_DISCOVERY_DATE_RE.match(v):
            raise ValueError(
                f"discovery_date={v!r} is not a valid priors date.  Priors "
                "require YYYY-MM-DD with month 01-12 AND day 01-31 — day=00 "
                "(month-only precision) is not accepted in the priors file.  "
                "If a precise day cannot be determined for this nova, leave "
                "the source CSV cell empty so the entry serializes to None "
                "and the discovery date can be populated later by "
                "refresh_references."
            )
        return v

    @field_validator("simbad_main_id", "peak_mag_band")
    @classmethod
    def _reject_blank_optional_strings(cls, v: str | None) -> str | None:
        """Turn empty/whitespace-only strings into None.

        The build script maps empty CSV cells to None already, but an
        accidental space-only cell would otherwise sneak through as a
        non-None blank string.
        """
        if v is None:
            return None
        stripped = v.strip()
        return stripped or None

    # ------------------------------------------------------------------
    # Cross-field validator
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def _validate_peak_mag_cofields(self) -> NovaPriorsEntry:
        has_mag = self.peak_mag is not None
        has_band = self.peak_mag_band is not None

        if has_mag != has_band:
            raise ValueError(
                "peak_mag and peak_mag_band must be both present or both absent.  "
                f"Got peak_mag={self.peak_mag!r}, peak_mag_band={self.peak_mag_band!r}."
            )
        if self.peak_mag_uncertain and not has_mag:
            raise ValueError(
                "peak_mag_uncertain=True requires peak_mag to be non-None.  "
                "An uncertainty flag on a missing measurement is meaningless."
            )
        return self


__all__ = ["NovaPriorsEntry"]
