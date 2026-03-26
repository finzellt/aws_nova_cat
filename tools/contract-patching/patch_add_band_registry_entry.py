#!/usr/bin/env python3
"""
Patch: add BandRegistryEntry to contracts/models/entities.py

ADR-017 Decision 8 specifies that BandRegistryEntry is the public return type
of the band registry Python interface.  This script appends the model to the
contracts layer.

Pre-conditions checked before any write:
  1. Target file exists.
  2. BandRegistryEntry is NOT already defined (idempotency guard).
  3. SpectralCoordUnit IS defined (dependency guard).
  4. BaseModel IS imported (Pydantic convention guard).
  5. ConfigDict IS imported (config guard).

Usage:
    python patch_add_band_registry_entry.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

TARGET = Path("contracts/models/entities.py")

# ---------------------------------------------------------------------------
# New model source — appended verbatim after the final class in the file.
# ---------------------------------------------------------------------------
NEW_MODEL = '''

# ---------------------------------------------------------------------------
# BandRegistryEntry
# ---------------------------------------------------------------------------


class BandRegistryEntry(BaseModel):
    """
    Pydantic contract for a single Band Registry entry.

    Mirrors the JSON schema from ADR-017 Decision 3.  Used as the public
    return type of the band registry Python interface (ADR-017 Decision 8).

    This model carries the fields consumed by application code.  Registry-
    internal sub-objects (calibration blocks, detector_type, observatory_
    facility, etc.) are not reflected here; they are not part of the public
    interface contract.

    The model is frozen (immutable after construction) because the band
    registry is read-only at runtime (ADR-017 Decision 8).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    band_id: str = Field(
        ...,
        min_length=1,
        description=(
            "Stable NovaCat canonical band identifier (ADR-017 Decision 2). "
            "Globally unique within the registry. Always the first element of aliases."
        ),
    )
    aliases: list[str] = Field(
        ...,
        description=(
            "All known string forms by which this band appears in source files. "
            "Case-sensitive (ADR-017 Decision 4). band_id is always the first element."
        ),
    )
    regime: str | None = Field(
        default=None,
        description=(
            "Broad wavelength regime. Controlled vocabulary: "
            "optical, uv, nir, mir, fir, xray, radio, gamma. "
            "None for excluded entries (ADR-017 §3.3)."
        ),
    )
    svo_filter_id: str | None = Field(
        default=None,
        description=(
            "SVO Filter Profile Service identifier. "
            "None if no SVO entry exists for this band."
        ),
    )
    lambda_eff: float | None = Field(
        default=None,
        description=(
            "Effective (flux-weighted mean) wavelength. "
            "Units given by spectral_coord_unit; None for sparse or excluded entries."
        ),
    )
    spectral_coord_unit: SpectralCoordUnit | None = Field(
        default=None,
        description=(
            "Unit of lambda_eff and bandpass_width. "
            "None when both spectral fields are None."
        ),
    )
    bandpass_width: float | None = Field(
        default=None,
        description=(
            "Bandpass width in the units of spectral_coord_unit. "
            "None for sparse or excluded entries."
        ),
    )
    is_excluded: bool = Field(
        ...,
        description=(
            "True if this entry represents a non-photometric observation mode "
            "to be rejected at ingestion (e.g. visual estimates, unfiltered)."
        ),
    )
    exclusion_reason: str | None = Field(
        default=None,
        description=(
            "Human-readable rejection reason. "
            "Non-None when is_excluded=True; None otherwise."
        ),
    )
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print patch without writing.")
    args = parser.parse_args()

    # --- Pre-condition 1: file exists ------------------------------------
    if not TARGET.exists():
        print(f"ERROR: target file not found: {TARGET}", file=sys.stderr)
        print("Run this script from the repository root.", file=sys.stderr)
        return 1

    source = TARGET.read_text(encoding="utf-8")

    # --- Pre-condition 2: idempotency guard ------------------------------
    if "class BandRegistryEntry" in source:
        print("SKIP: BandRegistryEntry already defined in entities.py — nothing to do.")
        return 0

    # --- Pre-condition 3: SpectralCoordUnit dependency -------------------
    if "class SpectralCoordUnit" not in source:
        print(
            "ERROR: SpectralCoordUnit not found in entities.py — dependency missing.",
            file=sys.stderr,
        )
        return 1

    # --- Pre-condition 4: BaseModel import -------------------------------
    if "BaseModel" not in source:
        print(
            "ERROR: BaseModel not found in entities.py — Pydantic import missing.",
            file=sys.stderr,
        )
        return 1

    # --- Pre-condition 5: ConfigDict import ------------------------------
    if "ConfigDict" not in source:
        print(
            "ERROR: ConfigDict not found in entities.py — Pydantic import missing.",
            file=sys.stderr,
        )
        return 1

    # --- Build patched source --------------------------------------------
    # Append after the last non-blank line to avoid trailing-whitespace issues.
    patched = source.rstrip("\n") + NEW_MODEL

    if args.dry_run:
        print("--- DRY RUN: would append the following to contracts/models/entities.py ---")
        print(NEW_MODEL)
        print("--- END DRY RUN ---")
        return 0

    TARGET.write_text(patched, encoding="utf-8")
    print(f"OK: BandRegistryEntry appended to {TARGET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
