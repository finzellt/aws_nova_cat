#!/usr/bin/env python3
"""
ADR-031 Decision 4 patch — spectra_validator/handler.py

Adds web-ready CSV generation to the ValidateBytes VALID path.

Changes:
  1. Adds import for build_web_ready_csv, write_web_ready_csv_to_s3 from nova_common.
  2. Inserts best-effort CSV write after enrichment field extraction, before the
     "ValidateBytes complete" log line.  Skipped for duplicates.
  3. Updates module docstring to document the new behaviour.

Usage:
    python patch_handler_web_ready_csv.py path/to/services/spectra_validator/handler.py
"""

from __future__ import annotations

import sys


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _replace_once(content: str, old: str, new: str, label: str) -> str:
    if old not in content:
        print(f"REPLACE FAILED — anchor for {label!r} not found.")
        sys.exit(1)
    if content.count(old) > 1:
        print(f"REPLACE FAILED — anchor for {label!r} appears more than once.")
        sys.exit(1)
    return content.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/handler.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(src, "from nova_common.tracing import tracer", "tracer import")
    _require(
        src,
        "flux_unit: str | None = raw_flux_unit if raw_flux_unit else None\n",
        "enrichment field extraction (flux_unit)",
    )
    _require(
        src,
        '    logger.info(\n        "ValidateBytes complete",',
        "ValidateBytes complete log line",
    )
    _require(
        src,
        "observation_date_mjd, flux_unit. These are consumed by artifact generators",
        "module docstring enrichment fields note",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add import
    # =========================================================================
    OLD_IMPORT = "from nova_common.tracing import tracer"
    NEW_IMPORT = (
        "from nova_common.tracing import tracer\n"
        "from nova_common.web_ready_csv import build_web_ready_csv, write_web_ready_csv_to_s3"
    )
    src = _replace_once(src, OLD_IMPORT, NEW_IMPORT, "web_ready_csv import")

    # =========================================================================
    # Patch 2 — Insert CSV write between enrichment extraction and log line
    # =========================================================================
    OLD_LOG = (
        "    flux_unit: str | None = raw_flux_unit if raw_flux_unit else None\n"
        "\n"
        "    logger.info(\n"
        '        "ValidateBytes complete",'
    )
    NEW_LOG = (
        "    flux_unit: str | None = raw_flux_unit if raw_flux_unit else None\n"
        "\n"
        "    # --- Write web-ready CSV (ADR-031 P-4) ---\n"
        "    # Best-effort: a failed write logs a warning but does not fail the\n"
        "    # validation.  The CSV is a derived artifact that can be regenerated\n"
        "    # from the raw FITS via the backfill script.\n"
        "    # Skipped for duplicates — the canonical product's CSV already exists.\n"
        "    if not duplicate_id and spectrum:\n"
        "        try:\n"
        "            csv_content = build_web_ready_csv(\n"
        "                wavelength=spectrum.spectral_axis,\n"
        "                flux=spectrum.flux_axis,\n"
        "                spectral_units=spectrum.spectral_units,\n"
        "            )\n"
        "            write_web_ready_csv_to_s3(\n"
        "                csv_content=csv_content,\n"
        "                nova_id=nova_id,\n"
        "                data_product_id=data_product_id,\n"
        "                s3=_s3,\n"
        "                bucket=_PRIVATE_BUCKET,\n"
        "            )\n"
        "        except Exception:\n"
        "            logger.warning(\n"
        '                "Failed to write web-ready CSV — validation result unaffected",\n'
        "                extra={\n"
        '                    "data_product_id": data_product_id,\n'
        '                    "nova_id": nova_id,\n'
        "                },\n"
        "                exc_info=True,\n"
        "            )\n"
        "\n"
        "    logger.info(\n"
        '        "ValidateBytes complete",'
    )
    src = _replace_once(src, OLD_LOG, NEW_LOG, "web-ready CSV write block")

    # =========================================================================
    # Patch 3 — Update module docstring
    # =========================================================================
    OLD_DOCSTRING = (
        "    observation_date_mjd, flux_unit. These are consumed by artifact generators\n"
        "    (DESIGN-003 §7) and must not require FITS header reads at generation time."
    )
    NEW_DOCSTRING = (
        "    observation_date_mjd, flux_unit. These are consumed by artifact generators\n"
        "    (DESIGN-003 §7) and must not require FITS header reads at generation time.\n"
        "\n"
        "  ValidateBytes also writes a web-ready CSV (ADR-031 Decision 4) to\n"
        "    derived/spectra/<nova_id>/<data_product_id>/web_ready.csv in the private\n"
        "    S3 bucket after successful validation. This is best-effort: a failed\n"
        "    write does not affect the validation outcome. Skipped for duplicates."
    )
    src = _replace_once(src, OLD_DOCSTRING, NEW_DOCSTRING, "module docstring update")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    checks = [
        ("from nova_common.web_ready_csv import build_web_ready_csv", "web_ready_csv import"),
        ("if not duplicate_id and spectrum:", "duplicate guard"),
        ("build_web_ready_csv(", "build_web_ready_csv call"),
        ("write_web_ready_csv_to_s3(", "write_web_ready_csv_to_s3 call"),
        ("bucket=_PRIVATE_BUCKET,", "private bucket reference"),
        ("ValidateBytes also writes a web-ready CSV", "docstring update"),
    ]

    failed = False
    for marker, label in checks:
        if marker not in src:
            print(f"POSTCONDITION FAILED — {label!r}")
            failed = True

    if failed:
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")


if __name__ == "__main__":
    main()
