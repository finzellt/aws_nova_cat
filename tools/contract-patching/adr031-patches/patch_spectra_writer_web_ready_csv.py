#!/usr/bin/env python3
"""
ADR-031 Decision 4 patch — ticket_ingestor/spectra_writer.py

Adds web-ready CSV generation to the spectra writer after the FITS S3 upload.

Changes:
  1. Adds imports for build_web_ready_csv, write_web_ready_csv_to_s3, and astropy.io.fits.
  2. Adds a private_bucket parameter to write_spectrum.
  3. Inserts CSV generation and upload between DataProduct PutItem and FileObject PutItem.

Usage:
    python patch_spectra_writer_web_ready_csv.py path/to/services/ticket_ingestor/spectra_writer.py
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
        print(f"Usage: {sys.argv[0]} <path/to/spectra_writer.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src, "from ticket_ingestor.spectra_reader import SpectrumResult", "SpectrumResult import"
    )
    _require(src, "def write_spectrum(", "write_spectrum function")
    _require(src, "table: Any,  # boto3 DynamoDB Table resource", "table parameter")
    _require(src, "table.put_item(Item=dp_item)", "DataProduct PutItem")
    _require(src, "# ── 3. FileObject PutItem", "FileObject section header")
    _require(
        src,
        "write_spectrum(result, nova_id, job_run_id, bucket, s3, table) -> None",
        "Public API docstring",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add imports
    # =========================================================================
    OLD_IMPORT = "from ticket_ingestor.spectra_reader import SpectrumResult"
    NEW_IMPORT = (
        "import io\n"
        "\n"
        "import astropy.io.fits as fits\n"
        "\n"
        "from nova_common.web_ready_csv import build_web_ready_csv, write_web_ready_csv_to_s3\n"
        "from ticket_ingestor.spectra_reader import SpectrumResult"
    )
    src = _replace_once(src, OLD_IMPORT, NEW_IMPORT, "imports")

    # =========================================================================
    # Patch 2 — Add private_bucket parameter to write_spectrum signature
    # =========================================================================
    OLD_SIG = "    table: Any,  # boto3 DynamoDB Table resource\n) -> None:"
    NEW_SIG = (
        "    table: Any,  # boto3 DynamoDB Table resource\n"
        "    private_bucket: str | None = None,  # private S3 bucket for derived artifacts\n"
        ") -> None:"
    )
    src = _replace_once(src, OLD_SIG, NEW_SIG, "private_bucket parameter")

    # =========================================================================
    # Patch 3 — Update Public API docstring
    # =========================================================================
    OLD_API_DOC = "write_spectrum(result, nova_id, job_run_id, bucket, s3, table) -> None"
    NEW_API_DOC = (
        "write_spectrum(result, nova_id, job_run_id, bucket, s3, table, private_bucket) -> None"
    )
    src = _replace_once(src, OLD_API_DOC, NEW_API_DOC, "Public API docstring update")

    # =========================================================================
    # Patch 4 — Insert web-ready CSV generation between DataProduct PutItem
    #           and FileObject PutItem
    # =========================================================================
    OLD_FILE_OBJECT = "    table.put_item(Item=dp_item)\n\n    # ── 3. FileObject PutItem"
    NEW_FILE_OBJECT = (
        "    table.put_item(Item=dp_item)\n"
        "\n"
        "    # ── 2b. Web-ready CSV (ADR-031 P-4) ────────────────────────────────\n"
        "    #\n"
        "    # Best-effort: a failed write logs a warning but does not fail the\n"
        "    # spectrum write.  The ticket path has the FITS bytes in memory, so\n"
        "    # we re-open them to extract the validated arrays and spectral_units.\n"
        "    if private_bucket is not None:\n"
        "        try:\n"
        "            with fits.open(io.BytesIO(result.fits_bytes), memmap=False) as hdul:\n"
        "                primary_data = hdul[0].data\n"
        '                spectral_units = hdul[0].header.get("CUNIT1", "Angstrom")\n'
        "                # Flux is in the primary HDU data array; wavelength must be\n"
        "                # reconstructed from WCS keywords (CRVAL1 + CDELT1 * index).\n"
        '                crval1 = float(hdul[0].header["CRVAL1"])\n'
        '                cdelt1 = float(hdul[0].header.get("CDELT1", 1.0))\n'
        '                crpix1 = float(hdul[0].header.get("CRPIX1", 1.0))\n'
        "                import numpy as np\n"
        "                n_pix = len(primary_data)\n"
        "                wavelength = crval1 + cdelt1 * (np.arange(n_pix) - (crpix1 - 1.0))\n"
        "                flux = np.asarray(primary_data, dtype=np.float64)\n"
        "\n"
        "            csv_content = build_web_ready_csv(\n"
        "                wavelength=wavelength,\n"
        "                flux=flux,\n"
        "                spectral_units=spectral_units,\n"
        "            )\n"
        "            write_web_ready_csv_to_s3(\n"
        "                csv_content=csv_content,\n"
        "                nova_id=nova_id_str,\n"
        "                data_product_id=data_product_id_str,\n"
        "                s3=s3,\n"
        "                bucket=private_bucket,\n"
        "            )\n"
        "        except Exception:  # noqa: BLE001\n"
        "            import logging\n"
        "            logging.getLogger(__name__).warning(\n"
        '                "Failed to write web-ready CSV for ticket spectrum — continuing",\n'
        "                exc_info=True,\n"
        "                extra={\n"
        '                    "data_product_id": data_product_id_str,\n'
        '                    "nova_id": nova_id_str,\n'
        "                },\n"
        "            )\n"
        "\n"
        "    # ── 3. FileObject PutItem"
    )
    src = _replace_once(src, OLD_FILE_OBJECT, NEW_FILE_OBJECT, "web-ready CSV block")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    checks = [
        ("from nova_common.web_ready_csv import build_web_ready_csv", "web_ready_csv import"),
        ("import astropy.io.fits as fits", "astropy import"),
        ("private_bucket: str | None = None", "private_bucket parameter"),
        ("if private_bucket is not None:", "private_bucket guard"),
        ("build_web_ready_csv(", "build_web_ready_csv call"),
        ("write_web_ready_csv_to_s3(", "write_web_ready_csv_to_s3 call"),
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
    print()
    print("Next steps:")
    print("  1. Update handler.py _ingest_spectra to pass private_bucket=_PRIVATE_BUCKET")
    print("     to write_spectrum calls.")
    print(
        "  2. Run: mypy --strict services/ticket_ingestor/ && ruff check services/ticket_ingestor/"
    )


if __name__ == "__main__":
    main()
