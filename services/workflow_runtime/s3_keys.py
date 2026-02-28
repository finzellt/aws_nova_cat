"""
S3 key construction helpers aligned with the Nova Cat S3 layout.

This module provides deterministic builders for S3 prefixes and keys
used by workflows.

Examples:

- raw/spectra/<nova_id>/<data_product_id>/
- quarantine/spectra/<nova_id>/<data_product_id>/<timestamp>/
- raw/photometry/uploads/<ingest_file_id>/original/<filename>
- raw/photometry/uploads/<ingest_file_id>/split/<nova_id>/<filename>
- derived/photometry/<nova_id>/photometry_table.parquet

These helpers:

- Do not perform any S3 I/O.
- Do not embed workflow logic.
- Simply construct canonical paths consistent with the documented layout.

Centralizing key construction ensures layout stability across epics.
"""

from __future__ import annotations

import posixpath


def _join(*parts: str) -> str:
    return posixpath.join(*[p.strip("/") for p in parts if p is not None and p != ""])  # type: ignore[arg-type]


# Spectra


def spectra_raw_prefix(nova_id: str, data_product_id: str) -> str:
    return _join("raw", "spectra", nova_id, data_product_id) + "/"


def spectra_derived_prefix(nova_id: str, data_product_id: str) -> str:
    return _join("derived", "spectra", nova_id, data_product_id) + "/"


def spectra_quarantine_prefix(nova_id: str, data_product_id: str, timestamp_iso: str) -> str:
    return _join("quarantine", "spectra", nova_id, data_product_id, timestamp_iso) + "/"


# Photometry


def photometry_upload_prefix() -> str:
    return _join("raw", "photometry", "uploads") + "/"


def photometry_upload_key(ingest_file_id: str, filename: str) -> str:
    """
    S3 key for the original uploaded photometry file.

    Layout (per s3-layout.md):
      raw/photometry/uploads/<ingest_file_id>/original/<filename>
    """
    safe_filename = filename.lstrip("/")
    return f"raw/photometry/uploads/{ingest_file_id}/original/{safe_filename}"


def photometry_derived_key(nova_id: str) -> str:
    return _join("derived", "photometry", nova_id, "photometry_table.parquet")


def photometry_split_upload_key(ingest_file_id: str, nova_id: str, filename: str) -> str:
    """
    S3 key for a split photometry upload artifact.

    Layout (per s3-layout.md):
      raw/photometry/uploads/<ingest_file_id>/split/<nova_id>/<filename>
    """
    safe_filename = filename.lstrip("/")
    return f"raw/photometry/uploads/{ingest_file_id}/split/{nova_id}/{safe_filename}"
