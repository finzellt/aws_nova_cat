from services.workflow_runtime.s3_keys import (
    photometry_derived_key,
    photometry_split_upload_key,
    photometry_upload_key,
    photometry_upload_prefix,
    spectra_derived_prefix,
    spectra_quarantine_prefix,
    spectra_raw_prefix,
)


def test_spectra_prefixes() -> None:
    assert spectra_raw_prefix("N1", "DP1") == "raw/spectra/N1/DP1/"
    assert spectra_derived_prefix("N1", "DP1") == "derived/spectra/N1/DP1/"
    assert (
        spectra_quarantine_prefix("N1", "DP1", "2026-01-01T00:00:00Z")
        == "quarantine/spectra/N1/DP1/2026-01-01T00:00:00Z/"
    )


def test_photometry_keys() -> None:
    assert photometry_upload_prefix() == "raw/photometry/uploads/"
    assert (
        photometry_upload_key("ing1", "file.csv") == "raw/photometry/uploads/ing1/original/file.csv"
    )
    assert (
        photometry_split_upload_key("ing1", "N1", "file.csv")
        == "raw/photometry/uploads/ing1/split/N1/file.csv"
    )
    assert photometry_derived_key("N1") == "derived/photometry/N1/photometry_table.parquet"
