"""Bundle ZIP generator (DESIGN-003 §10).

Assembles a per-nova downloadable archive containing the complete,
research-grade data package: README, structured metadata, provenance,
citation-ready BibTeX, consolidated photometry FITS table, and raw
spectra FITS files.

The bundle is the last generator in the per-nova dependency chain.
By the time it runs, ``nova_context`` is fully populated by upstream
generators.
"""

from __future__ import annotations

import io
import json
import logging
import os
import tempfile
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from astropy.io import fits  # type: ignore[import-untyped]
from boto3.dynamodb.conditions import Attr, Key  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from generators.shared import format_coordinates, generated_at_timestamp

_logger = logging.getLogger("artifact_generator")

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_bundle_zip(
    nova_id: str,
    table: Any,
    s3_client: Any,
    private_bucket: str,
    public_bucket: str,
    nova_context: dict[str, Any],
    *,
    s3_key_prefix: str = "",
) -> dict[str, Any]:
    """Generate the bundle.zip artifact for a nova.

    Unlike JSON generators, this writes a ZIP file to S3 and returns
    a metadata dict (not the artifact content itself).
    """
    nova_item: dict[str, Any] = nova_context["nova_item"]
    primary_name: str = nova_item["primary_name"]
    hyphenated = _hyphenate(primary_name)
    now = generated_at_timestamp()
    date_str = datetime.now(UTC).strftime("%Y%m%d")

    bundle_filename = f"{hyphenated}_bundle_{date_str}.zip"

    # Query spectra DataProducts (own DDB query — broader than spectra.json)
    data_products = _query_spectra_data_products(nova_id, table)

    # Fetch photometry from context
    photometry_raw: list[dict[str, Any]] = nova_context.get("photometry_raw_items", [])

    # Build the ZIP on disk
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip", dir="/tmp")
    os.close(tmp_fd)

    spectra_included: list[dict[str, Any]] = []
    spectra_skipped = 0
    bundle_files: list[str] = []

    try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
            # 1. README.txt
            readme = _build_readme(
                nova_item=nova_item,
                hyphenated=hyphenated,
                now=now,
                data_products=data_products,
                photometry_count=len(photometry_raw),
                references_count=nova_context.get("references_count", 0),
            )
            zf.writestr("README.txt", readme)
            bundle_files.append("README.txt")

            # 2. Spectra FITS files
            for dp in data_products:
                dp_id: str = dp["data_product_id"]
                s3_key = f"raw/spectra/{nova_id}/{dp_id}/primary.fits"
                try:
                    resp = s3_client.get_object(Bucket=private_bucket, Key=s3_key)
                    fits_bytes: bytes = resp["Body"].read()
                except ClientError as exc:
                    _logger.warning(
                        "Spectrum FITS missing in S3, skipping",
                        extra={
                            "nova_id": nova_id,
                            "data_product_id": dp_id,
                            "error": str(exc),
                        },
                    )
                    spectra_skipped += 1
                    continue

                spectrum_filename = _spectrum_filename(hyphenated, dp)
                arc_path = f"spectra/{spectrum_filename}"
                zf.writestr(arc_path, fits_bytes)
                bundle_files.append(arc_path)
                spectra_included.append(
                    {
                        "data_product_id": dp_id,
                        "bundle_filename": spectrum_filename,
                        "provider": dp.get("provider", "unknown"),
                        "telescope": dp.get("telescope") or "unknown",
                        "instrument": dp.get("instrument") or "unknown",
                        "epoch_mjd": _decimal_to_float(dp.get("observation_date_mjd")),
                        "bibcode": dp.get("bibcode"),
                        "data_url": dp.get("data_url"),
                        "data_rights": dp.get("data_rights", "public"),
                    }
                )

            # 3. Photometry FITS table
            photometry_rows = len(photometry_raw)
            if photometry_rows > 0:
                phot_fits_bytes = _build_photometry_fits(photometry_raw)
                phot_filename = f"{hyphenated}_photometry.fits"
                zf.writestr(phot_filename, phot_fits_bytes)
                bundle_files.append(phot_filename)

            # 4. Metadata JSON
            metadata = _build_metadata(
                nova_item=nova_item,
                hyphenated=hyphenated,
                now=now,
                spectra_count=len(spectra_included),
                photometry_count=photometry_rows,
                references_count=nova_context.get("references_count", 0),
            )
            meta_filename = f"{hyphenated}_metadata.json"
            zf.writestr(meta_filename, _json_dumps(metadata))
            bundle_files.append(meta_filename)

            # 5. Sources JSON
            sources = _build_sources(
                nova_id=nova_id,
                now=now,
                spectra_included=spectra_included,
                photometry_raw=photometry_raw,
            )
            sources_filename = f"{hyphenated}_sources.json"
            zf.writestr(sources_filename, _json_dumps(sources))
            bundle_files.append(sources_filename)

            # 6. References BibTeX
            references_output: list[dict[str, Any]] = nova_context.get("references_output", [])
            bib = _build_bibtex(references_output)
            bib_filename = f"{hyphenated}_references.bib"
            zf.writestr(bib_filename, bib)
            bundle_files.append(bib_filename)

        # Upload ZIP to S3 under the release prefix (§12.4, §12.5).
        s3_key = f"{s3_key_prefix}nova/{nova_id}/{bundle_filename}"
        s3_client.upload_file(
            tmp_path,
            public_bucket,
            s3_key,
            ExtraArgs={
                "ContentType": "application/zip",
                "ContentDisposition": f'attachment; filename="{bundle_filename}"',
            },
        )

        _logger.info(
            "Bundle uploaded to S3",
            extra={
                "nova_id": nova_id,
                "s3_key": s3_key,
                "spectra_included": len(spectra_included),
                "spectra_skipped": spectra_skipped,
                "photometry_rows": photometry_rows,
            },
        )

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return {
        "s3_key": s3_key,
        "bundle_filename": bundle_filename,
        "spectra_included": len(spectra_included),
        "spectra_skipped": spectra_skipped,
        "photometry_rows": photometry_rows,
        "references_count": len(references_output),
        "files": bundle_files,
    }


# ---------------------------------------------------------------------------
# DynamoDB query
# ---------------------------------------------------------------------------


def _query_spectra_data_products(
    nova_id: str,
    table: Any,
) -> list[dict[str, Any]]:
    """Query all VALID SPECTRA DataProduct items for the nova."""
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
        ),
        "FilterExpression": Attr("validation_status").eq("VALID"),
    }
    while True:
        response: dict[str, Any] = table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if last_key is None:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------


def _hyphenate(name: str) -> str:
    """Replace spaces with hyphens for filesystem-safe names."""
    return name.replace(" ", "-")


def _spectrum_filename(hyphenated: str, dp: dict[str, Any]) -> str:
    """Build the ADR-014 spectrum filename from a DataProduct item."""
    provider = dp.get("provider", "unknown") or "unknown"
    telescope = dp.get("telescope", "unknown") or "unknown"
    instrument = dp.get("instrument", "unknown") or "unknown"
    epoch_mjd = dp.get("observation_date_mjd")
    epoch_str = f"{float(epoch_mjd):.4f}" if epoch_mjd is not None else "unknown"
    return f"{hyphenated}_spectrum_{provider}_{telescope}_{instrument}_{epoch_str}.fits"


# ---------------------------------------------------------------------------
# Decimal / JSON helpers
# ---------------------------------------------------------------------------


def _decimal_to_float(value: Any) -> float | None:
    """Convert a DynamoDB Decimal to float, or return None."""
    if value is None:
        return None
    return float(value)


class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that converts Decimal to float/int."""

    def default(self, o: object) -> Any:
        if isinstance(o, Decimal):
            if o == int(o):
                return int(o)
            return float(o)
        return super().default(o)


def _json_dumps(obj: Any) -> str:
    """Serialize to pretty JSON, handling Decimals."""
    return json.dumps(obj, cls=_DecimalEncoder, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# README builder
# ---------------------------------------------------------------------------


def _build_readme(
    *,
    nova_item: dict[str, Any],
    hyphenated: str,
    now: str,
    data_products: list[dict[str, Any]],
    photometry_count: int,
    references_count: int,
) -> str:
    """Generate the plain-text README for the bundle."""
    primary_name: str = nova_item["primary_name"]
    aliases: list[str] = nova_item.get("aliases", [])
    ra_deg = float(nova_item["ra_deg"])
    dec_deg = float(nova_item["dec_deg"])
    ra_str, dec_str = format_coordinates(ra_deg, dec_deg)
    discovery_date: str | None = nova_item.get("discovery_date")

    alias_line = ", ".join(aliases) if aliases else "(none)"
    disco_line = discovery_date if discovery_date else "Unknown"

    lines = [
        f"{'=' * 60}",
        f"  {primary_name} — NovaCat Data Bundle",
        f"{'=' * 60}",
        "",
        "Nova Identity",
        f"  Primary name:    {primary_name}",
        f"  Aliases:         {alias_line}",
        f"  RA (J2000):      {ra_str}",
        f"  Dec (J2000):     {dec_str}",
        f"  Discovery date:  {disco_line}",
        "",
        "Bundle Information",
        f"  Generated:       {now}",
        f"  Spectra files:   {len(data_products)}",
        f"  Photometry rows: {photometry_count}",
        f"  References:      {references_count}",
        "",
        "File Inventory",
        "  README.txt                       This file",
        f"  {hyphenated}_metadata.json       Nova properties and counts",
        f"  {hyphenated}_sources.json        Data provenance records",
        f"  {hyphenated}_references.bib      BibTeX references",
    ]
    if photometry_count > 0:
        lines.append(f"  {hyphenated}_photometry.fits     Consolidated photometry table")
    lines.append("  spectra/                         Raw spectra FITS files")
    lines.extend(
        [
            "",
            "Format Descriptions",
            "  Spectra FITS: IVOA Spectrum DM v1.2, BINTABLE with WAVELENGTH",
            "  and FLUX columns in original flux units.",
            "  Photometry FITS: BINTABLE with columns TIME_MJD, BAND_ID,",
            "  BAND_NAME, REGIME, MAGNITUDE, MAG_ERR, FLUX_DENSITY,",
            "  FLUX_DENSITY_ERR, FLUX_DENSITY_UNIT, IS_UPPER_LIMIT,",
            "  TELESCOPE, INSTRUMENT, OBSERVER, ORIG_CATALOG, BIBCODE,",
            "  BAND_RES_TYPE, BAND_RES_CONF.",
            "",
            "Citation Guidance",
            "  If you use data from this bundle in a publication, please",
            f"  cite the original data sources listed in {hyphenated}_references.bib.",
            "  We also ask that you cite the Open Nova Catalog itself.",
            "",
            "Nova Page",
            f"  https://aws-nova-cat.vercel.app/nova/{hyphenated}",
            "",
            "Contact",
            "  To report issues or contribute data, visit the catalog",
            "  website or open an issue on the project repository.",
        ]
    )
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Metadata JSON builder
# ---------------------------------------------------------------------------


def _build_metadata(
    *,
    nova_item: dict[str, Any],
    hyphenated: str,
    now: str,
    spectra_count: int,
    photometry_count: int,
    references_count: int,
) -> dict[str, Any]:
    """Build the _metadata.json content (§10.6)."""
    ra_deg = float(nova_item["ra_deg"])
    dec_deg = float(nova_item["dec_deg"])
    ra_str, dec_str = format_coordinates(ra_deg, dec_deg)
    return {
        "nova_id": nova_item["nova_id"],
        "primary_name": nova_item["primary_name"],
        "aliases": nova_item.get("aliases", []),
        "ra": ra_str,
        "dec": dec_str,
        "discovery_date": nova_item.get("discovery_date"),
        "nova_type": nova_item.get("nova_type"),
        "spectra_count": spectra_count,
        "photometry_count": photometry_count,
        "references_count": references_count,
        "generated_at": now,
    }


# ---------------------------------------------------------------------------
# Sources JSON builder
# ---------------------------------------------------------------------------


def _build_sources(
    *,
    nova_id: str,
    now: str,
    spectra_included: list[dict[str, Any]],
    photometry_raw: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the _sources.json content (§10.7)."""
    # Photometry provenance: aggregate by source key (bibcode or orig_catalog)
    phot_sources = _aggregate_photometry_sources(photometry_raw)

    return {
        "nova_id": nova_id,
        "generated_at": now,
        "spectra": spectra_included,
        "photometry_sources": phot_sources,
    }


def _aggregate_photometry_sources(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Aggregate photometry rows by source (bibcode or orig_catalog)."""
    # Key: (bibcode, orig_catalog) → accumulator
    accum: dict[tuple[str | None, str | None], dict[str, Any]] = {}

    for row in rows:
        bibcode: str | None = row.get("bibcode")
        orig_catalog: str | None = row.get("orig_catalog")
        source_key = (bibcode, orig_catalog)

        if source_key not in accum:
            accum[source_key] = {
                "bibcode": bibcode,
                "orig_catalog": orig_catalog,
                "observation_count": 0,
                "regimes": set(),
                "bands": set(),
            }

        entry = accum[source_key]
        entry["observation_count"] += 1
        regime = row.get("regime")
        if regime:
            entry["regimes"].add(regime)
        band_name = row.get("band_name") or ""
        if not band_name:
            # Fallback for pre-migration rows without stored band_name.
            band_id = row.get("band_id", "")
            band_name = band_id.rsplit("_", 1)[-1] if "_" in band_id else band_id
        if band_name:
            entry["bands"].add(band_name)

    result: list[dict[str, Any]] = []
    for entry in accum.values():
        result.append(
            {
                "bibcode": entry["bibcode"],
                "orig_catalog": entry["orig_catalog"],
                "observation_count": entry["observation_count"],
                "regimes": sorted(entry["regimes"]),
                "bands": sorted(entry["bands"]),
                "data_url": None,
                "data_rights": "public",
            }
        )
    return result


# ---------------------------------------------------------------------------
# BibTeX builder
# ---------------------------------------------------------------------------


def _build_bibtex(references: list[dict[str, Any]]) -> str:
    """Build BibTeX entries from references_output (§10.8)."""
    entries: list[str] = []
    for ref in references:
        bibcode: str = ref.get("bibcode", "unknown")
        lines = [f"@article{{{bibcode},"]

        authors: list[str] = ref.get("authors", [])
        if authors:
            author_str = " and ".join(authors)
            lines.append(f"  author  = {{{author_str}}},")

        title: str | None = ref.get("title")
        if title:
            lines.append(f"  title   = {{{title}}},")

        year: int | None = ref.get("year")
        if year is not None:
            lines.append(f"  year    = {{{year}}},")

        # bibcode as non-standard convenience field
        lines.append(f"  bibcode = {{{bibcode}}},")

        doi: str | None = ref.get("doi")
        if doi:
            lines.append(f"  doi     = {{{doi}}},")

        # Close the entry — replace trailing comma on last field
        last = lines[-1]
        if last.endswith(","):
            lines[-1] = last[:-1]
        lines.append("}")
        entries.append("\n".join(lines))

    return "\n\n".join(entries) + ("\n" if entries else "")


# ---------------------------------------------------------------------------
# Photometry FITS builder
# ---------------------------------------------------------------------------


def _build_photometry_fits(rows: list[dict[str, Any]]) -> bytes:
    """Build a consolidated photometry FITS BINTABLE (§10.5)."""

    # Extract columns, converting Decimals to native types
    obs_ids: list[str] = []
    time_mjds: list[float] = []
    band_ids: list[str] = []
    band_names: list[str] = []
    regimes: list[str] = []
    magnitudes: list[float] = []
    mag_errs: list[float] = []
    flux_densities: list[float] = []
    flux_density_errs: list[float] = []
    flux_density_units: list[str] = []
    is_upper_limits: list[bool] = []
    telescopes: list[str] = []
    instruments: list[str] = []
    observers: list[str] = []
    orig_catalogs: list[str] = []
    bibcodes: list[str] = []
    band_res_types: list[str] = []
    band_res_confs: list[str] = []

    nan = float("nan")

    for row in rows:
        obs_ids.append(str(row.get("row_id", "")))
        time_mjds.append(_decimal_to_float(row.get("time_mjd")) or nan)

        bid: str = row.get("band_id", "")
        band_ids.append(bid)
        # Short display label from last segment
        band_names.append(bid.rsplit("_", 1)[-1] if "_" in bid else bid)

        regimes.append(str(row.get("regime", "")))
        magnitudes.append(_decimal_to_float(row.get("magnitude")) or nan)
        mag_errs.append(_decimal_to_float(row.get("mag_err")) or nan)
        flux_densities.append(_decimal_to_float(row.get("flux_density")) or nan)
        flux_density_errs.append(_decimal_to_float(row.get("flux_density_err")) or nan)
        flux_density_units.append(str(row.get("flux_density_unit", "") or ""))
        is_upper_limits.append(bool(row.get("is_upper_limit", False)))
        telescopes.append(str(row.get("telescope", "") or ""))
        instruments.append(str(row.get("instrument", "") or ""))
        observers.append(str(row.get("observer", "") or ""))
        orig_catalogs.append(str(row.get("orig_catalog", "") or ""))
        bibcodes.append(str(row.get("bibcode", "") or ""))
        band_res_types.append(str(row.get("band_res_type", "") or ""))
        band_res_confs.append(str(row.get("band_res_conf", "") or ""))

    # Determine max string lengths for FITS columns (minimum 1 to avoid 0-width)
    def _max_len(values: list[str]) -> str:
        length = max((len(v) for v in values), default=1)
        return str(max(length, 1)) + "A"

    columns = [
        fits.Column(name="OBS_ID", format=_max_len(obs_ids), array=obs_ids),
        fits.Column(name="TIME_MJD", format="D", array=time_mjds),
        fits.Column(name="BAND_ID", format=_max_len(band_ids), array=band_ids),
        fits.Column(name="BAND_NAME", format=_max_len(band_names), array=band_names),
        fits.Column(name="REGIME", format=_max_len(regimes), array=regimes),
        fits.Column(name="MAGNITUDE", format="D", array=magnitudes),
        fits.Column(name="MAG_ERR", format="D", array=mag_errs),
        fits.Column(name="FLUX_DENSITY", format="D", array=flux_densities),
        fits.Column(name="FLUX_DENSITY_ERR", format="D", array=flux_density_errs),
        fits.Column(
            name="FLUX_DENSITY_UNIT",
            format=_max_len(flux_density_units),
            array=flux_density_units,
        ),
        fits.Column(name="IS_UPPER_LIMIT", format="L", array=is_upper_limits),
        fits.Column(name="TELESCOPE", format=_max_len(telescopes), array=telescopes),
        fits.Column(name="INSTRUMENT", format=_max_len(instruments), array=instruments),
        fits.Column(name="OBSERVER", format=_max_len(observers), array=observers),
        fits.Column(
            name="ORIG_CATALOG",
            format=_max_len(orig_catalogs),
            array=orig_catalogs,
        ),
        fits.Column(name="BIBCODE", format=_max_len(bibcodes), array=bibcodes),
        fits.Column(
            name="BAND_RES_TYPE",
            format=_max_len(band_res_types),
            array=band_res_types,
        ),
        fits.Column(
            name="BAND_RES_CONF",
            format=_max_len(band_res_confs),
            array=band_res_confs,
        ),
    ]

    hdu = fits.BinTableHDU.from_columns(columns)
    hdu.header["EXTNAME"] = "PHOTOMETRY"

    buf = io.BytesIO()
    hdu_list = fits.HDUList([fits.PrimaryHDU(), hdu])
    hdu_list.writeto(buf)
    return buf.getvalue()
