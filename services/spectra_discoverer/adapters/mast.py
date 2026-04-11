"""
adapters/mast.py

MAST (Mikulski Archive for Space Telescopes) adapter for spectra discovery.

Implements SpectraDiscoveryAdapter for HST HASP (Hubble Advanced Spectral
Products) co-added spectra served via the MAST archive at mast.stsci.edu.

Query strategy:
    Name-based object search via astroquery.mast.Observations.query_object()
    with a 1-arcmin search radius. MAST internally resolves the target name
    to coordinates (via SIMBAD/NED) and performs a cone search. This approach
    is required because HST Target-of-Opportunity (ToO) observations often
    have significant coordinate offsets from SIMBAD positions, causing pure
    coordinate-based queries to miss targets.

    Fallback: if the primary name yields zero HASP observations, each alias
    is tried in order. The first name that produces results wins. This handles
    cases where HST proposals used a pre-designation name (e.g. "NOVADEL2013"
    instead of "V339 Del").

    Two-step discovery:
      1. query_object() → observation-level results
         Filter: obs_collection == "HST", dataproduct_type == "spectrum",
                 project == "HASP"
      2. get_product_list() → data-product-level results
         Filter: productFilename ends with "_cspec.fits"

    HASP cspec files are per-visit co-added spectra — safe for transient
    science because all constituent exposures are from the same epoch
    (single HST visit = single scheduling block).

Identity strategy (per ADR-003):
    NATIVE_ID — productFilename is a stable, unique identifier assigned by
    the HASP pipeline. Example:
      "hst_13388_stis_novadel2013_e140m_oc7r06_cspec.fits"

    The filename encodes: program ID, instrument, target, grating, visit ID.
    It is deterministic and does not change across re-queries.

Hints preserved for downstream FITS profile selection:
    instrument       — HST instrument name (e.g. "STIS", "COS/FUV")
    target_name      — MAST target name (as used by the HST proposal)
    proposal_id      — HST program number
    obs_id           — MAST observation ID
    t_min_mjd        — observation start (MJD)
    t_max_mjd        — observation end (MJD)

Dependencies:
    astroquery >= 0.4 — MAST query client (Observations)
"""

from __future__ import annotations

import math
import time
from decimal import Decimal
from typing import Any

from astroquery.mast import Observations  # type: ignore[import-untyped]
from nova_common.errors import RetryableError
from nova_common.logging import logger

# MAST download URL template. dataURI is the unique file identifier returned
# by get_product_list(). No authentication required for public HST data.
_MAST_DOWNLOAD_URL_TEMPLATE = "https://mast.stsci.edu/api/v0.1/Download/file?uri={data_uri}"

# Search radius for query_object. 1 arcmin accommodates HST ToO coordinate
# offsets while remaining tight enough to avoid unrelated sources.
_SEARCH_RADIUS = "1 arcmin"

# Application-level retry parameters for MAST queries.
_MAX_QUERY_ATTEMPTS = 3
_QUERY_RETRY_DELAY_S = 3

# Polite delay between successive MAST API calls (seconds).
_INTER_CALL_DELAY_S = 0.5


class MASTAdapter:
    """
    MAST HASP spectra discovery adapter.

    Satisfies SpectraDiscoveryAdapter Protocol.
    """

    provider: str = "MAST"

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    def query(
        self,
        *,
        nova_id: str,
        ra_deg: float,
        dec_deg: float,
        primary_name: str | None = None,
        aliases: list[str] | None = None,
    ) -> list[dict]:
        """
        Query MAST for HASP cspec data products associated with the nova.

        Uses name-based search (query_object) with primary_name, falling
        back to aliases if the primary name yields zero HASP observations.
        Coordinates (ra_deg, dec_deg) are accepted for Protocol compatibility
        but not used directly — MAST resolves names to coordinates internally.

        Returns a list of raw product dicts, each enriched with observation-
        level metadata (instrument, target_name, proposal_id, t_min, t_max).

        Raises RetryableError on transient MAST service failures.
        Raises ValueError if no names are available for querying.
        """
        if not primary_name:
            raise ValueError(
                f"MAST adapter requires primary_name for nova_id={nova_id!r}. "
                "Coordinate-only queries are not supported."
            )

        # Build ordered list of names to try: primary first, then aliases.
        names_to_try = [primary_name]
        if aliases:
            names_to_try.extend(alias for alias in aliases if alias != primary_name)

        # Try each name until we get HASP observations.
        hasp_obs = None
        matched_name: str | None = None
        for name in names_to_try:
            obs = self._query_mast_observations(nova_id=nova_id, name=name)
            if obs is not None and len(obs) > 0:
                hasp_obs = obs
                matched_name = name
                logger.info(
                    "MAST query matched on name",
                    extra={
                        "nova_id": nova_id,
                        "matched_name": name,
                        "hasp_obs_count": len(obs),
                    },
                )
                break

            logger.info(
                "MAST query returned no HASP observations for name",
                extra={"nova_id": nova_id, "tried_name": name},
            )
            time.sleep(_INTER_CALL_DELAY_S)

        if hasp_obs is None or len(hasp_obs) == 0:
            logger.info(
                "MAST: no HASP observations found for any name",
                extra={
                    "nova_id": nova_id,
                    "names_tried": names_to_try,
                },
            )
            return []

        # Step 2: Get data products for the matched HASP observations.
        products = self._get_cspec_products(nova_id=nova_id, observations=hasp_obs)

        # Enrich each product with observation-level metadata by joining
        # on obsID. Build an obs_id → observation-metadata lookup first.
        obs_lookup: dict[str, dict[str, Any]] = {}
        for row in hasp_obs:
            obs_id_key = str(row["obsid"])
            obs_lookup[obs_id_key] = {
                "instrument_name": str(row["instrument_name"]),
                "target_name": str(row["target_name"]),
                "proposal_id": str(row["proposal_id"]),
                "obs_id": str(row["obs_id"]),
                "t_min": _safe_float(row.get("t_min")),
                "t_max": _safe_float(row.get("t_max")),
            }

        raw_products: list[dict[str, Any]] = []
        for prod in products:
            product_filename = str(prod["productFilename"])
            data_uri = str(prod["dataURI"])
            obs_id_key = str(prod["obsID"])

            raw: dict[str, Any] = {
                "productFilename": product_filename,
                "dataURI": data_uri,
                "obsID": obs_id_key,
                "size": _safe_int(prod.get("size")),
                "productSubGroupDescription": str(prod.get("productSubGroupDescription", "")),
            }

            # Merge observation-level metadata.
            obs_meta = obs_lookup.get(obs_id_key, {})
            raw.update(obs_meta)

            raw_products.append(raw)

        logger.info(
            "MAST discovery complete",
            extra={
                "nova_id": nova_id,
                "matched_name": matched_name,
                "hasp_obs_count": len(hasp_obs),
                "cspec_product_count": len(raw_products),
            },
        )
        return raw_products

    def normalize(
        self,
        *,
        nova_id: str,
        raw: dict,
    ) -> dict | None:
        """
        Normalize one raw MAST product record into Nova Cat's internal
        discovered-product shape.

        Returns None for records that cannot be safely normalized.
        """
        product_filename: str | None = raw.get("productFilename")
        data_uri: str | None = raw.get("dataURI")

        if not product_filename:
            logger.warning(
                "MAST record missing productFilename — skipping",
                extra={"nova_id": nova_id, "raw_keys": list(raw.keys())},
            )
            return None

        if not data_uri:
            logger.warning(
                "MAST record missing dataURI — skipping",
                extra={
                    "nova_id": nova_id,
                    "productFilename": product_filename,
                },
            )
            return None

        # Identity: NATIVE_ID using productFilename as the stable key.
        provider_product_key = product_filename
        locator_identity = f"provider_product_id:{product_filename}"
        identity_strategy = "NATIVE_ID"

        # Construct download URL from dataURI.
        download_url = _MAST_DOWNLOAD_URL_TEMPLATE.format(data_uri=data_uri)
        locators = [{"kind": "URL", "role": "PRIMARY", "value": download_url}]

        hints = _extract_hints(raw)

        return {
            "provider": self.provider,
            "nova_id": nova_id,
            "provider_product_key": provider_product_key,
            "locator_identity": locator_identity,
            "identity_strategy": identity_strategy,
            "locators": locators,
            "hints": hints,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _query_mast_observations(
        self,
        *,
        nova_id: str,
        name: str,
    ) -> Any | None:
        """
        Query MAST for HST HASP spectral observations of the named target.

        Returns an astropy Table of matching observations, or None on failure.
        Retries transient errors up to _MAX_QUERY_ATTEMPTS times.
        """
        last_exc: Exception | None = None
        for attempt in range(1, _MAX_QUERY_ATTEMPTS + 1):
            try:
                all_obs = Observations.query_object(name, radius=_SEARCH_RADIUS)
                break
            except Exception as exc:
                last_exc = exc
                if attempt < _MAX_QUERY_ATTEMPTS:
                    logger.warning(
                        "MAST query_object attempt %d/%d failed, retrying in %ds",
                        attempt,
                        _MAX_QUERY_ATTEMPTS,
                        _QUERY_RETRY_DELAY_S,
                        extra={
                            "nova_id": nova_id,
                            "query_name": name,
                            "error": str(exc),
                        },
                    )
                    time.sleep(_QUERY_RETRY_DELAY_S)
        else:
            raise RetryableError(
                f"MAST query_object failed for nova_id={nova_id!r} name={name!r}: {last_exc}"
            ) from last_exc

        if all_obs is None or len(all_obs) == 0:
            return None

        # Filter to HST spectral HASP observations.
        mask = (
            (all_obs["obs_collection"] == "HST")
            & (all_obs["dataproduct_type"] == "spectrum")
            & (_str_upper_col(all_obs["project"]) == "HASP")
        )
        hasp_obs = all_obs[mask]

        return hasp_obs if len(hasp_obs) > 0 else None

    def _get_cspec_products(
        self,
        *,
        nova_id: str,
        observations: Any,
    ) -> list[Any]:
        """
        Retrieve data products for the given observations and filter to
        HASP cspec FITS files only.

        Returns a list of product rows (astropy Table rows or similar).
        """
        time.sleep(_INTER_CALL_DELAY_S)

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_QUERY_ATTEMPTS + 1):
            try:
                products = Observations.get_product_list(observations)
                break
            except Exception as exc:
                last_exc = exc
                if attempt < _MAX_QUERY_ATTEMPTS:
                    logger.warning(
                        "MAST get_product_list attempt %d/%d failed, retrying in %ds",
                        attempt,
                        _MAX_QUERY_ATTEMPTS,
                        _QUERY_RETRY_DELAY_S,
                        extra={"nova_id": nova_id, "error": str(exc)},
                    )
                    time.sleep(_QUERY_RETRY_DELAY_S)
        else:
            raise RetryableError(
                f"MAST get_product_list failed for nova_id={nova_id!r}: {last_exc}"
            ) from last_exc

        if products is None or len(products) == 0:
            logger.info(
                "MAST: no data products returned",
                extra={"nova_id": nova_id},
            )
            return []

        # Filter to cspec FITS files (HASP co-added spectra).
        cspec_products = [
            row
            for row in products
            if str(row.get("productFilename", "")).lower().endswith("_cspec.fits")
        ]

        logger.info(
            "MAST product filtering complete",
            extra={
                "nova_id": nova_id,
                "total_products": len(products),
                "cspec_products": len(cspec_products),
            },
        )
        return cspec_products


# ------------------------------------------------------------------
# Module-level helpers (private)
# ------------------------------------------------------------------


def _str_upper_col(col: Any) -> Any:
    """
    Convert an astropy Table column to uppercase strings for comparison.
    Handles masked/null values gracefully.
    """
    import numpy as np

    result = np.array([str(v).upper() if v is not None else "" for v in col])
    return result


def _safe_float(value: Any) -> float | None:
    """Convert a value to float, returning None for non-finite or missing values."""
    if value is None:
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    """Convert a value to int, returning None for missing values."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_hints(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Extract provider metadata hints for downstream FITS profile selection
    and operator visibility.

    Float values are converted to Decimal for DynamoDB compatibility.
    """
    hints: dict[str, Any] = {}

    _maybe_set_str(hints, "instrument", raw.get("instrument_name"))
    _maybe_set_str(hints, "target_name", raw.get("target_name"))
    _maybe_set_str(hints, "proposal_id", raw.get("proposal_id"))
    _maybe_set_str(hints, "obs_id", raw.get("obs_id"))
    _maybe_set_numeric(hints, "t_min_mjd", raw.get("t_min"))
    _maybe_set_numeric(hints, "t_max_mjd", raw.get("t_max"))

    return hints


def _maybe_set_str(hints: dict[str, Any], key: str, value: Any) -> None:
    """Set hints[key] = str(value) if value is truthy."""
    if value is not None and str(value).strip():
        hints[key] = str(value).strip()


def _maybe_set_numeric(hints: dict[str, Any], key: str, value: Any) -> None:
    """
    Set hints[key] = Decimal(value) if value is a finite number.
    DynamoDB's boto3 resource layer rejects raw Python floats.
    """
    if value is None:
        return
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return
        hints[key] = Decimal(str(f))
    except (TypeError, ValueError):
        pass
