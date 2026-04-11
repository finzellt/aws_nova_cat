"""
adapters/eso.py

ESO SSAP (Simple Spectral Access Protocol) adapter for spectra discovery.

Implements SpectraDiscoveryAdapter for the ESO public archive at
archive.eso.org/ssap.

Query strategy:
    Cone search centered on the nova's ICRS coordinates, using FK5 as the
    wire format (the ESO SSAP service expects FK5; offset from ICRS is
    < 20 mas and negligible relative to the cone size).

    Search cone diameter: 0.001 deg (= 3.6 arcsec). Tight enough to avoid
    contamination from nearby unrelated sources; generous enough to
    accommodate typical nova positional uncertainty.

Identity strategy (per ADR-003):
    NATIVE_ID    — CREATORDID field is present and non-empty. This is the
                   ESO-assigned dataset identifier and is stable across
                   re-queries. Preferred.
    METADATA_KEY — CREATORDID absent; fall back to normalized access_url.
    WEAK         — neither CREATORDID nor access_url present; record is
                   returned as WEAK and the handler will assign a uuid4().
                   Definitive dedup is deferred to byte-fingerprint
                   resolution in acquire_and_validate_spectra.

Hints preserved for downstream FITS profile selection:
    collection  — ESO instrument collection string (e.g. "UVES", "HARPS")
    specrp      — spectral resolving power
    snr         — signal-to-noise ratio
    t_min_mjd   — observation start (MJD)
    t_max_mjd   — observation end (MJD)
    em_min_m    — wavelength minimum (metres, per SSAP standard)
    em_max_m    — wavelength maximum (metres, per SSAP standard)

Dependencies:
    pyvo >= 1.4    — VO protocol client (SSAP queries)
    astropy >= 5.0 — SkyCoord, Quantity
"""

from __future__ import annotations

import math
import time
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse, urlunparse

import pyvo as vo  # type: ignore[import-untyped]
from astropy.coordinates import SkyCoord  # type: ignore[import-untyped]
from astropy.units import Quantity  # type: ignore[import-untyped]
from nova_common.errors import RetryableError
from nova_common.logging import logger

# Search cone diameter in degrees.
# 0.001 deg = 3.6 arcsec diameter / 1.8 arcsec radius.
_SEARCH_DIAMETER_DEG = (
    20 / 3600
)  # 20 arcsec diameter, divided by 2 for radius, converted to degrees

_SSAP_ENDPOINT = "http://archive.eso.org/ssap"

# Per-request HTTP timeout in seconds.
_REQUEST_TIMEOUT_S = 15

# Application-level retry parameters for the SSAP query.
_MAX_QUERY_ATTEMPTS = 3
_QUERY_RETRY_DELAY_S = 3

# SSAP fields extracted from each ESO result row.
_SSAP_FIELDS = [
    "COLLECTION",
    "TARGETNAME",
    "s_ra",
    "s_dec",
    "em_min",
    "em_max",
    "SPECRP",
    "SNR",
    "t_min",
    "t_max",
    "CREATORDID",
    "access_url",
]


class ESOAdapter:
    """
    ESO SSAP spectra discovery adapter.

    Satisfies SpectraDiscoveryAdapter Protocol.
    """

    provider: str = "ESO"

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
        Execute an ESO SSAP cone search and return raw result records.

        ESO uses coordinate-based cone search exclusively; primary_name and
        aliases are accepted for Protocol compatibility but not used.

        Raises RetryableError on any network or service failure so the
        handler's retry policy can engage.
        """
        ssap_service = vo.dal.SSAService(_SSAP_ENDPOINT)
        ssap_service._session.timeout = _REQUEST_TIMEOUT_S
        pos = SkyCoord(ra_deg, dec_deg, unit="deg")
        size = Quantity(_SEARCH_DIAMETER_DEG, unit="deg")

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_QUERY_ATTEMPTS + 1):
            try:
                resultset = ssap_service.search(pos=pos.fk5, diameter=size)
                break
            except Exception as exc:
                last_exc = exc
                if attempt < _MAX_QUERY_ATTEMPTS:
                    logger.warning(
                        "ESO SSAP query attempt %d/%d failed, retrying in %ds",
                        attempt,
                        _MAX_QUERY_ATTEMPTS,
                        _QUERY_RETRY_DELAY_S,
                        extra={"nova_id": nova_id, "error": str(exc)},
                    )
                    time.sleep(_QUERY_RETRY_DELAY_S)
        else:
            raise RetryableError(
                f"ESO SSAP query failed for nova_id={nova_id!r} ra={ra_deg} dec={dec_deg}: {last_exc}"
            ) from last_exc

        raw_products: list[dict[str, Any]] = []
        for row in resultset:
            raw: dict[str, Any] = {}
            for field in _SSAP_FIELDS:
                try:
                    raw[field] = _sanitize_value(row[field])
                except Exception:
                    raw[field] = None
            raw_products.append(raw)

        logger.info(
            "ESO SSAP query complete",
            extra={"nova_id": nova_id, "result_count": len(raw_products)},
        )
        return raw_products

    def normalize(
        self,
        *,
        nova_id: str,
        raw: dict,
    ) -> dict | None:
        """
        Normalize one raw ESO SSAP row into Nova Cat's internal
        discovered-product shape.

        Returns None for records that cannot be safely normalized.
        """
        access_url: str | None = raw.get("access_url")
        creator_did: str | None = raw.get("CREATORDID")

        # Determine identity strategy and derive locator_identity.
        if creator_did and str(creator_did).strip():
            provider_product_key: str | None = str(creator_did).strip()
            locator_identity = f"provider_product_id:{provider_product_key}"
            identity_strategy = "NATIVE_ID"
        elif access_url:
            normalized_url = _normalize_url(access_url)
            if not normalized_url:
                logger.warning(
                    "ESO record has unparseable access_url — skipping",
                    extra={"access_url": access_url, "nova_id": nova_id},
                )
                return None
            provider_product_key = None
            locator_identity = f"url:{normalized_url}"
            identity_strategy = "METADATA_KEY"
        else:
            # Neither CREATORDID nor access_url — cannot construct any identity.
            logger.warning(
                "ESO record missing both CREATORDID and access_url — skipping",
                extra={"targetname": raw.get("TARGETNAME"), "nova_id": nova_id},
            )
            return None

        if not access_url:
            # CREATORDID present but no download URL — we know the product
            # exists but cannot acquire it yet. Return as WEAK so it is
            # stubbed and can be re-evaluated when a locator is available.
            logger.warning(
                "ESO record has CREATORDID but no access_url — WEAK identity",
                extra={"creator_did": creator_did, "nova_id": nova_id},
            )
            locators = []
            identity_strategy = "WEAK"
        else:
            locators = [{"kind": "URL", "role": "PRIMARY", "value": access_url}]

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
# Module-level helpers (private)
# ------------------------------------------------------------------


def _sanitize_value(value: Any) -> Any:
    """
    Convert a pyvo/astropy SSAP field value to a JSON-safe Python type.

    bytes        → decode to str (strip whitespace)
    numpy scalar → .item() to Python native
    nan/inf      → None (not JSON-serializable)
    everything else → unchanged
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").strip() or None
    if hasattr(value, "item"):  # numpy scalar
        value = value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _normalize_url(url: str) -> str | None:
    """
    Produce a canonical URL form for use as a locator_identity key.

    Normalises: lowercase scheme + host, strips fragment.
    Preserves: path, params, query string (all content-significant for ESO).
    Returns None if the URL cannot be parsed.
    """
    try:
        parsed = urlparse(url.strip())
        normalised = urlunparse(
            (
                parsed.scheme.lower(),
                parsed.netloc.lower(),
                parsed.path,
                parsed.params,
                parsed.query,
                "",  # strip fragment
            )
        )
        return normalised or None
    except Exception:
        return None


def _extract_hints(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Extract provider metadata hints for downstream FITS profile selection.

    ESO SSAP wavelength values (em_min, em_max) are in metres per the
    SSAP standard. Stored with explicit unit label in the key name.

    Float values are converted to Decimal for DynamoDB compatibility.
    nan/inf are dropped — they are not representable in DynamoDB.
    """
    hints: dict[str, Any] = {}

    _maybe_set(hints, "collection", raw.get("COLLECTION"), str)
    _maybe_set_numeric(hints, "specrp", raw.get("SPECRP"))
    _maybe_set_numeric(hints, "snr", raw.get("SNR"))
    _maybe_set_numeric(hints, "t_min_mjd", raw.get("t_min"))
    _maybe_set_numeric(hints, "t_max_mjd", raw.get("t_max"))
    _maybe_set_numeric(hints, "em_min_m", raw.get("em_min"))
    _maybe_set_numeric(hints, "em_max_m", raw.get("em_max"))

    return hints


def _maybe_set(hints: dict, key: str, value: Any, cast: type) -> None:
    """Set hints[key] = cast(value) if value is truthy."""
    if value is not None and value != "":
        hints[key] = cast(value)


def _maybe_set_numeric(hints: dict, key: str, value: Any) -> None:
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
    except Exception:
        pass
