"""
archive_resolver Lambda handler

Queries public astronomical archives to resolve a candidate name to
coordinates and nova classification.

This Lambda is container-based (not zip+layer) due to astropy/astroquery
dependencies. See infra/nova_constructs/compute.py for container config.

Task dispatch table:
  ResolveCandidateAgainstPublicArchives — query SIMBAD (and TNS if needed);
                                          return coordinates + nova classification
                                          + SIMBAD aliases

Resolution strategy:
  1. Query SIMBAD via astroquery — authoritative for named objects
  2. If SIMBAD returns no result, query TNS REST API (plain HTTP)
  3. Merge results; raise QuarantineError if sources conflict

Output contract (consumed by CheckExistingNovaByCoordinates,
CandidateIsNova?/CandidateIsClassicalNova? choice states, and
UpsertMinimalNovaMetadata for alias persistence):
  is_nova           — bool
  is_classical_nova — "true" | "false"
  resolved_ra       — ICRS RA in degrees (present when is_nova=True)
  resolved_dec      — ICRS Dec in degrees (present when is_nova=True)
  resolved_epoch    — always "J2000" for SIMBAD
  resolver_source   — "SIMBAD" | "TNS" | "SIMBAD+TNS" | "NONE"
  aliases           — list of raw alias strings from SIMBAD ids field
                      (present when resolver_source includes "SIMBAD";
                      empty list otherwise). Used by UpsertMinimalNovaMetadata
                      to persist NameMapping items for each alias, and by
                      refresh_references for ADS bibliography lookups.

SIMBAD otypes.otype_txt → nova classification:
  No*, No?, NL*  → is_nova=True, is_classical_nova="true"
  RNe, RN*       → is_nova=True, is_classical_nova="false" (recurrent)
  Anything else  → is_nova=False
  Conflicting    → QuarantineError

SIMBAD alias extraction:
  The ids field is a pipe-delimited string of catalogue identifiers,
  e.g. "V* V1324 Sco|NOVA Sco 2012|Gaia DR3 4043499439062100096".
  Each token is trimmed of whitespace. The "V* " prefix is stripped
  (SIMBAD variable star annotation — not part of the searchable name).
  All other prefixes (Gaia DR3, 2MASS J, NOVA, MOA, etc.) are kept
  verbatim as they are genuine searchable catalogue identifiers.

astropy/astroquery cache:
  Redirected to /tmp at module load — Lambda filesystem is read-only
  except for /tmp. See _bootstrap_astropy().
"""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any, cast


def _bootstrap_astropy(base: str = "/tmp") -> None:
    """Redirect astropy/astroquery cache dirs to /tmp for Lambda compatibility."""
    os.environ.setdefault("ASTROPY_CONFIGDIR", f"{base}/astropy/config")
    os.environ.setdefault("ASTROPY_CACHE_DIR", f"{base}/astropy/cache")
    os.environ.setdefault("ASTROQUERY_CACHE_DIR", f"{base}/astroquery")
    os.environ.setdefault("XDG_CACHE_HOME", f"{base}/.cache")
    os.environ.setdefault("HOME", base)
    for p in (
        os.environ["ASTROPY_CONFIGDIR"],
        os.environ["ASTROPY_CACHE_DIR"],
        os.environ["ASTROQUERY_CACHE_DIR"],
        os.environ["XDG_CACHE_HOME"],
    ):
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)


# Must run before astropy/astroquery imports
_bootstrap_astropy()

from astroquery.simbad import Simbad  # type: ignore[import-untyped]  # noqa: E402
from nova_common.errors import QuarantineError, RetryableError  # noqa: E402
from nova_common.logging import configure_logging, logger  # noqa: E402
from nova_common.tracing import tracer  # noqa: E402

_TNS_API_URL = "https://www.wis-tns.org/api/get/object"
_TNS_API_KEY = os.environ.get("TNS_API_KEY", "")
_HTTP_TIMEOUT = 30

_NOVA_TYPES = {"No*", "No?", "NL*"}
_RECURRENT_TYPES = {"RNe", "RN*"}

# Module-level Simbad client — instantiated once per cold start
_simbad = Simbad()
_simbad.add_votable_fields("ra", "dec", "otypes", "ids")
# _simbad.ROW_LIMIT = 1

with contextlib.suppress(Exception):
    _simbad.TIMEOUT = int(os.getenv("SIMBAD_TIMEOUT_SEC", "30"))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    handler_fn = _TASK_HANDLERS.get(task_name)  # type: ignore[arg-type]
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}")
    return handler_fn(event, context)


# ---------------------------------------------------------------------------
# Task implementations
# ---------------------------------------------------------------------------


@tracer.capture_method
def _resolve_candidate_against_public_archives(
    event: dict[str, Any], context: object
) -> dict[str, Any]:
    """
    Query SIMBAD and TNS to resolve a candidate name.

    Tries SIMBAD first. Falls back to TNS if SIMBAD returns no result.
    Raises QuarantineError if results from both sources conflict.
    """
    candidate_name: str = event["candidate_name"]

    logger.info("Querying public archives", extra={"candidate_name": candidate_name})

    simbad_result = _query_simbad(candidate_name)
    tns_result: dict[str, Any] | None = None

    if simbad_result is None:
        logger.info("SIMBAD returned no result — querying TNS")
        tns_result = _query_tns(candidate_name)

    if simbad_result is None and tns_result is None:
        logger.info("No result from any archive")
        return {
            "is_nova": False,
            "is_classical_nova": "false",
            "resolver_source": "NONE",
            "aliases": [],
        }

    if simbad_result is not None and tns_result is not None:
        result = _merge_results(simbad_result, tns_result)
        result["resolver_source"] = "SIMBAD+TNS"
    elif simbad_result is not None:
        result = simbad_result
        result["resolver_source"] = "SIMBAD"
    else:
        result = cast(dict[str, Any], tns_result)
        result["resolver_source"] = "TNS"
        result.setdefault("aliases", [])

    logger.info(
        "Archive resolution complete",
        extra={
            "is_nova": result.get("is_nova"),
            "is_classical_nova": result.get("is_classical_nova"),
            "resolver_source": result.get("resolver_source"),
            "alias_count": len(result.get("aliases", [])),
        },
    )

    return result


# ---------------------------------------------------------------------------
# Archive queries
# ---------------------------------------------------------------------------


@tracer.capture_method
def _query_simbad(candidate_name: str) -> dict[str, Any] | None:
    """
    Query SIMBAD via astroquery.

    SIMBAD returns one row per object type. We collect all otype_txt values
    across rows and take coordinates and ids from the first row.

    Returns None if no object found. Raises RetryableError on transient
    network failures.
    """
    try:
        tbl = _simbad.query_object(candidate_name)
    except Exception as e:
        err_str = str(e).lower()
        if any(t in err_str for t in ("timeout", "connection", "network", "500", "503")):
            raise RetryableError(f"SIMBAD query failed (transient): {e}") from e
        raise RetryableError(f"SIMBAD query failed: {e}") from e

    if tbl is None or len(tbl) == 0:
        return None

    row0 = tbl[0]

    def _raw(col: str, row: Any = None) -> Any:
        r = row if row is not None else row0
        v = r[col]
        if getattr(v, "mask", False):
            return None
        return v.item() if hasattr(v, "item") else (str(v) if v is not None else None)

    # Collect all object types across all rows
    all_otypes = {
        str(_raw("otypes.otype_txt", r)) for r in tbl if _raw("otypes.otype_txt", r) is not None
    }

    ra = _raw("ra")
    dec = _raw("dec")
    ids_raw = _raw("ids")

    is_nova, is_classical_nova = _classify_otypes(all_otypes)

    result: dict[str, Any] = {
        "is_nova": is_nova,
        "is_classical_nova": is_classical_nova,
        "resolved_epoch": "J2000",
        "aliases": _parse_simbad_ids(ids_raw),
    }

    if is_nova and ra is not None and dec is not None:
        result["resolved_ra"] = float(ra)
        result["resolved_dec"] = float(dec)

    return result


@tracer.capture_method
def _query_tns(candidate_name: str) -> dict[str, Any] | None:
    """
    Query the Transient Name Server for the candidate name.

    Returns None if not found or if TNS_API_KEY is not configured.
    """
    if not _TNS_API_KEY:
        logger.warning("TNS_API_KEY not set — skipping TNS query")
        return None

    payload = json.dumps(
        {
            "api_key": _TNS_API_KEY,
            "data": json.dumps({"objname": candidate_name, "photometry": "0"}),
        }
    ).encode()

    req = urllib.request.Request(
        _TNS_API_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code in (429, 502, 503, 504):
            raise RetryableError(f"TNS HTTP {e.code}") from e
        if e.code == 404:
            return None
        raise RetryableError(f"TNS HTTP {e.code}") from e
    except (urllib.error.URLError, TimeoutError) as e:
        raise RetryableError(f"TNS network error: {e}") from e

    obj = data.get("data", {}).get("reply", {})
    if not obj or obj.get("name") is None:
        return None

    obj_type = str(obj.get("type", {}).get("name", "")).lower()
    ra = obj.get("radeg")
    dec = obj.get("decdeg")

    is_nova = "nova" in obj_type
    is_classical_nova = "true" if (is_nova and "recurrent" not in obj_type) else "false"

    result: dict[str, Any] = {
        "is_nova": is_nova,
        "is_classical_nova": is_classical_nova,
        "resolved_epoch": "J2000",
    }
    if is_nova and ra is not None and dec is not None:
        result["resolved_ra"] = float(ra)
        result["resolved_dec"] = float(dec)

    return result


# ---------------------------------------------------------------------------
# Classification and alias helpers
# ---------------------------------------------------------------------------


def _classify_otypes(otypes: set[str]) -> tuple[bool, str]:
    """
    Classify a set of SIMBAD otype_txt values into nova classification.

    Returns (is_nova, is_classical_nova) where is_classical_nova is:
      "true"  — confirmed classical nova
      "false" — not a nova, or confirmed recurrent nova
    """
    if not otypes:
        return False, "false"

    is_recurrent = bool(otypes & _RECURRENT_TYPES)
    is_nova = bool((otypes & _NOVA_TYPES) or is_recurrent)

    if not is_nova:
        return False, "false"
    if is_recurrent:
        return True, "false"
    return True, "true"


def _parse_simbad_ids(ids_raw: str | None) -> list[str]:
    """
    Parse the SIMBAD ids pipe-delimited string into a clean alias list.

    Rules:
      - Split on "|"
      - Strip leading/trailing whitespace from each token
      - Strip leading "V* " prefix (SIMBAD variable star annotation —
        not part of the searchable name)
      - Drop empty strings

    All other catalogue prefixes (Gaia DR3, 2MASS J, NOVA, MOA, etc.)
    are kept verbatim — they are genuine searchable identifiers.

    Example input:  "V* V1324 Sco|NOVA Sco 2012|Gaia DR3 4043499439062100096"
    Example output: ["V1324 Sco", "NOVA Sco 2012", "Gaia DR3 4043499439062100096"]
    """
    if not ids_raw:
        return []

    aliases = []
    for token in ids_raw.split("|"):
        token = token.strip()
        if token.startswith("V* "):
            token = token[3:]
        token = token.strip()
        if token:
            aliases.append(token)

    return aliases


def _merge_results(
    simbad: dict[str, Any],
    tns: dict[str, Any],
) -> dict[str, Any]:
    """
    Merge SIMBAD and TNS results, raising QuarantineError on conflict.
    When both agree, prefer SIMBAD coordinates and aliases.
    """
    if simbad["is_nova"] != tns["is_nova"]:
        raise QuarantineError(
            f"SIMBAD and TNS disagree on nova classification: "
            f"SIMBAD={simbad['is_nova']}, TNS={tns['is_nova']}"
        )

    result = dict(simbad)
    if not result.get("resolved_ra") and tns.get("resolved_ra"):
        result["resolved_ra"] = tns["resolved_ra"]
        result["resolved_dec"] = tns["resolved_dec"]

    return result


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "ResolveCandidateAgainstPublicArchives": _resolve_candidate_against_public_archives,
}
