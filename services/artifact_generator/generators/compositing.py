"""Spectra compositing pipeline.

Groups same-instrument, same-night spectra into compositing candidates,
cleans and resamples them onto a common wavelength grid, combines them
via median, and persists the results to DynamoDB and S3.  Also computes
deterministic composite identifiers and fingerprints for rebuild
avoidance.

Pure-computation functions (clustering, grid, resampling, combination)
are testable without mocks.  DDB/S3 operations (queries, writes, CSV
uploads) follow the same inline-IO pattern as the other generators.

See ADR-033 for design rationale.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import UTC, datetime
from typing import Any, TypedDict

import numpy as np
from nova_common.spectral import der_snr
from numpy.typing import NDArray

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: MJD gap threshold for night clustering.
#: 0.5 days = 12 hours.  Separates consecutive observing nights at all
#: major professional spectroscopic facilities.
NIGHT_GAP_THRESHOLD_DAYS: float = 0.5

#: Minimum native-resolution data points for a spectrum to be included
#: in a composite.
MIN_POINTS_FOR_COMPOSITE: int = 2000

#: Relative SNR threshold for compositing group membership.
#: A spectrum with SNR below this fraction of the group's median
#: is excluded from the composite but still displayed individually
#: (if above the absolute display floor in spectra.py).
_COMPOSITING_SNR_RELATIVE_THRESHOLD: float = 1.0 / 3.0

#: NovaCat UUID v5 namespace for deterministic composite IDs.
#: Generated once via uuid.uuid4() and frozen here.
_NOVACAT_UUID_NAMESPACE: uuid.UUID = uuid.UUID("7f1b3c5e-8a2d-4e6f-b9c1-d3e5f7a8b0c2")


def _effective_snr_for_product(product: dict[str, Any]) -> float:
    """Compute effective SNR for a compositing candidate.

    Priority: DDB top-level snr → hints.snr → DER_SNR on raw flux.
    Requires ``_raw_fluxes`` to be attached to the product dict
    (set during FITS reading in ``_process_group``).
    """
    snr_val = product.get("snr")
    if snr_val is not None:
        try:
            return float(snr_val)
        except (TypeError, ValueError):
            pass

    hints = product.get("hints")
    if isinstance(hints, dict):
        hints_snr = hints.get("snr")
        if hints_snr is not None:
            try:
                return float(hints_snr)
            except (TypeError, ValueError):
                pass

    raw_fluxes = product.get("_raw_fluxes")
    if raw_fluxes is not None and len(raw_fluxes) > 0:
        return der_snr(raw_fluxes)

    return 0.0


# ---------------------------------------------------------------------------
# Typed structures
# ---------------------------------------------------------------------------


class CompositingGroup(TypedDict):
    """A group of spectra eligible for compositing evaluation.

    All members share the same instrument and observing night.
    """

    instrument: str
    products: list[dict[str, Any]]


class CleanedSpectrum(TypedDict):
    """A cleaned spectrum ready for resampling.

    Wavelengths and fluxes are parallel arrays of equal length,
    monotonically increasing in wavelength, with detector artifacts
    removed.
    """

    data_product_id: str
    wavelengths: NDArray[np.float64]
    fluxes: NDArray[np.float64]


# ---------------------------------------------------------------------------
# Night clustering
# ---------------------------------------------------------------------------


def cluster_by_night(
    products: list[dict[str, Any]],
    gap_threshold: float = NIGHT_GAP_THRESHOLD_DAYS,
) -> list[list[dict[str, Any]]]:
    """Group DataProduct dicts into observing nights by MJD gap detection.

    Single-linkage clustering on ``observation_date_mjd`` with a
    configurable gap threshold.

    Parameters
    ----------
    products:
        DataProduct-like dicts.  Each must contain an
        ``observation_date_mjd`` key with a numeric value (``float``,
        ``int``, or ``Decimal``).
    gap_threshold:
        Maximum MJD difference between consecutive spectra within the
        same night.  Defaults to 0.5 days (12 hours).

    Returns
    -------
    list[list[dict]]:
        Groups of products, each representing one observing night.
        Within each group, products are sorted by MJD ascending.
        Groups are returned in chronological order (earliest night
        first).

    Raises
    ------
    ValueError:
        If *products* is empty or any product lacks
        ``observation_date_mjd``.
    """
    if not products:
        raise ValueError("products must not be empty")

    # Sort by MJD.  Convert Decimal → float defensively.
    sorted_prods = sorted(products, key=lambda p: float(p["observation_date_mjd"]))

    groups: list[list[dict[str, Any]]] = [[sorted_prods[0]]]
    for prev, curr in zip(sorted_prods, sorted_prods[1:], strict=False):
        gap = float(curr["observation_date_mjd"]) - float(prev["observation_date_mjd"])
        if gap > gap_threshold:
            groups.append([curr])
        else:
            groups[-1].append(curr)

    return groups


def identify_compositing_groups(
    products: list[dict[str, Any]],
    gap_threshold: float = NIGHT_GAP_THRESHOLD_DAYS,
) -> list[CompositingGroup]:
    """Identify all compositing groups for a single nova.

    Groups products by instrument, then clusters each instrument's
    products into observing nights.  Only returns groups where at
    least 2 products share the same instrument and night — singletons
    are excluded because they produce no composite.

    Parameters
    ----------
    products:
        All VALID spectra DataProduct dicts for one nova.  Each must
        have ``instrument`` (str) and ``observation_date_mjd`` (numeric).
    gap_threshold:
        Night clustering gap threshold (days).

    Returns
    -------
    list[CompositingGroup]:
        Groups with ≥ 2 members, suitable for compositing evaluation.
        Empty list if no multi-member groups exist.
    """
    # Group by instrument.
    by_instrument: dict[str, list[dict[str, Any]]] = {}
    for p in products:
        inst = str(p["instrument"])
        by_instrument.setdefault(inst, []).append(p)

    groups: list[CompositingGroup] = []
    for instrument, inst_products in by_instrument.items():
        if len(inst_products) < 2:
            continue
        nights = cluster_by_night(inst_products, gap_threshold)
        for night in nights:
            if len(night) >= 2:
                groups.append(CompositingGroup(instrument=instrument, products=night))

    return groups


# ---------------------------------------------------------------------------
# Common wavelength grid
# ---------------------------------------------------------------------------


def determine_common_grid(
    spectra: list[CleanedSpectrum],
) -> NDArray[np.float64]:
    """Build the common wavelength grid for resampling.

    Grid spacing is determined by the coarsest-resolution spectrum in
    the group.  The grid spans the full union wavelength range.

    Parameters
    ----------
    spectra:
        Cleaned spectra with monotonically increasing wavelength arrays.
        Must contain at least 2 spectra (compositing requires ≥ 2).

    Returns
    -------
    NDArray[np.float64]:
        Uniformly spaced wavelength grid.

    Raises
    ------
    ValueError:
        If fewer than 2 spectra are provided, or any spectrum has
        fewer than 2 data points.
    """
    if len(spectra) < 2:
        raise ValueError(f"Need ≥ 2 spectra for compositing, got {len(spectra)}")

    # Find the coarsest median step and the union wavelength range.
    max_median_step: float = 0.0
    global_wl_min: float = np.inf
    global_wl_max: float = -np.inf

    for spec in spectra:
        wl = spec["wavelengths"]
        if len(wl) < 2:
            raise ValueError(f"Spectrum {spec['data_product_id']} has < 2 points after cleaning")
        steps = np.diff(wl)
        median_step = float(np.median(steps))
        if median_step > max_median_step:
            max_median_step = median_step

        global_wl_min = min(global_wl_min, float(wl[0]))
        global_wl_max = max(global_wl_max, float(wl[-1]))

    if max_median_step <= 0:
        raise ValueError("Coarsest median step is <= 0; degenerate input")

    # Build uniformly spaced grid.
    n_points = int(np.ceil((global_wl_max - global_wl_min) / max_median_step)) + 1
    grid = np.linspace(global_wl_min, global_wl_max, n_points)
    return grid


# ---------------------------------------------------------------------------
# Composite fingerprint
# ---------------------------------------------------------------------------


def compute_composite_fingerprint(
    evaluated_ids: list[str],
    sha256_by_id: dict[str, str],
) -> str:
    """Compute a deterministic fingerprint for a compositing group.

    The fingerprint covers all evaluated data_product_ids (both
    constituents and rejected) and their content sha256 hashes.
    Including the full evaluated set ensures that changes to any
    group member — including rejected spectra — trigger a rebuild.

    The fingerprint is a SHA-256 hex digest of the concatenation of
    sorted ``(data_product_id, sha256)`` pairs.  Deterministic sorting
    ensures the same set of inputs always produces the same fingerprint
    regardless of iteration order.

    Parameters
    ----------
    evaluated_ids:
        ``data_product_id`` values of ALL spectra in the compositing
        group (constituents + rejected).
    sha256_by_id:
        Mapping from ``data_product_id`` to its content ``sha256`` hash
        string (from the DataProduct DDB item).

    Returns
    -------
    str:
        SHA-256 hex digest (64 characters).

    Raises
    ------
    KeyError:
        If any ID in *evaluated_ids* is missing from *sha256_by_id*.
    ValueError:
        If *evaluated_ids* is empty.
    """
    if not evaluated_ids:
        raise ValueError("evaluated_ids must not be empty")

    # Sort for determinism, then concatenate id:sha256 pairs.
    sorted_ids = sorted(evaluated_ids)
    parts: list[str] = []
    for eid in sorted_ids:
        sha = sha256_by_id[eid]
        parts.append(f"{eid}:{sha}")

    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Deterministic composite ID (UUID v5)
# ---------------------------------------------------------------------------


def compute_composite_id(constituent_ids: list[str]) -> str:
    """Compute a deterministic composite UUID from constituent IDs.

    Uses UUID v5 (SHA-1 based, namespace-scoped) so that the same set
    of constituents always produces the same composite ID.

    Parameters
    ----------
    constituent_ids:
        ``data_product_id`` values of the spectra being combined.
        Order does not matter — IDs are sorted internally.

    Returns
    -------
    str:
        UUID v5 string (lowercase, hyphenated).

    Raises
    ------
    ValueError:
        If *constituent_ids* is empty.
    """
    if not constituent_ids:
        raise ValueError("constituent_ids must not be empty")

    sorted_ids = sorted(constituent_ids)
    name = "|".join(sorted_ids)
    return str(uuid.uuid5(_NOVACAT_UUID_NAMESPACE, name))


# ---------------------------------------------------------------------------
# Spectrum cleaning (wraps shared.py utilities)
# ---------------------------------------------------------------------------


def clean_spectrum(
    data_product_id: str,
    wavelengths: NDArray[np.float64],
    fluxes: NDArray[np.float64],
) -> CleanedSpectrum | None:
    """Apply the full cleaning pipeline to a single raw spectrum.

    Wraps the three shared cleaning functions in the order required
    by the compositing pipeline: edge trimming → interior dead run
    removal → chip gap artifact rejection.

    Parameters
    ----------
    data_product_id:
        ID for logging context (passed through to cleaning functions).
    wavelengths:
        Monotonically increasing wavelength array (nm) from FITS.
    fluxes:
        Flux array parallel to *wavelengths*.

    Returns
    -------
    CleanedSpectrum | None:
        Cleaned spectrum, or ``None`` if cleaning eliminated all points.
    """
    from generators.shared import (
        reject_chip_gap_artifacts,
        remove_interior_dead_runs,
        trim_dead_edges,
    )

    wl = wavelengths.tolist()
    fx = fluxes.tolist()

    wl, fx = trim_dead_edges(wl, fx, data_product_id)
    if not wl:
        return None

    wl, fx = remove_interior_dead_runs(wl, fx, data_product_id)
    if not wl:
        return None

    wl, fx = reject_chip_gap_artifacts(wl, fx, data_product_id)
    if not wl:
        return None

    return CleanedSpectrum(
        data_product_id=data_product_id,
        wavelengths=np.asarray(wl, dtype=np.float64),
        fluxes=np.asarray(fx, dtype=np.float64),
    )


# ---------------------------------------------------------------------------
# Resampling
# ---------------------------------------------------------------------------


def resample_to_grid(
    spectrum: CleanedSpectrum,
    grid: NDArray[np.float64],
) -> NDArray[np.float64]:
    """Resample a cleaned spectrum onto a common wavelength grid.

    Uses linear interpolation.  Grid points outside the spectrum's
    wavelength range are set to NaN (no extrapolation).

    Parameters
    ----------
    spectrum:
        Cleaned spectrum with monotonically increasing wavelengths.
    grid:
        Target wavelength grid (uniformly spaced, from
        ``determine_common_grid``).

    Returns
    -------
    NDArray[np.float64]:
        Flux values on the common grid.  NaN where the grid extends
        beyond this spectrum's coverage.
    """
    wl = spectrum["wavelengths"]
    fx = spectrum["fluxes"]

    # np.interp extrapolates by default; we want NaN outside coverage.
    resampled = np.interp(grid, wl, fx)

    # Mask points outside the spectrum's wavelength range.
    out_of_range = (grid < wl[0]) | (grid > wl[-1])
    resampled[out_of_range] = np.nan

    return resampled


# ---------------------------------------------------------------------------
# Combination
# ---------------------------------------------------------------------------


def combine_spectra(
    resampled_fluxes: list[NDArray[np.float64]],
) -> NDArray[np.float64]:
    """Median-combine resampled flux arrays with subset-aware averaging.

    At each grid point, the median is taken over only the spectra
    that have coverage there (non-NaN values).  Grid points with no
    coverage from any spectrum are NaN in the output.  Median is
    preferred over mean for robustness against calibration artifacts
    and cosmic ray residuals.

    Parameters
    ----------
    resampled_fluxes:
        One flux array per spectrum, all on the same common grid.
        Must contain at least 2 arrays.

    Returns
    -------
    NDArray[np.float64]:
        Combined flux array on the common grid.
    """
    if len(resampled_fluxes) < 2:
        raise ValueError(f"Need ≥ 2 flux arrays for combination, got {len(resampled_fluxes)}")

    stacked = np.vstack(resampled_fluxes)
    with np.errstate(all="ignore"):
        combined: NDArray[np.float64] = np.nanmedian(stacked, axis=0)
    return combined


# ---------------------------------------------------------------------------
# CSV serialization
# ---------------------------------------------------------------------------


def composite_to_csv(
    wavelengths: NDArray[np.float64],
    fluxes: NDArray[np.float64],
) -> str:
    """Serialize a composite spectrum to CSV format.

    Produces a two-column CSV (``wavelength_nm,flux``) matching the
    web-ready CSV format used by the spectra generator.  NaN flux
    values (grid points with no spectral coverage) are excluded.

    Parameters
    ----------
    wavelengths:
        Common grid wavelength array (nm).
    fluxes:
        Combined flux array (from ``combine_spectra``).

    Returns
    -------
    str:
        CSV string with header row, ready for S3 upload.
    """
    mask = np.isfinite(fluxes)
    wl_clean = wavelengths[mask]
    fx_clean = fluxes[mask]

    lines = ["wavelength_nm,flux"]
    for w, f in zip(wl_clean, fx_clean, strict=True):
        lines.append(f"{w:.6f},{f}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# DDB queries
# ---------------------------------------------------------------------------

_COMPOSITE_MARKER = "COMPOSITE"


def find_compositable_products(
    table: Any,
    nova_id: str,
) -> list[dict[str, Any]]:
    """Query all VALID individual spectra DataProducts for a nova.

    Returns only non-composite items (SK does not contain the
    ``COMPOSITE`` segment).  Results are used by the compositing sweep
    to identify candidate groups.

    Parameters
    ----------
    table:
        Boto3 DynamoDB Table resource.
    nova_id:
        Nova UUID string (the partition key value).

    Returns
    -------
    list[dict]:
        VALID individual spectra DataProduct items from DDB.
    """
    from boto3.dynamodb.conditions import Attr, Key

    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
        ),
        "FilterExpression": Attr("validation_status").eq("VALID"),
    }

    while True:
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            sk = str(item.get("SK", ""))
            if _COMPOSITE_MARKER not in sk:
                items.append(item)
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    return items


def find_existing_composites(
    table: Any,
    nova_id: str,
) -> list[dict[str, Any]]:
    """Query all existing composite DataProduct items for a nova.

    Composites are identified by the ``COMPOSITE`` segment in their
    sort key.

    Parameters
    ----------
    table:
        Boto3 DynamoDB Table resource.
    nova_id:
        Nova UUID string.

    Returns
    -------
    list[dict]:
        Composite DataProduct items, possibly empty.
    """
    from boto3.dynamodb.conditions import Key

    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": (
            Key("PK").eq(nova_id) & Key("SK").begins_with("PRODUCT#SPECTRA#")
        ),
    }

    while True:
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            sk = str(item.get("SK", ""))
            if _COMPOSITE_MARKER in sk:
                items.append(item)
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    return items


# ---------------------------------------------------------------------------
# DDB writes
# ---------------------------------------------------------------------------


def write_composite_data_product(
    table: Any,
    nova_id: str,
    composite_id: str,
    provider: str,
    instrument: str,
    telescope: str | None,
    observation_date_mjd: float,
    constituent_data_product_ids: list[str],
    rejected_data_product_ids: list[str],
    composite_fingerprint: str,
    composite_s3_key: str | None,
    web_ready_s3_key: str | None,
) -> None:
    """Write a composite (or degenerate composite) DataProduct to DDB.

    Uses unconditional PutItem — the compositing sweep is the sole
    writer of composite items and runs sequentially within a single
    Fargate task.

    For degenerate composites (single survivor after threshold
    filtering), ``composite_s3_key`` and ``web_ready_s3_key`` are
    ``None`` — no CSV artifacts exist.

    Parameters
    ----------
    table:
        Boto3 DynamoDB Table resource.
    nova_id:
        Nova UUID string.
    composite_id:
        Deterministic UUID v5 for this composite.
    provider:
        Shared provider of the compositing group.
    instrument:
        Shared instrument of the compositing group.
    telescope:
        Shared telescope (may be None).
    observation_date_mjd:
        Mean MJD of the constituent spectra.
    constituent_data_product_ids:
        IDs of spectra that were combined (sorted).
    rejected_data_product_ids:
        IDs of spectra considered but excluded (e.g., below
        2000-point threshold).  May be empty.
    composite_fingerprint:
        SHA-256 fingerprint covering constituents and their content
        hashes.
    composite_s3_key:
        S3 key for composite_full.csv, or None for degenerate composites.
    web_ready_s3_key:
        S3 key for web_ready.csv, or None for degenerate composites.
    """
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    sk = f"PRODUCT#SPECTRA#{provider}#COMPOSITE#{composite_id}"

    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": sk,
        "entity_type": "DataProduct",
        "data_product_id": composite_id,
        "product_type": "SPECTRA",
        "provider": provider,
        "instrument": instrument,
        "observation_date_mjd": _to_decimal(observation_date_mjd),
        "constituent_data_product_ids": constituent_data_product_ids,
        "rejected_data_product_ids": rejected_data_product_ids,
        "composite_fingerprint": composite_fingerprint,
        "validation_status": "VALID",
        "eligibility": "NONE",
        "created_at": now,
        "updated_at": now,
    }

    if telescope is not None:
        item["telescope"] = telescope
    if composite_s3_key is not None:
        item["composite_s3_key"] = composite_s3_key
    if web_ready_s3_key is not None:
        item["web_ready_s3_key"] = web_ready_s3_key

    table.put_item(Item=item)

    _logger.info(
        "Wrote composite DataProduct",
        extra={
            "nova_id": nova_id,
            "composite_id": composite_id,
            "instrument": instrument,
            "n_constituents": len(constituent_data_product_ids),
            "n_rejected": len(rejected_data_product_ids),
            "degenerate": composite_s3_key is None,
        },
    )


def _to_decimal(value: float) -> Any:
    """Convert a float to Decimal for DynamoDB storage."""
    from decimal import Decimal

    return Decimal(str(value))


# ---------------------------------------------------------------------------
# S3 writes + LTTB downsampling
# ---------------------------------------------------------------------------


def persist_composite_csvs(
    s3_client: Any,
    bucket: str,
    nova_id: str,
    composite_id: str,
    grid_wavelengths: NDArray[np.float64],
    combined_fluxes: NDArray[np.float64],
) -> tuple[str, str]:
    """Write composite_full.csv and web_ready.csv to S3.

    The full-resolution CSV is persisted so that LTTB threshold changes
    don't require recompositing from FITS.  The web-ready CSV is
    LTTB-downsampled to ≤ 2000 points for the spectra generator.

    Parameters
    ----------
    s3_client:
        Boto3 S3 client.
    bucket:
        Private data bucket name.
    nova_id:
        Nova UUID string.
    composite_id:
        Composite UUID string.
    grid_wavelengths:
        Common wavelength grid (nm).
    combined_fluxes:
        Median-combined flux array on the common grid.

    Returns
    -------
    tuple[str, str]:
        ``(composite_s3_key, web_ready_s3_key)`` — the S3 keys written.
    """
    prefix = f"derived/spectra/{nova_id}/{composite_id}"
    composite_s3_key = f"{prefix}/composite_full.csv"
    web_ready_s3_key = f"{prefix}/web_ready.csv"

    # Full-resolution composite.
    full_csv = composite_to_csv(grid_wavelengths, combined_fluxes)
    s3_client.put_object(
        Bucket=bucket,
        Key=composite_s3_key,
        Body=full_csv.encode("utf-8"),
        ContentType="text/csv",
    )

    # LTTB-downsampled web-ready composite.
    web_wl, web_fx = _lttb_downsample(grid_wavelengths, combined_fluxes)
    web_csv = composite_to_csv(
        np.asarray(web_wl, dtype=np.float64),
        np.asarray(web_fx, dtype=np.float64),
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=web_ready_s3_key,
        Body=web_csv.encode("utf-8"),
        ContentType="text/csv",
    )

    _logger.info(
        "Persisted composite CSVs to S3",
        extra={
            "nova_id": nova_id,
            "composite_id": composite_id,
            "composite_s3_key": composite_s3_key,
            "web_ready_s3_key": web_ready_s3_key,
            "full_points": len(grid_wavelengths),
            "web_ready_points": len(web_wl),
        },
    )

    return composite_s3_key, web_ready_s3_key


def _lttb_downsample(
    wavelengths: NDArray[np.float64],
    fluxes: NDArray[np.float64],
) -> tuple[list[float], list[float]]:
    """Downsample a composite spectrum to ≤ 2000 points via LTTB.

    Strips NaN values before downsampling (LTTB expects a clean array),
    then delegates to ``segment_aware_lttb`` from shared.py.

    Parameters
    ----------
    wavelengths:
        Common grid wavelength array (nm).
    fluxes:
        Combined flux array (may contain NaN at no-coverage grid
        points).

    Returns
    -------
    tuple[list[float], list[float]]:
        ``(wavelengths, fluxes)`` downsampled to ≤ 2000 points.
    """
    from generators.shared import segment_aware_lttb

    # Strip NaN values — LTTB operates on clean arrays.
    mask = np.isfinite(fluxes)
    clean_wl = wavelengths[mask].tolist()
    clean_fx = fluxes[mask].tolist()

    return segment_aware_lttb(clean_wl, clean_fx)


# ---------------------------------------------------------------------------
# Sweep result type
# ---------------------------------------------------------------------------


class CompositingSweepResult(TypedDict):
    """Summary of a compositing sweep for one nova."""

    groups_found: int
    skipped: int
    built: int
    degenerate: int
    errors: int


# ---------------------------------------------------------------------------
# D1 — Sweep orchestration
# ---------------------------------------------------------------------------


def run_compositing_sweep(
    nova_id: str,
    table: Any,
    s3_client: Any,
    bucket: str,
) -> CompositingSweepResult:
    """Run the compositing sweep for a single nova.

    Implements the full rebuild decision tree: identify compositing
    groups, check fingerprints against existing composites, and build
    or skip as appropriate.  Called by ``main.py`` as Phase 1 before
    the per-nova artifact generators run.

    Parameters
    ----------
    nova_id:
        Nova UUID string.
    table:
        Boto3 DynamoDB Table resource.
    s3_client:
        Boto3 S3 client.
    bucket:
        Private data bucket name.

    Returns
    -------
    CompositingSweepResult:
        Counts of groups found, skipped, built, degenerate, and errors.
    """

    result = CompositingSweepResult(
        groups_found=0,
        skipped=0,
        built=0,
        degenerate=0,
        errors=0,
    )

    # 1. Query DDB for individual spectra and existing composites.
    individuals = find_compositable_products(table, nova_id)
    if len(individuals) < 2:
        return result

    existing_composites = find_existing_composites(table, nova_id)

    # Index existing composites by fingerprint for O(1) lookup.
    existing_by_fp: dict[str, dict[str, Any]] = {
        str(c["composite_fingerprint"]): c
        for c in existing_composites
        if "composite_fingerprint" in c
    }

    # 2. Group by instrument + night.
    groups = identify_compositing_groups(individuals)
    result["groups_found"] = len(groups)

    if not groups:
        return result

    # 3. Process each group.
    for group in groups:
        try:
            _process_compositing_group(
                nova_id=nova_id,
                group=group,
                existing_by_fp=existing_by_fp,
                table=table,
                s3_client=s3_client,
                bucket=bucket,
                result=result,
            )
        except Exception:
            _logger.warning(
                "Error processing compositing group",
                extra={
                    "nova_id": nova_id,
                    "instrument": group["instrument"],
                    "group_size": len(group["products"]),
                },
                exc_info=True,
            )
            result["errors"] += 1

    _logger.info(
        "Compositing sweep complete",
        extra={"nova_id": nova_id, **result},
    )

    return result


def _process_compositing_group(
    nova_id: str,
    group: CompositingGroup,
    existing_by_fp: dict[str, dict[str, Any]],
    table: Any,
    s3_client: Any,
    bucket: str,
    result: CompositingSweepResult,
) -> None:
    """Process a single compositing group through the rebuild decision tree.

    Mutates *result* in place to track counts.
    """
    from generators.fits_reader import read_fits_spectrum

    products = group["products"]
    instrument = group["instrument"]

    # Compute fingerprint from all group members.
    member_ids = [str(p["data_product_id"]) for p in products]
    sha256_map = {str(p["data_product_id"]): str(p["sha256"]) for p in products}
    fingerprint = compute_composite_fingerprint(member_ids, sha256_map)

    # Fingerprint check — skip if unchanged.
    if fingerprint in existing_by_fp:
        _logger.debug(
            "Compositing group unchanged, skipping",
            extra={
                "nova_id": nova_id,
                "instrument": instrument,
                "fingerprint": fingerprint[:16],
            },
        )
        result["skipped"] += 1
        return

    # --- Fingerprint miss: read FITS and evaluate threshold. ---

    # Read FITS for each group member, check native point counts.
    constituents: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for product in products:
        dp_id = str(product["data_product_id"])
        raw_key = product.get("raw_s3_key")
        raw_bucket = product.get("raw_s3_bucket", bucket)

        if not raw_key:
            _logger.warning(
                "No raw_s3_key on DataProduct, skipping for compositing",
                extra={
                    "nova_id": nova_id,
                    "data_product_id": dp_id,
                    "instrument": product.get("instrument", "unknown"),
                    "telescope": product.get("telescope", "unknown"),
                    "provider": product.get("provider", "unknown"),
                },
            )
            rejected.append(product)
            continue

        _logger.info("Reading FITS for compositing", extra={"data_product_id": dp_id})
        fits_result = read_fits_spectrum(s3_client, raw_bucket, raw_key, dp_id)
        if fits_result is None:
            _logger.warning(
                "Could not read FITS for compositing",
                extra={
                    "nova_id": nova_id,
                    "data_product_id": dp_id,
                    "instrument": product.get("instrument", "unknown"),
                    "provider": product.get("provider", "unknown"),
                },
            )
            rejected.append(product)
            continue

        wavelengths, fluxes = fits_result
        if len(wavelengths) < MIN_POINTS_FOR_COMPOSITE:
            _logger.debug(
                "Spectrum below compositing threshold",
                extra={
                    "data_product_id": dp_id,
                    "n_points": len(wavelengths),
                    "threshold": MIN_POINTS_FOR_COMPOSITE,
                },
            )
            rejected.append(product)
            continue

        # Attach raw arrays for downstream processing.
        product["_raw_wavelengths"] = wavelengths
        product["_raw_fluxes"] = fluxes
        constituents.append(product)

    # --- Relative SNR gate (epic/29) ---
    # Exclude spectra whose SNR is far below the group's median.
    # These are moved to the rejected list, not silently dropped.
    if len(constituents) >= 2:
        snr_values = [_effective_snr_for_product(p) for p in constituents]
        group_median_snr = float(np.median(snr_values))

        if group_median_snr > 0:
            snr_floor = group_median_snr * _COMPOSITING_SNR_RELATIVE_THRESHOLD
            snr_passed: list[dict[str, Any]] = []
            for p, snr in zip(constituents, snr_values, strict=False):
                if 0 < snr < snr_floor:
                    _logger.info(
                        "Spectrum excluded from composite by relative SNR gate",
                        extra={
                            "data_product_id": str(p["data_product_id"]),
                            "effective_snr": round(snr, 2),
                            "group_median_snr": round(group_median_snr, 2),
                            "threshold": round(snr_floor, 2),
                        },
                    )
                    rejected.append(p)
                else:
                    snr_passed.append(p)
            constituents = snr_passed

    constituent_ids = sorted(str(p["data_product_id"]) for p in constituents)
    rejected_ids = sorted(str(p["data_product_id"]) for p in rejected)

    # Shared metadata from the group.
    provider = str(products[0]["provider"])
    telescope = products[0].get("telescope")
    if telescope is not None:
        telescope = str(telescope)
    mean_mjd = float(np.mean([float(p["observation_date_mjd"]) for p in products]))

    # --- Degenerate case: 0 or 1 constituents. ---
    if len(constituents) < 2:
        if len(constituents) == 1:
            # Degenerate composite — point at the survivor's web-ready CSV.
            survivor = constituents[0]
            survivor_id = str(survivor["data_product_id"])
            composite_id = compute_composite_id([survivor_id])

            # Build the web_ready_s3_key from the survivor's existing CSV.
            survivor_web_ready = f"derived/spectra/{nova_id}/{survivor_id}/web_ready.csv"

            write_composite_data_product(
                table=table,
                nova_id=nova_id,
                composite_id=composite_id,
                provider=provider,
                instrument=instrument,
                telescope=telescope,
                observation_date_mjd=mean_mjd,
                constituent_data_product_ids=constituent_ids,
                rejected_data_product_ids=rejected_ids,
                composite_fingerprint=fingerprint,
                composite_s3_key=None,
                web_ready_s3_key=survivor_web_ready,
            )
            result["degenerate"] += 1
        # else: 0 constituents — nothing to do, all rejected.
        return

    # --- Real composite: ≥ 2 constituents. ---
    composite_id = compute_composite_id(constituent_ids)

    # Clean each constituent.
    cleaned_spectra: list[CleanedSpectrum] = []
    for product in constituents:
        dp_id = str(product["data_product_id"])
        cleaned = clean_spectrum(
            dp_id,
            product["_raw_wavelengths"],
            product["_raw_fluxes"],
        )
        if cleaned is None:
            _logger.warning(
                "Cleaning eliminated all points",
                extra={"data_product_id": dp_id},
            )
            continue
        cleaned_spectra.append(cleaned)

    if len(cleaned_spectra) < 2:
        _logger.warning(
            "Fewer than 2 spectra survived cleaning, skipping composite",
            extra={
                "nova_id": nova_id,
                "instrument": instrument,
                "cleaned_count": len(cleaned_spectra),
            },
        )
        return

    # Resample onto common grid.
    grid = determine_common_grid(cleaned_spectra)
    resampled = [resample_to_grid(spec, grid) for spec in cleaned_spectra]

    # Combine via median.
    combined = combine_spectra(resampled)

    # Persist CSVs to S3.
    composite_s3_key, web_ready_s3_key = persist_composite_csvs(
        s3_client,
        bucket,
        nova_id,
        composite_id,
        grid,
        combined,
    )

    # Write composite DataProduct to DDB.
    write_composite_data_product(
        table=table,
        nova_id=nova_id,
        composite_id=composite_id,
        provider=provider,
        instrument=instrument,
        telescope=telescope,
        observation_date_mjd=mean_mjd,
        constituent_data_product_ids=constituent_ids,
        rejected_data_product_ids=rejected_ids,
        composite_fingerprint=fingerprint,
        composite_s3_key=composite_s3_key,
        web_ready_s3_key=web_ready_s3_key,
    )
    result["built"] += 1
