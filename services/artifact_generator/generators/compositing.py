"""Spectra compositing — pure computation utilities.

Core building blocks for the spectra compositing pipeline: grouping
same-instrument spectra into observing nights, determining a common
wavelength grid for resampling, cleaning and resampling individual
spectra, combining them into composites, and computing deterministic
composite identifiers and fingerprints for rebuild avoidance.

All functions are pure (no AWS I/O, no side effects) and operate on
plain dicts / numpy arrays so they can be tested without mocks.
The one exception is ``clean_spectrum``, which imports from
``generators.shared`` at call time (deferred import to avoid circular
dependencies in the Fargate module layout).

See ADR-033 for design rationale.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any, TypedDict

import numpy as np
from numpy.typing import NDArray

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

#: NovaCat UUID v5 namespace for deterministic composite IDs.
#: Generated once via uuid.uuid4() and frozen here.
_NOVACAT_UUID_NAMESPACE: uuid.UUID = uuid.UUID("7f1b3c5e-8a2d-4e6f-b9c1-d3e5f7a8b0c2")


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
    constituent_ids: list[str],
    sha256_by_id: dict[str, str],
) -> str:
    """Compute a deterministic fingerprint for a compositing group.

    The fingerprint covers constituent data_product_ids and their
    content sha256 hashes.

    The fingerprint is a SHA-256 hex digest of the concatenation of
    sorted ``(data_product_id, sha256)`` pairs.  Deterministic sorting
    ensures the same set of inputs always produces the same fingerprint
    regardless of iteration order.

    Parameters
    ----------
    constituent_ids:
        ``data_product_id`` values of the spectra that will be combined.
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
        If any ID in *constituent_ids* is missing from *sha256_by_id*.
    ValueError:
        If *constituent_ids* is empty.
    """
    if not constituent_ids:
        raise ValueError("constituent_ids must not be empty")

    # Sort for determinism, then concatenate id:sha256 pairs.
    sorted_ids = sorted(constituent_ids)
    parts: list[str] = []
    for cid in sorted_ids:
        sha = sha256_by_id[cid]
        parts.append(f"{cid}:{sha}")

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
    """Average resampled flux arrays with subset-aware averaging.

    At each grid point, the average is taken over only the spectra
    that have coverage there (non-NaN values).  Grid points with no
    coverage from any spectrum are NaN in the output.

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
