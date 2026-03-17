# services/photometry_ingestor/adapters/base.py
"""
Adapter contracts for the photometry ingestion pipeline.

This module defines the PhotometryAdapter Protocol and its associated result types.
It is deliberately separate from entities.py: adapters are ingestion-time plumbing,
not persistent domain objects.

See: ADR-015 — Photometry Ingestion Mechanism and Column Mapping Strategy.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from contracts.models.entities import PhotometryRow

# ---------------------------------------------------------------------------
# Failure / result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AdaptationFailure:
    """
    Records a single row-level failure encountered during adaptation.

    Failures are collected across all rows before any quarantine decision is
    made (ADR-015, Decision 2: threshold-based quarantine policy).  The adapter
    must NOT fail fast on the first bad row.

    Attributes
    ----------
    row_index:
        0-based index of the failing row in the source iterable.
    raw_row:
        The original, un-adapted row dict as it came from the parser.
        Stored for diagnostic context in quarantine payloads.
    error:
        Human-readable description of why this row failed (e.g. coercion
        error message, Pydantic validation summary).
    """

    row_index: int
    raw_row: dict[str, Any]
    error: str


@dataclass(frozen=True)
class AdaptationResult:
    """
    Aggregate result of running a PhotometryAdapter over a complete source file.

    Invariant: ``len(valid_rows) + len(failures) == total_row_count``.

    The caller (ValidatePhotometry handler) is responsible for the threshold
    decision: if ``failure_rate`` exceeds the configured threshold, the file is
    quarantined with reason code ``COERCION_FAILURE_THRESHOLD_EXCEEDED``; below
    the threshold, ``failures`` are dropped and logged, and processing continues
    with ``valid_rows``.
    """

    valid_rows: list[PhotometryRow]
    failures: list[AdaptationFailure]
    total_row_count: int

    @property
    def failure_rate(self) -> float:
        """Fraction of rows that failed adaptation.  0.0 when source is empty."""
        if self.total_row_count == 0:
            return 0.0
        return len(self.failures) / self.total_row_count


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class PhotometryAdapter(Protocol):
    """
    Protocol for source-specific photometry adapters.

    An adapter is responsible for:

    1. Normalising column names from a heterogeneous source file onto canonical
       ``PhotometryRow`` field names (Tier 1 + Tier 2 of the ADR-015 mapping
       strategy: direct canonical headers, then synonym registry lookup).
    2. Coercing raw string values to their expected Python types.
    3. Constructing and validating ``PhotometryRow`` instances via Pydantic.
    4. Collecting all per-row failures without failing fast.

    Implementations MUST be:

    - **Stateless and deterministic**: the same input must always produce the
      same output.  No AI calls, no external I/O, no randomness at ``adapt()``
      time (ADR-015, Decision 2, Tier 4 constraint).
    - **Non-mutating**: the ``raw_rows`` iterable must not be consumed more than
      once, but adapters must not modify raw row dicts in place.

    Identity fields (``nova_id``, ``primary_name``, ``ra_deg``, ``dec_deg``) are
    injected by the workflow from the resolved ``Nova`` entity.  They are NOT
    expected to be present in source rows; any source column mapping to these
    fields is silently ignored after injection.

    See: ADR-015, Decision 2 — Column Mapping: Three-Tier Architecture.
    """

    def adapt(
        self,
        raw_rows: Iterable[dict[str, Any]],
        nova_id: UUID,
        primary_name: str,
        ra_deg: float,
        dec_deg: float,
    ) -> AdaptationResult:
        """
        Adapt an iterable of raw source rows into validated ``PhotometryRow`` instances.

        Parameters
        ----------
        raw_rows:
            Raw parsed rows from the source file.  Keys are source column names
            before synonym normalisation; values may be raw strings or native
            Python types depending on the upstream parser.
        nova_id:
            Resolved nova UUID.  Injected by the workflow; overrides any
            ``nova_id`` column that may be present in the source.
        primary_name:
            Canonical nova display name.  Injected by the workflow.
        ra_deg:
            Right Ascension of the nova (ICRS, decimal degrees, J2000).
            Injected from the resolved ``Nova`` entity.
        dec_deg:
            Declination of the nova (ICRS, decimal degrees, J2000).
            Injected from the resolved ``Nova`` entity.

        Returns
        -------
        AdaptationResult
            Contains all successfully validated rows and all per-row failures.
            The caller decides whether the failure rate warrants quarantine.
        """
        ...
