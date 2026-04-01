"""Regeneration pipeline domain models (DESIGN-003 §3–§4).

This module defines the data shapes shared across the three Epic 2
components — coordinator Lambda, Fargate task scaffold, and Finalize
Lambda — plus the pure-function dependency matrix that maps dirty types
to artifacts.

No AWS dependencies.  Everything here is importable from any context.

Models
------
RegenBatchPlan
    The §4.3 DDB item that records the coordinator's decisions for
    auditability and recovery.  ``PK=REGEN_PLAN``,
    ``SK=<created_at>#<plan_id>``.

NovaManifest
    Per-nova record of which dirty types triggered regeneration and
    which artifacts need to be generated.

NovaResult
    Per-nova output from the Fargate task, consumed by the Finalize
    Lambda to commit counts and delete WorkItems.

Enums
-----
ArtifactType
    The seven artifact types from §3.4.

PlanStatus
    Lifecycle statuses for RegenBatchPlan (§4.3).

Functions
---------
artifacts_for_dirty_types(dirty_types)
    Apply the §3.4 dependency matrix: given a set of dirty types,
    return the set of artifacts that need regeneration.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from contracts.models.entities import PersistentBase

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ArtifactType(str, Enum):
    """Artifact types produced by the regeneration pipeline (§3.4)."""

    spectra_json = "spectra.json"
    photometry_json = "photometry.json"
    sparkline_svg = "sparkline.svg"
    references_json = "references.json"
    nova_json = "nova.json"
    bundle_zip = "bundle.zip"
    catalog_json = "catalog.json"


class PlanStatus(str, Enum):
    """RegenBatchPlan lifecycle statuses (§4.3)."""

    pending = "PENDING"
    in_progress = "IN_PROGRESS"
    completed = "COMPLETED"
    failed = "FAILED"
    abandoned = "ABANDONED"


# ---------------------------------------------------------------------------
# §3.4 Dependency matrix
# ---------------------------------------------------------------------------

# Frozen mapping: dirty_type string → frozenset of ArtifactTypes.
# The three invariants (§3.4):
#   - nova.json regenerates on any change
#   - bundle.zip regenerates on any change
#   - catalog.json regenerates on any change to any nova
_ALWAYS: frozenset[ArtifactType] = frozenset(
    {ArtifactType.nova_json, ArtifactType.bundle_zip, ArtifactType.catalog_json}
)

DEPENDENCY_MATRIX: dict[str, frozenset[ArtifactType]] = {
    "spectra": frozenset({ArtifactType.spectra_json}) | _ALWAYS,
    "photometry": frozenset({ArtifactType.photometry_json, ArtifactType.sparkline_svg}) | _ALWAYS,
    "references": frozenset({ArtifactType.references_json}) | _ALWAYS,
}

# §4.4: Per-nova generation order (dependency order).  catalog.json is
# global and runs after all novae — it is not in this sequence.
GENERATION_ORDER: tuple[ArtifactType, ...] = (
    ArtifactType.references_json,
    ArtifactType.spectra_json,
    ArtifactType.photometry_json,
    ArtifactType.sparkline_svg,
    ArtifactType.nova_json,
    ArtifactType.bundle_zip,
)


def artifacts_for_dirty_types(dirty_types: set[str]) -> frozenset[ArtifactType]:
    """Apply the §3.4 dependency matrix.

    Given a set of dirty type strings (e.g. ``{"spectra", "photometry"}``),
    return the union of all artifact types that need regeneration.

    Raises
    ------
    ValueError
        If *dirty_types* is empty or contains an unknown dirty type.
    """
    if not dirty_types:
        raise ValueError("dirty_types must not be empty")

    result: set[ArtifactType] = set()
    for dt in dirty_types:
        artifacts = DEPENDENCY_MATRIX.get(dt)
        if artifacts is None:
            raise ValueError(f"Unknown dirty_type: {dt!r}")
        result |= artifacts
    return frozenset(result)


# ---------------------------------------------------------------------------
# Per-nova models
# ---------------------------------------------------------------------------


class NovaManifest(BaseModel):
    """Per-nova regeneration manifest (§4.2 step 3).

    Records which dirty types triggered regeneration and the resulting
    set of artifacts to generate.
    """

    model_config = ConfigDict(extra="forbid")

    dirty_types: list[str] = Field(
        ...,
        min_length=1,
        description="Distinct dirty_type values present for this nova.",
    )
    artifacts: list[ArtifactType] = Field(
        ...,
        min_length=1,
        description="Artifacts to regenerate, derived from the dependency matrix.",
    )


class NovaResult(BaseModel):
    """Per-nova result from the Fargate task (§4.4 step 5).

    The Finalize Lambda reads these to decide which WorkItems to delete
    and which observation counts to write.
    """

    model_config = ConfigDict(extra="forbid")

    nova_id: str = Field(..., description="Nova UUID string.")
    success: bool = Field(..., description="Whether all artifacts for this nova succeeded.")
    error: str | None = Field(
        default=None,
        description="Error message if success is False.",
    )

    # Observation counts computed during generation (§5.4).
    # None when success is False — no counts are written on failure.
    spectra_count: int | None = Field(default=None, ge=0)
    photometry_count: int | None = Field(default=None, ge=0)
    references_count: int | None = Field(default=None, ge=0)
    has_sparkline: bool | None = Field(default=None)


# ---------------------------------------------------------------------------
# RegenBatchPlan (§4.3)
# ---------------------------------------------------------------------------


class RegenBatchPlan(PersistentBase):
    """Batch plan DDB item (§4.3).

    Key structure::

        PK = "REGEN_PLAN"
        SK = "<created_at>#<plan_id>"

    The plan is the coordinator's snapshot of what it decided.  The
    ``workitem_sks`` list ensures that only the WorkItems present at
    plan creation are deleted on success — not any that arrived during
    execution.

    TTL is 7 days from creation (short-lived operational record).
    """

    plan_id: UUID = Field(
        default_factory=uuid4,
        description="Unique identifier for this batch plan.",
    )
    status: PlanStatus = Field(
        default=PlanStatus.pending,
        description="Plan lifecycle status.",
    )

    nova_manifests: dict[str, NovaManifest] = Field(
        ...,
        description=(
            "Per-nova regeneration manifests, keyed by nova_id. "
            "Example: {'<uuid>': {'dirty_types': ['spectra'], "
            "'artifacts': ['spectra.json', 'nova.json', ...]}}."
        ),
    )
    nova_count: int = Field(
        ...,
        ge=0,
        description="Count of novae in this plan.",
    )
    workitem_sks: list[str] = Field(
        ...,
        description=(
            "Sort keys of all WorkItems consumed by this plan. "
            "Snapshot at plan creation for correct cleanup (§4.3)."
        ),
    )

    completed_at: datetime | None = Field(
        default=None,
        description="Set when plan reaches a terminal status.",
    )
    execution_arn: str | None = Field(
        default=None,
        max_length=2048,
        description="Step Functions execution ARN, set after workflow launch.",
    )
    ttl: int = Field(
        ...,
        description="DynamoDB TTL attribute (Unix epoch seconds). 7 days from creation.",
    )

    # Results populated by the Fargate task / Finalize Lambda.
    nova_results: list[NovaResult] | None = Field(
        default=None,
        description="Per-nova results from the Fargate task. Populated after execution.",
    )
