# contracts/models/events.py
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from contracts.models.entities import JobType


def utcnow() -> datetime:
    return datetime.now(UTC)


class EventBase(BaseModel):
    """
    Base class for all Step Functions boundary events.

    Notes:
    - Boundary events MUST NOT include workflow idempotency keys or step dedupe keys.
    - correlation_id SHOULD be provided by callers; if absent, the workflow (via this model)
      generates one and propagates it downstream.
    - initiated_at represents when the caller originated the request. It is distinct from
      workflow execution start time and is useful for latency analysis and event age checks.
    """

    model_config = ConfigDict(extra="forbid")

    event_version: str = Field(
        default="1.0.0", description="Semantic version for this event contract."
    )
    correlation_id: UUID = Field(default_factory=uuid4)
    initiated_at: datetime = Field(default_factory=utcnow)

    @field_validator("initiated_at")
    @classmethod
    def ensure_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise ValueError("initiated_at must be timezone-aware (UTC).")
        return v


# ----------------------------
# Event payloads
# ----------------------------


class InitializeNovaEvent(EventBase):
    """
    Name-only front door.

    Input is a candidate name; the workflow resolves identity and emits a nova_id downstream.
    """

    event_version: Literal["1.0.0"] = "1.0.0"
    job_type: Literal[JobType.initialize_nova] = JobType.initialize_nova

    candidate_name: str = Field(..., min_length=1, max_length=256)

    source: str | None = Field(
        default=None, description="Where this request came from (e.g., UI, ingest feed)."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)


class IngestNewNovaEvent(EventBase):
    """
    Coordinator workflow that bootstraps ingestion for an already-established nova_id.

    Note: force_refresh and similar behavioral knobs are passed via attributes rather than
    typed fields, to keep the typed contract minimal and stable. Callers may include
    {"force_refresh": true} in attributes; workflow implementations read from there.
    """

    event_version: Literal["1.0.0"] = "1.0.0"
    job_type: Literal[JobType.ingest_new_nova] = JobType.ingest_new_nova

    nova_id: UUID

    attributes: dict[str, Any] = Field(default_factory=dict)


class RefreshReferencesEvent(EventBase):
    """
    Note: ADS query hints (query_terms, since_year, etc.) are passed via attributes rather
    than typed fields. The workflow spec requires only nova_id; query hints are implementation
    details that may evolve without breaking the contract.
    """

    event_version: Literal["1.0.0"] = "1.0.0"
    job_type: Literal[JobType.refresh_references] = JobType.refresh_references

    nova_id: UUID

    attributes: dict[str, Any] = Field(default_factory=dict)


class DiscoverSpectraProductsEvent(EventBase):
    """
    Note: Discovery source constraints (e.g., preferred providers) are passed via attributes
    rather than typed fields, consistent with the workflow spec which requires only nova_id.
    Callers may include {"sources": ["ESO", "MAST"]} in attributes.
    """

    event_version: Literal["1.0.0"] = "1.0.0"
    job_type: Literal[JobType.discover_spectra_products] = JobType.discover_spectra_products

    nova_id: UUID

    attributes: dict[str, Any] = Field(default_factory=dict)


class AcquireAndValidateSpectraEvent(EventBase):
    """
    One data_product_id per execution (Mode 1).

    The workflow reads locator/provenance/acquisition metadata from the persisted DataProduct record.
    Boundary event identifies the target data product only.

    Note: provider is included in the boundary event because the DynamoDB item key is
    PRODUCT#SPECTRA#<provider>#<data_product_id> — the Lambda needs provider to construct
    the lookup key without an extra read.
    """

    event_version: Literal["1.0.0"] = "1.0.0"
    job_type: Literal[JobType.acquire_and_validate_spectra] = JobType.acquire_and_validate_spectra

    nova_id: UUID
    provider: str = Field(..., min_length=1, max_length=128)
    data_product_id: UUID

    attributes: dict[str, Any] = Field(default_factory=dict)


class IngestPhotometryEvent(EventBase):
    """
    Photometry ingestion front door.

    Accepts either:
    - candidate_name (workflow will resolve to nova_id), OR
    - nova_id (already resolved)

    No dataset abstraction exists.
    """

    event_version: Literal["1.0.0"] = "1.0.0"
    job_type: Literal[JobType.ingest_photometry] = JobType.ingest_photometry

    candidate_name: str | None = Field(default=None, min_length=1, max_length=256)
    nova_id: UUID | None = None

    # Forward-compatible: fixed in MVP; snapshots occur only when schema version changes.
    photometry_schema_version: str | None = Field(default=None, min_length=1, max_length=64)

    source: str | None = Field(
        default=None, description="Where this request came from (e.g., UI, ingest feed)."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_identifiers(self) -> IngestPhotometryEvent:
        if self.nova_id is None and self.candidate_name is None:
            raise ValueError("Either nova_id or candidate_name must be provided.")
        return self


class NameCheckAndReconcileEvent(EventBase):
    """
    Note: proposed_public_name and proposed_aliases are passed via attributes rather than
    typed fields. The workflow spec requires only nova_id; proposed naming hints are
    operator-supplied inputs that may evolve without requiring a contract version bump.
    Callers may include {"proposed_public_name": "V1234 Sco", "proposed_aliases": [...]}
    in attributes.
    """

    event_version: Literal["1.0.0"] = "1.0.0"
    job_type: Literal[JobType.name_check_and_reconcile] = JobType.name_check_and_reconcile

    nova_id: UUID
    attributes: dict[str, Any] = Field(default_factory=dict)
