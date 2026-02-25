# contracts/models/events.py
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
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

    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.initialize_nova)

    candidate_name: str = Field(..., min_length=1, max_length=256)

    source: str | None = Field(
        default=None, description="Where this request came from (e.g., UI, ingest feed)."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)


class IngestNewNovaEvent(EventBase):
    """
    Coordinator workflow that bootstraps ingestion for an already-established nova_id.
    """

    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.ingest_new_nova)

    nova_id: UUID

    # minimal knobs to control ingestion behavior without coupling
    force_refresh: bool = False
    attributes: dict[str, Any] = Field(default_factory=dict)


class RefreshReferencesEvent(EventBase):
    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.refresh_references)

    nova_id: UUID

    # ADS query hints; keep generic
    query_terms: list[str] = Field(default_factory=list)
    since_year: int | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)


class DiscoverSpectraProductsEvent(EventBase):
    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.discover_spectra_products)

    nova_id: UUID

    # optionally constrain discovery sources
    sources: list[str] = Field(
        default_factory=list, description="Preferred discovery sources (optional)."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)


class AcquireAndValidateSpectraEvent(EventBase):
    """
    One data_product_id per execution (Mode 1).

    The workflow reads locator/provenance/acquisition metadata from the persisted DataProduct record.
    Boundary event identifies the target data product only.
    """

    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.acquire_and_validate_spectra)

    nova_id: UUID

    # Provider is included because the persisted product item keying is provider-scoped.
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

    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.ingest_photometry)

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
    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.name_check_and_reconcile)

    nova_id: UUID
    proposed_public_name: str | None = None
    proposed_aliases: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
