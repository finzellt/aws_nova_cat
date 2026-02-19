# contracts/models/events.py
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from contracts.models.entities import DatasetKind, JobType


def utcnow() -> datetime:
    return datetime.now(UTC)


class EventBase(BaseModel):
    """
    Base class for all Step Functions boundary events.
    """

    model_config = ConfigDict(extra="forbid")

    event_version: str = Field(
        default="1.0.0", description="Semantic version for this event contract."
    )
    correlation_id: UUID = Field(default_factory=uuid4)
    idempotency_key: str = Field(..., min_length=8, max_length=256)
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
    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.initialize_nova)
    # create-or-get semantics: allow providing a proposed nova_uuid, or omit to have system create one.
    proposed_nova_uuid: UUID | None = None
    public_name: str = Field(..., min_length=1, max_length=256)
    aliases: list[str] = Field(default_factory=list)
    source: str | None = Field(
        default=None, description="Where this request came from (e.g., UI, ingest feed)."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)


class IngestNewNovaEvent(EventBase):
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


class DownloadAndValidateSpectraEvent(EventBase):
    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.download_and_validate_spectra)
    nova_id: UUID
    dataset_id: UUID
    # idempotent-safe “work list” should be explicit
    file_urls: list[str] = Field(
        default_factory=list, description="Upstream URLs to download/validate."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)


class IngestPhotometryDatasetEvent(EventBase):
    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.ingest_photometry_dataset)
    nova_id: UUID
    dataset_kind: DatasetKind = Field(default=DatasetKind.photometry)
    dataset_id: UUID | None = None  # allow create-or-update behavior
    file_urls: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)


class NameCheckAndReconcileEvent(EventBase):
    event_version: str = Field(default="1.0.0")
    job_type: JobType = Field(default=JobType.name_check_and_reconcile)
    nova_id: UUID
    proposed_public_name: str | None = None
    proposed_aliases: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
