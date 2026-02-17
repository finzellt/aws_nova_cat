# contracts/models/entities.py
from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator


def utcnow() -> datetime:
    return datetime.now(UTC)


# ----------------------------
# Shared / foundational models
# ----------------------------


class VersionedModel(BaseModel):
    """Base class for all contracts with explicit versioning."""

    model_config = ConfigDict(extra="forbid", frozen=False)

    schema_version: str = Field(
        default="1.0.0",
        description="Semantic version for this persistent entity contract.",
        examples=["1.0.0"],
    )


class TimestampedModel(BaseModel):
    """Base class for created/updated timestamps."""

    model_config = ConfigDict(extra="forbid")

    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    @field_validator("created_at", "updated_at")
    @classmethod
    def ensure_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise ValueError("Timestamp must be timezone-aware (UTC).")
        return v


class ProvenanceMethod(str, Enum):
    manual = "manual"
    scraped = "scraped"
    computed = "computed"
    imported = "imported"


class Provenance(VersionedModel):
    """
    Scientific provenance: enough to trace 'what said this' and 'when',
    without overfitting to any single upstream system.
    """

    source: str = Field(
        ...,
        description="Canonical identifier for upstream source (e.g., 'ASAS-SN', 'ADS', 'AAVSO', 'UserUpload').",
        min_length=1,
    )
    source_record_id: str | None = Field(
        default=None,
        description="Upstream record identifier, if provided.",
    )
    retrieved_at: datetime = Field(default_factory=utcnow)
    method: ProvenanceMethod = Field(default=ProvenanceMethod.imported)
    asserted_by: str | None = Field(
        default=None,
        description="Human or system actor that asserted this fact (e.g., ORCID, service name).",
    )
    citation: str | None = Field(
        default=None,
        description="Optional citation string or bibcode/doi reference.",
    )
    notes: str | None = Field(default=None, max_length=2000)


class EntityBase(VersionedModel, TimestampedModel):
    """Base for persistent entities with stable IDs."""

    id: UUID = Field(default_factory=uuid4)


# ----------------------------
# Persistent Entities
# ----------------------------


class AliasType(str, Enum):
    official = "official"
    common = "common"
    survey = "survey"
    historical = "historical"
    other = "other"


class Alias(EntityBase):
    """
    A name that refers to a nova. Names can evolve; UUID is stable.
    """

    nova_id: UUID
    value: str = Field(..., min_length=1, max_length=256)
    type: AliasType = Field(default=AliasType.other)

    # Optional metadata / provenance for the alias itself:
    provenance: Provenance | None = None

    @field_validator("value")
    @classmethod
    def normalize_alias(cls, v: str) -> str:
        return " ".join(v.split())


class NovaStatus(str, Enum):
    candidate = "candidate"
    confirmed = "confirmed"
    retracted = "retracted"


class SkyCoordFrame(str, Enum):
    icrs = "icrs"  # keep minimal; extend later if needed


class Position(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ra_deg: float = Field(..., ge=0.0, lt=360.0, description="Right ascension in degrees.")
    dec_deg: float = Field(..., ge=-90.0, le=90.0, description="Declination in degrees.")
    frame: SkyCoordFrame = Field(default=SkyCoordFrame.icrs)
    epoch: str | None = Field(default="J2000", description="Epoch label, e.g., 'J2000'.")

    provenance: Provenance | None = None


class Nova(EntityBase):
    """
    Core nova record. Keep minimal: identity + a small set of stable scientific metadata.
    Put the rest behind extensions as needed.
    """

    nova_uuid: UUID = Field(
        default_factory=uuid4,
        description="Stable identifier used across the system. Duplicates id for clarity at boundaries.",
    )

    # Public-facing name may change; aliases capture history and alternates.
    public_name: str = Field(..., min_length=1, max_length=256)
    status: NovaStatus = Field(default=NovaStatus.candidate)

    # Minimal astrophysical metadata hooks (extensible later)
    position: Position | None = None
    discovery_date: datetime | None = None

    # Provenance for the top-level nova record (e.g. who/what created it)
    provenance: Provenance | None = None

    # Forward references are okay, but keep entity separation clean:
    alias_ids: list[UUID] = Field(
        default_factory=list, description="IDs of Alias records linked to this Nova."
    )

    @field_validator("public_name")
    @classmethod
    def normalize_name(cls, v: str) -> str:
        return " ".join(v.split())

    @model_validator(mode="after")
    def ensure_uuid_consistency(self) -> Nova:
        # Allow either separate or same; but keep stable.
        # If you prefer "id is the canonical", enforce equality.
        return self


class DatasetKind(str, Enum):
    spectra = "spectra"
    photometry = "photometry"


class DatasetStatus(str, Enum):
    discovered = "discovered"
    validated = "validated"
    published = "published"
    rejected = "rejected"


class FileRole(str, Enum):
    primary = "primary"
    preview = "preview"
    auxiliary = "auxiliary"
    metadata = "metadata"


class ContentDigest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    algorithm: Literal["sha256"] = "sha256"
    value: str = Field(..., min_length=64, max_length=128, description="Hex digest.")

    @field_validator("value")
    @classmethod
    def lower_hex(cls, v: str) -> str:
        return v.lower()


class FileObject(EntityBase):
    """
    Logical file contract (not storage layout): identity + integrity + descriptive metadata.
    """

    filename: str = Field(..., min_length=1, max_length=512)
    media_type: str | None = Field(
        default=None, description="IANA media type if known, e.g. 'application/fits'."
    )
    size_bytes: int | None = Field(default=None, ge=0)
    role: FileRole = Field(default=FileRole.primary)
    url: HttpUrl | None = Field(default=None, description="Upstream URL if externally sourced.")
    digest: ContentDigest | None = Field(default=None)

    provenance: Provenance | None = None


class Dataset(EntityBase):
    """
    Dataset ties a nova to one scientific product line (spectra or photometry).
    Files are separate entities; dataset references file IDs.
    """

    nova_id: UUID
    kind: DatasetKind
    status: DatasetStatus = Field(default=DatasetStatus.discovered)

    # One or more files comprise a dataset (spectrum FITS + header, photometry CSV + meta, etc.)
    file_ids: list[UUID] = Field(default_factory=list)

    # Minimal common metadata hooks
    title: str | None = Field(default=None, max_length=512)
    observed_start: datetime | None = None
    observed_end: datetime | None = None

    # Provenance at dataset level (discovery + validation chain)
    provenance: Provenance | None = None

    # Extensibility point for instrument/survey-specific details without schema explosion
    attributes: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form attributes for incremental enrichment (kept small; avoid embedding huge payloads).",
    )


class PaperIdType(str, Enum):
    bibcode = "bibcode"
    doi = "doi"
    arxiv = "arxiv"


class Paper(EntityBase):
    """
    Paper metadata (primarily from ADS). Keep fields stable and portable.
    """

    id_type: PaperIdType
    identifier: str = Field(..., min_length=1, max_length=128)  # bibcode/doi/arxiv id
    title: str | None = Field(default=None, max_length=2000)
    year: int | None = Field(default=None, ge=1800, le=2500)
    authors: list[str] = Field(default_factory=list)
    url: HttpUrl | None = None

    provenance: Provenance | None = None

    @field_validator("authors")
    @classmethod
    def strip_authors(cls, v: list[str]) -> list[str]:
        return [" ".join(a.split()) for a in v if a.strip()]


class AttemptStatus(str, Enum):
    started = "started"
    succeeded = "succeeded"
    failed = "failed"
    timed_out = "timed_out"
    cancelled = "cancelled"


class Attempt(EntityBase):
    """
    One execution attempt for a job/workflow task. Stored for traceability.
    """

    job_run_id: UUID
    attempt_number: int = Field(..., ge=1)
    status: AttemptStatus = Field(default=AttemptStatus.started)

    started_at: datetime = Field(default_factory=utcnow)
    finished_at: datetime | None = None

    # Error reporting: keep compact and safe to persist
    error_code: str | None = Field(default=None, max_length=128)
    error_message: str | None = Field(default=None, max_length=4000)

    # Trace linkouts
    request_id: str | None = Field(default=None, description="AWS Lambda request id, if relevant.")
    execution_arn: str | None = Field(
        default=None, description="Step Functions execution ARN, if relevant."
    )

    @model_validator(mode="after")
    def validate_finished_at(self) -> Attempt:
        if self.finished_at is not None and self.finished_at < self.started_at:
            raise ValueError("finished_at cannot be earlier than started_at.")
        return self


class JobType(str, Enum):
    initialize_nova = "InitializeNova"
    ingest_new_nova = "IngestNewNova"
    refresh_papers = "RefreshPapers"
    discover_spectra_products = "DiscoverSpectraProducts"
    download_and_validate_spectra = "DownloadAndValidateSpectra"
    ingest_photometry_dataset = "IngestPhotometryDataset"
    name_check_and_reconcile = "NameCheckAndReconcile"


class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class JobRun(EntityBase):
    """
    Persistent record of a workflow/job invocation.
    """

    job_type: JobType
    status: JobStatus = Field(default=JobStatus.queued)

    correlation_id: UUID = Field(..., description="Correlates this run across events/services.")
    idempotency_key: str = Field(..., min_length=8, max_length=256)

    initiated_at: datetime = Field(default_factory=utcnow)
    finished_at: datetime | None = None

    # Optional linkage
    nova_id: UUID | None = None
    dataset_id: UUID | None = None

    # Operational metadata
    initiated_by: str | None = Field(
        default=None, description="Actor or service initiating the run."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_finished(self) -> JobRun:
        if self.finished_at is not None and self.finished_at < self.initiated_at:
            raise ValueError("finished_at cannot be earlier than initiated_at.")
        return self
