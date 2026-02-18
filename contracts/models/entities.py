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


class PersistentBase(BaseModel):
    """
    Base for all persistent entities.

    NOTE: We intentionally do NOT include a generic `id` field.
    Every entity uses an explicit `<entity>_id` field for clarity in contracts.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(
        default="1.0.0",
        description="Semantic version for this persistent entity contract.",
        examples=["1.0.0"],
    )
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


class Provenance(PersistentBase):
    """
    Scientific provenance: enough to trace 'what said this' and 'when',
    without overfitting to any single upstream system.
    """

    source: str = Field(
        ...,
        description="Canonical identifier for upstream source (e.g., 'SIMBAD', 'ADS', 'ASAS-SN', 'AAVSO', 'UserUpload').",
        min_length=1,
        max_length=128,
    )
    source_record_id: str | None = Field(
        default=None,
        description="Upstream record identifier, if provided.",
        max_length=256,
    )
    retrieved_at: datetime = Field(default_factory=utcnow)
    method: ProvenanceMethod = Field(default=ProvenanceMethod.imported)
    asserted_by: str | None = Field(
        default=None,
        description="Human or system actor that asserted this fact (e.g., ORCID, service name).",
        max_length=256,
    )
    citation: str | None = Field(
        default=None,
        description="Optional citation string or bibcode/doi reference.",
        max_length=512,
    )
    notes: str | None = Field(default=None, max_length=2000)

    @field_validator("retrieved_at")
    @classmethod
    def ensure_retrieved_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise ValueError("retrieved_at must be timezone-aware (UTC).")
        return v


# ----------------------------
# Nova
# ----------------------------


class NovaStatus(str, Enum):
    candidate = "candidate"
    confirmed = "confirmed"
    retracted = "retracted"


class SkyCoordFrame(str, Enum):
    icrs = "icrs"


class Position(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ra_deg: float = Field(..., ge=0.0, lt=360.0, description="Right ascension in degrees.")
    dec_deg: float = Field(..., ge=-90.0, le=90.0, description="Declination in degrees.")
    frame: SkyCoordFrame = Field(default=SkyCoordFrame.icrs)
    epoch: str | None = Field(default="J2000", description="Epoch label, e.g., 'J2000'.")

    provenance: Provenance | None = None


class Nova(PersistentBase):
    """
    Core nova record.

    Identity: nova_id (UUID) is the only authoritative internal identifier.
    Naming: public_name is the current canonical display name.
    Aliases: pragmatic search terms (often copied from SIMBAD / observed elsewhere).
    """

    nova_id: UUID = Field(
        default_factory=uuid4, description="Stable, system-wide identifier for this nova."
    )
    public_name: str = Field(..., min_length=1, max_length=256)
    status: NovaStatus = Field(default=NovaStatus.candidate)

    position: Position | None = None
    discovery_date: datetime | None = None

    # Aliases are search terms; we keep them simple and minimally validated.
    aliases: list[str] = Field(
        default_factory=list,
        description="Known external name strings used for discovery/matching (e.g., SIMBAD alias list).",
    )

    provenance: Provenance | None = None

    @field_validator("public_name")
    @classmethod
    def reject_blank_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("public_name cannot be blank.")
        return v

    @field_validator("aliases")
    @classmethod
    def validate_aliases(cls, v: list[str]) -> list[str]:
        # Keep aliases as "as-seen" search terms; only reject useless entries and exact duplicates.
        out: list[str] = []
        seen: set[str] = set()
        for s in v:
            if s is None:
                continue
            if s.strip() == "":
                continue
            if len(s) > 512:
                raise ValueError("Alias too long (>512 chars).")
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out

    @field_validator("discovery_date")
    @classmethod
    def ensure_discovery_tz_aware_if_present(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise ValueError("discovery_date must be timezone-aware (UTC).")
        return v


# ----------------------------
# Files and Datasets
# ----------------------------


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


class FileObject(PersistentBase):
    """
    Logical file contract (not storage layout): identity + integrity + descriptive metadata.
    """

    file_id: UUID = Field(default_factory=uuid4)

    filename: str = Field(..., min_length=1, max_length=512)
    media_type: str | None = Field(
        default=None, description="IANA media type if known, e.g. 'application/fits'."
    )
    size_bytes: int | None = Field(default=None, ge=0)
    role: FileRole = Field(default=FileRole.primary)
    url: HttpUrl | None = Field(default=None, description="Upstream URL if externally sourced.")
    digest: ContentDigest | None = Field(default=None)

    provenance: Provenance | None = None


class Dataset(PersistentBase):
    """
    A coherent scientific dataset associated with a single nova.
    Many datasets may reference the same nova via `nova_id`.
    Datasets are the unit of ingestion, validation, and publication.
    """

    dataset_id: UUID = Field(default_factory=uuid4)

    nova_id: UUID
    kind: DatasetKind
    status: DatasetStatus = Field(default=DatasetStatus.discovered)

    file_ids: list[UUID] = Field(default_factory=list)

    title: str | None = Field(default=None, max_length=512)
    observed_start: datetime | None = None
    observed_end: datetime | None = None

    provenance: Provenance | None = None

    attributes: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form attributes for incremental enrichment (kept small; avoid embedding huge payloads).",
    )

    @model_validator(mode="after")
    def validate_observation_window(self) -> Dataset:
        if self.observed_start and self.observed_end and self.observed_end < self.observed_start:
            raise ValueError("observed_end cannot be earlier than observed_start.")
        return self


# ----------------------------
# Papers
# ----------------------------


class PaperIdType(str, Enum):
    bibcode = "bibcode"
    doi = "doi"
    arxiv = "arxiv"


class Paper(PersistentBase):
    """
    Paper metadata (primarily from ADS). Keep fields stable and portable.
    """

    paper_id: UUID = Field(default_factory=uuid4)

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


# ----------------------------
# JobRun / Attempt (operational trace)
# ----------------------------


class AttemptStatus(str, Enum):
    started = "started"
    succeeded = "succeeded"
    failed = "failed"
    timed_out = "timed_out"
    cancelled = "cancelled"


class Attempt(PersistentBase):
    """
    One execution attempt for a job/workflow task. Stored for traceability.
    """

    attempt_id: UUID = Field(default_factory=uuid4)

    job_run_id: UUID
    attempt_number: int = Field(..., ge=1)
    status: AttemptStatus = Field(default=AttemptStatus.started)

    started_at: datetime = Field(default_factory=utcnow)
    finished_at: datetime | None = None

    error_code: str | None = Field(default=None, max_length=128)
    error_message: str | None = Field(default=None, max_length=4000)

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


class JobRun(PersistentBase):
    """
    Persistent record of a workflow/job invocation.
    """

    job_run_id: UUID = Field(default_factory=uuid4)

    job_type: JobType
    status: JobStatus = Field(default=JobStatus.queued)

    correlation_id: UUID = Field(..., description="Correlates this run across events/services.")
    idempotency_key: str = Field(..., min_length=8, max_length=256)

    initiated_at: datetime = Field(default_factory=utcnow)
    finished_at: datetime | None = None

    # Optional linkage for convenience/traceability
    nova_id: UUID | None = None
    dataset_id: UUID | None = None

    initiated_by: str | None = Field(
        default=None, description="Actor or service initiating the run."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_finished(self) -> JobRun:
        if self.finished_at is not None and self.finished_at < self.initiated_at:
            raise ValueError("finished_at cannot be earlier than initiated_at.")
        return self
