# contracts/models/entities.py
from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any
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

    schema_version: str = Field(default="1.0.0")

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
# Nova + global identity
# ----------------------------


class NovaStatus(str, Enum):
    # Aligns to the persistence model (ACTIVE/MERGED/DEPRECATED) plus the
    # explicit workflow-driven quarantine state.
    active = "ACTIVE"
    quarantined = "QUARANTINED"
    merged = "MERGED"
    deprecated = "DEPRECATED"


class SkyCoordFrame(str, Enum):
    icrs = "ICRS"


class Position(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ra_deg: float = Field(..., ge=0.0, lt=360.0, description="Right ascension in degrees (ICRS).")
    dec_deg: float = Field(..., ge=-90.0, le=90.0, description="Declination in degrees (ICRS).")
    frame: SkyCoordFrame = Field(default=SkyCoordFrame.icrs)
    epoch: str | None = Field(default="J2000", description="Epoch label, e.g., 'J2000'.")

    provenance: Provenance | None = None


class Nova(PersistentBase):
    """
    Canonical nova record.

    Identity: nova_id (UUID) is the only authoritative internal identifier.
    Naming: primary_name is the current canonical display name.
    Coordinates: flattened in persistence, but modeled here as Position for clarity.
    """

    schema_version: str = Field(default="1.0.0")

    nova_id: UUID = Field(
        default_factory=uuid4, description="Stable, system-wide identifier for this nova."
    )
    primary_name: str = Field(..., min_length=1, max_length=256)
    primary_name_normalized: str = Field(..., min_length=1, max_length=256)
    status: NovaStatus = Field(default=NovaStatus.active)

    # Coordinates are modeled as a sub-object contractually; persistence may flatten them.
    position: Position | None = None

    discovery_date: datetime | None = None

    # Optional quarantine context (only meaningful when status == QUARANTINED)
    quarantine_reason_code: str | None = Field(default=None, max_length=128)
    manual_review_status: str | None = Field(default=None, max_length=64)

    provenance: Provenance | None = None

    @field_validator("primary_name")
    @classmethod
    def reject_blank_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("primary_name cannot be blank.")
        return v

    @field_validator("primary_name_normalized")
    @classmethod
    def reject_blank_normalized_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("primary_name_normalized cannot be blank.")
        return v

    @field_validator("discovery_date")
    @classmethod
    def ensure_discovery_tz_aware_if_present(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise ValueError("discovery_date must be timezone-aware (UTC).")
        return v


class NameKind(str, Enum):
    primary = "PRIMARY"
    alias = "ALIAS"


class NameMappingSource(str, Enum):
    user_input = "USER_INPUT"
    ingestion = "INGESTION"
    simbad = "SIMBAD"
    tns = "TNS"
    other = "OTHER"


class NameMapping(PersistentBase):
    """
    Global identity mapping: normalized name -> nova_id.

    NOTE: This is a global-partition item in the single table (PK = NAME#...).
    """

    schema_version: str = Field(default="1.0.0")

    name_raw: str = Field(..., min_length=1, max_length=256)
    name_normalized: str = Field(..., min_length=1, max_length=256)
    name_kind: NameKind = Field(default=NameKind.alias)
    nova_id: UUID
    source: NameMappingSource = Field(default=NameMappingSource.other)

    @field_validator("name_raw", "name_normalized")
    @classmethod
    def reject_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Name fields cannot be blank.")
        return v


# ----------------------------
# DataProducts + LocatorAlias
# ----------------------------


class ProductType(str, Enum):
    photometry_table = "PHOTOMETRY_TABLE"
    spectra = "SPECTRA"


class LocatorKind(str, Enum):
    url = "URL"
    s3 = "S3"
    other = "OTHER"


class LocatorRole(str, Enum):
    primary = "PRIMARY"
    mirror = "MIRROR"


class Locator(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: LocatorKind
    role: LocatorRole = Field(default=LocatorRole.primary)
    value: str = Field(..., min_length=1, max_length=2048)


class AcquisitionStatus(str, Enum):
    stub = "STUB"
    acquired = "ACQUIRED"
    failed = "FAILED"


class ValidationStatus(str, Enum):
    unvalidated = "UNVALIDATED"
    valid = "VALID"
    quarantined = "QUARANTINED"
    invalid = "INVALID"


class Eligibility(str, Enum):
    acquire = "ACQUIRE"
    none = "NONE"


class ManualReviewStatus(str, Enum):
    pending = "PENDING"
    cleared_retry_approved = "CLEARED_RETRY_APPROVED"
    cleared_terminal = "CLEARED_TERMINAL"


class DataProduct(PersistentBase):
    """
    Core unit of work.

    There are two product types:
    - PHOTOMETRY_TABLE (exactly one per nova)
    - SPECTRA (many per nova; one item per atomic spectra product)

    This contract is intentionally permissive: fields are type-dependent.
    """

    schema_version: str = Field(default="1.0.0")

    data_product_id: UUID = Field(default_factory=uuid4)
    nova_id: UUID
    product_type: ProductType

    # --- spectra-only identity fields
    provider: str | None = Field(default=None, max_length=128)
    locator_identity: str | None = Field(
        default=None,
        max_length=2048,
        description="Normalized stable locator identity (e.g., provider_product_id:<id> or url:<normalized_url>).",
    )
    locators: list[Locator] = Field(default_factory=list)
    hints: dict[str, Any] = Field(default_factory=dict)

    # --- lifecycle
    acquisition_status: AcquisitionStatus | None = None
    validation_status: ValidationStatus | None = None
    eligibility: Eligibility | None = None

    # --- cooldown/backoff fields (spectra)
    attempt_count: int | None = Field(default=None, ge=0)
    last_attempt_at: datetime | None = None
    next_eligible_attempt_at: datetime | None = None
    last_error_fingerprint: str | None = Field(default=None, max_length=256)

    # --- fingerprints/checksums (spectra)
    byte_length: int | None = Field(default=None, ge=0)
    etag: str | None = Field(default=None, max_length=512)
    sha256: str | None = Field(default=None, max_length=256)
    header_signature_hash: str | None = Field(default=None, max_length=256)

    # --- profile-driven validation outputs (spectra)
    fits_profile_id: str | None = Field(default=None, max_length=256)
    profile_selection_inputs: dict[str, Any] = Field(default_factory=dict)
    normalization_notes: list[str] = Field(default_factory=list)

    # --- quarantine gating (spectra and/or nova-level workflows may reference these)
    quarantine_reason_code: str | None = Field(default=None, max_length=128)
    manual_review_status: ManualReviewStatus | None = None

    # --- S3 pointers (spectra raw) / (photometry derived)
    raw_s3_bucket: str | None = Field(default=None, max_length=256)
    raw_s3_key: str | None = Field(default=None, max_length=2048)
    derived_s3_prefix: str | None = Field(default=None, max_length=2048)

    # photometry-table current parquet pointer
    s3_bucket: str | None = Field(default=None, max_length=256)
    s3_key: str | None = Field(default=None, max_length=2048)

    # photometry ingestion summary
    last_ingestion_at: datetime | None = None
    last_ingestion_source: str | None = Field(default=None, max_length=512)
    ingestion_count: int | None = Field(default=None, ge=0)

    provenance: Provenance | None = None

    @model_validator(mode="after")
    def validate_by_product_type(self) -> DataProduct:
        if self.product_type == ProductType.spectra:
            if not self.provider or not self.provider.strip():
                raise ValueError("SPECTRA DataProduct requires provider.")
            if not self.locator_identity or not self.locator_identity.strip():
                raise ValueError("SPECTRA DataProduct requires locator_identity.")
            if self.acquisition_status is None:
                raise ValueError("SPECTRA DataProduct requires acquisition_status.")
            if self.validation_status is None:
                raise ValueError("SPECTRA DataProduct requires validation_status.")
            if self.eligibility is None:
                raise ValueError("SPECTRA DataProduct requires eligibility.")
            if self.attempt_count is None:
                # keep MVP-friendly: default to 0 when omitted
                self.attempt_count = 0

        if self.product_type == ProductType.photometry_table and (
            self.provider is not None or self.locator_identity is not None or self.locators
        ):
            # Photometry is a singleton product per nova; no provider/locator identity required.
            # Require only that we can point at the current parquet when present.
            raise ValueError("PHOTOMETRY_TABLE DataProduct must not set provider/locator fields.")
        return self


class LocatorAlias(PersistentBase):
    """
    Global identity mapping: (provider + locator_identity) -> data_product_id

    NOTE: This is a global-partition item in the single table (PK = LOCATOR#...).
    """

    schema_version: str = Field(default="1.0.0")

    provider: str = Field(..., min_length=1, max_length=128)
    locator_identity: str = Field(..., min_length=1, max_length=2048)
    data_product_id: UUID
    nova_id: UUID


# ----------------------------
# FileObject (optional registry for S3 objects)
# ----------------------------


class FileRole(str, Enum):
    raw_fits = "RAW_FITS"
    quarantine_context = "QUARANTINE_CONTEXT"
    normalized = "NORMALIZED"
    plot = "PLOT"
    manifest = "MANIFEST"
    data_bundle = "DATA_BUNDLE"
    other = "OTHER"


class FileObject(PersistentBase):
    """
    Registry entry for an S3 object associated with a data product.

    This is not the S3 layout spec; it's a lightweight index/provenance record.
    """

    schema_version: str = Field(default="1.0.0")

    file_id: UUID = Field(default_factory=uuid4)

    nova_id: UUID
    data_product_id: UUID
    product_type: ProductType

    role: FileRole
    bucket: str = Field(..., min_length=1, max_length=256)
    key: str = Field(..., min_length=1, max_length=2048)

    content_type: str | None = Field(default=None, max_length=256)
    byte_length: int | None = Field(default=None, ge=0)
    etag: str | None = Field(default=None, max_length=512)
    sha256: str | None = Field(default=None, max_length=256)

    created_by: str | None = Field(
        default=None,
        max_length=512,
        description="Workflow and run context (e.g., '<job_type>:<job_run_id>').",
    )

    url: HttpUrl | None = Field(
        default=None,
        description="Optional upstream URL if the object was sourced externally.",
    )
    provenance: Provenance | None = None


# ----------------------------
# References
# ----------------------------


class ReferenceType(str, Enum):
    journal_article = "journal_article"
    conference_abstract = "conference_abstract"
    poster = "poster"
    catalog = "catalog"
    software = "software"
    other = "other"


class IdentifierType(str, Enum):
    ads_bibcode = "ads_bibcode"
    doi = "doi"
    arxiv = "arxiv"
    vizier = "vizier"
    url = "url"
    other = "other"


class Identifier(BaseModel):
    """
    A typed identifier for a Reference (supports ADS bibcodes, DOIs, arXiv IDs, VizieR IDs, URLs, etc.).
    """

    model_config = ConfigDict(extra="forbid")

    id_type: IdentifierType
    value: str = Field(..., min_length=1, max_length=256)

    @field_validator("value")
    @classmethod
    def reject_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Identifier value cannot be blank.")
        return v


class Reference(PersistentBase):
    """
    Global bibliographic/work record (paper, poster, catalog entry, etc.).
    Deduplicated by stable external identifiers when available.
    """

    schema_version: str = Field(default="1.0.0")

    reference_id: UUID = Field(default_factory=uuid4)
    reference_type: ReferenceType = Field(default=ReferenceType.journal_article)

    identifiers: list[Identifier] = Field(
        default_factory=list,
        description="Typed external identifiers for this reference (e.g., ADS bibcode, DOI, arXiv, VizieR, URL).",
    )

    title: str | None = Field(default=None, max_length=2000)
    year: int | None = Field(default=None, ge=1800, le=2500)
    authors: list[str] = Field(default_factory=list)
    url: HttpUrl | None = Field(
        default=None,
        description="Canonical URL if available (often derived from identifiers, but included for convenience).",
    )

    provenance: Provenance | None = None

    @model_validator(mode="after")
    def validate_identifiers(self) -> Reference:
        # Require at least one stable identifier for global dedupe, OR allow url/title as a fallback.
        if not self.identifiers and not self.url and not self.title:
            raise ValueError(
                "Reference must include at least one identifier, or a url/title fallback."
            )

        # Deduplicate identifiers by (type, value) exact match.
        seen: set[tuple[str, str]] = set()
        deduped: list[Identifier] = []
        for ident in self.identifiers:
            key = (ident.id_type.value, ident.value)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(ident)
        self.identifiers = deduped
        return self


class NovaReference(PersistentBase):
    """
    Per-nova link connecting a Nova to a global Reference.
    This is the unit used to build a nova's bibliography page.
    """

    schema_version: str = Field(default="1.0.0")

    nova_reference_id: UUID = Field(default_factory=uuid4)

    nova_id: UUID
    reference_id: UUID

    notes: str | None = Field(default=None, max_length=4000)

    # Link-level provenance: how/why this Reference was associated with this nova (e.g., ADS query terms).
    provenance: Provenance | None = None


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

    schema_version: str = Field(default="1.0.0")

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
    refresh_references = "RefreshReferences"
    discover_spectra_products = "DiscoverSpectraProducts"
    acquire_and_validate_spectra = "AcquireAndValidateSpectra"
    ingest_photometry = "IngestPhotometryEvent"
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

    schema_version: str = Field(default="1.0.0")

    job_run_id: UUID = Field(default_factory=uuid4)

    job_type: JobType
    status: JobStatus = Field(default=JobStatus.queued)

    correlation_id: UUID = Field(..., description="Correlates this run across events/services.")
    idempotency_key: str = Field(..., min_length=8, max_length=256)

    initiated_at: datetime = Field(default_factory=utcnow)
    finished_at: datetime | None = None

    # Optional linkage for convenience/traceability
    nova_id: UUID | None = None
    data_product_id: UUID | None = None

    initiated_by: str | None = Field(
        default=None, description="Actor or service initiating the run."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_finished(self) -> JobRun:
        if self.finished_at is not None and self.finished_at < self.initiated_at:
            raise ValueError("finished_at cannot be earlier than initiated_at.")
        return self
