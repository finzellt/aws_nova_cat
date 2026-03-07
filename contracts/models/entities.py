# contracts/models/entities.py
from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator


def utcnow() -> datetime:
    return datetime.now(UTC)


# Shared format validator for discovery_date (Nova) and publication_date (Reference).
# YYYY-MM-DD where month is 01-12 and day is 00-31.
# Day 00 signals month-only precision (mirrors ADS pubdate convention).
# Never use 01 as a proxy for unknown day -- that is a data integrity error.
_DISCOVERY_DATE_RE = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[0-9]|[12][0-9]|3[01])$")

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

    discovery_date: str | None = Field(
        default=None,
        description=(
            "Discovery date in YYYY-MM-DD format. "
            "Day may be 00 when only month precision is available "
            "(e.g. '2013-06-00'). Sourced from ADS publication dates via "
            "ComputeDiscoveryDate. Never use 01 as a proxy for unknown day."
        ),
    )

    aliases: list[str] = Field(
        default_factory=list,
        description=(
            "Raw alias strings from SIMBAD ids field, e.g. 'NOVA Sco 2012', "
            "'Gaia DR3 4043499439062100096'. Denormalized on the Nova item so "
            "refresh_references can retrieve all known names in a single get_item "
            "call. Empty list if no SIMBAD aliases were returned at ingestion time."
        ),
    )

    # Optional quarantine context (only meaningful when status == QUARANTINED)
    quarantine_reason_code: NovaQuarantineReasonCode | None = None
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
    def validate_discovery_date_format(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not _DISCOVERY_DATE_RE.match(v):
            raise ValueError(
                "discovery_date must be YYYY-MM-DD format. "
                "Use 00 for day when day is unknown (e.g. '2013-06-00')."
            )
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
    failed_retryable = "FAILED_RETRYABLE"
    skipped_duplicate = "SKIPPED_DUPLICATE"
    skipped_backoff = "SKIPPED_BACKOFF"


class ValidationStatus(str, Enum):
    unvalidated = "UNVALIDATED"
    valid = "VALID"
    quarantined = "QUARANTINED"
    terminal_invalid = "TERMINAL_INVALID"


class Eligibility(str, Enum):
    acquire = "ACQUIRE"
    none = "NONE"


class ManualReviewStatus(str, Enum):
    pending = "PENDING"
    cleared_retry_approved = "CLEARED_RETRY_APPROVED"
    cleared_terminal = "CLEARED_TERMINAL"


class LastAttemptOutcome(str, Enum):
    """
    Operational outcome of the most recent acquisition/validation attempt.

    Separate from ValidationStatus and AcquisitionStatus by design:
    scientific state must not encode retryability (see execution-governance.md).
    """

    success = "SUCCESS"
    retryable_failure = "RETRYABLE_FAILURE"
    terminal_failure = "TERMINAL_FAILURE"
    quarantine = "QUARANTINE"


class NovaQuarantineReasonCode(str, Enum):
    """
    Quarantine reason codes for Nova identity quarantine (initialize_nova workflow).
    """

    coordinate_ambiguity = "COORDINATE_AMBIGUITY"
    other = "OTHER"


class SpectraQuarantineReasonCode(str, Enum):
    """
    Quarantine reason codes for spectra DataProduct validation quarantine
    (acquire_and_validate_spectra workflow).
    """

    unknown_profile = "UNKNOWN_PROFILE"
    missing_critical_metadata = "MISSING_CRITICAL_METADATA"
    checksum_mismatch = "CHECKSUM_MISMATCH"
    coordinate_proximity = "COORDINATE_PROXIMITY"
    other = "OTHER"


class DataProduct(PersistentBase):
    """
    Core unit of work.

    There are two product types:
    - PHOTOMETRY_TABLE (exactly one per nova)
    - SPECTRA (many per nova; one item per atomic spectra product)

    This contract is intentionally permissive: fields are type-dependent.

    data_product_id identity (SPECTRA only):
        Minted during discover_spectra_products via deterministic derivation:
            UUID(hash(provider + provider_product_key))        [preferred]
            UUID(hash(provider + normalized_canonical_locator)) [fallback when no native ID]
        Immutable once assigned; never reused across distinct products.
        See ADR-003 for the full specification.
    """

    schema_version: str = Field(default="1.0.0")

    data_product_id: UUID = Field(
        default_factory=uuid4,
        description=(
            "Stable, immutable UUID for this data product. "
            "For SPECTRA: minted during discover_spectra_products via deterministic derivation — "
            "UUID(hash(provider + provider_product_key)), falling back to "
            "UUID(hash(provider + normalized_canonical_locator)) when no provider-native ID exists. "
            "See ADR-003 for full specification."
        ),
    )
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
    last_attempt_outcome: LastAttemptOutcome | None = Field(
        default=None,
        description=(
            "Operational outcome of the most recent attempt. "
            "Kept separate from validation_status per the scientific/operational state separation invariant."
        ),
    )
    duplicate_of_data_product_id: UUID | None = Field(
        default=None,
        description=(
            "If this product was found to be a byte-level duplicate of an existing validated product, "
            "this field holds the canonical data_product_id. Set during acquire_and_validate_spectra "
            "when content_fingerprint matches an existing VALID product."
        ),
    )

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
    quarantine_reason_code: SpectraQuarantineReasonCode | None = None
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
    Global identity mapping: (provider + locator_identity) -> data_product_id.

    NOTE: This is a global-partition item in the single table (PK = LOCATOR#...).

    data_product_id here is the stable UUID previously minted during discover_spectra_products
    via: UUID(hash(provider + provider_product_key)) or, as a fallback,
    UUID(hash(provider + normalized_canonical_locator)). See ADR-003 for full specification.
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
    spectra_raw_fits = "SPECTRA_RAW_FITS"
    spectra_quarantine_context = "SPECTRA_QUARANTINE_CONTEXT"
    spectra_normalized = "SPECTRA_NORMALIZED"
    spectra_plot = "SPECTRA_PLOT"
    photometry_table = "PHOTOMETRY_TABLE"
    photometry_snapshot = "PHOTOMETRY_SNAPSHOT"
    workflow_quarantine_context = "WORKFLOW_QUARANTINE_CONTEXT"  # ← covers initialize_nova case
    bundle_manifest = "BUNDLE_MANIFEST"
    bundle_zip = "BUNDLE_ZIP"
    other = "OTHER"


class FileObject(PersistentBase):
    """
    Registry entry for an S3 object associated with a data product.

    This is not the S3 layout spec; it's a lightweight index/provenance record.
    """

    schema_version: str = Field(default="1.0.0")

    file_id: UUID = Field(default_factory=uuid4)
    nova_id: UUID | None = None  # None before nova exists (e.g. initialize_nova quarantine)
    data_product_id: UUID | None = Field(
        default=None,
        description=(
            "UUID of the associated data product, if product-scoped. None when not product-scoped. "
            "For SPECTRA products, this is the stable UUID minted during discover_spectra_products — "
            "see DataProduct.data_product_id and ADR-003 for derivation details."
        ),
    )
    role: FileRole
    bucket: str = Field(..., min_length=1, max_length=256)
    key: str = Field(..., min_length=1, max_length=2048)
    content_type: str | None = None
    byte_length: int | None = Field(default=None, ge=0)
    etag: str | None = Field(default=None, max_length=512)
    sha256: str | None = Field(default=None, max_length=256)
    provenance: Provenance | None = None

    created_by: str | None = Field(
        default=None,
        max_length=512,
        description="Workflow and run context (e.g., '<job_type>:<job_run_id>').",
    )

    url: HttpUrl | None = Field(
        default=None,
        description="Optional upstream URL if the object was sourced externally.",
    )


# ----------------------------
# References
# ----------------------------


class ReferenceType(str, Enum):
    journal_article = "journal_article"
    conference_abstract = "conference_abstract"
    poster = "poster"
    catalog = "catalog"
    software = "software"
    atel = "atel"
    cbat_circular = "cbat_circular"
    arxiv_preprint = "arxiv_preprint"
    other = "other"


class Reference(PersistentBase):
    """
    Global bibliographic record for an ADS-sourced work.

    Identity: bibcode is the stable, globally unique ADS identifier and serves
    as the DDB partition key (REFERENCE#<bibcode>). No internal UUID is assigned.

    Dedup: UpsertReferenceEntity performs a direct GetItem on REFERENCE#<bibcode>.
    No secondary lookup table (REFINDEX) is required.

    Scope: ADS-only. Donated data provenance belongs on DataProduct.provenance,
    not in the reference system.
    """

    schema_version: str = Field(default="1.0.0")

    bibcode: str = Field(
        ...,
        min_length=1,
        max_length=19,
        description="ADS bibcode. Globally unique and stable. Used as the DDB partition key.",
    )
    reference_type: ReferenceType = Field(default=ReferenceType.journal_article)

    title: str | None = Field(default=None, max_length=2000)
    year: int | None = Field(default=None, ge=1800, le=2500)
    publication_date: str | None = Field(
        default=None,
        description=(
            "Publication date in YYYY-MM-DD format. "
            "Day may be 00 when only month precision is available "
            "(e.g. '2013-06-00'). Sourced directly from ADS pubdate field, "
            "which already uses YYYY-MM-00 format -- no transformation required. "
            "Never use 01 as a proxy for unknown day."
        ),
    )
    authors: list[str] = Field(default_factory=list)

    doi: str | None = Field(
        default=None,
        max_length=512,
        description="DOI string if available (e.g. 10.1088/2041-8205/770/1/L32).",
    )
    arxiv_id: str | None = Field(
        default=None,
        max_length=32,
        description="Bare arXiv ID if available (e.g. 1306.1213). No arXiv: prefix.",
    )

    provenance: Provenance | None = None

    @field_validator("bibcode")
    @classmethod
    def reject_blank_bibcode(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("bibcode cannot be blank.")
        return v

    @field_validator("publication_date")
    @classmethod
    def validate_publication_date_format(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not _DISCOVERY_DATE_RE.match(v):
            raise ValueError(
                "publication_date must be YYYY-MM-DD format. "
                "Use 00 for day when day is unknown (e.g. '2013-06-00')."
            )
        return v

    @field_validator("arxiv_id")
    @classmethod
    def strip_arxiv_prefix(cls, v: str | None) -> str | None:
        """Normalise to bare ID regardless of how the caller passes it in."""
        if v is None:
            return None
        v = v.strip()
        if v.lower().startswith("arxiv:"):
            v = v[6:]
        return v or None


class NovaReferenceRole(str, Enum):
    discovery = "DISCOVERY"
    spectra_source = "SPECTRA_SOURCE"
    photometry_source = "PHOTOMETRY_SOURCE"
    other = "OTHER"


class NovaReference(PersistentBase):
    """
    Per-nova link connecting a Nova to a global Reference.
    This is the unit used to build a nova bibliography page.

    Identity: the link is fully identified by (nova_id, bibcode).
    DDB key: PK=<nova_id>, SK=NOVAREF#<bibcode>.
    No internal UUID is assigned to the link itself.
    """

    schema_version: str = Field(default="1.0.0")

    nova_id: UUID
    bibcode: str = Field(
        ...,
        min_length=1,
        max_length=19,
        description="ADS bibcode. FK to REFERENCE#<bibcode> / METADATA.",
    )

    role: NovaReferenceRole = Field(
        default=NovaReferenceRole.other,
        description="The role this reference plays for this nova.",
    )
    added_by_workflow: str | None = Field(
        default=None,
        max_length=128,
        description="Name of the workflow that created this link.",
    )

    notes: str | None = Field(default=None, max_length=4000)

    # Link-level provenance: how/why this Reference was associated with this nova
    # (e.g., which ADS query strategy surfaced it).
    provenance: Provenance | None = None


# ----------------------------
# JobRun / Attempt (operational trace)
# ----------------------------


class AttemptStatus(str, Enum):
    started = "STARTED"
    succeeded = "SUCCEEDED"
    failed = "FAILED"
    timed_out = "TIMED_OUT"
    cancelled = "CANCELLED"


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
    ended_at: datetime | None = None

    error_type: str | None = Field(default=None, max_length=128)
    error_message: str | None = Field(default=None, max_length=4000)

    task_name: str | None = Field(
        default=None,
        max_length=256,
        description="Step Functions state name for this attempt.",
    )
    duration_ms: int | None = Field(default=None, ge=0)

    request_id: str | None = Field(default=None, description="AWS Lambda request id, if relevant.")
    execution_arn: str | None = Field(
        default=None, description="Step Functions execution ARN, if relevant."
    )

    @model_validator(mode="after")
    def validate_finished_at(self) -> Attempt:
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot be earlier than started_at.")
        return self


class JobType(str, Enum):
    initialize_nova = "InitializeNova"
    ingest_new_nova = "IngestNewNova"
    refresh_references = "RefreshReferences"
    discover_spectra_products = "DiscoverSpectraProducts"
    acquire_and_validate_spectra = "AcquireAndValidateSpectra"
    ingest_photometry = "IngestPhotometry"
    name_check_and_reconcile = "NameCheckAndReconcile"


class JobStatus(str, Enum):
    queued = "QUEUED"
    running = "RUNNING"
    succeeded = "SUCCEEDED"
    failed = "FAILED"
    quarantined = "QUARANTINED"
    cancelled = "CANCELLED"


class JobRun(PersistentBase):
    """
    Persistent record of a workflow/job invocation.
    """

    schema_version: str = Field(default="1.0.0")

    job_run_id: UUID = Field(default_factory=uuid4)

    job_type: JobType
    workflow_name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Human-readable Step Functions workflow name (e.g., 'acquire_and_validate_spectra').",
    )
    status: JobStatus = Field(default=JobStatus.queued)

    execution_arn: str | None = Field(
        default=None,
        max_length=2048,
        description="Step Functions execution ARN for this run. Used to trace back to execution history.",
    )

    correlation_id: UUID = Field(..., description="Correlates this run across events/services.")
    idempotency_key: str = Field(..., min_length=8, max_length=256)

    started_at: datetime = Field(default_factory=utcnow)
    ended_at: datetime | None = None

    # Optional linkage for convenience/traceability
    nova_id: UUID | None = None
    data_product_id: UUID | None = Field(
        default=None,
        description=(
            "UUID of the data product associated with this run, if applicable. "
            "For SPECTRA workflows, this is the stable UUID minted during discover_spectra_products. "
            "See DataProduct.data_product_id and ADR-003 for derivation details."
        ),
    )

    initiated_by: str | None = Field(
        default=None, description="Actor or service initiating the run."
    )
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_finished(self) -> JobRun:
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot be earlier than started_at.")
        return self
