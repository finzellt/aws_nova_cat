# Nova Cat — Compressed Contracts & ADRs

---

## 1. ENTITIES (`contracts/models/entities.py`)

### Shared foundations

```python
# All timestamps must be timezone-aware UTC.
# schema_version default = "1.0.0" on all persistent entities.
# model_config = ConfigDict(extra="forbid") on all models.

_DISCOVERY_DATE_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[0-9]|[12][0-9]|3[01])$"
# Shared by Nova.discovery_date and Reference.publication_date
# Day 00 = month-only precision. NEVER use 01 as proxy for unknown day.
# Valid: "2013-06-00" (month known), "2013-06-14" (full). Invalid: "2013-6-0", "2013-00-00", "2013-06"

class PersistentBase(BaseModel):
    schema_version: str = "1.0.0"
    created_at: datetime = utcnow()
    updated_at: datetime = utcnow()

class ProvenanceMethod(Enum): manual | scraped | computed | imported

class Provenance(PersistentBase):
    source: str                        # e.g. "SIMBAD", "ADS", "AAVSO"
    source_record_id: str | None
    retrieved_at: datetime
    method: ProvenanceMethod = imported
    asserted_by: str | None
    citation: str | None
    notes: str | None
```

### Nova + Identity

```python
class NovaStatus(Enum):        ACTIVE | QUARANTINED | MERGED | DEPRECATED
class SkyCoordFrame(Enum):     ICRS
class NovaQuarantineReasonCode(Enum): COORDINATE_AMBIGUITY | OTHER
class NameKind(Enum):          PRIMARY | ALIAS
class NameMappingSource(Enum): USER_INPUT | INGESTION | SIMBAD | TNS | OTHER

class Position(BaseModel):
    ra_deg: float      # ge=0.0, lt=360.0
    dec_deg: float     # ge=-90.0, le=90.0
    frame: SkyCoordFrame = ICRS
    epoch: str | None = "J2000"
    provenance: Provenance | None

class Nova(PersistentBase):
    nova_id: UUID = uuid4()
    primary_name: str              # non-blank, max 256
    primary_name_normalized: str   # non-blank, max 256
    status: NovaStatus = ACTIVE
    position: Position | None
    discovery_date: str | None     # YYYY-MM-DD; day 00 ok; validated by _DISCOVERY_DATE_RE
    aliases: list[str] = []        # raw SIMBAD ids; denormalized for single get_item in refresh_references
    quarantine_reason_code: NovaQuarantineReasonCode | None
    manual_review_status: str | None
    provenance: Provenance | None

class NameMapping(PersistentBase):
    # DDB: PK="NAME#<normalized_name>", SK="NOVA#<nova_id>"
    name_raw: str
    name_normalized: str
    name_kind: NameKind = ALIAS
    nova_id: UUID
    source: NameMappingSource = OTHER
```

### DataProduct + Locators

```python
class ProductType(Enum):       PHOTOMETRY_TABLE | SPECTRA
class LocatorKind(Enum):       URL | S3 | OTHER
class LocatorRole(Enum):       PRIMARY | MIRROR
class AcquisitionStatus(Enum): STUB | ACQUIRED | FAILED_RETRYABLE | SKIPPED_DUPLICATE | SKIPPED_BACKOFF
class ValidationStatus(Enum):  UNVALIDATED | VALID | QUARANTINED | TERMINAL_INVALID
class Eligibility(Enum):       ACQUIRE | NONE
class ManualReviewStatus(Enum): PENDING | CLEARED_RETRY_APPROVED | CLEARED_TERMINAL
class LastAttemptOutcome(Enum): SUCCESS | RETRYABLE_FAILURE | TERMINAL_FAILURE | QUARANTINE
    # Separate from ValidationStatus/AcquisitionStatus — scientific state must not encode retryability

class SpectraQuarantineReasonCode(Enum):
    UNKNOWN_PROFILE | MISSING_CRITICAL_METADATA | CHECKSUM_MISMATCH | COORDINATE_PROXIMITY | OTHER

class Locator(BaseModel):
    kind: LocatorKind
    role: LocatorRole = PRIMARY
    value: str   # max 2048

class DataProduct(PersistentBase):
    data_product_id: UUID = uuid4()
    nova_id: UUID
    product_type: ProductType

    # spectra-only identity
    provider: str | None                  # required for SPECTRA
    locator_identity: str | None          # required for SPECTRA; "provider_product_id:<id>" or "url:<url>"
    locators: list[Locator] = []
    hints: dict = {}

    # lifecycle
    acquisition_status: AcquisitionStatus | None   # required for SPECTRA
    validation_status: ValidationStatus | None     # required for SPECTRA
    eligibility: Eligibility | None                # required for SPECTRA

    # cooldown (spectra)
    attempt_count: int | None              # ge=0; defaults to 0 if omitted
    last_attempt_at: datetime | None
    next_eligible_attempt_at: datetime | None
    last_error_fingerprint: str | None
    last_attempt_outcome: LastAttemptOutcome | None
    duplicate_of_data_product_id: UUID | None   # set when byte-level dup of existing VALID product

    # fingerprints (spectra; populated post-acquisition)
    byte_length: int | None
    etag: str | None
    sha256: str | None
    header_signature_hash: str | None

    # profile-driven validation (spectra)
    fits_profile_id: str | None
    profile_selection_inputs: dict = {}
    normalization_notes: list[str] = []

    # quarantine gating
    quarantine_reason_code: SpectraQuarantineReasonCode | None
    manual_review_status: ManualReviewStatus | None

    # S3 (spectra)
    raw_s3_bucket: str | None
    raw_s3_key: str | None
    derived_s3_prefix: str | None

    # S3 (photometry)
    s3_bucket: str | None
    s3_key: str | None
    last_ingestion_at: datetime | None
    last_ingestion_source: str | None
    ingestion_count: int | None

    provenance: Provenance | None

    # model_validator: SPECTRA requires provider, locator_identity, acquisition_status,
    #   validation_status, eligibility; attempt_count defaults to 0.
    # PHOTOMETRY_TABLE must NOT set provider/locator_identity/locators.

class LocatorAlias(PersistentBase):
    # DDB: PK="LOCATOR#<provider>#<locator_identity>", SK="DATA_PRODUCT#<data_product_id>"
    provider: str
    locator_identity: str
    data_product_id: UUID
    nova_id: UUID
```

### FileObject

```python
class FileRole(Enum):
    SPECTRA_RAW_FITS | SPECTRA_QUARANTINE_CONTEXT | SPECTRA_NORMALIZED | SPECTRA_PLOT
    PHOTOMETRY_TABLE | PHOTOMETRY_SNAPSHOT
    WORKFLOW_QUARANTINE_CONTEXT   # PK=WORKFLOW#<correlation_id> when nova_id not yet known
    BUNDLE_MANIFEST | BUNDLE_ZIP | OTHER

class FileObject(PersistentBase):
    file_id: UUID = uuid4()
    nova_id: UUID | None           # None before nova exists
    data_product_id: UUID | None   # None when not product-scoped
    role: FileRole
    bucket: str
    key: str
    content_type: str | None
    byte_length: int | None
    etag: str | None
    sha256: str | None
    provenance: Provenance | None
    created_by: str | None         # "<workflow>:<job_run_id>"
    url: HttpUrl | None            # if sourced externally
```

### References

```python
class ReferenceType(Enum):
    journal_article | conference_abstract | poster | catalog | software
    atel | cbat_circular | arxiv_preprint | other

class Reference(PersistentBase):
    # DDB: PK="REFERENCE#<bibcode>", SK="METADATA". Global; no internal UUID.
    bibcode: str           # max 19 chars; ADS globally unique stable key
    reference_type: ReferenceType = journal_article
    title: str | None
    year: int | None       # ge=1800, le=2500
    publication_date: str | None   # YYYY-MM-DD; day 00 ok; validated by _DISCOVERY_DATE_RE
    authors: list[str] = []
    doi: str | None
    arxiv_id: str | None   # bare ID, no "arXiv:" prefix (stripped by validator)
    provenance: Provenance | None
    # ADS URL always derivable: https://ui.adsabs.harvard.edu/abs/<bibcode> — NOT stored.

class NovaReferenceRole(Enum): DISCOVERY | SPECTRA_SOURCE | PHOTOMETRY_SOURCE | OTHER

class NovaReference(PersistentBase):
    # DDB: PK=<nova_id>, SK="NOVAREF#<bibcode>". No internal UUID on link.
    nova_id: UUID
    bibcode: str           # max 19 chars; FK to REFERENCE#<bibcode>/METADATA
    role: NovaReferenceRole = OTHER
    added_by_workflow: str | None
    notes: str | None      # max 4000
    provenance: Provenance | None
```

### JobRun / Attempt

```python
class JobType(Enum):
    InitializeNova | IngestNewNova | RefreshReferences | DiscoverSpectraProducts
    AcquireAndValidateSpectra | IngestPhotometry | NameCheckAndReconcile

class JobStatus(Enum): QUEUED | RUNNING | SUCCEEDED | FAILED | QUARANTINED | CANCELLED
class AttemptStatus(Enum): STARTED | SUCCEEDED | FAILED | TIMED_OUT | CANCELLED

class JobRun(PersistentBase):
    job_run_id: UUID = uuid4()
    job_type: JobType
    workflow_name: str
    status: JobStatus = QUEUED
    execution_arn: str | None
    correlation_id: UUID           # required; correlates across events/services
    idempotency_key: str           # internal only; min 8, max 256
    initiated_at: datetime
    finished_at: datetime | None   # must not be < initiated_at
    nova_id: UUID | None
    data_product_id: UUID | None
    initiated_by: str | None
    attributes: dict = {}

class Attempt(PersistentBase):
    attempt_id: UUID = uuid4()
    job_run_id: UUID
    attempt_number: int            # ge=1
    status: AttemptStatus = STARTED
    started_at: datetime
    finished_at: datetime | None   # must not be < started_at
    error_code: str | None
    error_message: str | None      # max 4000
    request_id: str | None         # AWS Lambda request id
    execution_arn: str | None
```

---

## 2. EVENTS (`contracts/models/events.py`)

```python
class EventBase(BaseModel):
    # extra="forbid"
    # Boundary events MUST NOT include idempotency keys or step dedupe keys.
    event_version: str = "1.0.0"
    correlation_id: UUID = uuid4()    # generate if caller omits; propagate downstream
    initiated_at: datetime             # when caller originated request; must be tz-aware

# All event classes inherit EventBase, set event_version: Literal["1.0.0"],
# and set job_type: Literal[JobType.<value>].
# Behavioral knobs use attributes: dict[str, Any] = {} rather than typed fields.
```

| Event | Required fields | Notes |
|---|---|---|
| `InitializeNovaEvent` | `candidate_name` | `source` optional |
| `IngestNewNovaEvent` | `nova_id` | behavioral knobs in `attributes` (e.g. `force_refresh`) |
| `RefreshReferencesEvent` | `nova_id` | `attributes.ads_name_hints: list[str]` for extra ADS aliases |
| `DiscoverSpectraProductsEvent` | `nova_id` | `attributes.sources: list[str]` for provider filter |
| `AcquireAndValidateSpectraEvent` | `nova_id`, `provider`, `data_product_id` | `provider` included because DDB key is `PRODUCT#SPECTRA#<provider>#<data_product_id>` — avoids extra read |
| `IngestPhotometryEvent` | `candidate_name` OR `nova_id` (one required) | `photometry_schema_version` optional; no `dataset_id` |
| `NameCheckAndReconcileEvent` | `nova_id` | proposed name hints in `attributes` |

---

## 3. ADRs (Decisions Only)

### ADR-001: Contract Governance (Accepted)
- **Pydantic models are source of truth** for all contracts (entities + events).
- **JSON Schemas are generated from models** and committed under `/schemas`; treated as stable, reviewable artifacts.
- **Versioning**: `schema_version` on every entity; `event_version` on every event. Breaking = removing/renaming fields, changing types, tightening validation. Non-breaking = adding optional fields, expanding enums (if consumers tolerate unknowns).
- **CI enforces**: schema regeneration from models; no drift between generated and committed schemas; example fixtures validate correctly.
- **Validation policy**: all boundary inputs (events) validated at Lambda/SFN task start; all persistent entities validated before write.
- Storage modeling (DDB keys, S3, GSIs) is explicitly out of scope for contracts.

---

### ADR-002: Workflow Orchestration (Proposed; superseded in part by ADR-004 and ADR-005)
> `dataset_id` references are obsolete. `reference_id` references are obsolete; now bibcodes.

- **AWS Step Functions** as orchestration backbone.
- **Modular workflow strategy**: each business capability is a distinct workflow with narrow versioned event contracts.
- **UUID-first execution rule**: workflows consume/emit stable UUIDs. Names permitted only at NameCheckAndReconcile boundary.
- **Idempotency**: workflow-level and step-level keys for exactly-once logical effects under at-least-once execution.
- **Failure classification**: RETRYABLE | TERMINAL | QUARANTINE with bounded retries.
- **Observability**: JobRun + Attempt records + structured logs with `correlation_id` propagation.
- `ComputeDiscoveryDate` lives in `RefreshReferences` (post-reconciliation).
- Provider-specific normalization is contained within `DiscoverSpectraProducts`.

---

### ADR-004: Architecture Baseline (Accepted; superseded in part by ADR-005 re: bibcodes)
> `dataset_id` and `reference_id` references are obsolete.

**Authoritative source hierarchy** (in order):
1. DynamoDB item model
2. Workflow specifications
3. Contracts (Pydantic models in `contracts/models/`)
4. Generated schemas

**Key architectural commitments:**
- UUID-first identity: `nova_id`, `data_product_id`
- No Dataset abstraction; `DataProduct` is the atomic scientific unit
- Spectra workflows operate on one `data_product_id` per execution
- Photometry table is canonical and overwritten in place
- Photometry snapshots occur only on schema version changes
- Idempotency keys are internal-only; never in boundary schemas
- Any documentation referencing `dataset_id` is stale

---

### ADR-005: Reference Model and ADS Integration (Accepted, 2026-03-03)

**Five decisions:**

1. **ADS belongs exclusively to `reference_manager`**. `archive_resolver` is scoped to nova identity resolution (SIMBAD + TNS) only. Never route ADS through `archive_resolver`.

2. **ADS query strategy**: parallel name query (all known aliases, OR'd) + coordinate cone search (10" radius); merge by bibcode dedup. *(Note: Amendment to this in practice — coordinate search abandoned; see refresh_references.md which uses name-only query. ADS positional search not reliably documented.)*

3. **`Reference` is a global DDB entity**: `PK=REFERENCE#<bibcode>` / `SK=METADATA`. Nova↔Reference link is `NOVAREF` (nova-scoped). One canonical `Reference`, many `NOVAREF` links. No internal UUID on `Reference` or `NovaReference`.

4. **Discovery date rule v1.0**: `discovery_date` = `publication_date` of the Reference with the earliest `publication_date` among all ADS results for that nova. Tiebreaker: lexicographically smallest bibcode. `UpsertDiscoveryDateMetadata` enforces **monotonically earlier** invariant (only overwrites with strictly earlier value). Rule version stored in idempotency key: `DiscoveryDate:{nova_id}:{earliest_bibcode}:1.0`.

5. **Deferred**: non-ADS sources out of scope; donated data provenance belongs on `DataProduct.provenance`. `NovaReferenceRole` promotion (e.g. to DISCOVERY) deferred to future workflow; `refresh_references` assigns `OTHER` by default.

---

### ADR-005 Amendment: Discovery Date Precision (Accepted, 2026-03-03)

**Decision**: Discovery dates stored as `str` in `YYYY-MM-DD`. Day `00` = month-only precision.

- Applies to: `Nova.discovery_date` and `Reference.publication_date`
- `date` type rejected because Python's `date` rejects day 0
- `00` day is unambiguous signal; `01` would be a data integrity error (indistinguishable from "actual first of month")
- ADS `pubdate` is already `YYYY-MM-00` format → zero transformation needed
- Lexicographic comparison on `YYYY-MM-DD` strings is correct for monotonicity checks
- Validator `_DISCOVERY_DATE_RE`: month `01-12`; day `00-31` (upper bound logical range only; semantic validity not enforced at model layer)
