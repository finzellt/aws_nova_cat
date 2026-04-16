# contracts/models/outputs.py
"""
Output contracts for Nova Cat workflows.

This module mirrors events.py: where events.py models what goes *in* to a
workflow boundary, this module models what comes *out*.

Two levels are modeled for each workflow:

  1. Task-level outputs — the dict a Lambda returns, which Step Functions
     writes into $ at the ResultPath declared in the ASL. One model per task.

  2. Workflow-level terminal outputs — the full accumulated shape of $ at
     each "End": true state. This is the execution output the tests assert
     against.

── Annotation convention ────────────────────────────────────────────────────
  "# from handler" — derived directly from the return statement in the
     Lambda handler file. Ground truth; will not drift unless the handler
     is changed.
  "# inferred from ASL" — derived from Parameter references in the ASL
     for tasks whose handler is not yet in scope. Should be promoted to
     "# from handler" when that handler becomes available.
─────────────────────────────────────────────────────────────────────────────

── Model config note ────────────────────────────────────────────────────────
  All output models use extra="ignore". Step Functions execution output is
  the full accumulated $ object, which also contains the original input
  fields (candidate_name, source, etc.) that are not part of the output
  contract. extra="ignore" lets model_validate() succeed when given the raw
  execution output dict without needing to strip input fields first.
─────────────────────────────────────────────────────────────────────────────

── $ Accumulation Reference (initialize_nova) ───────────────────────────────
  Key              | ResultPath          | EXISTS_AND_LAUNCHED | NOT_FOUND | CREATED_AND_LAUNCHED | FAILED
  -----------------|---------------------|---------------------|-----------|----------------------|-------
  (input fields)   | (original input)    | ✓                   | ✓         | ✓                    | ✓
  $.job_run        | BeginJobRun         | ✓                   | ✓         | ✓                    | ✓
  $.normalization  | NormalizeCandidName | ✓                   | ✓         | ✓                    | ✓
  $.name_check     | CheckByName         | ✓                   | ✓         | ✓                    | ✓
  $.idempotency    | AcquireIdemLock     | –                   | ✓         | ✓                    | ✓ *
  $.resolution     | ResolveVsArchives   | –                   | ✓         | ✓                    | ✓ *
  $.nova_creation  | CreateNovaId        | –                   | –         | ✓                    | ✓ *
  $.upsert         | UpsertMinMetadata   | –                   | –         | ✓                    | ✓ *
  $.launch         | PublishIngestNova   | ✓                   | –         | ✓                    | –
  $.error          | (Catch ResultPath)  | –                   | –         | –                    | ✓
  $.terminal_fail  | TerminalFailHandler | –                   | –         | –                    | ✓
  $.finalize       | FinalizeJobRun*     | ✓                   | ✓         | ✓                    | ✓

  * On FAILED paths, keys marked ✓* are present only if the failure occurred
    AFTER that task ran.

  NOTE: The coordinate-check branch (CheckExistingNovaByCoordinates,
  UpsertAliasForExistingNova, QuarantineHandler) is specified in
  initialize-nova.md and the nova_resolver handler has the implementation,
  but these states are NOT yet present in the current ASL
  (initialize_nova.asl.json). Models for those tasks are included below;
  the terminal output model marks them clearly as not-yet-active.
─────────────────────────────────────────────────────────────────────────────

── $ Accumulation Reference (ingest_new_nova) ───────────────────────────────
  Key           | ResultPath         | LAUNCHED | FAILED
  --------------|--------------------|----------|-------
  (input fields)| (original input)   | ✓        | ✓
  $.job_run     | BeginJobRun        | ✓        | ✓
  $.idempotency | AcquireIdemLock    | ✓        | ✓ *
  $.downstream  | LaunchDownstream   | ✓        | –
  $.error       | (Catch ResultPath) | –        | ✓
  $.terminal_fail| TerminalFail…     | –        | ✓
  $.finalize    | FinalizeJobRun*    | ✓        | ✓

  $.downstream is a list of two branch outputs from the Parallel state:
    [0] — LaunchRefreshReferences return value
    [1] — LaunchDiscoverSpectraProducts return value
─────────────────────────────────────────────────────────────────────────────

── $ Accumulation Reference (refresh_references) ────────────────────────────
  Key               | ResultPath              | SUCCEEDED | FAILED
  ------------------|-------------------------|-----------|-------
  (input fields)    | (original input)        | ✓         | ✓
  $.job_run         | BeginJobRun             | ✓         | ✓
  $.idempotency     | AcquireIdempotencyLock  | ✓         | ✓ *
  $.fetch           | FetchReferenceCandidates| ✓         | ✓ *
  $.reconcile       | ReconcileReferences Map | ✓         | ✓ *
  $.discovery       | ComputeDiscoveryDate    | ✓         | ✓ *
  $.discovery_upsert| UpsertDiscoveryDate…   | ✓         | ✓ *
  $.error           | (Catch ResultPath)      | –         | ✓
  $.terminal_fail   | TerminalFailHandler     | –         | ✓
  $.finalize        | FinalizeJobRun*         | ✓         | ✓

  $.reconcile is a list (one element per ADS candidate doc). Each successful
  iteration ends at LinkNovaReference (no ResultPath — result replaces $),
  so each element is a LinkNovaReferenceOutput-shaped dict. Failed iterations
  route to ItemFailureHandler (ResultPath: $.quarantine), so each failed
  element contains the accumulated state at point of error plus $.quarantine.
─────────────────────────────────────────────────────────────────────────────

── $ Accumulation Reference (discover_spectra_products) ─────────────────────
  Key                 | ResultPath               | COMPLETED | FAILED
  --------------------|--------------------------|-----------|-------
  (input fields)      | (original input)         | ✓         | ✓
  $.job_run           | BeginJobRun              | ✓         | ✓
  $.idempotency       | AcquireIdempotencyLock   | ✓         | ✓ *
  $.providers         | PrepareProviderList      | ✓         | ✓ *
  $.discovery_results | DiscoverAcrossProviders  | ✓         | ✓ *
  $.error             | (Catch ResultPath)       | –         | ✓
  $.terminal_fail     | TerminalFailHandler      | –         | ✓
  $.finalize          | FinalizeJobRun*          | ✓         | ✓

  $.providers is a Pass state result: [{"provider": "ESO"}] (hardcoded MVP list).

  $.discovery_results is a list (one element per provider). Each element is
  the accumulated $ of the completed provider iteration, containing:
    provider, nova_id, correlation_id, job_run (from ItemSelector)
    query_result   → QueryProviderForProductsOutput
    normalize_result → NormalizeProviderProductsOutput
    dedup_result   → DeduplicateAndAssignDataProductIdsOutput
    persist_result → PersistDataProductMetadataOutput
    publish_result → PublishAcquireAndValidateSpectraRequests return value

  ToleratedFailurePercentage=100: a failed provider iteration does not abort
  the Map. Failed iterations are included in $.discovery_results as error dicts.
─────────────────────────────────────────────────────────────────────────────

── $ Accumulation Reference (acquire_and_validate_spectra) ──────────────────
  Key             | ResultPath             | COMPLETED | FAILED
  ----------------|------------------------|-----------|-------
  (input fields)  | (original input)       | ✓         | ✓
  $.job_run       | BeginJobRun            | ✓         | ✓
  $.idempotency   | AcquireIdempotencyLock | ✓         | ✓ *
  $.status        | CheckOperationalStatus | ✓         | ✓ *
  $.skip_reason   | AlreadyValidated /     | ✓ (skip)  | –
                  |   QuarantineBlocked /  |           |
                  |   CooldownActive       |           |
  $.acquisition   | AcquireArtifact        | ✓ (acq)   | ✓ *
  $.validation    | ValidateBytes          | ✓ (acq)   | ✓ *
  $.record_result | RecordValidationResult | ✓ (acq)   | ✓ *
                  |   / RecordDuplicate…   |           |
  $.error         | (Catch ResultPath)     | –         | ✓
  $.terminal_fail | TerminalFailHandler    | –         | ✓
  $.finalize      | FinalizeJobRun*        | ✓         | ✓

  All success paths use outcome="COMPLETED". The $.validation.validation_outcome
  field (VALID | QUARANTINED | TERMINAL_INVALID) and $.skip_reason.outcome
  (SKIPPED_ALREADY_VALIDATED | SKIPPED_QUARANTINE_BLOCKED | SKIPPED_COOLDOWN_ACTIVE)
  carry path-specific detail within the single COMPLETED outcome.

  NOTE: There is NO QuarantineHandler Lambda call in this workflow. QUARANTINED
  products are handled by RecordValidationResult writing validation_status=QUARANTINED
  directly onto the DataProduct, then routing to FinalizeJobRunSuccess.
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Shared model config
# ---------------------------------------------------------------------------

_OUTPUT_CONFIG = ConfigDict(extra="ignore")


# ===========================================================================
# 1. TASK-LEVEL OUTPUT MODELS
#
# Each model documents what a single Lambda task returns. Step Functions
# writes this return value into $ at the ResultPath declared in the ASL.
# The ResultPath for each task is noted in the class docstring.
# ===========================================================================


# ---------------------------------------------------------------------------
# job_run_manager tasks
# Source: services/job_run_manager/handler.py
# ---------------------------------------------------------------------------


class BeginJobRunOutput(BaseModel):
    """
    Returned by the BeginJobRun task.
    ResultPath: $.job_run

    Source: services/job_run_manager/handler.py :: _begin_job_run()
    """

    model_config = _OUTPUT_CONFIG

    job_run_id: str = Field(description="UUID for this workflow execution.")  # from handler
    correlation_id: str = Field(
        description="Caller-supplied or freshly generated UUID."
    )  # from handler
    started_at: str = Field(
        description="ISO-8601 UTC timestamp when the JobRun was started."
    )  # from handler
    pk: str = Field(description="DynamoDB PK: WORKFLOW#<correlation_id>.")  # from handler
    sk: str = Field(
        description="DynamoDB SK: JOBRUN#<workflow_name>#<started_at>#<job_run_id>."
    )  # from handler


class FinalizeJobRunSuccessOutput(BaseModel):
    """
    Returned by any FinalizeJobRunSuccess* task.
    ResultPath: $.finalize

    The outcome value is workflow-specific. See per-workflow output models
    below for constrained Literal types on each workflow's outcome field.

    Source: services/job_run_manager/handler.py :: _finalize_job_run_success()
    """

    model_config = _OUTPUT_CONFIG

    outcome: str = Field(
        description="Terminal outcome string. Constrained per workflow — see workflow output models."
    )  # from handler
    ended_at: str = Field(
        description="ISO-8601 UTC timestamp when the JobRun was finalized."
    )  # from handler


class FinalizeJobRunFailedOutput(BaseModel):
    """
    Returned by the FinalizeJobRunFailed task.
    ResultPath: $.finalize

    Source: services/job_run_manager/handler.py :: _finalize_job_run_failed()
    """

    model_config = _OUTPUT_CONFIG

    status: Literal["FAILED"]  # from handler
    ended_at: str  # from handler


class FinalizeJobRunQuarantinedOutput(BaseModel):
    """
    Returned by the FinalizeJobRunQuarantined task.
    ResultPath: $.finalize

    Source: services/job_run_manager/handler.py :: _finalize_job_run_quarantined()
    """

    model_config = _OUTPUT_CONFIG

    status: Literal["QUARANTINED"]  # from handler
    ended_at: str  # from handler


class TerminalFailHandlerOutput(BaseModel):
    """
    Returned by the TerminalFailHandler task.
    ResultPath: $.terminal_fail

    This is a pre-classification step that runs before FinalizeJobRunFailed.
    It persists error_classification and error_fingerprint onto the JobRun
    record so that diagnostic context is available before the final FAILED
    status is written.

    Source: services/job_run_manager/handler.py :: _terminal_fail_handler()
    """

    model_config = _OUTPUT_CONFIG

    error_classification: Literal["RETRYABLE", "TERMINAL"]  # from handler
    error_fingerprint: str = Field(
        description=(
            "12-char hex SHA-256 digest of (error_type + job_run_id + cause[:100]). "
            "Stable across retries of the same logical failure. "
            "Cross-referenceable with CloudWatch logs."
        )
    )  # from handler


# ---------------------------------------------------------------------------
# nova_resolver tasks
# Source: services/nova_resolver/handler.py
# ---------------------------------------------------------------------------


class NormalizeCandidateNameOutput(BaseModel):
    """
    Returned by the NormalizeCandidateName task.
    ResultPath: $.normalization

    Source: services/nova_resolver/handler.py :: _normalize_candidate_name()
    """

    model_config = _OUTPUT_CONFIG

    normalized_candidate_name: str  # from handler


class CheckExistingNovaByNameOutput(BaseModel):
    """
    Returned by the CheckExistingNovaByName task.
    ResultPath: $.name_check

    Source: services/nova_resolver/handler.py :: _check_existing_nova_by_name()
    """

    model_config = _OUTPUT_CONFIG

    exists: bool  # from handler
    nova_id: str | None = Field(
        default=None,
        description="UUID of the matched nova. Present only when exists=True.",
    )  # from handler


class CheckExistingNovaByCoordinatesOutput(BaseModel):
    """
    Returned by the CheckExistingNovaByCoordinates task.
    ResultPath: $.coordinate_check

    Thresholds (from initialize-nova.md):
      < 2"   → DUPLICATE  (upsert alias, launch ingest_new_nova)
      2"–10" → AMBIGUOUS  (quarantine — human review required)
      > 10"  → NONE       (distinct object — proceed to CreateNovaId)

    NOTE: This task is implemented in nova_resolver/handler.py but the
    corresponding ASL state is NOT yet present in initialize_nova.asl.json.

    Source: services/nova_resolver/handler.py :: _check_existing_nova_by_coordinates()
    """

    model_config = _OUTPUT_CONFIG

    match_outcome: Literal["DUPLICATE", "AMBIGUOUS", "NONE"]  # from handler
    min_sep_arcsec: float | None = Field(
        description=(
            "Minimum angular separation in arcseconds. "
            "None when no novae with coordinates exist in the database."
        )
    )  # from handler
    matched_nova_id: str | None = Field(
        default=None,
        description="nova_id of the closest match. Present only when match_outcome=DUPLICATE.",
    )  # from handler


class CreateNovaIdOutput(BaseModel):
    """
    Returned by the CreateNovaId task.
    ResultPath: $.nova_creation

    Source: services/nova_resolver/handler.py :: _create_nova_id()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str = Field(description="Freshly minted UUID for the new nova.")  # from handler


class UpsertMinimalNovaMetadataOutput(BaseModel):
    """
    Returned by the UpsertMinimalNovaMetadata task.
    ResultPath: $.upsert

    Source: services/nova_resolver/handler.py :: _upsert_minimal_nova_metadata()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str  # from handler


class UpsertAliasForExistingNovaOutput(BaseModel):
    """
    Returned by the UpsertAliasForExistingNova task.
    ResultPath: $.upsert_alias

    NOTE: This task is implemented in nova_resolver/handler.py but the
    corresponding ASL state is NOT yet present in initialize_nova.asl.json.

    Source: services/nova_resolver/handler.py :: _upsert_alias_for_existing_nova()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str = Field(description="The existing nova_id the alias was added to.")  # from handler


# ---------------------------------------------------------------------------
# idempotency_guard tasks
# Source: services/idempotency_guard/handler.py
# ---------------------------------------------------------------------------


class AcquireIdempotencyLockOutput(BaseModel):
    """
    Returned by the AcquireIdempotencyLock task.
    ResultPath: $.idempotency  (all workflows that use this task)

    Acquires a workflow-level idempotency lock via conditional DynamoDB put.
    Raises RetryableError if the lock is already held — Step Functions retries
    with backoff. The lock TTL is 15 minutes; stale locks can be manually deleted
    (see idempotency_guard/handler.py module docstring for CLI command).

    Idempotency key format:
      {workflow_name}:{primary_id}:{schema_version}:{time_bucket}
      where time_bucket = YYYY-MM-DDTHH (1-hour granularity)

    Source: services/idempotency_guard/handler.py :: _acquire_idempotency_lock()
    """

    model_config = _OUTPUT_CONFIG

    idempotency_key: str = Field(
        description=(
            "Full computed key: '{workflow_name}:{primary_id}:1:{YYYY-MM-DDTHH}'. "
            "Internal — for logging and manual lock management only."
        )
    )  # from handler
    acquired_at: str = Field(
        description="ISO-8601 UTC timestamp when the lock was acquired."
    )  # from handler


# ---------------------------------------------------------------------------
# archive_resolver tasks
# Source: services/archive_resolver/handler.py
# ---------------------------------------------------------------------------


class ResolveCandidateAgainstPublicArchivesOutput(BaseModel):
    """
    Returned by the ResolveCandidateAgainstPublicArchives task.
    ResultPath: $.resolution

    NOTE: The handler also returns an `is_classical_nova` field from a
    former CandidateIsClassicalNova? choice state that has since been
    removed. That field is intentionally omitted here; extra="ignore"
    silently drops it.

    Source: services/archive_resolver/handler.py ::
            _resolve_candidate_against_public_archives()
    """

    model_config = _OUTPUT_CONFIG

    is_nova: bool  # from handler
    resolver_source: Literal["SIMBAD", "TNS", "SIMBAD+TNS", "NONE"]  # from handler
    resolved_ra: float | None = Field(default=None)  # from handler
    resolved_dec: float | None = Field(default=None)  # from handler
    resolved_epoch: str | None = Field(
        default=None,
        description="Coordinate epoch label. Always 'J2000' for SIMBAD results.",
    )  # from handler
    aliases: list[str] = Field(
        default_factory=list,
        description=(
            "Raw alias strings from the SIMBAD ids field. "
            "Empty when resolver_source is TNS or NONE."
        ),
    )  # from handler


# ---------------------------------------------------------------------------
# quarantine_handler tasks
# Source: services/quarantine_handler/handler.py
# ---------------------------------------------------------------------------


class QuarantineHandlerOutput(BaseModel):
    """
    Returned by the QuarantineHandler task.
    ResultPath: $.quarantine  (initialize_nova);
                $.quarantine  (refresh_references Map item-level ItemFailureHandler)

    Updates (does not create) the existing JobRun record with quarantine
    diagnostic fields, then publishes a best-effort SNS notification.
    SNS failure is swallowed — it must not cause the workflow to fail.

    Used by:
      - initialize_nova: QuarantineHandler state (NOT yet in ASL)
      - refresh_references: ItemFailureHandler state within ReconcileReferences Map
    NOT used by:
      - acquire_and_validate_spectra: QUARANTINED products are handled by
        RecordValidationResult writing validation_status=QUARANTINED directly.

    Source: services/quarantine_handler/handler.py :: _quarantine_handler()
    """

    model_config = _OUTPUT_CONFIG

    quarantine_reason_code: str = Field(
        description=(
            "Echoed from event. "
            "See NovaQuarantineReasonCode / SpectraQuarantineReasonCode in entities.py."
        )
    )  # from handler
    error_fingerprint: str = Field(
        description=(
            "12-char hex SHA-256 digest of "
            "(quarantine_reason_code + workflow_name + primary_id). "
            "Stable across retries. Cross-referenceable with CloudWatch logs "
            "and SNS notification payload."
        )
    )  # from handler
    quarantined_at: str = Field(description="ISO-8601 UTC timestamp.")  # from handler


# ---------------------------------------------------------------------------
# workflow_launcher tasks
# Source: services/workflow_launcher/handler.py
# ---------------------------------------------------------------------------


class PublishIngestNewNovaOutput(BaseModel):
    """
    Returned by the PublishIngestNewNova task (both _Exists and _Created variants).
    ResultPath: $.launch  (initialize_nova)

    Source: services/workflow_launcher/handler.py :: _publish_ingest_new_nova()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str  # from handler
    execution_name: str = Field(
        description="SFN execution name: '<nova_id>-<job_run_id[:8]>'."
    )  # from handler
    execution_arn: str | None = Field(
        default=None,
        description="SFN execution ARN. Absent when already_existed=True.",
    )  # from handler
    already_existed: bool = Field(
        default=False,
        description=(
            "True when ExecutionAlreadyExists was returned by SFN — "
            "idempotent success; execution is already running."
        ),
    )  # from handler


class LaunchWorkflowOutput(BaseModel):
    """
    Returned by the LaunchRefreshReferences and LaunchDiscoverSpectraProducts
    tasks in ingest_new_nova.
    ResultPath: none (Parallel branch terminal state — branch output IS the return value)

    These tasks start child workflow executions non-blocking via SFN StartExecution.
    Return shape matches the _start_execution() helper used by all launch tasks.

    Source: services/workflow_launcher/handler.py :: _launch_refresh_references()
            / _launch_discover_spectra_products()
    """

    model_config = _OUTPUT_CONFIG
    nova_id: str  # from handler
    execution_name: str  # from handler
    execution_arn: str | None = None  # from handler
    already_existed: bool = False  # from handler


# ---------------------------------------------------------------------------
# reference_manager tasks
# Source: services/reference_manager/handler.py
# Workflow: refresh_references
# ---------------------------------------------------------------------------


class FetchReferenceCandidatesOutput(BaseModel):
    """
    Returned by the FetchReferenceCandidates task.
    ResultPath: $.fetch

    Loads nova aliases from DynamoDB, queries ADS via OR-joined name query,
    and returns the raw candidate docs. The candidates list is the ItemsPath
    for the downstream ReconcileReferences Map state ($.fetch.candidates).

    Source: services/reference_manager/handler.py :: _handle_fetchReferenceCandidates()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str  # from handler
    candidates: list[dict[str, Any]] = Field(
        description=(
            "Raw ADS doc list. Each doc has all ADS_FIELDS present "
            "(absent fields are None). "
            "Feeds into ReconcileReferences Map as ItemsPath ($.fetch.candidates)."
        )
    )  # from handler
    candidate_count: int = Field(description="len(candidates).")  # from handler


class NormalizeReferenceOutput(BaseModel):
    """
    Returned by the NormalizeReference task.
    ResultPath: none (result replaces $ within the Map iteration).

    Called once per ADS doc in the ReconcileReferences Map state. Maps a
    raw ADS doc to the Reference schema. nova_id is injected alongside the
    Map item via ASL ItemSelector.

    Because NormalizeReference has no ResultPath in the ASL, its return value
    REPLACES the iteration $ entirely. The UpsertReferenceEntity task
    receives this dict as its full input.

    ADS date normalization: ADS returns YYYY-MM-01T00:00:00Z when only month
    precision is available. Day is always discarded; stored as YYYY-MM-00.

    Source: services/reference_manager/handler.py :: _handle_normalizeReference()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str | None  # from handler (pass-through from ItemSelector)
    bibcode: str  # from handler
    reference_type: str = Field(
        description=(
            "Mapped from ADS doctype. One of: journal_article, arxiv_preprint, "
            "conference_abstract, cbat_circular, atel, catalog, software, other."
        )
    )  # from handler
    title: str | None = None  # from handler (first element of ADS title list)
    year: int | None = None  # from handler (extracted from publication_date)
    publication_date: str | None = Field(
        default=None,
        description="YYYY-MM-00 format. Day is always 00 (not meaningful in ADS).",
    )  # from handler
    authors: list[str] = Field(default_factory=list)  # from handler
    doi: str | None = None  # from handler (first element if ADS returns a list)
    arxiv_id: str | None = None  # from handler (bare ID with 'arXiv:' prefix stripped)


class UpsertReferenceEntityOutput(BaseModel):
    """
    Returned by the UpsertReferenceEntity task.
    ResultPath: none (result replaces $ within the Map iteration).

    Writes or updates the global Reference entity at PK=REFERENCE#<bibcode>,
    SK=METADATA. Preserves created_at from the existing item when updating.

    Because UpsertReferenceEntity has no ResultPath in the ASL, its return
    value REPLACES the iteration $ entirely. LinkNovaReference receives this
    dict as its full input.

    Source: services/reference_manager/handler.py :: _handle_upsertReferenceEntity()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str | None  # from handler (pass-through)
    bibcode: str  # from handler
    publication_date: str | None = None  # from handler (pass-through for LinkNovaReference)


class LinkNovaReferenceOutput(BaseModel):
    """
    Returned by the LinkNovaReference task.
    ResultPath: none (result replaces $ and is the Map iteration terminal output).

    Creates the NOVAREF link at PK=<nova_id>, SK=NOVAREF#<bibcode>.
    Idempotent: ConditionalCheckFailedException → link already exists → no-op.

    Because LinkNovaReference has no ResultPath in the ASL and is End:true,
    its return value becomes the element written into $.reconcile for this
    iteration (on the success path).

    Source: services/reference_manager/handler.py :: _handle_linkNovaReference()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str  # from handler
    bibcode: str  # from handler
    publication_date: str | None = None  # from handler
    linked: Literal[True]  # from handler (always True — failures raise, not return)


class FetchAndReconcileReferencesOutput(BaseModel):
    """
    Returned by the FetchAndReconcileReferences combined task.
    ResultPath: $.reconcile_summary

    Replaces the FetchReferenceCandidates → ReconcileReferences Map pattern.
    Fetches ADS candidates and reconciles each one internally (normalize →
    upsert → link). Per-item failures are quarantined inside the Lambda.
    The full candidate list never transits through SFn state.

    Source: services/reference_manager/handler.py :: _handle_fetchAndReconcileReferences()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str  # from handler
    total_candidates: int = Field(
        description="Total number of ADS candidate docs returned by the query."
    )  # from handler
    reconciled: int = Field(
        description="Number of candidates successfully reconciled (normalize + upsert + link)."
    )  # from handler
    quarantined: int = Field(
        description="Number of candidates that failed and were quarantined."
    )  # from handler
    quarantined_bibcodes: list[str] = Field(
        default_factory=list,
        description=(
            "Bibcodes of quarantined candidates. 'unknown' for candidates missing a bibcode."
        ),
    )  # from handler


class ComputeDiscoveryDateOutput(BaseModel):
    """
    Returned by the ComputeDiscoveryDate task.
    ResultPath: $.discovery

    Queries all NOVAREF links for the nova, batch-fetches Reference
    publication_dates, and returns the earliest.

    Tiebreaker: lexicographically smallest bibcode. Comparison uses month
    granularity only (YYYY, MM); the day component is ignored because
    day-00 signals unknown precision (see _date_sort_key in handler).

    Source: services/reference_manager/handler.py :: _handle_computeDiscoveryDate()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str  # from handler
    earliest_bibcode: str | None = Field(
        default=None,
        description=(
            "Bibcode of the reference with the earliest publication_date. "
            "None if no dated references exist."
        ),
    )  # from handler
    earliest_publication_date: str | None = Field(
        default=None,
        description="YYYY-MM-00 format. None if no dated references exist.",
    )  # from handler


class UpsertDiscoveryDateMetadataOutput(BaseModel):
    """
    Returned by the UpsertDiscoveryDateMetadata task.
    ResultPath: $.discovery_upsert

    Updates Nova.discovery_date only if the new date is strictly earlier than
    the current value (monotonically earlier invariant, ADR-005 §4).
    No-op when: no date was computed, or Nova already has an equal/earlier date.

    Source: services/reference_manager/handler.py :: _handle_upsertDiscoveryDateMetadata()
    """

    model_config = _OUTPUT_CONFIG

    nova_id: str  # from handler
    updated: bool  # from handler
    discovery_date: str | None = Field(
        default=None,
        description=(
            "The current (post-update) discovery_date. None when no dated references exist."
        ),
    )  # from handler
    discovery_date_old: str | None = Field(
        default=None,
        description="The previous discovery_date. Present only when updated=True.",
    )  # from handler


# ---------------------------------------------------------------------------
# spectra_discoverer tasks
# Source: services/spectra_discoverer/handler.py
# Workflow: discover_spectra_products
# ---------------------------------------------------------------------------


class QueryProviderForProductsOutput(BaseModel):
    """
    Returned by the QueryProviderForProducts task.
    ResultPath: $.query_result  (within the DiscoverAcrossProviders Map iteration)

    Fetches nova coordinates from DynamoDB, then delegates to the provider
    adapter (e.g. ESOAdapter) to execute a cone search.

    Source: services/spectra_discoverer/handler.py :: _handle_query_provider_for_products()
    """

    model_config = _OUTPUT_CONFIG

    raw_products: list[dict[str, Any]] = Field(
        description=(
            "Provider-native record dicts. JSON-safe (numpy scalars sanitized, nan/inf → None)."
        )
    )  # from handler


class NormalizeProviderProductsOutput(BaseModel):
    """
    Returned by the NormalizeProviderProducts task.
    ResultPath: $.normalize_result  (within the DiscoverAcrossProviders Map iteration)

    Delegates normalization to the provider adapter. Malformed records
    (adapter.normalize returns None) are silently dropped.

    Source: services/spectra_discoverer/handler.py :: _handle_normalize_provider_products()
    """

    model_config = _OUTPUT_CONFIG

    normalized_products: list[dict[str, Any]] = Field(
        description=(
            "Normalized product dicts conforming to SpectraDiscoveryAdapter shape. "
            "Required fields per product: provider, nova_id, locator_identity, "
            "identity_strategy, locators. Optional: provider_product_key, hints."
        )
    )  # from handler


class DeduplicateAndAssignDataProductIdsOutput(BaseModel):
    """
    Returned by the DeduplicateAndAssignDataProductIds task.
    ResultPath: $.dedup_result  (within the DiscoverAcrossProviders Map iteration)

    For each normalized product: checks LocatorAlias for an existing
    data_product_id; if absent, derives a deterministic UUID via uuid5
    (NATIVE_ID or METADATA_KEY) or uuid4 (WEAK). Sets skip_acquisition=True
    for products already marked VALID.

    Source: services/spectra_discoverer/handler.py ::
            _handle_deduplicate_and_assign_data_product_ids()
    """

    model_config = _OUTPUT_CONFIG

    products_with_ids: list[dict[str, Any]] = Field(
        description=(
            "Normalized products enriched with: "
            "data_product_id (str), is_new (bool), skip_acquisition (bool)."
        )
    )  # from handler


class PersistDataProductMetadataOutput(BaseModel):
    """
    Returned by the PersistDataProductMetadata task.
    ResultPath: $.persist_result  (within the DiscoverAcrossProviders Map iteration)

    For each product: writes LocatorAlias (conditional put; first writer wins)
    and inserts a DataProduct stub for new products. Already-VALID products
    are skipped entirely. Stub initial state: acquisition_status=STUB,
    validation_status=UNVALIDATED, eligibility=ACQUIRE, attempt_count=0.

    Source: services/spectra_discoverer/handler.py ::
            _handle_persist_data_product_metadata()
    """

    model_config = _OUTPUT_CONFIG

    persisted_products: list[dict[str, Any]] = Field(
        description=(
            "Newly stubbed products eligible for acquisition. "
            "Each dict has: data_product_id, provider, nova_id. "
            "Fed to PublishAcquireAndValidateSpectraRequests."
        )
    )  # from handler


# ---------------------------------------------------------------------------
# spectra_validator tasks
# Source: services/spectra_validator/handler.py
# Workflow: acquire_and_validate_spectra
# ---------------------------------------------------------------------------


class CheckOperationalStatusOutput(BaseModel):
    """
    Returned by the CheckOperationalStatus task.
    ResultPath: $.status

    Loads the DataProduct from DynamoDB and computes the three operational
    decision flags used by the CheckOperationalStatusOutcome Choice state.
    Also returns the full product record for downstream tasks (AcquireArtifact,
    ValidateBytes) so they avoid additional DynamoDB reads.

    Source: services/spectra_validator/handler.py ::
            _handle_check_operational_status()
    """

    model_config = _OUTPUT_CONFIG

    already_validated: bool = Field(
        description="True when validation_status == VALID. Routes to AlreadyValidated."
    )  # from handler
    cooldown_active: bool = Field(
        description="True when now < next_eligible_attempt_at. Routes to CooldownActive."
    )  # from handler
    is_quarantined: bool = Field(
        description=(
            "True when validation_status == QUARANTINED and no manual clearance. "
            "Clearance signals: CLEARED_RETRY_APPROVED or CLEARED_TERMINAL. "
            "Routes to QuarantineBlocked."
        )
    )  # from handler
    data_product: dict[str, Any] = Field(
        description="Full DynamoDB DataProduct item dict for downstream tasks."
    )  # from handler


# ---------------------------------------------------------------------------
# spectra_acquirer tasks
# Source: services/spectra_acquirer/handler.py
# Workflow: acquire_and_validate_spectra
# ---------------------------------------------------------------------------


class AcquireArtifactOutput(BaseModel):
    """
    Returned by the AcquireArtifact task on success.
    ResultPath: $.acquisition

    Downloads FITS bytes from the product's primary URL locator, computes
    SHA-256 in flight, and writes raw bytes to S3. Attempt tracking occurs
    at the START of the download (before bytes arrive) so Lambda timeouts
    count as attempts.

    On retryable or terminal failure the handler raises (no return value).
    The final lifecycle state (ACQUIRED, eligibility=NONE) is persisted by
    RecordValidationResult, not here.

    S3 key format: raw/{nova_id}/{provider}/{data_product_id}.fits

    Source: services/spectra_acquirer/handler.py :: _handle_acquire_artifact()
    """

    model_config = _OUTPUT_CONFIG

    raw_s3_bucket: str = Field(description="S3 bucket holding raw FITS bytes.")  # from handler
    raw_s3_key: str = Field(
        description="S3 key: raw/{nova_id}/{provider}/{data_product_id}.fits"
    )  # from handler
    sha256: str = Field(description="Hex-encoded SHA-256 digest of raw bytes.")  # from handler
    byte_length: int = Field(description="Total byte count of the downloaded file.")  # from handler
    etag: str = Field(
        description="S3 ETag (for integrity cross-reference). Stripped of quotes."
    )  # from handler


# ---------------------------------------------------------------------------
# spectra_validator tasks (continued)
# ---------------------------------------------------------------------------


class ValidateBytesOutput(BaseModel):
    """
    Returned by the ValidateBytes task.
    ResultPath: $.validation

    Reads raw FITS bytes from S3. Selects a FITS profile via the profile
    registry (first match wins). Delegates validation to the profile.
    Performs sha256 duplicate detection against existing VALID products in
    the nova partition after successful validation.

    The DuplicateByFingerprint? Choice state branches on $.validation.is_duplicate.
    All three validation_outcome values (VALID, QUARANTINED, TERMINAL_INVALID)
    route to RecordValidationResult on the non-duplicate path.

    Source: services/spectra_validator/handler.py :: _handle_validate_bytes()
    """

    model_config = _OUTPUT_CONFIG

    validation_outcome: Literal["VALID", "QUARANTINED", "TERMINAL_INVALID"]  # from handler
    is_duplicate: bool  # from handler
    duplicate_of_data_product_id: str | None = None  # from handler
    fits_profile_id: str | None = Field(
        default=None,
        description="Profile selected (e.g. 'ESO_UVES'). None when no profile matched.",
    )  # from handler
    profile_selection_inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Inputs used for profile selection: provider, instrume, telescop, origin.",
    )  # from handler
    header_signature_hash: str | None = Field(
        default=None,
        description=(
            "16-char hex SHA-256 digest of key discriminating header fields. "
            "For traceability only — not used for dedup."
        ),
    )  # from handler
    normalization_notes: list[str] = Field(
        default_factory=list,
        description="Human-readable notes from profile normalization. May be empty.",
    )  # from handler
    quarantine_reason: str | None = None  # from handler
    quarantine_reason_code: str | None = None  # from handler


class RecordValidationResultOutput(BaseModel):
    """
    Returned by the RecordValidationResult task.
    ResultPath: $.record_result

    Persists the final lifecycle state for a validated, quarantined, or
    terminal-invalid DataProduct. Clears eligibility=NONE and removes
    GSI1PK/GSI1SK atomically (product drops off EligibilityIndex).

    Outcome → DynamoDB state mapping:
      VALID            → validation_status=VALID, acquisition_status=ACQUIRED,
                         last_attempt_outcome=SUCCESS
      QUARANTINED      → validation_status=QUARANTINED, acquisition_status=ACQUIRED,
                         last_attempt_outcome=QUARANTINE
      TERMINAL_INVALID → validation_status=TERMINAL_INVALID, acquisition_status=ACQUIRED,
                         last_attempt_outcome=TERMINAL_FAILURE

    Source: services/spectra_validator/handler.py :: _handle_record_validation_result()
    """

    model_config = _OUTPUT_CONFIG

    persisted_outcome: Literal["VALID", "QUARANTINED", "TERMINAL_INVALID"]  # from handler


class RecordDuplicateLinkageOutput(BaseModel):
    """
    Returned by the RecordDuplicateLinkage task.
    ResultPath: $.record_result  (same key as RecordValidationResult — the ASL
    branches to one or the other; both write $.record_result)

    Marks the current DataProduct as a byte-level duplicate. Sets
    duplicate_of_data_product_id, acquisition_status=SKIPPED_DUPLICATE,
    eligibility=NONE. Removes GSI1PK/GSI1SK atomically.

    Note: validation_status is NOT set to VALID — the duplicate is not canonical.
    last_attempt_outcome=SUCCESS (acquisition itself succeeded).

    Source: services/spectra_validator/handler.py :: _handle_record_duplicate_linkage()
    """

    model_config = _OUTPUT_CONFIG

    canonical_data_product_id: str = Field(
        description=("Stable UUID of the canonical (VALID) product this duplicate was linked to.")
    )  # from handler


# ===========================================================================
# 2. WORKFLOW-LEVEL TERMINAL OUTPUT MODELS
#
# Each model represents the full shape of $ at an "End": true state.
# This is the object returned by Step Functions as execution output and
# what `out = _output(resp)` produces in the smoke tests.
# ===========================================================================


# ── initialize_nova ──────────────────────────────────────────────────────────


class InitializeNovaFinalizeOutput(FinalizeJobRunSuccessOutput):
    """
    Constrained finalize output for initialize_nova terminal success paths.
    Narrows FinalizeJobRunSuccessOutput.outcome to the three valid values.
    """

    outcome: Literal[  # type: ignore[assignment]
        "CREATED_AND_LAUNCHED",
        "EXISTS_AND_LAUNCHED",
        "NOT_FOUND",
    ]


class InitializeNovaTerminalOutput(BaseModel):
    """
    Full execution output shape for the initialize_nova workflow.

    ── Path-by-path field presence ───────────────────────────────────────
    EXISTS_AND_LAUNCHED (name found in DB):
      job_run ✓ | normalization ✓ | name_check ✓ | launch ✓ | finalize ✓
      idempotency – | resolution – | nova_creation – | upsert –

    NOT_FOUND (archive returned no match):
      job_run ✓ | normalization ✓ | name_check ✓ | idempotency ✓
      resolution ✓ | finalize ✓
      launch – | nova_creation – | upsert –

    CREATED_AND_LAUNCHED (new nova):
      job_run ✓ | normalization ✓ | name_check ✓ | idempotency ✓
      resolution ✓ | nova_creation ✓ | upsert ✓ | launch ✓ | finalize ✓

    FAILED (any task raised an unrecoverable error):
      job_run ✓ | terminal_fail ✓ | finalize ✓
      all other fields present only if the failure occurred after that task ran

    ── Not yet active in current ASL ─────────────────────────────────────
    coordinate_check, upsert_alias, and quarantine correspond to states
    specified in initialize-nova.md and implemented in the Lambda handlers,
    but NOT yet present in initialize_nova.asl.json.
    """

    model_config = _OUTPUT_CONFIG

    job_run: BeginJobRunOutput
    normalization: NormalizeCandidateNameOutput | None = None
    name_check: CheckExistingNovaByNameOutput | None = None
    idempotency: AcquireIdempotencyLockOutput | None = None
    resolution: ResolveCandidateAgainstPublicArchivesOutput | None = None

    # NOT YET ACTIVE — CheckExistingNovaByCoordinates not yet in ASL.
    coordinate_check: CheckExistingNovaByCoordinatesOutput | None = None

    nova_creation: CreateNovaIdOutput | None = None
    upsert: UpsertMinimalNovaMetadataOutput | None = None

    # NOT YET ACTIVE — UpsertAliasForExistingNova not yet in ASL.
    upsert_alias: UpsertAliasForExistingNovaOutput | None = None

    launch: PublishIngestNewNovaOutput | None = None

    # NOT YET ACTIVE — QuarantineHandler not yet in ASL.
    quarantine: QuarantineHandlerOutput | None = None

    terminal_fail: TerminalFailHandlerOutput | None = None

    finalize: (
        InitializeNovaFinalizeOutput | FinalizeJobRunFailedOutput | FinalizeJobRunQuarantinedOutput
    )


# ── ingest_new_nova ───────────────────────────────────────────────────────────


class IngestNewNovaFinalizeOutput(FinalizeJobRunSuccessOutput):
    """Constrained finalize output for ingest_new_nova terminal success paths."""

    outcome: Literal["LAUNCHED"]  # type: ignore[assignment]


class IngestNewNovaTerminalOutput(BaseModel):
    """
    Full execution output shape for the ingest_new_nova workflow.

    Acquires an idempotency lock, then launches refresh_references and
    discover_spectra_products in parallel (Parallel state). Both branches
    are fire-and-forget SFN starts; the Parallel state completes once both
    launcher tasks return.

    ── Path-by-path field presence ───────────────────────────────────────
    LAUNCHED (normal path):
      job_run ✓ | idempotency ✓ | downstream ✓ | finalize ✓

    FAILED:
      job_run ✓ | terminal_fail ✓ | finalize ✓
      idempotency / downstream present only if failure occurred after each ran

    ── $.downstream shape ────────────────────────────────────────────────
    $.downstream is the Parallel state result: a list of two branch outputs,
    index-ordered by branch definition:
      [0] — LaunchRefreshReferences return value (LaunchWorkflowOutput shape)
      [1] — LaunchDiscoverSpectraProducts return value (LaunchWorkflowOutput shape)
    """

    model_config = _OUTPUT_CONFIG

    job_run: BeginJobRunOutput
    idempotency: AcquireIdempotencyLockOutput | None = None

    # Parallel state result: list of 2 LaunchWorkflowOutput dicts
    downstream: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "Parallel state result. Two elements: "
            "[0] = LaunchRefreshReferences output, "
            "[1] = LaunchDiscoverSpectraProducts output. "
            "Each conforms to LaunchWorkflowOutput shape."
        ),
    )

    terminal_fail: TerminalFailHandlerOutput | None = None

    finalize: IngestNewNovaFinalizeOutput | FinalizeJobRunFailedOutput


# ── refresh_references ───────────────────────────────────────────────────────


class RefreshReferencesFinalizeOutput(FinalizeJobRunSuccessOutput):
    """Constrained finalize output for refresh_references terminal success paths."""

    outcome: Literal["SUCCEEDED"]  # type: ignore[assignment]


class RefreshReferencesTerminalOutput(BaseModel):
    """
    Full execution output shape for the refresh_references workflow.

    ── Path-by-path field presence ───────────────────────────────────────
    SUCCEEDED (normal completion — all references reconciled, discovery_date updated):
      job_run ✓ | idempotency ✓ | reconcile_summary ✓ | discovery ✓
      discovery_upsert ✓ | finalize ✓

    FAILED:
      job_run ✓ | terminal_fail ✓ | finalize ✓
      all other fields present only if the failure occurred after that task ran

    ── $.reconcile_summary shape ─────────────────────────────────────────
    $.reconcile_summary is the FetchAndReconcileReferences output: a lightweight
    summary with counts and quarantined bibcodes. The full candidate list never
    transits through SFn state (eliminating the 256KB payload limit).

    Per-item failures are quarantined inside the Lambda and reported in
    quarantined_bibcodes. Item-level quarantine writes diagnostics to the
    JobRun record directly (DDB update_item, no SNS — see handler for details).
    """

    model_config = _OUTPUT_CONFIG

    job_run: BeginJobRunOutput
    idempotency: AcquireIdempotencyLockOutput | None = None
    reconcile_summary: FetchAndReconcileReferencesOutput | None = None
    discovery: ComputeDiscoveryDateOutput | None = None
    discovery_upsert: UpsertDiscoveryDateMetadataOutput | None = None
    terminal_fail: TerminalFailHandlerOutput | None = None

    finalize: RefreshReferencesFinalizeOutput | FinalizeJobRunFailedOutput


# ── discover_spectra_products ─────────────────────────────────────────────────


class DiscoverSpectraProductsFinalizeOutput(FinalizeJobRunSuccessOutput):
    """Constrained finalize output for discover_spectra_products terminal success paths."""

    outcome: Literal["COMPLETED"]  # type: ignore[assignment]


class DiscoverSpectraProductsTerminalOutput(BaseModel):
    """
    Full execution output shape for the discover_spectra_products workflow.

    Iterates over a provider list ($.providers) via a Map state
    (DiscoverAcrossProviders, MaxConcurrency=1, ToleratedFailurePercentage=100).
    Each iteration runs five tasks sequentially; the accumulated per-provider
    iteration state lands in $.discovery_results.

    ── Path-by-path field presence ───────────────────────────────────────
    COMPLETED (Map finished — zero or more providers succeeded):
      job_run ✓ | idempotency ✓ | providers ✓ | discovery_results ✓ | finalize ✓

    FAILED (Map-level failure or pre-Map task failure):
      job_run ✓ | terminal_fail ✓ | finalize ✓
      idempotency / providers present only if failure occurred after each ran

    ── $.providers shape ─────────────────────────────────────────────────
    PrepareProviderList (Pass state) writes the hardcoded MVP list:
      [{"provider": "ESO"}]

    ── $.discovery_results shape ─────────────────────────────────────────
    DiscoverAcrossProviders Map result: one element per provider. Each element
    is the accumulated $ of the completed iteration (from ItemSelector + task
    ResultPaths within the iterator):
      {
        provider:          str,                            # from ItemSelector
        nova_id:           str,                            # from ItemSelector
        correlation_id:    str,                            # from ItemSelector
        job_run:           BeginJobRunOutput,              # from ItemSelector
        query_result:      QueryProviderForProductsOutput,
        normalize_result:  NormalizeProviderProductsOutput,
        dedup_result:      DeduplicateAndAssignDataProductIdsOutput,
        persist_result:    PersistDataProductMetadataOutput,
        publish_result:    <workflow_launcher return value>,
      }

    ToleratedFailurePercentage=100: failed provider iterations appear in
    $.discovery_results as error dicts rather than aborting the workflow.
    """

    model_config = _OUTPUT_CONFIG

    job_run: BeginJobRunOutput
    idempotency: AcquireIdempotencyLockOutput | None = None

    # PrepareProviderList (Pass state): [{"provider": "ESO"}] for MVP.
    providers: list[dict[str, Any]] | None = Field(
        default=None,
        description='PrepareProviderList Pass state result. MVP: [{"provider": "ESO"}].',
    )

    # DiscoverAcrossProviders Map result: one element per provider.
    discovery_results: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "DiscoverAcrossProviders Map result. "
            "Each element is the accumulated per-provider iteration state "
            "(see class docstring for full key breakdown)."
        ),
    )

    terminal_fail: TerminalFailHandlerOutput | None = None

    finalize: DiscoverSpectraProductsFinalizeOutput | FinalizeJobRunFailedOutput


# ── acquire_and_validate_spectra ──────────────────────────────────────────────


class AcquireAndValidateSpectraFinalizeOutput(FinalizeJobRunSuccessOutput):
    """Constrained finalize output for acquire_and_validate_spectra terminal success paths.

    All success paths (VALID, QUARANTINED, TERMINAL_INVALID, and all SKIPPED_*
    variants) use outcome="COMPLETED". Path-specific detail is carried in
    $.validation.validation_outcome and $.skip_reason.outcome respectively.
    """

    outcome: Literal["COMPLETED"]  # type: ignore[assignment]


class AcquireAndValidateSpectraTerminalOutput(BaseModel):
    """
    Full execution output shape for the acquire_and_validate_spectra workflow.

    One execution per data_product_id; started non-blocking by
    discover_spectra_products via workflow_launcher.

    ── Path-by-path field presence ───────────────────────────────────────
    Acquisition paths (status flags all False → AcquireArtifact runs):
      job_run ✓ | idempotency ✓ | status ✓ | acquisition ✓ | validation ✓
      record_result ✓ | finalize ✓

    Skip paths (already_validated / is_quarantined / cooldown_active):
      job_run ✓ | idempotency ✓ | status ✓ | skip_reason ✓ | finalize ✓
      acquisition –, validation –, record_result –

    FAILED:
      job_run ✓ | terminal_fail ✓ | finalize ✓
      all other fields present only if the failure occurred after that task ran

    ── $.skip_reason shape ───────────────────────────────────────────────
    Written by Pass states (AlreadyValidated / QuarantineBlocked / CooldownActive):
      {"outcome": "SKIPPED_ALREADY_VALIDATED" |
                  "SKIPPED_QUARANTINE_BLOCKED" |
                  "SKIPPED_COOLDOWN_ACTIVE"}

    ── $.record_result shape ─────────────────────────────────────────────
    Written by RecordValidationResult → {"persisted_outcome": str}
    Written by RecordDuplicateLinkage → {"canonical_data_product_id": str}
    Discriminate by key presence.

    ── No QuarantineHandler ──────────────────────────────────────────────
    There is no QuarantineHandler Lambda call in this workflow. QUARANTINED
    products are handled by RecordValidationResult writing
    validation_status=QUARANTINED directly, then routing to FinalizeJobRunSuccess.
    """

    model_config = _OUTPUT_CONFIG

    job_run: BeginJobRunOutput
    idempotency: AcquireIdempotencyLockOutput | None = None
    status: CheckOperationalStatusOutput | None = None

    # Written by AlreadyValidated / QuarantineBlocked / CooldownActive Pass states.
    skip_reason: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Pass state result on skip paths. "
            'Key: "outcome". Values: "SKIPPED_ALREADY_VALIDATED" | '
            '"SKIPPED_QUARANTINE_BLOCKED" | "SKIPPED_COOLDOWN_ACTIVE".'
        ),
    )

    acquisition: AcquireArtifactOutput | None = None
    validation: ValidateBytesOutput | None = None

    # RecordValidationResult → {persisted_outcome: str}
    # RecordDuplicateLinkage → {canonical_data_product_id: str}
    record_result: dict[str, Any] | None = Field(
        default=None,
        description=(
            "RecordValidationResult output: {persisted_outcome} or "
            "RecordDuplicateLinkage output: {canonical_data_product_id}."
        ),
    )

    terminal_fail: TerminalFailHandlerOutput | None = None

    finalize: AcquireAndValidateSpectraFinalizeOutput | FinalizeJobRunFailedOutput
