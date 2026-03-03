## 13. Lambda Implementations

> 📄 Source: `services/<lambda_name>/handler.py` (or nested layer paths).
> All handlers use task dispatch pattern: `event["task_name"]` → `_TASK_HANDLERS` dict → private fn.
> All use `nova_common` layer for logging/tracing.

### 13.1 `nova_common` Layer
**Path:** `services/nova_common_layer/python/nova_common/`

| Module | Contents |
|---|---|
| `errors.py` | `RetryableError`, `TerminalError`, `QuarantineError` — map to SFN `ErrorEquals` by class `__name__` |
| `logging.py` | Powertools `Logger`; `configure_logging(event)` injects `correlation_id`, `job_run_id`, `workflow_name`, `state_name`, `candidate_name`, `nova_id` as persistent keys |
| `tracing.py` | Powertools `Tracer` singleton; service name from `POWERTOOLS_SERVICE_NAME` env var |
| `__init__.py` | Package marker only |

### 13.2 `archive_resolver` ✅ Implemented
**Path:** `services/archive_resolver/handler.py` | **Container-based** (astropy/astroquery deps)

| Task | Status | Notes |
|---|---|---|
| `ResolveCandidateAgainstPublicArchives` | ✅ | SIMBAD first, TNS fallback; `QuarantineError` if conflict |

- **SIMBAD otype → classification:** `No*, No?, NL*` → classical nova; `RNe, RN*` → recurrent
  (is_nova=True, is_classical_nova="false"); else not nova.
- **Output:** `is_nova`, `is_classical_nova` ("true"/"false"), `resolved_ra/dec/epoch`,
  `resolver_source` ("SIMBAD"|"TNS"|"SIMBAD+TNS"|"NONE")
- **astropy cache** redirected to `/tmp` at module load via `_bootstrap_astropy()`
- **TNS:** plain HTTP POST to `wis-tns.org/api`; skipped if `TNS_API_KEY` not set;
  429/502/503/504 → `RetryableError`

### 13.3 `idempotency_guard` ✅ Implemented
**Path:** `services/idempotency_guard/handler.py`

| Task | Status | Notes |
|---|---|---|
| `AcquireIdempotencyLock` | ✅ | Conditional DDB put; `RetryableError` if lock already held |

- **DDB item:** `PK="IDEMPOTENCY#<key>"`, `SK="LOCK"`. TTL=24h.
- **Key format:** `{workflow_name}:{normalized_candidate_name}:{schema_version}:{YYYY-MM-DDTHH}` (1h bucket)
- **Manual release:** `aws dynamodb delete-item --key '{"PK":{"S":"IDEMPOTENCY#<key>"},"SK":{"S":"LOCK"}}'`

### 13.4 `job_run_manager` ✅ Implemented
**Path:** `services/job_run_manager/handler.py`

| Task | Status | Notes |
|---|---|---|
| `BeginJobRun` | ✅ | Generates `job_run_id` + `correlation_id` if missing; returns `pk`, `sk` for subsequent updates |
| `FinalizeJobRunSuccess` | ✅ | Sets status=SUCCEEDED + outcome |
| `FinalizeJobRunFailed` | ✅ | Sets status=FAILED + error_type/message (truncated to 500 chars) |
| `FinalizeJobRunQuarantined` | ✅ | Sets status=QUARANTINED |

- **Pre-nova PK:** `WORKFLOW#<correlation_id>` — used by `initialize_nova` before `nova_id` exists;
  permanent for NOT_FOUND/NOT_A_CLASSICAL_NOVA outcomes.
- **Post-nova PK:** `<nova_id>`. SK: `JOBRUN#<workflow_name>#<started_at>#<job_run_id>`

### 13.5 `nova_resolver` ✅ Implemented
**Path:** `services/nova_resolver/handler.py`

| Task | Status | Notes |
|---|---|---|
| `NormalizeCandidateName` | ✅ | Lowercase, collapse whitespace; `TerminalError` if empty |
| `CheckExistingNovaByName` | ✅ | Query `NAME#<normalized>` partition; returns `{exists, nova_id?}` |
| `CheckExistingNovaByCoordinates` | ✅ | Full table scan (acceptable at MVP scale); haversine; returns `{match_outcome, min_sep_arcsec, matched_nova_id?}` |
| `CreateNovaId` | ✅ | UUID4; writes Nova stub with `status=PENDING` |
| `UpsertMinimalNovaMetadata` | ✅ | Updates Nova to `status=ACTIVE` with ICRS coords; writes PRIMARY NameMapping |
| `UpsertAliasForExistingNova` | ✅ | Writes ALIAS NameMapping for existing `nova_id` |

- **Coord separation:** haversine (no astropy); `< 2"` DUPLICATE, `2–10"` AMBIGUOUS, `> 10"` NONE
- **DDB coord storage:** `Decimal(str(float))` for `ra_deg`/`dec_deg`

### 13.6 `workflow_launcher` ⚙️ Stubs only
**Path:** `services/workflow_launcher/handler.py`

Tasks (all `NotImplementedError`): `PublishIngestNewNova`, `LaunchRefreshReferences`,
`LaunchDiscoverSpectraProducts`, `PublishAcquireAndValidateSpectraRequests`

### 13.7 `quarantine_handler` ⚙️ Stub only
**Path:** `services/quarantine_handler/handler.py`

Tasks (all `NotImplementedError`): `QuarantineHandler`

### 13.8 `reference_manager` ⚙️ Stubs only
**Path:** `services/reference_manager/handler.py` | Workflow: `refresh_references`

Tasks (all `NotImplementedError`): `FetchReferenceCandidates`, `NormalizeReference`,
`UpsertReferenceEntity`, `LinkNovaReference`, `ComputeDiscoveryDate`, `UpsertDiscoveryDateMetadata`

### 13.9 `spectra_acquirer` ⚙️ Stub only
**Path:** `services/spectra_acquirer/handler.py` | Workflow: `acquire_and_validate_spectra`

Tasks (all `NotImplementedError`): `AcquireArtifact`

### 13.10 `spectra_discoverer` ⚙️ Stubs only
**Path:** `services/spectra_discoverer/handler.py` | Workflow: `discover_spectra_products`

Tasks (all `NotImplementedError`): `QueryProviderForProducts`, `NormalizeProviderProducts`,
`DeduplicateAndAssignDataProductIds`, `PersistDataProductMetadata`

### 13.11 `spectra_validator` ⚙️ Stubs only
**Path:** `services/spectra_validator/handler.py` | Workflow: `acquire_and_validate_spectra`

Tasks (all `NotImplementedError`): `CheckOperationalStatus`, `ValidateBytes`,
`RecordValidationResult`, `RecordDuplicateLinkage`

### 13.12 `photometry_ingestor` ⚙️ Stubs only
**Path:** `services/photometry_ingestor/handler.py` | Workflow: `ingest_photometry`

Tasks (all `NotImplementedError`): `CheckOperationalStatus`, `ValidatePhotometry`,
`RebuildPhotometryTable`, `PersistPhotometryMetadata`

### 13.13 `name_reconciler` ⚙️ Stubs only
**Path:** `services/name_reconciler/handler.py` | Workflow: `name_check_and_reconcile`

Tasks (all `NotImplementedError`): `FetchCurrentNamingState`, `QueryAuthorityA`, `QueryAuthorityB`,
`ReconcileNaming`, `ApplyNameUpdates`, `PublishNameReconciled`

---

### Common env vars (all Lambdas, injected by CDK)
`NOVA_CAT_TABLE_NAME`, `NOVA_CAT_PRIVATE_BUCKET`, `NOVA_CAT_PUBLIC_SITE_BUCKET`,
`NOVA_CAT_QUARANTINE_TOPIC_ARN`, `LOG_LEVEL` (default INFO), `POWERTOOLS_SERVICE_NAME`
