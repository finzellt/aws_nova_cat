## 13. Lambda Implementations — Epic 10 & 11 Updates

> 📄 Updates to `service_files_compressed_3_2_26.md` sections 13.3, 13.4, 13.6, 13.7.
> Corrections to 13.8. New sections: test coverage summary.

---

### 13.3 `idempotency_guard` ✅ Implemented (updated)

**Key format change (Epic 11):** `normalized_candidate_name` replaced by `primary_id`.
Each workflow supplies its natural identifier for the idempotency key:
- `initialize_nova` → passes `$.normalization.normalized_candidate_name` as `primary_id`
- `ingest_new_nova` and all downstream workflows → pass `nova_id` as `primary_id`

**Key format:** `{workflow_name}:{primary_id}:{schema_version}:{YYYY-MM-DDTHH}` (1h bucket)
**DDB item fields:** `primary_id` (replaces `normalized_candidate_name`), `workflow_name`,
`job_run_id`, `acquired_at`, `ttl` (24h).

---

### 13.4 `job_run_manager` ✅ Implemented (updated)

**`BeginJobRun` change (Epic 11):** `candidate_name` made optional; `nova_id` also optional.
Whichever identifier is present is stored on the JobRun item. Both may be absent from the
item if neither is supplied (though at least one should always be passed in practice).
- `initialize_nova` → passes `candidate_name` (no `nova_id` yet)
- `ingest_new_nova` and downstream → passes `nova_id` only

**`FinalizeJobRunSuccess` outcomes (updated):** Added `LAUNCHED` for `ingest_new_nova`.
Full set: `CREATED_AND_LAUNCHED | EXISTS_AND_LAUNCHED | NOT_FOUND | NOT_A_CLASSICAL_NOVA | LAUNCHED`

---

### 13.6 `workflow_launcher` ✅ Implemented

**Path:** `services/workflow_launcher/handler.py`

| Task | Status | Notes |
|---|---|---|
| `PublishIngestNewNova` | ✅ | Starts `ingest_new_nova` SFN execution |
| `LaunchRefreshReferences` | ✅ | Starts `refresh_references` SFN execution |
| `LaunchDiscoverSpectraProducts` | ✅ | Starts `discover_spectra_products` SFN execution |
| `PublishAcquireAndValidateSpectraRequests` | ⚙️ stub | `NotImplementedError` |

**Shared `_start_execution` helper:** all three implemented tasks delegate here.
- Execution name: `{nova_id}-{job_run_id[:8]}` (45 chars, unique, traceable)
- Continuation event: `{nova_id, correlation_id}` — downstream workflows fetch
  additional data from DDB rather than receiving it in the payload
- `ExecutionAlreadyExists` (typed or `ClientError`) → idempotent success, `already_existed=True`
- `ClientError` with `ThrottlingException | ServiceUnavailable | InternalServerError` → `RetryableError`
- All other `ClientError` → re-raised

**Env vars (injected by CDK `NovaCatWorkflows`):**
`INGEST_NEW_NOVA_STATE_MACHINE_ARN`, `REFRESH_REFERENCES_STATE_MACHINE_ARN`,
`DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN`

---

### 13.7 `quarantine_handler` ✅ Implemented

**Path:** `services/quarantine_handler/handler.py`

| Task | Status | Notes |
|---|---|---|
| `QuarantineHandler` | ✅ | Updates existing JobRun; best-effort SNS |

- **DDB:** `update_item` on existing JobRun (pk/sk from `event["job_run"]`). Writes:
  `quarantine_reason_code`, `classification_reason`, `error_fingerprint`, `quarantined_at`,
  `extra_context` (optional dict, e.g. `{min_sep_arcsec: Decimal}`).
- **`error_fingerprint`:** SHA-256 of `quarantine_reason_code + workflow_name + candidate_name`,
  truncated to 12 hex chars. Stable for same inputs.
- **Float → Decimal:** `extra_context` float values converted via `Decimal(str(float_val))`
  before DDB storage.
- **SNS:** best-effort publish to `NOVA_CAT_QUARANTINE_TOPIC_ARN`. Errors swallowed and
  logged — never fail the workflow.
- **SNS payload fields:** `workflow_name`, `primary_id` (nova_id if present, else
  candidate_name), `correlation_id`, `error_fingerprint`, `quarantine_reason_code`,
  `classification_reason`.
- **Classification reasons:**
  - `COORDINATE_AMBIGUITY`: `"2\"–10\" separation band — manual review required"`
  - `OTHER`: `"Ambiguous classification — conflicting resolver results"`
  - All others: fallback string.

---

### 13.8 `reference_manager` ⚙️ Stubs only (corrected)

**Path:** `services/reference_manager/handler.py` | Workflow: `refresh_references`
**Primary purpose:** Retrieve ADS (NASA Astrophysics Data System) references
(papers, books, posters) for a given nova; maintain a running bibliography per nova.
Secondary: compute `discovery_date` from earliest credible reference.

**Note:** Handler exists with stub dispatch table only. Does NOT yet use
`nova_common` logging/tracing pattern — needs migration to standard handler
conventions before Epic 12 implementation begins.

| Task | Status |
|---|---|
| `FetchReferenceCandidates` | ⚙️ stub |
| `NormalizeReference` | ⚙️ stub |
| `UpsertReferenceEntity` | ⚙️ stub |
| `LinkNovaReference` | ⚙️ stub |
| `ComputeDiscoveryDate` | ⚙️ stub |
| `UpsertDiscoveryDateMetadata` | ⚙️ stub |

**Epic 12 prerequisites (documentation pass required before implementation):**
- ADS API query strategy (by name? coordinates? both?)
- `Reference` entity DDB item model (not yet in `dynamodb-item-model.md`)
- Definition of "normalize" for a reference (dedup key, canonical form)
- Discovery date computation rule ("earliest credible reference" needs definition)
- Whether `archive_resolver` is also involved (ADS vs TNS separation of concerns)

---

### Test coverage summary (as of Epic 11)

| Module | Unit tests | Integration tests |
|---|---|---|
| `job_run_manager` | ✅ `tests/services/test_job_run_manager.py` | — |
| `idempotency_guard` | ✅ `tests/services/test_idempotency_guard.py` | — |
| `nova_resolver` | — | via integration |
| `archive_resolver` | — | via integration (mocked) |
| `workflow_launcher` | ✅ `tests/services/test_workflow_launcher.py` | — |
| `quarantine_handler` | ✅ `tests/services/test_quarantine_handler.py` | — |
| `initialize_nova` workflow | — | ✅ `tests/integration/test_initialize_nova_integration.py` (7 paths) |
| `ingest_new_nova` workflow | — | ✅ `tests/integration/test_ingest_new_nova_integration.py` (4 paths) |
| CDK synth | ✅ `tests/infra/test_synth.py` | — |

**Integration test pattern:** handlers called directly in ASL order; shared mocked
DynamoDB via moto; external calls (SFN, SNS, SIMBAD/TNS) patched via `patch.object`.
Handler modules reloaded fresh per test via `_load_handlers()` / `_load_handler()`.

---

### Key conventions established (all handlers)

- Module docstring: purpose, task table, data model, design notes
- `@tracer.capture_lambda_handler` on `handle()`; `@tracer.capture_method` on task fns
- `configure_logging(event)` first line of `handle()`
- Dispatch table `_TASK_HANDLERS` at bottom of module (after all fns defined)
- `logger.info/warning/error` with `extra={}` for structured fields
- `_now()` helper: `datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")`
- `float` → `Decimal(str(float_val))` before any DDB storage
- Module-level boto3 clients (`_table`, `_sns`, `_sfn`) instantiated once at cold start
