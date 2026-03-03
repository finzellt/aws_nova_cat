## 14. Infrastructure (CDK) — Epic 10 & 11 Updates

> 📄 Updates to `infra_files_compressed_3_2_26.md` sections 14.3 and 14.4.

---

### 14.3 `NovaCatCompute` — No changes to function specs

All 12 functions unchanged. `workflow_launcher` now receives three additional
env vars injected by `NovaCatWorkflows` (see 14.4):
- `INGEST_NEW_NOVA_STATE_MACHINE_ARN`
- `REFRESH_REFERENCES_STATE_MACHINE_ARN`
- `DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN`

---

### 14.4 `NovaCatWorkflows` ⚙️ Partially implemented (updated)

**Dependency cycle note:** `ingest_new_nova` execution role invokes `workflow_launcher`
(grant_invoke), and `workflow_launcher` needs downstream SFN ARNs as env vars. To break
the CDK dependency cycle, all state machine ARNs injected into `workflow_launcher` are
constructed via `stack.format_arn(service="states", resource_name="nova-cat-*")` rather
than referencing `attr_arn` directly. The `states:StartExecution` grant is also scoped
to `nova-cat-*` pattern for the same reason.

| State machine | Status | ASL file | CFN export |
|---|---|---|---|
| `nova-cat-initialize-nova` | ✅ | `infra/workflows/initialize_nova.asl.json` | `NovaCat-InitializeNovaStateMachineArn` |
| `nova-cat-ingest-new-nova` | ✅ | `infra/workflows/ingest_new_nova.asl.json` | `NovaCat-IngestNewNovaStateMachineArn` |
| `nova-cat-refresh-references` | ⚙️ placeholder | `infra/workflows/refresh_references.asl.json` | `NovaCat-RefreshReferencesStateMachineArn` |
| `nova-cat-discover-spectra-products` | ⚙️ placeholder | `infra/workflows/discover_spectra_products.asl.json` | `NovaCat-DiscoverSpectraProductsStateMachineArn` |

**Placeholder ASLs** (refresh_references, discover_spectra_products): single `Fail` state,
`Error="NotImplemented"`. Provisioned so `workflow_launcher` has valid ARNs at deploy time.

**Token → Lambda mapping for `ingest_new_nova`:**

| ASL token | Lambda |
|---|---|
| `BeginJobRunFunctionArn` | `job_run_manager` |
| `FinalizeJobRunSuccess/FailedFunctionArn` | `job_run_manager` |
| `AcquireIdempotencyLockFunctionArn` | `idempotency_guard` |
| `LaunchRefreshReferencesFunctionArn` | `workflow_launcher` |
| `LaunchDiscoverSpectraProductsFunctionArn` | `workflow_launcher` |

**`_grant_start_execution` helper:** grants `states:StartExecution` on `nova-cat-*`
pattern via `stack.format_arn`. Takes `(fn, stack)` — not a specific state machine ARN.

---

### ASL design notes

**`initialize_nova.asl.json`** — state order fix (Epic 10→11):
`BeginJobRun → NormalizeCandidateName → AcquireIdempotencyLock → CheckExistingNovaByName → ...`
`AcquireIdempotencyLock` Parameters uses `primary_id.$: $.normalization.normalized_candidate_name`.

**`ingest_new_nova.asl.json`** — coordinator pattern:
`BeginJobRun → AcquireIdempotencyLock → LaunchDownstream (Parallel) → FinalizeJobRunSuccess`
Parallel branches are independent — one branch failing does not affect the other.
No `SummarizeLaunch` state (removed by design — `FinalizeJobRunSuccess` serves that role).
