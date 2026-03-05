# Nova Cat — Compressed Test Suite

---

## SHARED PATTERNS (all service unit tests)

Every service test file follows this identical scaffold — stated once here, not repeated per file:

**`aws_env` fixture** (`autouse=True`): sets `NOVA_CAT_TABLE_NAME`, `NOVA_CAT_QUARANTINE_TOPIC_ARN`, `INGEST_*_STATE_MACHINE_ARN` (where applicable), `AWS_DEFAULT_REGION=us-east-1`, `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`, `POWERTOOLS_SERVICE_NAME=nova-cat-test`, `LOG_LEVEL=DEBUG`.

**`table` fixture**: `mock_aws()` + `dynamodb.create_table(_TABLE_NAME, PK+SK string, PAY_PER_REQUEST)`. Integration tests also add GSI1.

**`_load_handler()`**: deletes cached module from `sys.modules`, then `importlib.import_module(...)`. All tests re-import fresh inside the moto context.

**`_base_event(**kwargs)`**: minimal valid event with `task_name`, common fields, `**kwargs` override.

**All tests** wrap handler calls in `with mock_aws():` and re-call `_load_handler()` per test.

---

## INFRA TESTS — `test_synth.py`

**Fixture**: `template` (module scope) — synthesizes `NovaCatStack("NovaCatTest", account="000000000000", region="us-east-1")` once for all tests.

### TestDynamoDb
| Test | Assertion |
|---|---|
| single_table_exists | `resource_count_is("AWS::DynamoDB::Table", 1)` |
| table_name | `TableName: "NovaCat"` |
| primary_key_schema | `PK=HASH, SK=RANGE` |
| primary_key_attribute_types | PK/SK/GSI1PK/GSI1SK all `AttributeType: S` |
| billing_mode | `BillingMode: PAY_PER_REQUEST` |
| pitr_disabled_by_default | `PointInTimeRecoveryEnabled: false` |
| eligibility_gsi | `IndexName: EligibilityIndex`, `GSI1PK=HASH, GSI1SK=RANGE`, `Projection: ALL` |

### TestS3
| Test | Assertion |
|---|---|
| private_bucket_versioning | `Status: Enabled`; lifecycle rules `ExpireQuarantineObjects` (prefix=`quarantine/`, 365d) and `ExpireWorkflowPayloadSnapshots` (prefix=`workflow-payloads/`, 30d) |
| private_bucket_blocks_public | All four `BlockPublic*/IgnorePublic*/Restrict*` = true |
| public_site_releases_lifecycle | `ExpireOldReleases` (prefix=`releases/`, 730d) |
| public_site_versioning_disabled | Bucket with `ExpireOldReleases` has no `VersioningConfiguration` |
| both_buckets_enforce_ssl | Scans all `BucketPolicy` resources; asserts `aws:SecureTransport` condition present on ≥2 |

### TestSns
| Test | Assertion |
|---|---|
| quarantine_topic_exists | `TopicName: nova-cat-quarantine-notifications` |

### TestLambda
Parameterized against `_EXPECTED_FUNCTIONS` dict (12 entries):
```
nova-cat-nova-resolver      256MB  30s
nova-cat-job-run-manager    256MB  30s
nova-cat-idempotency-guard  256MB  30s
nova-cat-archive-resolver   256MB  90s
nova-cat-workflow-launcher  256MB  30s
nova-cat-reference-manager  256MB  90s
nova-cat-spectra-discoverer 256MB  60s
nova-cat-spectra-acquirer   512MB  900s
nova-cat-spectra-validator  512MB  300s
nova-cat-photometry-ingestor 512MB 300s
nova-cat-quarantine-handler 256MB  30s
nova-cat-name-reconciler    256MB  90s
```
| Test | Assertion |
|---|---|
| all_twelve_functions_exist | all 12 names present in template |
| all_functions_use_python_311 | `Runtime: python3.11` per function |
| all_functions_have_xray_tracing | `TracingConfig.Mode: Active` per function |
| all_functions_have_required_env_vars | all 6 standard env vars present per function |
| function_memory_and_timeout (parametrized) | correct memory + timeout per entry in table above |

**`_REQUIRED_ENV_VARS`**: `NOVA_CAT_TABLE_NAME`, `NOVA_CAT_PRIVATE_BUCKET`, `NOVA_CAT_PUBLIC_SITE_BUCKET`, `NOVA_CAT_QUARANTINE_TOPIC_ARN`, `LOG_LEVEL`, `POWERTOOLS_SERVICE_NAME`

### TestOutputs
Asserts `has_output("*", {"Export": {"Name": X}})` for each of:
`NovaCat-TableName`, `NovaCat-PrivateBucketName`, `NovaCat-PublicSiteBucketName`, `NovaCat-QuarantineTopicArn`, `NovaCat-InitializeNovaStateMachineArn`, `NovaCat-IngestNewNovaStateMachineArn`, `NovaCat-RefreshReferencesStateMachineArn`, `NovaCat-DiscoverSpectraProductsStateMachineArn`

### TestStepFunctions
`_EXPECTED_STATE_MACHINES`: `nova-cat-initialize-nova`, `nova-cat-ingest-new-nova`, `nova-cat-refresh-references`, `nova-cat-discover-spectra-products`
| Test | Assertion |
|---|---|
| all_state_machines_exist | each name present |
| all_standard_workflow | `StateMachineType: STANDARD` per name |
| all_have_execution_role | `RoleArn` present on each |
| initialize_nova_can_invoke_lambdas | ≥1 IAM policy with `lambda:InvokeFunction` |
| workflow_launcher_can_start_executions | ≥1 IAM policy with `states:StartExecution` |

---

## SERVICE UNIT TESTS

### `test_idempotency_guard.py`
`_TABLE_NAME = "NovaCat-Test"`. `_base_event()`: `task_name=AcquireIdempotencyLock, workflow_name=initialize_nova, primary_id="v1324 sco", job_run_id="job-001", correlation_id="corr-001"`.

**TestAcquireIdempotencyLock**:
- `acquires_lock_and_returns_key` → result has `idempotency_key`, `acquired_at`
- `key_contains_workflow_name_and_primary_id` → key contains `"initialize_nova"` and `"v1324 sco"`
- `writes_dynamodb_item` → `PK=IDEMPOTENCY#<key>, SK=LOCK`; item has `workflow_name`, `primary_id`, `job_run_id`, `ttl`
- `lock_already_held_raises_retryable_error` → second call with same inputs raises `RetryableError(match="already held")`
- `key_is_stable_for_same_inputs` → `_compute_key` deterministic within hour
- `different_primary_ids_produce_different_keys` → `"v1324 sco"` ≠ `"rs oph"`
- `works_with_nova_id_as_primary_id` → `workflow_name="ingest_new_nova"`, `primary_id="4e9b0e88-..."` appear in key
- `different_workflows_produce_different_keys` → `initialize_nova` ≠ `ingest_new_nova`

**TestDispatch**: unknown task → `ValueError(match="Unknown task_name")`

---

### `test_job_run_manager.py`
`_base_event()`: `task_name=BeginJobRun, workflow_name=initialize_nova, candidate_name="V1324 Sco", correlation_id="corr-001"`.
`_finalize_event(task_name, job_run, **kwargs)`: adds `job_run` dict.

**TestBeginJobRun**:
- `returns_job_run_id` → 36-char UUID
- `uses_supplied_correlation_id` → echoed exactly
- `generates_correlation_id_when_absent` → 36-char UUID generated
- `writes_dynamodb_item` → `status=RUNNING, workflow_name=initialize_nova, candidate_name="V1324 Sco"`, `job_run_id` matches
- `pk_uses_workflow_correlation_prefix` → `PK="WORKFLOW#corr-abc"`

**TestFinalizeJobRunSuccess**:
- `updates_status_to_succeeded` → `status=SUCCEEDED, outcome=CREATED_AND_LAUNCHED`
- `all_valid_outcomes` (parametrized): `CREATED_AND_LAUNCHED`, `EXISTS_AND_LAUNCHED`, `NOT_FOUND`, `NOT_A_CLASSICAL_NOVA` — all return correctly

**TestFinalizeJobRunFailed**:
- `updates_status_to_failed` → `status=FAILED, error_type="TerminalError", error_message="Missing field"`
- `handles_missing_error_field` → no error, `status=FAILED`

**TestFinalizeJobRunQuarantined**: `status=QUARANTINED`

**TestDispatch**: unknown task → `ValueError`

---

### `test_nova_resolver.py`
Helpers: `_seed_nova(table, nova_id, ra, dec)` — writes `PK=<nova_id>, SK=NOVA` with `ra_deg/dec_deg` as `Decimal`. `_seed_name_mapping(table, normalized, nova_id)` — writes `PK=NAME#<normalized>, SK=NOVA#<nova_id>`.

**TestNormalizeCandidateName** (parametrized):
- `"V1324 Sco"` → `"v1324 sco"`
- `"  RS  Oph  "` → `"rs oph"`
- `"V407Cyg"` → `"v407cyg"`
- `"NOVA SCO 2012"` → `"nova sco 2012"`
- blank/whitespace → `TerminalError`

**TestCheckExistingNovaByName**:
- found → `{exists: True, nova_id: <uuid>}`
- not found → `{exists: False}`, no `nova_id` key

**TestCheckExistingNovaByCoordinates** (base coords `_RA=267.56, _DEC=-32.55`):
- offset `+0.5/3600` in RA → `DUPLICATE`, `matched_nova_id` set
- offset `+5.0/3600` in RA → `AMBIGUOUS`
- offset `+30.0/3600` in RA → `NONE`
- empty DB → `NONE`, no `matched_nova_id`

**TestCreateNovaId**:
- returns 36-char UUID
- writes `PK=<nova_id>, SK=NOVA` with `status=PENDING, primary_name="V1324 Sco", entity_type=Nova`

**TestUpsertMinimalNovaMetadata**:
- promotes nova to `ACTIVE`, sets `ra_deg ≈ 267.56`
- writes `PK=NAME#v1324 sco, SK=NOVA#<nova_id>` with `name_kind=PRIMARY`

**TestUpsertAliasForExistingNova**:
- writes `PK=NAME#nova sco 2012, SK=NOVA#<nova_id>` with `name_kind=ALIAS`

**TestAngularSeparation**:
- same point → `0.0` (abs=1e-6)
- 1 arcsec dec offset → `≈1.0` (abs=0.001)
- 10 arcsec dec offset → `≈10.0` (abs=0.01)

**TestDispatch**: unknown task → `ValueError`

---

### `test_quarantine_handler.py`
**Extra fixtures**: `topic` — moto SNS `create_topic(Name="nova-cat-quarantine-test")`.

Pre-seeded JobRun item: `PK="WORKFLOW#corr-001", SK="JOBRUN#initialize_nova#2026-01-01T00:00:00Z#job-001", status=RUNNING`.

`_base_event()`: `task_name=QuarantineHandler, workflow_name=initialize_nova, quarantine_reason_code=COORDINATE_AMBIGUITY, candidate_name="V1324 Sco", correlation_id=corr-001, job_run={pk, sk, job_run_id, correlation_id}`.

**TestQuarantineHandlerPersistence** (all read back the JobRun item):
- `writes_quarantine_reason_code` → `COORDINATE_AMBIGUITY`
- `writes_error_fingerprint` → present, `len==12` (truncated SHA-256)
- `writes_classification_reason` → present, non-empty
- `writes_quarantined_at` → present
- `captures_extra_context_min_sep_arcsec` → `_base_event(min_sep_arcsec=5.3)` → `item["extra_context"] == {"min_sep_arcsec": Decimal("5.3")}`
- `no_extra_context_when_absent` → `"extra_context"` not in item
- `unknown_reason_code_uses_fallback_classification` → still writes `error_fingerprint` + non-empty `classification_reason`

**TestQuarantineHandlerReturnValue**:
- returns `quarantine_reason_code`, `error_fingerprint` (len 12), `quarantined_at`
- `error_fingerprint_is_stable` → two calls with same inputs → same fingerprint

**TestQuarantineHandlerSns** (patches `handler._sns`):
- `sns_failure_does_not_raise` → `publish.side_effect = Exception(...)` → no raise, result has `error_fingerprint`
- `uses_candidate_name_as_primary_id_when_no_nova_id` → SNS payload `primary_id == "V1324 Sco"`
- `uses_nova_id_as_primary_id_when_present` → `_base_event(nova_id="nova-uuid-001")` → `primary_id == "nova-uuid-001"`
- `sns_payload_contains_required_fields` → payload has `workflow_name`, `primary_id`, `correlation_id`, `error_fingerprint`, `quarantine_reason_code`, `classification_reason`

**TestDispatch**: unknown task → `ValueError`

---

### `test_workflow_launcher.py`
`_FAKE_NOVA_ID = "nova-uuid-001"`, `_FAKE_JOB_RUN_ID = "job-run-abcdef12"`, `_EXPECTED_EXECUTION_NAME = f"{_FAKE_NOVA_ID}-{_FAKE_JOB_RUN_ID[:8]}"`.

**`state_machines` fixture**: moto SFN; creates all 3 state machines (`nova-cat-ingest-new-nova`, `nova-cat-refresh-references`, `nova-cat-discover-spectra-products`) with trivial `Succeed` ASL.

**TestLaunchTasks** (parametrized: `PublishIngestNewNova/ingest_new_nova`, `LaunchRefreshReferences/refresh_references`, `LaunchDiscoverSpectraProducts/discover_spectra_products`):
- `starts_execution_and_returns_arn` → `execution_arn` in result, SM name in ARN
- `returns_nova_id` → `result["nova_id"] == _FAKE_NOVA_ID`
- `execution_name_derived_from_nova_id_and_job_run_id` → `result["execution_name"] == _EXPECTED_EXECUTION_NAME`
- `execution_input_contains_nova_id_and_correlation_id` → fetches execution via `sfn.describe_execution`; verifies `nova_id` and `correlation_id` in parsed input JSON
- `execution_already_exists_treated_as_success` → patches `_sfn.start_execution` with `ClientError(ExecutionAlreadyExists)` → `result["already_existed"] is True`

**TestThrottling**: `ThrottlingException` ClientError → `RetryableError`

**TestStubTasks**: `PublishAcquireAndValidateSpectraRequests` → `NotImplementedError`

**TestDispatch**: unknown task → `ValueError`

---

### `test_archive_resolver.py`
No DynamoDB. Patches `astroquery.simbad.Simbad` at import time. Helper `_make_mock_table(otypes, ra, dec)` builds mock astropy Table. `_resolve_event(**kwargs)` → `task_name=ResolveCandidateAgainstPublicArchives, candidate_name="V1324 Sco"`.

**TestSimbadClassification**:
- `["No*", "V*"]` → `is_nova=True, is_classical_nova="true", resolver_source="SIMBAD"`
- `["RNe", "V*"]` → `is_nova=True, is_classical_nova="false"`
- `["Star", "V*"]` → `is_nova=False`
- nova otype → `resolved_ra/dec` present, `resolved_epoch="J2000"`
- non-nova otype → no `resolved_ra` in result

**TestNoResult**:
- `query_object=None`, `TNS_API_KEY=""` → `is_nova=False, resolver_source="NONE"`
- empty mock table (`len=0`) → `is_nova=False`

**TestErrorHandling**:
- `query_object` raises `Exception("timeout...")` → `RetryableError`
- SIMBAD says not-nova + TNS says nova → `_merge_results(simbad_result, tns_result)` raises `QuarantineError`

**TestClassifyOtypes** (parametrized, calls `handler._classify_otypes(otypes)` directly):
| otypes | is_nova | is_classical |
|---|---|---|
| `{"No*", "V*"}` | True | "true" |
| `{"No?"}` | True | "true" |
| `{"NL*", "Star"}` | True | "true" |
| `{"RNe", "V*"}` | True | "false" |
| `{"RN*"}` | True | "false" |
| `{"Star", "V*"}` | False | "false" |
| `set()` | False | "false" |

---

## INTEGRATION TESTS

### Shared infrastructure (both integration test files)

**`table` fixture** (both files): `mock_aws()` + DynamoDB with PK/SK + **GSI1** (`EligibilityIndex`, GSI1PK/GSI1SK, ALL projection).

**Handler loader**: `_load_handlers()` — deletes all relevant module entries from `sys.modules`, re-imports all handlers fresh inside moto context.

**`_run_prefix(h, ...)`**: runs the common state machine prefix shared by all paths.

---

### `test_initialize_nova_integration.py`

**Constants**:
- `_EXISTING_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"`
- `_EXISTING_NOVA_RA/DEC = 270.0, -30.0`
- `_DUPLICATE_RA/DEC = 270.000001, -30.000001` (< 2" away)
- `_AMBIGUOUS_RA/DEC = 270.0015, -30.0` (~5" away)

**Common prefix**: `BeginJobRun → NormalizeCandidateName → AcquireIdempotencyLock`. Returns `{candidate_name, job_run, normalization}`.

Helpers: `_finalize_success(h, state, outcome, nova_id=None)`, `_finalize_quarantined(h, state)`, `_get_job_run(table, state)`.

All archive resolver calls patch `_query_simbad` and `_query_tns`; all SFN calls patch `_sfn`.

**7 paths tested**:

| Path | Class | Key setup | Key assertions |
|---|---|---|---|
| CREATED_AND_LAUNCHED | `TestCreatedAndLaunched` | SIMBAD: `is_classical_nova="true"`, `aliases=["NOVA Test 2026", "Gaia DR3 1234567890"]`; empty DB | JobRun `SUCCEEDED/CREATED_AND_LAUNCHED`; Nova `status=ACTIVE, aliases=[...]`; PRIMARY NameMapping; 2 ALIAS NameMappings (`name_kind=ALIAS, source=SIMBAD`) |
| EXISTS_AND_LAUNCHED (name) | `TestExistsAndLaunchedByName` | Seed Nova + `NAME#v1324 sco` NameMapping | `CheckExistingNovaByName` returns `exists=True, nova_id=_EXISTING_NOVA_ID`; JobRun `SUCCEEDED/EXISTS_AND_LAUNCHED` |
| EXISTS_AND_LAUNCHED (coord) | `TestExistsAndLaunchedByCoordinates` | Seed Nova with `ra_deg/dec_deg=Decimal`; SIMBAD returns `_DUPLICATE_RA/DEC` | `CheckExistingNovaByCoordinates` returns `DUPLICATE, matched_nova_id=_EXISTING_NOVA_ID`; alias NameMapping written; JobRun `SUCCEEDED/EXISTS_AND_LAUNCHED` |
| NOT_FOUND | `TestNotFound` | SIMBAD: `is_nova=False` | `resolution["is_nova"] is False`; JobRun `SUCCEEDED/NOT_FOUND` |
| NOT_A_CLASSICAL_NOVA | `TestNotAClassicalNova` | SIMBAD: `is_nova=True, is_classical_nova="false"` | coord check returns `NONE`; JobRun `SUCCEEDED/NOT_A_CLASSICAL_NOVA` |
| QUARANTINE (coord ambiguity) | `TestQuarantineCoordinateAmbiguity` | Seed Nova at `_EXISTING_NOVA_RA/DEC`; SIMBAD returns `_AMBIGUOUS_RA/DEC`; patch `_sns` | coord check `AMBIGUOUS`; quarantine result has `error_fingerprint`; JobRun `QUARANTINED, quarantine_reason_code=COORDINATE_AMBIGUITY` |
| QUARANTINE (classification) | `TestQuarantineClassificationAmbiguity` | SIMBAD: `is_classical_nova="ambiguous"`; patch `_sns` | coord check `NONE`; quarantine `reason_code=OTHER`; JobRun `QUARANTINED` |

---

### `test_ingest_new_nova_integration.py`

**Constants**: `_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"`, `_CORRELATION_ID = "integ-ingest-corr-001"`.

**Common prefix** (`_run_prefix(h)`): `BeginJobRun(workflow_name=ingest_new_nova, nova_id=_NOVA_ID) → AcquireIdempotencyLock`. Returns `{nova_id, job_run}`.

All SFN calls patch `h["workflow_launcher"]._sfn` with `start_execution.return_value = {"executionArn": _FAKE_EXECUTION_ARN}`.

**3 paths**:

| Path | Class | Test | Key assertion |
|---|---|---|---|
| Happy path | `TestHappyPath` | `both_branches_succeed` | Both launches return `execution_arn`; JobRun `SUCCEEDED/LAUNCHED` |
| | | `sfn_called_twice_for_both_branches` | `mock_sfn.start_execution.call_count == 2` |
| | | `both_branches_idempotent_on_retry` | `start_execution.side_effect = ClientError(ExecutionAlreadyExists)`; both return `already_existed=True`; JobRun `SUCCEEDED/LAUNCHED` |
| Failure path | `TestFailurePath` | `terminal_error_routes_to_fail_handler` | First launch succeeds; second raises `ClientError(AccessDeniedException)` → caught by test; `FinalizeJobRunFailed` called manually; JobRun `FAILED, error_type=ClientError` |
