# Workflow Spec: refresh_references

## Purpose
Fetch ADS reference data for a `nova_id`, upsert global `Reference` entities keyed
by ADS bibcode, link them to the nova via `NovaReference` items, and compute
`discovery_date` from the earliest credible publication date.

**Note:** `ComputeDiscoveryDate` lives here (post-reconciliation), by design.

---

## Triggers
- Scheduled refresh (time-bucketed)
- Triggered after `ingest_new_nova`
- Manual/operator re-run

---

## Event Contracts

### Input Event Schema
- Schema name: `refresh_references`
- Schema path: `schemas/events/refresh_references/latest.json`
- Required identifiers: `nova_id`
- Optional: `correlation_id` (workflow generates if missing)
- Optional behavioral knobs (via `attributes: dict`):
  - `ads_name_hints: list[str]` — additional nova name aliases to include in the
    ADS name-based query, beyond those already registered in the nova's NameMapping
    partition. Useful for operator-triggered re-runs where a new alias is known but
    not yet reconciled.

### Output Event Schema (Downstream Published Event)
- Typically no downstream workflow event required.
- If you maintain a "completed" event schema, it would be:
  - `schemas/events/refresh_references_completed/latest.json` *(optional, if exists)*

---

## ADS Query Strategy

`FetchReferenceCandidates` executes a single name-based query against the ADS
search API. All known nova name aliases are drawn from the nova’s `NameMapping`
partition, plus any `ads_name_hints` supplied in the input event. Names are
individually quoted and joined with `OR`:

    "V1324 Sco" OR "Nova Sco 2013" OR "PNV J17291350-3846120"

Results are deduplicated by bibcode before the `ReconcileReferences` Map step.

**Note:** A coordinate cone search was considered but abandoned — the ADS
positional search feature (`pos(circle, ...)`) is not reliably documented and
could not be validated. Adding a coordinate search as a supplementary query is a
natural `rule_version` increment if ADS publishes stable documentation in future.

**Rate limits and auth:** ADS API requests require a token passed via `Authorization:
Bearer <token>`. The token is read from AWS Secrets Manager at handler startup.
Unauthenticated requests are rate-limited to 5 requests/day. Authenticated requests
are rate-limited to 5000 requests/day. The `FetchReferenceCandidates` handler MUST
treat HTTP 429 responses as retryable.

---

## Reference Type Classification

`NormalizeReference` maps ADS `doctype` field values to `ReferenceType` enum values:

| ADS `doctype` | `ReferenceType` |
|---|---|
| `article` | `journal_article` |
| `eprint` | `arxiv_preprint` |
| `inproceedings` | `conference_abstract` |
| `abstract` | `conference_abstract` |
| `circular` | `cbat_circular` |
| `telegram` | `atel` |
| `catalog` | `catalog` |
| `software` | `software` |
| *(anything else)* | `other` |

---

## Normalization Rules

`NormalizeReference` applies the following transformations before `UpsertReferenceEntity`:

- `bibcode` — taken directly from ADS response; no transformation.
- `publication_date` — derived from the ADS `date` field. ADS returns dates as
  `YYYY-MM-01T00:00:00Z` (day is always `01` when only month precision is available).
  `NormalizeReference` extracts year and month and stores as `YYYY-MM-00`, discarding
  the day entirely since it cannot be trusted. Day `00` signals month-only precision.
- `arxiv_id` — strip `arXiv:` prefix if present; store bare ID only.
- `authors` — store as-is from ADS `author` list; no normalization in MVP.
- `title` — store as-is; no normalization in MVP.
- `doi` — store as-is if present; omit if absent.

---

## Input Validation

`ValidateInput` and `EnsureCorrelationId` Pass/Choice states are **not present** in
this workflow's ASL. In MVP, `refresh_references` is only triggered internally — by
`ingest_new_nova` and by the scheduled refresh — whose outbound Pydantic event models
enforce the contract at the publishing boundary. Workflow-entry validation is therefore
redundant and omitted by design.

**Post-MVP:** When a standalone API entrypoint for `refresh_references` is introduced
(e.g. operator-triggered re-run or ad-hoc refresh), `ValidateInput` and
`EnsureCorrelationId` should be added as the first two states, consistent with the
pattern in `initialize_nova_asl.json`. This will be a non-breaking ASL change as long
as the input event schema is a strict superset of the internally-published schema.

---

## State Machine (Explicit State List)

1. **BeginJobRun** (Task)
2. **AcquireIdempotencyLock** (Task)
3. **FetchReferenceCandidates** (Task)
4. **ReconcileReferences** (Map)
   - NormalizeReference (Task)
   - UpsertReferenceEntity (Task) → yields `bibcode`
   - LinkNovaReference (Task)
   - ItemFailureHandler (Catch → QuarantineItem + Continue)
5. **ComputeDiscoveryDate** (Task)
6. **UpsertDiscoveryDateMetadata** (Task) (no-op if unchanged)
7. **FinalizeJobRunSuccess** (Task)
8. **TerminalFailHandler** (Task)
9. **FinalizeJobRunFailed** (Task)

---

## Retry / Timeout Policy (per state)

- BeginJobRun / AcquireIdempotencyLock:
  - Timeout 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- FetchReferenceCandidates:
  - Timeout 60s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- Map item tasks:
  - Timeout 20s each; Retry MaxAttempts 2; Backoff 2s, 10s
  - Map MaxConcurrency: MVP default 5 (tunable)
- ComputeDiscoveryDate:
  - Timeout 20s; Retry MaxAttempts 2 (internal transient only)
- UpsertDiscoveryDateMetadata:
  - Timeout 20s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s

---

## Failure Classification Policy

- Retryable:
  - transient upstream/service failures; throttling; timeouts; ADS HTTP 429
- Terminal:
  - schema/version mismatch
  - missing/invalid `nova_id`
- Quarantine:
  - item-level reference parse failures (continue Map)
  - discovery date cannot be selected due to irreconcilable conflicts (rare)

### Quarantine Handling

When a workflow transitions to **QuarantineHandler**, it MUST:

1. Persist quarantine status and relevant diagnostic metadata.
2. Emit a JobRun outcome of `QUARANTINED`.
3. Publish a notification event to an SNS topic for operational review.

SNS notification requirements:
- Include workflow name
- Include primary identifier (e.g., `nova_id` or `data_product_id`)
- Include `correlation_id`
- Include `error_fingerprint`
- Include brief classification reason

The SNS notification is best-effort and MUST NOT cause the workflow to fail if
notification delivery fails.

---

## Idempotency Guarantees & Invariants

- Workflow idempotency key (time-bucketed): `refresh_references:{nova_id}:{schema_version}:{time_bucket}`
- Reference upsert dedupe key: `ReferenceUpsert:ADS:{bibcode}:{schema_version}`
- Relationship dedupe key: `NovaReferenceLink:{nova_id}:{bibcode}`
- DiscoveryDate dedupe key: `DiscoveryDate:{nova_id}:{earliest_bibcode}:{rule_version}`
- Invariant: `discovery_date` update is monotonically earlier (unless explicitly
  configured otherwise). `discovery_date` is stored as `YYYY-MM-DD` string; day `00`
  signals month-only precision. Comparison uses month granularity only `(YYYY, MM)`;
  the day component is ignored because `00` means unknown precision. A day-00 date
  and a day-precise date in the same month are treated as equal (no overwrite).
- Invariant: `idempotency_key` is internal-only (not in event schemas).

---

## JobRun / Attempt Emissions and Required Log Fields

- Map item failures MUST emit Attempt with `error_classification=QUARANTINE` and continue.
- Required structured log fields:
  - `workflow_name`, `execution_arn`, `job_run_id`, `state_name`, `attempt_number`
  - `schema_version`, `correlation_id`, `nova_id`
  - `reference_source`, `candidate_count`, `upsert_count`, `link_count`, `quarantined_count`
  - `discovery_date_old`, `discovery_date_new`
