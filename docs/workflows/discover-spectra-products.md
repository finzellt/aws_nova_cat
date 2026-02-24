# Workflow Spec: discover_spectra_products

## Purpose

Discover candidate **spectra data products** for a given `nova_id` across one or more providers/archives.

Responsibilities:

- Query providers via provider-specific adapters
- Normalize provider responses into Nova Cat’s internal “discovered spectra product” shape
- Assign stable `data_product_id` values for newly discovered products
- Persist critical product metadata (provider, locators, provenance hints)
- Publish continuation events to trigger downstream acquisition+validation

Does NOT download/acquire bytes.

Provider-specific access patterns (API shape, pagination, auth, rate limits, etc.) are implemented inside provider adapters, not in Step Functions branching.

---

## Triggers

- Triggered by `ingest_new_nova` via `discover_spectra_products`
- Scheduled refresh (time-bucketed)
- Manual/operator invocation for an existing `nova_id`

---

## Event Contracts

### Input Event Schema
- Schema name: `discover_spectra_products`
- Schema path: `schemas/events/discover_spectra_products/latest.json`
- Required: `nova_id`
- Optional: `correlation_id` (generated if missing)

### Output Event Schema (Downstream Published Event)
- Schema name: `acquire_and_validate_spectra`
- Schema path: `schemas/events/acquire_and_validate_spectra/latest.json`

This is the intended consumer schema.

Published event(s) MUST contain:
- `nova_id`
- `data_product_id`
- `correlation_id`

---

## State Machine (Explicit State List)

1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
   - If `correlation_id` missing: generate a new UUID
3. **BeginJobRun** (Task)
4. **AcquireIdempotencyLock** (Task)
5. **DiscoverAcrossProviders** (Map)
   - **QueryProviderForProducts** (Task)
   - **NormalizeProviderProducts** (Task)
   - **DeduplicateAndAssignDataProductIds** (Task)
   - **PersistDataProductMetadata** (Task)
   - **PublishAcquireAndValidateSpectraRequests** (Task)
6. **SummarizeDiscovery** (Task)
7. **FinalizeJobRunSuccess** (Task)
8. **TerminalFailHandler** (Task)
9. **FinalizeJobRunFailed** (Task)

---

## Provider-specific discovery model

- Each provider is processed via a provider-specific adapter.
- The adapter may implement provider-specific request patterns and pagination internally.
- Output of each provider adapter MUST be normalized into a common internal shape prior to persistence.

This workflow does not branch by provider in Step Functions; provider variability is encapsulated in the adapter.

---

## Data Product Identity & Locator Alias Policy (MVP)

Discovery performs **metadata-level deduplication** when possible, and defers ambiguous cases to post-acquisition checks.

### Identity Ladder (Discovery-time)

For each discovered provider record, the workflow MUST determine an `identity_strategy`:

1. **NATIVE_ID (Strong)**
   If a provider-native product identifier is available, it MUST be used as the primary identity key.

2. **METADATA_KEY (Strong, fallback)**
   If no native ID exists, a strong metadata key MAY be used if sufficient fields are present.
   Recommended minimum fields:
   - `provider`
   - `instrument`
   - `observation_time` (or equivalent timestamp)
   Additional fields may be included (e.g., telescope, program_id, pipeline_tag).

3. **WEAK (Defer)**
   If neither (1) nor (2) can be constructed deterministically, identity is considered WEAK and definitive dedupe MUST be deferred to `acquire_and_validate_spectra` (byte-level fingerprint match).

The workflow SHOULD persist `identity_strategy` (`NATIVE_ID|METADATA_KEY|WEAK`) on the data product record for diagnostics.

### Locator Alias Rule

If a discovered record resolves to an existing `data_product_id`, and the newly discovered locator is not already recorded, it MUST be persisted as an additional locator alias.

### Publication Rule

If a discovered record resolves to an existing `data_product_id` that is already `VALIDATED`, the workflow MUST NOT publish a new `acquire_and_validate_spectra` request for it.

---

## Persisted Metadata Requirements (Data Product Entity)

Each newly discovered spectra data product MUST persist:

### Identity / correlation
- `data_product_id`
- `nova_id`
- `provider`
- `correlation_id` (when available; may also only exist in JobRun/Attempt/logs)

### Acquisition descriptors (MVP Mode 1)
- provider-native product identifier (if available)
- one or more locator fields sufficient for the downstream acquisition step (URL/API reference/etc.)
- one primary locator plus zero or more locator aliases (additional access paths discovered later)

### Provenance / format hints (to support FITS profiles)
- `instrument` (if available)
- `telescope` (if available)
- `pipeline_tag` or `collection` (if available)
- any provider metadata that improves FITS Profile selection downstream

### Mode 2 friendliness (NOT used in MVP, but persisted if available)
- optional `bundle_locator` or `acquisition_group_key` when provider supplies a bulk/bundle acquisition option

---

## Retry / Timeout Policy (per state)

- **QueryProviderForProducts**
  - Timeout: 60s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- **NormalizeProviderProducts**
  - Timeout: 30s
  - Retry: internal transient only; MaxAttempts 2
- **DeduplicateAndAssignDataProductIds**
  - Timeout: 30s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- **PersistDataProductMetadata**
  - Timeout: 30s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- **PublishAcquireAndValidateSpectraRequests**
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 2; Backoff 2s, 10s
- **DiscoverAcrossProviders (Map)**
  - MaxConcurrency: MVP default 1 (sequential providers; lowest complexity/cost); tunable later

---

## Failure Classification Policy

- **Retryable**
  - provider timeouts / throttling / transient 5xx
  - transient internal dependency failures
- **Terminal**
  - schema mismatch/version mismatch
  - invalid or missing `nova_id`
  - internal invariant violations (e.g., cannot construct product identity key)
- **Quarantine (provider-item level)**
  - malformed provider record that cannot be normalized safely
  - ambiguous/unsafe identity mapping (cannot assign stable data_product_id deterministically)

Note: item-level quarantines should not fail the entire workflow; they should be counted and surfaced in summary/metrics.

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

The SNS notification is best-effort and MUST NOT cause the workflow to fail if notification delivery fails.

---

## Idempotency Guarantees & Invariants

Workflow idempotency key (time-bucketed):
- `DiscoverSpectraProducts:{nova_id}:{schema_version}:{time_bucket}`

Data product identity key (conceptual; stable):
- `provider + product_timestamp + (url OR size OR provider_key)`

Invariants:
- No name-based logic (UUID-first)
- Only UUIDs are published downstream (`nova_id`, `data_product_id`)
- `correlation_id` is propagated when present; generated if missing
- `idempotency_key` is internal-only and MUST NOT be part of event payloads
