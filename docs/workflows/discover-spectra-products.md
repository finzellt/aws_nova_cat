# Workflow Spec: DiscoverSpectraProducts

## Purpose
Discover candidate spectra products for a `nova_id` across multiple archives/providers, normalize into a minimal internal product shape,
deduplicate, and assign stable `dataset_id` values. Does NOT download files.

## Triggers
- Scheduled discovery refresh (time-bucketed)
- Triggered after IngestNewNova
- Manual/operator re-run

## Event Contracts
### Input Event Schema
- Schema name: `DiscoverSpectraProducts`
- Required: `nova_id`
- Optional: providers list, cursor, correlation_id

### Output Event Schema
- Schema name: `SpectraProductsDiscovered`
- Includes: list of dataset_id(s) discovered/confirmed; per-provider summary

## State Machine (Explicit State List)
1. ValidateInput (Pass)
2. BeginJobRun (Task)
3. AcquireIdempotencyLock (Task)
4. DiscoverAcrossProviders (Map)  <-- Pattern A
   - QueryProviderForProducts (Task)
   - NormalizeProviderProducts (Task)  <-- provider-specific normalization boundary
   - DeduplicateAndAssignDatasetIds (Task)
   - PublishDatasetDiscoveredEvents (Task)  (one per dataset or batched)
   - ProviderItemFailureHandler (Catch -> QuarantineProvider + Continue)
5. SummarizeDiscovery (Task)
6. PublishSpectraProductsDiscovered (Task)
7. FinalizeJobRunSuccess (Task)
8. TerminalFailHandler (Task)
9. FinalizeJobRunFailed (Task)

## Retry / Timeout Policy
- QueryProviderForProducts:
  - Timeout 60s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- NormalizeProviderProducts:
  - Timeout 30s; Retry MaxAttempts 2 (internal transient only)
- DeduplicateAndAssignDatasetIds:
  - Timeout 30s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s
- PublishDatasetDiscoveredEvents:
  - Timeout 10s; Retry MaxAttempts 2
- Map MaxConcurrency:
  - MVP default 1 (sequential providers, lowest complexity/cost); tunable later

## Failure Classification Policy
- Retryable: transient provider failures, throttling
- Terminal: invalid/missing nova_id; schema/version mismatch; provider list invalid
- Quarantine:
  - provider returns malformed records
  - ambiguous product identity key construction
  - normalization failures due to unknown schema variants

## Idempotency Guarantees & Invariants
- Workflow idempotency key (time-bucketed): `DiscoverSpectraProducts:{nova_id}:{schema_version}:{time_bucket}`
- Dataset identity key (conceptual; must be stable):
  - `dataset_identity = provider + product_timestamp + (url OR size OR provider_key)`
- Dedupe key: `DatasetIdentity:{dataset_identity}:{schema_version}`
- Invariant: this workflow outputs only UUIDs (dataset_id, nova_id), never unresolved names.

## JobRun / Attempt Emissions + Required Log Fields
- Required fields include: nova_id, providers[], provider, dataset_count_new, dataset_count_existing, quarantined_count.
