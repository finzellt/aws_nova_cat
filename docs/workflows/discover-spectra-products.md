# Workflow Spec: discover_spectra_products

## Purpose

Discover candidate spectra products for `nova_id` across multiple providers.

Responsibilities:

- Query providers
- Normalize provider metadata
- Assign stable `dataset_id`
- Persist dataset metadata (including provider + locator + format hints)
- Publish continuation events for downstream validation

Does NOT download files.

Provider-specific access patterns are contained within provider adapters.

---

## Triggers
- Triggered by `ingest_new_nova` via `discover_spectra_products`
- Scheduled (time-bucketed)
- Manual/operator invocation

## Event Contracts

### Input Event Schema
- Schema name: `discover_spectra_products`
- Schema path: `schemas/events/discover_spectra_products/latest.json`
- Required: `nova_id`
- Optional: `correlation_id` (generated if missing)

### Output Event Schema (Downstream Published Event)
- Schema name: `download_and_validate_spectra`
- Schema path: `schemas/events/download_and_validate_spectra/latest.json`

This is the intended consumer schema.

---

## State Machine
1. ValidateInput
2. EnsureCorrelationId
3. BeginJobRun
4. AcquireIdempotencyLock
5. DiscoverAcrossProviders (Map)
   - QueryProviderForProducts
   - NormalizeProviderProducts
   - DeduplicateAndAssignDatasetIds
   - PersistDatasetMetadata
   - PublishDownloadAndValidateSpectraRequests
6. SummarizeDiscovery
7. FinalizeJobRunSuccess
8. TerminalFailHandler
9. FinalizeJobRunFailed

---

## Important Metadata Persistence
Each dataset entry must store:

- dataset_id
- nova_id
- provider
- product locator(s)
- optional format hints (instrument, pipeline tag, etc.)

This supports profile-driven validation in downstream workflow.

---

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

---

## Idempotency

Workflow idempotency key:
`DiscoverSpectraProducts:{nova_id}:{schema_version}:{time_bucket}`

Dataset identity key (conceptual):
`provider + product_timestamp + (url OR size OR provider_key)`

Idempotency key is internal-only.

---

## Invariants
- No names used.
- Only UUIDs published downstream.
- Event payload acts as continuation payload.
- Provider information must be persisted for downstream profile selection.
