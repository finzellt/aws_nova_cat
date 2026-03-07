# Workflow Spec: acquire_and_validate_spectra

## Purpose

Atomic acquisition and validation of a **single spectra data product**.

This workflow is source-agnostic: "acquire" may mean downloading from a provider, reading from an internal mirror, or (future) using donated data.
Validation is profile-driven and aligned with IVOA Spectrum Data Model conventions where possible (see `docs/specs/spectra-fits-profiles.md`).

MVP: Mode 1 only (one `data_product_id` per execution), while persisting metadata that keeps the system friendly to future batching (Mode 2).

**`data_product_id`** is the stable, immutable UUID identifying the target spectra data product.
It was minted during `discover_spectra_products` via deterministic derivation:
`UUID(hash(provider + provider_product_key))` (preferred) or
`UUID(hash(provider + normalized_canonical_locator))` (fallback when no native ID exists).


Terminology:
- Exists (metadata only)
- Acquired (bytes retrieved and fingerprinted)
- Validated (bytes passed integrity + format + lightweight domain checks)
- Published-ready (future)

---

## Triggers

- Triggered by `discover_spectra_products` via `acquire_and_validate_spectra`
- Scheduled retry runs (time-bucketed) for eligible products (optional)
- Manual/operator invocation for a specific `data_product_id`

---

## Event Contracts

### Input Event Schema
- Schema name: `acquire_and_validate_spectra`
- Schema path: `schemas/events/acquire_and_validate_spectra/latest.json`
- Required: `nova_id`, `data_product_id`
- Optional: `correlation_id` (generated if missing)

### Output Event Schema
- Optional validated event (if defined later):
  - `schemas/events/spectra_data_product_validated/latest.json` (optional)

---

## State Machine (Explicit State List)

1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
   - If `correlation_id` missing: generate a new UUID
3. **BeginJobRun** (Task)
4. **AcquireIdempotencyLock** (Task)
5. **LoadDataProductMetadata** (Task)
6. **CheckOperationalStatus** (Task)
7. **AlreadyValidated?** (Choice)
   - Yes -> **FinalizeJobRunSuccess** (outcome = `SKIPPED_DUPLICATE`)
   - No  -> continue
8. **CooldownActive?** (Choice)
   - Yes -> **FinalizeJobRunSuccess** (outcome = `SKIPPED_BACKOFF`)
   - No  -> continue
9. **AcquireArtifact** (Task)
10. **ValidateBytes (Profile-Driven)** (Task)
11. **DuplicateByFingerprint?** (Choice)
    - Yes -> **RecordDuplicateLinkage** -> **FinalizeJobRunSuccess** (outcome = `DUPLICATE_OF_EXISTING`)
    - No  -> continue
12. **RecordValidationResult** (Task)
13. **FinalizeJobRunSuccess** (Task) (outcome = `VALIDATED`)
14. **QuarantineHandler** (Task)
15. **FinalizeJobRunQuarantined** (Task)
16. **TerminalFailHandler** (Task)
17. **FinalizeJobRunFailed** (Task)

---

## Persisted Operational Fields (Data Product)

To control retry frequency and prevent hammering providers, the data product SHOULD persist:

Minimum viable fields:
- `eligibility` (`ACQUIRE | NONE`) — controls EligibilityIndex (GSI1) visibility;
  set to `ACQUIRE` on stub creation by `discover_spectra_products`; cleared to
  `NONE` (with GSI1 attributes removed) on any terminal outcome
- `validation_status` (`UNVALIDATED | VALID | QUARANTINED | TERMINAL_INVALID`)
- `acquisition_status` (`STUB | ACQUIRED | FAILED_RETRYABLE | SKIPPED_DUPLICATE | SKIPPED_BACKOFF`)
- `attempt_count`
- `last_attempt_at`
- `last_attempt_outcome` (`SUCCESS | RETRYABLE_FAILURE | TERMINAL_FAILURE | QUARANTINE`)
- `last_error_fingerprint`
- `next_eligible_attempt_at`  ← primary anti-ping control
- `sha256` (when acquired; stable content fingerprint)
- `duplicate_of_data_product_id` (when a byte-level duplicate of an existing VALID product is detected)

Rich attempt details belong in JobRun/Attempt records and logs.

---

## Acquisition

**AcquireArtifact** retrieves bytes according to the data product's acquisition descriptors.

MVP assumptions:
- Most products are FITS; some may be ZIP bundles.
- If ZIP: unpack and select primary spectral FITS using stored hints or simple heuristics.

Provider-specific acquisition logic may be implemented via:
- one Lambda with internal provider dispatch, or
- provider plugin modules invoked by a single Lambda

No provider branching is introduced in Step Functions.

Future-friendly:
- data products may include `acquisition_type` (e.g., `PROVIDER_REMOTE`, `DONATED`) without changing orchestration.

---

## Validation (Profile-Driven, IVOA-aligned)

**ValidateBytes (Profile-Driven)**:
1. Open FITS
2. Select FITS Profile using:
   - provider (from data product metadata)
   - optional header signature fields (e.g., `INSTRUME`, `TELESCOP`)
3. Extract spectral arrays via profile mapping
4. Normalize relevant header fields into canonical metadata
5. Apply lightweight sanity checks (axis monotonicity, units, finiteness, etc.)
6. Produce validation summary + fingerprint

If no profile matches, or required metadata/units are missing/unresolvable → QUARANTINE.

---

## Post-acquisition Duplicate Detection (Byte-level)

Some providers may expose the same underlying product under multiple identifiers or locators.
Discovery attempts metadata-level dedupe, but definitive dedupe may require acquired bytes.

After acquisition (and once bytes are available), the workflow MUST:

1. Compute a stable content fingerprint (`sha256` of canonical bytes or a deterministic normalization).
2. Check whether an existing **VALID** data product already has the same fingerprint.
3. If a match exists:
   - Mark the current data product as a duplicate of the canonical product (`duplicate_of_data_product_id = <canonical>`).
   - Optionally append this product's locator(s) as aliases to the canonical product.
   - Finalize the JobRun successfully with outcome `DUPLICATE_OF_EXISTING`.
   - The current data product MUST NOT be marked `VALID`.
4. If no match exists:
   - Continue normal validation result recording and mark `VALID`.

This preserves stable UUIDs while avoiding duplicate scientific products downstream.

---

## Retry / Timeout Policy (per state)

- **LoadDataProductMetadata**
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- **CheckOperationalStatus**
  - Timeout: 10s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s
- **AcquireArtifact**
  - Timeout: 15m
  - Retry: Retryable only; MaxAttempts 3; Backoff 10s, 60s, 180s
  - On explicit throttling (e.g., HTTP 429): treat as retryable and apply longer cooldown in persisted fields
- **ValidateBytes (Profile-Driven)**
  - Timeout: 5m
  - Retry: deterministic validation failures are NOT retried; MaxAttempts 2 for internal transient-only failures
- **RecordValidationResult**
  - Timeout: 20s
  - Retry: Retryable only; MaxAttempts 3; Backoff 2s, 10s, 30s

Cooldown behavior:
- Retryable acquisition failures MUST update `next_eligible_attempt_at` based on a capped exponential backoff policy (provider-tunable later).

---

## Failure Classification

- **Retryable**
  - transient acquisition failures (timeouts, 5xx, throttling)
- **Terminal**
  - invalid/missing `nova_id` or `data_product_id`
  - schema mismatch/version mismatch
  - internal invariant violations
- **Quarantine**
  - checksum/fingerprint mismatch
  - unreadable/corrupt FITS or bundle
  - unknown profile / missing required metadata
  - invalid/unknown units
  - failed domain sanity checks

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

Workflow idempotency key:
- `AcquireAndValidateSpectra:{data_product_id}:{schema_version}`

Step dedupe keys (internal):
- Acquire: `Acquire:{data_product_id}:{expected_identity_or_locator}`
- Validate: `Validate:{data_product_id}:{sha256}`

Invariants:
- Exactly one `data_product_id` per execution (MVP Mode 1)
- "AlreadyValidated?" is a defensive guardrail (replays happen)
- Cooldown prevents hammering providers across repeated executions
- Profile logic must not alter UUID identity
- `idempotency_key` remains internal-only (never part of event payload)
