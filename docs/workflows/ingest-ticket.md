# Workflow Spec: ingest_ticket

## Purpose

Ticket-driven ingestion of photometry observations and spectra data files using
hand-curated metadata tickets (DESIGN-004).

Given a ticket file (`.txt`), this workflow:
1) Parses the ticket into a validated, typed model (photometry or spectra).
2) Resolves the ticket's `OBJECT NAME` to a `nova_id` via `initialize_nova`.
3) Branches by ticket type and executes the appropriate ingestion path:
   - **Photometry:** Reads a headerless CSV using ticket-supplied column indices,
     resolves filter strings against the band registry (ADR-017), and writes
     `PhotometryRow` items to DynamoDB (ADR-020).
   - **Spectra:** Reads a metadata CSV (two-hop indirection: ticket → metadata CSV
     → spectrum data files), converts each spectrum CSV to a FITS file with
     reconstructed headers, uploads to the Public S3 bucket, and inserts a
     DynamoDB reference item per spectrum.

This workflow is the primary ingestion path for MVP. It replaces the runtime
heuristic path (ADR-021 Layer 0) for files that have a corresponding ticket. The
heuristic path remains as the fallback for files without tickets.

**Scope boundary:** This workflow covers ticket-driven ingestion only. It does not
replace ADR-021 (Layer 0 heuristics), ADR-018 (disambiguation algorithm), or any
downstream persistence decisions in ADR-020. It provides an alternative entry point
that bypasses Layer 0's runtime inference when a ticket exists.

---

## Triggers

- Operator-triggered with a path to a ticket `.txt` file
- Future: batch operator script iterating over a directory of ticket files

---

## Event Contracts

### Input Event Schema

- Schema name: `ingest_ticket`
- Schema path: `schemas/events/ingest_ticket/latest.json`
- Required: `ticket_path` (S3 key or local path to the `.txt` ticket file)
- Required: `data_dir` (S3 prefix or local directory containing the data file(s)
  referenced by the ticket)
- Optional: `correlation_id` (generated if missing)

### Output Event Schema

No downstream workflow event is published. This workflow is a terminal ingestion
path — it reads source data and writes to DynamoDB and/or S3 directly.

---

## State Machine (Explicit State List)

1. **ValidateInput** (Pass)
2. **EnsureCorrelationId** (Choice + Pass)
   - If `correlation_id` missing: generate new UUID
3. **BeginJobRun** (Task)
4. **AcquireIdempotencyLock** (Task)
5. **ParseTicket** (Task)
   - Reads the `.txt` file at `ticket_path`
   - Parses key-value pairs into a raw dict
   - Discriminates ticket type (`DATA FILENAME` → photometry, `METADATA FILENAME` → spectra)
   - Validates and coerces into `PhotometryTicket` or `SpectraTicket` (Pydantic)
   - Returns: serialized ticket + `ticket_type` discriminator
6. **ResolveNova** (Task)
   - Extracts `object_name` from the parsed ticket
   - Checks DynamoDB `NameMapping` for an existing `nova_id`
   - If not found: fires `initialize_nova` synchronously via
     `sfn:StartSyncExecution` and reads the result from the response
   - Returns: `nova_id`, `primary_name`, `ra_deg`, `dec_deg`
   - Failure modes:
     - `initialize_nova` returns `NOT_FOUND` → quarantine with
       `UNRESOLVABLE_OBJECT_NAME`
     - `initialize_nova` returns `QUARANTINED` → quarantine with
       `IDENTITY_AMBIGUITY`
     - `initialize_nova` fails → terminal failure
7. **TicketTypeBranch** (Choice)
   - `ticket_type == "photometry"` → **IngestPhotometry**
   - `ticket_type == "spectra"` → **IngestSpectra**

### Photometry branch

8a. **IngestPhotometry** (Task)
   - Reads the headerless CSV at `data_dir/data_filename`
   - Iterates rows using ticket-supplied column indices
   - For each row:
     - Extracts time, flux, error, filter string, upper limit flag
     - Applies ticket-level defaults for fields without per-row columns
     - Converts time to MJD (from JD/HJD/BJD per `time_system`)
     - Resolves filter string against `band_registry.json` (ADR-017)
     - Constructs a `PhotometryRow` (ADR-019 v2.0)
   - Writes rows to DynamoDB via conditional `PutItem` (ADR-020)
   - Updates `PRODUCT#PHOTOMETRY_TABLE` envelope item (row count, ingestion metadata)
   - Returns: `rows_written`, `rows_skipped_duplicate`, `rows_failed`
   - → **FinalizeJobRunSuccess** (outcome = `INGESTED_PHOTOMETRY`)

### Spectra branch

8b. **IngestSpectra** (Task)
   - Reads the metadata CSV at `data_dir/metadata_filename`
   - For each row in the metadata CSV:
     - Extracts the spectrum filename, per-spectrum metadata (date, telescope,
       instrument, observer, dispersion, wavelength range, flux units)
     - Reads the spectrum data CSV at `data_dir/<spectrum_filename>` using
       per-spectrum column indices from the metadata CSV
     - Converts CSV → FITS:
       - Primary HDU: flux array
       - Reconstructs FITS header from ticket + metadata CSV fields
       - Populates standard keywords: OBJECT, DATE-OBS, TELESCOP, INSTRUME,
         OBSERVER, CRVAL1, CDELT1, NAXIS1, BUNIT, BIBCODE
     - Uploads FITS to Public S3 bucket
     - Inserts DynamoDB reference item (DataProduct + FileObject)
   - Returns: `spectra_ingested`, `spectra_failed`
   - → **FinalizeJobRunSuccess** (outcome = `INGESTED_SPECTRA`)

### Terminal states

9. **FinalizeJobRunSuccess** (Task)
10. **QuarantineHandler** (Task)
11. **FinalizeJobRunQuarantined** (Task)
12. **TerminalFailHandler** (Task)
13. **FinalizeJobRunFailed** (Task)

---

## Retry / Timeout Policy (per state)

- BeginJobRun / AcquireIdempotencyLock:
  - Timeout 10s; Retry MaxAttempts 3; Backoff 2s, 10s, 30s

- ParseTicket:
  - Timeout 30s
  - Retry: none (deterministic — a parse failure is an authoring error, not transient)

- ResolveNova:
  - Timeout 120s (accounts for `initialize_nova` Express execution)
  - Retry MaxAttempts 2 for transient failures (throttling, network)

- IngestPhotometry:
  - Timeout 5m
  - Retry MaxAttempts 2 for transient DDB failures

- IngestSpectra:
  - Timeout 10m (multiple FITS conversions + S3 uploads)
  - Retry MaxAttempts 2 for transient S3/DDB failures

---

## Failure Classification Policy

- **Retryable:**
  - Transient DDB/S3 failures, throttling, network timeouts
  - `initialize_nova` transient failures

- **Terminal:**
  - Ticket parse failure (malformed ticket — operator authoring error)
  - Schema/version mismatch
  - Missing data file(s) referenced by the ticket
  - Band resolution failure for a filter string not in the registry

- **Quarantine:**
  - `initialize_nova` returns `NOT_FOUND` (object name unresolvable)
  - `initialize_nova` returns `QUARANTINED` (coordinate ambiguity)

---

## Idempotency Guarantees & Invariants

- Workflow idempotency key: `IngestTicket:{ticket_filename}:{schema_version}`
- Row-level idempotency: `PhotometryRow` writes use conditional `PutItem` with
  deterministic `row_id` (ADR-020). Duplicate rows from re-running the same ticket
  are suppressed at write time.
- Spectra idempotency: FITS files are written to deterministic S3 keys. Re-upload
  overwrites the same object. DDB reference items use deterministic keys.
- **Invariant:** After `ResolveNova` succeeds, all downstream operations use `nova_id`
  only. No name-based operations beyond the resolution boundary.
- **Invariant:** The ticket file is read-only. The workflow never modifies the ticket
  or the source data files.

---

## Nova Resolution Strategy

Nova resolution uses Lambda-encapsulated synchronous invocation of `initialize_nova`:

1. **Preflight check:** Query DynamoDB `NameMapping` partition
   (`PK = "NAME#<normalized_object_name>"`) for an existing `nova_id`.
2. **If found:** Return the existing `nova_id` immediately. Fetch `ra_deg`, `dec_deg`
   from the `Nova` item.
3. **If not found:** Fire `initialize_nova` synchronously via
   `sfn:StartSyncExecution` with `candidate_name = object_name`. The call blocks
   until the Express workflow reaches a terminal state and returns the result
   inline.
4. **On `CREATED_AND_LAUNCHED` or `EXISTS_AND_LAUNCHED`:** Extract `nova_id` from the
   synchronous execution response. Fetch coordinates from the `Nova` item.
5. **On `NOT_FOUND`:** Raise `QuarantineError` with reason `UNRESOLVABLE_OBJECT_NAME`.
6. **On `QUARANTINED`:** Raise `QuarantineError` with reason `IDENTITY_AMBIGUITY`.
7. **On failure:** Raise `TerminalError`.

This approach requires zero modifications to `initialize_nova`. Because
`initialize_nova` is an Express Workflow, `StartSyncExecution` blocks until
completion and returns the output directly — no polling loop is needed.
`initialize_nova` completes in 5–15s for the happy path, and each ticket contains
exactly one `OBJECT NAME`.

---

## Data Flow Summary

```
ticket.txt ──→ ParseTicket ──→ ResolveNova ──→ TicketTypeBranch
                                  │                    │
                          initialize_nova          ┌───┴───┐
                          (sync invocation)        │       │
                                               photometry  spectra
                                                   │       │
                                              CSV rows   metadata CSV
                                                   │       │
                                              band reg   per-spectrum:
                                              resolve     CSV → FITS
                                                   │       │
                                              DDB PutItem  S3 upload +
                                              (PhotometryRow) DDB ref
```

---

## Lambda Handlers

This workflow requires three new Lambda handlers (plus the existing shared handlers
for JobRun management, idempotency, quarantine, and finalization):

| Handler | Task States | Description |
|---|---|---|
| `ticket_parser` | ParseTicket | Reads `.txt` file, validates into typed Pydantic model |
| `nova_resolver_ticket` | ResolveNova | DDB lookup + `initialize_nova` sync invocation |
| `ticket_ingestor` | IngestPhotometry, IngestSpectra | Dispatches on ticket type; reads data, transforms, persists |

`ticket_ingestor` is a single Lambda with internal dispatch based on `ticket_type`.
The two ingestion paths share no processing logic but share the Lambda deployment
artifact and entry point for operational simplicity.

---

## Relationship to Existing Workflows

| Workflow | Relationship |
|---|---|
| `initialize_nova` | Called synchronously (`StartSyncExecution`) by `ResolveNova` for unknown object names. Not modified. |
| `ingest_photometry` | Not called. `ingest_ticket` writes `PhotometryRow` items directly using the same DDB schema (ADR-020) but through a different code path (ticket-driven vs. heuristic). |
| `acquire_and_validate_spectra` | Not called. `ingest_ticket` produces FITS files and DDB references directly, bypassing the discovery/acquisition/validation pipeline. The output artifacts are compatible. |

---

## Open Questions

| # | Question | Blocking? |
|---|---|---|
| OQ-1 | Deterministic `row_id` derivation for ticket-ingested `PhotometryRow` items. Must be identical to the derivation used by the heuristic path (ADR-020 OQ-1) so that the same observation ingested via either path produces the same `row_id`. | Blocks implementation |
| OQ-2 | S3 key structure for ticket-ingested FITS files. Must be compatible with the existing spectra file layout so that `generate_nova_bundle` can find them. | Blocks spectra implementation |
| OQ-3 | Should `ingest_ticket` create the `PRODUCT#PHOTOMETRY_TABLE` and `PRODUCT#COLOR_TABLE` envelope items if they don't exist, or require that `initialize_nova` has already created them? | Blocks implementation |
