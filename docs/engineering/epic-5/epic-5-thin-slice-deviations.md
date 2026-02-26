# Epic 5 Thin Slice Deviations (Explicit Omissions)

This document lists what we intentionally omit or stub in Epic 5,
so we can “thicken” the vertical slice later without losing track.

All omissions must be marked in code with:
- log warning: "EPIC5_STUB: <description>"
- and/or comment: "# EPIC5_STUB: <description>"

---

## Global deviations

### Providers
- Only one stub spectra provider is implemented for discovery.
- Provider Map remains MaxConcurrency=1.

### Acquisition realism
- No real external downloads required.
- AcquireArtifact may:
  - write placeholder bytes to raw bucket, OR
  - skip S3 put entirely and record acquisition_status accordingly.

### FITS parsing & canonicalization
- FITS parsing is not implemented.
- Profile-driven validation is stubbed:
  - may always succeed, or quarantine deterministically.
- No IVOA canonical model generation yet.

### Fingerprints / dedupe by content
- sha256/header signature hash may be omitted if no bytes are acquired.
- Duplicate-by-fingerprint is deferred unless bytes exist.

### RefreshReferences
- ingest_new_nova includes the branch in design, but Epic 5 may stub it as:
  - Pass state, or a Task that no-ops and logs EPIC5_STUB.

### Notifications
- Quarantine notifications are best-effort and must not fail workflows if publish fails.

---

## Workflow-specific deviations

### initialize_nova
- Public archive resolution is stubbed (deterministic outcome).
- Coordinate-based duplicate detection may be stubbed (but must follow the threshold outcomes once implemented).

### ingest_new_nova
- RefreshReferences launch is stubbed/no-op (see global deviations).
- Still must enforce: if Nova.status != ACTIVE, short-circuit and finalize.

### discover_spectra_products
- Adapter returns exactly one fake product.
- Identity ladder: implement at least strong identity (native id / metadata key) deterministically.
- Item-level quarantine may be used for weak identities; workflow continues and summarizes.

### acquire_and_validate_spectra
- Cooldown enforcement is real.
- Acquisition may be stubbed (placeholder bytes).
- Validation may be stubbed (always success or deterministic quarantine).
- Eligibility removal semantics are real.

---

## Thickening checklist (later)
- Add real provider adapters + normalization rules
- Implement real download + archive formats (zip handling)
- Compute sha256/header signature and implement duplicate-by-fingerprint
- Implement real profile selection + FITS normalization to canonical model
- Implement refresh_references workflow and wire ingest_new_nova branch
- Add integration tests and repair/sweep utilities
