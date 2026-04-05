Task: Merge multi-arm spectra at artifact generation time (task S4)
Problem: UVES (and eventually X-shooter) produces separate FITS files for blue and red arms of the same observation. The pipeline correctly treats them as separate data_product_ids, but the waterfall plot shows two traces at nearly identical epochs, which is confusing. These should be visually merged into a single spectrum at artifact generation time.
File to modify:
services/artifact_generator/generators/spectra.py
Design (decided in conversation): This is a display-layer merge only. The raw data_product_ids and FITS files are untouched. The merge happens after individual spectra are processed but before the final artifact is assembled.
Implementation:

Add module-level constants:

python   _ARM_MERGE_MJD_TOLERANCE = 0.1    # days (~2.4 hours) — same-visit detection
   _ARM_MERGE_OVERLAP_MAX = 0.05     # max fractional overlap to qualify for merge (5%)

After all spectra are individually processed (Step 2 in generate_spectra_json), add a merge pass before sorting:
a. Group candidates. Group spectra by instrument (exact match). Within each instrument group, find pairs where abs(epoch_mjd_A - epoch_mjd_B) < _ARM_MERGE_MJD_TOLERANCE.
b. Validate merge candidates. For each candidate pair, check that their wavelength ranges are non-overlapping or minimally overlapping: compute the overlap region and verify it's less than _ARM_MERGE_OVERLAP_MAX of the total combined range. If the overlap exceeds the threshold, skip the merge (these might be genuinely different observations).
c. Merge. For qualifying pairs:

Concatenate wavelength and flux arrays, sort by wavelength
Use the earlier epoch_mjd as the merged spectrum's epoch
Set wavelength_min and wavelength_max from the combined range
Keep one data_product_id as the primary identifier (the one with the shorter wavelength range, i.e., the blue arm — arbitrary but deterministic)
Add a field merged_data_product_ids: list[str] containing both IDs for traceability
Set instrument from the shared instrument name
Log an INFO message: "Merged multi-arm spectra" with both data_product_ids, the instrument, and the epoch

d. Replace the two individual spectrum records with the single merged record in the spectra list.
Groups of 3+ (defensive). If an instrument+epoch group has more than 2 members (e.g., X-shooter's 3 arms someday), apply the merge to all members that pass the non-overlap check — concatenate all qualifying wavelength/flux arrays together. Log a warning noting the unusual group size.
No instrument match = no merge. Spectra with different instrument values at the same epoch are never merged, even if their wavelength ranges are complementary.

Note on the artifact schema: Adding merged_data_product_ids is a new field. Make it optional (default None or absent for non-merged spectra). The frontend doesn't need to know about merging — it just sees one spectrum with a wider wavelength range.
Tests to add:

Two-arm merge: Two spectra, same instrument, MJD within tolerance, non-overlapping wavelength ranges (300-500nm and 500-1000nm). Assert: merged into one spectrum, wavelength range 300-1000nm, flux arrays concatenated and sorted, merged_data_product_ids contains both IDs.
No merge (different instruments): Two spectra at same epoch but different instruments. Assert: both remain separate.
No merge (same epoch, overlapping ranges): Two spectra, same instrument, same epoch, but wavelength ranges overlap by >5%. Assert: both remain separate.
No merge (different epochs): Two spectra, same instrument, but MJD difference > tolerance. Assert: both remain separate.
Single spectrum (no merge candidates): Only one spectrum for the nova. Assert: unchanged, no merged_data_product_ids field.

Quality gates: mypy --strict on services/artifact_generator/, ruff check, all existing tests pass, new tests pass.


----------------------------------------------------------------------------------------------------


Task: Add structured error context on all exception/failure paths (task L4)
Problem: When workflows fail, the TerminalFailHandler and Catch paths often log generic messages without enough detail to diagnose root cause from CloudWatch alone. Operators have to cross-reference Step Functions execution history, which is slower and harder to query across many executions.
Files to modify: Multiple service handlers — specifically every handler that contains a TerminalFailHandler task or handles errors from Catch paths.
Investigation and implementation:

Find all TerminalFailHandler implementations. Search across all service handlers for functions handling the TerminalFailHandler task name. The primary one is in services/job_run_manager/handler.py (_terminal_fail_handler).
Enrich the TerminalFailHandler log. When _terminal_fail_handler is invoked, Step Functions passes the error context in the event (from the Catch block's ResultPath). Ensure the handler logs at ERROR level with these structured fields:

error_type — the Error field from the Step Functions error object
error_cause — the Cause field (often contains the exception message)
failed_state — which state in the workflow failed (if available from the error object)
workflow_name — already available from the event
nova_id — already available from the event
correlation_id — already available from the event
error_classification — TERMINAL (since this is the terminal fail handler)
data_product_id — if present in the event (for acquire_and_validate_spectra)
provider — if present in the event


Check Catch blocks in ASL files. In each workflow's ASL, Catch blocks route to either TerminalFailHandler or workflow-specific error states. The error object is placed in $.error (per the ResultPath in the Catch config). Verify that the handler receiving $.error is actually logging its contents, not just the existence of an error.
Enrich QuarantineHandler logging. In services/quarantine_handler/handler.py, ensure that quarantine events log at WARNING level with:

quarantine_reason_code
nova_id
data_product_id (if applicable)
bibcode (if applicable)
Full context about what triggered the quarantine


Enrich RetryableError logging. In handlers that raise RetryableError, ensure the raise site logs at WARNING level with the retry context before raising. The Step Functions retry policy will handle the retry, but the log should capture why the retry was triggered.
Don't duplicate existing good logging. Some handlers may already log errors thoroughly. Verify before adding — the goal is filling gaps, not cluttering.

Quality gates: mypy --strict on all modified service directories, ruff check, all existing tests pass. No new tests required — this is observability instrumentation.

----------------------------------------------------------------------------------------------------

Task: Fix ingest_ticket integration test fixtures for R2 NameMapping preflight
Problem: Two integration tests are failing after the R2 change (DDB preflight check before initialize_nova in the ticket path):
FAILED tests/integration/test_ingest_ticket_integration.py::TestSpectraHappyPath::test_spectra_ingested_end_to_end
FAILED tests/integration/test_ingest_ticket_integration.py::TestPhotometryHappyPath::test_photometry_rows_written_end_to_end
Both fail with: AssertionError: _sfn.start_sync_execution was called unexpectedly — NameMapping preflight should have returned a hit.
Root cause: The test fixtures seed a Nova item (PK=<nova_id>, SK=NOVA) but do NOT seed a corresponding NameMapping item. The new _resolve_nova preflight queries PK = "NAME#<normalized_name>" — since no NameMapping exists in the test table, the preflight misses and falls through to calling initialize_nova, which the test has intentionally mocked to raise an error.
File to modify:
tests/integration/test_ingest_ticket_integration.py
Fix: In the test setup/fixture for both TestPhotometryHappyPath and TestSpectraHappyPath, where the Nova item is seeded into the mocked DynamoDB table, also seed a NameMapping item:
pythontable.put_item(Item={
    "PK": "NAME#v4739 sgr",          # normalized: lowercase, underscore→space
    "SK": f"NOVA#{nova_id}",
    "entity_type": "NameMapping",
    "schema_version": "1",
    "name_raw": "V4739_Sgr",
    "name_normalized": "v4739 sgr",
    "name_kind": "PRIMARY",
    "nova_id": nova_id,
    "source": "INGESTION",
    "created_at": "2026-01-01T00:00:00Z",
    "updated_at": "2026-01-01T00:00:00Z",
})
The normalized name must be "v4739 sgr" (lowercase, underscore replaced with space per the I1 fix). Check the ticket's OBJECT NAME field in each test to confirm the exact name used, and normalize it the same way the production code does: name.strip().lower().replace("_", " ") then collapse whitespace.
For the spectra test, check if it uses a different nova name — if so, seed the NameMapping with the corresponding normalized form.
Do NOT change any production code. This is purely a test fixture gap.
Quality gates: All 1130 tests pass (the 1128 that were passing plus the 2 that were failing). ruff check. No mypy changes needed (test files).

----------------------------------------------------------------------------------------------------

Task: Fix partial copy-forward for swept novae in artifact regeneration pipeline
Problem: When a sweep regenerates artifacts for a nova, only the artifacts specified in the nova's manifest are generated (e.g., photometry WorkItem → regenerate photometry.json, sparkline.svg, nova.json, bundle.zip). But artifacts that are NOT in the manifest (e.g., spectra.json, references.json) are also NOT copied forward from the previous release. They simply disappear from the new release.
The current copy-forward logic in main.py only operates at the whole-nova level: novae that are NOT in the sweep batch get all their artifacts copied from the previous release. Novae that ARE in the sweep get only their freshly generated artifacts — nothing else.
Root cause location: services/artifact_generator/main.py — the per-nova processing loop and the interaction with release_publisher.py.
Expected behavior: After generating the manifest-specified artifacts for a swept nova, any artifacts that exist in the previous release for that nova but were NOT regenerated should be copied forward to the new release. For example, if the manifest says to regenerate photometry.json, sparkline.svg, nova.json, and bundle.zip, then spectra.json and references.json should be copied from the previous release (if they exist there).
Implementation:

In services/artifact_generator/release_publisher.py, add a new method:

python   def copy_forward_missing_artifacts(
       self,
       nova_id: str,
       generated_artifacts: set[str],
   ) -> int:
This method:

Lists all keys under releases/<previous_release_id>/nova/<nova_id>/ in S3
For each artifact found, extracts the filename (e.g., spectra.json, references.json)
If the filename is NOT in generated_artifacts, copies it from the previous release to the new release
If the filename IS in generated_artifacts, skips it (the fresh version was already uploaded)
Returns the count of artifacts copied
If _previous_release_id is None (bootstrap), returns 0 immediately — nothing to copy
Logs each copy at DEBUG level and a summary at INFO level with nova_id, copied count, and skipped count


In services/artifact_generator/main.py, in the per-nova processing loop, after all generators for the nova have run successfully and their artifacts have been uploaded:

Build a set[str] of artifact filenames that were just generated (e.g., {"photometry.json", "sparkline.svg", "nova.json", "bundle.zip"})
Call publisher.copy_forward_missing_artifacts(nova_id, generated_artifacts)
Log the result

Find the exact location in the loop — it should be after the per-nova generators run but before the nova is recorded as succeeded.
Don't break the existing whole-nova copy-forward. The Phase 2 copy_forward_unchanged_novae() logic should remain unchanged — it handles novae that have NO WorkItems at all. The new method handles novae that ARE in the sweep but only have a partial manifest.

Edge cases:

Bootstrap (first sweep ever): _previous_release_id is None. The new method returns 0 immediately. Correct — there's nothing to copy.
Nova is new (first time being swept): No artifacts exist in the previous release. The S3 listing returns empty. 0 artifacts copied. Correct.
All artifacts were regenerated: The generated_artifacts set covers everything in the previous release. 0 artifacts copied. Correct — no unnecessary copies.
Generator failed for this nova: If the nova is recorded as failed, the per-nova copy-forward should NOT run — the whole nova's artifacts should be copied from the previous release via the existing Phase 2 mechanism. Make sure the new copy-forward call is inside the success path, not the failure path.

Tests to add (in tests/services/test_publication.py or equivalent):

Partial manifest — missing artifacts copied: Seed a previous release with spectra.json, references.json, photometry.json, and nova.json for a nova. Call copy_forward_missing_artifacts(nova_id, {"photometry.json", "nova.json"}). Assert spectra.json and references.json were copied to the new release. Assert photometry.json and nova.json were NOT copied (they were freshly generated).
Bootstrap — no-op: Set _previous_release_id to None. Call the method. Assert 0 returned, no S3 calls made.
Nothing to copy — all generated: Seed previous release with photometry.json and nova.json. Call with generated_artifacts={"photometry.json", "nova.json"}. Assert 0 copied.
New nova — nothing in previous release: Previous release exists but has no artifacts for this nova. Assert 0 copied, no errors.

Quality gates: mypy --strict on services/artifact_generator/, ruff check, all existing tests pass, new tests pass.
