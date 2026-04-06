"""
ASL <-> Lambda contract boundary tests.

Verifies that Step Functions ASL state definitions reference field paths
that actually exist in the Lambda handler return shapes. Catches the class
of bug where an ASL refactor silently breaks a Choice state Variable or
Task Parameter reference — invisible at deploy time and runtime, only
detectable by noticing that downstream work didn't happen.

Strategy: static shape registry + ASL parsing. No AWS calls, no mocks.
"""

from __future__ import annotations

import json
from glob import glob
from pathlib import Path
from typing import Any, cast

import pytest

WORKFLOWS_DIR = Path(__file__).resolve().parents[2] / "infra" / "workflows"

# ---------------------------------------------------------------------------
# Handler return shape registry
#
# Keyed by the task_name value passed in ASL Parameters. Each entry is the
# set of top-level keys the handler can return (union across all code paths).
#
# Built by reading actual handler source code — not guessed. When a handler
# conditionally includes/omits a field, the union of all paths is listed.
# ---------------------------------------------------------------------------

TASK_RETURN_SHAPES: dict[str, set[str]] = {
    # -- job_run_manager (services/job_run_manager/handler.py) ---------------
    "BeginJobRun": {"job_run_id", "correlation_id", "started_at", "pk", "sk"},
    "FinalizeJobRunSuccess": {"outcome", "ended_at"},
    "FinalizeJobRunFailed": {"status", "ended_at"},
    "FinalizeJobRunQuarantined": {"status", "ended_at"},
    "TerminalFailHandler": {"error_classification", "error_fingerprint"},
    # -- idempotency_guard (services/idempotency_guard/handler.py) -----------
    "AcquireIdempotencyLock": {"idempotency_key", "acquired_at"},
    # -- spectra_discoverer (services/spectra_discoverer/handler.py) ---------
    "DiscoverAndPersistProducts": {
        "provider",
        "nova_id",
        "total_queried",
        "total_normalized",
        "total_new",
        "total_existing",
    },
    "QueryProviderForProducts": {"raw_products"},
    "NormalizeProviderProducts": {"normalized_products"},
    "DeduplicateAndAssignDataProductIds": {"products_with_ids"},
    "PersistDataProductMetadata": {"persisted_products"},
    # -- workflow_launcher (services/workflow_launcher/handler.py) ------------
    # _start_execution returns different fields on success vs already-exists;
    # union of both paths listed here.
    "PublishIngestNewNova": {
        "nova_id",
        "execution_arn",
        "execution_name",
        "already_existed",
    },
    "LaunchRefreshReferences": {
        "nova_id",
        "execution_arn",
        "execution_name",
        "already_existed",
    },
    "LaunchDiscoverSpectraProducts": {
        "nova_id",
        "execution_arn",
        "execution_name",
        "already_existed",
    },
    "PublishAcquireAndValidateSpectraRequests": {
        "launched",
        "failed",
        "total",
    },
    # -- spectra_validator (services/spectra_validator/handler.py) ------------
    "CheckOperationalStatus": {
        "already_validated",
        "cooldown_active",
        "is_quarantined",
        "data_product",
    },
    "ValidateBytes": {
        "validation_outcome",
        "is_duplicate",
        "duplicate_of_data_product_id",
        "fits_profile_id",
        "profile_selection_inputs",
        "header_signature_hash",
        "normalization_notes",
        "quarantine_reason",
        "quarantine_reason_code",
        "instrument",
        "telescope",
        "observation_date_mjd",
        "flux_unit",
    },
    "RecordValidationResult": {"persisted_outcome"},
    "RecordDuplicateLinkage": {"canonical_data_product_id"},
    # -- spectra_acquirer (services/spectra_acquirer/handler.py) --------------
    "AcquireArtifact": {
        "raw_s3_bucket",
        "raw_s3_key",
        "sha256",
        "byte_length",
        "etag",
    },
    # -- quarantine_handler (services/quarantine_handler/handler.py) ----------
    "QuarantineHandler": {
        "quarantine_reason_code",
        "error_fingerprint",
        "quarantined_at",
    },
    # -- nova_resolver (services/nova_resolver/handler.py) -------------------
    "NormalizeCandidateName": {"normalized_candidate_name"},
    # exists=False path omits nova_id; exists=True includes it.
    "CheckExistingNovaByName": {"exists", "nova_id"},
    # matched_nova_id only present when match_outcome=DUPLICATE.
    "CheckExistingNovaByCoordinates": {
        "match_outcome",
        "min_sep_arcsec",
        "matched_nova_id",
    },
    "CreateNovaId": {"nova_id"},
    "UpsertMinimalNovaMetadata": {"nova_id"},
    "UpsertAliasForExistingNova": {"nova_id"},
    # -- archive_resolver (services/archive_resolver/handler.py) -------------
    # resolved_ra/dec/epoch only present when is_nova=True.
    "ResolveCandidateAgainstPublicArchives": {
        "is_nova",
        "is_classical_nova",
        "resolver_source",
        "aliases",
        "resolved_ra",
        "resolved_dec",
        "resolved_epoch",
    },
    # -- ticket_parser (services/ticket_parser/handler.py) -------------------
    "ParseTicket": {"ticket_type", "object_name", "ticket"},
    # -- nova_resolver_ticket (services/nova_resolver_ticket/handler.py) -----
    "ResolveNova": {"nova_id", "primary_name", "ra_deg", "dec_deg"},
    # -- ticket_ingestor (services/ticket_ingestor/handler.py) ---------------
    "IngestPhotometry": {"rows_produced", "failures"},
    "IngestSpectra": {"spectra_ingested", "spectra_failed"},
    # -- reference_manager (services/reference_manager/handler.py) -----------
    "FetchReferenceCandidates": {"nova_id", "candidates", "candidate_count"},
    "NormalizeReference": {
        "nova_id",
        "bibcode",
        "reference_type",
        "title",
        "year",
        "publication_date",
        "authors",
        "doi",
        "arxiv_id",
    },
    "UpsertReferenceEntity": {"nova_id", "bibcode", "publication_date"},
    "LinkNovaReference": {
        "nova_id",
        "bibcode",
        "publication_date",
        "linked",
    },
    "ComputeDiscoveryDate": {
        "nova_id",
        "earliest_bibcode",
        "earliest_publication_date",
    },
    # discovery_date_old only present when updating an existing date.
    "UpsertDiscoveryDateMetadata": {
        "nova_id",
        "updated",
        "discovery_date",
        "discovery_date_old",
    },
    # -- artifact_finalizer (services/artifact_finalizer/handler.py) ---------
    "UpdatePlanInProgress": {"plan_id", "status"},
    "Finalize": {"plan_id", "status", "novae_succeeded", "novae_failed"},
    "FailHandler": {"plan_id", "status"},
}


# ---------------------------------------------------------------------------
# ASL parsing helpers
# ---------------------------------------------------------------------------


def _load_asl(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text()))


def _workflow_name(path: Path) -> str:
    return path.stem


def _get_task_name(state: dict[str, Any]) -> str | None:
    """Extract the handler task_name from a Task state's Parameters."""
    result: str | None = state.get("Parameters", {}).get("task_name")
    return result


def _build_result_path_map(states: dict[str, Any]) -> dict[str, str]:
    """Map ResultPath prefix -> state name for Task states in a scope.

    Only includes explicit ResultPaths (not null, not ``$``).
    """
    mapping: dict[str, str] = {}
    for name, state in states.items():
        if state.get("Type") != "Task":
            continue
        rp = state.get("ResultPath")
        if rp is not None and rp != "$":
            mapping[rp] = name
    return mapping


def _extract_choice_variables(state: dict[str, Any]) -> list[str]:
    """Extract all Variable paths from a Choice state's rules."""
    paths: list[str] = []

    def _collect(condition: dict[str, Any]) -> None:
        if "Variable" in condition:
            paths.append(condition["Variable"])
        for sub in condition.get("And", []):
            _collect(sub)
        for sub in condition.get("Or", []):
            _collect(sub)
        if "Not" in condition:
            _collect(condition["Not"])

    for choice in state.get("Choices", []):
        _collect(choice)
    return paths


def _extract_dollar_refs(state: dict[str, Any]) -> list[str]:
    """Extract all ``.$`` reference paths from Parameters and ItemSelector."""
    paths: list[str] = []

    def _recurse(obj: object) -> None:
        if isinstance(obj, dict):
            for key, val in obj.items():
                if key.endswith(".$") and isinstance(val, str):
                    paths.append(val)
                else:
                    _recurse(val)
        elif isinstance(obj, list):
            for item in obj:
                _recurse(item)

    _recurse(state.get("Parameters", {}))
    _recurse(state.get("ItemSelector", {}))
    return paths


def _resolve_field_ref(
    path: str,
    result_path_map: dict[str, str],
) -> tuple[str, str] | None:
    """Resolve a JSONPath to ``(source_state_name, field_name)``.

    Returns ``None`` when the path does not reference a known ResultPath
    prefix (e.g. execution input, ``$$`` context, intrinsic functions, or
    a reference to the whole result object without a field suffix).
    """
    if not path.startswith("$.") or path.startswith("$$."):
        return None

    for rp, state_name in result_path_map.items():
        if path == rp:
            # Referencing the whole result object — no field to check.
            return None
        if path.startswith(rp + "."):
            remaining = path[len(rp) + 1 :]
            field = remaining.split(".")[0]
            return (state_name, field)
    return None


def _collect_scopes(
    asl: dict[str, Any],
) -> list[tuple[str, dict[str, Any], dict[str, str]]]:
    """Collect ``(scope_label, states_dict, result_path_map)`` for every
    scope in an ASL definition.

    Handles top-level states, Map Iterator states, and Parallel branch
    states. Parallel branches inherit the parent scope's ResultPath map.
    Map Iterators are independent scopes (input comes from ItemSelector).
    """
    top_states = asl["States"]
    top_rpm = _build_result_path_map(top_states)
    scopes: list[tuple[str, dict[str, Any], dict[str, str]]] = [
        ("top", top_states, top_rpm),
    ]

    for name, state in top_states.items():
        if state.get("Type") == "Map" and "Iterator" in state:
            iter_states = state["Iterator"]["States"]
            iter_rpm = _build_result_path_map(iter_states)
            scopes.append((f"Map:{name}", iter_states, iter_rpm))
        if state.get("Type") == "Parallel":
            for i, branch in enumerate(state.get("Branches", [])):
                branch_states = branch["States"]
                merged_rpm = {
                    **top_rpm,
                    **_build_result_path_map(branch_states),
                }
                scopes.append((f"Parallel:{name}[{i}]", branch_states, merged_rpm))

    return scopes


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

ASL_FILES = sorted(glob(str(WORKFLOWS_DIR / "*.asl.json")))
ASL_IDS = [Path(f).stem for f in ASL_FILES]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestChoiceStateContracts:
    """Every Choice state Variable path must exist in the preceding
    task's return shape."""

    @pytest.mark.parametrize("asl_file", ASL_FILES, ids=ASL_IDS)
    def test_choice_variables_match_handler_returns(self, asl_file: str) -> None:
        asl = _load_asl(Path(asl_file))
        wf = _workflow_name(Path(asl_file))
        violations: list[str] = []

        for scope_label, states, rpm in _collect_scopes(asl):
            for state_name, state in states.items():
                if state.get("Type") != "Choice":
                    continue
                for var_path in _extract_choice_variables(state):
                    if var_path.startswith("$$"):
                        continue
                    resolved = _resolve_field_ref(var_path, rpm)
                    if resolved is None:
                        continue
                    source_state, field = resolved
                    task_name = _get_task_name(states.get(source_state, {}))
                    if task_name is None:
                        continue
                    shape = TASK_RETURN_SHAPES.get(task_name)
                    if shape is None:
                        violations.append(
                            f"[{scope_label}] {state_name}: "
                            f"Variable {var_path!r} references task "
                            f"{source_state!r} (task_name={task_name!r}) "
                            f"which has no entry in TASK_RETURN_SHAPES"
                        )
                        continue
                    if field not in shape:
                        violations.append(
                            f"[{scope_label}] {state_name}: "
                            f"Variable {var_path!r} references field "
                            f"{field!r} not in {task_name!r} return "
                            f"shape {sorted(shape)}"
                        )

        assert not violations, f"Choice state contract violations in {wf}:\n" + "\n".join(
            f"  - {v}" for v in violations
        )


class TestTaskParameterReferences:
    """Every ``$.path`` reference in Task/Map Parameters must exist in the
    source state's return shape."""

    @pytest.mark.parametrize("asl_file", ASL_FILES, ids=ASL_IDS)
    def test_parameter_refs_match_handler_returns(self, asl_file: str) -> None:
        asl = _load_asl(Path(asl_file))
        wf = _workflow_name(Path(asl_file))
        violations: list[str] = []

        for scope_label, states, rpm in _collect_scopes(asl):
            for state_name, state in states.items():
                if state.get("Type") not in ("Task", "Map", "Parallel"):
                    continue
                for ref_path in _extract_dollar_refs(state):
                    if ref_path.startswith("$$") or ref_path.startswith("States."):
                        continue
                    resolved = _resolve_field_ref(ref_path, rpm)
                    if resolved is None:
                        continue
                    source_state, field = resolved
                    task_name = _get_task_name(states.get(source_state, {}))
                    if task_name is None:
                        # Source might be in the parent scope for Map
                        # ItemSelector refs — check top-level states.
                        task_name = _get_task_name(asl["States"].get(source_state, {}))
                    if task_name is None:
                        continue
                    shape = TASK_RETURN_SHAPES.get(task_name)
                    if shape is None:
                        violations.append(
                            f"[{scope_label}] {state_name}: "
                            f"param ref {ref_path!r} references task "
                            f"{source_state!r} (task_name={task_name!r}) "
                            f"which has no entry in TASK_RETURN_SHAPES"
                        )
                        continue
                    if field not in shape:
                        violations.append(
                            f"[{scope_label}] {state_name}: "
                            f"param ref {ref_path!r} references field "
                            f"{field!r} not in {task_name!r} return "
                            f"shape {sorted(shape)}"
                        )

        assert not violations, f"Task parameter reference violations in {wf}:\n" + "\n".join(
            f"  - {v}" for v in violations
        )


class TestRegistryCompleteness:
    """Every Lambda task referenced by an ASL must have an entry in
    ``TASK_RETURN_SHAPES`` so the contract tests above can verify it."""

    @pytest.mark.parametrize("asl_file", ASL_FILES, ids=ASL_IDS)
    def test_all_lambda_tasks_registered(self, asl_file: str) -> None:
        asl = _load_asl(Path(asl_file))
        wf = _workflow_name(Path(asl_file))
        missing: list[str] = []

        for scope_label, states, _ in _collect_scopes(asl):
            for state_name, state in states.items():
                if state.get("Type") != "Task":
                    continue
                task_name = _get_task_name(state)
                if task_name is None:
                    continue
                if task_name not in TASK_RETURN_SHAPES:
                    missing.append(f"[{scope_label}] {state_name}: task_name={task_name!r}")

        assert not missing, (
            f"Unregistered Lambda tasks in {wf} — add entries to "
            f"TASK_RETURN_SHAPES:\n" + "\n".join(f"  - {m}" for m in missing)
        )
