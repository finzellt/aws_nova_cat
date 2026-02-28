from __future__ import annotations

import json
import re
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

from services.workflow_runtime.validation import (
    ContractValidationError,
    get_model_for_workflow,
    validate_enveloped_input,
)

FIXTURE_ROOT = Path("contracts/fixtures/events")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return cast(dict[str, Any], data)


def _envelope(input_payload: dict[str, Any]) -> dict[str, Any]:
    return {"input": input_payload, "context": {"request_id": "unit-test"}}


def _event_models_module() -> ModuleType:
    # Import inside a function to keep test discovery light.
    from contracts.models import events as event_models

    return event_models


def _iter_event_fixture_dirs() -> list[Path]:
    assert FIXTURE_ROOT.exists(), f"Fixture root does not exist: {FIXTURE_ROOT}"
    return sorted([p for p in FIXTURE_ROOT.iterdir() if p.is_dir()])


def test_all_valid_fixtures_validate() -> None:
    event_models = _event_models_module()

    for event_dir in _iter_event_fixture_dirs():
        valid_path = event_dir / "valid.json"
        assert valid_path.exists(), f"Missing valid fixture: {valid_path}"

        model_name = event_dir.name
        model = getattr(event_models, model_name)
        payload = _load_json(valid_path)

        result = validate_enveloped_input(_envelope(payload), model)
        assert isinstance(result, model), (
            f"{model_name}: expected model instance, got {type(result)}"
        )


def test_all_invalid_fixtures_raise_terminal_contract_error() -> None:
    event_models = _event_models_module()

    for event_dir in _iter_event_fixture_dirs():
        model_name = event_dir.name
        model = getattr(event_models, model_name)

        invalid_paths = [
            event_dir / "invalid_missing_required.json",
            event_dir / "invalid_wrong_type.json",
        ]

        # IngestPhotometryEvent has an extra validator-based invalid fixture.
        extra_invalid = event_dir / "invalid_validator_neither_id_nor_name.json"
        if extra_invalid.exists():
            invalid_paths.append(extra_invalid)

        for path in invalid_paths:
            assert path.exists(), f"Missing invalid fixture: {path}"
            payload = _load_json(path)

            with pytest.raises(ContractValidationError) as excinfo:
                validate_enveloped_input(_envelope(payload), model)

            err = excinfo.value
            assert err.classification == "TERMINAL", (
                f"{model_name}/{path.name}: wrong classification {err.classification}"
            )
            assert err.error_fingerprint, f"{model_name}/{path.name}: missing error_fingerprint"
            assert SHA256_RE.match(err.error_fingerprint), (
                f"{model_name}/{path.name}: fingerprint not sha256 hex: {err.error_fingerprint}"
            )


def test_extra_fields_rejected_by_eventbase_extra_forbid() -> None:
    event_models = _event_models_module()

    # One representative model; EventBase is shared.
    model = event_models.InitializeNovaEvent
    payload = _load_json(FIXTURE_ROOT / "InitializeNovaEvent" / "valid.json")
    payload["unexpected_field"] = "nope"

    with pytest.raises(ContractValidationError) as excinfo:
        validate_enveloped_input(_envelope(payload), model)

    err = excinfo.value
    assert err.classification == "TERMINAL"
    assert SHA256_RE.match(err.error_fingerprint)
    assert err.details, "Expected pydantic error details to be present for contract failures."


def test_ingest_photometry_validator_requires_nova_id_or_candidate_name() -> None:
    event_models = _event_models_module()
    model = event_models.IngestPhotometryEvent

    path = FIXTURE_ROOT / "IngestPhotometryEvent" / "invalid_validator_neither_id_nor_name.json"
    payload = _load_json(path)

    with pytest.raises(ContractValidationError) as excinfo:
        validate_enveloped_input(_envelope(payload), model)

    err = excinfo.value
    assert err.classification == "TERMINAL"
    assert SHA256_RE.match(err.error_fingerprint)


def test_registry_mapping_known_and_unknown_workflows() -> None:
    # Known
    model = get_model_for_workflow("initialize_nova")
    assert model.__name__ == "InitializeNovaEvent"

    # Unknown
    with pytest.raises(ContractValidationError) as excinfo:
        get_model_for_workflow("totally_unknown_workflow")

    err = excinfo.value
    assert err.classification == "TERMINAL"
    assert SHA256_RE.match(err.error_fingerprint)
