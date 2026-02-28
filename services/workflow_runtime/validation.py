from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeVar
from uuid import uuid4

from pydantic import BaseModel, ValidationError

TModel = TypeVar("TModel", bound=BaseModel)


def _sha256_hex(payload: Mapping[str, Any]) -> str:
    """
    Compute a stable-ish sha256 fingerprint from a JSON-serializable payload.
    """
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
    return hashlib.sha256(raw).hexdigest()


def _safe_error_details(err: ValidationError) -> list[dict[str, Any]]:
    """
    Return a minimal, stable subset of Pydantic error details suitable for logs and fingerprints.
    """
    details: list[dict[str, Any]] = []
    for e in err.errors():
        details.append(
            {
                "loc": list(e.get("loc", ())),
                "type": e.get("type"),
                "msg": e.get("msg"),
            }
        )
    return details


@dataclass(frozen=True, slots=True)
class ContractValidationError(Exception):
    """
    Standardized exception for boundary contract failures.

    This is intended to plug into Epic-7 error taxonomy handling:
      - classification is always TERMINAL for contract validation failures
      - error_fingerprint is a sha256 hex string
    """

    message: str
    classification: str
    error_fingerprint: str
    correlation_id: str
    details: list[dict[str, Any]] | None = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


def validate_enveloped_input(envelope: dict[str, Any], model: type[TModel]) -> TModel:
    """
    Validate an Epic-7 internal-task envelope input payload against a Pydantic contract model.

    Envelope shape is preserved:
      { "input": {...}, "context": {...} }
    """
    if not isinstance(envelope, dict):
        raise ContractValidationError(
            message="Envelope must be a dict.",
            classification="TERMINAL",
            error_fingerprint=_sha256_hex({"kind": "envelope_type", "expected": "dict"}),
            correlation_id=str(uuid4()),
            details=None,
        )

    raw_input = envelope.get("input")
    if not isinstance(raw_input, dict):
        raise ContractValidationError(
            message='Envelope["input"] must be a dict.',
            classification="TERMINAL",
            error_fingerprint=_sha256_hex({"kind": "input_type", "expected": "dict"}),
            correlation_id=str(uuid4()),
            details=None,
        )

    # Correlation handling:
    # - If input has correlation_id, use it.
    # - If it doesn't, allow model defaults to generate it on success.
    # - On failure, still provide a correlation_id for error plumbing/logging.
    correlation_id = raw_input.get("correlation_id")
    correlation_for_error = str(correlation_id) if correlation_id is not None else str(uuid4())

    try:
        return model.model_validate(raw_input)
    except ValidationError as err:
        details = _safe_error_details(err)
        fingerprint = _sha256_hex(
            {"kind": "contract_validation", "model": model.__name__, "errors": details}
        )
        raise ContractValidationError(
            message=f"Contract validation failed for {model.__name__}.",
            classification="TERMINAL",
            error_fingerprint=fingerprint,
            correlation_id=correlation_for_error,
            details=details,
        ) from err


def get_model_for_workflow(workflow_name: str) -> type[BaseModel]:
    """
    Workflow-name -> boundary event contract model mapping.

    Keep this mapping explicit and small; extend it as new workflows are introduced.
    """
    # Local import avoids potential import cycles and keeps module import light.
    from contracts.models import events as event_models

    registry: dict[str, type[BaseModel]] = {
        # Preferred snake_case workflow names.
        "initialize_nova": event_models.InitializeNovaEvent,
        "ingest_new_nova": event_models.IngestNewNovaEvent,
        "refresh_references": event_models.RefreshReferencesEvent,
        "discover_spectra_products": event_models.DiscoverSpectraProductsEvent,
        "acquire_and_validate_spectra": event_models.AcquireAndValidateSpectraEvent,
        "ingest_photometry": event_models.IngestPhotometryEvent,
        "name_check_and_reconcile": event_models.NameCheckAndReconcileEvent,
        # Optional convenience aliases (class-name keys).
        "InitializeNovaEvent": event_models.InitializeNovaEvent,
        "IngestNewNovaEvent": event_models.IngestNewNovaEvent,
        "RefreshReferencesEvent": event_models.RefreshReferencesEvent,
        "DiscoverSpectraProductsEvent": event_models.DiscoverSpectraProductsEvent,
        "AcquireAndValidateSpectraEvent": event_models.AcquireAndValidateSpectraEvent,
        "IngestPhotometryEvent": event_models.IngestPhotometryEvent,
        "NameCheckAndReconcileEvent": event_models.NameCheckAndReconcileEvent,
    }

    if workflow_name in registry:
        return registry[workflow_name]

    fingerprint = _sha256_hex({"kind": "unknown_workflow", "workflow_name": workflow_name})
    raise ContractValidationError(
        message=f"Unknown workflow_name '{workflow_name}'. No contract model is registered.",
        classification="TERMINAL",
        error_fingerprint=fingerprint,
        correlation_id=str(uuid4()),
        details=None,
    )
