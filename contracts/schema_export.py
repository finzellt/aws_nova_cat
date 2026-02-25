# contracts/schema_export.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel

from contracts.models.entities import (
    Attempt,
    DataProduct,
    FileObject,
    JobRun,
    LocatorAlias,
    NameMapping,
    Nova,
    NovaReference,
    Reference,
)
from contracts.models.events import (
    AcquireAndValidateSpectraEvent,
    DiscoverSpectraProductsEvent,
    IngestNewNovaEvent,
    IngestPhotometryEvent,
    InitializeNovaEvent,
    NameCheckAndReconcileEvent,
    RefreshReferencesEvent,
)

SCHEMAS_ROOT = Path("schemas")


@dataclass(frozen=True)
class SchemaTarget:
    kind: str  # "entities" or "events"
    name: str
    model: type[BaseModel]


TARGETS: list[SchemaTarget] = [
    # ----------------------------
    # Entities (persistent contracts)
    # ----------------------------
    SchemaTarget("entities", "nova", Nova),
    SchemaTarget("entities", "name_mapping", NameMapping),
    SchemaTarget("entities", "locator_alias", LocatorAlias),
    SchemaTarget("entities", "data_product", DataProduct),
    SchemaTarget("entities", "file_object", FileObject),
    SchemaTarget("entities", "reference", Reference),
    SchemaTarget("entities", "nova_reference", NovaReference),
    SchemaTarget("entities", "job_run", JobRun),
    SchemaTarget("entities", "attempt", Attempt),
    # ----------------------------
    # Events (workflow boundary contracts)
    # ----------------------------
    SchemaTarget("events", "initialize_nova", InitializeNovaEvent),
    SchemaTarget("events", "ingest_new_nova", IngestNewNovaEvent),
    SchemaTarget("events", "refresh_references", RefreshReferencesEvent),
    SchemaTarget("events", "discover_spectra_products", DiscoverSpectraProductsEvent),
    SchemaTarget("events", "acquire_and_validate_spectra", AcquireAndValidateSpectraEvent),
    SchemaTarget("events", "ingest_photometry", IngestPhotometryEvent),
    SchemaTarget("events", "name_check_and_reconcile", NameCheckAndReconcileEvent),
]


def _version(model: type[BaseModel]) -> str:
    """
    Determine contract version for filename.

    - Entities use `schema_version`
    - Events use `event_version`
    """
    field = model.model_fields.get("schema_version") or model.model_fields.get("event_version")
    if field is None:
        raise RuntimeError(f"{model.__name__} must define schema_version or event_version.")
    default = field.default
    if not isinstance(default, str) or not default:
        raise RuntimeError(f"{model.__name__} version default must be a non-empty string.")
    return default


def export_all() -> None:
    for t in TARGETS:
        version = _version(t.model)
        out_dir = SCHEMAS_ROOT / t.kind / t.name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{version}.json"

        schema = t.model.model_json_schema()

        # Canonical "$id" for tooling / traceability
        schema["$id"] = f"nova-cat://schemas/{t.kind}/{t.name}/{version}"

        out_path.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    export_all()
