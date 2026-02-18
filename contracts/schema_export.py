# contracts/schema_export.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel

from contracts.models.entities import (
    Attempt,
    Dataset,
    FileObject,
    JobRun,
    Nova,
    NovaReference,
    Reference,
)
from contracts.models.events import (
    DiscoverSpectraProductsEvent,
    DownloadAndValidateSpectraEvent,
    IngestNewNovaEvent,
    IngestPhotometryDatasetEvent,
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
    # Entities
    SchemaTarget("entities", "nova", Nova),
    SchemaTarget("entities", "dataset", Dataset),
    SchemaTarget("entities", "file_object", FileObject),
    SchemaTarget("entities", "reference", Reference),
    SchemaTarget("entities", "nova_reference", NovaReference),
    SchemaTarget("entities", "job_run", JobRun),
    SchemaTarget("entities", "attempt", Attempt),
    # Events
    SchemaTarget("events", "initialize_nova", InitializeNovaEvent),
    SchemaTarget("events", "ingest_new_nova", IngestNewNovaEvent),
    SchemaTarget("events", "refresh_papers", RefreshReferencesEvent),
    SchemaTarget("events", "discover_spectra_products", DiscoverSpectraProductsEvent),
    SchemaTarget("events", "download_and_validate_spectra", DownloadAndValidateSpectraEvent),
    SchemaTarget("events", "ingest_photometry_dataset", IngestPhotometryDatasetEvent),
    SchemaTarget("events", "name_check_and_reconcile", NameCheckAndReconcileEvent),
]


def _schema_version(model: type[BaseModel]) -> str:
    # Pydantic v2: we can look at the default field value without instantiating.
    field = model.model_fields.get("schema_version") or model.model_fields.get("event_version")
    if field is None:
        raise RuntimeError(f"{model.__name__} must define schema_version or event_version.")
    default = field.default
    if not isinstance(default, str) or not default:
        raise RuntimeError(f"{model.__name__} version default must be a non-empty string.")
    return default


def export_all() -> None:
    for t in TARGETS:
        version = _schema_version(t.model)
        out_dir = SCHEMAS_ROOT / t.kind / t.name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{version}.json"

        schema = t.model.model_json_schema()

        # Optional: enforce stable keys and a canonical "$id" for tooling
        schema["$id"] = f"nova-cat://schemas/{t.kind}/{t.name}/{version}"

        out_path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    export_all()
