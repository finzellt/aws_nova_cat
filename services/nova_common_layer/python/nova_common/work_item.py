"""work_item — best-effort WorkItem creation for the regeneration pipeline.

Implements ADR-031 Decision 7 (DESIGN-003 §3.2–§3.3): ingestion workflows
write WorkItems to a ``WORKQUEUE`` partition after scientific data is
persisted, signalling to the artifact regeneration pipeline which novae
have new data.

WorkItems are additive work orders, not boolean dirty flags. Each ingestion
event produces a discrete item. The coordinator (DESIGN-003 §4) consumes
them to build per-nova regeneration manifests, then deletes them on success.

Key structure (DESIGN-003 §3.2)::

    PK = "WORKQUEUE"
    SK = "<nova_id>#<dirty_type>#<created_at>"

The sort key orders items by nova → dirty_type → timestamp, so the
coordinator can derive per-nova manifests directly from the key structure.

This module requires no heavy dependencies — only boto3 and stdlib. It is
safe to import from any Lambda (zip-bundled or Docker-based).

Public API
----------
write_work_item(table, nova_id, dirty_type, source_workflow, job_run_id,
                correlation_id) -> None
    Best-effort PutItem. Logs warning on failure; never raises.

DirtyType
    String enum: "spectra", "photometry", "references".
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from enum import Enum
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WORKQUEUE_PK = "WORKQUEUE"
_ENTITY_TYPE = "WorkItem"
_SCHEMA_VERSION = "1.0.0"
_TTL_DAYS = 30
_SECONDS_PER_DAY = 86_400


# ---------------------------------------------------------------------------
# DirtyType enum
# ---------------------------------------------------------------------------


class DirtyType(str, Enum):
    """Artifact dirty types (DESIGN-003 §3.4 dependency matrix)."""

    spectra = "spectra"
    photometry = "photometry"
    references = "references"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

_logger = logging.getLogger(__name__)


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with 'Z' suffix."""
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _ttl_epoch(created_at: datetime) -> int:
    """Return a Unix epoch timestamp 30 days after *created_at*."""
    return int(created_at.timestamp()) + (_TTL_DAYS * _SECONDS_PER_DAY)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def write_work_item(
    table: Any,
    *,
    nova_id: str,
    dirty_type: DirtyType,
    source_workflow: str,
    job_run_id: str,
    correlation_id: str,
) -> None:
    """Write a WorkItem to the WORKQUEUE partition (best-effort).

    This function catches all exceptions and logs a warning rather than
    raising. A missed WorkItem means the nova's artifacts are not
    regenerated until the next ingestion event or a manual operator
    trigger — acceptable at MVP scale (DESIGN-003 §3.3).

    Parameters
    ----------
    table:
        Injected boto3 DynamoDB Table resource (main NovaCat table).
    nova_id:
        Nova UUID string.
    dirty_type:
        Which artifact domain changed: spectra, photometry, or references.
    source_workflow:
        Name of the workflow writing this item (e.g.
        ``"acquire_and_validate_spectra"``, ``"ingest_ticket"``,
        ``"refresh_references"``).
    job_run_id:
        JobRun UUID string — audit trail back to the specific ingestion run.
    correlation_id:
        Cross-workflow tracing identifier.
    """
    now = datetime.now(UTC)
    created_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    sk = f"{nova_id}#{dirty_type.value}#{created_at}"

    try:
        table.put_item(
            Item={
                "PK": _WORKQUEUE_PK,
                "SK": sk,
                "entity_type": _ENTITY_TYPE,
                "schema_version": _SCHEMA_VERSION,
                "nova_id": nova_id,
                "dirty_type": dirty_type.value,
                "source_workflow": source_workflow,
                "job_run_id": job_run_id,
                "correlation_id": correlation_id,
                "created_at": created_at,
                "ttl": _ttl_epoch(now),
            }
        )
    except Exception:
        _logger.warning(
            "Failed to write WorkItem — regeneration pipeline will not detect this "
            "change until the next ingestion event or a manual operator trigger",
            exc_info=True,
            extra={
                "nova_id": nova_id,
                "dirty_type": dirty_type.value,
                "source_workflow": source_workflow,
                "job_run_id": job_run_id,
            },
        )
