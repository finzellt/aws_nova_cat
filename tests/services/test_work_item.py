"""Unit tests for nova_common.work_item.

Coverage
--------
DirtyType:
  - All three values present: spectra, photometry, references

write_work_item:
  - Happy path: item written with correct PK, SK structure, fields, and TTL
  - SK ordering: nova_id → dirty_type → created_at
  - TTL is approximately 30 days from now
  - Best-effort: DDB failure logs warning but does not raise
  - All DirtyType values produce valid items
"""

from __future__ import annotations

import logging
import time
from typing import Any
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from nova_common.work_item import (
    _TTL_DAYS,
    _WORKQUEUE_PK,
    DirtyType,
    write_work_item,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NOVA_ID = "aaaaaaaa-0000-0000-0000-000000000001"
_JOB_RUN_ID = "bbbbbbbb-1111-1111-1111-000000000001"
_CORRELATION_ID = "cccccccc-2222-2222-2222-000000000001"
_SOURCE_WORKFLOW = "acquire_and_validate_spectra"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_table() -> MagicMock:
    return MagicMock()


def _call_write(
    table: Any = None,
    *,
    dirty_type: DirtyType = DirtyType.spectra,
    source_workflow: str = _SOURCE_WORKFLOW,
) -> Any:
    """Call write_work_item with standard args, return the mock table."""
    if table is None:
        table = _mock_table()

    write_work_item(
        table,
        nova_id=_NOVA_ID,
        dirty_type=dirty_type,
        source_workflow=source_workflow,
        job_run_id=_JOB_RUN_ID,
        correlation_id=_CORRELATION_ID,
    )
    return table


def _get_written_item(table: MagicMock) -> Any:
    """Extract the Item dict from the first put_item call."""
    table.put_item.assert_called_once()
    return table.put_item.call_args[1]["Item"]


# ---------------------------------------------------------------------------
# DirtyType enum
# ---------------------------------------------------------------------------


class TestDirtyType:
    def test_all_values_present(self) -> None:
        assert DirtyType.spectra.value == "spectra"
        assert DirtyType.photometry.value == "photometry"
        assert DirtyType.references.value == "references"

    def test_exactly_three_members(self) -> None:
        assert len(DirtyType) == 3


# ---------------------------------------------------------------------------
# write_work_item — happy path
# ---------------------------------------------------------------------------


class TestWriteWorkItemHappyPath:
    def test_pk_is_workqueue(self) -> None:
        table = _call_write()
        item = _get_written_item(table)
        assert item["PK"] == _WORKQUEUE_PK

    def test_sk_starts_with_nova_id(self) -> None:
        table = _call_write()
        item = _get_written_item(table)
        assert item["SK"].startswith(f"{_NOVA_ID}#")

    def test_sk_contains_dirty_type(self) -> None:
        table = _call_write(dirty_type=DirtyType.photometry)
        item = _get_written_item(table)
        parts = item["SK"].split("#")
        assert parts[1] == "photometry"

    def test_sk_ordering_nova_dirty_timestamp(self) -> None:
        """SK = <nova_id>#<dirty_type>#<created_at> per DESIGN-003 §3.2."""
        table = _call_write()
        item = _get_written_item(table)
        parts = item["SK"].split("#")
        assert len(parts) == 3
        assert parts[0] == _NOVA_ID
        assert parts[1] == "spectra"
        # parts[2] is the ISO timestamp

    def test_entity_type(self) -> None:
        table = _call_write()
        item = _get_written_item(table)
        assert item["entity_type"] == "WorkItem"

    def test_nova_id_field(self) -> None:
        table = _call_write()
        item = _get_written_item(table)
        assert item["nova_id"] == _NOVA_ID

    def test_dirty_type_field(self) -> None:
        table = _call_write(dirty_type=DirtyType.references)
        item = _get_written_item(table)
        assert item["dirty_type"] == "references"

    def test_source_workflow_field(self) -> None:
        table = _call_write(source_workflow="ingest_ticket")
        item = _get_written_item(table)
        assert item["source_workflow"] == "ingest_ticket"

    def test_job_run_id_field(self) -> None:
        table = _call_write()
        item = _get_written_item(table)
        assert item["job_run_id"] == _JOB_RUN_ID

    def test_correlation_id_field(self) -> None:
        table = _call_write()
        item = _get_written_item(table)
        assert item["correlation_id"] == _CORRELATION_ID

    def test_created_at_is_iso_utc(self) -> None:
        table = _call_write()
        item = _get_written_item(table)
        assert item["created_at"].endswith("Z")

    def test_ttl_approximately_30_days_from_now(self) -> None:
        table = _call_write()
        item = _get_written_item(table)
        expected_ttl = int(time.time()) + (_TTL_DAYS * 86_400)
        # Allow 10 seconds of clock drift
        assert abs(item["ttl"] - expected_ttl) < 10


# ---------------------------------------------------------------------------
# write_work_item — all dirty types
# ---------------------------------------------------------------------------


class TestWriteWorkItemAllDirtyTypes:
    @pytest.mark.parametrize("dirty_type", list(DirtyType))
    def test_each_dirty_type_writes_successfully(self, dirty_type: DirtyType) -> None:
        table = _call_write(dirty_type=dirty_type)
        item = _get_written_item(table)
        assert item["dirty_type"] == dirty_type.value


# ---------------------------------------------------------------------------
# write_work_item — best-effort failure handling
# ---------------------------------------------------------------------------


class TestWriteWorkItemBestEffort:
    def test_ddb_failure_does_not_raise(self) -> None:
        """A DynamoDB error is swallowed — write_work_item never raises."""
        table = _mock_table()
        table.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "mocked"}},
            "PutItem",
        )

        # Must not raise
        write_work_item(
            table,
            nova_id=_NOVA_ID,
            dirty_type=DirtyType.spectra,
            source_workflow=_SOURCE_WORKFLOW,
            job_run_id=_JOB_RUN_ID,
            correlation_id=_CORRELATION_ID,
        )

    def test_ddb_failure_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """A DynamoDB error produces a warning log line."""
        table = _mock_table()
        table.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "mocked"}},
            "PutItem",
        )

        with caplog.at_level(logging.WARNING, logger="nova_common.work_item"):
            write_work_item(
                table,
                nova_id=_NOVA_ID,
                dirty_type=DirtyType.spectra,
                source_workflow=_SOURCE_WORKFLOW,
                job_run_id=_JOB_RUN_ID,
                correlation_id=_CORRELATION_ID,
            )

        assert len(caplog.records) == 1
        assert "Failed to write WorkItem" in caplog.records[0].message

    def test_arbitrary_exception_does_not_raise(self) -> None:
        """Even unexpected exceptions (TypeError, etc.) are swallowed."""
        table = _mock_table()
        table.put_item.side_effect = TypeError("unexpected")

        write_work_item(
            table,
            nova_id=_NOVA_ID,
            dirty_type=DirtyType.spectra,
            source_workflow=_SOURCE_WORKFLOW,
            job_run_id=_JOB_RUN_ID,
            correlation_id=_CORRELATION_ID,
        )
