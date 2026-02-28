from typing import Any

from botocore.stub import ANY

from services.workflow_runtime.ddb import TableRef
from services.workflow_runtime.idempotency import acquire_lock, release_lock


def test_acquire_lock_first_wins_second_loses(ddb_stubber: Any) -> None:
    client, stubber = ddb_stubber
    table = TableRef(name="nova-table")

    # First put succeeds.
    stubber.add_response(
        "put_item",
        {},
        {
            "TableName": "nova-table",
            "Item": ANY,
            "ConditionExpression": "attribute_not_exists(PK)",
        },
    )

    # Second put fails with conditional check.
    stubber.add_client_error(
        "put_item",
        service_error_code="ConditionalCheckFailedException",
        service_message="exists",
        http_status_code=400,
        expected_params={
            "TableName": "nova-table",
            "Item": ANY,
            "ConditionExpression": "attribute_not_exists(PK)",
        },
    )

    assert acquire_lock("k1", 60, now_epoch=1, ddb=client, table=table) is True
    assert acquire_lock("k1", 60, now_epoch=2, ddb=client, table=table) is False


def test_release_lock_calls_delete(ddb_stubber: Any) -> None:
    client, stubber = ddb_stubber
    table = TableRef(name="nova-table")

    stubber.add_response(
        "delete_item",
        {},
        {
            "TableName": "nova-table",
            "Key": {"PK": {"S": "LOCK#k1"}, "SK": {"S": "LOCK"}},
        },
    )

    release_lock("k1", ddb=client, table=table)
