from botocore.stub import Stubber

from services.workflow_runtime.attempt import record_attempt_finished, record_attempt_started
from services.workflow_runtime.ddb import TableRef, dynamodb_client, to_ddb_item
from services.workflow_runtime.jobrun import begin_job_run, finalize_job_run


def test_begin_job_run_puts_expected_shape() -> None:
    client = dynamodb_client()
    table = TableRef(name="nova-table")

    expected_item = to_ddb_item(
        {
            "PK": "NOVA1",
            "SK": "JOBRUN#AcquireAndValidateSpectra#2026-02-23T18:10:00Z#jr1",
            "entity_type": "JobRun",
            "schema_version": "1",
            "job_run_id": "jr1",
            "workflow_name": "AcquireAndValidateSpectra",
            "execution_arn": "arn:states:1",
            "status": "RUNNING",
            "started_at": "2026-02-23T18:10:00Z",
            "correlation_id": "cid",
            "idempotency_key": "idem-123",
            "created_at": "2026-02-23T18:10:00Z",
            "updated_at": "2026-02-23T18:10:00Z",
            "data_product_id": "DP1",
        }
    )

    with Stubber(client) as stubber:
        stubber.add_response(
            "put_item",
            {},
            {
                "TableName": "nova-table",
                "Item": expected_item,
                "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
            },
        )

        job_run_id = begin_job_run(
            nova_id="NOVA1",
            workflow_name="AcquireAndValidateSpectra",
            execution_arn="arn:states:1",
            correlation_id="cid",
            idempotency_key="idem-123",
            identifiers={"data_product_id": "DP1"},
            started_at="2026-02-23T18:10:00Z",
            job_run_id="jr1",
            ddb=client,
            table=table,
        )

        assert job_run_id == "jr1"


def test_finalize_job_run_updates_with_condition() -> None:
    client = dynamodb_client()
    table = TableRef(name="nova-table")

    with Stubber(client) as stubber:
        stubber.add_response(
            "update_item",
            {},
            {
                "TableName": "nova-table",
                "Key": {
                    "PK": {"S": "NOVA1"},
                    "SK": {"S": "JOBRUN#AcquireAndValidateSpectra#2026-02-23T18:10:00Z#jr1"},
                },
                "UpdateExpression": "SET #st = :st, ended_at = :ended_at, updated_at = :updated_at, outcome = :outcome, summary = :summary",
                "ExpressionAttributeNames": {"#st": "status"},
                "ExpressionAttributeValues": {
                    ":st": {"S": "SUCCEEDED"},
                    ":ended_at": {"S": "2026-02-23T18:12:00Z"},
                    ":updated_at": {"S": "2026-02-23T18:12:00Z"},
                    ":outcome": {"S": "ok"},
                    ":summary": {"M": {"items": {"N": "3"}}},
                },
                "ConditionExpression": "attribute_not_exists(ended_at)",
            },
        )

        finalize_job_run(
            nova_id="NOVA1",
            workflow_name="AcquireAndValidateSpectra",
            started_at="2026-02-23T18:10:00Z",
            job_run_id="jr1",
            status="SUCCEEDED",
            outcome="ok",
            ended_at="2026-02-23T18:12:00Z",
            summary_fields={"items": 3},
            ddb=client,
            table=table,
        )


def test_attempt_start_put_item_shape() -> None:
    client = dynamodb_client()
    table = TableRef(name="nova-table")

    expected_item = to_ddb_item(
        {
            "PK": "NOVA1",
            "SK": "ATTEMPT#jr1#download_bytes#1#2026-02-23T18:10:10Z",
            "entity_type": "Attempt",
            "schema_version": "1",
            "job_run_id": "jr1",
            "task_name": "download_bytes",
            "attempt_no": 1,
            "status": "STARTED",
            "started_at": "2026-02-23T18:10:10Z",
            "created_at": "2026-02-23T18:10:10Z",
            "updated_at": "2026-02-23T18:10:10Z",
            "reference_id": "R1",
        }
    )

    with Stubber(client) as stubber:
        stubber.add_response(
            "put_item",
            {},
            {
                "TableName": "nova-table",
                "Item": expected_item,
                "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
            },
        )

        sk = record_attempt_started(
            nova_id="NOVA1",
            job_run_id="jr1",
            task_name="download_bytes",
            attempt_no=1,
            started_at="2026-02-23T18:10:10Z",
            identifiers={"reference_id": "R1"},
            ddb=client,
            table=table,
        )

        assert sk == "ATTEMPT#jr1#download_bytes#1#2026-02-23T18:10:10Z"


def test_attempt_finish_update_item_shape() -> None:
    client = dynamodb_client()
    table = TableRef(name="nova-table")

    with Stubber(client) as stubber:
        stubber.add_response(
            "update_item",
            {},
            {
                "TableName": "nova-table",
                "Key": {
                    "PK": {"S": "NOVA1"},
                    "SK": {"S": "ATTEMPT#jr1#download_bytes#1#2026-02-23T18:10:10Z"},
                },
                "UpdateExpression": "SET #st = :st, finished_at = :finished_at, duration_ms = :duration_ms, updated_at = :updated_at, error = :error",
                "ExpressionAttributeNames": {"#st": "status"},
                "ExpressionAttributeValues": {
                    ":st": {"S": "FAILED"},
                    ":finished_at": {"S": "2026-02-23T18:10:20Z"},
                    ":duration_ms": {"N": "123"},
                    ":updated_at": {"S": "2026-02-23T18:10:20Z"},
                    ":error": {
                        "M": {
                            "error_classification": {"S": "RETRYABLE"},
                            "error_fingerprint": {"S": "abc"},
                        }
                    },
                },
                "ConditionExpression": "attribute_exists(PK) AND attribute_exists(SK)",
            },
        )

        record_attempt_finished(
            nova_id="NOVA1",
            attempt_sk="ATTEMPT#jr1#download_bytes#1#2026-02-23T18:10:10Z",
            status="FAILED",
            duration_ms=123,
            ended_at="2026-02-23T18:10:20Z",
            error_fields={"error_classification": "RETRYABLE", "error_fingerprint": "abc"},
            ddb=client,
            table=table,
        )
