from botocore.exceptions import ClientError

from services.workflow_runtime.errors import (
    ErrorClassification,
    SuspectDataError,
    ValidationError,
    classify_exception,
    fingerprint_error,
)


def test_fingerprint_stable_for_same_message() -> None:
    a = fingerprint_error("hello   world")
    b = fingerprint_error("hello world")
    assert a == b


def test_fingerprint_stable_for_same_exception_type_and_message() -> None:
    e1 = RuntimeError("boom")
    e2 = RuntimeError("boom")
    assert fingerprint_error(e1) == fingerprint_error(e2)


def test_classify_suspect_data_quarantine() -> None:
    cls, fp, msg = classify_exception(SuspectDataError("bad photometry"))
    assert cls == ErrorClassification.QUARANTINE
    assert len(fp) == 16
    assert "bad photometry" in msg


def test_classify_validation_terminal() -> None:
    cls, fp, _ = classify_exception(ValidationError("nope"))
    assert cls == ErrorClassification.TERMINAL


def test_classify_throttling_retryable() -> None:
    err = ClientError(
        error_response={
            "Error": {"Code": "ThrottlingException", "Message": "slow down"},
            "ResponseMetadata": {
                "RequestId": "req",
                "HostId": "host",
                "HTTPStatusCode": 400,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            },
        },
        operation_name="PutItem",
    )
    cls, fp, _ = classify_exception(err)
    assert cls == ErrorClassification.RETRYABLE
    assert len(fp) == 16
