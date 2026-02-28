from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from botocore.stub import Stubber


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: Any) -> None:
    # Ensure boto3 doesn't try to hit real AWS.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def ddb_stubber(monkeypatch: Any) -> Iterator[tuple[Any, Stubber]]:
    monkeypatch.setenv("NOVA_DDB_TABLE_NAME", "nova-table")
    client = boto3.client("dynamodb")
    stubber = Stubber(client)
    stubber.activate()
    try:
        yield client, stubber
    finally:
        stubber.deactivate()


@pytest.fixture
def log_level_env(monkeypatch: Any) -> None:
    monkeypatch.setenv("LOG_LEVEL", "INFO")
