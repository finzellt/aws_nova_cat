"""
Minimal DynamoDB client helpers for workflow runtime primitives.

This module provides thin wrappers around the low-level boto3 DynamoDB
client for use by JobRun, Attempt, and idempotency lock primitives.

Design principles:

- Uses low-level client API for precise control and testability.
- Performs minimal attribute serialization.
- Does not implement domain access patterns.
- Does not assume full repository access patterns.

All operational records are written to the table defined by
NOVA_DDB_TABLE_NAME.

This module should remain small and focused strictly on runtime persistence.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from .config import ddb_table_name

_serializer = TypeSerializer()
_deserializer = TypeDeserializer()


def dynamodb_client() -> Any:
    """Create a low-level DynamoDB client.

    We intentionally use the low-level client API for precise request-shape
    control and fast unit testing via botocore Stubber.
    """

    return boto3.client("dynamodb")


def to_ddb_item(item: Mapping[str, Any]) -> dict[str, Any]:
    """Convert a Python dict to DynamoDB AttributeValue map."""
    return {k: _serializer.serialize(v) for k, v in item.items()}


def to_ddb_value(value: Any) -> Any:
    """Serialize a single Python value to a DynamoDB AttributeValue."""

    return _serializer.serialize(value)


def from_ddb_item(item: Mapping[str, Any]) -> dict[str, Any]:
    return {k: _deserializer.deserialize(v) for k, v in item.items()}


@dataclass(frozen=True)
class TableRef:
    name: str


def table_ref() -> TableRef:
    return TableRef(name=ddb_table_name())
