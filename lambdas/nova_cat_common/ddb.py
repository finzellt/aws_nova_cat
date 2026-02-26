from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, cast

import boto3  # type: ignore[import-untyped]

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]

_dynamodb = boto3.resource("dynamodb")
table = _dynamodb.Table(_TABLE_NAME)


def put_item(item: dict[str, Any]) -> None:
    table.put_item(Item=item)


def get_item(pk: str, sk: str) -> dict[str, Any] | None:
    # With boto3-stubs installed, this is a TypedDict-like shape.
    resp: Mapping[str, Any] = table.get_item(Key={"PK": pk, "SK": sk})
    item = resp.get("Item")
    if item is None:
        return None
    if not isinstance(item, dict):
        return None
    # Narrow to the type we want to expose from this wrapper.
    return cast(dict[str, Any], item)
