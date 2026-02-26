from __future__ import annotations

import os
from typing import Any

import boto3

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_dynamodb = boto3.resource("dynamodb")
table = _dynamodb.Table(_TABLE_NAME)


def put_item(item: dict[str, Any]) -> None:
    table.put_item(Item=item)


def get_item(pk: str, sk: str) -> dict[str, Any] | None:
    resp: dict[str, Any] = table.get_item(Key={"PK": pk, "SK": sk})
    item = resp.get("Item")
    if item is None:
        return None
    if not isinstance(item, dict):
        return None
    return item
