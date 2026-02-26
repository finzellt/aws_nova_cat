from __future__ import annotations

import os

import boto3

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]

_dynamodb = boto3.resource("dynamodb")
table = _dynamodb.Table(_TABLE_NAME)


def put_item(item: dict) -> None:
    table.put_item(Item=item)


def get_item(pk: str, sk: str) -> dict | None:
    resp = table.get_item(Key={"PK": pk, "SK": sk})
    return resp.get("Item")
