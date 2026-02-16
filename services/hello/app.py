from __future__ import annotations

from typing import Any


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN001, ANN201
    name = None
    if isinstance(event, dict):
        name = event.get("name")
    return {"ok": True, "message": f"hello {name or 'world'}"}
