from __future__ import annotations

import json
import logging
import os
from typing import Any

_LOG_LEVEL = os.environ.get("NOVA_CAT_LOG_LEVEL", "INFO").upper()


class JsonLogger:
    def __init__(self, name: str = "nova_cat") -> None:
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            self._logger.addHandler(handler)
        self._logger.setLevel(_LOG_LEVEL)

    def info(self, msg: str, extra: dict[str, Any] | None = None) -> None:
        self._logger.info(self._fmt(msg, extra))

    def exception(self, msg: str, extra: dict[str, Any] | None = None) -> None:
        self._logger.exception(self._fmt(msg, extra))

    def warning(self, msg: str, extra: dict[str, Any] | None = None) -> None:
        self._logger.warning(self._fmt(msg, extra))

    def _fmt(self, msg: str, extra: dict[str, Any] | None) -> str:
        payload = {"message": msg}
        if extra:
            payload.update(extra)
        return json.dumps(payload, default=str)


def get_logger() -> JsonLogger:
    return JsonLogger()
