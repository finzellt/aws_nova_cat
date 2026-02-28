"""
Runtime configuration helpers.

This module centralizes access to environment variables required by the
workflow runtime primitives.

The runtime depends on the following environment variables:

- NOVA_DDB_TABLE_NAME  — DynamoDB table used for operational persistence
- NOVA_BUCKET_NAME     — S3 bucket used for data storage
- LOG_LEVEL            — Structured logging level (default: INFO)

This module intentionally performs minimal validation and does not assume
CDK or infrastructure presence. It is safe to import in any Lambda runtime.
"""

from __future__ import annotations

import os


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required env var {name}. Set it on the Lambda runtime (or in tests)."
        )
    return value


def ddb_table_name() -> str:
    return require_env("NOVA_DDB_TABLE_NAME")


def bucket_name() -> str:
    return require_env("NOVA_BUCKET_NAME")


def log_level() -> str:
    return os.getenv("LOG_LEVEL", "INFO")


def powertools_service_name() -> str | None:
    return os.getenv("POWERTOOLS_SERVICE_NAME")
