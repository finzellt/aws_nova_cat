"""
archive_resolver — Lambda handler

Description: External public archive queries (SIMBAD, TNS)
Workflows:   initialize_nova
Tasks:       ResolveCandidateAgainstPublicArchives

Step Functions passes a `task_name` field in the event payload so this
single Lambda can serve multiple state machine task states. Each task
maps to a private _handle_<taskName> function below.

Environment variables (injected by CDK):
    NOVA_CAT_TABLE_NAME           — DynamoDB table name
    NOVA_CAT_PRIVATE_BUCKET       — private data S3 bucket name
    NOVA_CAT_PUBLIC_SITE_BUCKET   — public site S3 bucket name
    NOVA_CAT_QUARANTINE_TOPIC_ARN — quarantine notifications SNS topic ARN
    LOG_LEVEL                     — logging level (default INFO)
    POWERTOOLS_SERVICE_NAME       — AWS Lambda Powertools service name
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def handle(event: dict, context: object) -> dict:
    """
    Lambda entry point.

    Expected event shape (minimum):
        {
            "task_name": "<StateName>",    # Step Functions state name
            "correlation_id": "<uuid>",
            "nova_id": "<uuid>",           # present for most tasks
            ... task-specific fields ...
        }
    """
    task_name = event.get("task_name")
    if not task_name:
        raise ValueError("Missing required field: task_name")

    handler_fn = _TASK_HANDLERS.get(task_name)
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}. Known tasks: {list(_TASK_HANDLERS)}")

    logger.info(
        "Dispatching task",
        extra={
            "task_name": task_name,
            "correlation_id": event.get("correlation_id"),
            "nova_id": event.get("nova_id"),
        },
    )

    return handler_fn(event, context)


# ------------------------------------------------------------------
# Per-task handler stubs
# ------------------------------------------------------------------


def _handle_resolveCandidateAgainstPublicArchives(event: dict, context: object) -> dict:
    """
    TODO: implement ResolveCandidateAgainstPublicArchives.

    Receives the Step Functions task input event and the Lambda context.
    Must return a dict that will be used as the task output in the state machine.
    """
    raise NotImplementedError("ResolveCandidateAgainstPublicArchives not yet implemented")


# ------------------------------------------------------------------
# Dispatch table — defined after stubs to avoid forward references
# ------------------------------------------------------------------
_TASK_HANDLERS: dict[str, Callable[[dict, object], dict]] = {
    "ResolveCandidateAgainstPublicArchives": _handle_resolveCandidateAgainstPublicArchives,
}
