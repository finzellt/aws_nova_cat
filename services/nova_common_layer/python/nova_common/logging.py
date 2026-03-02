"""
nova_common.logging

Pre-configured Powertools Logger for all Nova Cat Lambda functions.

Every log line emitted after configure_logging(event) is called will
carry the following fields automatically:

    service          — "nova-cat" (from POWERTOOLS_SERVICE_NAME env var)
    cold_start       — true on first invocation after a cold start
    function_name    — Lambda function name
    correlation_id   — workflow-level trace key (stitches cross-Lambda logs)
    job_run_id       — unique ID for this workflow execution's JobRun record
    workflow_name    — e.g. "initialize_nova"
    state_name       — Step Functions state that invoked this Lambda

These fields are sufficient to reconstruct the full narrative of any
workflow execution via a single CloudWatch Insights query:

    fields @timestamp, function_name, state_name, level, message
    | filter correlation_id = "<id>"
    | sort @timestamp asc
"""

from __future__ import annotations

from typing import Any

from aws_lambda_powertools import Logger

# Single Logger instance per Lambda process. Powertools handles
# thread-safety and cold-start detection internally.
logger = Logger()


def configure_logging(event: dict[str, Any]) -> None:
    """
    Inject standard structured fields from the Step Functions event payload.

    Call this once at the top of every handler before any logging occurs.
    Fields are persisted for the lifetime of the invocation.

    Fields injected (when present in event):
        correlation_id   — from event["correlation_id"] or event["job_run"]["correlation_id"]
        job_run_id       — from event["job_run_id"] or event["job_run"]["job_run_id"]
        workflow_name    — from event["workflow_name"]
        state_name       — from event["task_name"] (Step Functions task name)
        candidate_name   — from event["candidate_name"] (when present)
        nova_id          — from event["nova_id"] (when present)
    """
    # correlation_id may be at the top level (early states) or nested
    # under job_run (after BeginJobRun has returned)
    job_run: dict[str, Any] = event.get("job_run", {})

    correlation_id = event.get("correlation_id") or job_run.get("correlation_id")
    job_run_id = event.get("job_run_id") or job_run.get("job_run_id")

    persistent_keys: dict[str, Any] = {}

    if correlation_id:
        persistent_keys["correlation_id"] = correlation_id
    if job_run_id:
        persistent_keys["job_run_id"] = job_run_id
    if workflow_name := event.get("workflow_name"):
        persistent_keys["workflow_name"] = workflow_name
    if state_name := event.get("task_name"):
        persistent_keys["state_name"] = state_name
    if candidate_name := event.get("candidate_name"):
        persistent_keys["candidate_name"] = candidate_name
    if nova_id := event.get("nova_id"):
        persistent_keys["nova_id"] = nova_id

    logger.append_keys(**persistent_keys)
