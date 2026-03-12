"""
Smoke test configuration and shared fixtures.

Resolves all Nova Cat resource identifiers from the deployed CloudFormation
stack's exported outputs at session scope. If the stack is not deployed or
any expected output is missing, the entire smoke suite is skipped with a
clear message — it never fails cryptically.

Usage:
    pytest tests/smoke/ -v

Prerequisites:
    - AWS credentials configured (env vars, ~/.aws/credentials, or instance role)
    - NovaCat stack deployed: cdk deploy -c account=<ACCOUNT_ID>
    - Target region: us-east-1 (override with AWS_DEFAULT_REGION if needed)
"""

from __future__ import annotations

import dataclasses
import os
import time
from typing import Any, cast

import boto3
import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STACK_NAME = "NovaCatSmoke"
_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
_POLL_INTERVAL_SECONDS = 5

# CloudFormation export names, keyed by the attribute they populate on
# StackOutputs. Must stay in sync with the CfnOutput definitions in
# nova_constructs/storage.py and nova_constructs/workflows.py.
_CF_EXPORT_MAP: dict[str, str] = {
    "table_name": "NovaCatSmoke-TableName",
    "private_bucket_name": "NovaCatSmoke-PrivateBucketName",
    "public_site_bucket_name": "NovaCatSmoke-PublicSiteBucketName",
    "quarantine_topic_arn": "NovaCatSmoke-QuarantineTopicArn",
    "initialize_nova_arn": "NovaCatSmoke-InitializeNovaStateMachineArn",
    "ingest_new_nova_arn": "NovaCatSmoke-IngestNewNovaStateMachineArn",
    "refresh_references_arn": "NovaCatSmoke-RefreshReferencesStateMachineArn",
    "discover_spectra_products_arn": "NovaCatSmoke-DiscoverSpectraProductsStateMachineArn",
    "acquire_and_validate_spectra_arn": "NovaCatSmoke-AcquireAndValidateSpectraStateMachineArn",
}

# Lambda function names as provisioned by NovaCatCompute.
# Sourced from compute.py's _FUNCTION_SPECS and DockerImageFunction definitions.
EXPECTED_LAMBDA_NAMES: list[str] = [
    "nova-cat-nova-resolver",
    "nova-cat-job-run-manager",
    "nova-cat-idempotency-guard",
    "nova-cat-workflow-launcher",
    "nova-cat-reference-manager",
    "nova-cat-spectra-acquirer",
    "nova-cat-photometry-ingestor",
    "nova-cat-quarantine-handler",
    "nova-cat-name-reconciler",
    "nova-cat-archive-resolver",
    "nova-cat-spectra-discoverer",
    "nova-cat-spectra-validator",
]

# Expected Step Functions state machines.
EXPECTED_STATE_MACHINE_NAMES: list[str] = [
    "nova-cat-initialize-nova",
    "nova-cat-ingest-new-nova",
    "nova-cat-refresh-references",
    "nova-cat-discover-spectra-products",
    "nova-cat-acquire-and-validate-spectra",
]

# Required env vars injected into every Lambda (from compute.py shared_env).
REQUIRED_ENV_VARS: set[str] = {
    "NOVA_CAT_TABLE_NAME",
    "NOVA_CAT_PRIVATE_BUCKET",
    "NOVA_CAT_PUBLIC_SITE_BUCKET",
    "NOVA_CAT_QUARANTINE_TOPIC_ARN",
    "LOG_LEVEL",
    "POWERTOOLS_SERVICE_NAME",
}


# ---------------------------------------------------------------------------
# StackOutputs — resolved once per session
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class StackOutputs:
    """Typed container for all CloudFormation stack outputs Nova Cat exports."""

    table_name: str
    private_bucket_name: str
    public_site_bucket_name: str
    quarantine_topic_arn: str
    initialize_nova_arn: str
    ingest_new_nova_arn: str
    refresh_references_arn: str
    discover_spectra_products_arn: str
    acquire_and_validate_spectra_arn: str


def _resolve_stack_outputs(cf_client: Any) -> StackOutputs:
    """
    Describe the NovaCat stack and extract all expected outputs.

    Raises pytest.skip() if:
      - The stack doesn't exist or isn't in CREATE_COMPLETE / UPDATE_COMPLETE
      - Any expected export key is absent from the stack outputs
    """
    try:
        resp = cf_client.describe_stacks(StackName=_STACK_NAME)
    except cf_client.exceptions.ClientError as exc:
        pytest.skip(f"NovaCat stack not found or inaccessible: {exc}")

    stacks = resp.get("Stacks", [])
    if not stacks:
        pytest.skip("NovaCat stack returned no results from describe_stacks")

    stack = stacks[0]
    status = stack.get("StackStatus", "")
    terminal_ok = {"CREATE_COMPLETE", "UPDATE_COMPLETE"}
    if status not in terminal_ok:
        pytest.skip(
            f"NovaCat stack is in status '{status}' — expected one of {terminal_ok}. "
            "Deploy the stack before running smoke tests."
        )

    # Build export-name → output-value mapping
    outputs_by_export: dict[str, str] = {}
    for output in stack.get("Outputs", []):
        export_name = output.get("ExportName")
        if export_name:
            outputs_by_export[export_name] = output["OutputValue"]

    # Verify all expected exports are present
    missing = [
        export_name
        for export_name in _CF_EXPORT_MAP.values()
        if export_name not in outputs_by_export
    ]
    if missing:
        pytest.skip(
            f"NovaCat stack is missing expected CloudFormation exports: {missing}. "
            "Re-deploy the stack and ensure all CfnOutputs are present."
        )

    return StackOutputs(
        **{attr: outputs_by_export[export_name] for attr, export_name in _CF_EXPORT_MAP.items()}
    )


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def cf_client() -> Any:
    return boto3.client("cloudformation", region_name=_REGION)


@pytest.fixture(scope="session")
def stack(cf_client: Any) -> StackOutputs:
    """Resolve CloudFormation outputs once for the entire session."""
    return _resolve_stack_outputs(cf_client)


@pytest.fixture(scope="session")
def lambda_client() -> Any:
    return boto3.client("lambda", region_name=_REGION)


@pytest.fixture(scope="session")
def sfn_client() -> Any:
    return boto3.client("stepfunctions", region_name=_REGION)


@pytest.fixture(scope="session")
def dynamodb_client() -> Any:
    return boto3.client("dynamodb", region_name=_REGION)


@pytest.fixture(scope="session")
def s3_client() -> Any:
    return boto3.client("s3", region_name=_REGION)


@pytest.fixture(scope="session")
def sns_client() -> Any:
    return boto3.client("sns", region_name=_REGION)


# ---------------------------------------------------------------------------
# Execution polling helper (used by test_workflows.py and test_e2e.py)
# ---------------------------------------------------------------------------


def poll_execution(
    sfn_client: Any,
    execution_arn: str,
    timeout_seconds: int = 60,
    poll_interval: int = _POLL_INTERVAL_SECONDS,
) -> dict[str, Any]:
    """
    Poll a Step Functions execution until it reaches a terminal status
    (SUCCEEDED, FAILED, TIMED_OUT, ABORTED) or the timeout is exceeded.

    Returns the describe_execution response dict on terminal status.
    Raises TimeoutError if the execution has not completed within timeout_seconds.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        resp = sfn_client.describe_execution(executionArn=execution_arn)
        status = resp["status"]
        if status != "RUNNING":
            return cast(dict[str, Any], resp)
        time.sleep(poll_interval)

    raise TimeoutError(
        f"Execution {execution_arn} did not complete within {timeout_seconds}s. "
        "Increase timeout or check CloudWatch Logs for the state machine."
    )


@pytest.fixture(scope="session")
def dynamodb_resource() -> Any:
    return boto3.resource("dynamodb", region_name=_REGION)


@pytest.fixture(scope="session", autouse=True)
def purge_smoke_items_before_session(stack: StackOutputs, dynamodb_resource: Any) -> None:
    """
    Purge all smoke-test-related DynamoDB items once before the session starts.

    This guards against stale items left by a previous test run — for example
    LOCATOR# aliases written before the per-test cleanup fixture existed, or
    from a run that was interrupted before cleanup could fire. Without this,
    DeduplicateAndAssign finds existing LocatorAlias items and sets is_new=False,
    suppressing DataProduct stub writes and causing DynamoDB-asserting tests to
    skip even though the workflow executed correctly.

    The per-test cleanup_smoke_items fixture handles ongoing cleanup after each
    test; this fixture just ensures the session starts with a known-clean state.
    """
    _wipe_smoke_test_table(dynamodb_resource.Table(stack.table_name))


def _wipe_smoke_test_table(table: Any) -> None:
    """
    Delete every item in the smoke test table.

    The smoke test table is dedicated exclusively to smoke test runs, so a
    full wipe is always safe — there is no production data to protect. This
    replaces the previous multi-pass targeted scan approach, which was
    fragile (required tagging every item type) and dangerous (could have
    nuked real data if pointed at a shared table by mistake).
    """
    response = table.scan(ProjectionExpression="PK, SK")
    with table.batch_writer() as batch:
        for item in response.get("Items", []):
            batch.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
    # Handle pagination — scan returns at most 1 MB per call
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="PK, SK",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        with table.batch_writer() as batch:
            for item in response.get("Items", []):
                batch.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})


@pytest.fixture(autouse=True)
def cleanup_smoke_items(stack: StackOutputs, dynamodb_resource: Any) -> Any:
    """Wipe the smoke test table after each test."""
    yield  # test runs here
    _wipe_smoke_test_table(dynamodb_resource.Table(stack.table_name))
