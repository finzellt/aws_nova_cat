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
    - NovaCatSmoke stack deployed: cdk deploy -c account=<ACCOUNT_ID>
    - Target region: us-east-1 (override with AWS_DEFAULT_REGION if needed)
"""

from __future__ import annotations

import dataclasses
import os
from typing import Any

import boto3
import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STACK_NAME = "NovaCatSmoke"
_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
_POLL_INTERVAL_SECONDS = 5

# ---------------------------------------------------------------------------
# Resource name prefix
#
# Every Lambda, state machine, and IAM resource in the smoke stack is named
# with this prefix.  The smoke stack exists so that deployment-validation
# tests never touch production resources — but that isolation only works if
# the tests actually address the *smoke* resources.
#
# Previously, these name lists were hardcoded with the production prefix
# ("nova-cat-"), which meant the smoke tests were inspecting production
# Lambdas and comparing their env vars against smoke-stack CloudFormation
# outputs.  That mismatch was the root cause of
# test_workflow_launcher_has_all_state_machine_arns failing: the production
# workflow_launcher had production ARNs, but the test expected smoke ARNs.
#
# All resource names are now derived from _ENV_PREFIX so the two stacks
# can never be confused.  If the smoke stack's env_prefix changes in
# app.py, update this single constant and everything follows.
# ---------------------------------------------------------------------------
_ENV_PREFIX = "nova-cat-smoke"
_CF_PREFIX = "NovaCatSmoke"

# CloudFormation export names, keyed by the attribute they populate on
# StackOutputs. Must stay in sync with the CfnOutput definitions in
# nova_constructs/storage.py and nova_constructs/workflows.py.
_CF_EXPORT_MAP: dict[str, str] = {
    "table_name": f"{_CF_PREFIX}-TableName",
    "private_bucket_name": f"{_CF_PREFIX}-PrivateBucketName",
    "public_site_bucket_name": f"{_CF_PREFIX}-PublicSiteBucketName",
    "quarantine_topic_arn": f"{_CF_PREFIX}-QuarantineTopicArn",
    "initialize_nova_arn": f"{_CF_PREFIX}-InitializeNovaStateMachineArn",
    "ingest_new_nova_arn": f"{_CF_PREFIX}-IngestNewNovaStateMachineArn",
    "refresh_references_arn": f"{_CF_PREFIX}-RefreshReferencesStateMachineArn",
    "discover_spectra_products_arn": f"{_CF_PREFIX}-DiscoverSpectraProductsStateMachineArn",
    "acquire_and_validate_spectra_arn": f"{_CF_PREFIX}-AcquireAndValidateSpectraStateMachineArn",
    "ingest_ticket_arn": f"{_CF_PREFIX}-IngestTicketStateMachineArn",
    "photometry_table_name": f"{_CF_PREFIX}-PhotometryTableName",
}

# ---------------------------------------------------------------------------
# Lambda function names as provisioned by NovaCatCompute.
#
# Sourced from compute.py's _FUNCTION_SPECS (zip-bundled) and the
# DockerImageFunction definitions (container-based). The bare suffixes
# below are the hyphenated form of the Python function spec keys.
# ---------------------------------------------------------------------------

_LAMBDA_SUFFIXES: list[str] = [
    "nova-resolver",
    "job-run-manager",
    "idempotency-guard",
    "workflow-launcher",
    "reference-manager",
    "spectra-acquirer",
    "photometry-ingestor",
    "quarantine-handler",
    "name-reconciler",
    # Container-based (DockerImageFunction)
    "archive-resolver",
    "spectra-discoverer",
    "spectra-validator",
    "ticket-parser",
    "nova-resolver-ticket",
    "ticket-ingestor",
]

EXPECTED_LAMBDA_NAMES: list[str] = [f"{_ENV_PREFIX}-{s}" for s in _LAMBDA_SUFFIXES]

# Expected Step Functions state machines.
_STATE_MACHINE_SUFFIXES: list[str] = [
    "initialize-nova",
    "ingest-new-nova",
    "refresh-references",
    "discover-spectra-products",
    "acquire-and-validate-spectra",
    "ingest-ticket",
]

EXPECTED_STATE_MACHINE_NAMES: list[str] = [f"{_ENV_PREFIX}-{s}" for s in _STATE_MACHINE_SUFFIXES]

# Required env vars injected into every Lambda (from compute.py shared_env).
REQUIRED_ENV_VARS: set[str] = {
    "NOVA_CAT_TABLE_NAME",
    "NOVA_CAT_PRIVATE_BUCKET",
    "NOVA_CAT_PUBLIC_SITE_BUCKET",
    "NOVA_CAT_QUARANTINE_TOPIC_ARN",
    "LOG_LEVEL",
    "POWERTOOLS_SERVICE_NAME",
}

# Expose the prefix so test_deploy.py (and any other smoke module) can
# build function names without duplicating the constant.
ENV_PREFIX = _ENV_PREFIX


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
    ingest_ticket_arn: str
    photometry_table_name: str


def _resolve_stack_outputs(cf_client: Any) -> StackOutputs:
    """
    Describe the NovaCatSmoke stack and extract all expected outputs.

    Raises pytest.skip() if:
      - The stack doesn't exist or isn't in CREATE_COMPLETE / UPDATE_COMPLETE
      - Any expected export key is absent from the stack outputs
    """
    try:
        resp = cf_client.describe_stacks(StackName=_STACK_NAME)
    except cf_client.exceptions.ClientError as exc:
        pytest.skip(f"NovaCatSmoke stack not found or inaccessible: {exc}")

    stacks = resp.get("Stacks", [])
    if not stacks:
        pytest.skip("NovaCatSmoke stack returned no results from describe_stacks")

    stack = stacks[0]
    status = stack.get("StackStatus", "")
    terminal_ok = {"CREATE_COMPLETE", "UPDATE_COMPLETE"}
    if status not in terminal_ok:
        pytest.skip(
            f"NovaCatSmoke stack is in status '{status}' — expected one of {terminal_ok}. "
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
            f"NovaCatSmoke stack is missing expected CloudFormation exports: {missing}. "
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
# DynamoDB wipe helpers
# ---------------------------------------------------------------------------


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
    _wipe_smoke_test_table(dynamodb_resource.Table(stack.photometry_table_name))


def _wipe_smoke_test_table(table: Any) -> None:
    """
    Delete every item in the smoke test table.

    The smoke test table is dedicated exclusively to smoke test runs, so a
    full wipe is always safe — there is no production data to protect. This
    replaces the previous multi-pass targeted scan approach, which was
    fragile (required tagging every item type) and dangerous (could have
    nuked real data if pointed at a shared table by mistake).

    Works for any table with a PK (String) + SK (String) key schema,
    including both the main NovaCat table and the dedicated photometry table.
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
    """Wipe both smoke test tables before each test.

    Cleanup runs BEFORE the test, not after. This is deliberate:
    initialize_nova fires async downstream workflows (ingest_new_nova →
    refresh_references, discover_spectra_products) via non-blocking
    StartExecution. Those workflows may still be running when the test
    returns. Wiping after the test would delete the Nova item while
    refresh_references is trying to read it, causing spurious
    "Nova not found in DDB" TerminalErrors.

    Wiping before the next test gives async workflows from the previous
    test time to complete (pytest overhead + fixture setup is typically
    several seconds — more than enough for Express workflows). Each test
    still starts with a clean table.
    """
    _wipe_smoke_test_table(dynamodb_resource.Table(stack.table_name))
    _wipe_smoke_test_table(dynamodb_resource.Table(stack.photometry_table_name))
    yield
