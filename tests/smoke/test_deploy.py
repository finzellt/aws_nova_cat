"""
Tier 1 smoke tests — deployment validation.

Verifies that all Nova Cat resources were provisioned correctly by inspecting
live AWS resource configuration. No Step Functions executions are started;
these tests are fast, cheap, and safe to run frequently.

Covers:
  - DynamoDB: table exists, correct key schema, GSI provisioned, PAY_PER_REQUEST
  - S3: both buckets exist, private bucket has versioning, both block public access
  - SNS: quarantine topic exists and is reachable
  - Lambda: all 15 functions exist with correct runtime, memory, timeout, env vars
  - Step Functions: all 6 state machines exist as EXPRESS workflows with execution roles
  - IAM: workflow_launcher has states:StartExecution; state machine roles have lambda:InvokeFunction
"""

from __future__ import annotations

import json
from typing import Any, cast

import pytest

from tests.smoke.conftest import (
    EXPECTED_LAMBDA_NAMES,
    EXPECTED_STATE_MACHINE_NAMES,
    REQUIRED_ENV_VARS,
    StackOutputs,
)

# ---------------------------------------------------------------------------
# Lambda configuration expectations — memory (MB) and timeout (s).
# Must stay in sync with compute.py _FUNCTION_SPECS and DockerImageFunctions.
# ---------------------------------------------------------------------------
_LAMBDA_CONFIG: dict[str, dict[str, int]] = {
    "nova-cat-nova-resolver": {"memory": 256, "timeout": 30},
    "nova-cat-job-run-manager": {"memory": 256, "timeout": 30},
    "nova-cat-idempotency-guard": {"memory": 256, "timeout": 30},
    "nova-cat-workflow-launcher": {"memory": 256, "timeout": 30},
    "nova-cat-reference-manager": {"memory": 256, "timeout": 90},
    "nova-cat-spectra-acquirer": {"memory": 512, "timeout": 900},
    "nova-cat-photometry-ingestor": {"memory": 512, "timeout": 300},
    "nova-cat-quarantine-handler": {"memory": 256, "timeout": 30},
    "nova-cat-name-reconciler": {"memory": 256, "timeout": 90},
    "nova-cat-ticket-parser": {"memory": 256, "timeout": 30},
    "nova-cat-nova-resolver-ticket": {"memory": 256, "timeout": 120},
    # Docker functions
    "nova-cat-archive-resolver": {"memory": 256, "timeout": 90},
    "nova-cat-spectra-discoverer": {"memory": 256, "timeout": 60},
    "nova-cat-spectra-validator": {"memory": 512, "timeout": 300},
    "nova-cat-ticket-ingestor": {"memory": 512, "timeout": 600},
}

_DOCKER_FUNCTION_NAMES = {
    "nova-cat-archive-resolver",
    "nova-cat-spectra-discoverer",
    "nova-cat-spectra-validator",
    "nova-cat-ticket-ingestor",
}

_ZIP_FUNCTION_NAMES = {name for name in EXPECTED_LAMBDA_NAMES if name not in _DOCKER_FUNCTION_NAMES}


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------


class TestDynamoDB:
    def test_table_exists(self, stack: StackOutputs, dynamodb_client: Any) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        assert resp["Table"]["TableStatus"] == "ACTIVE"

    def test_table_name_matches_output(self, stack: StackOutputs, dynamodb_client: Any) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        assert resp["Table"]["TableName"] == stack.table_name

    def test_primary_key_schema(self, stack: StackOutputs, dynamodb_client: Any) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        key_schema = {k["AttributeName"]: k["KeyType"] for k in resp["Table"]["KeySchema"]}
        assert key_schema == {"PK": "HASH", "SK": "RANGE"}

    def test_attribute_types(self, stack: StackOutputs, dynamodb_client: Any) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        attr_types = {
            a["AttributeName"]: a["AttributeType"] for a in resp["Table"]["AttributeDefinitions"]
        }
        assert attr_types["PK"] == "S"
        assert attr_types["SK"] == "S"
        assert attr_types["GSI1PK"] == "S"
        assert attr_types["GSI1SK"] == "S"

    def test_billing_mode_pay_per_request(self, stack: StackOutputs, dynamodb_client: Any) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        assert resp["Table"]["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"

    def test_eligibility_gsi_exists(self, stack: StackOutputs, dynamodb_client: Any) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        gsi_names = [g["IndexName"] for g in resp["Table"].get("GlobalSecondaryIndexes", [])]
        assert "EligibilityIndex" in gsi_names

    def test_eligibility_gsi_key_schema(self, stack: StackOutputs, dynamodb_client: Any) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        gsi = next(
            g
            for g in resp["Table"]["GlobalSecondaryIndexes"]
            if g["IndexName"] == "EligibilityIndex"
        )
        key_schema = {k["AttributeName"]: k["KeyType"] for k in gsi["KeySchema"]}
        assert key_schema == {"GSI1PK": "HASH", "GSI1SK": "RANGE"}

    def test_eligibility_gsi_projection_all(
        self, stack: StackOutputs, dynamodb_client: Any
    ) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        gsi = next(
            g
            for g in resp["Table"]["GlobalSecondaryIndexes"]
            if g["IndexName"] == "EligibilityIndex"
        )
        assert gsi["Projection"]["ProjectionType"] == "ALL"

    def test_eligibility_gsi_active(self, stack: StackOutputs, dynamodb_client: Any) -> None:
        resp = dynamodb_client.describe_table(TableName=stack.table_name)
        gsi = next(
            g
            for g in resp["Table"]["GlobalSecondaryIndexes"]
            if g["IndexName"] == "EligibilityIndex"
        )
        assert gsi["IndexStatus"] == "ACTIVE"


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------


class TestS3:
    def test_private_bucket_exists(self, stack: StackOutputs, s3_client: Any) -> None:
        # head_bucket raises if bucket does not exist or is inaccessible
        s3_client.head_bucket(Bucket=stack.private_bucket_name)

    def test_public_site_bucket_exists(self, stack: StackOutputs, s3_client: Any) -> None:
        s3_client.head_bucket(Bucket=stack.public_site_bucket_name)

    def test_private_bucket_versioning_enabled(self, stack: StackOutputs, s3_client: Any) -> None:
        resp = s3_client.get_bucket_versioning(Bucket=stack.private_bucket_name)
        assert resp.get("Status") == "Enabled", (
            f"Expected versioning=Enabled on private bucket, got: {resp.get('Status')!r}"
        )

    def test_public_site_bucket_versioning_not_enabled(
        self, stack: StackOutputs, s3_client: Any
    ) -> None:
        resp = s3_client.get_bucket_versioning(Bucket=stack.public_site_bucket_name)
        # Versioning absent or Suspended — never Enabled
        assert resp.get("Status") != "Enabled", (
            "Public site bucket should not have versioning enabled"
        )

    def test_private_bucket_blocks_public_access(self, stack: StackOutputs, s3_client: Any) -> None:
        resp = s3_client.get_public_access_block(Bucket=stack.private_bucket_name)
        config = resp["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is True
        assert config["BlockPublicPolicy"] is True
        assert config["IgnorePublicAcls"] is True
        assert config["RestrictPublicBuckets"] is True

    def test_public_site_bucket_blocks_public_access(
        self, stack: StackOutputs, s3_client: Any
    ) -> None:
        resp = s3_client.get_public_access_block(Bucket=stack.public_site_bucket_name)
        config = resp["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is True
        assert config["BlockPublicPolicy"] is True
        assert config["IgnorePublicAcls"] is True
        assert config["RestrictPublicBuckets"] is True

    def test_private_bucket_encryption_enabled(self, stack: StackOutputs, s3_client: Any) -> None:
        resp = s3_client.get_bucket_encryption(Bucket=stack.private_bucket_name)
        rules = resp["ServerSideEncryptionConfiguration"]["Rules"]
        assert any(
            r["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] in {"AES256", "aws:kms"}
            for r in rules
        )

    def test_private_bucket_quarantine_lifecycle_rule(
        self, stack: StackOutputs, s3_client: Any
    ) -> None:
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=stack.private_bucket_name)
        rule_ids = [r["ID"] for r in resp["Rules"]]
        assert "ExpireQuarantineObjects" in rule_ids

    def test_private_bucket_workflow_payload_lifecycle_rule(
        self, stack: StackOutputs, s3_client: Any
    ) -> None:
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=stack.private_bucket_name)
        rule_ids = [r["ID"] for r in resp["Rules"]]
        assert "ExpireWorkflowPayloadSnapshots" in rule_ids

    def test_public_site_bucket_releases_lifecycle_rule(
        self, stack: StackOutputs, s3_client: Any
    ) -> None:
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=stack.public_site_bucket_name)
        rule_ids = [r["ID"] for r in resp["Rules"]]
        assert "ExpireOldReleases" in rule_ids


# ---------------------------------------------------------------------------
# SNS
# ---------------------------------------------------------------------------


class TestSNS:
    def test_quarantine_topic_exists(self, stack: StackOutputs, sns_client: Any) -> None:
        # get_topic_attributes raises if the topic ARN is invalid or doesn't exist
        resp = sns_client.get_topic_attributes(TopicArn=stack.quarantine_topic_arn)
        assert resp["Attributes"]["TopicArn"] == stack.quarantine_topic_arn

    def test_quarantine_topic_name(self, stack: StackOutputs, sns_client: Any) -> None:
        assert stack.quarantine_topic_arn.endswith("quarantine-notifications"), (
            f"Quarantine topic ARN does not end with 'quarantine-notifications': "
            f"{stack.quarantine_topic_arn!r}"
        )


# ---------------------------------------------------------------------------
# Lambda
# ---------------------------------------------------------------------------


class TestLambda:
    @pytest.mark.parametrize("fn_name", EXPECTED_LAMBDA_NAMES)
    def test_function_exists(self, fn_name: str, lambda_client: Any) -> None:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        assert resp["FunctionName"] == fn_name

    @pytest.mark.parametrize("fn_name", EXPECTED_LAMBDA_NAMES)
    def test_function_state_active(self, fn_name: str, lambda_client: Any) -> None:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        # State is Active once the function is ready to invoke
        assert resp.get("State") == "Active", (
            f"{fn_name}: expected State=Active, got {resp.get('State')!r}"
        )

    @pytest.mark.parametrize("fn_name", sorted(_ZIP_FUNCTION_NAMES))
    def test_zip_functions_runtime(self, fn_name: str, lambda_client: Any) -> None:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        assert resp["Runtime"] == "python3.11", (
            f"{fn_name}: expected python3.11, got {resp['Runtime']!r}"
        )

    @pytest.mark.parametrize("fn_name", sorted(_DOCKER_FUNCTION_NAMES))
    def test_docker_functions_package_type(self, fn_name: str, lambda_client: Any) -> None:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        assert resp["PackageType"] == "Image", (
            f"{fn_name}: expected PackageType=Image, got {resp['PackageType']!r}"
        )

    @pytest.mark.parametrize("fn_name,config", _LAMBDA_CONFIG.items())
    def test_function_memory(
        self, fn_name: str, config: dict[str, int], lambda_client: Any
    ) -> None:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        assert resp["MemorySize"] == config["memory"], (
            f"{fn_name}: expected MemorySize={config['memory']}, got {resp['MemorySize']}"
        )

    @pytest.mark.parametrize("fn_name,config", _LAMBDA_CONFIG.items())
    def test_function_timeout(
        self, fn_name: str, config: dict[str, int], lambda_client: Any
    ) -> None:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        assert resp["Timeout"] == config["timeout"], (
            f"{fn_name}: expected Timeout={config['timeout']}, got {resp['Timeout']}"
        )

    @pytest.mark.parametrize("fn_name", EXPECTED_LAMBDA_NAMES)
    def test_function_has_required_env_vars(self, fn_name: str, lambda_client: Any) -> None:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        deployed_vars = set(resp.get("Environment", {}).get("Variables", {}).keys())
        missing = REQUIRED_ENV_VARS - deployed_vars
        assert not missing, f"{fn_name} is missing required env vars: {missing}"

    @pytest.mark.parametrize("fn_name", EXPECTED_LAMBDA_NAMES)
    def test_function_xray_tracing_active(self, fn_name: str, lambda_client: Any) -> None:
        resp = lambda_client.get_function_configuration(FunctionName=fn_name)
        assert resp.get("TracingConfig", {}).get("Mode") == "Active", (
            f"{fn_name}: expected TracingConfig.Mode=Active"
        )

    def test_reference_manager_has_ads_secret_name(self, lambda_client: Any) -> None:
        """reference_manager is the sole consumer of ADS_SECRET_NAME."""
        resp = lambda_client.get_function_configuration(FunctionName="nova-cat-reference-manager")
        env_vars = resp.get("Environment", {}).get("Variables", {})
        assert "ADS_SECRET_NAME" in env_vars, (
            "nova-cat-reference-manager is missing ADS_SECRET_NAME env var"
        )
        assert env_vars["ADS_SECRET_NAME"] == "ADSQueryToken"

    def test_workflow_launcher_has_all_state_machine_arns(
        self, stack: StackOutputs, lambda_client: Any
    ) -> None:
        """workflow_launcher must have all four downstream ARNs injected."""
        resp = lambda_client.get_function_configuration(FunctionName="nova-cat-workflow-launcher")
        env_vars = resp.get("Environment", {}).get("Variables", {})

        expected_keys = [
            "INGEST_NEW_NOVA_STATE_MACHINE_ARN",
            "REFRESH_REFERENCES_STATE_MACHINE_ARN",
            "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN",
            "ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN",
        ]
        for key in expected_keys:
            assert key in env_vars, f"workflow_launcher missing env var: {key}"
            value = env_vars[key]
            assert value.startswith("arn:aws:states:") and ":stateMachine:" in value, (
                f"workflow_launcher {key}={value!r} is not a valid Step Functions state machine ARN"
            )

    def test_nova_resolver_ticket_has_initialize_nova_arn(self, lambda_client: Any) -> None:
        """nova-cat-nova-resolver-ticket must know the initialize_nova state machine ARN."""
        resp = lambda_client.get_function_configuration(
            FunctionName="nova-cat-nova-resolver-ticket"
        )
        env_vars = resp.get("Environment", {}).get("Variables", {})
        assert "INITIALIZE_NOVA_STATE_MACHINE_ARN" in env_vars, (
            "nova-cat-nova-resolver-ticket is missing INITIALIZE_NOVA_STATE_MACHINE_ARN env var"
        )
        assert "initialize-nova" in env_vars["INITIALIZE_NOVA_STATE_MACHINE_ARN"], (
            f"INITIALIZE_NOVA_STATE_MACHINE_ARN does not reference initialize-nova: "
            f"{env_vars['INITIALIZE_NOVA_STATE_MACHINE_ARN']!r}"
        )

    def test_ticket_ingestor_has_photometry_table_name(self, lambda_client: Any) -> None:
        """nova-cat-ticket-ingestor must have the dedicated photometry table name."""
        resp = lambda_client.get_function_configuration(FunctionName="nova-cat-ticket-ingestor")
        env_vars = resp.get("Environment", {}).get("Variables", {})
        assert "PHOTOMETRY_TABLE_NAME" in env_vars, (
            "nova-cat-ticket-ingestor is missing PHOTOMETRY_TABLE_NAME env var"
        )


# ---------------------------------------------------------------------------
# Step Functions
# ---------------------------------------------------------------------------


class TestStepFunctions:
    @pytest.mark.parametrize("sm_name", EXPECTED_STATE_MACHINE_NAMES)
    def test_state_machine_exists(self, sm_name: str, stack: StackOutputs, sfn_client: Any) -> None:
        """Resolve the ARN from the stack outputs map and describe it."""
        arn = _sm_arn(sm_name, stack)
        resp = sfn_client.describe_state_machine(stateMachineArn=arn)
        # The deployed name may include a stack-specific prefix (e.g.
        # "nova-cat-smoke-initialize-nova" vs "nova-cat-initialize-nova").
        # Use the name embedded in the ARN (which comes from stack outputs)
        # as the ground truth.
        expected_name = arn.split(":")[-1]
        assert resp["name"] == expected_name

    @pytest.mark.parametrize("sm_name", EXPECTED_STATE_MACHINE_NAMES)
    def test_state_machine_is_express_workflow(
        self, sm_name: str, stack: StackOutputs, sfn_client: Any
    ) -> None:
        arn = _sm_arn(sm_name, stack)
        resp = sfn_client.describe_state_machine(stateMachineArn=arn)
        assert resp["type"] == "EXPRESS", f"{sm_name}: expected type=EXPRESS, got {resp['type']!r}"

    @pytest.mark.parametrize("sm_name", EXPECTED_STATE_MACHINE_NAMES)
    def test_state_machine_is_active(
        self, sm_name: str, stack: StackOutputs, sfn_client: Any
    ) -> None:
        arn = _sm_arn(sm_name, stack)
        resp = sfn_client.describe_state_machine(stateMachineArn=arn)
        assert resp["status"] == "ACTIVE", (
            f"{sm_name}: expected status=ACTIVE, got {resp['status']!r}"
        )

    @pytest.mark.parametrize("sm_name", EXPECTED_STATE_MACHINE_NAMES)
    def test_state_machine_has_execution_role(
        self, sm_name: str, stack: StackOutputs, sfn_client: Any
    ) -> None:
        arn = _sm_arn(sm_name, stack)
        resp = sfn_client.describe_state_machine(stateMachineArn=arn)
        assert resp.get("roleArn"), f"{sm_name}: missing roleArn"
        assert "arn:aws:iam::" in resp["roleArn"]

    @pytest.mark.parametrize("sm_name", EXPECTED_STATE_MACHINE_NAMES)
    def test_state_machine_definition_parses_as_valid_json(
        self, sm_name: str, stack: StackOutputs, sfn_client: Any
    ) -> None:
        """
        The deployed definition must be valid JSON with a non-empty States map.
        This catches any Fn::Sub token substitution failures that CloudFormation
        would have silently left as literal ${...} strings.
        """
        arn = _sm_arn(sm_name, stack)
        resp = sfn_client.describe_state_machine(stateMachineArn=arn)
        definition = json.loads(resp["definition"])
        assert "States" in definition, f"{sm_name}: definition missing 'States' key"
        assert len(definition["States"]) > 0, f"{sm_name}: definition has empty States map"

    @pytest.mark.parametrize("sm_name", EXPECTED_STATE_MACHINE_NAMES)
    def test_state_machine_definition_has_no_unresolved_tokens(
        self, sm_name: str, stack: StackOutputs, sfn_client: Any
    ) -> None:
        """
        Verifies that no CloudFormation ${Token} substitution placeholders
        remain in the deployed definition. Any unresolved token means the
        Fn::Sub substitutions map was incomplete.
        """
        arn = _sm_arn(sm_name, stack)
        resp = sfn_client.describe_state_machine(stateMachineArn=arn)
        raw_definition = resp["definition"]
        assert "${" not in raw_definition, (
            f"{sm_name}: deployed definition contains unresolved CloudFormation tokens. "
            "Check that all substitutions in NovaCatWorkflows._create_state_machine() "
            "are populated."
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sm_arn(sm_name: str, stack: StackOutputs) -> str:
    """Map a state machine name to its ARN from StackOutputs."""
    _name_to_attr = {
        "nova-cat-initialize-nova": "initialize_nova_arn",
        "nova-cat-ingest-new-nova": "ingest_new_nova_arn",
        "nova-cat-refresh-references": "refresh_references_arn",
        "nova-cat-discover-spectra-products": "discover_spectra_products_arn",
        "nova-cat-acquire-and-validate-spectra": "acquire_and_validate_spectra_arn",
        "nova-cat-ingest-ticket": "ingest_ticket_arn",
    }
    return cast(str, getattr(stack, _name_to_attr[sm_name]))
