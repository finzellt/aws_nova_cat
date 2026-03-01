"""
Nova Cat CDK synth tests.

Uses aws_cdk.assertions to synthesize the stack once and assert that the
resulting CloudFormation template matches the expected architecture.

These tests pin architectural decisions — billing mode, GSI shape, bucket
policies, Lambda configuration — so that future changes can't silently
regress them. They are not integration tests and make no AWS API calls.

Run with:
    pytest infra/tests/test_synth.py
"""

from __future__ import annotations

import aws_cdk as cdk
import pytest
from aws_cdk import assertions
from nova_cat.nova_cat_stack import NovaCatStack

_ACCOUNT = "000000000000"
_REGION = "us-east-1"


@pytest.fixture(scope="module")
def template() -> assertions.Template:
    """Synthesize the stack once for all tests in this module."""
    app = cdk.App(context={"account": _ACCOUNT})
    stack = NovaCatStack(
        app,
        "NovaCatTest",
        env=cdk.Environment(account=_ACCOUNT, region=_REGION),
    )
    return assertions.Template.from_stack(stack)


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------


class TestDynamoDb:
    def test_single_table_exists(self, template: assertions.Template) -> None:
        template.resource_count_is("AWS::DynamoDB::Table", 1)

    def test_table_name(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {"TableName": "NovaCat"},
        )

    def test_primary_key_schema(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "KeySchema": [
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "SK", "KeyType": "RANGE"},
                ],
            },
        )

    def test_primary_key_attribute_types(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "AttributeDefinitions": assertions.Match.array_with(
                    [
                        {"AttributeName": "PK", "AttributeType": "S"},
                        {"AttributeName": "SK", "AttributeType": "S"},
                        {"AttributeName": "GSI1PK", "AttributeType": "S"},
                        {"AttributeName": "GSI1SK", "AttributeType": "S"},
                    ]
                ),
            },
        )

    def test_billing_mode_pay_per_request(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {"BillingMode": "PAY_PER_REQUEST"},
        )

    def test_pitr_disabled_by_default(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "PointInTimeRecoverySpecification": {
                    "PointInTimeRecoveryEnabled": False,
                },
            },
        )

    def test_eligibility_gsi_exists(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "GlobalSecondaryIndexes": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "IndexName": "EligibilityIndex",
                                "KeySchema": [
                                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                                ],
                                "Projection": {"ProjectionType": "ALL"},
                            }
                        )
                    ]
                ),
            },
        )


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------


class TestS3:
    def test_private_bucket_versioning_enabled(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "VersioningConfiguration": {"Status": "Enabled"},
                "LifecycleConfiguration": assertions.Match.object_like(
                    {
                        "Rules": assertions.Match.array_with(
                            [
                                assertions.Match.object_like(
                                    {
                                        "Id": "ExpireQuarantineObjects",
                                        "Prefix": "quarantine/",
                                        "Status": "Enabled",
                                        "ExpirationInDays": 365,
                                    }
                                ),
                                assertions.Match.object_like(
                                    {
                                        "Id": "ExpireWorkflowPayloadSnapshots",
                                        "Prefix": "workflow-payloads/",
                                        "Status": "Enabled",
                                        "ExpirationInDays": 30,
                                    }
                                ),
                            ]
                        ),
                    }
                ),
            },
        )

    def test_private_bucket_blocks_public_access(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "VersioningConfiguration": {"Status": "Enabled"},
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
            },
        )

    def test_public_site_bucket_releases_lifecycle(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "LifecycleConfiguration": assertions.Match.object_like(
                    {
                        "Rules": assertions.Match.array_with(
                            [
                                assertions.Match.object_like(
                                    {
                                        "Id": "ExpireOldReleases",
                                        "Prefix": "releases/",
                                        "Status": "Enabled",
                                        "ExpirationInDays": 730,
                                    }
                                ),
                            ]
                        ),
                    }
                ),
            },
        )

    def test_public_site_bucket_versioning_disabled(self, template: assertions.Template) -> None:
        buckets = template.find_resources(
            "AWS::S3::Bucket",
            {
                "Properties": {
                    "LifecycleConfiguration": assertions.Match.object_like(
                        {
                            "Rules": assertions.Match.array_with(
                                [assertions.Match.object_like({"Id": "ExpireOldReleases"})]
                            )
                        }
                    )
                }
            },
        )
        for bucket in buckets.values():
            assert "VersioningConfiguration" not in bucket.get("Properties", {}), (
                "Public site bucket should not have versioning enabled"
            )

    def test_both_buckets_enforce_ssl(self, template: assertions.Template) -> None:
        policies = template.find_resources("AWS::S3::BucketPolicy")
        ssl_enforced_count = 0
        for policy in policies.values():
            statements = policy.get("Properties", {}).get("PolicyDocument", {}).get("Statement", [])
            for stmt in statements:
                condition = stmt.get("Condition", {})
                if "Bool" in condition and "aws:SecureTransport" in condition["Bool"]:
                    ssl_enforced_count += 1
                    break
        assert ssl_enforced_count >= 2, (
            f"Expected SSL enforcement on at least 2 buckets, found {ssl_enforced_count}"
        )


# ---------------------------------------------------------------------------
# SNS
# ---------------------------------------------------------------------------


class TestSns:
    def test_quarantine_topic_exists(self, template: assertions.Template) -> None:
        template.has_resource_properties(
            "AWS::SNS::Topic",
            {"TopicName": "nova-cat-quarantine-notifications"},
        )


# ---------------------------------------------------------------------------
# Lambda
# ---------------------------------------------------------------------------

_EXPECTED_FUNCTIONS: dict[str, dict[str, int]] = {
    "nova-cat-nova-resolver": {"memory": 256, "timeout": 30},
    "nova-cat-job-run-manager": {"memory": 256, "timeout": 30},
    "nova-cat-idempotency-guard": {"memory": 256, "timeout": 30},
    "nova-cat-archive-resolver": {"memory": 256, "timeout": 90},
    "nova-cat-workflow-launcher": {"memory": 256, "timeout": 30},
    "nova-cat-reference-manager": {"memory": 256, "timeout": 90},
    "nova-cat-spectra-discoverer": {"memory": 256, "timeout": 60},
    "nova-cat-spectra-acquirer": {"memory": 512, "timeout": 900},
    "nova-cat-spectra-validator": {"memory": 512, "timeout": 300},
    "nova-cat-photometry-ingestor": {"memory": 512, "timeout": 300},
    "nova-cat-quarantine-handler": {"memory": 256, "timeout": 30},
    "nova-cat-name-reconciler": {"memory": 256, "timeout": 90},
}

_REQUIRED_ENV_VARS = {
    "NOVA_CAT_TABLE_NAME",
    "NOVA_CAT_PRIVATE_BUCKET",
    "NOVA_CAT_PUBLIC_SITE_BUCKET",
    "NOVA_CAT_QUARANTINE_TOPIC_ARN",
    "LOG_LEVEL",
    "POWERTOOLS_SERVICE_NAME",
}


class TestLambda:
    def test_all_twelve_functions_exist(self, template: assertions.Template) -> None:
        functions = template.find_resources("AWS::Lambda::Function")
        function_names = {
            props.get("Properties", {}).get("FunctionName") for props in functions.values()
        }
        for expected_name in _EXPECTED_FUNCTIONS:
            assert expected_name in function_names, (
                f"Lambda function '{expected_name}' not found in template"
            )

    def test_all_functions_use_python_311(self, template: assertions.Template) -> None:
        for fn_name in _EXPECTED_FUNCTIONS:
            template.has_resource_properties(
                "AWS::Lambda::Function",
                {
                    "FunctionName": fn_name,
                    "Runtime": "python3.11",
                },
            )

    def test_all_functions_have_xray_tracing(self, template: assertions.Template) -> None:
        for fn_name in _EXPECTED_FUNCTIONS:
            template.has_resource_properties(
                "AWS::Lambda::Function",
                {
                    "FunctionName": fn_name,
                    "TracingConfig": {"Mode": "Active"},
                },
            )

    def test_all_functions_have_required_env_vars(self, template: assertions.Template) -> None:
        functions = template.find_resources("AWS::Lambda::Function")
        for resource in functions.values():
            props = resource.get("Properties", {})
            fn_name = props.get("FunctionName", "")
            if fn_name not in _EXPECTED_FUNCTIONS:
                continue
            env_vars = props.get("Environment", {}).get("Variables", {})
            missing = _REQUIRED_ENV_VARS - set(env_vars.keys())
            assert not missing, f"Function '{fn_name}' is missing env vars: {missing}"

    @pytest.mark.parametrize("fn_name,config", _EXPECTED_FUNCTIONS.items())
    def test_function_memory_and_timeout(
        self,
        template: assertions.Template,
        fn_name: str,
        config: dict[str, int],
    ) -> None:
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": fn_name,
                "MemorySize": config["memory"],
                "Timeout": config["timeout"],
            },
        )


# ---------------------------------------------------------------------------
# Stack outputs
# ---------------------------------------------------------------------------


class TestOutputs:
    def test_table_name_output_exists(self, template: assertions.Template) -> None:
        template.has_output("*", {"Export": {"Name": "NovaCat-TableName"}})

    def test_private_bucket_output_exists(self, template: assertions.Template) -> None:
        template.has_output("*", {"Export": {"Name": "NovaCat-PrivateBucketName"}})

    def test_public_site_bucket_output_exists(self, template: assertions.Template) -> None:
        template.has_output("*", {"Export": {"Name": "NovaCat-PublicSiteBucketName"}})

    def test_quarantine_topic_output_exists(self, template: assertions.Template) -> None:
        template.has_output("*", {"Export": {"Name": "NovaCat-QuarantineTopicArn"}})
