from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import (
    aws_dynamodb as dynamodb,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as _lambda,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_stepfunctions as sfn,
)
from aws_cdk import (
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class NovaCatStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- DynamoDB (single table) ---
        # Table name is normative: `NovaCat`. :contentReference[oaicite:6]{index=6}
        table = dynamodb.Table(
            self,
            "NovaCatTable",
            table_name="NovaCat",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,  # flip to RETAIN when ready
        )

        # GSI1 EligibilityIndex (normative) :contentReference[oaicite:7]{index=7}
        table.add_global_secondary_index(
            index_name="EligibilityIndex",
            partition_key=dynamodb.Attribute(name="GSI1PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="GSI1SK", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # --- S3 buckets ---
        raw_bucket = s3.Bucket(
            self,
            "RawBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        derived_bucket = s3.Bucket(
            self,
            "DerivedBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        site_bucket = s3.Bucket(
            self,
            "SiteBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # --- Lambda: task router (manifest-driven) ---
        router_fn = _lambda.Function(
            self,
            "TaskRouterFn",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.main",
            code=_lambda.Code.from_asset("lambdas/task_router"),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "NOVA_CAT_TABLE_NAME": table.table_name,
                "NOVA_CAT_RAW_BUCKET": raw_bucket.bucket_name,
                "NOVA_CAT_DERIVED_BUCKET": derived_bucket.bucket_name,
                "NOVA_CAT_SITE_BUCKET": site_bucket.bucket_name,
                "NOVA_CAT_LOG_LEVEL": "INFO",
                # router uses this to locate task implementations
                "NOVA_CAT_TASK_MANIFEST": "NOVA_CAT_EPIC5",
            },
        )

        # Minimal permissions only:
        table.grant_read_write_data(router_fn)
        raw_bucket.grant_read_write(router_fn)
        derived_bucket.grant_read_write(router_fn)
        site_bucket.grant_read_write(router_fn)

        # --- Step Functions: state machines ---
        # We keep workflows as workflows (not single-lambda) per your spec. :contentReference[oaicite:8]{index=8}

        def lambda_task(state_name: str) -> tasks.LambdaInvoke:
            # Use StepFunctions context to pass state name + attempt count
            # (router writes Attempt records keyed by task_name=state_name). :contentReference[oaicite:9]{index=9}
            return tasks.LambdaInvoke(
                self,
                f"Invoke{state_name}",
                lambda_function=router_fn,
                payload=sfn.TaskInput.from_object(
                    {
                        "input.$": "$",
                        "context": {
                            "state_name": state_name,
                            "execution_arn.$": "$$.Execution.Id",
                            "entered_time.$": "$$.State.EnteredTime",
                            "retry_count.$": "$$.State.RetryCount",
                        },
                    }
                ),
                output_path="$.Payload",
            )

        # initialize_nova (thin but aligned to explicit list) :contentReference[oaicite:10]{index=10}
        initialize_def = (
            sfn.Pass(self, "ValidateInput")
            .next(lambda_task("BeginJobRun"))
            .next(lambda_task("NormalizeCandidateName"))
            .next(lambda_task("CheckExistingNovaByName"))
            # Router returns {"exists_in_db": bool, ...}
            .next(
                sfn.Choice(self, "ExistsInDB?")
                .when(
                    sfn.Condition.boolean_equals("$.exists_in_db", True),
                    lambda_task("PublishIngestNewNova").next(lambda_task("FinalizeJobRunSuccess")),
                )
                .otherwise(
                    # Thin-slice: skip public resolvers + coordinate logic for now (explicitly omitted later)
                    lambda_task("CreateNovaId")
                    .next(lambda_task("UpsertMinimalNovaMetadata"))
                    .next(lambda_task("PublishIngestNewNova"))
                    .next(lambda_task("FinalizeJobRunSuccess"))
                )
            )
        )

        initialize_sm = sfn.StateMachine(
            self,
            "InitializeNovaStateMachine",
            state_machine_name="initialize_nova",
            definition_body=sfn.DefinitionBody.from_chainable(initialize_def),
            timeout=Duration.minutes(10),
        )

        # ingest_new_nova (coordinator; launches discover_spectra_products only in this slice)
        # Launch semantics are explicit in spec. :contentReference[oaicite:11]{index=11}
        ingest_def = (
            sfn.Pass(self, "ValidateInput_ingest_new_nova")
            .next(lambda_task("BeginJobRun_ingest_new_nova"))
            .next(lambda_task("EnsurePhotometryTableProduct"))
            .next(lambda_task("LaunchDiscoverSpectraProducts"))
            .next(lambda_task("FinalizeJobRunSuccess_ingest_new_nova"))
        )

        ingest_sm = sfn.StateMachine(
            self,
            "IngestNewNovaStateMachine",
            state_machine_name="ingest_new_nova",
            definition_body=sfn.DefinitionBody.from_chainable(ingest_def),
            timeout=Duration.minutes(10),
        )

        # discover_spectra_products (Map across providers; concurrency 1 is normative MVP default) :contentReference[oaicite:12]{index=12}
        provider_map = sfn.Map(
            self,
            "DiscoverAcrossProviders",
            max_concurrency=1,
            items_path="$.providers",
            result_path="$.provider_results",
        )

        provider_map.iterator(
            lambda_task("QueryProviderForProducts")
            .next(lambda_task("NormalizeProviderProducts"))
            .next(lambda_task("DeduplicateAndAssignDataProductIds"))
            .next(lambda_task("PersistDataProductMetadata"))
            .next(lambda_task("PublishAcquireAndValidateSpectraRequests"))
        )

        discover_def = (
            sfn.Pass(self, "ValidateInput_discover_spectra_products")
            .next(lambda_task("BeginJobRun_discover_spectra_products"))
            .next(provider_map)
            .next(lambda_task("SummarizeDiscovery"))
            .next(lambda_task("FinalizeJobRunSuccess_discover_spectra_products"))
        )

        discover_sm = sfn.StateMachine(
            self,
            "DiscoverSpectraProductsStateMachine",
            state_machine_name="discover_spectra_products",
            definition_body=sfn.DefinitionBody.from_chainable(discover_def),
            timeout=Duration.minutes(10),
        )

        # acquire_and_validate_spectra (thin but respects skip rules + eligibility removal semantics later)
        acquire_def = (
            sfn.Pass(self, "ValidateInput_acquire_and_validate_spectra")
            .next(lambda_task("BeginJobRun_acquire_and_validate_spectra"))
            .next(lambda_task("LoadDataProductMetadata"))
            .next(lambda_task("CheckOperationalStatus"))
            .next(
                sfn.Choice(self, "AlreadyValidated?")
                .when(
                    sfn.Condition.boolean_equals("$.already_validated", True),
                    lambda_task("FinalizeJobRunSuccess_acquire_and_validate_spectra"),
                )
                .otherwise(
                    sfn.Choice(self, "CooldownActive?")
                    .when(
                        sfn.Condition.boolean_equals("$.cooldown_active", True),
                        lambda_task("FinalizeJobRunSuccess_acquire_and_validate_spectra"),
                    )
                    .otherwise(
                        lambda_task("AcquireArtifact")
                        .next(lambda_task("ValidateBytes_Profile_Driven"))
                        .next(lambda_task("RecordValidationResult"))
                        .next(lambda_task("FinalizeJobRunSuccess_acquire_and_validate_spectra"))
                    )
                )
            )
        )

        acquire_sm = sfn.StateMachine(
            self,
            "AcquireAndValidateSpectraStateMachine",
            state_machine_name="acquire_and_validate_spectra",
            definition_body=sfn.DefinitionBody.from_chainable(acquire_def),
            timeout=Duration.minutes(10),
        )

        # --- Allow router to start downstream workflows (minimal IAM) ---
        # initialize_nova publishes ingest_new_nova; ingest_new_nova launches discover; discovery publishes acquire.
        router_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[
                    ingest_sm.state_machine_arn,
                    discover_sm.state_machine_arn,
                    acquire_sm.state_machine_arn,
                ],
            )
        )

        # Expose ARNs as outputs (handy for manual testing)
        cdk.CfnOutput(self, "InitializeNovaArn", value=initialize_sm.state_machine_arn)
        cdk.CfnOutput(self, "IngestNewNovaArn", value=ingest_sm.state_machine_arn)
        cdk.CfnOutput(self, "DiscoverSpectraArn", value=discover_sm.state_machine_arn)
        cdk.CfnOutput(self, "AcquireValidateSpectraArn", value=acquire_sm.state_machine_arn)
