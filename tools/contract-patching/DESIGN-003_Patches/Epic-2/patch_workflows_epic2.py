#!/usr/bin/env python3
"""
Epic 2 patch — workflows.py additions for artifact regeneration pipeline.

Changes:
  1. Adds imports for ``events``, ``cloudwatch``, ``ecs``, ``ec2``, ``sns``.
  2. Updates the NovaCatWorkflows class docstring to document
     ``regenerate_artifacts``.
  3. Adds constructor parameters: ``vpc``, ``quarantine_topic``,
     ``private_bucket``, ``public_site_bucket``, ``table``.
  4. Creates an ECS cluster and Fargate task definition for the artifact
     generator container (2 vCPU / 8 GB, §4.4).
  5. Registers the ``regenerate_artifacts`` state machine as a **Standard**
     workflow (not Express — the .sync ECS integration can wait hours).
  6. Grants the state machine IAM role: Lambda invoke (finalizer) +
     ECS RunTask + IAM PassRole for the task execution/task roles.
  7. Grants the artifact_coordinator sfn:StartExecution on the workflow.
  8. Injects ``REGENERATE_ARTIFACTS_STATE_MACHINE_ARN`` into the
     artifact_coordinator Lambda.
  9. Creates an EventBridge rule (6-hour cron) invoking the coordinator.
  10. Creates two CloudWatch alarms: sweep failure and 48-hour sweep skip
      (§15.4).
  11. Adds a CfnOutput for the regenerate_artifacts state machine ARN.

Usage:
    python patch_workflows_epic2.py path/to/infra/nova_constructs/workflows.py

After applying this patch:
  1. Update nova_cat_stack.py to pass the new constructor parameters to
     NovaCatWorkflows:
       - vpc=ec2.Vpc.from_lookup(self, "Vpc", is_default=True)
         (or your preferred VPC construct)
       - quarantine_topic=self.storage.quarantine_topic
       - private_bucket=self.storage.private_bucket
       - public_site_bucket=self.storage.public_site_bucket
       - table=self.storage.table
  2. Create services/artifact_generator/Dockerfile for the Fargate
     container image (Epic 3 — stub image is sufficient for Epic 2
     testing).
  3. Run: mypy --strict infra/ && ruff check infra/
"""

from __future__ import annotations

import sys


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _replace_once(content: str, old: str, new: str, label: str) -> str:
    if old not in content:
        print(f"REPLACE FAILED — anchor for {label!r} not found.")
        sys.exit(1)
    if content.count(old) > 1:
        print(f"REPLACE FAILED — anchor for {label!r} appears more than once.")
        sys.exit(1)
    return content.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/workflows.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # =========================================================================
    # Precondition checks
    # =========================================================================
    _require(
        src,
        "ingest_ticket              — the ingest_ticket state machine",
        "ingest_ticket in class docstring (Chunk 5a must be applied first)",
    )
    _require(
        src,
        '"IngestTicketStateMachineArn",',
        "IngestTicket CfnOutput (last output before _create_state_machine)",
    )
    _require(
        src,
        "import aws_cdk.aws_stepfunctions as sfn",
        "sfn import",
    )
    _require(
        src,
        "from nova_constructs.compute import NovaCatCompute",
        "NovaCatCompute import",
    )
    _require(
        src,
        'export_name=f"{cf_prefix}-IngestTicketStateMachineArn",',
        "IngestTicket CfnOutput export_name",
    )
    _require(
        src,
        "def _create_state_machine(",
        "_create_state_machine definition",
    )
    _require(
        src,
        '"INITIALIZE_NOVA_STATE_MACHINE_ARN",',
        "INITIALIZE_NOVA env var injection (anchor for coordinator env var)",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add imports for events, cloudwatch, ecs, ec2, sns, s3,
    #           dynamodb after the existing sfn import.
    # =========================================================================
    OLD_IMPORTS = "import aws_cdk.aws_stepfunctions as sfn"

    NEW_IMPORTS = """\
import aws_cdk.aws_cloudwatch as cloudwatch
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as events_targets
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sns as sns
import aws_cdk.aws_stepfunctions as sfn"""

    src = _replace_once(src, OLD_IMPORTS, NEW_IMPORTS, "additional CDK imports")

    # =========================================================================
    # Patch 2 — Update class docstring to include regenerate_artifacts.
    # =========================================================================
    OLD_DOCSTRING = "      ingest_ticket              — the ingest_ticket state machine"
    NEW_DOCSTRING = (
        "      ingest_ticket              — the ingest_ticket state machine\n"
        "      regenerate_artifacts       — the artifact regeneration sweep workflow (Standard)"
    )
    src = _replace_once(src, OLD_DOCSTRING, NEW_DOCSTRING, "class docstring Exposes list")

    # =========================================================================
    # Patch 3 — Add constructor parameters after cf_prefix.
    # =========================================================================
    OLD_CTOR_PARAMS = """\
        compute: NovaCatCompute,
        env_prefix: str = "nova-cat",
        cf_prefix: str = "NovaCat","""

    NEW_CTOR_PARAMS = """\
        compute: NovaCatCompute,
        vpc: ec2.IVpc,
        table: dynamodb.Table,
        private_bucket: s3.Bucket,
        public_site_bucket: s3.Bucket,
        quarantine_topic: sns.Topic,
        sweep_schedule_hours: int = 6,
        env_prefix: str = "nova-cat",
        cf_prefix: str = "NovaCat","""

    src = _replace_once(src, OLD_CTOR_PARAMS, NEW_CTOR_PARAMS, "constructor parameters")

    # =========================================================================
    # Patch 4 — Add the regenerate_artifacts workflow block, Fargate task
    #           definition, EventBridge rule, CloudWatch alarms, and
    #           coordinator env var injection.
    #
    # Anchor: after INITIALIZE_NOVA_STATE_MACHINE_ARN injection, before
    # the Stack outputs section.
    # =========================================================================
    OLD_BEFORE_OUTPUTS = """\
        compute.nova_resolver_ticket.add_environment(
            "INITIALIZE_NOVA_STATE_MACHINE_ARN",
            initialize_nova_arn,
        )

        # ------------------------------------------------------------------
        # Stack outputs
        # ------------------------------------------------------------------"""

    NEW_BEFORE_OUTPUTS = """\
        compute.nova_resolver_ticket.add_environment(
            "INITIALIZE_NOVA_STATE_MACHINE_ARN",
            initialize_nova_arn,
        )

        # ------------------------------------------------------------------
        # regenerate_artifacts — Fargate task definition (§4.4)
        # ------------------------------------------------------------------

        # ECS cluster — shared by all Fargate tasks (only one at MVP).
        cluster = ecs.Cluster(
            self, "ArtifactCluster",
            cluster_name=f"{env_prefix}-artifact-cluster",
            vpc=vpc,
        )

        # Fargate task definition: 2 vCPU / 8 GB (§4.4 MVP sizing).
        task_def = ecs.FargateTaskDefinition(
            self, "ArtifactGeneratorTaskDef",
            family=f"{env_prefix}-artifact-generator",
            cpu=2048,
            memory_limit_mib=8192,
        )

        # Container — builds from services/artifact_generator/Dockerfile.
        # The build context is services/ (same pattern as Docker Lambdas).
        services_root = os.path.join(os.path.dirname(__file__), "../../services")
        task_def.add_container(
            "artifact-generator",
            image=ecs.ContainerImage.from_asset(
                services_root,
                file="artifact_generator/Dockerfile",
            ),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="artifact-generator",
            ),
            environment={
                "NOVA_CAT_TABLE_NAME": table.table_name,
                "NOVA_CAT_PRIVATE_BUCKET": private_bucket.bucket_name,
                "NOVA_CAT_PUBLIC_SITE_BUCKET": public_site_bucket.bucket_name,
                "LOG_LEVEL": "INFO",
                # PLAN_ID is injected at runtime via container overrides
            },
        )

        # Task role grants (the Fargate task's runtime identity)
        table.grant_read_write_data(task_def.task_role)
        private_bucket.grant_read(task_def.task_role)
        public_site_bucket.grant_read_write(task_def.task_role)

        # Resolve subnet IDs for the ASL substitution.
        # Use public subnets with auto-assign public IP for ECR image pull
        # (avoids NAT Gateway cost at MVP — §15.8).
        subnet_ids = [s.subnet_id for s in vpc.public_subnets]
        subnet_ids_json = cdk.Fn.join(
            '","', [cdk.Token.as_string(sid) for sid in subnet_ids]
        )
        subnet_ids_sub = cdk.Fn.join("", ['"["', subnet_ids_json, '"]"'])

        # ------------------------------------------------------------------
        # regenerate_artifacts — Standard Workflow (§4.5)
        #
        # Standard (not Express) because the .sync ECS integration can
        # wait for hours.  Built directly rather than via
        # _create_state_machine which hardcodes EXPRESS.
        # ------------------------------------------------------------------
        asl_path = os.path.join(self._workflows_dir, "regenerate_artifacts.asl.json")
        with open(asl_path) as f:
            asl_body = json.load(f)

        regen_role = iam.Role(
            self,
            "RegenerateArtifactsRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            description=(
                f"Execution role for {env_prefix}-regenerate-artifacts "
                f"Standard Workflow"
            ),
        )

        # Lambda invoke for the finalizer (3 task_names: UpdatePlanInProgress,
        # Finalize, FailHandler)
        compute.artifact_finalizer.grant_invoke(regen_role)

        # ECS RunTask for the .sync integration
        regen_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ecs:RunTask"],
                resources=[task_def.task_definition_arn],
            )
        )
        regen_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ecs:StopTask", "ecs:DescribeTasks"],
                resources=["*"],
                conditions={
                    "ArnEquals": {"ecs:cluster": cluster.cluster_arn},
                },
            )
        )
        # PassRole for the ECS task execution role and task role
        regen_role.add_to_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[
                    task_def.execution_role.role_arn if task_def.execution_role else "*",
                    task_def.task_role.role_arn,
                ],
            )
        )
        # Events integration for .sync (SFn polls ECS on our behalf)
        regen_role.add_to_policy(
            iam.PolicyStatement(
                actions=["events:PutTargets", "events:PutRule", "events:DescribeRule"],
                resources=[
                    cdk.Stack.of(self).format_arn(
                        service="events",
                        resource="rule",
                        resource_name="StepFunctionsGetEventsForECSTaskRule",
                    )
                ],
            )
        )
        # CloudWatch Logs
        regen_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups",
                ],
                resources=["*"],
            )
        )

        regen_substitutions = {
            "ArtifactFinalizerFunctionArn": compute.artifact_finalizer.function_arn,
            "EcsClusterArn": cluster.cluster_arn,
            "TaskDefinitionArn": task_def.task_definition_arn,
            "SubnetIdsJson": subnet_ids_sub,
        }

        self.regenerate_artifacts = sfn.CfnStateMachine(
            self,
            "RegenerateArtifacts",
            state_machine_name=f"{env_prefix}-regenerate-artifacts",
            state_machine_type="STANDARD",
            role_arn=regen_role.role_arn,
            definition_substitutions=regen_substitutions,
            definition_string=cdk.Fn.sub(
                json.dumps(asl_body, separators=(",", ":")),
                regen_substitutions,
            ),
        )

        # ------------------------------------------------------------------
        # Coordinator: sfn:StartExecution + env var injection
        # ------------------------------------------------------------------
        regen_sfn_arn = cdk.Stack.of(self).format_arn(
            service="states",
            resource="stateMachine",
            resource_name=f"{env_prefix}-regenerate-artifacts",
            arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
        )

        compute.artifact_coordinator.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[regen_sfn_arn],
            )
        )

        compute.artifact_coordinator.add_environment(
            "REGENERATE_ARTIFACTS_STATE_MACHINE_ARN",
            regen_sfn_arn,
        )

        # ------------------------------------------------------------------
        # EventBridge rule: 6-hour sweep cadence (§4.1)
        # ------------------------------------------------------------------
        sweep_rule = events.Rule(
            self,
            "SweepScheduleRule",
            rule_name=f"{env_prefix}-artifact-sweep",
            schedule=events.Schedule.rate(
                cdk.Duration.hours(sweep_schedule_hours),
            ),
            description=(
                f"Invokes artifact_coordinator every {sweep_schedule_hours} hours "
                f"to trigger artifact regeneration sweeps (DESIGN-003 §4.1)."
            ),
        )
        sweep_rule.add_target(
            events_targets.LambdaFunction(compute.artifact_coordinator)
        )

        # ------------------------------------------------------------------
        # CloudWatch alarms (§15.4)
        # ------------------------------------------------------------------

        # Sweep failure alarm — fires when the workflow enters FAILED state.
        cloudwatch.Alarm(
            self,
            "SweepFailureAlarm",
            alarm_name=f"{env_prefix}-sweep-failure",
            metric=cloudwatch.Metric(
                namespace="AWS/States",
                metric_name="ExecutionsFailed",
                dimensions_map={
                    "StateMachineArn": self.regenerate_artifacts.attr_arn,
                },
                statistic="Sum",
                period=cdk.Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=(
                cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
            ),
            alarm_description="Artifact regeneration sweep failed (DESIGN-003 §15.4)",
            actions_enabled=True,
        )

        # Sweep skip alarm — fires when no sweep has succeeded in 48 hours.
        cloudwatch.Alarm(
            self,
            "SweepSkipAlarm",
            alarm_name=f"{env_prefix}-sweep-skip",
            metric=cloudwatch.Metric(
                namespace="AWS/States",
                metric_name="ExecutionsSucceeded",
                dimensions_map={
                    "StateMachineArn": self.regenerate_artifacts.attr_arn,
                },
                statistic="Sum",
                period=cdk.Duration.hours(48),
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=(
                cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD
            ),
            alarm_description=(
                "No artifact regeneration sweep has succeeded in 48 hours "
                "(DESIGN-003 §15.4)"
            ),
            actions_enabled=True,
        )

        # ------------------------------------------------------------------
        # Stack outputs
        # ------------------------------------------------------------------"""

    src = _replace_once(src, OLD_BEFORE_OUTPUTS, NEW_BEFORE_OUTPUTS, "regenerate_artifacts block")

    # =========================================================================
    # Patch 5 — Add CfnOutput for regenerate_artifacts after IngestTicket
    #           output, before _create_state_machine.
    # =========================================================================
    OLD_LAST_OUTPUT = """\
        cdk.CfnOutput(
            self,
            "IngestTicketStateMachineArn",
            value=self.ingest_ticket.attr_arn,
            description="ingest_ticket Step Functions state machine ARN",
            export_name=f"{cf_prefix}-IngestTicketStateMachineArn",
        )

    def _create_state_machine("""

    NEW_LAST_OUTPUT = """\
        cdk.CfnOutput(
            self,
            "IngestTicketStateMachineArn",
            value=self.ingest_ticket.attr_arn,
            description="ingest_ticket Step Functions state machine ARN",
            export_name=f"{cf_prefix}-IngestTicketStateMachineArn",
        )
        cdk.CfnOutput(
            self,
            "RegenerateArtifactsStateMachineArn",
            value=self.regenerate_artifacts.attr_arn,
            description="regenerate_artifacts Step Functions state machine ARN (Standard Workflow)",
            export_name=f"{cf_prefix}-RegenerateArtifactsStateMachineArn",
        )

    def _create_state_machine("""

    src = _replace_once(src, OLD_LAST_OUTPUT, NEW_LAST_OUTPUT, "CfnOutput for RegenerateArtifacts")

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        ("import aws_cdk.aws_cloudwatch as cloudwatch", "cloudwatch import"),
        ("import aws_cdk.aws_ecs as ecs", "ecs import"),
        ("import aws_cdk.aws_events as events", "events import"),
        ("import aws_cdk.aws_events_targets as events_targets", "events_targets import"),
        ("import aws_cdk.aws_ec2 as ec2", "ec2 import"),
        ("import aws_cdk.aws_sns as sns", "sns import"),
        (
            "regenerate_artifacts       — the artifact regeneration sweep workflow",
            "class docstring updated",
        ),
        ("vpc: ec2.IVpc,", "vpc constructor parameter"),
        ("quarantine_topic: sns.Topic,", "quarantine_topic constructor parameter"),
        ("sweep_schedule_hours: int = 6,", "sweep_schedule_hours constructor parameter"),
        ('cluster_name=f"{env_prefix}-artifact-cluster"', "ECS cluster"),
        ('family=f"{env_prefix}-artifact-generator"', "Fargate task definition"),
        ("cpu=2048,", "Fargate CPU sizing"),
        ("memory_limit_mib=8192,", "Fargate memory sizing"),
        ('state_machine_type="STANDARD"', "Standard Workflow type"),
        ('"ArtifactFinalizerFunctionArn":', "finalizer substitution"),
        ('"EcsClusterArn":', "ECS cluster substitution"),
        ('"TaskDefinitionArn":', "task definition substitution"),
        ('"SubnetIdsJson":', "subnet IDs substitution"),
        ('"REGENERATE_ARTIFACTS_STATE_MACHINE_ARN"', "coordinator env var"),
        ('actions=["states:StartExecution"]', "coordinator SFn grant"),
        ('rule_name=f"{env_prefix}-artifact-sweep"', "EventBridge rule"),
        ('"SweepFailureAlarm"', "sweep failure alarm"),
        ('"SweepSkipAlarm"', "sweep skip alarm"),
        ('"RegenerateArtifactsStateMachineArn"', "CfnOutput"),
    ]

    failed = False
    for marker, label in checks:
        if marker not in src:
            print(f"POSTCONDITION FAILED — {label!r}")
            failed = True

    if failed:
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")
    print()
    print("Next steps:")
    print("  1. Update nova_cat_stack.py to pass new NovaCatWorkflows constructor params:")
    print("       vpc=ec2.Vpc.from_lookup(self, 'Vpc', is_default=True)")
    print("       table=self.storage.table")
    print("       private_bucket=self.storage.private_bucket")
    print("       public_site_bucket=self.storage.public_site_bucket")
    print("       quarantine_topic=self.storage.quarantine_topic")
    print("  2. Create services/artifact_generator/Dockerfile")
    print("     (stub image is sufficient for Epic 2 end-to-end testing)")
    print("  3. Update tests/smoke/conftest.py:")
    print("       - Add 'artifact-coordinator' and 'artifact-finalizer' to _LAMBDA_SUFFIXES")
    print("       - Add 'regenerate-artifacts' to _STATE_MACHINE_SUFFIXES")
    print("       - Add 'regenerate_artifacts_arn' to _CF_EXPORT_MAP")
    print("  4. Run: mypy --strict infra/ && ruff check infra/")


if __name__ == "__main__":
    main()
