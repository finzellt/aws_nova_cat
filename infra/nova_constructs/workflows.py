"""
Nova Cat Workflows Construct

Provisions Step Functions state machines for all Nova Cat workflows.

Each state machine is defined as an ASL JSON file under infra/workflows/,
with ${FunctionArn} tokens substituted at synth time using CDK's
CfnStateMachine and definition_substitutions.

Design decisions:
  - Standard Workflows (not Express): Nova Cat operates at low throughput
    with operator-triggered executions. Standard Workflows provide exact-once
    semantics, unlimited duration, and full execution history — appropriate
    for a scientific data pipeline where auditability matters.
  - ASL files are kept as plain JSON rather than CDK's higher-level
    StepFunctions constructs (Chain, Task, etc.) — the ASL is the spec
    artifact and keeping it as readable JSON makes it easier to validate
    against the workflow specs and share with non-CDK contexts.
  - Token substitution uses CloudFormation's Fn::Sub via CfnStateMachine,
    which resolves Lambda ARNs at deploy time rather than synth time.
    This means the ASL JSON stays portable and readable.
  - IAM role grants invoke permission only on the Lambdas actually used
    by each state machine — not all 12.
"""

from __future__ import annotations

import json
import os

import aws_cdk as cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_stepfunctions as sfn
from constructs import Construct

from nova_constructs.compute import NovaCatCompute


class NovaCatWorkflows(Construct):
    """
    Workflows layer for Nova Cat.

    Provisions one Step Functions state machine per workflow, wired to
    the Lambda functions from NovaCatCompute.

    Exposes:
      initialize_nova  — the initialize_nova state machine
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        compute: NovaCatCompute,
    ) -> None:
        super().__init__(scope, construct_id)

        self._workflows_dir = os.path.join(os.path.dirname(__file__), "../workflows")

        # ------------------------------------------------------------------
        # initialize_nova state machine
        # ------------------------------------------------------------------
        self.initialize_nova = self._create_state_machine(
            name="initialize-nova",
            asl_file="initialize_nova.asl.json",
            # Token → Lambda mapping: every ${TokenName} in the ASL maps
            # to the ARN of the Lambda that handles those task states.
            # Multiple tokens may map to the same Lambda (e.g. job_run_manager
            # handles all FinalizeJobRun* tasks via its dispatch table).
            substitutions={
                "BeginJobRunFunctionArn": compute.job_run_manager.function_arn,
                "FinalizeJobRunSuccessFunctionArn": compute.job_run_manager.function_arn,
                "FinalizeJobRunFailedFunctionArn": compute.job_run_manager.function_arn,
                "FinalizeJobRunQuarantinedFunctionArn": compute.job_run_manager.function_arn,
                "AcquireIdempotencyLockFunctionArn": compute.idempotency_guard.function_arn,
                "NormalizeCandidateNameFunctionArn": compute.nova_resolver.function_arn,
                "CheckExistingNovaByNameFunctionArn": compute.nova_resolver.function_arn,
                "CheckExistingNovaByCoordinatesFunctionArn": compute.nova_resolver.function_arn,
                "CreateNovaIdFunctionArn": compute.nova_resolver.function_arn,
                "UpsertMinimalNovaMetadataFunctionArn": compute.nova_resolver.function_arn,
                "UpsertAliasForExistingNovaFunctionArn": compute.nova_resolver.function_arn,
                "ResolveCandidateAgainstPublicArchivesFunctionArn": compute.archive_resolver.function_arn,
                "PublishIngestNewNovaFunctionArn": compute.workflow_launcher.function_arn,
                "QuarantineHandlerFunctionArn": compute.quarantine_handler.function_arn,
                "TerminalFailHandlerFunctionArn": compute.quarantine_handler.function_arn,
            },
            invokable_functions=[
                compute.job_run_manager,
                compute.idempotency_guard,
                compute.nova_resolver,
                compute.archive_resolver,
                compute.workflow_launcher,
                compute.quarantine_handler,
            ],
        )

        # ------------------------------------------------------------------
        # Stack outputs
        # ------------------------------------------------------------------
        cdk.CfnOutput(
            self,
            "InitializeNovaStateMachineArn",
            value=self.initialize_nova.attr_arn,
            description="initialize_nova Step Functions state machine ARN",
            export_name="NovaCat-InitializeNovaStateMachineArn",
        )

    def _create_state_machine(
        self,
        name: str,
        asl_file: str,
        substitutions: dict[str, str],
        invokable_functions: list[lambda_.Function],
    ) -> sfn.CfnStateMachine:
        """
        Create a Standard Workflow state machine from an ASL file.

        Loads the ASL JSON, substitutes ${Token} placeholders with Lambda ARNs
        via CloudFormation Fn::Sub, and provisions the state machine with a
        least-privilege IAM execution role.

        Args:
            name:                Hyphenated state machine name (e.g. "initialize-nova")
            asl_file:            Filename under infra/workflows/
            substitutions:       Mapping of token name → Lambda ARN (CDK token)
            invokable_functions: Lambda functions this state machine may invoke
        """
        asl_path = os.path.join(self._workflows_dir, asl_file)
        with open(asl_path) as f:
            asl_body = json.load(f)

        # IAM execution role for the state machine
        role = iam.Role(
            self,
            f"{_to_pascal(name)}Role",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            description=f"Execution role for nova-cat-{name} state machine",
        )

        # Grant lambda:InvokeFunction scoped to only the Lambdas this
        # workflow actually calls — not a wildcard grant.
        for fn in invokable_functions:
            fn.grant_invoke(role)

        # Allow the state machine to write execution history to CloudWatch Logs
        role.add_to_policy(
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

        # CfnStateMachine (L1) rather than StateMachine (L2) because the L2
        # doesn't cleanly support loading ASL from a file with CloudFormation
        # token substitution. definition_string + definition_substitutions
        # maps directly to CloudFormation's DefinitionString + Fn::Sub.
        return sfn.CfnStateMachine(
            self,
            _to_pascal(name),
            state_machine_name=f"nova-cat-{name}",
            state_machine_type="STANDARD",
            role_arn=role.role_arn,
            definition_substitutions=substitutions,
            definition_string=cdk.Fn.sub(
                json.dumps(asl_body, separators=(",", ":")),
                substitutions,
            ),
        )


def _to_pascal(kebab: str) -> str:
    """Convert kebab-case or snake_case to PascalCase for CloudFormation logical IDs."""
    return "".join(word.capitalize() for word in kebab.replace("-", "_").split("_"))
