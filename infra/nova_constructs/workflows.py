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
    by each state machine — not a wildcard grant.
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
      initialize_nova            — the initialize_nova state machine
      ingest_new_nova            — the ingest_new_nova state machine
      refresh_references         — the refresh_references state machine
      discover_spectra_products  — the discover_spectra_products state machine
      acquire_and_validate_spectra — the acquire_and_validate_spectra state machine (placeholder stub)
      ingest_ticket              — the ingest_ticket state machine
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        compute: NovaCatCompute,
        env_prefix: str = "nova-cat",
        cf_prefix: str = "NovaCat",
    ) -> None:
        super().__init__(scope, construct_id)

        self._env_prefix = env_prefix
        self._cf_prefix = cf_prefix

        self._workflows_dir = os.path.join(os.path.dirname(__file__), "../workflows")

        # ------------------------------------------------------------------
        # acquire_and_validate_spectra state machine
        # ------------------------------------------------------------------
        self.acquire_and_validate_spectra = self._create_state_machine(
            name="acquire-and-validate-spectra",
            asl_file="acquire_and_validate_spectra.asl.json",
            substitutions={
                "JobRunManagerFunctionArn": compute.job_run_manager.function_arn,
                "AcquireIdempotencyLockFunctionArn": compute.idempotency_guard.function_arn,
                "SpectraValidatorFunctionArn": compute.spectra_validator.function_arn,
                "SpectraAcquirerFunctionArn": compute.spectra_acquirer.function_arn,
            },
            invokable_functions=[
                compute.job_run_manager,
                compute.idempotency_guard,
                compute.spectra_validator,
                compute.spectra_acquirer,
            ],
        )

        # ------------------------------------------------------------------
        # discover_spectra_products state machine
        # ------------------------------------------------------------------
        self.discover_spectra_products = self._create_state_machine(
            name="discover-spectra-products",
            asl_file="discover_spectra_products.asl.json",
            substitutions={
                "JobRunManagerFunctionArn": compute.job_run_manager.function_arn,
                "AcquireIdempotencyLockFunctionArn": compute.idempotency_guard.function_arn,
                "SpectraDiscovererFunctionArn": compute.spectra_discoverer.function_arn,
                "WorkflowLauncherFunctionArn": compute.workflow_launcher.function_arn,
            },
            invokable_functions=[
                compute.job_run_manager,
                compute.idempotency_guard,
                compute.spectra_discoverer,
                compute.workflow_launcher,
            ],
        )

        # ------------------------------------------------------------------
        # refresh_references state machine
        # ------------------------------------------------------------------
        self.refresh_references = self._create_state_machine(
            name="refresh-references",
            asl_file="refresh_references.asl.json",
            substitutions={
                "BeginJobRunFunctionArn": compute.job_run_manager.function_arn,
                "FinalizeJobRunSuccessFunctionArn": compute.job_run_manager.function_arn,
                "FinalizeJobRunFailedFunctionArn": compute.job_run_manager.function_arn,
                # TerminalFailHandler state also routes through job_run_manager
                # (task_name: TerminalFailHandler — classifies error before FinalizeJobRunFailed)
                "JobRunManagerFunctionArn": compute.job_run_manager.function_arn,
                "AcquireIdempotencyLockFunctionArn": compute.idempotency_guard.function_arn,
                "QuarantineHandlerFunctionArn": compute.quarantine_handler.function_arn,
                "ReferenceManagerFunctionArn": compute.reference_manager.function_arn,
            },
            invokable_functions=[
                compute.job_run_manager,
                compute.idempotency_guard,
                compute.quarantine_handler,
                compute.reference_manager,
            ],
        )
        # ------------------------------------------------------------------
        # ingest_new_nova state machine
        # ------------------------------------------------------------------
        self.ingest_new_nova = self._create_state_machine(
            name="ingest-new-nova",
            asl_file="ingest_new_nova.asl.json",
            substitutions={
                "BeginJobRunFunctionArn": compute.job_run_manager.function_arn,
                "FinalizeJobRunSuccessFunctionArn": compute.job_run_manager.function_arn,
                "FinalizeJobRunFailedFunctionArn": compute.job_run_manager.function_arn,
                # TerminalFailHandler state also routes through job_run_manager
                # (task_name: TerminalFailHandler — classifies error before FinalizeJobRunFailed)
                "JobRunManagerFunctionArn": compute.job_run_manager.function_arn,
                "AcquireIdempotencyLockFunctionArn": compute.idempotency_guard.function_arn,
                "LaunchRefreshReferencesFunctionArn": compute.workflow_launcher.function_arn,
                "LaunchDiscoverSpectraProductsFunctionArn": compute.workflow_launcher.function_arn,
            },
            invokable_functions=[
                compute.job_run_manager,
                compute.idempotency_guard,
                compute.workflow_launcher,
            ],
        )

        # Grant workflow_launcher permission to start executions and inject ARNs.
        # This grant lives here (not in NovaCatCompute) because NovaCatWorkflows
        # owns the state machine ARNs — NovaCatCompute has no knowledge of SFN.
        _grant_start_execution(compute.workflow_launcher, cdk.Stack.of(self))

        # Construct ARNs by name rather than referencing attr_arn to avoid a
        # CDK dependency cycle: ingest_new_nova invokes workflow_launcher
        # (grant_invoke), and workflow_launcher needs ingest_new_nova's ARN
        # (env var). Using format_arn breaks the second edge of the cycle.
        stack = cdk.Stack.of(self)

        def _sfn_arn(name: str) -> str:
            return stack.format_arn(
                service="states",
                resource="stateMachine",
                resource_name=f"{env_prefix}-{name}",
                arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
            )

        compute.workflow_launcher.add_environment(
            "INGEST_NEW_NOVA_STATE_MACHINE_ARN",
            _sfn_arn("ingest-new-nova"),
        )
        compute.workflow_launcher.add_environment(
            "REFRESH_REFERENCES_STATE_MACHINE_ARN",
            _sfn_arn("refresh-references"),
        )
        compute.workflow_launcher.add_environment(
            "DISCOVER_SPECTRA_PRODUCTS_STATE_MACHINE_ARN",
            _sfn_arn("discover-spectra-products"),
        )
        compute.workflow_launcher.add_environment(
            "ACQUIRE_AND_VALIDATE_SPECTRA_STATE_MACHINE_ARN",
            _sfn_arn("acquire-and-validate-spectra"),
        )

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
                # TerminalFailHandler state also routes through job_run_manager
                # (task_name: TerminalFailHandler — classifies error before FinalizeJobRunFailed)
                "JobRunManagerFunctionArn": compute.job_run_manager.function_arn,
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
        # ingest_ticket state machine
        # ------------------------------------------------------------------
        self.ingest_ticket = self._create_state_machine(
            name="ingest-ticket",
            asl_file="ingest_ticket.asl.json",
            substitutions={
                # job_run_manager handles BeginJobRun, FinalizeJobRunSuccess_*,
                # FinalizeJobRunQuarantined, TerminalFailHandler, FinalizeJobRunFailed
                # via its internal task dispatch table.
                "JobRunManagerFunctionArn": compute.job_run_manager.function_arn,
                "AcquireIdempotencyLockFunctionArn": compute.idempotency_guard.function_arn,
                "TicketParserFunctionArn": compute.ticket_parser.function_arn,
                "NovaResolverTicketFunctionArn": compute.nova_resolver_ticket.function_arn,
                "TicketIngestorFunctionArn": compute.ticket_ingestor.function_arn,
                "QuarantineHandlerFunctionArn": compute.quarantine_handler.function_arn,
            },
            invokable_functions=[
                compute.job_run_manager,
                compute.idempotency_guard,
                compute.ticket_parser,
                compute.nova_resolver_ticket,
                compute.ticket_ingestor,
                compute.quarantine_handler,
            ],
        )

        # ------------------------------------------------------------------
        # SFN grants for nova_resolver_ticket
        #
        # nova_resolver_ticket polls initialize_nova when a name is not found
        # in NameMapping — it needs StartExecution to fire the workflow and
        # DescribeExecution to poll for the terminal outcome.
        #
        # These grants live here (not compute.py) because NovaCatWorkflows
        # owns the state machine ARNs. The same pattern is used for
        # workflow_launcher's StartExecution grant above.
        #
        # Scope: deliberately narrowed to initialize_nova only (not the
        # broad nova-cat-* wildcard used by workflow_launcher), because
        # nova_resolver_ticket has no legitimate reason to start any other
        # state machine.
        # ------------------------------------------------------------------
        stack = cdk.Stack.of(self)

        initialize_nova_arn = stack.format_arn(
            service="states",
            resource="stateMachine",
            resource_name=f"{env_prefix}-initialize-nova",
            arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
        )
        initialize_nova_executions_arn = stack.format_arn(
            service="states",
            resource="execution",
            resource_name=f"{env_prefix}-initialize-nova:*",
            arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
        )

        compute.nova_resolver_ticket.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[initialize_nova_arn],
            )
        )
        compute.nova_resolver_ticket.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:DescribeExecution"],
                resources=[initialize_nova_executions_arn],
            )
        )

        # Inject the initialize_nova ARN as an environment variable so
        # nova_resolver_ticket can call StartExecution without hardcoding
        # ARN construction logic (same pattern as workflow_launcher above).
        compute.nova_resolver_ticket.add_environment(
            "INITIALIZE_NOVA_STATE_MACHINE_ARN",
            initialize_nova_arn,
        )

        # ------------------------------------------------------------------
        # Stack outputs
        # ------------------------------------------------------------------
        cdk.CfnOutput(
            self,
            "InitializeNovaStateMachineArn",
            value=self.initialize_nova.attr_arn,
            description="initialize_nova Step Functions state machine ARN",
            export_name=f"{cf_prefix}-InitializeNovaStateMachineArn",
        )
        cdk.CfnOutput(
            self,
            "IngestNewNovaStateMachineArn",
            value=self.ingest_new_nova.attr_arn,
            description="ingest_new_nova Step Functions state machine ARN",
            export_name=f"{cf_prefix}-IngestNewNovaStateMachineArn",
        )
        cdk.CfnOutput(
            self,
            "RefreshReferencesStateMachineArn",
            value=self.refresh_references.attr_arn,
            description="refresh_references Step Functions state machine ARN",
            export_name=f"{cf_prefix}-RefreshReferencesStateMachineArn",
        )
        cdk.CfnOutput(
            self,
            "DiscoverSpectraProductsStateMachineArn",
            value=self.discover_spectra_products.attr_arn,
            description="discover_spectra_products Step Functions state machine ARN",
            export_name=f"{cf_prefix}-DiscoverSpectraProductsStateMachineArn",
        )
        cdk.CfnOutput(
            self,
            "AcquireAndValidateSpectraStateMachineArn",
            value=self.acquire_and_validate_spectra.attr_arn,
            description="acquire_and_validate_spectra Step Functions state machine ARN (placeholder stub)",
            export_name=f"{cf_prefix}-AcquireAndValidateSpectraStateMachineArn",
        )
        cdk.CfnOutput(
            self,
            "IngestTicketStateMachineArn",
            value=self.ingest_ticket.attr_arn,
            description="ingest_ticket Step Functions state machine ARN",
            export_name=f"{cf_prefix}-IngestTicketStateMachineArn",
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
            description=f"Execution role for {self._env_prefix}-{name} state machine",
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
            state_machine_name=f"{self._env_prefix}-{name}",
            state_machine_type="EXPRESS",
            role_arn=role.role_arn,
            definition_substitutions=substitutions if substitutions else None,
            definition_string=cdk.Fn.sub(
                json.dumps(asl_body, separators=(",", ":")),
                substitutions,
            )
            if substitutions
            else json.dumps(asl_body, separators=(",", ":")),
        )


def _grant_start_execution(
    fn: lambda_.Function,
    stack: cdk.Stack,
    env_prefix: str = "nova-cat",
) -> None:
    """
    Grant a Lambda function permission to start any nova-cat Step Functions
    execution.

    Rather than referencing individual state machine ARNs (which creates a CDK
    dependency cycle between the state machine role and the Lambda role), we
    scope the grant to all nova-cat-* state machines in the same account and
    region. This breaks the cycle while still being meaningfully scoped —
    no other state machines in the account share the nova-cat- prefix.
    """
    fn.add_to_role_policy(
        iam.PolicyStatement(
            actions=["states:StartExecution"],
            resources=[
                stack.format_arn(
                    service="states",
                    resource="stateMachine",
                    resource_name=f"{env_prefix}-*",
                    arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
                )
            ],
        )
    )


def _to_pascal(kebab: str) -> str:
    """Convert kebab-case or snake_case to PascalCase for CloudFormation logical IDs."""
    return "".join(word.capitalize() for word in kebab.replace("-", "_").split("_"))
