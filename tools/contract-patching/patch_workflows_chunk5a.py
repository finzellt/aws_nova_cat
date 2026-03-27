#!/usr/bin/env python3
"""
Chunk 5a patch — workflows.py additions for ingest_ticket workflow.

Changes:
  1. Updates the NovaCatWorkflows class docstring to document ingest_ticket.
  2. Registers the ingest_ticket state machine via _create_state_machine().
  3. Grants nova_resolver_ticket:
       - sfn:StartExecution on the initialize_nova state machine (to invoke it)
       - sfn:DescribeExecution on initialize_nova executions (for polling)
     These grants live here (not compute.py) because NovaCatWorkflows owns the
     state machine ARNs — consistent with how workflow_launcher's SFN grants
     are managed in this file.
  4. Injects INITIALIZE_NOVA_STATE_MACHINE_ARN into nova_resolver_ticket via
     add_environment (same pattern as workflow_launcher env vars).
  5. Adds a CfnOutput for the ingest_ticket state machine ARN.

Usage:
    python patch_workflows_chunk5a.py path/to/infra/nova_constructs/workflows.py

Precondition assertions abort with a clear message if the target text is not
found exactly as expected — safe to re-run after a failed partial application.
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

    # -------------------------------------------------------------------------
    # Precondition checks
    # -------------------------------------------------------------------------
    _require(
        src,
        "acquire_and_validate_spectra — the acquire_and_validate_spectra state machine (placeholder stub)",
        "class docstring sentinel",
    )
    _require(
        src,
        '"AcquireAndValidateSpectraStateMachineArn",',
        "AcquireAndValidateSpectra CfnOutput logical ID",
    )
    _require(
        src,
        "def _create_state_machine(",
        "_create_state_machine definition",
    )
    _require(
        src,
        'export_name=f"{cf_prefix}-AcquireAndValidateSpectraStateMachineArn",\n        )\n\n    def _create_state_machine(',
        "CfnOutput block → _create_state_machine transition",
    )
    _require(
        src,
        "self.initialize_nova = self._create_state_machine(",
        "initialize_nova state machine registration",
    )
    _require(
        src,
        "_grant_start_execution(compute.workflow_launcher, cdk.Stack.of(self))",
        "_grant_start_execution call for workflow_launcher",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Class docstring: add ingest_ticket to the Exposes list.
    # =========================================================================
    OLD_DOCSTRING = (
        "      acquire_and_validate_spectra — the acquire_and_validate_spectra"
        " state machine (placeholder stub)"
    )
    NEW_DOCSTRING = (
        "      acquire_and_validate_spectra — the acquire_and_validate_spectra"
        " state machine (placeholder stub)\n"
        "      ingest_ticket              — the ingest_ticket state machine"
    )
    src = _replace_once(src, OLD_DOCSTRING, NEW_DOCSTRING, "class docstring Exposes list")

    # =========================================================================
    # Patch 2 — Register ingest_ticket state machine + SFN grants for
    #           nova_resolver_ticket, inserted immediately before the Stack
    #           outputs block (i.e. before the first CfnOutput after
    #           initialize_nova).
    #
    # Anchor: the closing paren of the last _create_state_machine call
    # (initialize_nova) runs right into the "Stack outputs" comment.
    # =========================================================================
    OLD_OUTPUTS_HEADER = """\
        # ------------------------------------------------------------------
        # Stack outputs
        # ------------------------------------------------------------------
        cdk.CfnOutput(
            self,
            "InitializeNovaStateMachineArn","""

    NEW_OUTPUTS_HEADER = """\
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
            "InitializeNovaStateMachineArn","""

    src = _replace_once(
        src, OLD_OUTPUTS_HEADER, NEW_OUTPUTS_HEADER, "Stack outputs header → ingest_ticket block"
    )

    # =========================================================================
    # Patch 3 — CfnOutput: add IngestTicket output after the existing
    #           AcquireAndValidateSpectra output, immediately before
    #           _create_state_machine.
    # =========================================================================
    OLD_LAST_OUTPUT = """\
        cdk.CfnOutput(
            self,
            "AcquireAndValidateSpectraStateMachineArn",
            value=self.acquire_and_validate_spectra.attr_arn,
            description="acquire_and_validate_spectra Step Functions state machine ARN (placeholder stub)",
            export_name=f"{cf_prefix}-AcquireAndValidateSpectraStateMachineArn",
        )

    def _create_state_machine("""

    NEW_LAST_OUTPUT = """\
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

    def _create_state_machine("""

    src = _replace_once(src, OLD_LAST_OUTPUT, NEW_LAST_OUTPUT, "CfnOutput for IngestTicket")

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        ("ingest_ticket              — the ingest_ticket state machine", "class docstring updated"),
        ('name="ingest-ticket",', "ingest_ticket state machine name"),
        (
            '"TicketParserFunctionArn": compute.ticket_parser.function_arn,',
            "TicketParser substitution",
        ),
        (
            '"NovaResolverTicketFunctionArn": compute.nova_resolver_ticket.function_arn,',
            "NovaResolverTicket substitution",
        ),
        (
            '"TicketIngestorFunctionArn": compute.ticket_ingestor.function_arn,',
            "TicketIngestor substitution",
        ),
        ("compute.ticket_parser,", "ticket_parser in invokable_functions"),
        ("compute.nova_resolver_ticket,", "nova_resolver_ticket in invokable_functions"),
        ("compute.ticket_ingestor,", "ticket_ingestor in invokable_functions"),
        ('actions=["states:StartExecution"]', "StartExecution grant"),
        ('actions=["states:DescribeExecution"]', "DescribeExecution grant"),
        ("initialize_nova_executions_arn", "execution ARN variable"),
        ('"INITIALIZE_NOVA_STATE_MACHINE_ARN",', "INITIALIZE_NOVA env var injection"),
        ('"IngestTicketStateMachineArn",', "IngestTicket CfnOutput"),
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
    print("  1. Apply storage.py patch (Chunk 5a file 4) to add photometry_table.")
    print("  2. Update nova_cat_stack.py: pass photometry_table=self.storage.photometry_table")
    print("     to the NovaCatCompute constructor.")
    print("  3. Run: mypy --strict infra/ && ruff check infra/")


if __name__ == "__main__":
    main()
