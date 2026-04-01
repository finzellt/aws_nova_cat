#!/usr/bin/env python3
"""
Epic 2 patch — compute.py additions for artifact regeneration pipeline.

Changes:
  1. Adds ``artifact_coordinator`` and ``artifact_finalizer`` to
     ``_FUNCTION_SPECS``.
  2. Adds attribute declarations for both on ``NovaCatCompute``.
  3. Adds IAM grants in ``_grant_permissions``:
     - artifact_coordinator: table read/write (WORKQUEUE queries, REGEN_PLAN
       read/write).  sfn:StartExecution is granted in workflows.py.
     - artifact_finalizer: table read/write (REGEN_PLAN status updates,
       WorkItem batch deletes, Nova item count writeback).

Usage:
    python patch_compute_epic2.py path/to/infra/nova_constructs/compute.py

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
        print(f"Usage: {sys.argv[0]} <path/to/compute.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # =========================================================================
    # Precondition checks
    # =========================================================================
    _require(
        src,
        '"nova_resolver_ticket": _FunctionSpec(',
        "nova_resolver_ticket in _FUNCTION_SPECS (last existing entry)",
    )
    _require(
        src,
        "ticket_ingestor: lambda_.DockerImageFunction",
        "ticket_ingestor attribute declaration",
    )
    _require(
        src,
        "# ingest_ticket workflow grants (Chunk 5a)",
        "ingest_ticket grants section header",
    )
    _require(
        src,
        "public_site_bucket.grant_write(\n"
        '            self._functions["ticket_ingestor"],\n'
        '            "raw/*",\n'
        "        )",
        "ticket_ingestor public_site_bucket grant (last grant in _grant_permissions)",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add artifact_coordinator and artifact_finalizer to
    #           _FUNCTION_SPECS, after nova_resolver_ticket.
    # =========================================================================
    OLD_SPECS_END = """\
    "nova_resolver_ticket": _FunctionSpec(
        service_dir="nova_resolver_ticket",
        description=(
            "Resolves OBJECT NAME to nova_id via NameMapping. Invokes initialize_nova "
            "if absent (StartSyncExecution — Express workflow). Raises UNRESOLVABLE_OBJECT_NAME "
            "or IDENTITY_AMBIGUITY for quarantine outcomes. "
            "Handles ResolveNova. Used by: ingest_ticket."
        ),
        timeout=cdk.Duration.seconds(120),  # initialize_nova may take up to ~60s
    ),
}"""

    NEW_SPECS_END = """\
    "nova_resolver_ticket": _FunctionSpec(
        service_dir="nova_resolver_ticket",
        description=(
            "Resolves OBJECT NAME to nova_id via NameMapping. Invokes initialize_nova "
            "if absent (StartSyncExecution — Express workflow). Raises UNRESOLVABLE_OBJECT_NAME "
            "or IDENTITY_AMBIGUITY for quarantine outcomes. "
            "Handles ResolveNova. Used by: ingest_ticket."
        ),
        timeout=cdk.Duration.seconds(120),  # initialize_nova may take up to ~60s
    ),
    # ------------------------------------------------------------------
    # regenerate_artifacts workflow functions (Epic 2)
    # ------------------------------------------------------------------
    "artifact_coordinator": _FunctionSpec(
        service_dir="artifact_coordinator",
        description=(
            "Sweep coordinator: queries WORKQUEUE, builds per-nova manifests via "
            "the dependency matrix, persists a RegenBatchPlan, and launches the "
            "regenerate_artifacts Step Functions workflow. "
            "Invoked by EventBridge (6h cron) or manually. "
            "Handles: single entry point (no task_name dispatch). "
            "Used by: regenerate_artifacts (EventBridge → coordinator → SFn)."
        ),
        timeout=cdk.Duration.seconds(60),  # paginated WORKQUEUE query + plan write + SFn start
    ),
    "artifact_finalizer": _FunctionSpec(
        service_dir="artifact_finalizer",
        description=(
            "Sweep finalization: commits succeeded novae (deletes consumed WorkItems, "
            "writes observation counts to Nova DDB items), updates RegenBatchPlan status. "
            "Also handles UpdatePlanInProgress and FailHandler for Fargate crashes. "
            "Handles UpdatePlanInProgress, Finalize, FailHandler. "
            "Used by: regenerate_artifacts."
        ),
        timeout=cdk.Duration.seconds(300),  # batch WorkItem deletes for large sweeps
    ),
}"""

    src = _replace_once(src, OLD_SPECS_END, NEW_SPECS_END, "_FUNCTION_SPECS additions")

    # =========================================================================
    # Patch 2 — Add attribute declarations on NovaCatCompute class,
    #           after ticket_ingestor.
    # =========================================================================
    OLD_ATTRS_END = "    ticket_ingestor: lambda_.DockerImageFunction"

    NEW_ATTRS_END = """\
    ticket_ingestor: lambda_.DockerImageFunction
    # regenerate_artifacts workflow (Epic 2)
    artifact_coordinator: lambda_.Function
    artifact_finalizer: lambda_.Function"""

    src = _replace_once(src, OLD_ATTRS_END, NEW_ATTRS_END, "attribute declarations")

    # =========================================================================
    # Patch 3 — Add IAM grants after the ingest_ticket grants block.
    #
    # Anchor: the last grant in _grant_permissions is ticket_ingestor's
    # public_site_bucket.grant_write.
    # =========================================================================
    OLD_LAST_GRANT = """\
        public_site_bucket.grant_write(
            self._functions["ticket_ingestor"],
            "raw/*",
        )"""

    NEW_LAST_GRANT = """\
        public_site_bucket.grant_write(
            self._functions["ticket_ingestor"],
            "raw/*",
        )

        # ------------------------------------------------------------------
        # regenerate_artifacts workflow grants (Epic 2)
        # ------------------------------------------------------------------

        # artifact_coordinator: reads WORKQUEUE + REGEN_PLAN partitions,
        # writes REGEN_PLAN items.  sfn:StartExecution on the
        # regenerate_artifacts state machine is granted in workflows.py.
        table.grant_read_write_data(self._functions["artifact_coordinator"])

        # artifact_finalizer: reads REGEN_PLAN (plan loading), deletes
        # WORKQUEUE items (batch_write_item), writes observation counts
        # to Nova items (PK=<nova_id>, SK=NOVA), updates REGEN_PLAN status.
        table.grant_read_write_data(self._functions["artifact_finalizer"])"""

    src = _replace_once(src, OLD_LAST_GRANT, NEW_LAST_GRANT, "IAM grants for Epic 2")

    # =========================================================================
    # Post-condition checks
    # =========================================================================
    checks = [
        ('"artifact_coordinator": _FunctionSpec(', "artifact_coordinator in _FUNCTION_SPECS"),
        ('"artifact_finalizer": _FunctionSpec(', "artifact_finalizer in _FUNCTION_SPECS"),
        ("artifact_coordinator: lambda_.Function", "artifact_coordinator attribute declaration"),
        ("artifact_finalizer: lambda_.Function", "artifact_finalizer attribute declaration"),
        (
            'table.grant_read_write_data(self._functions["artifact_coordinator"])',
            "artifact_coordinator IAM grant",
        ),
        (
            'table.grant_read_write_data(self._functions["artifact_finalizer"])',
            "artifact_finalizer IAM grant",
        ),
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
    print("  1. Apply patch_workflows_epic2.py to register the regenerate_artifacts")
    print("     state machine, EventBridge rule, and CloudWatch alarms.")
    print("  2. Run: mypy --strict infra/ && ruff check infra/")


if __name__ == "__main__":
    main()
