#!/usr/bin/env python3
"""
run_refresh_references.py — Operator tool for running refresh_references
on individual novae or batches filtered by reference count.

Usage:
    # 1. List all novae with reference counts
    python tools/run_refresh_references.py --list

    # 2. Run for a single nova (by name), wait for completion
    python tools/run_refresh_references.py --name "V5668 Sgr"

    # 3. Run for a single nova (by UUID)
    python tools/run_refresh_references.py --nova-id <uuid>

    # 4. Run for all novae with fewer than X references
    python tools/run_refresh_references.py --below 5

    # 5. Dry run (show what would be launched, don't launch)
    python tools/run_refresh_references.py --below 5 --dry-run

    # 6. Skip waiting (fire and forget)
    python tools/run_refresh_references.py --name "IM Nor" --no-wait

Personal operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid

import boto3
from boto3.dynamodb.conditions import Key

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
_TABLE_NAME = os.environ.get("NOVA_CAT_TABLE_NAME", "nova-cat")
_POLL_INTERVAL_S = 10
_DEFAULT_TIMEOUT_S = 300  # 5 min per execution

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------

_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_CYAN = "\033[96m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"

# ---------------------------------------------------------------------------
# AWS clients (lazy init)
# ---------------------------------------------------------------------------

_ddb = None
_sfn = None
_table = None
_sm_arn = None


def _init_clients():
    global _ddb, _sfn, _table
    _ddb = boto3.resource("dynamodb", region_name=_REGION)
    _sfn = boto3.client("stepfunctions", region_name=_REGION)
    _table = _ddb.Table(_TABLE_NAME)


def _get_state_machine_arn() -> str:
    """Find the refresh-references state machine ARN."""
    global _sm_arn
    if _sm_arn:
        return _sm_arn

    paginator = _sfn.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page["stateMachines"]:
            if "refresh-references" in sm["name"] and "nova-cat" in sm["name"]:
                _sm_arn = sm["stateMachineArn"]
                return _sm_arn

    print(f"  {_RED}✗ Could not find nova-cat-refresh-references state machine{_RESET}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# DDB queries
# ---------------------------------------------------------------------------


def _resolve_name_to_nova_id(name: str) -> str | None:
    """Resolve a nova name to nova_id via the NAME# partition."""
    normalized = name.strip().lower().replace("_", " ")
    pk = f"NAME#{normalized}"
    resp = _table.query(
        KeyConditionExpression=Key("PK").eq(pk),
        Limit=1,
    )
    items = resp.get("Items", [])
    if not items:
        return None
    return items[0].get("nova_id")


def _scan_active_novae() -> list[dict]:
    """Scan for all ACTIVE Nova items."""
    items = []
    kwargs = {
        "FilterExpression": "entity_type = :et AND #s = :status",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":et": "Nova", ":status": "ACTIVE"},
        "ProjectionExpression": "nova_id, primary_name",
    }
    while True:
        resp = _table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if resp.get("LastEvaluatedKey") is None:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    items.sort(key=lambda x: x.get("primary_name", ""))
    return items


def _count_references(nova_id: str) -> int:
    """Count NOVAREF# items for a nova."""
    count = 0
    kwargs = {
        "KeyConditionExpression": Key("PK").eq(nova_id) & Key("SK").begins_with("NOVAREF#"),
        "Select": "COUNT",
    }
    while True:
        resp = _table.query(**kwargs)
        count += resp.get("Count", 0)
        if resp.get("LastEvaluatedKey") is None:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return count


def _list_novae_with_ref_counts() -> list[dict]:
    """Return all ACTIVE novae with their reference counts, sorted by count ascending."""
    novae = _scan_active_novae()
    results = []
    for nova in novae:
        nova_id = nova["nova_id"]
        name = nova.get("primary_name", "(unnamed)")
        ref_count = _count_references(nova_id)
        results.append(
            {
                "nova_id": nova_id,
                "primary_name": name,
                "ref_count": ref_count,
            }
        )
    results.sort(key=lambda x: (x["ref_count"], x["primary_name"]))
    return results


# ---------------------------------------------------------------------------
# Workflow execution
# ---------------------------------------------------------------------------


def _start_refresh_references(nova_id: str) -> str:
    """Start a refresh_references execution. Returns execution ARN."""
    sm_arn = _get_state_machine_arn()
    correlation_id = f"manual-refresh-{uuid.uuid4().hex[:12]}"
    job_run_id = str(uuid.uuid4())
    execution_name = f"{nova_id[:30]}-{job_run_id[:8]}"

    resp = _sfn.start_execution(
        stateMachineArn=sm_arn,
        name=execution_name,
        input=json.dumps(
            {
                "nova_id": nova_id,
                "correlation_id": correlation_id,
            }
        ),
    )
    return resp["executionArn"]


def _poll_execution(exec_arn: str, timeout: int) -> str:
    """Poll an execution until terminal. Returns final status."""
    deadline = time.time() + timeout

    while time.time() < deadline:
        desc = _sfn.describe_execution(executionArn=exec_arn)
        status = desc["status"]
        remaining = int(deadline - time.time())

        print(
            f"\r    {_DIM}⏳ {status}  ({remaining}s remaining){_RESET}      ",
            end="",
            flush=True,
        )

        if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            print()  # newline after the \r line
            return status

        time.sleep(_POLL_INTERVAL_S)

    print()
    return "TIMEOUT_LOCAL"


def _run_single(
    nova_id: str,
    name: str,
    wait: bool = True,
    timeout: int = _DEFAULT_TIMEOUT_S,
) -> str:
    """Run refresh_references for a single nova. Returns final status."""
    print(f"\n  {_BOLD}Launching refresh_references{_RESET}")
    print(f"    Nova:    {_CYAN}{name}{_RESET}")
    print(f"    nova_id: {_DIM}{nova_id}{_RESET}")

    try:
        exec_arn = _start_refresh_references(nova_id)
    except Exception as e:
        error_msg = str(e)
        # Handle ExecutionAlreadyExists — idempotent success
        if "ExecutionAlreadyExists" in error_msg:
            print(f"    {_YELLOW}⚠ Execution already exists (idempotent){_RESET}")
            return "ALREADY_EXISTS"
        print(f"    {_RED}✗ Failed to start: {e}{_RESET}")
        return "LAUNCH_FAILED"

    exec_name = exec_arn.split(":")[-1]
    print(f"    Exec:    {_DIM}{exec_name}{_RESET}")

    if not wait:
        print(f"    {_DIM}Launched (not waiting){_RESET}")
        return "LAUNCHED"

    status = _poll_execution(exec_arn, timeout)

    if status == "SUCCEEDED":
        print(f"    {_GREEN}✓ Succeeded{_RESET}")
    elif status == "TIMEOUT_LOCAL":
        print(f"    {_YELLOW}⚠ Local timeout ({timeout}s) — execution may still be running{_RESET}")
    else:
        print(f"    {_RED}✗ {status}{_RESET}")

    return status


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------


def cmd_list(args):
    """List all novae with reference counts."""
    _init_clients()
    print(f"\n{_BOLD}Scanning novae and counting references...{_RESET}\n")

    results = _list_novae_with_ref_counts()

    # Table header
    print(f"  {'Nova':<30} {'Refs':>6}   {'nova_id'}")
    print(f"  {'─' * 30} {'─' * 6}   {'─' * 36}")

    for r in results:
        ref_str = str(r["ref_count"])
        color = _RED if r["ref_count"] == 0 else (_YELLOW if r["ref_count"] < 5 else "")
        reset = _RESET if color else ""
        print(
            f"  {r['primary_name']:<30} {color}{ref_str:>6}{reset}   {_DIM}{r['nova_id']}{_RESET}"
        )

    total = len(results)
    zero = sum(1 for r in results if r["ref_count"] == 0)
    print(f"\n  {_DIM}{total} novae total, {zero} with zero references{_RESET}\n")


def cmd_single(args):
    """Run refresh_references for a single nova."""
    _init_clients()

    if args.nova_id:
        nova_id = args.nova_id
        name = f"(uuid: {nova_id[:12]}…)"
        # Try to find the name
        novae = _scan_active_novae()
        for n in novae:
            if n["nova_id"] == nova_id:
                name = n.get("primary_name", name)
                break
    else:
        nova_id = _resolve_name_to_nova_id(args.name)
        if not nova_id:
            print(f"\n  {_RED}✗ Could not resolve name: {args.name!r}{_RESET}\n")
            sys.exit(1)
        name = args.name

    ref_count = _count_references(nova_id)
    print(f"\n  {_DIM}Current references: {ref_count}{_RESET}")

    status = _run_single(
        nova_id,
        name,
        wait=not args.no_wait,
        timeout=args.timeout,
    )

    if status == "SUCCEEDED" and not args.no_wait:
        new_count = _count_references(nova_id)
        delta = new_count - ref_count
        delta_str = f"+{delta}" if delta >= 0 else str(delta)
        print(f"\n  References: {ref_count} → {new_count} ({delta_str})")

    print()


def cmd_batch(args):
    """Run refresh_references for all novae below a reference threshold."""
    _init_clients()

    threshold = args.below
    print(f"\n{_BOLD}Scanning novae with fewer than {threshold} references...{_RESET}\n")

    all_novae = _list_novae_with_ref_counts()
    targets = [r for r in all_novae if r["ref_count"] < threshold]

    if not targets:
        print(f"  {_DIM}No novae found with fewer than {threshold} references.{_RESET}\n")
        return

    print(f"  Found {len(targets)} novae:\n")
    for r in targets:
        print(f"    {r['primary_name']:<30} ({r['ref_count']} refs)")

    if args.dry_run:
        print(f"\n  {_YELLOW}[DRY RUN]{_RESET} Would launch {len(targets)} executions.\n")
        return

    # Confirm
    print()
    confirm = (
        input(f"  Launch {len(targets)} refresh_references executions? [y/N] ").strip().lower()
    )
    if confirm != "y":
        print(f"\n  {_DIM}Aborted.{_RESET}\n")
        return

    # Run sequentially with wait
    print()
    results = {"SUCCEEDED": 0, "FAILED": 0, "OTHER": 0}

    for i, r in enumerate(targets, 1):
        print(f"  {_DIM}[{i}/{len(targets)}]{_RESET}")
        status = _run_single(
            r["nova_id"],
            r["primary_name"],
            wait=not args.no_wait,
            timeout=args.timeout,
        )

        if status == "SUCCEEDED":
            results["SUCCEEDED"] += 1
        elif status in ("FAILED", "TIMED_OUT", "ABORTED", "LAUNCH_FAILED"):
            results["FAILED"] += 1
        else:
            results["OTHER"] += 1

        # Brief pause between launches to avoid throttling
        if i < len(targets):
            time.sleep(2)

    # Summary
    print(f"\n{_BOLD}Batch complete:{_RESET}")
    print(f"  {_GREEN}✓ Succeeded: {results['SUCCEEDED']}{_RESET}")
    if results["FAILED"]:
        print(f"  {_RED}✗ Failed:    {results['FAILED']}{_RESET}")
    if results["OTHER"]:
        print(f"  {_YELLOW}? Other:     {results['OTHER']}{_RESET}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Run refresh_references on individual or batched novae.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    # -- list --
    list_parser = subparsers.add_parser("list", help="List all novae with reference counts")
    list_parser.set_defaults(func=cmd_list)

    # -- run (single) --
    run_parser = subparsers.add_parser("run", help="Run refresh_references for a single nova")
    id_group = run_parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument("--nova-id", help="Nova UUID")
    id_group.add_argument("--name", help="Nova name (resolved via NameMapping)")
    run_parser.add_argument("--no-wait", action="store_true", help="Don't wait for completion")
    run_parser.add_argument(
        "--timeout",
        type=int,
        default=_DEFAULT_TIMEOUT_S,
        help=f"Max seconds to wait (default: {_DEFAULT_TIMEOUT_S})",
    )
    run_parser.set_defaults(func=cmd_single)

    # -- batch --
    batch_parser = subparsers.add_parser("batch", help="Run for novae below a reference threshold")
    batch_parser.add_argument(
        "--below", type=int, required=True, help="Reference count threshold (exclusive)"
    )
    batch_parser.add_argument(
        "--dry-run", action="store_true", help="Show targets without launching"
    )
    batch_parser.add_argument(
        "--no-wait", action="store_true", help="Don't wait for each execution"
    )
    batch_parser.add_argument(
        "--timeout",
        type=int,
        default=_DEFAULT_TIMEOUT_S,
        help=f"Max seconds per execution (default: {_DEFAULT_TIMEOUT_S})",
    )
    batch_parser.set_defaults(func=cmd_batch)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
