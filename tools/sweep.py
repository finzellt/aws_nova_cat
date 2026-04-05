#!/usr/bin/env python3
"""
Trigger a manual artifact regeneration sweep.

Invokes the artifact_coordinator Lambda, which queries the WORKQUEUE,
builds a batch plan, and launches the regenerate_artifacts workflow.
If no WorkItems are pending, exits cleanly with "no_work".

Usage:
    python tools/sweep.py          # trigger and show result
    python tools/sweep.py --wait   # trigger and poll until the sweep completes

Personal operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import boto3

_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"


def main() -> None:
    parser = argparse.ArgumentParser(description="Trigger a manual artifact sweep.")
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Poll the Step Functions execution until it completes",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1200,
        help="Max seconds to wait for sweep completion (default: 1200)",
    )
    args = parser.parse_args()

    lam = boto3.client("lambda", region_name=_REGION)
    sfn = boto3.client("stepfunctions", region_name=_REGION)

    # -- Invoke coordinator --
    print(f"\n{_BOLD}Invoking artifact_coordinator...{_RESET}\n")

    resp = lam.invoke(FunctionName="nova-cat-artifact-coordinator", Payload=b"{}")
    payload = json.loads(resp["Payload"].read())

    if resp.get("FunctionError"):
        print(f"  {_RED}✗ Lambda error:{_RESET} {payload.get('errorMessage')}")
        sys.exit(1)

    action = payload.get("action", "unknown")
    plan_id = payload.get("plan_id")
    nova_count = payload.get("nova_count")

    if action == "launched":
        print(f"  {_GREEN}✓{_RESET} Sweep launched")
        if plan_id:
            print(f"    plan_id:    {plan_id}")
        if nova_count is not None:
            print(f"    nova_count: {nova_count}")
    elif action == "no_work":
        print(f"  {_DIM}No pending WorkItems — nothing to sweep.{_RESET}")
        sys.exit(0)
    elif action == "in_progress":
        print(f"  {_YELLOW}⚠{_RESET} A sweep is already in progress.")
        if not args.wait:
            sys.exit(0)
    else:
        print(f"  {_YELLOW}?{_RESET} Unexpected action: {action}")
        print(f"    {json.dumps(payload, indent=2)}")

    # -- Optionally wait for completion --
    if not args.wait:
        print(f"\n  {_DIM}Use --wait to poll until the sweep finishes.{_RESET}\n")
        return

    # Find the regenerate-artifacts state machine
    regen_arn = None
    paginator = sfn.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page["stateMachines"]:
            if sm["name"] == "nova-cat-regenerate-artifacts":
                regen_arn = sm["stateMachineArn"]
                break
        if regen_arn:
            break

    if not regen_arn:
        print(f"  {_RED}✗{_RESET} Could not find regenerate-artifacts state machine")
        sys.exit(1)

    # Get most recent execution
    execs = sfn.list_executions(stateMachineArn=regen_arn, maxResults=1)["executions"]

    if not execs:
        print(f"  {_RED}✗{_RESET} No executions found")
        sys.exit(1)

    exec_arn = execs[0]["executionArn"]
    exec_name = exec_arn.split(":")[-1]
    print(f"\n  Monitoring: {_DIM}{exec_name}{_RESET}")

    # Poll
    deadline = time.time() + args.timeout
    status = "RUNNING"

    while time.time() < deadline:
        desc = sfn.describe_execution(executionArn=exec_arn)
        status = desc["status"]
        remaining = int(deadline - time.time())
        print(
            f"\r  {_DIM}⏳ {status}  ({remaining}s remaining){_RESET}    ",
            end="",
            flush=True,
        )
        if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            break
        time.sleep(15)

    print()

    if status == "SUCCEEDED":
        print(f"\n  {_GREEN}✓ Sweep completed successfully.{_RESET}\n")
    else:
        print(f"\n  {_RED}✗ Sweep ended with status: {status}{_RESET}")
        print(f"  {_DIM}Check: AWS Console → Step Functions → regenerate_artifacts{_RESET}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
