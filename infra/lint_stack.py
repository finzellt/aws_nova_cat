#!/usr/bin/env python3
"""
Nova Cat CDK pre-synth linter.

Validates CDK resource configurations against AWS service limits before
invoking cdk synth. Runs in milliseconds and catches whole classes of
errors that would otherwise only surface at synth time.

Usage:
    python3 infra/lint_stack.py

Exit codes:
    0 — all checks passed
    1 — one or more checks failed

CI placement: run before mypy and cdk synth.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from nova_constructs.compute import _FUNCTION_SPECS, _FunctionSpec

# ---------------------------------------------------------------------------
# AWS Lambda service limits
# ---------------------------------------------------------------------------
_LAMBDA_DESCRIPTION_MAX = 256
_LAMBDA_FUNCTION_NAME_MAX = 64


@dataclass
class _Check:
    name: str
    passed: bool
    message: str


def _check_function_descriptions(
    specs: dict[str, _FunctionSpec],
) -> list[_Check]:
    """All Lambda function descriptions must be <= 256 characters."""
    checks: list[_Check] = []
    for fn_name, spec in specs.items():
        length = len(spec.description)
        checks.append(
            _Check(
                name=f"lambda.description: {fn_name}",
                passed=length <= _LAMBDA_DESCRIPTION_MAX,
                message=(
                    f"description is {length} chars (max {_LAMBDA_DESCRIPTION_MAX})"
                    if length > _LAMBDA_DESCRIPTION_MAX
                    else f"{length} chars ✓"
                ),
            )
        )
    return checks


def _check_function_names(
    specs: dict[str, _FunctionSpec],
) -> list[_Check]:
    """All Lambda function names must be <= 64 characters."""
    checks: list[_Check] = []
    for fn_name in specs:
        full_name = f"nova-cat-{fn_name.replace('_', '-')}"
        length = len(full_name)
        checks.append(
            _Check(
                name=f"lambda.function_name: {fn_name}",
                passed=length <= _LAMBDA_FUNCTION_NAME_MAX,
                message=(
                    f"'{full_name}' is {length} chars (max {_LAMBDA_FUNCTION_NAME_MAX})"
                    if length > _LAMBDA_FUNCTION_NAME_MAX
                    else f"'{full_name}' ({length} chars) ✓"
                ),
            )
        )
    return checks


def _run_checks() -> bool:
    """Run all checks and print results. Returns True if all passed."""
    all_checks: list[_Check] = []
    all_checks.extend(_check_function_descriptions(_FUNCTION_SPECS))
    all_checks.extend(_check_function_names(_FUNCTION_SPECS))

    failures = [c for c in all_checks if not c.passed]
    passes = [c for c in all_checks if c.passed]

    if failures:
        print(f"Nova Cat CDK lint — {len(failures)} error(s), {len(passes)} passed\n")
        for check in failures:
            print(f"  ERROR  {check.name}: {check.message}")
        for check in passes:
            print(f"  ok     {check.name}: {check.message}")
    else:
        print(f"Nova Cat CDK lint — all {len(passes)} checks passed")
        for check in passes:
            print(f"  ok     {check.name}: {check.message}")

    return len(failures) == 0


def main() -> None:
    passed = _run_checks()
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
