#!/usr/bin/env python3
"""
Fix: nova_resolver_ticket Lambda description exceeds 256-char CDK limit (281 chars).

Usage:
    python fix_nova_resolver_ticket_desc.py path/to/infra/nova_constructs/compute.py
"""

from __future__ import annotations

import sys


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/compute.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    OLD = (
        '    "nova_resolver_ticket": _FunctionSpec(\n'
        '        service_dir="nova_resolver_ticket",\n'
        "        description=(\n"
        '            "Resolves OBJECT NAME to nova_id via NameMapping lookup. If absent, "\n'
        '            "invokes initialize_nova (StartExecution + DescribeExecution poll until "\n'
        '            "terminal). Raises UNRESOLVABLE_OBJECT_NAME or IDENTITY_AMBIGUITY for "\n'
        '            "quarantine-eligible outcomes. Handles ResolveNova. Used by: ingest_ticket."\n'
        "        ),"
    )

    NEW = (
        '    "nova_resolver_ticket": _FunctionSpec(\n'
        '        service_dir="nova_resolver_ticket",\n'
        "        description=(\n"
        '            "Resolves OBJECT NAME to nova_id via NameMapping. Invokes initialize_nova "\n'
        '            "if absent (StartExecution + poll). Raises UNRESOLVABLE_OBJECT_NAME or "\n'
        '            "IDENTITY_AMBIGUITY for quarantine outcomes. "\n'
        '            "Handles ResolveNova. Used by: ingest_ticket."\n'
        "        ),"
    )

    if OLD not in src:
        # Description text may have been written slightly differently —
        # fall back to matching on the unique surrounding key.
        MARKER = '"nova_resolver_ticket": _FunctionSpec('
        if MARKER not in src:
            print("FAILED — nova_resolver_ticket _FunctionSpec not found.")
            sys.exit(1)
        print("Could not match exact description text. Please shorten the")
        print("nova_resolver_ticket description in compute.py manually to ≤256 chars.")
        sys.exit(1)

    result = src.replace(OLD, NEW, 1)

    # Verify the replacement landed and the new string is ≤ 256 chars
    new_desc = (
        "Resolves OBJECT NAME to nova_id via NameMapping. Invokes initialize_nova "
        "if absent (StartExecution + poll). Raises UNRESOLVABLE_OBJECT_NAME or "
        "IDENTITY_AMBIGUITY for quarantine outcomes. "
        "Handles ResolveNova. Used by: ingest_ticket."
    )
    assert len(new_desc) <= 256, f"Still too long: {len(new_desc)} chars"

    with open(path, "w") as fh:
        fh.write(result)

    print(f"Fixed: {path}")
    print(f"New description length: {len(new_desc)} chars (limit: 256)")


if __name__ == "__main__":
    main()
