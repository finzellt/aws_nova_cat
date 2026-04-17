"""Pytest wrapper for ``tools/catalog-expansion/validate_nova_priors.py``.

Runs the validator as a subprocess from the repo root so ``pytest``
alone — whether invoked by GitHub Actions or the operator's local CI
script — catches any drift, hand-edit, or contract violation in the
committed ``nova_priors.json``.

Why a subprocess
----------------
The validator lives in ``tools/catalog-expansion/``, which is not a
Python package (no ``__init__.py``).  Importing its internal functions
would require ad-hoc ``sys.path`` manipulation that duplicates the
script's own setup and drifts over time.  Invoking the script as a
subprocess is:

  - exactly what GitHub Actions runs (identical code path)
  - immune to ``sys.path`` or import-order hazards
  - self-documenting — the test IS the CI invocation

The trade-off is slightly more verbose pytest failure output, which is
captured and attached below so the operator sees the full validator
diagnostic rather than a bare "exit code 1."

What this covers
----------------
Every check defined in ADR-036 Decision 4 — schema version, top-level
shape, Pydantic contract violations, alias collisions, and CSV-JSON
SHA drift.  The validator's per-path behavior is exhaustively tested
in ``test_nova_priors_reader.py`` (which exercises the same structural
checks via the ``reader._load_priors`` function) and by the manual
sweep that accompanied the validator's introduction.  This file is the
single CI gate that ties everything together against the real
committed artifacts.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

# ``tests/tools/test_nova_priors_bundle.py`` → repo root is three up.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_VALIDATOR_SCRIPT = _REPO_ROOT / "tools" / "catalog-expansion" / "validate_nova_priors.py"


def test_committed_bundle_is_valid() -> None:
    """The committed ``nova_priors.json`` is valid and in sync with the CSV.

    Invokes ``validate_nova_priors.py`` with its default paths (canonical
    JSON and canonical CSV).  A non-zero exit code fails the test with
    the validator's captured output attached so the operator can fix the
    underlying problem without rerunning the script manually.
    """
    result = subprocess.run(
        [sys.executable, str(_VALIDATOR_SCRIPT)],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(
            "validate_nova_priors.py reported bundle problems "
            f"(exit code {result.returncode}).\n\n"
            "--- stdout ---\n"
            f"{result.stdout}\n"
            "--- stderr ---\n"
            f"{result.stderr}"
        )
