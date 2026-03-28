"""Integration test configuration.

Provides module isolation between integration tests and unit tests that import
the same handler modules.

Background
----------
The integration tests use ``_load_handlers()`` to evict handler modules from
``sys.modules`` and reimport them inside an active ``mock_aws()`` context so
that module-level boto3 clients bind to moto fakes instead of real AWS.  After
each integration test completes, those reimported module objects (v2) remain in
``sys.modules``.

Any unit-test module collected *before* the integration tests ran imported the
same handlers at collection time (v1).  Functions like ``handle`` captured v1's
``__globals__`` dict at import time; that reference is immutable.  When a unit
test later calls ``patch("ticket_parser.handler.parse_ticket_file", ...)``,
``unittest.mock`` resolves the dotted name through ``sys.modules``, finding v2
and patching *its* namespace — while ``handle`` still looks up names in v1's
namespace.  The patch never takes effect.

Fix: snapshot the relevant ``sys.modules`` entries before the first integration
test in each file runs and restore them in teardown.  After the integration
module finishes, later unit tests see the original module objects (v1) in
``sys.modules`` and patches land in the right namespace.
"""

from __future__ import annotations

import sys
from collections.abc import Generator
from types import ModuleType

import pytest

# Exactly the set of modules that _load_handlers() evicts and reimports.
# Keep in sync with that function when new handlers are added.
_HANDLER_MODULE_NAMES: tuple[str, ...] = (
    "job_run_manager.handler",
    "idempotency_guard.handler",
    "quarantine_handler.handler",
    "ticket_parser.handler",
    "nova_resolver_ticket.handler",
    "ticket_ingestor.handler",
)


@pytest.fixture(autouse=True, scope="module")
def restore_handler_modules() -> Generator[None, None, None]:
    """Snapshot and restore handler module objects around each integration module.

    Yields control to all tests in the module, then restores the
    ``sys.modules`` entries that ``_load_handlers()`` may have replaced.
    This guarantees that handler modules used by later unit-test modules
    are the same objects those modules bound at import time.
    """
    saved: dict[str, ModuleType | None] = {
        name: sys.modules.get(name) for name in _HANDLER_MODULE_NAMES
    }
    yield
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod
