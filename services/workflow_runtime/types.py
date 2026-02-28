"""
Shared runtime type definitions.

This module defines TypedDict structures and small type helpers used across
the workflow runtime primitives.

These types intentionally remain:

- Stable across schema evolution
- Loosely coupled to domain event/entity schemas
- Focused only on runtime metadata (not domain modeling)

The envelope and context types defined here form the contract between
Step Functions tasks within the Nova Cat workflow system.
"""

from __future__ import annotations

from typing import Any, TypedDict


class EnvelopeContext(TypedDict, total=False):
    workflow_name: str
    state_name: str
    execution_arn: str
    correlation_id: str

    job_run_id: str
    attempt_number: int

    # Optional identifiers that may or may not be known at a given point
    nova_id: str
    data_product_id: str
    reference_id: str


class Envelope(TypedDict):
    input: dict[str, Any]
    context: EnvelopeContext
