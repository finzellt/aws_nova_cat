"""
nova_common.tracing

Pre-configured Powertools Tracer for all Nova Cat Lambda functions.

Wraps AWS X-Ray tracing. The Tracer instance is shared across the
Lambda process — Powertools handles subsegment lifecycle internally.

Usage:

    from nova_common.tracing import tracer

    @tracer.capture_method
    def _my_db_call(nova_id: str) -> dict:
        ...

    # Or as a context manager for finer-grained segments:
    with tracer.provider.in_subsegment("dynamodb-put"):
        table.put_item(...)

The service name is read from POWERTOOLS_SERVICE_NAME ("nova-cat"),
which is injected into every Lambda's environment by NovaCatCompute.
"""

from __future__ import annotations

from aws_lambda_powertools import Tracer

# Single Tracer instance per Lambda process.
# POWERTOOLS_SERVICE_NAME env var sets the service annotation on all segments.
tracer = Tracer()
