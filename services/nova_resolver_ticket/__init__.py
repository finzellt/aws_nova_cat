"""nova_resolver_ticket service — public API.

Exposes the Lambda entry point for the ResolveNova task in the
ingest_ticket workflow (DESIGN-004).

Usage::

    from nova_resolver_ticket import handle
"""

from nova_resolver_ticket.handler import handle

__all__ = ["handle"]
