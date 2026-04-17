# services/nova_resolver/nova_priors/__init__.py

"""Nova priors package — the operator-curated enrichment source for initialize_nova.

Public API (ADR-036 Decision 8)
--------------------------------
    lookup(candidate_name)     — normalize + alias lookup; NovaPriorsEntry | None
    get_entry(normalized_name) — direct lookup by already-normalized name
    is_known_non_nova(name)    — convenience for the rejection flow
    list_entries()             — iterator over all entries

``NovaPriorsEntry`` is re-exported from ``contracts.models.priors`` so
callers only need a single import path:

    from nova_resolver.nova_priors import NovaPriorsEntry, lookup

See ``reader.py`` for the loader implementation and ADR-036 for the
full storage-and-maintenance model.
"""

from contracts.models.priors import NovaPriorsEntry

from .reader import get_entry, is_known_non_nova, list_entries, lookup

__all__ = [
    "NovaPriorsEntry",
    "get_entry",
    "is_known_non_nova",
    "list_entries",
    "lookup",
]
