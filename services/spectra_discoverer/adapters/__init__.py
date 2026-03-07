"""
adapters/__init__.py

Exports the adapter registry and the Protocol for type checking.

The registry maps provider string → adapter instance. handler.py imports
_PROVIDER_ADAPTERS directly and never needs to know about individual adapter
modules.

To register a new provider:
  1. Implement SpectraDiscoveryAdapter in adapters/<provider>.py
  2. Import the adapter class here and add an instance to _PROVIDER_ADAPTERS
  3. Add the provider string to PrepareProviderList in the ASL
"""

from .base import SpectraDiscoveryAdapter
from .eso import ESOAdapter

_PROVIDER_ADAPTERS: dict[str, SpectraDiscoveryAdapter] = {
    "ESO": ESOAdapter(),
}

__all__ = ["SpectraDiscoveryAdapter", "_PROVIDER_ADAPTERS"]
