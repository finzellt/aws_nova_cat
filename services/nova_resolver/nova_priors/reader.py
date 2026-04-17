# services/nova_resolver/nova_priors/reader.py

"""Nova priors reader module (ADR-036 Decision 8).

Loads ``nova_priors.json`` once at module import time and exposes a
minimal read-only API for ``initialize_nova`` to consume.

Public API
----------
  lookup(candidate_name)     — normalize + alias lookup; returns the entry
                               or None on miss
  get_entry(normalized_name) — direct lookup by already-normalized name
  is_known_non_nova(name)    — convenience for the rejection flow
  list_entries()             — iterator over all entries

``NovaPriorsEntry`` is imported from ``contracts.models.priors``.  The
package-level ``__init__.py`` re-exports it so callers can use a single
import path.

Load behavior (ADR-036 Decision 4)
-----------------------------------
At module import the reader:
  1. Reads ``nova_priors.json`` from this package directory.
  2. Checks the top-level ``_schema_version`` major version.
  3. Checks the top-level shape (``entries`` is a mapping).
  4. Constructs a ``NovaPriorsEntry`` per entry — Pydantic validates on
     construction, so this doubles as structural validation without a
     separate defensive pass (Decision 4).
  5. Builds the alias index with self-aliases (Decision 5), failing on
     cross-entry collisions.

Failure modes are reported as ``RuntimeError`` at import time so a broken
bundle fails deployment — not runtime traffic.

Versioning (ADR-036 Decision 11)
---------------------------------
The JSON carries a top-level ``_schema_version`` (semver).  On a major
version mismatch this module raises ``RuntimeError`` at import time
rather than silently operating against an incompatible schema.  Minor
and patch mismatches are accepted without error.

Miss semantics (ADR-036 Decision 9)
------------------------------------
A lookup that returns ``None`` is NOT a signal that the candidate is
invalid.  Priors are an enrichment source, not a gate; ``initialize_nova``
must fall back to archive-resolution for unknown candidates.  Only an
``is_nova == False`` hit drives the rejection flow — use
``is_known_non_nova()`` for that check.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from contracts.models.priors import NovaPriorsEntry

# ---------------------------------------------------------------------------
# Schema version guard
# ---------------------------------------------------------------------------

# The major version this module was written against.  Bump when a
# breaking change to the priors JSON entry schema requires coordinated
# code updates (ADR-036 Decision 11).
_SUPPORTED_MAJOR_VERSION: int = 1

# ---------------------------------------------------------------------------
# Bundle location
# ---------------------------------------------------------------------------

_PRIORS_PATH: Path = Path(__file__).parent / "nova_priors.json"


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------


def _normalize_name(name: str) -> str:
    """Canonical nova-name normalization (ADR-036 Decision 2).

    Mirrors ``nova_resolver._normalize_candidate_name`` exactly:
    strip → replace underscores with spaces → lowercase → collapse
    whitespace.
    """
    return re.sub(r"\s+", " ", name.replace("_", " ").strip().lower())


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------


def _load_priors(
    path: Path,
) -> tuple[dict[str, NovaPriorsEntry], dict[str, str]]:
    """Load the priors bundle and build entry + alias indexes.

    Returns
    -------
    tuple[dict[str, NovaPriorsEntry], dict[str, str]]
        ``entry_index`` maps normalized primary name → ``NovaPriorsEntry``.
        ``alias_index`` maps any normalized alias (including primary
        names, per the self-alias invariant) → normalized primary name.

    Raises
    ------
    RuntimeError
        On unsupported schema version, malformed top-level shape, entry
        key / ``primary_name`` mismatch, or alias collisions.
    FileNotFoundError
        If the JSON file is absent from the package directory.
    """
    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

    # --- Version guard (ADR-036 Decision 11) ---
    schema_version: str = raw.get("_schema_version", "0.0.0")
    try:
        major = int(schema_version.split(".")[0])
    except (ValueError, IndexError) as exc:
        raise RuntimeError(
            f"nova_priors.json: cannot parse _schema_version {schema_version!r}"
        ) from exc
    if major != _SUPPORTED_MAJOR_VERSION:
        raise RuntimeError(
            f"nova_priors.json major version {major} is not supported by "
            f"this module (expected {_SUPPORTED_MAJOR_VERSION}).  Update "
            "reader.py to handle the new schema."
        )

    # --- Top-level shape check ---
    entries_raw = raw.get("entries")
    if not isinstance(entries_raw, dict):
        raise RuntimeError(
            "nova_priors.json: 'entries' must be a mapping of normalized_name → entry"
        )

    entry_index: dict[str, NovaPriorsEntry] = {}
    alias_index: dict[str, str] = {}

    for key, entry_data in entries_raw.items():
        if not isinstance(entry_data, dict):
            raise RuntimeError(
                f"nova_priors.json: entry {key!r} must be an object, got "
                f"{type(entry_data).__name__}"
            )

        # Pydantic validates on construction (ADR-036 Decision 4 — no
        # extra defensive re-validation pass).
        entry = NovaPriorsEntry(**entry_data)

        # The JSON key must equal the normalized primary name.  If the
        # build script produced the bundle this is always true; a
        # mismatch means the bundle was hand-edited or built from a
        # divergent _normalize_name implementation.
        expected_key = _normalize_name(entry.primary_name)
        if key != expected_key:
            raise RuntimeError(
                f"nova_priors.json: entry key {key!r} does not match "
                f"normalize(primary_name)={expected_key!r}.  The bundle "
                "may have been hand-edited."
            )

        entry_index[key] = entry

        # Build alias index: self-alias + curated aliases (ADR-036
        # Decision 5).  The primary name is inserted first so that
        # ``lookup()`` can treat the index uniformly without special-
        # casing a primary-name path.
        for alias in (entry.primary_name, *entry.aliases):
            normalized = _normalize_name(alias)
            if not normalized:
                continue  # defensive; build script already filters blanks
            existing = alias_index.get(normalized)
            if existing is None:
                alias_index[normalized] = key
            elif existing != key:
                raise RuntimeError(
                    f"nova_priors.json: alias collision — {normalized!r} "
                    f"maps to both {existing!r} and {key!r}"
                )
            # else: already maps to this entry's key (e.g. primary_name
            # also appears in aliases list) — no-op

    return entry_index, alias_index


# ---------------------------------------------------------------------------
# Module-level singletons — loaded once at import time
# ---------------------------------------------------------------------------

_ENTRY_INDEX: dict[str, NovaPriorsEntry]
_ALIAS_INDEX: dict[str, str]
_ENTRY_INDEX, _ALIAS_INDEX = _load_priors(_PRIORS_PATH)


# ---------------------------------------------------------------------------
# Public API (ADR-036 Decision 8)
# ---------------------------------------------------------------------------


def lookup(candidate_name: str) -> NovaPriorsEntry | None:
    """Look up a priors entry by primary name or alias.

    Normalizes the input per ADR-036 Decision 2 and consults the alias
    index.  Returns ``None`` on miss.

    Miss semantics (ADR-036 Decision 9): ``None`` means "no priors
    available," NOT "candidate is invalid."  Callers must fall back to
    the archive-resolution flow for unknown candidates.
    """
    normalized = _normalize_name(candidate_name)
    primary_key = _ALIAS_INDEX.get(normalized)
    if primary_key is None:
        return None
    return _ENTRY_INDEX.get(primary_key)


def get_entry(normalized_name: str) -> NovaPriorsEntry | None:
    """Look up a priors entry by already-normalized primary name.

    Skips the normalization step of ``lookup()``.  For callers that have
    already normalized the candidate name (e.g. after calling
    ``NormalizeCandidateName`` in the ``initialize_nova`` pipeline).

    Unlike ``lookup()``, this function does NOT consult the alias index
    — only direct primary-name hits return a result.
    """
    return _ENTRY_INDEX.get(normalized_name)


def is_known_non_nova(candidate_name: str) -> bool:
    """Return ``True`` iff the candidate matches an ``is_nova=False`` prior.

    Scoped to the anticipated ``initialize_nova`` rejection flow
    (ADR-036 Decision 9, wiring in item-3 follow-up).  Returns ``False``
    for both "no prior" and "prior with ``is_nova=True``" — only an
    explicit non-nova prior triggers rejection.
    """
    entry = lookup(candidate_name)
    return entry is not None and not entry.is_nova


def list_entries() -> Iterator[NovaPriorsEntry]:
    """Iterate over all priors entries in load order.

    Provided for backfill scripts and test assertions.  For the hot
    ``initialize_nova`` path use ``lookup()`` or ``get_entry()`` —
    iteration defeats the purpose of the alias index.
    """
    return iter(_ENTRY_INDEX.values())


__all__ = [
    "get_entry",
    "is_known_non_nova",
    "list_entries",
    "lookup",
]
