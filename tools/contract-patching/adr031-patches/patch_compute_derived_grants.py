#!/usr/bin/env python3
"""
ADR-031 Decision 4 patch — infra/nova_constructs/compute.py

Grants spectra_validator and ticket_ingestor write access to the
``derived/*`` prefix in the private S3 bucket, required for web-ready
CSV uploads.

Changes:
  1. Adds private_bucket.grant_write for spectra_validator on "derived/*".
  2. Adds private_bucket.grant_write for ticket_ingestor on "derived/*".

Usage:
    python patch_compute_derived_grants.py path/to/infra/nova_constructs/compute.py
"""

from __future__ import annotations

import sys


def _require(content: str, marker: str, label: str) -> None:
    if marker not in content:
        print(f"PRECONDITION FAILED — {label!r} not found.")
        print(f"  Expected to find:\n{marker!r}")
        sys.exit(1)


def _replace_once(content: str, old: str, new: str, label: str) -> str:
    if old not in content:
        print(f"REPLACE FAILED — anchor for {label!r} not found.")
        sys.exit(1)
    if content.count(old) > 1:
        print(f"REPLACE FAILED — anchor for {label!r} appears more than once.")
        sys.exit(1)
    return content.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/compute.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # -------------------------------------------------------------------------
    # Precondition checks — find the grant blocks for each service
    # -------------------------------------------------------------------------
    # spectra_validator: look for the existing comment block that describes it
    _require(
        src,
        'self._functions["spectra_validator"] = spectra_validator',
        "spectra_validator function registration",
    )
    # ticket_ingestor: look for existing diagnostics grant
    _require(
        src,
        'private_bucket.grant_write(\n            self._functions["ticket_ingestor"],\n            "diagnostics/*",\n        )',
        "ticket_ingestor diagnostics grant",
    )

    print("All preconditions satisfied. Applying patches…")

    # =========================================================================
    # Patch 1 — Add derived/* write grant for ticket_ingestor
    #           Anchor: after the existing diagnostics/* grant
    # =========================================================================
    OLD_DIAG_GRANT = (
        "private_bucket.grant_write(\n"
        '            self._functions["ticket_ingestor"],\n'
        '            "diagnostics/*",\n'
        "        )"
    )
    NEW_DIAG_GRANT = (
        "private_bucket.grant_write(\n"
        '            self._functions["ticket_ingestor"],\n'
        '            "diagnostics/*",\n'
        "        )\n"
        "        # ADR-031 Decision 4: web-ready CSV uploads to derived/spectra/\n"
        "        private_bucket.grant_write(\n"
        '            self._functions["ticket_ingestor"],\n'
        '            "derived/*",\n'
        "        )"
    )
    src = _replace_once(src, OLD_DIAG_GRANT, NEW_DIAG_GRANT, "ticket_ingestor derived/* grant")

    # =========================================================================
    # Patch 2 — Add derived/* write grant for spectra_validator
    #           Anchor: after the spectra_validator function registration
    # =========================================================================
    OLD_SV_REG = 'self._functions["spectra_validator"] = spectra_validator'
    NEW_SV_REG = (
        'self._functions["spectra_validator"] = spectra_validator\n'
        "        # ADR-031 Decision 4: web-ready CSV uploads to derived/spectra/\n"
        "        private_bucket.grant_write(\n"
        "            spectra_validator,\n"
        '            "derived/*",\n'
        "        )"
    )
    src = _replace_once(src, OLD_SV_REG, NEW_SV_REG, "spectra_validator derived/* grant")

    # -------------------------------------------------------------------------
    # Postcondition checks
    # -------------------------------------------------------------------------
    # Both derived/* grants should now be present
    count = src.count('"derived/*"')
    if count < 2:
        print(f"POSTCONDITION FAILED — expected 2 derived/* grants, found {count}")
        sys.exit(1)

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")


if __name__ == "__main__":
    main()
