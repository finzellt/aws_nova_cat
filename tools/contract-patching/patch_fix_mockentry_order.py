#!/usr/bin/env python3
"""
Hotfix — reorder ``band_name`` in ``_MockEntry`` to satisfy dataclass rules.

Dataclass fields without defaults cannot follow fields with defaults.
The prior patch inserted ``band_name: str | None = None`` between
``band_id: str`` (no default) and ``regime: str`` (no default), which
is invalid.  This moves ``band_name`` after ``regime``.

Usage:
    python patch_fix_mockentry_order.py \\
        path/to/tests/services/test_ticket_ingestor_photometry_reader.py
"""

from __future__ import annotations

import sys


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
        print(f"Usage: {sys.argv[0]} <path/to/test_ticket_ingestor_photometry_reader.py>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    src = _replace_once(
        src,
        "    band_id: str\n"
        "    band_name: str | None = None\n"
        "    regime: str\n"
        "    svo_filter_id: str | None = None",
        "    band_id: str\n"
        "    regime: str\n"
        "    band_name: str | None = None\n"
        "    svo_filter_id: str | None = None",
        "Reorder band_name after regime in _MockEntry",
    )

    with open(path, "w") as fh:
        fh.write(src)

    print(f"Patched successfully: {path}")


if __name__ == "__main__":
    main()
