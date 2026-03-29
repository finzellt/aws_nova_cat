#!/usr/bin/env python3
"""
Patch: STALE-2 — Update README.md resource counts for ticket ingestion additions.

Three Lambda services were added (ticket_parser, nova_resolver_ticket,
ticket_ingestor) — one of which is Docker-based — and one new workflow
(ingest_ticket). Additionally, a dedicated photometry DynamoDB table was added.

Updates:
  1. "What This Project Demonstrates" bullet: 12 → 15 Lambdas, 5 → 6 workflows
  2. ASCII architecture diagram: 12 → 15 Lambda services
  3. Project Structure tree comment: 12 → 15, 3 → 4 Docker-based
  4. Infrastructure section: 12 → 15, 3 → 4 Docker-based; 5 → 6 workflows
  5. Infrastructure DynamoDB line: note dedicated photometry table (2 tables)

Usage:
    python patch_readme_stale2.py README.md
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
    count = content.count(old)
    if count > 1:
        print(f"REPLACE FAILED — anchor for {label!r} appears {count} times (expected 1).")
        sys.exit(1)
    return content.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path/to/README.md>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path) as fh:
        src = fh.read()

    # ── Precondition checks ───────────────────────────────────────────────
    _require(
        src,
        "12 Lambda functions, 5 Step Functions workflows, and\n  single-table DynamoDB design",
        "What This Project Demonstrates bullet",
    )
    _require(
        src,
        "12 Lambda services \u00b7 FITS profile validation \u00b7 quarantine",
        "ASCII architecture diagram",
    )
    _require(
        src,
        "services/           12 Lambda services (3 Docker-based for astropy/numpy)",
        "Project Structure tree comment",
    )
    _require(
        src,
        "- **Lambda:** 12 functions; 3 Docker-based (astropy/numpy compiled dependencies)",
        "Infrastructure Lambda line",
    )
    _require(
        src,
        "- **Step Functions:** 5 workflows orchestrating the ingestion pipeline",
        "Infrastructure Step Functions line",
    )
    _require(
        src,
        "- **DynamoDB:** Single-table design with namespaced partition keys and one GSI",
        "Infrastructure DynamoDB line",
    )

    print("All preconditions satisfied. Applying patches…")

    # ── 1. "What This Project Demonstrates" bullet ────────────────────────
    src = _replace_once(
        src,
        "12 Lambda functions, 5 Step Functions workflows, and\n  single-table DynamoDB design",
        "15 Lambda functions, 6 Step Functions workflows, and\n  single-table-plus-dedicated-photometry DynamoDB design",
        "What This Project Demonstrates bullet",
    )
    print("  \u2713 Updated 'What This Project Demonstrates' bullet")

    # ── 2. ASCII architecture diagram ─────────────────────────────────────
    src = _replace_once(
        src,
        "12 Lambda services \u00b7 FITS profile validation \u00b7 quarantine",
        "15 Lambda services \u00b7 FITS profile validation \u00b7 quarantine",
        "ASCII architecture diagram",
    )
    print("  \u2713 Updated ASCII architecture diagram")

    # ── 3. Project Structure tree comment ─────────────────────────────────
    src = _replace_once(
        src,
        "services/           12 Lambda services (3 Docker-based for astropy/numpy)",
        "services/           15 Lambda services (4 Docker-based for astropy/numpy)",
        "Project Structure tree comment",
    )
    print("  \u2713 Updated Project Structure tree comment")

    # ── 4. Infrastructure Lambda line ─────────────────────────────────────
    src = _replace_once(
        src,
        "- **Lambda:** 12 functions; 3 Docker-based (astropy/numpy compiled dependencies)",
        "- **Lambda:** 15 functions; 4 Docker-based (astropy/numpy compiled dependencies)",
        "Infrastructure Lambda line",
    )
    print("  \u2713 Updated Infrastructure Lambda line")

    # ── 5. Infrastructure Step Functions line ─────────────────────────────
    src = _replace_once(
        src,
        "- **Step Functions:** 5 workflows orchestrating the ingestion pipeline",
        "- **Step Functions:** 6 workflows orchestrating the ingestion pipeline",
        "Infrastructure Step Functions line",
    )
    print("  \u2713 Updated Infrastructure Step Functions line")

    # ── 6. Infrastructure DynamoDB line ───────────────────────────────────
    src = _replace_once(
        src,
        "- **DynamoDB:** Single-table design with namespaced partition keys and one GSI",
        "- **DynamoDB:** Main table (single-table design with namespaced partition keys and one GSI) plus dedicated photometry table",
        "Infrastructure DynamoDB line",
    )
    print("  \u2713 Updated Infrastructure DynamoDB line")

    # ── Post-condition checks ─────────────────────────────────────────────
    assert "12 Lambda" not in src, "Stale '12 Lambda' still present"
    assert "3 Docker-based" not in src, "Stale '3 Docker-based' still present"
    assert "5 workflows" not in src, "Stale '5 workflows' still present"
    assert "15 Lambda functions, 6 Step Functions workflows" in src
    assert "15 Lambda services (4 Docker-based" in src
    assert "15 functions; 4 Docker-based" in src
    assert "6 workflows" in src
    assert "dedicated photometry table" in src

    with open(path, "w") as fh:
        fh.write(src)

    print(f"\nDone. Wrote {path}")


if __name__ == "__main__":
    main()
