"""Cluster partitioning for the band offset algorithm.

Before running the permutation search, bands are partitioned into
**overlap clusters**: groups that are transitively connected by
significant pairwise overlap.  The permutation search runs independently
within each cluster, and singleton clusters receive zero offset
automatically.

This eliminates the "chain displacement" problem where a well-separated
band (e.g., I-band at 12 mag when V and R are at ~10 mag) gets an
artificial offset simply because it shares a global ordering chain with
the overlapping pair.

The partitioning uses a **union-find** (disjoint-set) data structure for
efficient connected-component detection.  Two bands are joined if their
``overlap_fraction`` in the gap table meets or exceeds the
``max_overlap_fraction`` threshold — meaning they spend a non-trivial
fraction of their shared time domain within ε of each other.

References
----------
- ADR-032 Decision 2: Global Ordering with Exhaustive Search
  (cluster partitioning reduces the effective *n* per search)
"""

from __future__ import annotations

import logging

from .types import (
    DEFAULT_MAX_OVERLAP_FRACTION,
    GapTable,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------


class _UnionFind:
    """Disjoint-set (union-find) with path compression and union by rank."""

    __slots__ = ("_parent", "_rank")

    def __init__(self, elements: list[str]) -> None:
        self._parent: dict[str, str] = {e: e for e in elements}
        self._rank: dict[str, int] = {e: 0 for e in elements}

    def find(self, x: str) -> str:
        """Return the root representative of *x* with path compression."""
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        # Path compression: point every node on the path directly at root.
        while self._parent[x] != root:
            next_x = self._parent[x]
            self._parent[x] = root
            x = next_x
        return root

    def union(self, a: str, b: str) -> None:
        """Merge the sets containing *a* and *b* (union by rank)."""
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a == root_b:
            return
        # Attach the shorter tree under the taller tree.
        if self._rank[root_a] < self._rank[root_b]:
            self._parent[root_a] = root_b
        elif self._rank[root_a] > self._rank[root_b]:
            self._parent[root_b] = root_a
        else:
            self._parent[root_b] = root_a
            self._rank[root_a] += 1

    def components(self) -> list[list[str]]:
        """Return all connected components as sorted lists of members."""
        groups: dict[str, list[str]] = {}
        for element in self._parent:
            root = self.find(element)
            groups.setdefault(root, []).append(element)
        # Sort members within each cluster, then sort clusters by first member.
        return sorted(sorted(members) for members in groups.values())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def partition_into_clusters(
    gap_table: GapTable,
    band_ids: list[str],
    *,
    max_overlap_fraction: float = DEFAULT_MAX_OVERLAP_FRACTION,
) -> list[list[str]]:
    """Partition bands into independent overlap clusters.

    Two bands are placed in the same cluster if they are **transitively
    connected** by significant overlap: band A overlaps with band B,
    and band B overlaps with band C, so {A, B, C} form one cluster even
    if A and C are well-separated from each other.

    A pair is considered "significantly overlapping" when its
    ``overlap_fraction`` meets or exceeds *max_overlap_fraction*.

    Parameters
    ----------
    gap_table:
        Precomputed pairwise gap statistics (must include
        ``overlap_fraction`` on each record).
    band_ids:
        All band identifiers participating in offset computation.
    max_overlap_fraction:
        Threshold for the overlap fraction.  Pairs at or above this
        threshold are connected in the overlap graph; pairs below it
        are not.

    Returns
    -------
    list[list[str]]:
        List of clusters, where each cluster is a sorted list of band
        identifiers.  Clusters are sorted by their first member for
        deterministic output.  Singleton clusters (bands with no
        significant overlap) are included.

    Examples
    --------
    >>> # V and R overlap; B and I are isolated
    >>> clusters = partition_into_clusters(gap_table, ["B", "I", "R", "V"])
    >>> clusters
    [['B'], ['I'], ['R', 'V']]
    """
    uf = _UnionFind(band_ids)

    edges = 0
    for (band_a, band_b), gap in gap_table.items():
        if gap.overlap_fraction >= max_overlap_fraction:
            uf.union(band_a, band_b)
            edges += 1

    clusters = uf.components()

    singletons = sum(1 for c in clusters if len(c) == 1)
    logger.info(
        "Cluster partitioning: %d bands → %d clusters (%d singletons, %d overlap edges)",
        len(band_ids),
        len(clusters),
        singletons,
        edges,
    )

    return clusters
