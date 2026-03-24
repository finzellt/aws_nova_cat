#!/usr/bin/env python3
"""
Generates a tree structure of a directory (max depth 3) with specific exclusions,
and writes the result to repo_tree_structure.txt.

Usage:
    python repo_tree.py /path/to/your/repo
"""

import os
import sys

# ── Exclusion rules ──────────────────────────────────────────────────────────

# Ignored only when it appears as a direct child of the root (top-level)
TOP_LEVEL_IGNORE = {"lambdas"}

# Ignored if this exact name appears ANYWHERE in the path segments
EXACT_NAME_IGNORE = {"cdk.out", ".git", ".venv", ".github", "node_modules"}

# Ignored if ANY path segment contains one of these substrings
SUBSTRING_IGNORE = {"cache", "compressed", "engineering", "diagrams"}

# Files ignored by exact name (not applied to directories)
FILE_NAME_IGNORE = {".DS_Store"}

# Directories that get one extra level of depth beyond the global max
DEEPER_DIRS: set[str] = {"None"}


def should_ignore(abs_path: str, root: str) -> bool:
    """Return True if this path should be excluded from the tree."""
    # Normalise so comparisons are reliable
    rel = os.path.relpath(abs_path, root)
    parts = rel.replace("\\", "/").split("/")

    # 1. Top-level-only ignore (e.g. "lambdas" only at depth 1)
    if len(parts) == 1 and parts[0] in TOP_LEVEL_IGNORE:
        return True

    # 2. Exact-name ignore anywhere in path (e.g. "cdk.out")
    if any(part in EXACT_NAME_IGNORE for part in parts):
        return True

    # 3. Substring ignore anywhere in path (e.g. "cache", "compressed", …)
    return any(sub in part.lower() for part in parts for sub in SUBSTRING_IGNORE)


# ── Tree builder ─────────────────────────────────────────────────────────────


def build_tree(root: str, max_depth: int = 3) -> list[str]:
    """Walk *root* up to *max_depth* and return lines that form the tree."""
    lines: list[str] = []
    root = os.path.abspath(root)
    lines.append(os.path.basename(root) + "/")

    def _walk(current: str, prefix: str, depth: int, local_max: int) -> None:
        if depth > local_max:
            return

        try:
            entries = sorted(os.scandir(current), key=lambda e: (not e.is_dir(), e.name.lower()))
        except PermissionError:
            return

        # Filter out ignored entries
        entries = [
            e
            for e in entries
            if not should_ignore(e.path, root) and not (e.is_file() and e.name in FILE_NAME_IGNORE)
        ]

        for idx, entry in enumerate(entries):
            is_last = idx == len(entries) - 1
            connector = "└── " if is_last else "├── "
            extension = "    " if is_last else "│   "

            label = entry.name + ("/" if entry.is_dir() else "")
            lines.append(f"{prefix}{connector}{label}")

            if entry.is_dir() and depth < local_max:
                # Grant one extra level if this directory is in DEEPER_DIRS
                child_max = local_max + 1 if entry.name in DEEPER_DIRS else local_max
                _walk(entry.path, prefix + extension, depth + 1, child_max)

    _walk(root, "", 1, max_depth)
    return lines


# ── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python repo_tree.py <directory>", file=sys.stderr)
        sys.exit(1)

    target = sys.argv[1]

    if not os.path.isdir(target):
        print(f"Error: '{target}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    tree_lines = build_tree(target, max_depth=3)
    output = "\n".join(tree_lines) + "\n"

    # Write output next to wherever the script is called from
    out_path = os.path.join(os.getcwd(), "repo_tree_structure.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# Repository Tree\n")
        f.write(f"# Root : {os.path.abspath(target)}\n")
        f.write("# Depth: 3\n")
        f.write("#\n")
        f.write("# Exclusions applied:\n")
        f.write(f"#   top-level dirs : {sorted(TOP_LEVEL_IGNORE)}\n")
        f.write(f"#   exact name     : {sorted(EXACT_NAME_IGNORE)}\n")
        f.write(f"#   substring match: {sorted(SUBSTRING_IGNORE)}\n")
        f.write(f"#   exact filenames : {sorted(FILE_NAME_IGNORE)}\n")
        f.write(f"#   deeper dirs     : {sorted(DEEPER_DIRS)}\n")
        f.write("#\n\n")
        f.write(output)

    print(f"Tree written to: {out_path}")


if __name__ == "__main__":
    main()
