#!/usr/bin/env python3
"""
Render Mermaid diagrams from markdown files in:
  <repo_root>/docs/diagrams/*.md
and write PNGs to:
  <repo_root>/docs/diagrams/rendered/

Repo root is determined as:
  repo_root = Path(__file__).resolve().parents[2]

Notes:
- If a file contains a ```mermaid ... ``` fenced block, render the *first* such block.
- Otherwise, treat the entire file as Mermaid source.
- Never modifies the original .md; uses a temp .mmd file.
- Requires mermaid-cli (mmdc) installed and available on PATH.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

MERMAID_BLOCK_RE = re.compile(
    r"```mermaid\s*\n(.*?)\n```",
    re.DOTALL | re.IGNORECASE,
)


def extract_mermaid_source(md_text: str) -> str:
    """Return Mermaid source from markdown text."""
    m = MERMAID_BLOCK_RE.search(md_text)
    if m:
        return m.group(1).strip() + "\n"
    return md_text.strip() + "\n"


def ensure_mmdc_available() -> str:
    """Return path to mmdc or exit with a helpful error."""
    mmdc_path = shutil.which("mmdc")
    if not mmdc_path:
        print(
            "ERROR: 'mmdc' (mermaid-cli) not found on PATH.\n"
            "Install it with:\n"
            "  npm install -g @mermaid-js/mermaid-cli\n",
            file=sys.stderr,
        )
        sys.exit(1)
    return mmdc_path


def render_one(md_path: Path, out_dir: Path, mmdc_path: str) -> tuple[bool, str]:
    """Render one markdown file to a PNG in out_dir. Returns (success, message)."""
    out_png = out_dir / f"{md_path.stem}.png"

    md_text = md_path.read_text(encoding="utf-8")
    mermaid_src = extract_mermaid_source(md_text)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_mmd = Path(tmpdir) / f"{md_path.stem}.mmd"
        tmp_mmd.write_text(mermaid_src, encoding="utf-8")

        cmd = [
            mmdc_path,
            "-i",
            str(tmp_mmd),
            "-o",
            str(out_png),
        ]

        try:
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError as e:
            return False, f"{md_path.name}: failed to run mmdc ({e})"

        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            return False, f"{md_path.name}: mmdc failed\n{err}"

    return True, f"{md_path.relative_to(md_path.parents[0])} -> {out_png}"


def main() -> None:
    mmdc_path = ensure_mmdc_available()

    repo_root = Path(__file__).resolve().parents[2]
    diagrams_dir = repo_root / "docs" / "diagrams"
    rendered_dir = diagrams_dir / "rendered"
    rendered_dir.mkdir(parents=True, exist_ok=True)

    md_files = sorted(diagrams_dir.glob("*.md"))
    if not md_files:
        print(f"No .md files found in: {diagrams_dir}")
        return

    ok_count = 0
    for md in md_files:
        success, msg = render_one(md, rendered_dir, mmdc_path)
        print(msg)
        ok_count += int(success)

    print(f"\nDone. Rendered {ok_count}/{len(md_files)} file(s) into {rendered_dir}")


if __name__ == "__main__":
    main()
