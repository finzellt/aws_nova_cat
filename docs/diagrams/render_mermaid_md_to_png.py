#!/usr/bin/env python3
"""
Render all .md files in the current directory (assumed to contain Mermaid diagrams)
to .png files with the same basename, using mermaid-cli (mmdc).

Behavior:
- If a file contains a ```mermaid ... ``` fenced block, render the *first* such block.
- Otherwise, treat the entire file as Mermaid source.
- Never modifies the original .md; uses a temp .mmd file.
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
    """
    Return Mermaid source from a markdown string.
    Prefers first ```mermaid fenced block if present; else returns whole content.
    """
    m = MERMAID_BLOCK_RE.search(md_text)
    if m:
        return m.group(1).strip() + "\n"
    return md_text.strip() + "\n"


def ensure_mmdc_available() -> str:
    """
    Return path to mmdc or exit with a helpful error.
    """
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


def render_one(md_path: Path, mmdc_path: str) -> tuple[bool, str]:
    """
    Render one markdown file to a PNG with same basename.
    Returns (success, message).
    """
    out_png = md_path.with_suffix(".png")

    md_text = md_path.read_text(encoding="utf-8")
    mermaid_src = extract_mermaid_source(md_text)

    # Write temp .mmd so we never touch the original markdown.
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_mmd = Path(tmpdir) / (md_path.stem + ".mmd")
        tmp_mmd.write_text(mermaid_src, encoding="utf-8")

        # Basic render command.
        # You can add options like theme or background if you want.
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

    return True, f"{md_path.name} -> {out_png.name}"


def main() -> None:
    mmdc_path = ensure_mmdc_available()

    md_files = sorted(Path(".").glob("*.md"))
    if not md_files:
        print("No .md files found in current directory.")
        return

    ok_count = 0
    for md in md_files:
        success, msg = render_one(md, mmdc_path)
        print(msg)
        ok_count += int(success)

    print(f"\nDone. Rendered {ok_count}/{len(md_files)} file(s).")


if __name__ == "__main__":
    main()
