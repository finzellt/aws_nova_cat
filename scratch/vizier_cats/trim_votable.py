#!/usr/bin/env python3
"""
Trim VOTable files to one row per unique filter value.
Usage: python trim_votable.py file1.vot [file2.vot ...]
Output: <name>_trimmed.vot alongside each input file.
"""

import sys
from pathlib import Path

from astropy.io.votable import from_table, parse
from astropy.table import unique

FILTER_UCDS = {"INST_FILTER_CODE", "OBS_BAND"}
FILTER_NAMES = {"filt", "band", "filter"}


def find_filter_col(table):
    for field in table.fields:
        if field.ucd and any(u in field.ucd.upper() for u in {"INST_FILTER", "OBS_BAND"}):
            return field.name
    for field in table.fields:
        if field.name.lower() in FILTER_NAMES:
            return field.name
    return None


def trim(path: Path):
    vot = parse(str(path))
    table = vot.get_first_table().to_table()

    col = find_filter_col(vot.get_first_table())
    if not col:
        print(f"SKIP {path.name}: no filter column detected")
        return

    trimmed = unique(table, keys=[col])
    trimmed.keep_columns([col])  # only the filter col — we just need unique values
    # Restore all columns but deduplicated on filter col
    trimmed = unique(table, keys=[col])

    out = path.with_name(path.stem + "_trimmed.vot")
    from_table(trimmed).to_xml(str(out))
    print(
        f"OK {path.name}: {len(table)} rows -> {len(trimmed)} unique '{col}' values -> {out.name}"
    )
    print(f"   Values: {sorted(str(v) for v in trimmed[col])}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: trim_votable.py file1.vot [file2.vot ...]")
        sys.exit(1)
    for arg in sys.argv[1:]:
        trim(Path(arg))
