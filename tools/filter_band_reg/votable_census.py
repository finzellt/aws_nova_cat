#!/usr/bin/env python3
"""
votable_census.py — NovaCat filter/band census from tabular astronomical data.

For each input file (VOTable, XML, CSV), classifies the table by data type and
wavelength regime, detects column roles, assesses instrument provenance, and
extracts per-filter context (telescope, instrument, regime).

Usage:
    python votable_census.py \\
        --files-dir ./data \\
        --manifest catalog_manifest.csv \\
        --registry detection_registry.json \\
        --band-registry band_registry.json \\
        --synonyms synonyms.json \\
        --output table_census.json \\
        --quarantine quarantine_log.json

Outputs:
    table_census.json   — one record per trusted table
    quarantine_log.json — one record per quarantined table
"""

import argparse
import csv
import json
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Table types that skip filter extraction
# ---------------------------------------------------------------------------

NON_PHOTOMETRY_TYPES = {"spectra", "metadata", "unknown"}

# ---------------------------------------------------------------------------
# Registry loading
# ---------------------------------------------------------------------------


def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def load_manifest(path: Path) -> dict:
    """Return dict keyed by vot_file basename -> manifest row."""
    result = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            result[row["vot_file"]] = row
    return result


def load_synonyms(path: Path) -> dict:
    """Return upper-cased synonym -> canonical field name mapping."""
    raw = load_json(path)
    return {k.upper(): v for k, v in raw.get("synonyms", {}).items()}


def load_band_registry(path: Path) -> dict:
    """Return dict keyed by filter_string -> band entry (must have wavelength_regime)."""
    raw = load_json(path)
    # Support both a top-level list or a dict with a 'bands' key
    entries = raw if isinstance(raw, list) else raw.get("bands", [])
    return {e["filter_string"]: e for e in entries if "filter_string" in e}


# ---------------------------------------------------------------------------
# Source format detection
# ---------------------------------------------------------------------------

SUFFIX_TO_FORMAT = {
    ".vot": "vot",
    ".xml": "xml",
    ".csv": "csv",
    ".tsv": "tsv",
    ".fits": "fits",
    ".fit": "fits",
    ".ecsv": "ecsv",
}


def detect_format(path: Path) -> str:
    return SUFFIX_TO_FORMAT.get(path.suffix.lower(), "unknown")


# ---------------------------------------------------------------------------
# File parsing — returns list of dicts: {name, ucd, unit, description}
# ---------------------------------------------------------------------------


def parse_fields(path: Path, fmt: str) -> list[dict]:
    """Extract field metadata from file. Returns [] on unsupported format."""
    if fmt in ("vot", "xml"):
        return _parse_votable_fields(path)
    if fmt == "csv":
        return _parse_csv_fields(path)
    return []


def _parse_votable_fields(path: Path) -> list[dict]:
    try:
        from astropy.io.votable import parse as vot_parse

        vot = vot_parse(str(path), verify="ignore")
        table = vot.get_first_table()
        return [
            {
                "name": f.name,
                "ucd": (f.ucd or "").strip(),
                "unit": str(f.unit or ""),
                "description": (f.description or "").strip(),
            }
            for f in table.fields
        ]
    except Exception as e:
        print(f"  WARN: could not parse {path.name}: {e}", file=sys.stderr)
        return []


def _parse_csv_fields(path: Path) -> list[dict]:
    try:
        with open(path, newline="") as f:
            reader = csv.reader(f)
            headers = next(reader)
        return [{"name": h.strip(), "ucd": "", "unit": "", "description": ""} for h in headers]
    except Exception as e:
        print(f"  WARN: could not parse {path.name}: {e}", file=sys.stderr)
        return []


def read_votable_data(path: Path, fmt: str):
    """Return astropy Table or None."""
    if fmt not in ("vot", "xml"):
        return None
    try:
        from astropy.io.votable import parse as vot_parse

        vot = vot_parse(str(path), verify="ignore")
        return vot.get_first_table().to_table()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# UCD matching (prefix, case-insensitive)
# ---------------------------------------------------------------------------


def match_ucd(ucd: str, pattern_map: dict) -> tuple[str | None, str | None]:
    """Return (value, matched_pattern) for first prefix match, else (None, None)."""
    ucd_upper = ucd.upper()
    for pattern, value in pattern_map.items():
        if ucd_upper.startswith(pattern.upper()):
            return value, pattern
    return None, None


# ---------------------------------------------------------------------------
# Regex pattern matching against column names (Tier 3)
# ---------------------------------------------------------------------------


def match_column_pattern(
    field_name: str, registry: dict
) -> tuple[str | None, str | None, str | None]:
    """
    Return (role, regime, matched_pattern) if field_name matches any
    column_name_patterns entry, else (None, None, None).
    """
    for entry in registry.get("column_name_patterns", {}).get("wavelength_regime", []):
        if re.search(entry["pattern"], field_name, re.IGNORECASE):
            return entry.get("role"), entry.get("regime"), entry["pattern"]
    return None, None, None


# ---------------------------------------------------------------------------
# Column role detection (step 3)
# ---------------------------------------------------------------------------


def detect_column_roles(fields: list[dict], registry: dict, synonyms: dict) -> dict:
    """
    Return dict mapping role -> list of {field_name, provenance}.
    Roles: filter_name, telescope, instrument, observer, time, magnitude.
    """
    role_map: dict[str, list[dict]] = {}
    ucd_role_patterns = registry["ucd_patterns"]["column_roles"]

    for field in fields:
        role, provenance = None, None

        # Tier 1: UCD
        if field["ucd"]:
            matched_role, matched_pattern = match_ucd(field["ucd"], ucd_role_patterns)
            if matched_role:
                role = matched_role
                provenance = {"ucd1" if "_" in matched_pattern else "ucd1plus": matched_pattern}

        # Tier 2: synonym
        if role is None:
            canonical = synonyms.get(field["name"].upper())
            if canonical:
                role = canonical
                provenance = {"synonym": field["name"]}

        # Tier 3: regex pattern on column name
        if role is None:
            p_role, p_regime, p_pattern = match_column_pattern(field["name"], registry)
            if p_role:
                role = p_role
                provenance = {"column_name_pattern": p_pattern}
                # Attach regime hint as a sidecar so caller can use it
                field["_regime_hint"] = p_regime

        if role:
            role_map.setdefault(role, []).append(
                {
                    "field_name": field["name"],
                    "provenance": provenance,
                }
            )

    return role_map


# ---------------------------------------------------------------------------
# Classification: data_type (step 2a)
# ---------------------------------------------------------------------------


def detect_data_type(fields: list[dict], description: str, registry: dict) -> tuple[str, dict]:
    """Return (data_type, provenance_dict)."""
    ucd_patterns = registry["ucd_patterns"]["data_type"]
    kw_patterns = registry["description_keywords"]["data_type"]

    # Tier 1: any field UCD matches
    for field in fields:
        if not field["ucd"]:
            continue
        value, pattern = match_ucd(field["ucd"], ucd_patterns)
        if value:
            key = "ucd1" if "_" in pattern else "ucd1plus"
            return value, {key: field["ucd"]}

    # Tier 2: description keyword
    desc_lower = description.lower()
    for dtype, keywords in kw_patterns.items():
        for kw in keywords:
            if kw.lower() in desc_lower:
                return dtype, {"description_keyword": kw}

    return "unknown", {"default": "no_signals_detected"}


# ---------------------------------------------------------------------------
# Classification: wavelength_regime (step 2b)
# ---------------------------------------------------------------------------


def detect_regime(fields: list[dict], description: str, registry: dict) -> tuple[str | None, dict]:
    """Return (regime, provenance_dict)."""
    ucd_patterns = registry["ucd_patterns"]["wavelength_regime"]
    kw_patterns = registry["description_keywords"]["wavelength_regime"]

    # Tier 1: UCD
    for field in fields:
        if not field["ucd"]:
            continue
        value, pattern = match_ucd(field["ucd"], ucd_patterns)
        if value:
            key = "ucd1" if "_" in pattern else "ucd1plus"
            return value, {key: field["ucd"]}

    # Tier 2: description keyword
    desc_lower = description.lower()
    for regime, keywords in kw_patterns.items():
        for kw in keywords:
            if kw.lower() in desc_lower:
                return regime, {"description_keyword": kw}

    # Tier 3: regex hints accumulated on fields by detect_column_roles
    regime_hints = [f["_regime_hint"] for f in fields if f.get("_regime_hint")]
    if regime_hints:
        # Use majority vote; fall back to first if tied
        best = max(set(regime_hints), key=regime_hints.count)
        return best, {
            "column_name_pattern": f"{regime_hints.count(best)}/{len(regime_hints)} columns matched"
        }

    return None, {"default": "no_signals_detected"}


# ---------------------------------------------------------------------------
# Table format detection: long vs. wide (step 3 sub-step)
# ---------------------------------------------------------------------------


def detect_table_format(
    role_map: dict, fields: list[dict], registry: dict
) -> tuple[str, list[str]]:
    """
    Return (format, wide_band_columns).
    format: 'long' | 'wide' | 'unknown'
    wide_band_columns: column names carrying embedded band names (wide only)
    """
    if "filter_name" in role_map:
        return "long", []

    suffixes = registry.get("wide_format_suffixes", [])
    error_prefixes = registry.get("wide_format_error_prefixes", [])
    # Sort suffixes longest-first to avoid partial matches (e.g. '_mag' before 'mag')
    suffixes_sorted = sorted(suffixes, key=len, reverse=True)

    wide_cols = []
    for field in fields:
        name = field["name"]
        # Skip error columns
        if any(name.startswith(pfx) for pfx in error_prefixes):
            continue
        for suffix in suffixes_sorted:
            if name.endswith(suffix) and len(name) > len(suffix):
                wide_cols.append(name)
                break

    if wide_cols:
        return "wide", wide_cols
    return "unknown", []


# ---------------------------------------------------------------------------
# Band name extraction from wide-format column names
# ---------------------------------------------------------------------------


def extract_band_from_column(col_name: str, registry: dict) -> str | None:
    """Strip known suffixes and photometric system names to recover band string."""
    suffixes = registry.get("wide_format_suffixes", [])
    suffixes_sorted = sorted(suffixes, key=len, reverse=True)
    for suffix in suffixes_sorted:
        if col_name.endswith(suffix) and len(col_name) > len(suffix):
            return col_name[: -len(suffix)]
    return None


# ---------------------------------------------------------------------------
# Instrument provenance assessment (step 4)
# ---------------------------------------------------------------------------


def assess_instrument_provenance(
    role_map: dict,
    data: object,  # astropy Table or None
    registry: dict,
) -> tuple[str, dict]:
    """Return (status, provenance). status: resolved | aavso | quarantine."""
    has_tel = "telescope" in role_map
    has_inst = "instrument" in role_map

    if has_tel or has_inst:
        col = role_map.get("telescope", role_map.get("instrument", [{}]))[0]
        return "resolved", {"column_name": col.get("field_name")}

    # Check for AAVSO signals
    aavso = registry["aavso_signals"]
    observer_roles = role_map.get("observer", [])
    for obs_role in observer_roles:
        field_name = obs_role["field_name"]
        if field_name in aavso["column_names"]:
            # If we have actual data, check values against pattern
            if data is not None and field_name in data.colnames:
                pattern = re.compile(aavso["observer_code_pattern"])
                values = [str(v).strip() for v in data[field_name]]
                for v in values:
                    if any(s in v for s in aavso["known_strings"]):
                        return "aavso", {"known_string": v}
                    if pattern.match(v):
                        return "aavso", {"observer_code_pattern": v}
            else:
                # Column name alone is a weak AAVSO signal
                return "aavso", {"column_name": field_name}

    return "quarantine", {"default": "no_instrument_or_aavso_signals"}


# ---------------------------------------------------------------------------
# Filter extraction (step 5)
# ---------------------------------------------------------------------------


def extract_filters_long(
    data,
    role_map: dict,
    table_regime: str | None,
    band_registry: dict,
    registry: dict,
) -> list[dict]:
    """Extract per-filter records from a long-format table."""
    if data is None:
        return []

    filter_roles = role_map.get("filter_name", [])
    tel_roles = role_map.get("telescope", [])
    inst_roles = role_map.get("instrument", [])

    if not filter_roles:
        return []

    filter_col = filter_roles[0]["field_name"]
    tel_col = tel_roles[0]["field_name"] if tel_roles else None
    inst_col = inst_roles[0]["field_name"] if inst_roles else None

    seen: dict[str, dict] = {}
    for row in data:
        fstr = str(row[filter_col]).strip()
        if fstr in seen:
            continue

        tel_raw = str(row[tel_col]).strip() if tel_col else None
        inst_raw = str(row[inst_col]).strip() if inst_col else None

        # Split "Telescope/Instrument" convention
        tel, inst = tel_raw, inst_raw
        if tel_raw and "/" in tel_raw and inst_raw is None:
            parts = tel_raw.split("/", 1)
            tel, inst = parts[0], parts[1]

        # Regime: band registry first, then table-level
        regime, regime_prov = _resolve_filter_regime(fstr, table_regime, band_registry)

        tel_prov = {"column_value": tel_raw} if tel_col else None
        inst_prov = {"column_value": inst_raw} if inst_col else None

        seen[fstr] = {
            "filter_string": fstr,
            "telescope": tel or None,
            "instrument": inst or None,
            "wavelength_regime": regime if regime != table_regime else None,
            "svo_query_hint": None,
            "detection_provenance": {
                "filter_string": filter_roles[0]["provenance"],
                "telescope": tel_prov,
                "instrument": inst_prov,
                "wavelength_regime": regime_prov if regime != table_regime else None,
            },
        }

    return list(seen.values())


def extract_filters_wide(
    wide_cols: list[str],
    table_regime: str | None,
    band_registry: dict,
    registry: dict,
) -> list[dict]:
    """Infer filter records from wide-format column names."""
    results = []
    for col in wide_cols:
        fstr = extract_band_from_column(col, registry)
        if not fstr:
            continue
        regime, regime_prov = _resolve_filter_regime(fstr, table_regime, band_registry)
        results.append(
            {
                "filter_string": fstr,
                "telescope": None,
                "instrument": None,
                "wavelength_regime": regime if regime != table_regime else None,
                "svo_query_hint": None,
                "detection_provenance": {
                    "filter_string": {"column_name": col},
                    "telescope": None,
                    "instrument": None,
                    "wavelength_regime": regime_prov if regime != table_regime else None,
                },
            }
        )
    return results


def _resolve_filter_regime(
    fstr: str,
    table_regime: str | None,
    band_registry: dict,
) -> tuple[str | None, dict | None]:
    """Return (regime, provenance). Prefers band registry; falls back to table regime."""
    entry = band_registry.get(fstr)
    if entry and entry.get("wavelength_regime"):
        return entry["wavelength_regime"], {"band_registry": fstr}
    if table_regime:
        return table_regime, {"inherited": "table_level"}
    return None, None


# ---------------------------------------------------------------------------
# Main per-file processing
# ---------------------------------------------------------------------------


def process_file(
    path: Path,
    manifest: dict,
    registry: dict,
    band_registry: dict,
    synonyms: dict,
) -> tuple[dict | None, dict | None]:
    """
    Return (census_record, quarantine_record).
    Exactly one of the two will be non-None.
    """
    fmt = detect_format(path)
    manifest_row = manifest.get(path.name, {})
    description = manifest_row.get("description", "")
    table_name = manifest_row.get("table_name", path.stem)

    fields = parse_fields(path, fmt)
    if not fields:
        return None, _quarantine(path, fmt, table_name, description, "no_fields_parsed", fields)

    data = read_votable_data(path, fmt)

    # Step 2: classify
    data_type, dt_prov = detect_data_type(fields, description, registry)
    table_regime, reg_prov = detect_regime(fields, description, registry)

    # Short-circuit: non-photometry tables skip steps 3-5
    if data_type in NON_PHOTOMETRY_TYPES:
        return {
            "source_file": path.name,
            "source_format": fmt,
            "table_name": table_name,
            "description": description,
            "table_format": None,
            "classification": {
                "data_type": data_type,
                "wavelength_regime": table_regime,
                "instrument_provenance_status": None,
                "detection_provenance": {
                    "data_type": dt_prov,
                    "wavelength_regime": reg_prov,
                    "instrument_provenance_status": None,
                },
            },
            "filters": [],
        }, None

    # Step 3: column roles + table format
    role_map = detect_column_roles(fields, registry, synonyms)
    table_fmt, wide_cols = detect_table_format(role_map, fields, registry)

    # Step 4: instrument provenance gate
    ip_status, ip_prov = assess_instrument_provenance(role_map, data, registry)
    if ip_status == "quarantine":
        return None, _quarantine(
            path, fmt, table_name, description, "instrument_provenance_unresolvable", fields
        )

    # Step 5: extract filters
    if table_fmt == "long":
        filters = extract_filters_long(data, role_map, table_regime, band_registry, registry)
    elif table_fmt == "wide":
        filters = extract_filters_wide(wide_cols, table_regime, band_registry, registry)
    else:
        filters = []

    record = {
        "source_file": path.name,
        "source_format": fmt,
        "table_name": table_name,
        "description": description,
        "table_format": table_fmt,
        "classification": {
            "data_type": data_type,
            "wavelength_regime": table_regime,
            "instrument_provenance_status": ip_status,
            "detection_provenance": {
                "data_type": dt_prov,
                "wavelength_regime": reg_prov,
                "instrument_provenance_status": ip_prov,
            },
        },
        "filters": filters,
    }
    return record, None


def _quarantine(path, fmt, table_name, description, reason, fields=None) -> dict:
    return {
        "source_file": path.name,
        "source_format": fmt,
        "table_name": table_name,
        "description": description,
        "quarantine_reason": reason,
        "detected_columns": [{f["name"]: f["ucd"] or None} for f in fields] if fields else [],
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files-dir", required=True, help="Directory containing source files")
    parser.add_argument("--manifest", required=True, help="catalog_manifest.csv")
    parser.add_argument("--registry", required=True, help="detection_registry.json")
    parser.add_argument("--band-registry", required=False, default=None, help="band_registry.json")
    parser.add_argument("--synonyms", required=True, help="synonyms.json")
    parser.add_argument("--output", default="table_census.json")
    parser.add_argument("--quarantine", default="quarantine_log.json")
    args = parser.parse_args()

    registry = load_json(Path(args.registry))
    band_registry = load_band_registry(Path(args.band_registry)) if args.band_registry else {}
    synonyms = load_synonyms(Path(args.synonyms))
    manifest = load_manifest(Path(args.manifest))

    files_dir = Path(args.files_dir)
    source_files = sorted(
        f
        for f in files_dir.iterdir()
        if f.suffix.lower() in SUFFIX_TO_FORMAT and not f.name.startswith(".")
    )

    census_records = []
    quarantine_records = []

    for path in source_files:
        print(f"Processing {path.name}...", file=sys.stderr)
        record, qrecord = process_file(path, manifest, registry, band_registry, synonyms)
        if record:
            census_records.append(record)
        if qrecord:
            quarantine_records.append(qrecord)

    with open(args.output, "w") as f:
        json.dump({"tables": census_records}, f, indent=2)
    print(f"Wrote {len(census_records)} records to {args.output}")

    with open(args.quarantine, "w") as f:
        json.dump({"quarantined": quarantine_records}, f, indent=2)
    print(f"Wrote {len(quarantine_records)} quarantine records to {args.quarantine}")


if __name__ == "__main__":
    main()
