#!/usr/bin/env python3
"""
SVO Filter Profile Service — Complete Database Harvester (v3)
=============================================================

Downloads the entire SVO FPS filter database (metadata + transmission curves)
into a local SQLite database for offline querying.

Usage
-----
    # Harvest everything
    python svo_harvest.py

    # Harvest a specific facility
    python svo_harvest.py --facility HST

    # Resume an interrupted harvest (skips completed filters)
    python svo_harvest.py --resume

    # Skip Phase 1 if index checkpoint already exists
    python svo_harvest.py --skip-index

    # Probe the API without downloading
    python svo_harvest.py --probe-only

    # Verbose logging
    python svo_harvest.py -v

Dependencies
------------
    pip install requests astropy

Checkpoints
-----------
The script writes intermediate checkpoint files during Phase 1 (index collection):
  - checkpoint_facility_NNN.json  — after every 50 facilities
  - checkpoint_index_complete.json — when all facilities are done

If Phase 1 crashes partway through, re-run and it will resume from the last
checkpoint. Or use --skip-index to jump straight to Phase 2 if you already
have checkpoint_index_complete.json.
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
import warnings
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from xml.etree import ElementTree

import requests
import urllib3

# ---------------------------------------------------------------------------
# Optional: astropy VOTable parser (preferred). Falls back to raw XML parsing.
# ---------------------------------------------------------------------------
try:
    from astropy.io.votable import parse as parse_votable
    from astropy.io.votable.exceptions import VOWarning

    warnings.filterwarnings("ignore", category=VOWarning)
    HAS_ASTROPY = True
except ImportError:
    HAS_ASTROPY = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# Configuration constants
# ============================================================================

SVO_ENDPOINT_CANDIDATES = [
    "http://svo2.cab.inta-csic.es/theory/fps/fps.php",
    "http://svo2.cab.inta-csic.es/svo/theory/fps/fps.php",
    "https://svo2.cab.inta-csic.es/theory/fps/fps.php",
    "https://svo2.cab.inta-csic.es/svo/theory/fps/fps.php",
]

DEFAULT_DB_PATH = "svo_fps.db"
CHECKPOINT_DIR = "checkpoints"
FACILITY_CHECKPOINT_INTERVAL = 50  # Write checkpoint every N facilities
REQUEST_DELAY = 0.5
MAX_RETRIES = 3
RETRY_BASE_DELAY = 5
REQUEST_TIMEOUT = 120
INDEX_REQUEST_TIMEOUT = 300
COMMIT_BATCH_SIZE = 25

# ============================================================================
# Logging
# ============================================================================

LOG_FORMAT = "%(asctime)s [%(levelname)-7s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

log = logging.getLogger("svo_harvest")


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    log.setLevel(level)
    log.addHandler(handler)


# ============================================================================
# Database schema — v3
# ============================================================================

SCHEMA_VERSION = 3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS filters (
    filter_id               TEXT PRIMARY KEY,
    facility                TEXT,
    instrument              TEXT,
    filter_name             TEXT,

    wavelength_ref          REAL,
    wavelength_eff          REAL,
    wavelength_mean         REAL,
    wavelength_cen          REAL,
    wavelength_pivot        REAL,
    wavelength_peak         REAL,
    wavelength_phot         REAL,
    wavelength_min          REAL,
    wavelength_max          REAL,
    width_eff               REAL,
    fwhm                    REAL,

    fsun                    REAL,

    zero_point              REAL,
    zero_point_unit         TEXT,
    zero_point_type         TEXT,
    mag_sys                 TEXT,
    mag0                    REAL,
    asinh_soft              REAL,
    phot_system             TEXT,
    phot_cal_id             TEXT,

    detector_type           TEXT,
    band                    TEXT,

    description             TEXT,
    comments                TEXT,
    profile_ref             TEXT,
    calib_ref               TEXT,

    filter_profile_service  TEXT,
    wavelength_unit         TEXT,
    wavelength_ucd          TEXT,
    transmission_curve_url  TEXT,

    raw_metadata            TEXT,
    harvested_at            TEXT,
    transmission_count      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS transmission_curves (
    filter_id       TEXT    NOT NULL,
    wavelength      REAL    NOT NULL,
    transmission    REAL    NOT NULL,
    FOREIGN KEY (filter_id) REFERENCES filters(filter_id)
);

CREATE INDEX IF NOT EXISTS idx_tc_filter_id ON transmission_curves(filter_id);
CREATE INDEX IF NOT EXISTS idx_filters_wavelength ON filters(wavelength_eff);
CREATE INDEX IF NOT EXISTS idx_filters_facility ON filters(facility, instrument);

CREATE TABLE IF NOT EXISTS harvest_log (
    filter_id       TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    attempted_at    TEXT,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS harvest_runs (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT,
    finished_at     TEXT,
    endpoint_used   TEXT,
    filters_total   INTEGER,
    filters_success INTEGER,
    filters_failed  INTEGER,
    cli_args        TEXT
);
"""

# All columns that MUST exist in the filters table (v3 schema).
# Used for migration: any missing columns get added via ALTER TABLE.
FILTERS_COLUMNS = {
    "filter_id": "TEXT PRIMARY KEY",
    "facility": "TEXT",
    "instrument": "TEXT",
    "filter_name": "TEXT",
    "wavelength_ref": "REAL",
    "wavelength_eff": "REAL",
    "wavelength_mean": "REAL",
    "wavelength_cen": "REAL",
    "wavelength_pivot": "REAL",
    "wavelength_peak": "REAL",
    "wavelength_phot": "REAL",
    "wavelength_min": "REAL",
    "wavelength_max": "REAL",
    "width_eff": "REAL",
    "fwhm": "REAL",
    "fsun": "REAL",
    "zero_point": "REAL",
    "zero_point_unit": "TEXT",
    "zero_point_type": "TEXT",
    "mag_sys": "TEXT",
    "mag0": "REAL",
    "asinh_soft": "REAL",
    "phot_system": "TEXT",
    "phot_cal_id": "TEXT",
    "detector_type": "TEXT",
    "band": "TEXT",
    "description": "TEXT",
    "comments": "TEXT",
    "profile_ref": "TEXT",
    "calib_ref": "TEXT",
    "filter_profile_service": "TEXT",
    "wavelength_unit": "TEXT",
    "wavelength_ucd": "TEXT",
    "transmission_curve_url": "TEXT",
    "raw_metadata": "TEXT",
    "harvested_at": "TEXT",
    "transmission_count": "INTEGER DEFAULT 0",
}


def _get_existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return set of column names that currently exist in a table."""
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _migrate_schema(conn: sqlite3.Connection):
    """
    Migrate an older database to the v3 schema.
    Adds any missing columns to the filters table via ALTER TABLE.
    """
    # Check if the filters table exists at all
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='filters'"
    )
    if not cur.fetchone():
        # No filters table — fresh database, just run the full schema
        return

    existing = _get_existing_columns(conn, "filters")
    added = []
    for col_name, col_type in FILTERS_COLUMNS.items():
        if col_name not in existing:
            # PRIMARY KEY columns can't be added, but filter_id should always exist
            if "PRIMARY KEY" in col_type:
                continue
            clean_type = col_type.replace("PRIMARY KEY", "").strip()
            try:
                conn.execute(
                    f"ALTER TABLE filters ADD COLUMN {col_name} {clean_type}"
                )
                added.append(col_name)
            except sqlite3.OperationalError as exc:
                log.warning("Could not add column %s: %s", col_name, exc)

    if added:
        log.info("Schema migration: added %d columns to filters: %s", len(added), added)
        conn.commit()


def init_db(db_path: str) -> sqlite3.Connection:
    """Create/open the database, run migrations, ensure schema is current."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Migrate first (adds missing columns to existing tables)
    _migrate_schema(conn)

    # Then create any tables that don't exist yet
    conn.executescript(SCHEMA_SQL)

    # Record schema version
    conn.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
    conn.commit()
    return conn


def get_completed_filter_ids(conn: sqlite3.Connection) -> set:
    cur = conn.execute(
        "SELECT filter_id FROM harvest_log WHERE status = 'success'"
    )
    return {row[0] for row in cur.fetchall()}


# ============================================================================
# Checkpoint management
# ============================================================================


def _checkpoint_dir(base_dir: str = CHECKPOINT_DIR) -> Path:
    p = Path(base_dir)
    p.mkdir(parents=True, exist_ok=True)
    return p


def save_facility_checkpoint(
    filters: list[dict],
    completed_facilities: list[str],
    facility_index: int,
    base_dir: str = CHECKPOINT_DIR,
):
    """Save intermediate checkpoint after a batch of facilities."""
    cp_dir = _checkpoint_dir(base_dir)
    cp_path = cp_dir / f"checkpoint_facility_{facility_index:03d}.json"
    data = {
        "timestamp": datetime.now(UTC).isoformat(),
        "facilities_completed": completed_facilities,
        "facility_index": facility_index,
        "filter_count": len(filters),
        "filters": filters,
    }
    cp_path.write_text(json.dumps(data, default=str))
    log.info("Checkpoint saved: %s (%d filters)", cp_path, len(filters))


def save_index_complete(
    filters: list[dict],
    facilities: list[str],
    base_dir: str = CHECKPOINT_DIR,
):
    """Save the final complete index checkpoint."""
    cp_dir = _checkpoint_dir(base_dir)
    cp_path = cp_dir / "checkpoint_index_complete.json"
    data = {
        "timestamp": datetime.now(UTC).isoformat(),
        "facilities": facilities,
        "filter_count": len(filters),
        "filters": filters,
    }
    cp_path.write_text(json.dumps(data, default=str))
    log.info("Complete index checkpoint saved: %s (%d filters)", cp_path, len(filters))


def load_latest_checkpoint(base_dir: str = CHECKPOINT_DIR) -> tuple[list[dict], list[str], int] | None:
    """
    Load the latest checkpoint file.
    Returns (filters, completed_facilities, last_facility_index) or None.
    """
    cp_dir = Path(base_dir)
    if not cp_dir.exists():
        return None

    # Prefer the complete checkpoint
    complete = cp_dir / "checkpoint_index_complete.json"
    if complete.exists():
        data = json.loads(complete.read_text())
        log.info(
            "Loaded complete index checkpoint: %d filters, %d facilities",
            data["filter_count"],
            len(data.get("facilities", [])),
        )
        return data["filters"], data.get("facilities", []), -1  # -1 = all done

    # Otherwise find the latest facility checkpoint
    checkpoints = sorted(cp_dir.glob("checkpoint_facility_*.json"))
    if not checkpoints:
        return None

    latest = checkpoints[-1]
    data = json.loads(latest.read_text())
    log.info(
        "Loaded checkpoint %s: %d filters, %d facilities completed",
        latest.name,
        data["filter_count"],
        len(data["facilities_completed"]),
    )
    return (
        data["filters"],
        data["facilities_completed"],
        data["facility_index"],
    )


def load_complete_checkpoint(base_dir: str = CHECKPOINT_DIR) -> list[dict] | None:
    """Load only the complete index checkpoint. Returns filter list or None."""
    complete = Path(base_dir) / "checkpoint_index_complete.json"
    if not complete.exists():
        return None
    data = json.loads(complete.read_text())
    log.info("Loaded complete index checkpoint: %d filters", data["filter_count"])
    return data["filters"]


# ============================================================================
# HTTP client
# ============================================================================


class SVOClient:
    def __init__(self, base_url: str, verify_ssl: bool = True):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.session.headers.update(
            {"User-Agent": "SVOHarvester/3.0 (astronomy research)"}
        )
        self._last_request_time = 0.0

    def _rate_limit(self):
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)

    def get(self, params: dict, timeout: int = REQUEST_TIMEOUT) -> bytes:
        for attempt in range(1, MAX_RETRIES + 1):
            self._rate_limit()
            try:
                log.debug("GET %s params=%s (attempt %d)", self.base_url, params, attempt)
                resp = self.session.get(self.base_url, params=params, timeout=timeout)
                self._last_request_time = time.monotonic()
                if resp.status_code == 200:
                    return resp.content
                else:
                    log.warning("HTTP %d (attempt %d/%d)", resp.status_code, attempt, MAX_RETRIES)
            except requests.RequestException as exc:
                log.warning("Request failed: %s (attempt %d/%d)", exc, attempt, MAX_RETRIES)

            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.info("Retrying in %ds ...", delay)
                time.sleep(delay)

        raise RuntimeError(
            f"Failed after {MAX_RETRIES} attempts: {self.base_url} params={params}"
        )


# ============================================================================
# VOTable parsing
# ============================================================================


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def parse_votable_xml(raw: bytes) -> tuple[list[str], list[dict]]:
    root = ElementTree.fromstring(raw)

    table_el = None
    for el in root.iter():
        if _strip_ns(el.tag) == "TABLE":
            table_el = el
            break

    if table_el is None:
        return [], []

    fields = []
    for el in table_el.iter():
        if _strip_ns(el.tag) == "FIELD":
            name = el.get("name") or el.get("ID") or f"col_{len(fields)}"
            fields.append(name)

    if not fields:
        return [], []

    rows = []
    for tr in table_el.iter():
        if _strip_ns(tr.tag) != "TR":
            continue
        cells = []
        for td in tr:
            if _strip_ns(td.tag) == "TD":
                cells.append(td.text.strip() if td.text else "")
        if cells:
            while len(cells) < len(fields):
                cells.append("")
            row = dict(zip(fields, cells[: len(fields)], strict=False))
            rows.append(row)

    return fields, rows


def parse_votable_astropy(raw: bytes) -> tuple[list[str], list[dict]]:
    votable = parse_votable(BytesIO(raw))
    for resource in votable.resources:
        for table in resource.tables:
            colnames = [f.name for f in table.fields]
            results = []
            for row in table.array:
                d = {}
                for i, name in enumerate(colnames):
                    val = row[i]
                    if hasattr(val, "item"):
                        val = val.item()
                    if isinstance(val, bytes):
                        val = val.decode("utf-8", errors="replace")
                    d[name] = val
                results.append(d)
            if results:
                return colnames, results
    return [], []


def parse_response(raw: bytes) -> tuple[list[str], list[dict]]:
    if HAS_ASTROPY:
        try:
            fields, rows = parse_votable_astropy(raw)
            if rows:
                return fields, rows
        except Exception as exc:
            log.debug("Astropy parse failed (%s), trying XML...", exc)

    try:
        return parse_votable_xml(raw)
    except Exception as exc:
        log.error("Both parsers failed: %s", exc)
        return [], []


# ============================================================================
# Service description parsing
# ============================================================================


def parse_service_description(raw: bytes) -> dict:
    root = ElementTree.fromstring(raw)
    facilities = []
    instruments = []
    field_names = []

    for el in root.iter():
        tag = _strip_ns(el.tag)
        if tag == "PARAM":
            param_name = el.get("name", "")
            if param_name == "INPUT:Facility":
                for child in el.iter():
                    if _strip_ns(child.tag) == "OPTION":
                        val = child.get("value", "").strip()
                        if val:
                            facilities.append(val)
            elif param_name == "INPUT:Instrument":
                for child in el.iter():
                    if _strip_ns(child.tag) == "OPTION":
                        val = child.get("value", "").strip()
                        if val:
                            instruments.append(val)
        if tag == "FIELD":
            name = el.get("name")
            if name:
                field_names.append(name)

    return {"facilities": facilities, "instruments": instruments, "field_names": field_names}


# ============================================================================
# API interaction
# ============================================================================


def probe_endpoint(url: str, verify_ssl: bool) -> bool:
    try:
        resp = requests.get(url, timeout=15, verify=verify_ssl)
        return resp.status_code == 200 and b"VOTABLE" in resp.content[:2000]
    except requests.RequestException:
        return False


def find_working_endpoint(verify_ssl: bool) -> str | None:
    for url in SVO_ENDPOINT_CANDIDATES:
        log.info("Probing %s ...", url)
        if probe_endpoint(url, verify_ssl):
            log.info("  -> Endpoint responding: %s", url)
            return url
        else:
            log.info("  -> No valid response")
    return None


def fetch_facility_list(client: SVOClient) -> list[str]:
    log.info("Fetching SVO service description to get facility list ...")
    raw = client.get({}, timeout=INDEX_REQUEST_TIMEOUT)
    log.info("Received %d bytes", len(raw))
    desc = parse_service_description(raw)
    log.info(
        "Found %d facilities, %d instruments, %d field definitions",
        len(desc["facilities"]),
        len(desc["instruments"]),
        len(desc["field_names"]),
    )
    return desc["facilities"]


def fetch_filters_for_facility(client: SVOClient, facility: str) -> list[dict]:
    raw = client.get({"Facility": facility}, timeout=INDEX_REQUEST_TIMEOUT)
    _fields, rows = parse_response(raw)
    return rows


def fetch_transmission_curve(client: SVOClient, filter_id: str) -> list[dict]:
    raw = client.get({"ID": filter_id})
    _fields, rows = parse_response(raw)
    return rows


# ============================================================================
# Data normalization
# ============================================================================

FIELD_MAP = {
    "FilterProfileService": "filter_profile_service",
    "filterID": "filter_id",
    "PhotCalID": "phot_cal_id",
    "WavelengthUnit": "wavelength_unit",
    "WavelengthUCD": "wavelength_ucd",
    "PhotSystem": "phot_system",
    "DetectorType": "detector_type",
    "Band": "band",
    "Instrument": "instrument",
    "Facility": "facility",
    "ProfileReference": "profile_ref",
    "CalibrationReference": "calib_ref",
    "Description": "description",
    "Comments": "comments",
    "WavelengthRef": "wavelength_ref",
    "WavelengthMean": "wavelength_mean",
    "WavelengthEff": "wavelength_eff",
    "WavelengthMin": "wavelength_min",
    "WavelengthMax": "wavelength_max",
    "WidthEff": "width_eff",
    "WavelengthCen": "wavelength_cen",
    "WavelengthPivot": "wavelength_pivot",
    "WavelengthPeak": "wavelength_peak",
    "WavelengthPhot": "wavelength_phot",
    "FWHM": "fwhm",
    "Fsun": "fsun",
    "MagSys": "mag_sys",
    "ZeroPoint": "zero_point",
    "ZeroPointUnit": "zero_point_unit",
    "ZeroPointType": "zero_point_type",
    "Mag0": "mag0",
    "AsinhSoft": "asinh_soft",
    "TrasmissionCurve": "transmission_curve_url",
}

FLOAT_COLUMNS = {
    "wavelength_ref", "wavelength_mean", "wavelength_eff", "wavelength_cen",
    "wavelength_pivot", "wavelength_peak", "wavelength_phot",
    "wavelength_min", "wavelength_max", "width_eff", "fwhm",
    "fsun", "zero_point", "mag0", "asinh_soft",
}


def normalize_metadata(raw_row: dict) -> dict:
    result = {"raw_metadata": json.dumps(raw_row, default=str)}

    for raw_key, raw_val in raw_row.items():
        mapped = FIELD_MAP.get(raw_key)
        if not mapped:
            continue

        if mapped in FLOAT_COLUMNS:
            try:
                val = float(raw_val) if raw_val not in (None, "", "NaN", "nan") else None
                if val is not None and (val != val):  # NaN check
                    val = None
                result[mapped] = val
            except (ValueError, TypeError):
                result[mapped] = None
        else:
            result[mapped] = str(raw_val).strip() if raw_val not in (None, "") else None

    fid = result.get("filter_id", "")
    if "." in fid:
        result["filter_name"] = fid.rsplit(".", 1)[-1]
    elif "/" in fid:
        result["filter_name"] = fid.rsplit("/", 1)[-1]
    else:
        result["filter_name"] = fid

    return result


def extract_transmission_data(rows: list[dict]) -> list[tuple[float, float]]:
    if not rows:
        return []

    sample = rows[0]
    keys_lower = {k.lower(): k for k in sample.keys()}

    wl_key = None
    tr_key = None
    for c in ("wavelength", "lambda", "wave", "wl"):
        if c in keys_lower:
            wl_key = keys_lower[c]
            break
    for c in ("transmission", "transmit", "throughput", "trans", "t"):
        if c in keys_lower:
            tr_key = keys_lower[c]
            break

    if not wl_key or not tr_key:
        all_keys = list(sample.keys())
        if len(all_keys) == 2:
            wl_key, tr_key = all_keys[0], all_keys[1]
        elif len(all_keys) >= 2:
            numeric_keys = []
            for k in all_keys:
                try:
                    float(sample[k])
                    numeric_keys.append(k)
                except (ValueError, TypeError):
                    pass
            if len(numeric_keys) >= 2:
                wl_key, tr_key = numeric_keys[0], numeric_keys[1]

    if not wl_key or not tr_key:
        log.debug("Cannot identify wavelength/transmission columns from: %s", list(sample.keys()))
        return []

    points = []
    for row in rows:
        try:
            wl = float(row[wl_key])
            tr = float(row[tr_key])
            points.append((wl, tr))
        except (ValueError, TypeError, KeyError):
            continue
    return points


def is_metadata_row(row: dict) -> bool:
    keys = set(row.keys())
    metadata_signals = {"filterID", "Facility", "Instrument", "WavelengthEff"}
    return bool(metadata_signals & keys)


# ============================================================================
# Database insertion
# ============================================================================

DB_COLUMNS = [
    "filter_id", "facility", "instrument", "filter_name",
    "wavelength_ref", "wavelength_eff", "wavelength_mean", "wavelength_cen",
    "wavelength_pivot", "wavelength_peak", "wavelength_phot",
    "wavelength_min", "wavelength_max", "width_eff", "fwhm",
    "fsun",
    "zero_point", "zero_point_unit", "zero_point_type",
    "mag_sys", "mag0", "asinh_soft", "phot_system", "phot_cal_id",
    "detector_type", "band",
    "description", "comments", "profile_ref", "calib_ref",
    "filter_profile_service", "wavelength_unit", "wavelength_ucd",
    "transmission_curve_url",
    "raw_metadata", "harvested_at", "transmission_count",
]


def insert_filter(conn: sqlite3.Connection, metadata: dict):
    values = [metadata.get(c) for c in DB_COLUMNS]
    placeholders = ", ".join(["?"] * len(DB_COLUMNS))
    col_names = ", ".join(DB_COLUMNS)
    conn.execute(
        f"INSERT OR REPLACE INTO filters ({col_names}) VALUES ({placeholders})",
        values,
    )


def insert_transmission(conn: sqlite3.Connection, filter_id: str, points: list[tuple[float, float]]):
    conn.execute("DELETE FROM transmission_curves WHERE filter_id = ?", (filter_id,))
    conn.executemany(
        "INSERT INTO transmission_curves (filter_id, wavelength, transmission) VALUES (?, ?, ?)",
        [(filter_id, wl, tr) for wl, tr in points],
    )


def log_harvest(conn: sqlite3.Connection, filter_id: str, status: str, error: str | None = None):
    conn.execute(
        "INSERT OR REPLACE INTO harvest_log (filter_id, status, attempted_at, error_message) "
        "VALUES (?, ?, ?, ?)",
        (filter_id, status, datetime.now(UTC).isoformat(), error),
    )


# ============================================================================
# Phase 1: Collect filter index
# ============================================================================


def collect_index(
    client: SVOClient,
    facilities: list[str],
    instrument_filter: str | None = None,
    checkpoint_dir: str = CHECKPOINT_DIR,
) -> list[dict]:
    """
    Query every facility and collect all filter metadata.
    Writes checkpoints every FACILITY_CHECKPOINT_INTERVAL facilities
    and resumes from the last checkpoint if one exists.
    """
    all_filters = []
    seen_ids = set()
    start_index = 0
    completed_facilities = []

    # Try to resume from checkpoint
    cp = load_latest_checkpoint(checkpoint_dir)
    if cp is not None:
        prev_filters, prev_facilities, last_idx = cp
        all_filters = prev_filters
        seen_ids = {f["filter_id"] for f in all_filters if f.get("filter_id")}
        completed_facilities = prev_facilities

        if last_idx == -1:
            # Complete checkpoint — skip Phase 1 entirely
            log.info("Index already complete (%d filters). Skipping Phase 1.", len(all_filters))
            return all_filters

        # Find where to resume
        completed_set = set(completed_facilities)
        start_index = 0
        for i, fac in enumerate(facilities):
            if fac not in completed_set:
                start_index = i
                break
        else:
            start_index = len(facilities)

        log.info(
            "Resuming from facility %d/%d (%d filters already collected)",
            start_index + 1, len(facilities), len(all_filters),
        )

    for fi in range(start_index, len(facilities)):
        fac = facilities[fi]
        log.info("[%d/%d] Querying facility: %s", fi + 1, len(facilities), fac)

        try:
            rows = fetch_filters_for_facility(client, fac)
        except Exception as exc:
            log.error("  Failed to query %s: %s", fac, exc)
            completed_facilities.append(fac)
            continue

        fac_count = 0
        for raw_row in rows:
            if not is_metadata_row(raw_row):
                continue
            meta = normalize_metadata(raw_row)
            fid = meta.get("filter_id")
            if not fid or fid in seen_ids:
                continue
            if instrument_filter and meta.get("instrument") != instrument_filter:
                continue
            all_filters.append(meta)
            seen_ids.add(fid)
            fac_count += 1

        completed_facilities.append(fac)
        log.info("  -> %d new filters (total: %d)", fac_count, len(all_filters))

        # Periodic checkpoint
        if (fi + 1) % FACILITY_CHECKPOINT_INTERVAL == 0:
            save_facility_checkpoint(all_filters, completed_facilities, fi + 1, checkpoint_dir)

    # Final complete checkpoint
    save_index_complete(all_filters, completed_facilities, checkpoint_dir)
    return all_filters


# ============================================================================
# Main harvest
# ============================================================================


def harvest(
    db_path: str,
    facility_filter: str | None = None,
    instrument_filter: str | None = None,
    resume: bool = False,
    verify_ssl: bool = True,
    probe_only: bool = False,
    skip_index: bool = False,
    checkpoint_dir: str = CHECKPOINT_DIR,
):
    log.info("=" * 60)
    log.info("SVO FPS Harvester v3 starting")
    log.info("=" * 60)
    log.info("Database:       %s", db_path)
    log.info("Checkpoints:    %s/", checkpoint_dir)
    log.info("SSL verify:     %s", "ON" if verify_ssl else "OFF")

    # --- Find endpoint ---
    endpoint = find_working_endpoint(verify_ssl)
    if not endpoint:
        log.error(
            "Could not reach any SVO endpoint. Things to try:\n"
            "  1. Run with --no-ssl (the cert may be expired)\n"
            "  2. Check if svo2.cab.inta-csic.es is reachable in a browser\n"
            "  3. The SVO might be temporarily down"
        )
        sys.exit(1)

    if probe_only:
        log.info("Probe successful -- %s is responding. Exiting.", endpoint)
        return

    client = SVOClient(endpoint, verify_ssl=verify_ssl)
    conn = init_db(db_path)

    # Verify the schema is correct before we go any further
    existing_cols = _get_existing_columns(conn, "filters")
    missing = set(DB_COLUMNS) - existing_cols
    if missing:
        log.error(
            "Schema migration failed! Still missing columns: %s\n"
            "Try deleting %s and running again with a fresh database.",
            missing, db_path,
        )
        sys.exit(1)

    # Record run
    run_started = datetime.now(UTC).isoformat()
    conn.execute(
        "INSERT INTO harvest_runs (started_at, endpoint_used, cli_args) VALUES (?, ?, ?)",
        (run_started, endpoint, json.dumps({"facility": facility_filter, "instrument": instrument_filter})),
    )
    conn.commit()

    # === Phase 1: Collect filter index ===
    if skip_index:
        all_filters = load_complete_checkpoint(checkpoint_dir)
        if all_filters is None:
            log.error("--skip-index used but no checkpoint_index_complete.json found!")
            sys.exit(1)
    else:
        if facility_filter:
            facilities = [facility_filter]
        else:
            facilities = fetch_facility_list(client)
            if not facilities:
                log.error("Could not get facility list. Aborting.")
                sys.exit(1)

        log.info("=" * 60)
        log.info("Phase 1: Collecting filter index (%d facilities)", len(facilities))
        log.info("=" * 60)

        all_filters = collect_index(
            client,
            facilities,
            instrument_filter=instrument_filter,
            checkpoint_dir=checkpoint_dir,
        )

    log.info("Total unique filters: %d", len(all_filters))

    if not all_filters:
        log.error("No filters found! Try --facility HST to test a single facility.")
        sys.exit(1)

    # --- Resume: skip completed ---
    if resume:
        completed = get_completed_filter_ids(conn)
        before = len(all_filters)
        all_filters = [f for f in all_filters if f["filter_id"] not in completed]
        log.info("Resume: skipping %d completed (%d remaining)", before - len(all_filters), len(all_filters))

    if not all_filters:
        log.info("Nothing to do — all filters already harvested!")
        conn.close()
        return

    # --- Store metadata ---
    now = datetime.now(UTC).isoformat()
    for meta in all_filters:
        meta["harvested_at"] = now
        insert_filter(conn, meta)
    conn.commit()
    log.info("Index metadata stored in database")

    # === Phase 2: Download transmission curves ===
    log.info("=" * 60)
    log.info("Phase 2: Downloading transmission curves (%d filters)", len(all_filters))
    log.info("=" * 60)

    total = len(all_filters)
    success_count = 0
    fail_count = 0

    for i, meta in enumerate(all_filters, 1):
        fid = meta["filter_id"]
        pct = (i / total) * 100
        log.info("[%d/%d  %.1f%%] %s", i, total, pct, fid)

        try:
            rows = fetch_transmission_curve(client, fid)
            data_rows = [r for r in rows if not is_metadata_row(r)]
            points = extract_transmission_data(data_rows)
            if not points and rows:
                points = extract_transmission_data(rows)

            if points:
                insert_transmission(conn, fid, points)
                conn.execute(
                    "UPDATE filters SET transmission_count = ? WHERE filter_id = ?",
                    (len(points), fid),
                )
                log_harvest(conn, fid, "success")
                success_count += 1
                log.info("  -> %d points", len(points))
            else:
                log_harvest(conn, fid, "success", "No transmission data")
                success_count += 1
                log.warning("  -> No transmission data")

        except Exception as exc:
            fail_count += 1
            log_harvest(conn, fid, "failed", str(exc))
            log.error("  -> FAILED: %s", exc)

        if i % COMMIT_BATCH_SIZE == 0:
            conn.commit()

    # --- Finalize ---
    conn.execute(
        "UPDATE harvest_runs SET finished_at = ?, filters_total = ?, "
        "filters_success = ?, filters_failed = ? WHERE started_at = ?",
        (datetime.now(UTC).isoformat(), total, success_count, fail_count, run_started),
    )
    conn.commit()
    conn.close()

    log.info("=" * 60)
    log.info("Harvest complete!")
    log.info("  Total:      %d", total)
    log.info("  Successful: %d", success_count)
    log.info("  Failed:     %d", fail_count)
    log.info("  Database:   %s", db_path)
    log.info("=" * 60)

    if fail_count > 0:
        log.info("Re-run with --resume to retry failures.")


# ============================================================================
# CLI
# ============================================================================


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Harvest the SVO Filter Profile Service into a local SQLite database.",
    )
    p.add_argument("-o", "--output", default=DEFAULT_DB_PATH, help="Output database path")
    p.add_argument("--facility", default=None, help="Only this facility (e.g. HST, JWST)")
    p.add_argument("--instrument", default=None, help="Only this instrument (e.g. NIRCam)")
    p.add_argument("--resume", action="store_true", help="Skip already-completed filters")
    p.add_argument("--no-ssl", action="store_true", help="Disable SSL verification")
    p.add_argument("--probe-only", action="store_true", help="Test connectivity only")
    p.add_argument("--skip-index", action="store_true",
                    help="Skip Phase 1, use existing checkpoint_index_complete.json")
    p.add_argument("--checkpoint-dir", default=CHECKPOINT_DIR, help="Checkpoint directory")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    p.add_argument("--delay", type=float, default=REQUEST_DELAY, help="Inter-request delay (s)")
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(verbose=args.verbose)

    global REQUEST_DELAY
    REQUEST_DELAY = args.delay

    harvest(
        db_path=args.output,
        facility_filter=args.facility,
        instrument_filter=args.instrument,
        resume=args.resume,
        verify_ssl=not args.no_ssl,
        probe_only=args.probe_only,
        skip_index=args.skip_index,
        checkpoint_dir=args.checkpoint_dir,
    )


if __name__ == "__main__":
    main()
