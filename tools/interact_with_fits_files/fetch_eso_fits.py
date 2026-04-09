#!/usr/bin/env python3
"""
Fetch ESO FITS files for a nova within an MJD date range.

Resolves the nova name to coordinates via SIMBAD, queries ESO SSAP for
spectra in the given MJD window, and downloads all non-embargoed FITS
files to a local directory.

Usage:
    python3 tools/fetch_eso_fits.py "V1324 Sco" --mjd-min 56000 --mjd-max 56200
    python3 tools/fetch_eso_fits.py "V1369 Cen" --mjd-min 56600 --mjd-max 56700 --out-dir /tmp/fits
    python3 tools/fetch_eso_fits.py "V1369 Cen" --mjd-min 56600 --mjd-max 56700 --dry-run

Personal operator tooling — not subject to CI, mypy strict, or ruff.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from pathlib import Path

import pyvo as vo
import requests
from astropy.coordinates import SkyCoord
from astropy.units import Quantity

# ── Constants ─────────────────────────────────────────────────────────────────

_SSAP_ENDPOINT = "http://archive.eso.org/ssap"
_SEARCH_DIAMETER_DEG = 40 / 3600  # 20 arcsec
_REQUEST_TIMEOUT_S = 15
_DOWNLOAD_TIMEOUT_S = 60
_MAX_QUERY_ATTEMPTS = 3
_QUERY_RETRY_DELAY_S = 3

# Terminal colours
_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"


# ── SIMBAD resolution ────────────────────────────────────────────────────────


def resolve_name(name: str) -> tuple[float, float]:
    """Resolve a nova name to (ra_deg, dec_deg) via SIMBAD."""
    print(f"Resolving {_BOLD}{name}{_RESET} via SIMBAD...")
    try:
        coord = SkyCoord.from_name(name)
    except Exception as exc:
        print(f"{_RED}SIMBAD resolution failed: {exc}{_RESET}")
        sys.exit(1)
    ra_deg = coord.ra.deg
    dec_deg = coord.dec.deg
    print(f"  RA={ra_deg:.6f}°  Dec={dec_deg:.6f}°")
    return ra_deg, dec_deg


# ── ESO SSAP query ───────────────────────────────────────────────────────────


def query_eso(ra_deg: float, dec_deg: float) -> list[dict]:
    """Run an ESO SSAP cone search; return list of result dicts."""
    print(f"\nQuerying ESO SSAP (cone={_SEARCH_DIAMETER_DEG * 3600:.1f} arcsec)...")
    ssap_service = vo.dal.SSAService(_SSAP_ENDPOINT)
    ssap_service._session.timeout = _REQUEST_TIMEOUT_S
    pos = SkyCoord(ra_deg, dec_deg, unit="deg")
    size = Quantity(_SEARCH_DIAMETER_DEG, unit="deg")

    last_exc = None
    for attempt in range(1, _MAX_QUERY_ATTEMPTS + 1):
        try:
            resultset = ssap_service.search(pos=pos.fk5, diameter=size)
            break
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_QUERY_ATTEMPTS:
                print(f"  {_YELLOW}Attempt {attempt} failed, retrying...{_RESET}")
                time.sleep(_QUERY_RETRY_DELAY_S)
    else:
        print(
            f"{_RED}ESO SSAP query failed after {_MAX_QUERY_ATTEMPTS} attempts: {last_exc}{_RESET}"
        )
        sys.exit(1)

    rows = []
    for row in resultset:
        rec = {}
        for field in [
            "COLLECTION",
            "TARGETNAME",
            "SNR",
            "SPECRP",
            "t_min",
            "t_max",
            "em_min",
            "em_max",
            "CREATORDID",
            "access_url",
        ]:
            try:
                val = row[field]
                # Convert masked/nan to None
                if val is None or isinstance(val, float) and math.isnan(val):
                    rec[field] = None
                else:
                    rec[field] = val
            except Exception:
                rec[field] = None
        rows.append(rec)

    print(f"  {len(rows)} total result(s) from ESO")
    return rows


# ── MJD filtering ────────────────────────────────────────────────────────────


def filter_by_mjd(rows: list[dict], mjd_min: float, mjd_max: float) -> list[dict]:
    """Keep only rows whose t_min falls within [mjd_min, mjd_max]."""
    filtered = []
    for row in rows:
        t_min = row.get("t_min")
        if t_min is None:
            continue
        try:
            t = float(t_min)
        except (ValueError, TypeError):
            continue
        if mjd_min <= t <= mjd_max:
            filtered.append(row)
    print(f"  {len(filtered)} result(s) in MJD range [{mjd_min}, {mjd_max}]")
    return filtered


# ── Download ─────────────────────────────────────────────────────────────────


def download_fits(url: str, out_path: Path) -> bool:
    """Download a FITS file. Returns True on success."""
    session = requests.Session()
    session.headers["User-Agent"] = "NovaCat-OperatorTool/1.0"
    try:
        resp = session.get(url, timeout=_DOWNLOAD_TIMEOUT_S, stream=True)
    except requests.RequestException as exc:
        print(f"    {_RED}Download error: {exc}{_RESET}")
        return False

    if resp.status_code == 401:
        print(f"    {_YELLOW}401 — embargoed (proprietary period){_RESET}")
        return False
    if resp.status_code != 200:
        print(f"    {_RED}HTTP {resp.status_code}{_RESET}")
        return False

    content = resp.content
    if len(content) == 0:
        print(f"    {_YELLOW}Empty response body — likely embargo{_RESET}")
        return False

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(content)
    print(f"    {_GREEN}Saved{_RESET} ({len(content):,} bytes)")
    return True


# ── Display ──────────────────────────────────────────────────────────────────


def print_summary_table(rows: list[dict]) -> None:
    """Print a summary table of matched results."""
    if not rows:
        return
    print(f"\n{'Collection':<14} {'t_min (MJD)':<14} {'SNR':>8} {'CREATORDID':<40} {'access_url'}")
    print("-" * 120)
    for r in rows:
        collection = str(r.get("COLLECTION") or "?")[:13]
        t_min = f"{float(r['t_min']):.4f}" if r.get("t_min") is not None else "?"
        snr = f"{float(r['SNR']):.1f}" if r.get("SNR") is not None else "—"
        did = str(r.get("CREATORDID") or "—")[:39]
        url = str(r.get("access_url") or "—")
        print(f"{collection:<14} {t_min:<14} {snr:>8} {did:<40} {url}")
    print()


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ESO FITS files for a nova within an MJD range."
    )
    parser.add_argument("nova_name", help="Nova name (resolved via SIMBAD)")
    parser.add_argument("--mjd-min", type=float, required=True, help="MJD range start (inclusive)")
    parser.add_argument("--mjd-max", type=float, required=True, help="MJD range end (inclusive)")
    parser.add_argument(
        "--out-dir", default="./fits_downloads", help="Output directory (default: ./fits_downloads)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Query and display results without downloading"
    )
    args = parser.parse_args()

    if args.mjd_min > args.mjd_max:
        print(f"{_RED}mjd-min ({args.mjd_min}) > mjd-max ({args.mjd_max}){_RESET}")
        sys.exit(1)

    ra_deg, dec_deg = resolve_name(args.nova_name)
    rows = query_eso(ra_deg, dec_deg)
    filtered = filter_by_mjd(rows, args.mjd_min, args.mjd_max)

    print_summary_table(filtered)

    if not filtered:
        print("Nothing to download.")
        return

    if args.dry_run:
        print(f"{_DIM}Dry run — skipping downloads.{_RESET}")
        return

    # Download
    out_dir = Path(args.out_dir)
    safe_name = args.nova_name.replace(" ", "_")
    ok = 0
    fail = 0
    for i, row in enumerate(filtered, 1):
        url = row.get("access_url")
        if not url:
            print(f"  [{i}/{len(filtered)}] No access_url — skipping")
            fail += 1
            continue

        # Build filename from CREATORDID or index
        did = row.get("CREATORDID")
        if did:
            # e.g. "ivo://eso.org/ID?ADP.2014-10-01T..." → "ADP.2014-10-01T..."
            fname = str(did).split("?")[-1].split("/")[-1]
            fname = fname.replace(":", "_").replace("?", "_")
        else:
            t_min = row.get("t_min")
            fname = f"spectrum_{i:03d}_mjd{float(t_min):.2f}" if t_min else f"spectrum_{i:03d}"

        if not fname.endswith(".fits"):
            fname += ".fits"

        dest = out_dir / safe_name / fname
        print(f"  [{i}/{len(filtered)}] {fname}")
        if download_fits(str(url), dest):
            ok += 1
        else:
            fail += 1

    print(f"\nDone: {_GREEN}{ok} downloaded{_RESET}, {_RED if fail else _DIM}{fail} failed{_RESET}")
    if ok:
        print(f"Files in: {out_dir / safe_name}")


if __name__ == "__main__":
    main()
