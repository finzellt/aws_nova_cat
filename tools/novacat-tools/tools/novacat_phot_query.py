"""
novacat_phot_query.py — DynamoDB query interface for NovaCat Photometry.

Queries the dedicated NovaCatPhotometry table (PhotometryRow items) and
the main NovaCat table (for name resolution via NameMapping items).

Two query modes:
  1. Per-nova views    — rows, band breakdown, regime summary, provenance
  2. Table-wide stats  — dashboard scan of the photometry table

Usage (notebook):
    from novacat_phot_query import NovaCatPhotQuery
    pq = NovaCatPhotQuery()

    # All photometry rows for a nova
    df = pq.rows("V4739 Sgr")

    # Band breakdown — one row per band with counts, mag range, MJD range
    df = pq.band_summary("V4739 Sgr")

    # Regime breakdown — one row per regime
    df = pq.regime_summary("V4739 Sgr")

    # Provenance — one row per bibcode/source
    df = pq.provenance("V4739 Sgr")

    # Table-wide stats
    stats = pq.dashboard()

Usage (CLI):
    python novacat_phot_query.py --nova "V4739 Sgr" --view rows
    python novacat_phot_query.py --nova "V4739 Sgr" --view bands
    python novacat_phot_query.py --nova "V4739 Sgr" --view regimes
    python novacat_phot_query.py --nova "V4739 Sgr" --view provenance
    python novacat_phot_query.py --dashboard
"""

from __future__ import annotations

import argparse

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

# ── Config ─────────────────────────────────────────────────────────────────────
MAIN_TABLE_NAME = "NovaCat"
PHOTOMETRY_TABLE_NAME = "NovaCatPhotometry"
REGION = "us-east-1"


class NovaCatPhotQuery:
    """
    Query interface for the NovaCat dedicated photometry DynamoDB table.

    Name resolution uses the main NovaCat table (NameMapping items).
    All photometry queries target the NovaCatPhotometry table.

    All per-nova methods accept a human name (primary or alias).
    """

    def __init__(
        self,
        main_table_name: str = MAIN_TABLE_NAME,
        photometry_table_name: str = PHOTOMETRY_TABLE_NAME,
        region: str = REGION,
    ):
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.main_table = self.dynamodb.Table(main_table_name)
        self.phot_table = self.dynamodb.Table(photometry_table_name)

    # ── Name resolution (via main NovaCat table) ───────────────────────────────

    def resolve_nova_id(self, name: str) -> str | None:
        """
        Resolve a nova name (primary name or alias) to its nova_id UUID.
        Uses the main NovaCat table's NameMapping items.
        Returns None if the name is not found.
        """
        normalized = _normalize_name(name)
        resp = self.main_table.query(
            KeyConditionExpression=Key("PK").eq(f"NAME#{normalized}"),
            ProjectionExpression="nova_id",
            Limit=1,
        )
        items = resp.get("Items", [])
        if not items:
            return None
        return str(items[0].get("nova_id", "")) or None

    # ── View 1: All photometry rows ───────────────────────────────────────────

    def rows(
        self,
        name: str,
        band: str | None = None,
        regime: str | None = None,
        upper_limits: bool | None = None,
    ) -> pd.DataFrame:
        """
        All PhotometryRow items for a nova, with optional filters.

        Returns a DataFrame sorted by time_mjd ascending. Each row is one
        photometric measurement.

        Args:
            name:         Nova name (primary or alias).
            band:         Optional band_id filter (e.g. "JohnsonCousins_V").
            regime:       Optional regime filter (e.g. "optical", "xray").
            upper_limits: If True, only upper limits. If False, only
                          detections. If None (default), both.

        Columns: row_id, time_mjd, band_id, regime, filter_name,
                 magnitude, mag_err, flux_density, flux_density_err,
                 flux_density_unit, count_rate, count_rate_err,
                 is_upper_limit, quality_flag, phot_system, mag_system,
                 telescope, instrument, bibcode, ingested_at
        """
        nova_id = self._require_nova_id(name)
        items = self._query_phot_prefix(nova_id, "PHOT#")
        if not items:
            return pd.DataFrame()

        rows = []
        for item in items:
            # Apply client-side filters
            if band and item.get("band_id") != band:
                continue
            if regime and item.get("regime") != regime:
                continue
            if upper_limits is True and not item.get("is_upper_limit"):
                continue
            if upper_limits is False and item.get("is_upper_limit"):
                continue

            rows.append(_extract_row(item))

        df = pd.DataFrame(rows)
        if not df.empty:
            df = _coerce_numerics(df)
            if "time_mjd" in df.columns:
                df = df.sort_values("time_mjd", ascending=True).reset_index(drop=True)
        return df

    # ── View 2: Band summary ──────────────────────────────────────────────────

    def band_summary(self, name: str) -> pd.DataFrame:
        """
        Aggregate photometry counts and ranges per band_id.

        Returns one row per band with total count, detection/upper-limit
        breakdown, magnitude range, and MJD time span.

        Columns: band_id, regime, filter_name, total, detections,
                 upper_limits, mag_min, mag_max, mjd_min, mjd_max,
                 n_sources
        """
        df = self.rows(name)
        if df.empty:
            return pd.DataFrame()

        agg_rows = []
        for band_id, grp in df.groupby("band_id", dropna=False):
            is_ul = grp["is_upper_limit"].fillna(False).astype(bool)
            detections = grp[~is_ul]
            agg_rows.append(
                {
                    "band_id": band_id,
                    "regime": grp["regime"].iloc[0] if not grp["regime"].isna().all() else None,
                    "filter_name": (
                        grp["filter_name"].iloc[0] if not grp["filter_name"].isna().all() else None
                    ),
                    "total": len(grp),
                    "detections": int((~is_ul).sum()),
                    "upper_limits": int(is_ul.sum()),
                    "mag_min": detections["magnitude"].min()
                    if not detections["magnitude"].isna().all()
                    else None,
                    "mag_max": detections["magnitude"].max()
                    if not detections["magnitude"].isna().all()
                    else None,
                    "mjd_min": grp["time_mjd"].min(),
                    "mjd_max": grp["time_mjd"].max(),
                    "n_sources": grp["bibcode"].nunique() if "bibcode" in grp.columns else None,
                }
            )

        return pd.DataFrame(agg_rows).sort_values("total", ascending=False).reset_index(drop=True)

    # ── View 3: Regime summary ────────────────────────────────────────────────

    def regime_summary(self, name: str) -> pd.DataFrame:
        """
        Aggregate photometry counts per regime (optical, xray, radio, etc.).

        Columns: regime, total, detections, upper_limits, n_bands, n_sources,
                 mjd_min, mjd_max
        """
        df = self.rows(name)
        if df.empty:
            return pd.DataFrame()

        agg_rows = []
        for regime, grp in df.groupby("regime", dropna=False):
            is_ul = grp["is_upper_limit"].fillna(False).astype(bool)
            agg_rows.append(
                {
                    "regime": regime,
                    "total": len(grp),
                    "detections": int((~is_ul).sum()),
                    "upper_limits": int(is_ul.sum()),
                    "n_bands": grp["band_id"].nunique(),
                    "n_sources": grp["bibcode"].nunique() if "bibcode" in grp.columns else None,
                    "mjd_min": grp["time_mjd"].min(),
                    "mjd_max": grp["time_mjd"].max(),
                }
            )

        return pd.DataFrame(agg_rows).sort_values("total", ascending=False).reset_index(drop=True)

    # ── View 4: Provenance ────────────────────────────────────────────────────

    def provenance(self, name: str) -> pd.DataFrame:
        """
        Provenance breakdown: one row per unique bibcode/source.

        Columns: bibcode, doi, orig_catalog, telescope, instrument,
                 observer, data_origin, n_rows, n_bands, regime_list,
                 mjd_min, mjd_max
        """
        df = self.rows(name)
        if df.empty:
            return pd.DataFrame()

        # Fetch full rows with provenance columns
        nova_id = self._require_nova_id(name)
        items = self._query_phot_prefix(nova_id, "PHOT#")
        prov_rows = []
        for item in items:
            prov_rows.append(
                {
                    "bibcode": item.get("bibcode"),
                    "doi": item.get("doi"),
                    "orig_catalog": item.get("orig_catalog"),
                    "telescope": item.get("telescope"),
                    "instrument": item.get("instrument"),
                    "observer": item.get("observer"),
                    "data_origin": item.get("data_origin"),
                    "band_id": item.get("band_id"),
                    "regime": item.get("regime"),
                    "time_mjd": _to_float(item.get("time_mjd")),
                }
            )

        pdf = pd.DataFrame(prov_rows)
        if pdf.empty:
            return pd.DataFrame()

        # Group by bibcode (or orig_catalog for non-literature sources)
        group_key = "bibcode"
        agg_rows = []
        for key, grp in pdf.groupby(group_key, dropna=False):
            regimes = sorted(grp["regime"].dropna().unique().tolist())
            agg_rows.append(
                {
                    "bibcode": key,
                    "doi": grp["doi"].dropna().iloc[0] if grp["doi"].notna().any() else None,
                    "orig_catalog": (
                        grp["orig_catalog"].dropna().iloc[0]
                        if grp["orig_catalog"].notna().any()
                        else None
                    ),
                    "telescope": (
                        grp["telescope"].dropna().iloc[0]
                        if grp["telescope"].notna().any()
                        else None
                    ),
                    "instrument": (
                        grp["instrument"].dropna().iloc[0]
                        if grp["instrument"].notna().any()
                        else None
                    ),
                    "observer": (
                        grp["observer"].dropna().iloc[0] if grp["observer"].notna().any() else None
                    ),
                    "data_origin": (
                        grp["data_origin"].dropna().iloc[0]
                        if grp["data_origin"].notna().any()
                        else None
                    ),
                    "n_rows": len(grp),
                    "n_bands": grp["band_id"].nunique(),
                    "regime_list": ", ".join(regimes) if regimes else None,
                    "mjd_min": grp["time_mjd"].min(),
                    "mjd_max": grp["time_mjd"].max(),
                }
            )

        return pd.DataFrame(agg_rows).sort_values("n_rows", ascending=False).reset_index(drop=True)

    # ── View 5: Envelope (from main table) ────────────────────────────────────

    def envelope(self, name: str) -> dict | None:
        """
        Fetch the PRODUCT#PHOTOMETRY_TABLE envelope item from the main
        NovaCat table. Contains ingestion metadata (last_ingestion_at,
        ingestion_count, schema_version) — not row data.
        """
        nova_id = self._require_nova_id(name)
        resp = self.main_table.get_item(Key={"PK": nova_id, "SK": "PRODUCT#PHOTOMETRY_TABLE"})
        return resp.get("Item")

    # ── View 6: Row count (fast) ──────────────────────────────────────────────

    def row_count(self, name: str) -> int:
        """
        Count PhotometryRow items for a nova without fetching full items.
        Uses Select=COUNT for efficiency.
        """
        nova_id = self._require_nova_id(name)
        total = 0
        kwargs: dict = dict(
            KeyConditionExpression=(Key("PK").eq(nova_id) & Key("SK").begins_with("PHOT#")),
            Select="COUNT",
        )
        while True:
            resp = self.phot_table.query(**kwargs)
            total += resp.get("Count", 0)
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
            kwargs["ExclusiveStartKey"] = last
        return total

    # ── Dashboard ──────────────────────────────────────────────────────────────

    def dashboard(self) -> dict:
        """
        Scan the NovaCatPhotometry table and return aggregate counts.

        Returns a dict with keys: total_rows, nova_count, regime_breakdown,
        band_count, upper_limit_count, detection_count, quality_flag_breakdown.

        NOTE: Full table scan. Fine at current catalog size; replace with a
        STATS item read when the table grows.
        """
        counts: dict = {
            "total_rows": 0,
            "nova_ids": set(),
            "band_ids": set(),
            "upper_limit_count": 0,
            "detection_count": 0,
            "regime_breakdown": {},
            "quality_flag_breakdown": {},
        }

        paginator = self.phot_table.meta.client.get_paginator("scan")
        pages = paginator.paginate(
            TableName=self.phot_table.name,
            ProjectionExpression="PK, band_id, regime, is_upper_limit, quality_flag",
        )

        for page in pages:
            for item in page.get("Items", []):
                counts["total_rows"] += 1
                counts["nova_ids"].add(item.get("PK"))
                band = item.get("band_id")
                if band:
                    counts["band_ids"].add(band)

                regime = item.get("regime", "unknown")
                counts["regime_breakdown"][regime] = counts["regime_breakdown"].get(regime, 0) + 1

                if item.get("is_upper_limit"):
                    counts["upper_limit_count"] += 1
                else:
                    counts["detection_count"] += 1

                qf = str(item.get("quality_flag", "unknown"))
                counts["quality_flag_breakdown"][qf] = (
                    counts["quality_flag_breakdown"].get(qf, 0) + 1
                )

        return {
            "total_rows": counts["total_rows"],
            "nova_count": len(counts["nova_ids"]),
            "band_count": len(counts["band_ids"]),
            "detection_count": counts["detection_count"],
            "upper_limit_count": counts["upper_limit_count"],
            "regime_breakdown": dict(sorted(counts["regime_breakdown"].items())),
            "quality_flag_breakdown": dict(sorted(counts["quality_flag_breakdown"].items())),
        }

    def dashboard_df(self) -> pd.DataFrame:
        """Returns dashboard counts as a tidy two-column DataFrame."""
        stats = self.dashboard()
        rows = []
        for k, v in stats.items():
            if isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    rows.append(
                        {"metric": f"{k.replace('_', ' ').title()} — {sub_k}", "count": sub_v}
                    )
            else:
                rows.append({"metric": k.replace("_", " ").title(), "count": v})
        return pd.DataFrame(rows)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _require_nova_id(self, name: str) -> str:
        nova_id = self.resolve_nova_id(name)
        if not nova_id:
            raise ValueError(f"Nova not found: {name!r}\nCheck the name spelling or try an alias.")
        return nova_id

    def _query_phot_prefix(self, nova_id: str, prefix: str) -> list[dict]:
        """Query all items in a nova partition with a given SK prefix. Handles pagination."""
        items: list[dict] = []
        kwargs: dict = dict(
            KeyConditionExpression=(Key("PK").eq(nova_id) & Key("SK").begins_with(prefix))
        )
        while True:
            resp = self.phot_table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
            kwargs["ExclusiveStartKey"] = last
        return items


# ── Row extraction ─────────────────────────────────────────────────────────────


def _extract_row(item: dict) -> dict:
    """Extract display columns from a raw PhotometryRow DDB item."""
    return {
        "row_id": item.get("SK", "").removeprefix("PHOT#"),
        "time_mjd": _to_float(item.get("time_mjd")),
        "band_id": item.get("band_id"),
        "regime": item.get("regime"),
        "filter_name": item.get("filter_name"),
        "magnitude": _to_float(item.get("magnitude")),
        "mag_err": _to_float(item.get("mag_err")),
        "flux_density": _to_float(item.get("flux_density")),
        "flux_density_err": _to_float(item.get("flux_density_err")),
        "flux_density_unit": item.get("flux_density_unit"),
        "count_rate": _to_float(item.get("count_rate")),
        "count_rate_err": _to_float(item.get("count_rate_err")),
        "is_upper_limit": item.get("is_upper_limit"),
        "quality_flag": item.get("quality_flag"),
        "phot_system": item.get("phot_system"),
        "mag_system": item.get("mag_system"),
        "telescope": item.get("telescope"),
        "instrument": item.get("instrument"),
        "bibcode": item.get("bibcode"),
        "ingested_at": item.get("ingested_at"),
    }


# ── Helpers ────────────────────────────────────────────────────────────────────


def _to_float(value: object) -> float | None:
    """Safely convert a DynamoDB Decimal (or string) to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _coerce_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Convert known numeric columns from object → float64."""
    numeric_cols = [
        "time_mjd",
        "magnitude",
        "mag_err",
        "flux_density",
        "flux_density_err",
        "count_rate",
        "count_rate_err",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _normalize_name(name: str) -> str:
    """
    Lowercase and collapse whitespace.
    Mirrors NovaCat's normalization in initialize_nova / nova_resolver.
    """
    return " ".join(name.lower().split())


# ── CLI ────────────────────────────────────────────────────────────────────────


def _hr() -> None:
    print("━" * 64)


def _print_df(df: pd.DataFrame, title: str) -> None:
    _hr()
    print(f"  {title}")
    _hr()
    if df.empty:
        print("  (no results)")
    else:
        with pd.option_context("display.max_columns", None, "display.width", 160):
            print(df.to_string(index=False))
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NovaCat Photometry DynamoDB query interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python novacat_phot_query.py --dashboard
  python novacat_phot_query.py --nova "V4739 Sgr" --view rows
  python novacat_phot_query.py --nova "V4739 Sgr" --view bands
  python novacat_phot_query.py --nova "V4739 Sgr" --view regimes
  python novacat_phot_query.py --nova "V4739 Sgr" --view provenance
  python novacat_phot_query.py --nova "V4739 Sgr" --view envelope
  python novacat_phot_query.py --nova "V4739 Sgr" --view count
        """,
    )
    parser.add_argument("--nova", type=str, help="Nova name (primary or alias)")
    parser.add_argument(
        "--view",
        choices=["rows", "bands", "regimes", "provenance", "envelope", "count"],
        default="rows",
        help="Which per-nova view to display (default: rows)",
    )
    parser.add_argument("--band", type=str, help="Filter --view rows to a specific band_id")
    parser.add_argument("--regime", type=str, help="Filter --view rows to a specific regime")
    parser.add_argument("--dashboard", action="store_true", help="Show table-wide stats")
    args = parser.parse_args()

    pq = NovaCatPhotQuery()

    if args.dashboard:
        stats = pq.dashboard()
        _hr()
        print("  NovaCat Photometry — Table Dashboard")
        _hr()
        for k, v in stats.items():
            label = k.replace("_", " ").title()
            if isinstance(v, dict):
                print(f"  {label}:")
                for sub_k, sub_v in v.items():
                    print(f"    {sub_k:<24} {sub_v}")
            else:
                print(f"  {label:<32} {v}")
        print()
        return

    if not args.nova:
        parser.error("--nova is required unless --dashboard is used")

    if args.view == "rows":
        df = pq.rows(args.nova, band=args.band, regime=args.regime)
        label = f"Photometry Rows — {args.nova}"
        if args.band:
            label += f" (band={args.band})"
        if args.regime:
            label += f" (regime={args.regime})"
        _print_df(df, label)

    elif args.view == "bands":
        _print_df(pq.band_summary(args.nova), f"Band Summary — {args.nova}")

    elif args.view == "regimes":
        _print_df(pq.regime_summary(args.nova), f"Regime Summary — {args.nova}")

    elif args.view == "provenance":
        _print_df(pq.provenance(args.nova), f"Provenance — {args.nova}")

    elif args.view == "envelope":
        env = pq.envelope(args.nova)
        _hr()
        print(f"  Photometry Envelope — {args.nova}")
        _hr()
        if not env:
            print("  (no PRODUCT#PHOTOMETRY_TABLE item found)")
        else:
            for k, v in env.items():
                if k in ("PK", "SK"):
                    continue
                print(f"  {k:<32} {v}")
        print()

    elif args.view == "count":
        count = pq.row_count(args.nova)
        print(f"{args.nova}: {count} photometry row(s)")


if __name__ == "__main__":
    main()
