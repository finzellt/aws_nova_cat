"""
novacat_query.py — DynamoDB query interface for NovaCat.

Usage (CLI):
    python novacat_query.py --nova "V1324 Sco" --view spectra
    python novacat_query.py --nova "V1324 Sco" --view summary
    python novacat_query.py --nova "V1324 Sco" --view jobs
    python novacat_query.py --nova "V1324 Sco" --view refs
    python novacat_query.py --dashboard

Usage (notebook):
    from novacat_query import NovaCatQuery
    q = NovaCatQuery()
    df = q.spectra("V1324 Sco")
    df = q.jobs("V1324 Sco")
    stats = q.dashboard()
"""

from __future__ import annotations

import argparse

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

# ── Config ─────────────────────────────────────────────────────────────────────
TABLE_NAME = "NovaCat"
REGION = "us-east-1"


class NovaCatQuery:
    """
    Query interface for the NovaCat DynamoDB table.

    All per-nova methods accept a human name (primary or alias).
    Name resolution happens automatically via the NameMapping index.
    """

    def __init__(self, table_name: str = TABLE_NAME, region: str = REGION):
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)

    # ── Name resolution ────────────────────────────────────────────────────────

    def resolve_nova_id(self, name: str) -> str | None:
        """
        Resolve a nova name (primary name or alias) to its nova_id UUID.
        Returns None if the name is not found.
        """
        normalized = _normalize_name(name)
        resp = self.table.query(
            KeyConditionExpression=Key("PK").eq(f"NAME#{normalized}"),
            ProjectionExpression="nova_id",
            Limit=1,
        )
        items = resp.get("Items", [])
        if not items:
            return None
        return str(items[0].get("nova_id", "")) or None

    def get_nova_item(self, nova_id: str) -> dict | None:
        """Fetch the raw NOVA item by nova_id UUID."""
        resp = self.table.get_item(Key={"PK": nova_id, "SK": "NOVA"})
        return resp.get("Item")

    # ── View 1: Spectra products + validation status ───────────────────────────

    def spectra(self, name: str) -> pd.DataFrame:
        """
        View 1 (highest priority): All spectra data products for a nova,
        with acquisition and validation status.

        Columns: data_product_id, provider, acquisition_status,
                 validation_status, eligibility, fits_profile_id,
                 attempt_count, last_attempt_at, quarantine_reason_code,
                 sha256, byte_length, updated_at
        """
        nova_id = self._require_nova_id(name)
        items = self._query_prefix(nova_id, "PRODUCT#SPECTRA#")
        if not items:
            return pd.DataFrame()

        rows = [
            {
                "data_product_id": item.get("data_product_id"),
                "provider": item.get("provider"),
                "acquisition_status": item.get("acquisition_status"),
                "validation_status": item.get("validation_status"),
                "eligibility": item.get("eligibility"),
                "fits_profile_id": item.get("fits_profile_id"),
                "attempt_count": item.get("attempt_count"),
                "last_attempt_at": item.get("last_attempt_at"),
                "quarantine_reason_code": item.get("quarantine_reason_code"),
                "sha256": item.get("sha256"),
                "byte_length": item.get("byte_length"),
                "updated_at": item.get("updated_at"),
            }
            for item in items
        ]
        return pd.DataFrame(rows).sort_values(["provider", "data_product_id"])

    # ── View 2: Full nova summary ──────────────────────────────────────────────

    def summary(self, name: str) -> dict:
        """
        View 2: Everything stored for a nova, bucketed by item type.

        Returns a dict with keys:
            nova          — raw Nova item (dict)
            photometry    — raw photometry DataProduct item (dict or None)
            spectra       — list of spectra DataProduct items
            refs          — list of NovaReference items
            jobs          — list of JobRun items
            attempts      — list of Attempt items
            files         — list of FileObject items
        """
        nova_id = self._require_nova_id(name)
        all_items = self._query_all(nova_id)

        result: dict = {
            "nova": None,
            "photometry": None,
            "spectra": [],
            "refs": [],
            "jobs": [],
            "attempts": [],
            "files": [],
        }

        for item in all_items:
            sk = item.get("SK", "")
            if sk == "NOVA":
                result["nova"] = item
            elif sk == "PRODUCT#PHOTOMETRY_TABLE":
                result["photometry"] = item
            elif sk.startswith("PRODUCT#SPECTRA#"):
                result["spectra"].append(item)
            elif sk.startswith("NOVAREF#"):
                result["refs"].append(item)
            elif sk.startswith("JOBRUN#"):
                result["jobs"].append(item)
            elif sk.startswith("ATTEMPT#"):
                result["attempts"].append(item)
            elif sk.startswith("FILE#"):
                result["files"].append(item)

        return result

    def summary_df(self, name: str) -> pd.DataFrame:
        """
        Convenience: returns a one-row DataFrame of key Nova fields,
        useful for display in a notebook.
        """
        s = self.summary(name)
        nova = s["nova"]
        if not nova:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "nova_id": nova.get("nova_id"),
                    "primary_name": nova.get("primary_name"),
                    "status": nova.get("status"),
                    "ra_deg": nova.get("ra_deg"),
                    "dec_deg": nova.get("dec_deg"),
                    "discovery_date": nova.get("discovery_date"),
                    "n_spectra": len(s["spectra"]),
                    "n_refs": len(s["refs"]),
                    "n_jobs": len(s["jobs"]),
                    "n_files": len(s["files"]),
                    "aliases": ", ".join(nova.get("aliases", [])),
                    "created_at": nova.get("created_at"),
                    "updated_at": nova.get("updated_at"),
                }
            ]
        )

    # ── View 3: Job run history ────────────────────────────────────────────────

    def jobs(self, name: str) -> pd.DataFrame:
        """
        View 3: Job run history for a nova, most recent first.

        Columns: workflow_name, status, started_at, ended_at,
                 correlation_id, job_run_id, error_classification,
                 error_fingerprint
        """
        nova_id = self._require_nova_id(name)
        items = self._query_prefix(nova_id, "JOBRUN#")
        if not items:
            return pd.DataFrame()

        rows = [
            {
                "workflow_name": item.get("workflow_name"),
                "status": item.get("status"),
                "started_at": item.get("started_at"),
                "ended_at": item.get("ended_at"),
                "correlation_id": item.get("correlation_id"),
                "job_run_id": item.get("job_run_id"),
                "error_classification": item.get("error_classification"),
                "error_fingerprint": item.get("error_fingerprint"),
            }
            for item in items
        ]
        df = pd.DataFrame(rows)
        if not df.empty and "started_at" in df.columns:
            df = df.sort_values("started_at", ascending=False)
        return df

    def attempts(self, name: str, job_run_id: str | None = None) -> pd.DataFrame:
        """
        Attempt records for a nova. Optionally filtered to a single job_run_id.

        Columns: job_run_id, task_name, attempt_number, status,
                 duration_ms, error_type, error_message
        """
        nova_id = self._require_nova_id(name)
        prefix = f"ATTEMPT#{job_run_id}#" if job_run_id else "ATTEMPT#"
        items = self._query_prefix(nova_id, prefix)
        if not items:
            return pd.DataFrame()

        rows = [
            {
                "job_run_id": item.get("job_run_id"),
                "task_name": item.get("task_name"),
                "attempt_number": item.get("attempt_number"),
                "status": item.get("status"),
                "duration_ms": item.get("duration_ms"),
                "error_type": item.get("error_type"),
                "error_message": item.get("error_message"),
            }
            for item in items
        ]
        return pd.DataFrame(rows)

    # ── View 4: References ─────────────────────────────────────────────────────

    def refs(self, name: str) -> pd.DataFrame:
        """
        View 4: References linked to a nova, with Reference metadata joined.
        Sorted by year descending.

        Columns: bibcode, role, reference_type, year, title,
                 authors, doi, added_by_workflow
        """
        nova_id = self._require_nova_id(name)
        novaref_items = self._query_prefix(nova_id, "NOVAREF#")
        if not novaref_items:
            return pd.DataFrame()

        rows = []
        for item in novaref_items:
            bibcode = item.get("bibcode")
            if not isinstance(bibcode, str):
                continue
            ref = self._get_reference(bibcode) or {}
            author_list = ref.get("authors", [])
            author_str = (
                "; ".join(author_list[:3]) + (" et al." if len(author_list) > 3 else "")
                if author_list
                else ""
            )
            rows.append(
                {
                    "bibcode": bibcode,
                    "role": item.get("role"),
                    "reference_type": ref.get("reference_type"),
                    "year": ref.get("year"),
                    "title": ref.get("title"),
                    "authors": author_str,
                    "doi": ref.get("doi"),
                    "added_by_workflow": item.get("added_by_workflow"),
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty and "year" in df.columns:
            df = df.sort_values("year", ascending=False, na_position="last")
        return df

    # ── View 5: Name mappings ──────────────────────────────────────────────────

    def name_mappings(self, name: str) -> pd.DataFrame:
        """
        View 5: All NameMapping items associated with a nova.

        Each alias stored on the Nova item corresponds to a NAME#<normalized>
        item in the table. This view fetches them all and shows the raw
        mapping fields — useful for verifying alias ingestion and understanding
        how names resolve to this nova_id.

        Columns: normalized_name, original_name, nova_id, source, created_at
        """
        nova_id = self._require_nova_id(name)
        nova = self.get_nova_item(nova_id)
        if not nova:
            return pd.DataFrame()

        aliases = nova.get("aliases", [])
        if not aliases:
            return pd.DataFrame()

        rows = []
        for alias in aliases:
            normalized = _normalize_name(str(alias))
            resp = self.table.get_item(Key={"PK": f"NAME#{normalized}", "SK": "NAMEMAPPING"})
            item = resp.get("Item")
            if item:
                rows.append(
                    {
                        "normalized_name": normalized,
                        "original_name": item.get("original_name") or alias,
                        "nova_id": item.get("nova_id"),
                        "source": item.get("source"),
                        "created_at": item.get("created_at"),
                    }
                )
            else:
                rows.append(
                    {
                        "normalized_name": normalized,
                        "original_name": alias,
                        "nova_id": nova_id,
                        "source": "(mapping item missing)",
                        "created_at": None,
                    }
                )

        return pd.DataFrame(rows).sort_values("normalized_name")

    def global_references(self, name: str) -> pd.DataFrame:
        """
        Fetch the global Reference entities (PK=REFERENCE#<bibcode>) for all
        bibcodes linked to this nova via NOVAREF items.

        Complements refs() — while refs() shows the per-nova link record joined
        with reference metadata, this returns the raw global Reference items
        exactly as stored, useful for verifying upsert correctness.

        Columns: bibcode, reference_type, title, year, publication_date,
                 authors, doi, arxiv_id, created_at, updated_at
        """
        nova_id = self._require_nova_id(name)
        novaref_items = self._query_prefix(nova_id, "NOVAREF#")
        if not novaref_items:
            return pd.DataFrame()

        rows = []
        for novaref in novaref_items:
            bibcode = novaref.get("bibcode")
            if not isinstance(bibcode, str):
                continue
            ref = self._get_reference(bibcode)
            if ref:
                author_list = ref.get("authors", [])
                rows.append(
                    {
                        "bibcode": bibcode,
                        "reference_type": ref.get("reference_type"),
                        "title": ref.get("title"),
                        "year": ref.get("year"),
                        "publication_date": ref.get("publication_date"),
                        "authors": "; ".join(author_list[:3])
                        + (" et al." if len(author_list) > 3 else "")
                        if author_list
                        else "",
                        "doi": ref.get("doi"),
                        "arxiv_id": ref.get("arxiv_id"),
                        "created_at": ref.get("created_at"),
                        "updated_at": ref.get("updated_at"),
                    }
                )
            else:
                rows.append({"bibcode": bibcode, "reference_type": "(global item missing)"})

        df = pd.DataFrame(rows)
        if not df.empty and "year" in df.columns:
            df = df.sort_values("year", ascending=False, na_position="last")
        return df

    # ── Dashboard ──────────────────────────────────────────────────────────────

    def dashboard(self) -> dict:
        """
        Scan the full NovaCat table and return aggregate counts.

        Returns a dict with keys: total_novas, active_novas,
        quarantined_novas, total_spectra_products, spectra_valid,
        spectra_quarantined, spectra_unvalidated, spectra_terminal_invalid,
        photometry_tables, total_references, total_job_runs.

        NOTE: This performs a full table scan. For MVP catalog sizes this is
        fast and cheap. When the catalog grows, replace this with a read of a
        dedicated STATS item (e.g. PK="STATS", SK="GLOBAL") that is atomically
        incremented on each write path. The interface of this method stays the same.
        """
        counts = {
            "total_novas": 0,
            "active_novas": 0,
            "quarantined_novas": 0,
            "total_spectra_products": 0,
            "spectra_valid": 0,
            "spectra_quarantined": 0,
            "spectra_unvalidated": 0,
            "spectra_terminal_invalid": 0,
            "photometry_tables": 0,
            "total_references": 0,
            "total_job_runs": 0,
        }

        paginator = self.table.meta.client.get_paginator("scan")
        pages = paginator.paginate(
            TableName=self.table.name,
            ProjectionExpression="entity_type, #st, product_type, validation_status",
            ExpressionAttributeNames={"#st": "status"},
        )

        for page in pages:
            for item in page["Items"]:
                et = item.get("entity_type")

                if et == "Nova":
                    counts["total_novas"] += 1
                    s = item.get("status")
                    if s == "ACTIVE":
                        counts["active_novas"] += 1
                    elif s == "QUARANTINED":
                        counts["quarantined_novas"] += 1

                elif et == "DataProduct":
                    pt = item.get("product_type")
                    if pt == "PHOTOMETRY_TABLE":
                        counts["photometry_tables"] += 1
                    elif pt == "SPECTRA":
                        counts["total_spectra_products"] += 1
                        vs = item.get("validation_status")
                        if vs == "VALID":
                            counts["spectra_valid"] += 1
                        elif vs == "QUARANTINED":
                            counts["spectra_quarantined"] += 1
                        elif vs == "UNVALIDATED":
                            counts["spectra_unvalidated"] += 1
                        elif vs == "TERMINAL_INVALID":
                            counts["spectra_terminal_invalid"] += 1

                elif et == "Reference":
                    counts["total_references"] += 1

                elif et == "JobRun":
                    counts["total_job_runs"] += 1

        return counts

    def dashboard_df(self) -> pd.DataFrame:
        """Returns dashboard counts as a tidy two-column DataFrame."""
        stats = self.dashboard()
        rows = [{"metric": k.replace("_", " ").title(), "count": v} for k, v in stats.items()]
        return pd.DataFrame(rows)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _require_nova_id(self, name: str) -> str:
        nova_id = self.resolve_nova_id(name)
        if not nova_id:
            raise ValueError(f"Nova not found: {name!r}\nCheck the name spelling or try an alias.")
        return nova_id

    def _query_prefix(self, nova_id: str, prefix: str) -> list[dict]:
        """Query all items in a nova partition with a given SK prefix. Handles pagination."""
        items = []
        kwargs: dict = dict(
            KeyConditionExpression=(Key("PK").eq(nova_id) & Key("SK").begins_with(prefix))
        )
        while True:
            resp = self.table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
            kwargs["ExclusiveStartKey"] = last
        return items

    def _query_all(self, nova_id: str) -> list[dict]:
        """Query all items in a nova partition. Handles pagination."""
        items = []
        kwargs: dict = dict(KeyConditionExpression=Key("PK").eq(nova_id))
        while True:
            resp = self.table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
            kwargs["ExclusiveStartKey"] = last
        return items

    def _get_reference(self, bibcode: str) -> dict | None:
        resp = self.table.get_item(Key={"PK": f"REFERENCE#{bibcode}", "SK": "METADATA"})
        return resp.get("Item")


# ── Name normalization ─────────────────────────────────────────────────────────


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
        description="NovaCat DynamoDB query interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python novacat_query.py --dashboard
  python novacat_query.py --nova "V1324 Sco" --view spectra
  python novacat_query.py --nova "V1324 Sco" --view summary
  python novacat_query.py --nova "V1324 Sco" --view jobs
  python novacat_query.py --nova "V1324 Sco" --view refs
        """,
    )
    parser.add_argument("--nova", type=str, help="Nova name (primary or alias)")
    parser.add_argument(
        "--view",
        choices=["spectra", "summary", "jobs", "refs", "name_mappings", "global_refs"],
        default="spectra",
        help="Which per-nova view to display (default: spectra)",
    )
    parser.add_argument("--dashboard", action="store_true", help="Show table-wide stats")
    args = parser.parse_args()

    q = NovaCatQuery()

    if args.dashboard:
        stats = q.dashboard()
        _hr()
        print("  NovaCat — Table Dashboard")
        _hr()
        for k, v in stats.items():
            label = k.replace("_", " ").title()
            print(f"  {label:<32} {v}")
        print()
        return

    if not args.nova:
        parser.error("--nova is required unless --dashboard is used")

    if args.view == "spectra":
        _print_df(q.spectra(args.nova), f"Spectra Products — {args.nova}")

    elif args.view == "summary":
        s = q.summary(args.nova)
        nova = s["nova"]
        if not nova:
            print("Nova item not found.")
            return
        _hr()
        print(f"  Nova Summary — {args.nova}")
        _hr()
        print(f"  nova_id:          {nova.get('nova_id')}")
        print(f"  status:           {nova.get('status')}")
        print(f"  ra_deg:           {nova.get('ra_deg')}")
        print(f"  dec_deg:          {nova.get('dec_deg')}")
        print(f"  discovery_date:   {nova.get('discovery_date')}")
        print(f"  aliases:          {', '.join(nova.get('aliases', []))}")
        print()
        print(f"  Spectra products: {len(s['spectra'])}")
        print(f"  References:       {len(s['refs'])}")
        print(f"  Job runs:         {len(s['jobs'])}")
        print(f"  Files:            {len(s['files'])}")
        print(f"  created_at:       {nova.get('created_at')}")
        print(f"  updated_at:       {nova.get('updated_at')}")
        print()

    elif args.view == "jobs":
        _print_df(q.jobs(args.nova), f"Job Run History — {args.nova}")

    elif args.view == "refs":
        _print_df(q.refs(args.nova), f"References — {args.nova}")

    elif args.view == "name_mappings":
        _print_df(q.name_mappings(args.nova), f"Name Mappings — {args.nova}")

    elif args.view == "global_refs":
        _print_df(q.global_references(args.nova), f"Global Reference Entities — {args.nova}")


if __name__ == "__main__":
    main()
