"""
Nova Cat CloudWatch Log Viewer — Flask app.

Run with:
    python app.py

Then open http://localhost:5000 in your browser.

Personal operator tooling — not production code.
"""

import os

from cloudwatch_client import (
    discover_log_groups,
    fetch_logs_range,
    fetch_recent_logs,
    trace_by_correlation_id,
)
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# Cache discovered log groups so we don't re-query on every page load.
# Restart the server if you deploy new Lambdas and need to pick them up.
_cached_log_groups: list[str] | None = None


def _get_log_groups() -> list[str]:
    global _cached_log_groups
    if _cached_log_groups is None:
        _cached_log_groups = discover_log_groups()
    return _cached_log_groups


# ── Routes ───────────────────────────────────────────────────────────


@app.route("/")
def index():
    """Serve the main log viewer page."""
    return render_template("index.html")


@app.route("/api/log-groups")
def api_log_groups():
    """Return the list of discovered log groups."""
    try:
        groups = _get_log_groups()
        return jsonify({"log_groups": groups})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs")
def api_logs():
    """
    Fetch logs and return as JSON.

    Query params:
        start_epoch — explicit start time (Unix epoch seconds)
        end_epoch   — explicit end time (Unix epoch seconds)
        minutes     — fallback: how far back from now (default 30, max 1440)
                      ignored if start_epoch/end_epoch are provided
        limit       — max rows to return (default 2000, max 10000)
        log_groups  — comma-separated log group names (default: all)
        query       — custom Insights query string (optional)
    """
    try:
        limit = request.args.get("limit", "2000")
        limit = max(100, min(int(limit), 10000))

        groups_param = request.args.get("log_groups", "")
        if groups_param.strip():
            log_groups = [g.strip() for g in groups_param.split(",") if g.strip()]
        else:
            log_groups = _get_log_groups()

        custom_query = request.args.get("query", "").strip() or None

        start_param = request.args.get("start_epoch", "").strip()
        end_param = request.args.get("end_epoch", "").strip()

        if start_param and end_param:
            result = fetch_logs_range(
                start_epoch=int(start_param),
                end_epoch=int(end_param),
                query_string=custom_query,
                log_groups=log_groups,
                limit=limit,
            )
        else:
            minutes = request.args.get("minutes", "30")
            minutes = max(1, min(int(minutes), 1440))
            result = fetch_recent_logs(
                minutes=minutes,
                query_string=custom_query,
                log_groups=log_groups,
                limit=limit,
            )

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/trace")
def api_trace():
    """
    Trace a workflow by correlation_id — returns every log line
    sharing that ID, sorted chronologically.

    Query params:
        correlation_id  — required
        hours_back      — how far back to search (default 24, max 168)
    """
    try:
        correlation_id = request.args.get("correlation_id", "").strip()
        if not correlation_id:
            return jsonify({"error": "correlation_id is required"}), 400

        hours_back = request.args.get("hours_back", "24")
        hours_back = max(1, min(int(hours_back), 168))  # clamp to 1hr–7 days

        result = trace_by_correlation_id(
            correlation_id=correlation_id,
            hours_back=hours_back,
            log_groups=_get_log_groups(),
        )
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/refresh-groups", methods=["POST"])
def api_refresh_groups():
    """Force re-discovery of log groups (e.g. after deploying new Lambdas)."""
    global _cached_log_groups
    _cached_log_groups = None
    groups = _get_log_groups()
    return jsonify({"log_groups": groups})


# ── Entry point ──────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("LOG_VIEWER_PORT", "5000"))
    print(f"Nova Cat Log Viewer starting on http://localhost:{port}")
    print("Press Ctrl+C to stop.\n")
    app.run(debug=True, port=port)
