"""sparkline.svg artifact generator (DESIGN-003 §9).

Generates a per-nova pre-rendered SVG thumbnail of the optical light
curve for the catalog table's "Light Curve" column.

Input source (§9.2):
    Per-nova context — processed photometry observations and band
    metadata from the ``photometry.json`` generator (§8).  No
    independent DDB query.

Output:
    Raw SVG string (``schema_version`` not applicable — this is an
    image, not a JSON artifact).  ``None`` when no sparkline can be
    generated.

Side effects on *nova_context*:
    ``has_sparkline`` — bool, whether a sparkline was produced.
"""

from __future__ import annotations

import logging
from typing import Any

from generators.shared import lttb

_logger = logging.getLogger("artifact_generator")

# ---------------------------------------------------------------------------
# Constants (§9.3–§9.6)
# ---------------------------------------------------------------------------

_SPARKLINE_BAND_MIN_POINTS = 5  # Preferred threshold for band selection
_SPARKLINE_BAND_ABS_MIN = 2  # Hard minimum — below this, no sparkline
_LTTB_THRESHOLD = 90  # Target points (= sparkline width in px)

# Viewport dimensions (§9.5).
_SVG_WIDTH = 90
_SVG_HEIGHT = 55
_PADDING = 4  # px on each side
_DRAW_X_MIN = _PADDING  # 4
_DRAW_X_MAX = _SVG_WIDTH - _PADDING  # 86
_DRAW_Y_MIN = _PADDING  # 4 (top — brightest)
_DRAW_Y_MAX = _SVG_HEIGHT - _PADDING  # 51 (bottom — faintest)

# Visual styling (§9.6).
_STROKE_COLOR = "#2A7D7B"  # ADR-012 --color-interactive (teal accent)
_FILL_OPACITY = "0.12"
_STROKE_WIDTH = "1.5"

# Preferred band for sparkline (§9.3).
_PREFERRED_BAND = "V"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_sparkline_svg(
    nova_id: str,
    nova_context: dict[str, Any],
) -> str | None:
    """Generate the ``sparkline.svg`` artifact for a nova.

    Parameters
    ----------
    nova_id
        Nova UUID string.
    nova_context
        Must contain ``photometry_observations`` and
        ``photometry_bands`` from the photometry generator.

    Returns
    -------
    str | None
        Raw SVG string, or ``None`` if no sparkline can be generated
        (no optical data with ≥ 2 points).
    """
    observations: list[dict[str, Any]] = nova_context.get(
        "photometry_observations",
        [],
    )

    # Step 1 — Filter to optical-regime observations with magnitude.
    optical_obs = [
        o
        for o in observations
        if o.get("regime") == "optical"
        and o.get("magnitude") is not None
        and not o.get("is_upper_limit", False)
    ]

    if not optical_obs:
        _logger.info(
            "No optical detections for sparkline",
            extra={"nova_id": nova_id, "phase": "generate_sparkline"},
        )
        nova_context["has_sparkline"] = False
        return None

    # Step 2 — Band selection (§9.3).
    selected_band = _select_band(optical_obs)
    if selected_band is None:
        _logger.info(
            "No band meets minimum point threshold for sparkline",
            extra={"nova_id": nova_id, "phase": "generate_sparkline"},
        )
        nova_context["has_sparkline"] = False
        return None

    # Step 3 — Extract (time, magnitude) pairs for the selected band.
    band_obs = [o for o in optical_obs if o.get("band") == selected_band]
    points: list[tuple[float, float]] = sorted(
        (float(o["epoch_mjd"]), float(o["magnitude"])) for o in band_obs
    )

    # Step 4 — LTTB downsampling to 90 points (§9.4).
    points = lttb(points, _LTTB_THRESHOLD)

    # Step 5 — Coordinate transform and SVG rendering.
    svg = _render_svg(points)

    nova_context["has_sparkline"] = True

    _logger.info(
        "Generated sparkline.svg",
        extra={
            "nova_id": nova_id,
            "band": selected_band,
            "points": len(points),
            "phase": "generate_sparkline",
        },
    )

    return svg


# ---------------------------------------------------------------------------
# Band selection (§9.3)
# ---------------------------------------------------------------------------


def _select_band(optical_obs: list[dict[str, Any]]) -> str | None:
    """Select the single best band for the sparkline.

    Algorithm (§9.3):
      1. Group by band display label.
      2. If V-band has ≥ ``_SPARKLINE_BAND_MIN_POINTS``, select V.
      3. Else select the band with the most observations.
      4. If no band has ≥ ``_SPARKLINE_BAND_MIN_POINTS``, relax to
         ≥ ``_SPARKLINE_BAND_ABS_MIN`` and select the best-sampled.
      5. If nothing qualifies, return ``None``.
    """
    # Count observations per band.
    band_counts: dict[str, int] = {}
    for o in optical_obs:
        label: str = o.get("band", "")
        band_counts[label] = band_counts.get(label, 0) + 1

    # Prefer V-band if it meets the threshold.
    v_count = band_counts.get(_PREFERRED_BAND, 0)
    if v_count >= _SPARKLINE_BAND_MIN_POINTS:
        return _PREFERRED_BAND

    # Select the best-sampled band meeting the preferred threshold.
    above_threshold = {b: c for b, c in band_counts.items() if c >= _SPARKLINE_BAND_MIN_POINTS}
    if above_threshold:
        return max(above_threshold, key=lambda b: above_threshold[b])

    # Relax to absolute minimum.
    above_abs_min = {b: c for b, c in band_counts.items() if c >= _SPARKLINE_BAND_ABS_MIN}
    if above_abs_min:
        return max(above_abs_min, key=lambda b: above_abs_min[b])

    return None


# ---------------------------------------------------------------------------
# Coordinate transform and SVG rendering (§9.5, §9.6)
# ---------------------------------------------------------------------------


def _render_svg(points: list[tuple[float, float]]) -> str:
    """Transform data coordinates to SVG and build the SVG string.

    Handles edge cases:
      - Single point → small circle (dot).
      - Identical magnitudes → horizontal line centered vertically.
      - Identical timestamps → dot at vertical center.
    """
    n = len(points)

    if n == 1:
        return _render_dot()

    t_values = [p[0] for p in points]
    m_values = [p[1] for p in points]

    t_min, t_max = min(t_values), max(t_values)
    m_min, m_max = min(m_values), max(m_values)

    # Degenerate x-range (all same time) → render as dot.
    if t_max - t_min < 1e-10:
        return _render_dot()

    # Degenerate y-range (identical magnitudes) → horizontal line.
    if m_max - m_min < 1e-10:
        y_center = (_DRAW_Y_MIN + _DRAW_Y_MAX) / 2.0
        px_points = [(_scale_x(t, t_min, t_max), y_center) for t in t_values]
        return _build_svg(px_points)

    # Normal case: full coordinate transform.
    px_points = [
        (
            _scale_x(t, t_min, t_max),
            _scale_y(m, m_min, m_max),
        )
        for t, m in points
    ]

    return _build_svg(px_points)


def _scale_x(t: float, t_min: float, t_max: float) -> float:
    """Map time to pixel x-coordinate (linear)."""
    frac = (t - t_min) / (t_max - t_min)
    return _DRAW_X_MIN + frac * (_DRAW_X_MAX - _DRAW_X_MIN)


def _scale_y(mag: float, mag_min: float, mag_max: float) -> float:
    """Map magnitude to pixel y-coordinate (inverted: brighter = top)."""
    frac = (mag - mag_min) / (mag_max - mag_min)
    # mag_min (brightest) → _DRAW_Y_MIN (top)
    # mag_max (faintest) → _DRAW_Y_MAX (bottom)
    return _DRAW_Y_MIN + frac * (_DRAW_Y_MAX - _DRAW_Y_MIN)


def _render_dot() -> str:
    """Render a single dot at the viewport center."""
    cx = (_DRAW_X_MIN + _DRAW_X_MAX) / 2.0
    cy = (_DRAW_Y_MIN + _DRAW_Y_MAX) / 2.0
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg"'
        f' viewBox="0 0 {_SVG_WIDTH} {_SVG_HEIGHT}"'
        f' width="{_SVG_WIDTH}" height="{_SVG_HEIGHT}">'
        f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="2"'
        f' fill="{_STROKE_COLOR}" />'
        f"</svg>"
    )


def _build_svg(px_points: list[tuple[float, float]]) -> str:
    """Construct the SVG string from pixel-space points.

    Produces a filled polygon (area under the curve) and a polyline
    (the light curve trace).
    """
    # Format points as "x,y" strings.
    point_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in px_points)

    # Fill polygon: curve points + bottom-right + bottom-left to close.
    # This creates the filled area under the light curve.
    first_x = px_points[0][0]
    last_x = px_points[-1][0]
    polygon_str = f"{point_str} {last_x:.1f},{_DRAW_Y_MAX:.1f} {first_x:.1f},{_DRAW_Y_MAX:.1f}"

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg"'
        f' viewBox="0 0 {_SVG_WIDTH} {_SVG_HEIGHT}"'
        f' width="{_SVG_WIDTH}" height="{_SVG_HEIGHT}">'
        f'<polygon points="{polygon_str}"'
        f' fill="{_STROKE_COLOR}" fill-opacity="{_FILL_OPACITY}" />'
        f'<polyline points="{point_str}" fill="none"'
        f' stroke="{_STROKE_COLOR}" stroke-width="{_STROKE_WIDTH}"'
        f' stroke-linejoin="round" stroke-linecap="round" />'
        f"</svg>"
    )
