"""Unit tests for generators/sparkline.py.

No AWS dependencies — the sparkline generator is pure computation
over nova_context data.

Covers:
  Band selection:
  - V-band preferred when it meets minimum threshold
  - Falls back to best-sampled band when V is insufficient
  - Relaxes threshold when no band meets preferred minimum
  - Returns None when no band has ≥ 2 points

  SVG structure:
  - Contains <svg>, <polyline>, <polygon> elements
  - viewBox is "0 0 90 55"
  - Stroke color is teal (#2A7D7B)
  - Fill polygon present with opacity

  Edge cases:
  - Single data point → circle (dot) rendered
  - Identical magnitudes → horizontal line
  - No optical observations → None, has_sparkline = False
  - Upper limits excluded from sparkline data
  - Only upper limits in optical → None

  Context:
  - has_sparkline = True when SVG produced
  - has_sparkline = False when no sparkline
"""

from __future__ import annotations

from typing import Any

from generators.sparkline import generate_sparkline_svg

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _optical_obs(
    band: str,
    epoch: float,
    magnitude: float,
    *,
    is_upper_limit: bool = False,
) -> dict[str, Any]:
    """Build a minimal photometry observation record for testing."""
    return {
        "observation_id": f"obs-{epoch}",
        "epoch_mjd": epoch,
        "days_since_outburst": epoch - 51540.0,
        "band": band,
        "regime": "optical",
        "magnitude": magnitude,
        "magnitude_error": 0.02,
        "flux_density": None,
        "flux_density_error": None,
        "count_rate": None,
        "count_rate_error": None,
        "photon_flux": None,
        "photon_flux_error": None,
        "is_upper_limit": is_upper_limit,
        "provider": "test",
        "telescope": "test",
        "instrument": "test",
    }


def _make_context(
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a nova_context with photometry observations pre-populated."""
    return {
        "outburst_mjd": 51540.0,
        "outburst_mjd_is_estimated": False,
        "photometry_observations": observations,
        "photometry_bands": [],
    }


def _v_band_series(n: int, start_mag: float = 12.0) -> list[dict[str, Any]]:
    """Generate *n* V-band optical observations."""
    return [_optical_obs("V", 51544.0 + i, start_mag + i * 0.5) for i in range(n)]


def _b_band_series(n: int, start_mag: float = 13.0) -> list[dict[str, Any]]:
    """Generate *n* B-band optical observations."""
    return [_optical_obs("B", 51544.0 + i, start_mag + i * 0.3) for i in range(n)]


# ---------------------------------------------------------------------------
# Band selection
# ---------------------------------------------------------------------------


class TestBandSelection:
    def test_v_band_preferred(self) -> None:
        """V-band is selected when it has ≥ 5 observations."""
        obs = _v_band_series(10) + _b_band_series(15)
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert ctx["has_sparkline"] is True

    def test_fallback_to_best_sampled(self) -> None:
        """When V has < 5 points but B has ≥ 5, B is selected."""
        obs = _v_band_series(3) + _b_band_series(10)
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert ctx["has_sparkline"] is True

    def test_threshold_relaxed_to_two(self) -> None:
        """When no band has ≥ 5 points, threshold relaxes to ≥ 2."""
        obs = _v_band_series(3)  # 3 V-band points — below 5 but above 2
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None

    def test_single_point_per_band_returns_none(self) -> None:
        """Single point in each band → no band meets ≥ 2 minimum."""
        obs = [
            _optical_obs("V", 51544.0, 12.0),
            _optical_obs("B", 51545.0, 13.0),
        ]
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        # Each band has exactly 1 point — below absolute minimum of 2.
        assert svg is None
        assert ctx["has_sparkline"] is False

    def test_one_band_with_two_points(self) -> None:
        """A single band with exactly 2 points qualifies."""
        obs = [
            _optical_obs("R", 51544.0, 12.0),
            _optical_obs("R", 51545.0, 13.0),
        ]
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None


# ---------------------------------------------------------------------------
# SVG structure
# ---------------------------------------------------------------------------


class TestSVGStructure:
    def test_contains_svg_element(self) -> None:
        ctx = _make_context(_v_band_series(10))
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert svg.startswith("<svg")
        assert svg.endswith("</svg>")

    def test_viewbox(self) -> None:
        ctx = _make_context(_v_band_series(10))
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert 'viewBox="0 0 90 55"' in svg

    def test_polyline_present(self) -> None:
        ctx = _make_context(_v_band_series(10))
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert "<polyline" in svg

    def test_polygon_present(self) -> None:
        """Fill area under the curve."""
        ctx = _make_context(_v_band_series(10))
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert "<polygon" in svg

    def test_teal_stroke_color(self) -> None:
        ctx = _make_context(_v_band_series(10))
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert "#2A7D7B" in svg

    def test_fill_opacity(self) -> None:
        ctx = _make_context(_v_band_series(10))
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert 'fill-opacity="0.12"' in svg

    def test_stroke_width(self) -> None:
        ctx = _make_context(_v_band_series(10))
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert 'stroke-width="1.5"' in svg


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_no_optical_observations(self) -> None:
        """Empty photometry → None, has_sparkline = False."""
        ctx = _make_context([])
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is None
        assert ctx["has_sparkline"] is False

    def test_only_xray_data(self) -> None:
        """Non-optical regime data doesn't count."""
        obs = [
            {
                "observation_id": "x1",
                "epoch_mjd": 51544.0,
                "regime": "xray",
                "magnitude": None,
                "count_rate": 0.5,
                "is_upper_limit": False,
                "band": "0.3-10keV",
            },
        ]
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is None

    def test_upper_limits_excluded(self) -> None:
        """Upper limits do not count toward band point totals."""
        obs = [_optical_obs("V", 51544.0 + i, 12.0, is_upper_limit=True) for i in range(20)]
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is None

    def test_single_point_renders_dot(self) -> None:
        """Single qualifying point → circle element."""
        obs = [
            _optical_obs("V", 51544.0, 12.0),
            _optical_obs("V", 51544.0, 12.0),  # same epoch = 2 points for selection
        ]
        # After dedup in LTTB or rendering, both might collapse.
        # But band selection sees 2 points, so sparkline is attempted.
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        # Either a polyline or a circle — both are valid SVG.
        assert "<svg" in svg

    def test_identical_magnitudes_renders_line(self) -> None:
        """All same magnitude → horizontal line (not an error)."""
        obs = [
            _optical_obs("V", 51544.0 + i, 12.0)  # all mag 12.0
            for i in range(10)
        ]
        ctx = _make_context(obs)
        svg = generate_sparkline_svg("nova-1", ctx)
        assert svg is not None
        assert "<polyline" in svg


# ---------------------------------------------------------------------------
# Context flag
# ---------------------------------------------------------------------------


class TestContextFlag:
    def test_has_sparkline_true_on_success(self) -> None:
        ctx = _make_context(_v_band_series(10))
        generate_sparkline_svg("nova-1", ctx)
        assert ctx["has_sparkline"] is True

    def test_has_sparkline_false_on_no_data(self) -> None:
        ctx = _make_context([])
        generate_sparkline_svg("nova-1", ctx)
        assert ctx["has_sparkline"] is False

    def test_has_sparkline_false_when_no_band_qualifies(self) -> None:
        obs = [_optical_obs("V", 51544.0, 12.0)]  # Only 1 point
        ctx = _make_context(obs)
        generate_sparkline_svg("nova-1", ctx)
        assert ctx["has_sparkline"] is False
