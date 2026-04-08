"""Unit tests for services/artifact_generator/generators/shared.py.

No AWS dependencies — all functions under test are pure computation.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest
from astropy.time import Time
from generators.shared import (
    format_coordinates,
    generated_at_timestamp,
    lttb,
    resolve_outburst_mjd,
)

# ======================================================================
# resolve_outburst_mjd
# ======================================================================


class TestResolveOutburstMjd:
    """§7.6 — Outburst MJD resolution."""

    # ------------------------------------------------------------------
    # Primary path: discovery date present, non-recurrent
    # ------------------------------------------------------------------

    def test_normal_discovery_date(self) -> None:
        """Precise YYYY-MM-DD discovery date converts to correct MJD."""
        # 2000-01-01T00:00:00 UTC → MJD 51544.0
        mjd, estimated = resolve_outburst_mjd("2000-01-01", None, [])
        assert mjd == pytest.approx(51544.0, abs=0.01)
        assert estimated is False

    def test_imprecise_day_defaults_to_first(self) -> None:
        """Day component 00 defaults to the 1st of the month."""
        # 2000-01-00 → treated as 2000-01-01
        mjd, estimated = resolve_outburst_mjd("2000-01-00", None, [])
        assert mjd == pytest.approx(51544.0, abs=0.01)
        assert estimated is False

    def test_imprecise_month_and_day_defaults_to_jan_first(self) -> None:
        """Month 00 and day 00 both default to January 1st."""
        # 2000-00-00 → treated as 2000-01-01
        mjd, estimated = resolve_outburst_mjd("2000-00-00", None, [])
        assert mjd == pytest.approx(51544.0, abs=0.01)
        assert estimated is False

    def test_discovery_date_with_nova_type_classical(self) -> None:
        """Non-recurrent nova_type uses discovery date (primary path)."""
        mjd, estimated = resolve_outburst_mjd("2000-01-01", "classical", [])
        assert mjd == pytest.approx(51544.0, abs=0.01)
        assert estimated is False

    def test_discovery_date_is_not_estimated(self) -> None:
        """Primary path always sets is_estimated to False."""
        _, estimated = resolve_outburst_mjd("2020-06-15", None, [50000.0])
        assert estimated is False

    # ------------------------------------------------------------------
    # Recurrent nova: always uses fallback
    # ------------------------------------------------------------------

    def test_recurrent_nova_with_discovery_date_uses_fallback(self) -> None:
        """Recurrent novae ignore discovery_date and use the fallback."""
        epochs = [51544.0, 51545.0, 51546.0]
        mjd, estimated = resolve_outburst_mjd("2000-01-01", "recurrent", epochs)
        # Fallback: min(epochs) - 1.0
        assert mjd == pytest.approx(51543.0, abs=0.01)
        assert estimated is True

    def test_recurrent_nova_without_discovery_date(self) -> None:
        """Recurrent nova with no discovery date still uses fallback."""
        epochs = [51544.0]
        mjd, estimated = resolve_outburst_mjd(None, "recurrent", epochs)
        assert mjd == pytest.approx(51543.0, abs=0.01)
        assert estimated is True

    def test_recurrent_nova_no_observations(self) -> None:
        """Recurrent nova with no observations and no date → None."""
        mjd, estimated = resolve_outburst_mjd(None, "recurrent", [])
        assert mjd is None
        assert estimated is False

    # ------------------------------------------------------------------
    # Fallback path: no discovery date
    # ------------------------------------------------------------------

    def test_fallback_earliest_observation_minus_one(self) -> None:
        """When discovery_date is None, outburst = min(epochs) - 1."""
        epochs = [51600.0, 51544.0, 51700.0]
        mjd, estimated = resolve_outburst_mjd(None, None, epochs)
        assert mjd == pytest.approx(51543.0, abs=0.01)
        assert estimated is True

    def test_fallback_single_observation(self) -> None:
        """Fallback with a single observation."""
        mjd, estimated = resolve_outburst_mjd(None, None, [60000.0])
        assert mjd == pytest.approx(59999.0, abs=0.01)
        assert estimated is True

    # ------------------------------------------------------------------
    # Edge case: no discovery date AND no observations
    # ------------------------------------------------------------------

    def test_no_date_no_observations_returns_none(self) -> None:
        """No discovery date and no observations → (None, False)."""
        mjd, estimated = resolve_outburst_mjd(None, None, [])
        assert mjd is None
        assert estimated is False

    def test_no_date_no_observations_unclassified(self) -> None:
        """Same edge case with explicit None nova_type."""
        mjd, estimated = resolve_outburst_mjd(None, None, [])
        assert mjd is None
        assert estimated is False

    # ------------------------------------------------------------------
    # MJD value verification against known astronomical dates
    # ------------------------------------------------------------------

    def test_gk_per_discovery_date(self) -> None:
        """GK Per discovery date (1901-02-21) produces correct MJD."""
        expected_mjd = float(
            Time("1901-02-21T00:00:00", format="isot", scale="utc").mjd,
        )
        mjd, estimated = resolve_outburst_mjd("1901-02-21", "classical", [])
        assert mjd == pytest.approx(expected_mjd, abs=0.001)
        assert estimated is False

    # ------------------------------------------------------------------
    # outburst_date priority
    # ------------------------------------------------------------------

    def test_outburst_date_used_when_present(self) -> None:
        """outburst_date present → used, is_estimated = False."""
        expected_mjd = float(
            Time("2021-08-08T00:00:00", format="isot", scale="utc").mjd,
        )
        mjd, estimated = resolve_outburst_mjd(
            None,
            None,
            [],
            outburst_date="2021-08-08",
        )
        assert mjd == pytest.approx(expected_mjd, abs=0.001)
        assert estimated is False

    def test_outburst_date_wins_over_discovery_date(self) -> None:
        """outburst_date and discovery_date both present → outburst_date wins."""
        outburst_mjd_expected = float(
            Time("2021-08-08T00:00:00", format="isot", scale="utc").mjd,
        )
        discovery_mjd = float(
            Time("2021-09-01T00:00:00", format="isot", scale="utc").mjd,
        )
        mjd, estimated = resolve_outburst_mjd(
            "2021-09-00",
            None,
            [],
            outburst_date="2021-08-08",
        )
        assert mjd == pytest.approx(outburst_mjd_expected, abs=0.001)
        assert mjd != pytest.approx(discovery_mjd, abs=0.5)
        assert estimated is False

    def test_outburst_date_ignored_for_recurrent_nova(self) -> None:
        """outburst_date present + recurrent nova → fallback used."""
        epochs = [51544.0, 51545.0]
        mjd, estimated = resolve_outburst_mjd(
            "2000-01-01",
            "recurrent",
            epochs,
            outburst_date="2000-01-05",
        )
        # Should use earliest-observation fallback, not outburst_date
        assert mjd == pytest.approx(51543.0, abs=0.01)
        assert estimated is True

    def test_outburst_date_none_falls_through_to_discovery(self) -> None:
        """outburst_date = None → falls through to discovery_date."""
        expected_mjd = float(
            Time("2000-01-01T00:00:00", format="isot", scale="utc").mjd,
        )
        mjd, estimated = resolve_outburst_mjd(
            "2000-01-01",
            None,
            [],
            outburst_date=None,
        )
        assert mjd == pytest.approx(expected_mjd, abs=0.001)
        assert estimated is False


# ======================================================================
# format_coordinates
# ======================================================================


class TestFormatCoordinates:
    """§5.3 — Coordinate formatting."""

    def test_ra_format_pattern(self) -> None:
        """RA string matches HH:MM:SS.ss pattern."""
        ra, _ = format_coordinates(52.799083, 43.904667)
        assert re.match(r"^\d{2}:\d{2}:\d{2}\.\d{2}$", ra), f"RA format mismatch: {ra}"

    def test_dec_format_pattern(self) -> None:
        """Dec string matches ±DD:MM:SS.s pattern."""
        _, dec = format_coordinates(52.799083, 43.904667)
        assert re.match(r"^[+-]\d{2}:\d{2}:\d{2}\.\d$", dec), f"Dec format mismatch: {dec}"

    def test_zero_ra(self) -> None:
        """RA = 0° → 00:00:00.00."""
        ra, _ = format_coordinates(0.0, 0.0)
        assert ra == "00:00:00.00"

    def test_ra_180_degrees(self) -> None:
        """RA = 180° → 12:00:00.00."""
        ra, _ = format_coordinates(180.0, 0.0)
        assert ra == "12:00:00.00"

    def test_zero_dec(self) -> None:
        """Dec = 0° → +00:00:00.0 (always has sign)."""
        _, dec = format_coordinates(0.0, 0.0)
        assert dec == "+00:00:00.0"

    def test_positive_dec(self) -> None:
        """Positive declination has '+' prefix."""
        _, dec = format_coordinates(0.0, 45.0)
        assert dec.startswith("+")
        assert dec == "+45:00:00.0"

    def test_negative_dec(self) -> None:
        """Negative declination has '-' prefix."""
        _, dec = format_coordinates(0.0, -45.0)
        assert dec.startswith("-")
        assert dec == "-45:00:00.0"

    def test_gk_per_coordinates(self) -> None:
        """GK Per (RA ~52.8°, Dec ~43.9°) produces expected sexagesimal."""
        ra, dec = format_coordinates(52.799083, 43.904667)
        # RA: 52.799083° / 15 = 3.51994h ≈ 03:31:11.78
        assert ra.startswith("03:31:")
        # Dec: 43.904667° ≈ +43:54:16.8
        assert dec.startswith("+43:54:")


# ======================================================================
# generated_at_timestamp
# ======================================================================


class TestGeneratedAtTimestamp:
    """Timestamp helper for artifact generated_at fields."""

    def test_ends_with_z(self) -> None:
        """Timestamp ends with 'Z' (UTC indicator)."""
        ts = generated_at_timestamp()
        assert ts.endswith("Z")

    def test_iso_8601_format(self) -> None:
        """Timestamp matches YYYY-MM-DDTHH:MM:SSZ format."""
        ts = generated_at_timestamp()
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", ts), (
            f"Timestamp format mismatch: {ts}"
        )

    def test_parseable_as_datetime(self) -> None:
        """Timestamp can be parsed back to a datetime object."""
        ts = generated_at_timestamp()
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        dt = dt.replace(tzinfo=UTC)
        # Should be within the last few seconds.
        delta = datetime.now(UTC) - dt
        assert delta.total_seconds() < 5.0

    def test_no_fractional_seconds(self) -> None:
        """Timestamp has no fractional seconds (clean for JSON)."""
        ts = generated_at_timestamp()
        assert "." not in ts


# ======================================================================
# lttb
# ======================================================================


class TestLttb:
    """§9.4 — Largest-Triangle-Three-Buckets downsampling."""

    # ------------------------------------------------------------------
    # Passthrough cases
    # ------------------------------------------------------------------

    def test_empty_input(self) -> None:
        """Empty input returns empty list."""
        assert lttb([], 10) == []

    def test_input_shorter_than_threshold(self) -> None:
        """Input with fewer points than threshold is returned as-is."""
        pts: list[tuple[float, float]] = [(1.0, 2.0), (3.0, 4.0)]
        result = lttb(pts, 10)
        assert result == pts

    def test_input_equal_to_threshold(self) -> None:
        """Input with exactly threshold points is returned as-is."""
        pts: list[tuple[float, float]] = [(float(i), float(i)) for i in range(5)]
        result = lttb(pts, 5)
        assert result == pts

    def test_threshold_below_three_returns_copy(self) -> None:
        """Threshold < 3 cannot downsample; returns input unchanged."""
        pts: list[tuple[float, float]] = [(float(i), float(i)) for i in range(10)]
        result = lttb(pts, 2)
        assert result == pts

    def test_returns_new_list(self) -> None:
        """Passthrough returns a copy, not the original object."""
        pts: list[tuple[float, float]] = [(1.0, 2.0), (3.0, 4.0)]
        result = lttb(pts, 10)
        assert result == pts
        assert result is not pts

    # ------------------------------------------------------------------
    # Downsampling behaviour
    # ------------------------------------------------------------------

    def test_preserves_first_and_last(self) -> None:
        """First and last points are always retained."""
        pts: list[tuple[float, float]] = [(float(i), float(i)) for i in range(100)]
        result = lttb(pts, 10)
        assert result[0] == pts[0]
        assert result[-1] == pts[-1]

    def test_output_length_equals_threshold(self) -> None:
        """Output has exactly threshold points."""
        pts: list[tuple[float, float]] = [(float(i), float(i)) for i in range(100)]
        result = lttb(pts, 10)
        assert len(result) == 10

    def test_preserves_peak(self) -> None:
        """LTTB preserves a prominent peak in otherwise flat data.

        Construct a flat line at y=0 with a single spike at the
        midpoint.  The spike should survive downsampling because it
        forms the largest triangle in its bucket.
        """
        n = 100
        pts: list[tuple[float, float]] = [(float(i), 0.0) for i in range(n)]
        # Insert a spike at the midpoint.
        pts[50] = (50.0, 100.0)

        result = lttb(pts, 10)
        y_values = [p[1] for p in result]
        assert 100.0 in y_values, "Peak at y=100 was not preserved"

    def test_preserves_trough(self) -> None:
        """LTTB preserves a prominent trough."""
        n = 100
        pts: list[tuple[float, float]] = [(float(i), 50.0) for i in range(n)]
        pts[50] = (50.0, -50.0)

        result = lttb(pts, 10)
        y_values = [p[1] for p in result]
        assert -50.0 in y_values, "Trough at y=-50 was not preserved"

    def test_monotonic_input_preserves_trend(self) -> None:
        """Downsampled monotonic data still starts low and ends high."""
        pts: list[tuple[float, float]] = [(float(i), float(i)) for i in range(100)]
        result = lttb(pts, 10)
        # First should be smallest y, last should be largest.
        assert result[0][1] < result[-1][1]

    def test_threshold_three_returns_first_middle_last(self) -> None:
        """Threshold=3 returns first point, one middle point, and last."""
        pts: list[tuple[float, float]] = [(float(i), float(i * i)) for i in range(20)]
        result = lttb(pts, 3)
        assert len(result) == 3
        assert result[0] == pts[0]
        assert result[-1] == pts[-1]

    def test_all_selected_points_come_from_input(self) -> None:
        """Every point in the output exists in the original input."""
        pts: list[tuple[float, float]] = [(float(i), float(i) ** 0.5) for i in range(200)]
        result = lttb(pts, 20)
        pts_set = set(pts)
        for p in result:
            assert p in pts_set

    def test_output_preserves_time_order(self) -> None:
        """Output points are in ascending x (time) order."""
        pts: list[tuple[float, float]] = [(float(i), float(i) ** 0.5) for i in range(200)]
        result = lttb(pts, 20)
        x_values = [p[0] for p in result]
        assert x_values == sorted(x_values)
