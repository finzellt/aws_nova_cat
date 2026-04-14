"""Unit tests for nova_common.spectral.der_snr.

Covers:
  - Known Gaussian noise recovery (within tolerance)
  - Robustness to emission lines (median ignores sharp features)
  - Constant flux → 0.0 (no noise to measure)
  - Negative median flux → 0.0
  - All-zero flux → 0.0
  - Empty array → 0.0
  - Too few points (< 5) → 0.0
  - Exactly 5 points (minimum viable)
  - NaN/Inf handling (stripped before computation)
  - List input (not just ndarray)
  - High SNR recovery
  - Low SNR recovery
"""

from __future__ import annotations

import numpy as np
import pytest
from nova_common.spectral import der_snr

# ---------------------------------------------------------------------------
# Known-noise recovery
# ---------------------------------------------------------------------------


class TestGaussianNoiseRecovery:
    """Verify DER_SNR recovers the known SNR of synthetic Gaussian noise."""

    @pytest.fixture()
    def rng(self) -> np.random.Generator:
        return np.random.default_rng(seed=42)

    def test_snr_50_recovery(self, rng: np.random.Generator) -> None:
        """Flat continuum + Gaussian noise with true SNR ≈ 50."""
        signal = 100.0
        noise_sigma = signal / 50.0  # true SNR = 50
        flux = signal + rng.normal(0, noise_sigma, size=5000)

        estimated = der_snr(flux)
        assert estimated == pytest.approx(50.0, rel=0.15)

    def test_snr_10_recovery(self, rng: np.random.Generator) -> None:
        """Low SNR ≈ 10 — noisier but still recoverable."""
        signal = 100.0
        noise_sigma = signal / 10.0
        flux = signal + rng.normal(0, noise_sigma, size=5000)

        estimated = der_snr(flux)
        assert estimated == pytest.approx(10.0, rel=0.15)

    def test_snr_200_recovery(self, rng: np.random.Generator) -> None:
        """High SNR ≈ 200 — very clean spectrum."""
        signal = 1000.0
        noise_sigma = signal / 200.0
        flux = signal + rng.normal(0, noise_sigma, size=5000)

        estimated = der_snr(flux)
        assert estimated == pytest.approx(200.0, rel=0.15)

    def test_different_signal_levels(self, rng: np.random.Generator) -> None:
        """SNR estimate is independent of the absolute flux level."""
        for signal in [1e-13, 1.0, 1e6]:
            noise_sigma = signal / 30.0
            flux = signal + rng.normal(0, noise_sigma, size=5000)
            estimated = der_snr(flux)
            assert estimated == pytest.approx(30.0, rel=0.20), f"Failed at signal={signal:.0e}"


# ---------------------------------------------------------------------------
# Robustness to spectral features
# ---------------------------------------------------------------------------


class TestEmissionLineRobustness:
    """DER_SNR should be insensitive to sparse sharp features."""

    @pytest.fixture()
    def rng(self) -> np.random.Generator:
        return np.random.default_rng(seed=99)

    def test_emission_lines_dont_inflate_snr(self, rng: np.random.Generator) -> None:
        """A few strong emission lines should not change the SNR estimate."""
        signal = 100.0
        noise_sigma = signal / 50.0
        flux = signal + rng.normal(0, noise_sigma, size=5000)

        # Add 20 strong emission lines (5× continuum)
        line_positions = rng.choice(5000, size=20, replace=False)
        flux[line_positions] = signal * 5.0

        estimated = der_snr(flux)
        # Should still be close to 50, not inflated by lines.
        assert estimated == pytest.approx(50.0, rel=0.20)

    def test_absorption_lines_dont_deflate_snr(self, rng: np.random.Generator) -> None:
        """Absorption features should not bias the estimate downward."""
        signal = 100.0
        noise_sigma = signal / 50.0
        flux = signal + rng.normal(0, noise_sigma, size=5000)

        # Add 20 absorption lines (drop to 20% of continuum)
        line_positions = rng.choice(5000, size=20, replace=False)
        flux[line_positions] = signal * 0.2

        estimated = der_snr(flux)
        assert estimated == pytest.approx(50.0, rel=0.20)


# ---------------------------------------------------------------------------
# Sloped continuum
# ---------------------------------------------------------------------------


class TestSlopedContinuum:
    """DER_SNR should handle spectra with a sloped continuum."""

    def test_linear_slope(self) -> None:
        """Linear continuum slope is cancelled by the second difference."""
        rng = np.random.default_rng(seed=77)
        n = 5000
        wavelength = np.linspace(300, 900, n)
        # Continuum rises from 50 to 150 across the spectrum.
        continuum = 50 + 100 * (wavelength - 300) / 600
        noise_sigma = np.median(continuum) / 40.0  # true SNR ≈ 40
        flux = continuum + rng.normal(0, noise_sigma, size=n)

        estimated = der_snr(flux)
        assert estimated == pytest.approx(40.0, rel=0.20)


# ---------------------------------------------------------------------------
# Degenerate inputs
# ---------------------------------------------------------------------------


class TestDegenerateInputs:
    """Edge cases that should return 0.0 without raising."""

    def test_constant_flux(self) -> None:
        """Constant flux → noise = 0 → returns 0.0."""
        flux = np.full(1000, 42.0)
        assert der_snr(flux) == 0.0

    def test_all_zeros(self) -> None:
        """All-zero flux → 0.0."""
        assert der_snr(np.zeros(1000)) == 0.0

    def test_empty_array(self) -> None:
        """Empty array → 0.0."""
        assert der_snr(np.array([])) == 0.0

    def test_single_value(self) -> None:
        """Single element → 0.0 (fewer than 5 points)."""
        assert der_snr(np.array([100.0])) == 0.0

    def test_four_points(self) -> None:
        """Exactly 4 points → 0.0 (minimum is 5)."""
        assert der_snr(np.array([1.0, 2.0, 3.0, 4.0])) == 0.0

    def test_five_points_works(self) -> None:
        """Exactly 5 points is the minimum viable input."""
        rng = np.random.default_rng(seed=11)
        flux = 100.0 + rng.normal(0, 5, size=5)
        result = der_snr(flux)
        # Not testing accuracy with 5 points — just that it runs.
        assert result >= 0.0

    def test_negative_median_flux(self) -> None:
        """Negative median → 0.0 (no meaningful signal)."""
        flux = np.full(1000, -50.0) + np.random.default_rng(0).normal(0, 1, 1000)
        assert der_snr(flux) == 0.0

    def test_all_nan(self) -> None:
        """All NaN → 0.0 (nothing finite to work with)."""
        assert der_snr(np.full(100, np.nan)) == 0.0

    def test_all_inf(self) -> None:
        """All Inf → 0.0."""
        assert der_snr(np.full(100, np.inf)) == 0.0


# ---------------------------------------------------------------------------
# NaN / Inf handling
# ---------------------------------------------------------------------------


class TestNonFiniteHandling:
    """Non-finite values are stripped; computation proceeds on the rest."""

    def test_scattered_nans_stripped(self) -> None:
        """A few NaNs mixed in don't break the estimate."""
        rng = np.random.default_rng(seed=55)
        flux = 100.0 + rng.normal(0, 2.0, size=5000)
        # Sprinkle 50 NaNs.
        nan_idx = rng.choice(5000, size=50, replace=False)
        flux[nan_idx] = np.nan

        estimated = der_snr(flux)
        assert estimated == pytest.approx(50.0, rel=0.15)

    def test_infs_stripped(self) -> None:
        """Inf values are removed, estimate uses remaining data."""
        rng = np.random.default_rng(seed=66)
        flux = 100.0 + rng.normal(0, 2.0, size=5000)
        flux[0] = np.inf
        flux[1] = -np.inf

        estimated = der_snr(flux)
        assert estimated == pytest.approx(50.0, rel=0.15)


# ---------------------------------------------------------------------------
# Input type flexibility
# ---------------------------------------------------------------------------


class TestInputTypes:
    """der_snr accepts array-like inputs, not just ndarray."""

    def test_list_input(self) -> None:
        """Plain Python list is accepted."""
        rng = np.random.default_rng(seed=88)
        flux = (100.0 + rng.normal(0, 2.0, size=1000)).tolist()
        estimated = der_snr(flux)
        assert estimated > 0

    def test_integer_array(self) -> None:
        """Integer flux values are promoted to float64."""
        rng = np.random.default_rng(seed=44)
        flux = (1000 + rng.normal(0, 20, size=1000)).astype(np.int32)
        estimated = der_snr(flux)
        assert estimated == pytest.approx(50.0, rel=0.20)

    def test_float32_array(self) -> None:
        """float32 input is handled correctly."""
        rng = np.random.default_rng(seed=33)
        flux = (100.0 + rng.normal(0, 2.0, size=1000)).astype(np.float32)
        estimated = der_snr(flux)
        assert estimated == pytest.approx(50.0, rel=0.15)
