"""Unit tests for ADR-034 spectra wavelength regime classification."""

from __future__ import annotations

from generators.spectra import (
    _SPECTRA_REGIME_DEFINITIONS,
    _assign_spectra_regime,
)

# ---------------------------------------------------------------------------
# _assign_spectra_regime boundary tests
# ---------------------------------------------------------------------------


def test_assign_spectra_regime_uv() -> None:
    # midpoint = (115 + 170) / 2 = 142.5 → uv regime
    assert _assign_spectra_regime(115.0, 170.0) == "uv"


def test_assign_spectra_regime_optical() -> None:
    # midpoint = (400 + 700) / 2 = 550 → [320, 1000) → optical
    assert _assign_spectra_regime(400.0, 700.0) == "optical"


def test_assign_spectra_regime_nir() -> None:
    # midpoint = (1200 + 2500) / 2 = 1850 → [1000, 5000) → nir
    assert _assign_spectra_regime(1200.0, 2500.0) == "nir"


def test_assign_spectra_regime_mir() -> None:
    # midpoint = (6000 + 10000) / 2 = 8000 → ≥ 5000 → mir
    assert _assign_spectra_regime(6000.0, 10000.0) == "mir"


def test_assign_spectra_regime_boundary_320() -> None:
    # Exact midpoint = 320 → 320 is NOT < 320 → falls to optical
    assert _assign_spectra_regime(220.0, 420.0) == "optical"


def test_assign_spectra_regime_stis_g430l() -> None:
    # STIS G430L: 290–570 nm, midpoint = 430 → optical
    # Crosses UV boundary but is overwhelmingly optical.
    assert _assign_spectra_regime(290.0, 570.0) == "optical"


# ---------------------------------------------------------------------------
# Regime records in artifact output
# ---------------------------------------------------------------------------


def test_regime_records_single_regime() -> None:
    """When all spectra are optical, regimes list has exactly one entry."""
    spectra = [
        {"regime": "optical", "epoch_mjd": 59000.0},
        {"regime": "optical", "epoch_mjd": 59001.0},
    ]

    present: dict[str, dict] = {}
    for sp in spectra:
        rid: str = str(sp["regime"])
        if rid not in present:
            present[rid] = dict(_SPECTRA_REGIME_DEFINITIONS[rid])

    regime_records = sorted(present.values(), key=lambda r: r["id"])

    assert len(regime_records) == 1
    assert regime_records[0]["id"] == "optical"
    assert regime_records[0]["label"] == "Optical"


def test_regime_records_multiple_regimes() -> None:
    """Mixed uv + optical produces two sorted entries."""
    from generators.spectra import _SPECTRA_REGIME_SORT_ORDER

    spectra = [
        {"regime": "optical", "epoch_mjd": 59000.0},
        {"regime": "uv", "epoch_mjd": 59001.0},
    ]

    present: dict[str, dict] = {}
    for sp in spectra:
        rid: str = str(sp["regime"])
        if rid not in present:
            present[rid] = dict(_SPECTRA_REGIME_DEFINITIONS[rid])

    regime_records = sorted(
        present.values(),
        key=lambda r: _SPECTRA_REGIME_SORT_ORDER.get(r["id"], 99),
    )

    assert len(regime_records) == 2
    assert regime_records[0]["id"] == "uv"
    assert regime_records[1]["id"] == "optical"


def test_spectra_sorted_by_regime_then_epoch() -> None:
    """Spectra sort by regime order first, then epoch_mjd within each regime."""
    from generators.spectra import _SPECTRA_REGIME_SORT_ORDER

    spectra = [
        {"regime": "optical", "epoch_mjd": 59002.0},
        {"regime": "uv", "epoch_mjd": 59001.0},
        {"regime": "optical", "epoch_mjd": 59000.0},
        {"regime": "uv", "epoch_mjd": 59003.0},
    ]

    spectra.sort(
        key=lambda s: (
            _SPECTRA_REGIME_SORT_ORDER.get(str(s["regime"]), 99),
            s["epoch_mjd"],
        )
    )

    assert spectra[0] == {"regime": "uv", "epoch_mjd": 59001.0}
    assert spectra[1] == {"regime": "uv", "epoch_mjd": 59003.0}
    assert spectra[2] == {"regime": "optical", "epoch_mjd": 59000.0}
    assert spectra[3] == {"regime": "optical", "epoch_mjd": 59002.0}
