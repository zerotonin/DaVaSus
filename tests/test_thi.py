"""Tests for :mod:`davasus.thi`."""

from __future__ import annotations

import numpy as np

from davasus.thi import compute_thi_mader, compute_thi_nrc


def test_thi_nrc_worked_example():
    """Worked example: at 25 °C and 50 % RH the NRC THI is 71.995.

    Hand-derived: (1.8·25 + 32) − (0.55 − 0.0055·50) · (1.8·25 − 26.8)
                = 77 − 0.275 · 18.2 = 71.995.
    """
    val = compute_thi_nrc(25.0, 50.0)
    assert abs(float(val) - 71.995) < 1e-3


def test_thi_nrc_vectorised_matches_scalar():
    """Vector input yields element-wise scalar results."""
    temps = np.array([20.0, 25.0, 30.0])
    rhs = np.array([60.0, 50.0, 40.0])
    out = compute_thi_nrc(temps, rhs)
    for i in range(len(temps)):
        assert abs(out[i] - compute_thi_nrc(float(temps[i]), float(rhs[i]))) < 1e-9


def test_thi_mader_reduces_to_nrc_plus_offset_when_no_wind_or_solar():
    """With u=0 and Rsw=0 the Mader form is NRC + 4.51."""
    base = compute_thi_nrc(25.0, 50.0)
    adj = compute_thi_mader(25.0, 50.0, 0.0, 0.0)
    assert abs(float(adj) - (float(base) + 4.51)) < 1e-9


def test_thi_mader_wind_cools_and_solar_warms():
    """Wind reduces THI; solar increases it."""
    no_wind_no_sun = compute_thi_mader(25.0, 50.0, 0.0, 0.0)
    windy = compute_thi_mader(25.0, 50.0, 5.0, 0.0)
    sunny = compute_thi_mader(25.0, 50.0, 0.0, 800.0)
    assert windy < no_wind_no_sun
    assert sunny > no_wind_no_sun
