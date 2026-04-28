"""Tests for :func:`davasus.sun.extract_sun_events`."""

from __future__ import annotations

import numpy as np
import pandas as pd

from davasus.sun import extract_sun_events


def _synth_day(
    date: str,
    sunrise_h: float,
    sunset_h: float,
    samples: int = 96,
) -> pd.DataFrame:
    """Synthesise (timestamp, pot_slr_rad_avg) for one day with known crossings."""
    base = pd.Timestamp(date, tz="UTC")
    seconds = np.linspace(0, 24 * 3600, samples, endpoint=False)
    hours = seconds / 3600.0
    # Triangular-shaped potential radiation, zero at sunrise / sunset.
    rad = np.where(
        (hours >= sunrise_h) & (hours <= sunset_h),
        np.maximum(
            0.0,
            np.minimum(hours - sunrise_h, sunset_h - hours)
            * 100.0,
        ),
        -20.0,
    )
    return pd.DataFrame(
        {
            "timestamp": [base + pd.Timedelta(seconds=s) for s in seconds],
            "pot_slr_rad_avg": rad,
        }
    )


def test_recovers_known_sunrise_and_sunset():
    """Synthetic day with sunrise=06.0, sunset=18.0 recovers within 0.5 h."""
    df = _synth_day("2024-06-21", sunrise_h=6.0, sunset_h=18.0)
    out = extract_sun_events(df)
    row = out.iloc[0]
    assert abs(row["sunrise_h"] - 6.0) < 0.5
    assert abs(row["sunset_h"] - 18.0) < 0.5
    assert abs(row["solar_noon_h"] - 12.0) < 0.5
    assert abs(row["photoperiod_h"] - 12.0) < 1.0


def test_empty_input_returns_empty_frame():
    """Empty input yields an empty (but well-typed) frame."""
    out = extract_sun_events(pd.DataFrame(columns=["timestamp", "pot_slr_rad_avg"]))
    assert list(out.columns) == [
        "date", "sunrise_h", "sunset_h", "solar_noon_h", "photoperiod_h",
    ]
    assert out.empty


def test_polar_winter_has_nan_crossings():
    """All-negative radiation = no crossings, NaN result."""
    base = pd.Timestamp("2024-12-21", tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp": [base + pd.Timedelta(hours=h) for h in range(24)],
            "pot_slr_rad_avg": [-10.0] * 24,
        }
    )
    out = extract_sun_events(df)
    row = out.iloc[0]
    assert np.isnan(row["sunrise_h"])
    assert np.isnan(row["sunset_h"])
