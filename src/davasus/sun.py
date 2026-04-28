"""Sunrise / sunset extraction from the weather station's potential
solar-radiation channel.

The Campbell logger reports ``pot_slr_rad_avg``, an astronomically
computed potential shortwave radiation (W/m²) that depends only on
date, time, and latitude. It is **negative at night and positive during
the day**, and crosses zero exactly at the geometric sunrise and
sunset.

We treat each calendar day independently:

1. Group the (timestamp, ``pot_slr_rad_avg``) series by date.
2. Find the two sign changes within each day (negative → positive =
   sunrise; positive → negative = sunset).
3. Linearly interpolate between the bracketing samples to refine the
   crossing time to sub-sample resolution.

The returned frame has one row per date with sunrise / sunset / mid-day
expressed as fractional hours-of-day in the local-clock convention
implied by the timestamps in the database. (The DB stores UTC; convert
to local civil time downstream if you need a wall-clock plot.)
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def extract_sun_events(weather: pd.DataFrame) -> pd.DataFrame:
    """Derive sunrise / sunset times from a weather DataFrame.

    Args:
        weather: DataFrame with at least ``timestamp`` (datetime, UTC)
            and ``pot_slr_rad_avg`` columns. Other columns are ignored.

    Returns:
        DataFrame with one row per date and columns
        ``date`` (date), ``sunrise_h`` (float hours), ``sunset_h``,
        ``solar_noon_h``, ``photoperiod_h``. Days without a clean
        sign change (rare polar/twilight artefacts) get NaN.
    """
    if weather.empty:
        return _empty_sun_frame()

    df = weather[["timestamp", "pot_slr_rad_avg"]].dropna().copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["date"] = df["timestamp"].dt.date
    df["hour"] = (
        df["timestamp"].dt.hour
        + df["timestamp"].dt.minute / 60.0
        + df["timestamp"].dt.second / 3600.0
    )
    rows: list[dict] = []
    for date, group in df.groupby("date"):
        sunrise_h, sunset_h = _crossings(
            group["hour"].to_numpy(), group["pot_slr_rad_avg"].to_numpy()
        )
        rows.append(
            {
                "date": date,
                "sunrise_h": sunrise_h,
                "sunset_h": sunset_h,
                "solar_noon_h": _midpoint(sunrise_h, sunset_h),
                "photoperiod_h": _photoperiod(sunrise_h, sunset_h),
            }
        )
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def _crossings(hours: np.ndarray, rad: np.ndarray) -> tuple[float, float]:
    """Locate the upward (sunrise) and downward (sunset) zero crossings.

    Args:
        hours: Sample times within the day, in fractional hours.
        rad: Potential solar radiation values, aligned with ``hours``.

    Returns:
        ``(sunrise_h, sunset_h)``, NaN when a crossing is absent.
    """
    if hours.size < 2:
        return float("nan"), float("nan")
    order = np.argsort(hours)
    h = hours[order]
    r = rad[order]

    sunrise = float("nan")
    sunset = float("nan")
    for i in range(1, h.size):
        if r[i - 1] < 0.0 <= r[i] and np.isnan(sunrise):
            sunrise = _interpolate_zero(h[i - 1], r[i - 1], h[i], r[i])
        elif r[i - 1] >= 0.0 > r[i] and np.isnan(sunset):
            sunset = _interpolate_zero(h[i - 1], r[i - 1], h[i], r[i])
    return sunrise, sunset


def _interpolate_zero(h0: float, r0: float, h1: float, r1: float) -> float:
    """Return the hour where the line through ``(h0, r0)–(h1, r1)`` hits 0."""
    if r1 == r0:
        return float((h0 + h1) / 2.0)
    return float(h0 - r0 * (h1 - h0) / (r1 - r0))


def _midpoint(sunrise: float, sunset: float) -> float:
    """Midpoint between sunrise and sunset (NaN-safe)."""
    if not (np.isfinite(sunrise) and np.isfinite(sunset)):
        return float("nan")
    return float((sunrise + sunset) / 2.0)


def _photoperiod(sunrise: float, sunset: float) -> float:
    """Photoperiod length in hours (NaN-safe)."""
    if not (np.isfinite(sunrise) and np.isfinite(sunset)):
        return float("nan")
    return float(sunset - sunrise)


def _empty_sun_frame() -> pd.DataFrame:
    """Return an empty frame with the expected columns and dtypes."""
    return pd.DataFrame(
        columns=["date", "sunrise_h", "sunset_h", "solar_noon_h", "photoperiod_h"],
    )
