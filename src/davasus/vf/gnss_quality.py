# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.gnss_quality                                       ║
# ║  « is it the cow or the ionosphere »                             ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Handover 4.9: GNSS-error proxies per 30 min (lost-collar        ║
# ║  jitter, lying-animal noise floor, impossible positions, fix     ║
# ║  gaps, common-mode fence approaches) joined to GFZ Hp30 / Kp     ║
# ║  and DLR ROTI, so behavioural scores can be rerun storm-free.    ║
# ╚══════════════════════════════════════════════════════════════════╝
"""GNSS quality proxies and space-weather covariates (handover 4.9)."""

from __future__ import annotations

import json
import sqlite3
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

PROXY_INTERVAL_MIN: int = 30
MAX_PLAUSIBLE_SPEED_M_PER_MIN: float = 100.0
# Hp30 >= 6 (NOAA G2 and above): flagged as reduced GPS sensitivity (D20).
HP30_STORM_THRESHOLD: float = 6.0
HP30_INTERVAL: pd.Timedelta = pd.Timedelta(minutes=30)
REDUCED_GPS_LABEL: str = "reduced GPS sensitivity"
PROXY_STORM_PERCENTILE: float = 95.0

# NOAA-scale storms in the study years; verify every window against Hp30.
CANDIDATE_STORMS_UTC: tuple[tuple[str, str], ...] = (
    ("2024-05-10 18:00", "2024-05-12 00:00"),
    ("2024-06-27 00:00", "2024-06-29 00:00"),
    ("2024-08-11 00:00", "2024-08-13 00:00"),
    ("2024-09-16 00:00", "2024-09-18 00:00"),
    ("2024-10-10 00:00", "2024-10-11 00:00"),
    ("2025-06-01 00:00", "2025-06-02 00:00"),
)

# ┌────────────────────────────────────────────────────────────┐
# │ Error proxies  « per 30 min, from the collars alone »      │
# └────────────────────────────────────────────────────────────┘


class GnssErrorProxyBuilder:
    """Per-interval GNSS-error proxies from the collar fixes."""

    def build(self, fixes: pd.DataFrame) -> pd.DataFrame:
        """One row per interval: jitter, noise floor, gap and jump rates."""
        raise NotImplementedError

    def flag_degraded_intervals(self, proxies: pd.DataFrame) -> pd.Series:
        """True where any proxy exceeds its percentile threshold."""
        raise NotImplementedError


# ┌────────────────────────────────────────────────────────────┐
# │ Space weather  « GFZ Hp30 / Kp, DLR ROTI, cached »         │
# └────────────────────────────────────────────────────────────┘


GFZ_API_URL: str = "https://kp.gfz.de/app/json/?start={start}&end={end}&index={index}"
GFZ_USER_AGENT: str = "davasus-vf/1.0"
GFZ_TIMEOUT_S: float = 60.0
GFZ_MISSING: float = -1.0
GFZ_INDICES: tuple[str, ...] = ("Hp30", "ap30", "Kp")


class SpaceWeatherFetcher:
    """Download and cache GFZ Potsdam geomagnetic indices (CC BY 4.0).

    One JSON file per index and window under ``cache_dir``; a cached file
    is never downloaded again.
    """

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir

    def fetch_hp30(self, start_utc: str, end_utc: str) -> pd.DataFrame:
        """Hp30, ap30 and Kp on a 30-min grid (Kp held for its 3 h)."""
        frames = [parse_gfz_json(self._fetch(index, start_utc, end_utc), index)
                  for index in GFZ_INDICES]
        return build_space_weather_table(*frames)

    def _fetch(self, index: str, start_utc: str, end_utc: str) -> dict:
        start, end = _gfz_time(start_utc), _gfz_time(end_utc)
        path = self.cache_dir / f"gfz_{index}_{start[:10]}_{end[:10]}.json"
        if not path.is_file():
            url = GFZ_API_URL.format(start=start, end=end, index=index)
            request = urllib.request.Request(url, headers={"User-Agent": GFZ_USER_AGENT})
            with urllib.request.urlopen(request, timeout=GFZ_TIMEOUT_S) as response:
                payload = response.read().decode("utf-8")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(payload, encoding="utf-8")
        return json.loads(path.read_text(encoding="utf-8"))


def parse_gfz_json(payload: dict, index: str) -> pd.DataFrame:
    """GFZ API response to (timestamp, <index lower case>); -1 becomes NaN."""
    values = pd.to_numeric(pd.Series(payload[index], dtype=float))
    return pd.DataFrame({
        "timestamp": pd.to_datetime(payload["datetime"], utc=True),
        index.lower(): values.where(values != GFZ_MISSING),
    })


def build_space_weather_table(
    hp30: pd.DataFrame, ap30: pd.DataFrame, kp: pd.DataFrame
) -> pd.DataFrame:
    """Join the indices on the Hp30 grid; Kp is forward-filled within 3 h."""
    table = hp30.merge(ap30, on="timestamp", how="outer").sort_values("timestamp")
    table = pd.merge_asof(table, kp.sort_values("timestamp"), on="timestamp",
                          tolerance=pd.Timedelta(hours=3), direction="backward")
    table["roti_farm"] = np.nan
    return table[["timestamp", "hp30", "ap30", "kp", "roti_farm"]].reset_index(drop=True)


def write_space_weather(connection: sqlite3.Connection, table: pd.DataFrame) -> int:
    """Upsert rows into ``space_weather``; returns the number written."""
    rows = [
        (ts.strftime("%Y-%m-%dT%H:%M:%S+00:00"), *(None if pd.isna(v) else float(v) for v in vals))
        for ts, *vals in table[["timestamp", "hp30", "ap30", "kp", "roti_farm"]].itertuples(
            index=False
        )
    ]
    connection.executemany(
        "INSERT OR REPLACE INTO space_weather (timestamp, hp30, ap30, kp, roti_farm)"
        " VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    connection.commit()
    return len(rows)


def _gfz_time(value: str) -> str:
    return pd.Timestamp(value, tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


# ┌────────────────────────────────────────────────────────────┐
# │ Reduced GPS sensitivity  « Hp30 >= 6, flag then omit »     │
# └────────────────────────────────────────────────────────────┘


def flag_reduced_gps(space_weather: pd.DataFrame) -> pd.Series:
    """True for Hp30 intervals at or above the storm threshold; NaN is False."""
    return space_weather["hp30"].ge(HP30_STORM_THRESHOLD).fillna(False).astype(bool)


def build_reduced_gps_intervals(space_weather: pd.DataFrame) -> pd.DataFrame:
    """Merge consecutive flagged half-hours into (start_utc, end_utc, max_hp30) rows."""
    flagged = space_weather[flag_reduced_gps(space_weather)].sort_values("timestamp")
    columns = ["start_utc", "end_utc", "max_hp30"]
    if flagged.empty:
        return pd.DataFrame(columns=columns)
    starts = flagged["timestamp"]
    new_run = starts.diff().ne(HP30_INTERVAL)
    run = new_run.cumsum()
    grouped = flagged.groupby(run.to_numpy())
    return pd.DataFrame({
        "start_utc": grouped["timestamp"].min().to_numpy(),
        "end_utc": (grouped["timestamp"].max() + HP30_INTERVAL).to_numpy(),
        "max_hp30": grouped["hp30"].max().to_numpy(),
    })[columns]


def annotate_reduced_gps(fixes: pd.DataFrame, space_weather: pd.DataFrame) -> pd.DataFrame:
    """Copy of ``fixes`` with ``is_gps_reduced`` from the Hp30 interval each fix falls in.

    Fixes outside any fetched interval are False; analyses decide whether
    to drop the flagged rows (run everything, decide later).
    """
    table = space_weather.assign(is_gps_reduced=flag_reduced_gps(space_weather))
    table = table[["timestamp", "is_gps_reduced"]].sort_values("timestamp")
    order = fixes["timestamp"].argsort(kind="stable")
    merged = pd.merge_asof(
        fixes.iloc[order][["timestamp"]].reset_index(), table, on="timestamp",
        direction="backward", tolerance=HP30_INTERVAL - pd.Timedelta(microseconds=1),
    ).set_index("index")
    flag = merged["is_gps_reduced"].astype("boolean").fillna(False).astype(bool)
    return fixes.assign(is_gps_reduced=flag.reindex(fixes.index).to_numpy())
