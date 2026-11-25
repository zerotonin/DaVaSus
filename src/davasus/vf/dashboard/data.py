# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.data                                     ║
# ║  « the whole trial as minute-by-animal arrays »                  ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Loads heifer_vf.db once into (minute, animal) cubes so any      ║
# ║  front end can scrub through 40 days without SQL per frame.      ║
# ║  Qt-free on purpose: the desktop GUI and a web app share it.     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Minute-indexed view of ``heifer_vf.db`` for interactive front ends."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import numpy as np
import pandas as pd

from davasus.thi import compute_thi_nrc
from davasus.vf.geometry import LocalProjection, is_plausible_fix
from davasus.vf.gnss_quality import HP30_STORM_THRESHOLD
from davasus.vf.learning import compute_success_ratio
from davasus.vf.load import load_events, load_heifers

FIX_COLUMNS: tuple[str, ...] = (
    "neckband_id", "lom", "timestamp", "gnss_lat", "gnss_lon", "outside",
    "audio_stim_new", "pulse_stim_new", "fence_dist_min", "fence_dist_max",
    "imu_tick_40mg", "odometer_km", "odometer_km_new",
)
WEATHER_COLUMNS: tuple[str, ...] = (
    "air_temp_avg", "rel_humid_avg", "rain_mm_tot", "bp_mbar_avg",
    "wind_spd_avg", "rad_net_avg", "sun_hrs_tot",
)
SPACE_COLUMNS: tuple[str, ...] = ("hp30", "ap30", "kp")
MISSING_CODE: int = -1
WEATHER_INTERVAL_MIN: int = 15

# ┌────────────────────────────────────────────────────────────┐
# │ Container  « everything a frame needs »                    │
# └────────────────────────────────────────────────────────────┘


@dataclass(frozen=True)
class DashboardData:
    """Trial data on a 1-min UTC grid; animal axis follows ``loms``.

    Position arrays are NaN where a fix is missing or implausible;
    ``outside`` is ``MISSING_CODE`` where no fix exists at all.
    """

    minutes: pd.DatetimeIndex
    loms: tuple[str, ...]
    neckbands: tuple[str, ...]
    heifers: pd.DataFrame
    events: pd.DataFrame
    projection: LocalProjection
    x_m: np.ndarray
    y_m: np.ndarray
    lat: np.ndarray
    lon: np.ndarray
    outside: np.ndarray
    audio: np.ndarray
    pulse: np.ndarray
    fence_dist_min: np.ndarray
    fence_dist_max: np.ndarray
    imu_40mg: np.ndarray
    odometer_km: np.ndarray
    odometer_new: np.ndarray
    odometer_cum: np.ndarray
    audio_cum: np.ndarray
    pulse_cum: np.ndarray
    weather: pd.DataFrame
    space_weather: pd.DataFrame
    event_index: np.ndarray
    is_gps_reduced: np.ndarray

    @property
    def n_minutes(self) -> int:
        return len(self.minutes)

    @property
    def n_animals(self) -> int:
        return len(self.loms)

    @property
    def has_space_weather(self) -> bool:
        return bool(self.space_weather.notna().any().any())

    def minute_of(self, timestamp: pd.Timestamp) -> int:
        """Grid index for ``timestamp``, clipped to the trial."""
        i = int(self.minutes.searchsorted(timestamp.floor("min")))
        return min(max(i, 0), self.n_minutes - 1)

    def animal_of(self, lom: str) -> int:
        return self.loms.index(lom)


def load_dashboard_data(connection: sqlite3.Connection) -> DashboardData:
    """Read fixes, weather, space weather and events into a ``DashboardData``."""
    fixes = _read_fixes(connection)
    heifers = load_heifers(connection).set_index("lom")
    events = load_events(connection)
    projection = LocalProjection.from_fixes(fixes)
    minutes = build_minute_grid(fixes["timestamp"])
    loms = tuple(sorted(fixes["lom"].unique()))
    neckbands = tuple(
        fixes.drop_duplicates("lom").set_index("lom").loc[list(loms), "neckband_id"].astype(str)
    )
    cubes = build_fix_cubes(fixes, minutes, loms, projection)
    space_weather = build_space_weather_frame(_read_table(connection, "space_weather"), minutes)
    return DashboardData(
        minutes=minutes,
        loms=loms,
        neckbands=neckbands,
        heifers=heifers,
        events=events,
        projection=projection,
        weather=build_weather_frame(_read_table(connection, "vf_weather"), minutes),
        space_weather=space_weather,
        event_index=build_event_index(events, minutes),
        is_gps_reduced=space_weather["hp30"].ge(HP30_STORM_THRESHOLD).to_numpy(),
        **cubes,
    )


def _read_fixes(connection: sqlite3.Connection) -> pd.DataFrame:
    sql = f"SELECT {', '.join(FIX_COLUMNS)} FROM heifer_fixes"
    fixes = pd.read_sql_query(sql, connection)
    fixes["timestamp"] = pd.to_datetime(fixes["timestamp"], utc=True, format="ISO8601")
    return fixes


def _read_table(connection: sqlite3.Connection, table: str) -> pd.DataFrame:
    frame = pd.read_sql_query(f"SELECT * FROM {table}", connection)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, format="ISO8601")
    return frame


# ┌────────────────────────────────────────────────────────────┐
# │ Builders  « long table to (minute, animal) »               │
# └────────────────────────────────────────────────────────────┘


def build_minute_grid(timestamps: pd.Series) -> pd.DatetimeIndex:
    """Every UTC minute from the first to the last fix."""
    return pd.date_range(timestamps.min().floor("min"), timestamps.max().floor("min"),
                         freq="min", tz="UTC")


def build_fix_cubes(
    fixes: pd.DataFrame,
    minutes: pd.DatetimeIndex,
    loms: tuple[str, ...],
    projection: LocalProjection,
) -> dict[str, np.ndarray]:
    """Scatter the long fix table into (minute, animal) arrays.

    Several fixes of one animal in the same minute keep the last one.
    """
    row = minutes.get_indexer(fixes["timestamp"].dt.floor("min"))
    col = pd.Index(loms).get_indexer(fixes["lom"])
    shape = (len(minutes), len(loms))
    ok = is_plausible_fix(fixes).to_numpy()
    x, y = projection.to_metres(fixes["gnss_lat"], fixes["gnss_lon"])

    def scatter(values: np.ndarray, fill: float, dtype: type) -> np.ndarray:
        cube = np.full(shape, fill, dtype=dtype)
        cube[row, col] = values
        return cube

    def numeric(column: str) -> np.ndarray:
        return fixes[column].to_numpy(dtype=float, na_value=np.nan)

    audio = scatter(np.nan_to_num(numeric("audio_stim_new")), 0.0, np.float32)
    pulse = scatter(np.nan_to_num(numeric("pulse_stim_new")), 0.0, np.float32)
    odometer_new = scatter(np.nan_to_num(numeric("odometer_km_new")), 0.0, np.float32)
    outside = fixes["outside"].to_numpy(dtype=float, na_value=MISSING_CODE)
    return {
        "x_m": scatter(np.where(ok, x, np.nan), np.nan, np.float32),
        "y_m": scatter(np.where(ok, y, np.nan), np.nan, np.float32),
        "lat": scatter(numeric("gnss_lat"), np.nan, np.float64),
        "lon": scatter(numeric("gnss_lon"), np.nan, np.float64),
        "outside": scatter(outside.astype(np.int8), MISSING_CODE, np.int8),
        "audio": audio,
        "pulse": pulse,
        "fence_dist_min": scatter(numeric("fence_dist_min"), np.nan, np.float32),
        "fence_dist_max": scatter(numeric("fence_dist_max"), np.nan, np.float32),
        "imu_40mg": scatter(numeric("imu_tick_40mg"), np.nan, np.float32),
        "odometer_km": scatter(numeric("odometer_km"), np.nan, np.float32),
        "odometer_new": odometer_new,
        "odometer_cum": np.cumsum(odometer_new, axis=0, dtype=np.float64),
        "audio_cum": np.cumsum(audio, axis=0, dtype=np.float64),
        "pulse_cum": np.cumsum(pulse, axis=0, dtype=np.float64),
    }


def build_weather_frame(weather: pd.DataFrame, minutes: pd.DatetimeIndex) -> pd.DataFrame:
    """Weather on the minute grid plus NRC THI (D10: computed, never read)."""
    frame = (
        weather.set_index("timestamp")
        .reindex(columns=list(WEATHER_COLUMNS))
        .sort_index()
        .reindex(minutes, method="ffill", limit=30)
    )
    frame["thi_nrc"] = compute_thi_nrc(frame["air_temp_avg"], frame["rel_humid_avg"])
    return frame


def build_space_weather_frame(space: pd.DataFrame, minutes: pd.DatetimeIndex) -> pd.DataFrame:
    """Hp30 / ap30 / Kp held for their interval; all NaN if never fetched."""
    if space.empty:
        return pd.DataFrame(np.nan, index=minutes, columns=list(SPACE_COLUMNS))
    frame = space.set_index("timestamp").reindex(columns=list(SPACE_COLUMNS)).sort_index()
    start = minutes[0].floor("30min")
    return frame[frame.index >= start - pd.Timedelta(hours=3)].reindex(
        minutes, method="ffill", limit=180
    )


def build_event_index(events: pd.DataFrame, minutes: pd.DatetimeIndex) -> np.ndarray:
    """Row of ``events`` in force at each minute (latest start), else -1."""
    order = events.sort_values("start_utc")
    starts = order["start_utc"].to_numpy(dtype="datetime64[ns]")
    grid = minutes.tz_convert(None).to_numpy(dtype="datetime64[ns]")
    position = np.searchsorted(starts, grid, side="right") - 1
    rows = order.index.to_numpy()
    return np.where(position >= 0, rows[np.clip(position, 0, None)], -1)


# ┌────────────────────────────────────────────────────────────┐
# │ Snapshots  « what the panels show at minute i »            │
# └────────────────────────────────────────────────────────────┘


@dataclass(frozen=True)
class HerdSnapshot:
    """Herd geometry at one minute."""

    centroid_x: float
    centroid_y: float
    n_plausible: int
    n_fixes: int
    distance_to_centroid: np.ndarray
    mean_distance_m: float


def compute_herd_snapshot(data: DashboardData, i: int) -> HerdSnapshot:
    x, y = data.x_m[i], data.y_m[i]
    ok = np.isfinite(x)
    if not ok.any():
        nan = np.full(data.n_animals, np.nan)
        return HerdSnapshot(np.nan, np.nan, 0, int((data.outside[i] >= 0).sum()), nan, np.nan)
    cx, cy = float(x[ok].mean()), float(y[ok].mean())
    distance = np.hypot(x - cx, y - cy)
    return HerdSnapshot(cx, cy, int(ok.sum()), int((data.outside[i] >= 0).sum()),
                        distance, float(np.nanmean(distance)))


@dataclass(frozen=True)
class AnimalCueSummary:
    """Cue counts and SR for one animal up to minute i."""

    audio_day: float
    pulse_day: float
    sr_day: float
    audio_total: float
    pulse_total: float
    sr_total: float


def compute_animal_cue_summary(data: DashboardData, i: int, a: int) -> AnimalCueSummary:
    """Same UTC day up to and including minute ``i``, and the trial so far."""
    day_start = data.minute_of(data.minutes[i].floor("D"))
    before = (data.audio_cum[day_start - 1, a], data.pulse_cum[day_start - 1, a]) \
        if day_start > 0 else (0.0, 0.0)
    audio_total, pulse_total = float(data.audio_cum[i, a]), float(data.pulse_cum[i, a])
    audio_day, pulse_day = audio_total - before[0], pulse_total - before[1]
    return AnimalCueSummary(
        audio_day, pulse_day, float(compute_success_ratio(audio_day, pulse_day)),
        audio_total, pulse_total, float(compute_success_ratio(audio_total, pulse_total)),
    )


def compute_animal_km_today(data: DashboardData, i: int, a: int) -> float:
    """Collar odometer distance since UTC midnight, up to minute ``i``."""
    day_start = data.minute_of(data.minutes[i].floor("D"))
    before = data.odometer_cum[day_start - 1, a] if day_start > 0 else 0.0
    return float(data.odometer_cum[i, a] - before)


def compute_trails(data: DashboardData, i: int, length: int) -> tuple[np.ndarray, np.ndarray]:
    """All animals' last ``length`` minutes as one NaN-separated polyline."""
    if length <= 0:
        return np.empty(0), np.empty(0)
    window = slice(max(0, i - length), i + 1)
    pad = np.full((1, data.n_animals), np.nan, dtype=np.float32)
    x = np.vstack([data.x_m[window], pad]).T.ravel()
    y = np.vstack([data.y_m[window], pad]).T.ravel()
    return x, y


def compute_footprint(data: DashboardData, bin_m: float) -> tuple[np.ndarray, tuple[float, ...]]:
    """2D count of all plausible fixes; returns (counts[y, x], extent)."""
    x, y = data.x_m.ravel(), data.y_m.ravel()
    ok = np.isfinite(x)
    x, y = x[ok], y[ok]
    x_lo, x_hi = np.percentile(x, [0.1, 99.9])
    y_lo, y_hi = np.percentile(y, [0.1, 99.9])
    x_edges = _bin_edges(x_lo, x_hi, bin_m)
    y_edges = _bin_edges(y_lo, y_hi, bin_m)
    counts, _, _ = np.histogram2d(y, x, bins=(y_edges, x_edges))
    return counts, (x_edges[0], x_edges[-1], y_edges[0], y_edges[-1])


def _bin_edges(lo: float, hi: float, bin_m: float) -> np.ndarray:
    """At least one bin, even when every fix sits on the same spot."""
    n_bins = max(1, int(np.ceil((hi - lo) / bin_m)))
    return lo + bin_m * np.arange(n_bins + 1)


# ┌────────────────────────────────────────────────────────────┐
# │ Timeline series  « hourly overview of the whole trial »    │
# └────────────────────────────────────────────────────────────┘


def compute_hourly_series(data: DashboardData, bin_rule: str) -> pd.DataFrame:
    """Herd cues, plausible-fix share, weather and space weather per bin."""
    index = data.minutes
    herd = pd.DataFrame({
        "audio": data.audio.sum(axis=1),
        "pulse": data.pulse.sum(axis=1),
        "plausible": np.isfinite(data.x_m).sum(axis=1),
        "fixes": (data.outside >= 0).sum(axis=1),
    }, index=index)
    binned = herd.resample(bin_rule).sum()
    binned["plausible_share"] = binned["plausible"] / binned["fixes"].where(binned["fixes"] > 0)
    weather = data.weather[["air_temp_avg", "rel_humid_avg", "rain_mm_tot", "thi_nrc"]]
    binned = binned.join(weather.resample(bin_rule).mean())
    # Station totals are per 15 min, repeated on every minute of the interval.
    per_minute = data.weather["rain_mm_tot"] / WEATHER_INTERVAL_MIN
    binned["rain_mm_tot"] = per_minute.resample(bin_rule).sum(min_count=1)
    return binned.join(data.space_weather.resample(bin_rule).max())


def compute_animal_hourly_cues(data: DashboardData, a: int, bin_rule: str) -> pd.DataFrame:
    return pd.DataFrame(
        {"audio": data.audio[:, a], "pulse": data.pulse[:, a]}, index=data.minutes
    ).resample(bin_rule).sum()


# ┌────────────────────────────────────────────────────────────┐
# │ Daily series  « focal heifer against the herd »            │
# └────────────────────────────────────────────────────────────┘


def compute_animal_daily_series(data: DashboardData, a: int) -> pd.DataFrame:
    """Per UTC day: collar odometer km, audio cues, pulses and fix count.

    Distance sums ``odometer_km_new`` (km per fix), so the collar's daily
    odometer reset does not matter. First and last trial days are partial;
    days without a single fix are NaN, not zero.
    """
    frame = pd.DataFrame({
        "km": data.odometer_new[:, a],
        "audio": data.audio[:, a],
        "pulse": data.pulse[:, a],
        "fixes": (data.outside[:, a] >= 0).astype(np.int32),
    }, index=data.minutes)
    daily = frame.resample("D").sum()
    no_fix = daily["fixes"] == 0
    daily.loc[no_fix, ["km", "audio", "pulse"]] = np.nan
    return daily


def compute_herd_daily_km(data: DashboardData) -> pd.DataFrame:
    """Median and IQR of daily odometer km across animals."""
    has_fix = pd.DataFrame(data.outside >= 0, index=data.minutes).resample("D").sum() > 0
    daily = pd.DataFrame(data.odometer_new, index=data.minutes).resample("D").sum()
    daily = daily.where(has_fix)
    return pd.DataFrame({
        "q25": daily.quantile(0.25, axis=1),
        "median": daily.median(axis=1),
        "q75": daily.quantile(0.75, axis=1),
    })


def compute_flag_runs(
    flags: np.ndarray, minutes: pd.DatetimeIndex
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """(start, end) of each run of True minutes; end is exclusive."""
    padded = np.concatenate([[False], flags.astype(bool), [False]])
    edges = np.flatnonzero(np.diff(padded.astype(np.int8)))
    step = pd.Timedelta(minutes=1)
    return [(minutes[s], minutes[e - 1] + step) for s, e in zip(edges[::2], edges[1::2], strict=True)]
