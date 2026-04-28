"""Database → analysis-ready DataFrames.

The :class:`RumenWeatherExtractor` joins a per-animal rumen-temperature
time series (`smaxtec.temp_without_drink_cycles`) to the weather station
record (`weather.air_temp_avg`, `rel_humid_avg`, `wind_spd_avg`,
`rad_swin_avg`) using :func:`pandas.merge_asof` with a 15-minute
tolerance, and computes both the NRC (1971) and Mader–Gaughan THI
variants on the joined frame.

The extractor is read-only: it never mutates the database. Open the
connection in ``mode=ro`` if multiple readers are expected.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pandas as pd

from davasus.thi import compute_thi_mader, compute_thi_nrc

log = logging.getLogger(__name__)

# 15-minute weather cadence → tolerate one full step on either side.
DEFAULT_MERGE_TOLERANCE = pd.Timedelta(minutes=15)


_SQL_BOLUS_ANIMALS = """
SELECT DISTINCT animal_id
FROM smaxtec
WHERE temp_without_drink_cycles IS NOT NULL
ORDER BY animal_id
"""

_SQL_RUMEN = """
SELECT animal_id,
       timestamp,
       temp_without_drink_cycles AS body_temp
FROM smaxtec
WHERE animal_id = ?
  AND temp_without_drink_cycles IS NOT NULL
  AND temp_without_drink_cycles BETWEEN 30 AND 43
ORDER BY timestamp
"""

_SQL_WEATHER = """
SELECT timestamp,
       air_temp_avg,
       rel_humid_avg,
       wind_spd_avg,
       rad_swin_avg
FROM weather
WHERE air_temp_avg IS NOT NULL
  AND rel_humid_avg IS NOT NULL
ORDER BY timestamp
"""


class RumenWeatherExtractor:
    """Pull per-animal rumen + weather frames out of a DaVaSus database.

    Attributes:
        connection: Open :class:`sqlite3.Connection`.
        merge_tolerance: ``merge_asof`` tolerance for matching rumen
            samples to the nearest weather record.
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        merge_tolerance: pd.Timedelta = DEFAULT_MERGE_TOLERANCE,
    ) -> None:
        """Bind the extractor to a connection.

        Args:
            connection: SQLite connection (read-only is fine).
            merge_tolerance: Tolerance for the rumen ↔ weather join.
        """
        self.connection = connection
        self.merge_tolerance = merge_tolerance
        self._weather_cache: pd.DataFrame | None = None

    # ── public API ──────────────────────────────────────────────────────

    def iter_bolus_animals(self) -> Iterator[int]:
        """Yield animal ids that have at least one usable bolus row.

        Yields:
            Integer animal ids in ascending order.
        """
        cur = self.connection.execute(_SQL_BOLUS_ANIMALS)
        for (animal_id,) in cur:
            yield int(animal_id)

    def extract(self, animal_id: int) -> pd.DataFrame:
        """Return the joined rumen + weather + THI frame for one animal.

        Args:
            animal_id: Local farm id.

        Returns:
            DataFrame with columns ``animal_id``, ``timestamp``,
            ``body_temp``, ``air_temp``, ``rh``, ``wind``, ``solar``,
            ``thi_nrc``, ``thi_mader``. Empty if the animal has no
            rumen rows that pass plausibility filters.
        """
        rumen = self._load_rumen(animal_id)
        if rumen.empty:
            return rumen.assign(
                air_temp=pd.NA, rh=pd.NA, wind=pd.NA, solar=pd.NA,
                thi_nrc=pd.NA, thi_mader=pd.NA,
            )
        weather = self._load_weather()
        merged = pd.merge_asof(
            rumen.sort_values("timestamp"),
            weather.sort_values("timestamp"),
            on="timestamp",
            tolerance=self.merge_tolerance,
            direction="nearest",
        )
        return self._with_thi(merged)

    # ── internals ───────────────────────────────────────────────────────

    def _load_rumen(self, animal_id: int) -> pd.DataFrame:
        """Load the per-animal rumen frame, parsing timestamps to datetimes.

        Args:
            animal_id: Local farm id.

        Returns:
            DataFrame with ``animal_id``, ``timestamp`` (datetime),
            ``body_temp``.
        """
        df = pd.read_sql_query(
            _SQL_RUMEN, self.connection, params=(int(animal_id),)
        )
        if df.empty:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df

    def _load_weather(self) -> pd.DataFrame:
        """Load (and cache) the weather frame, parsing timestamps.

        Returns:
            DataFrame with ``timestamp`` and the four meteorological
            columns used in THI computation.
        """
        if self._weather_cache is not None:
            return self._weather_cache
        df = pd.read_sql_query(_SQL_WEATHER, self.connection)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.rename(columns={
            "air_temp_avg": "air_temp",
            "rel_humid_avg": "rh",
            "wind_spd_avg": "wind",
            "rad_swin_avg": "solar",
        })
        self._weather_cache = df
        return df

    def _with_thi(self, df: pd.DataFrame) -> pd.DataFrame:
        """Append ``thi_nrc`` and ``thi_mader`` columns.

        Args:
            df: Frame after the merge_asof step.

        Returns:
            Same frame with two new columns. Solar / wind defaults to 0
            for the THI computation when missing — flagged separately by
            the column NULL-rate.
        """
        df = df.copy()
        df["thi_nrc"] = compute_thi_nrc(df["air_temp"], df["rh"])
        df["thi_mader"] = compute_thi_mader(
            df["air_temp"], df["rh"],
            df["wind"].fillna(0.0),
            df["solar"].fillna(0.0),
        )
        return df


def open_readonly(db_path: str | Path) -> sqlite3.Connection:
    """Open ``db_path`` as a read-only SQLite connection.

    Args:
        db_path: Path to the database file.

    Returns:
        Connection in ``mode=ro``.
    """
    return sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)


# ────────────────────────────────────────────────────────────────────────
#  « Hourly aggregation for cosinor / actogram analyses »
# ────────────────────────────────────────────────────────────────────────


_SQL_SMAXTEC_HOURLY = """
SELECT
    DATE("timestamp")                           AS date,
    CAST(strftime('%H', "timestamp") AS INTEGER) AS hour,
    AVG(CAST(temp_without_drink_cycles AS REAL)) AS rumen_temp,
    AVG(CAST(act_index AS REAL))                 AS act_index,
    AVG(CAST(rum_index_x AS REAL))               AS rum_index_x
FROM smaxtec
WHERE animal_id = ?
  AND temp_without_drink_cycles IS NOT NULL
  AND CAST(temp_without_drink_cycles AS REAL) BETWEEN 30 AND 43
GROUP BY date, hour
ORDER BY date, hour
"""

_SQL_ESHEPHERD_HOURLY = """
SELECT
    DATE("timestamp")                           AS date,
    CAST(strftime('%H', "timestamp") AS INTEGER) AS hour,
    AVG(
        COALESCE(imu_tick_40mg,  0)
      + COALESCE(imu_tick_80mg,  0)
      + COALESCE(imu_tick_120mg, 0)
      + COALESCE(imu_tick_160mg, 0)
      + COALESCE(imu_tick_200mg, 0)
      + COALESCE(imu_tick_240mg, 0)
    ) AS imu_activity
FROM eshepherd
WHERE animal_id = ?
  AND imu_tick_40mg IS NOT NULL
GROUP BY date, hour
ORDER BY date, hour
"""

_SQL_WEATHER_FULL = """
SELECT timestamp,
       air_temp_avg,
       rel_humid_avg,
       wind_spd_avg,
       rad_swin_avg,
       pot_slr_rad_avg
FROM weather
"""


class HourlyAggregator:
    """Pull hourly per-animal-day means for the four circadian signals.

    Attributes:
        connection: Open :class:`sqlite3.Connection`.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        """Bind the aggregator to a connection.

        Args:
            connection: SQLite connection (read-only is fine).
        """
        self.connection = connection
        self._weather: pd.DataFrame | None = None

    # ── per-animal hourly frames ───────────────────────────────────────

    def smaxtec_hourly(self, animal_id: int) -> pd.DataFrame:
        """Return hourly bolus aggregates for one animal.

        Args:
            animal_id: Local farm id.

        Returns:
            DataFrame with columns ``date``, ``hour``, ``rumen_temp``,
            ``act_index``, ``rum_index_x``. Sorted by ``(date, hour)``.
        """
        df = pd.read_sql_query(
            _SQL_SMAXTEC_HOURLY, self.connection, params=(int(animal_id),)
        )
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    def eshepherd_hourly(self, animal_id: int) -> pd.DataFrame:
        """Return hourly collar IMU-tick sum for one animal.

        Args:
            animal_id: Local farm id.

        Returns:
            DataFrame with ``date``, ``hour``, ``imu_activity``.
        """
        df = pd.read_sql_query(
            _SQL_ESHEPHERD_HOURLY, self.connection, params=(int(animal_id),)
        )
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    def signals(self, animal_id: int) -> pd.DataFrame:
        """Return all four signals on a common ``(date, hour)`` grid.

        Args:
            animal_id: Local farm id.

        Returns:
            DataFrame with one row per ``(date, hour)`` and columns
            ``rumen_temp``, ``act_index``, ``rum_index_x``,
            ``imu_activity``. Missing combinations are NaN.
        """
        sx = self.smaxtec_hourly(animal_id)
        es = self.eshepherd_hourly(animal_id)
        if sx.empty and es.empty:
            return pd.DataFrame(
                columns=[
                    "date", "hour",
                    "rumen_temp", "act_index", "rum_index_x", "imu_activity",
                ],
            )
        if sx.empty:
            return es.assign(
                rumen_temp=float("nan"),
                act_index=float("nan"),
                rum_index_x=float("nan"),
            )
        if es.empty:
            return sx.assign(imu_activity=float("nan"))
        return sx.merge(es, on=["date", "hour"], how="outer")

    # ── weather + heat-stress tagging ──────────────────────────────────

    def weather_with_thi(self) -> pd.DataFrame:
        """Return the cached full weather frame with both THI variants.

        Returns:
            DataFrame with ``timestamp``, ``date``, ``air_temp_avg``,
            ``rel_humid_avg``, ``wind_spd_avg``, ``rad_swin_avg``,
            ``pot_slr_rad_avg``, ``thi_nrc``, ``thi_mader``.
        """
        if self._weather is not None:
            return self._weather
        from davasus.thi import compute_thi_mader, compute_thi_nrc

        df = pd.read_sql_query(_SQL_WEATHER_FULL, self.connection)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["date"] = df["timestamp"].dt.date
        df["thi_nrc"] = compute_thi_nrc(df["air_temp_avg"], df["rel_humid_avg"])
        df["thi_mader"] = compute_thi_mader(
            df["air_temp_avg"], df["rel_humid_avg"],
            df["wind_spd_avg"].fillna(0.0),
            df["rad_swin_avg"].fillna(0.0),
        )
        self._weather = df
        return df

    def heat_stress_days(self, breakpoints: pd.DataFrame) -> pd.DataFrame:
        """Tag every ``(animal, date)`` as heat-stress or cool.

        Args:
            breakpoints: DataFrame with at least ``animal_id``,
                ``breakpoint``, and ``success`` columns
                (i.e. the ``broken_stick_results.csv`` schema).

        Returns:
            DataFrame with ``animal_id``, ``date``, ``thi_max_mader``,
            ``breakpoint``, ``heat_stress_day``. Only animals with
            ``success=True`` in ``breakpoints`` are included.
        """
        weather = self.weather_with_thi()
        daily = (
            weather.groupby("date", as_index=False)["thi_mader"]
            .max()
            .rename(columns={"thi_mader": "thi_max_mader"})
        )
        ok = breakpoints[breakpoints["success"]][["animal_id", "breakpoint"]].copy()
        if ok.empty or daily.empty:
            return pd.DataFrame(
                columns=[
                    "animal_id", "date",
                    "thi_max_mader", "breakpoint", "heat_stress_day",
                ],
            )
        out = ok.merge(daily, how="cross")
        out["heat_stress_day"] = out["thi_max_mader"] > out["breakpoint"]
        return out[
            ["animal_id", "date", "thi_max_mader", "breakpoint", "heat_stress_day"]
        ]
