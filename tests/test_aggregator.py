"""Tests for :class:`davasus.extract.HourlyAggregator`."""

from __future__ import annotations

import pandas as pd

from davasus.db import Database
from davasus.extract import HourlyAggregator
from davasus.ingest_merged import MergedIngestor
from davasus.ingest_weather import WeatherIngestor


def _ingest_synthetic(database: Database, merged_csv, weather_csv) -> None:
    WeatherIngestor(database).ingest(weather_csv)
    MergedIngestor(database).ingest(merged_csv)
    database.finalise()


def test_smaxtec_hourly_returns_one_row_per_hour(
    database: Database, merged_csv, weather_csv
):
    """One synthetic bolus row → one hour bucket per animal."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    df = HourlyAggregator(database.connection).smaxtec_hourly(1001)
    assert len(df) == 1
    assert df.iloc[0]["hour"] == 1
    assert df.iloc[0]["rumen_temp"] == 39.12


def test_eshepherd_hourly_sums_imu_ticks(
    database: Database, merged_csv, weather_csv
):
    """Returned ``imu_activity`` is the sum of the six tick channels."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    df = HourlyAggregator(database.connection).eshepherd_hourly(1001)
    # Synthetic row has ticks 61+35+27+25+21+19 = 188.
    assert df.iloc[0]["imu_activity"] == 188.0


def test_heat_stress_days_flags_correctly(
    database: Database, merged_csv, weather_csv
):
    """A breakpoint below the daily THI max ⇒ heat-stress; above ⇒ cool."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    breakpoints = pd.DataFrame(
        {
            "animal_id": [1001, 1002],
            "breakpoint": [40.0, 99.0],
            "success": [True, True],
        }
    )
    out = HourlyAggregator(database.connection).heat_stress_days(breakpoints)
    by_animal = out.groupby("animal_id")["heat_stress_day"].any().to_dict()
    assert by_animal[1001] is True   # daily THI > 40
    assert by_animal[1002] is False  # daily THI < 99
