"""Tests for :class:`davasus.ingest_weather.WeatherIngestor`."""

from __future__ import annotations

from davasus.db import Database
from davasus.ingest_weather import WeatherIngestor


def test_weather_ingest_inserts_two_rows(database: Database, weather_csv):
    """Two synthetic rows land in the ``weather`` table."""
    n = WeatherIngestor(database).ingest(weather_csv)
    assert n == 2
    assert database.count_rows("weather") == 2


def test_weather_timestamps_are_iso(database: Database, weather_csv):
    """Stored timestamps use the ``T`` separator + full ``+HH:MM`` offset."""
    WeatherIngestor(database).ingest(weather_csv)
    cur = database.connection.execute(
        "SELECT timestamp FROM weather ORDER BY timestamp"
    )
    timestamps = [r[0] for r in cur.fetchall()]
    assert timestamps == [
        "2024-02-21T00:00:00+00:00",
        "2024-02-21T00:15:00+00:00",
    ]


def test_weather_record_is_int(database: Database, weather_csv):
    """The ``record`` column is stored as INTEGER."""
    WeatherIngestor(database).ingest(weather_csv)
    cur = database.connection.execute("SELECT record FROM weather LIMIT 1")
    (record,) = cur.fetchone()
    assert isinstance(record, int)
    assert record == 51498


def test_weather_curated_columns_present(database: Database, weather_csv):
    """A representative curated column is non-NULL after ingest."""
    WeatherIngestor(database).ingest(weather_csv)
    cur = database.connection.execute(
        "SELECT pot_slr_rad_avg FROM weather ORDER BY timestamp LIMIT 1"
    )
    (val,) = cur.fetchone()
    assert val == -20.1
