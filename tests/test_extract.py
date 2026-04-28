"""Tests for :class:`davasus.extract.RumenWeatherExtractor`."""

from __future__ import annotations

import sqlite3

from davasus.db import Database
from davasus.extract import RumenWeatherExtractor, open_readonly
from davasus.ingest_merged import MergedIngestor
from davasus.ingest_weather import WeatherIngestor


def _ingest_synthetic(database: Database, merged_csv, weather_csv) -> None:
    """Helper: run both ingesters on the synthetic fixtures."""
    WeatherIngestor(database).ingest(weather_csv)
    MergedIngestor(database).ingest(merged_csv)
    database.finalise()


def test_iter_bolus_animals_returns_only_bolus_carriers(
    database: Database, merged_csv, weather_csv
):
    """Male / collar-only animals are not yielded."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    extractor = RumenWeatherExtractor(database.connection)
    animals = list(extractor.iter_bolus_animals())
    assert animals == [1001, 1002]  # 1003 is collar-only


def test_extract_returns_thi_columns(database: Database, merged_csv, weather_csv):
    """Both THI variants land on the joined frame."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    extractor = RumenWeatherExtractor(database.connection)
    df = extractor.extract(1001)
    assert {"body_temp", "thi_nrc", "thi_mader"}.issubset(df.columns)
    # The extractor pulls temp_without_drink_cycles, which the fixture
    # sets to 39.12 for animal 1001 (the bare ``temp`` column is 39.06).
    assert df.loc[0, "body_temp"] == 39.12


def test_open_readonly_rejects_writes(tmp_path, merged_csv, weather_csv):
    """A connection from open_readonly cannot insert."""
    db_path = tmp_path / "ro.db"
    with Database(db_path) as db:
        db.initialise()
        _ingest_synthetic(db, merged_csv, weather_csv)
    con = open_readonly(db_path)
    try:
        try:
            con.execute("INSERT INTO animals (animal_id) VALUES (9999)")
            con.commit()
            raised = False
        except sqlite3.OperationalError:
            raised = True
    finally:
        con.close()
    assert raised
