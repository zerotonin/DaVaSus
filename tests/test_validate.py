"""Tests for :mod:`davasus.validate`."""

from __future__ import annotations

from davasus.cli import validate_main
from davasus.db import Database
from davasus.ingest_merged import MergedIngestor
from davasus.ingest_weather import WeatherIngestor
from davasus.validate import Validator, render_report


def _ingest_synthetic(database: Database, merged_csv, weather_csv) -> None:
    """Helper: run both ingesters on the synthetic fixtures."""
    WeatherIngestor(database).ingest(weather_csv)
    MergedIngestor(database).ingest(merged_csv)
    database.finalise()


def test_validator_reports_table_summaries(database: Database, merged_csv, weather_csv):
    """Every expected table appears in the report with the correct row count."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    report = Validator(database.connection, database.path).run()
    counts = {t.name: t.rows for t in report.tables}
    assert counts == {
        "animals": 3,
        "neckbands": 3,
        "source_files": 2,
        "eshepherd": 3,
        "smaxtec": 2,
        "weather": 2,
    }


def test_validator_finds_no_orphans_on_clean_ingest(
    database: Database, merged_csv, weather_csv
):
    """The synthetic ingest produces no FK orphans."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    report = Validator(database.connection, database.path).run()
    assert all(n == 0 for n in report.orphans.values())


def test_validator_reports_value_ranges(database: Database, merged_csv, weather_csv):
    """Range checks return observed min/max for non-empty columns."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    report = Validator(database.connection, database.path).run()
    by_col = {(r.table, r.column): r for r in report.ranges}
    temp = by_col[("smaxtec", "temp")]
    assert temp.observed_min is not None
    assert temp.observed_max is not None
    assert 30.0 <= temp.observed_min <= 43.0


def test_render_report_contains_section_headers(
    database: Database, merged_csv, weather_csv
):
    """The text rendering exposes the expected sections."""
    _ingest_synthetic(database, merged_csv, weather_csv)
    text = render_report(Validator(database.connection, database.path).run())
    for header in ("Tables", "NULL rates", "Value ranges", "Referential integrity"):
        assert header in text


def test_validate_main_exit_code_clean(tmp_path, merged_csv, weather_csv, capsys):
    """End-to-end CLI run on a clean DB returns exit code 0."""
    db_path = tmp_path / "v.db"
    with Database(db_path) as db:
        db.initialise()
        _ingest_synthetic(db, merged_csv, weather_csv)
    rc = validate_main(["--db", str(db_path)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "DaVaSus validation report" in out
