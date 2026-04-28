"""End-to-end test for the ``davasus-ingest`` CLI."""

from __future__ import annotations

import sqlite3

from davasus.cli import ingest_main


def test_ingest_main_end_to_end(tmp_path, merged_csv, weather_csv):
    """`ingest_main` runs the whole pipeline and writes a usable DB."""
    db_path = tmp_path / "out.db"
    rc = ingest_main([
        "--merged", str(merged_csv),
        "--weather", str(weather_csv),
        "--db", str(db_path),
    ])
    assert rc == 0
    assert db_path.is_file()

    con = sqlite3.connect(db_path)
    try:
        weather_n = con.execute("SELECT COUNT(*) FROM weather").fetchone()[0]
        eshepherd_n = con.execute("SELECT COUNT(*) FROM eshepherd").fetchone()[0]
        smaxtec_n = con.execute("SELECT COUNT(*) FROM smaxtec").fetchone()[0]
    finally:
        con.close()

    assert weather_n == 2
    assert eshepherd_n == 3
    assert smaxtec_n == 2
