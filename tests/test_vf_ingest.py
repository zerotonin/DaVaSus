"""Tests for :mod:`davasus.vf.ingest` and the ``davasus-vf-ingest`` CLI."""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from davasus.vf.cli import ingest_main
from davasus.vf.ingest import REQUIRED_COLUMNS, HeaderError, HeiferVFIngestor

SENTINEL = -2147483647.0
NB1, NB2, NB_OFF = "111111111", "222222222", "333333333"


def _row(neckband: str, minute: int, label: str, **overrides: object) -> dict[str, object]:
    stamp = f"2024-08-02 10:{minute:02d}:00+00:00"
    row: dict[str, object] = dict.fromkeys(REQUIRED_COLUMNS, 0.0)
    row.update(
        {
            "Neckband_Number": int(neckband),
            "LOM_X": f"DE00{neckband[0]}",
            "Ohrmarke": int(neckband[0]),
            "GEB_DATR": "21/04/2023",
            "GESCHL_R": "W",
            "RASSE": "AA",
            "GVE": "0,6",
            "Unix_Timestamp": stamp,
            "TIMESTAMP": stamp,
            "GNSS_Latitude": 52.39,
            "GNSS_Longitude": 14.28,
            "Distance_To_Fence_Max": -10.0,
            "Distance_To_Fence_Min": -12.0,
            "eShepherd parcel activation": label,
            "comments": None,
            "neckband problem with GPS": None,
            "neckband not activated for new parcel": None,
            "AirT_C_Avg": 20.0 + minute,
        }
    )
    if label == "heifers 3_2":
        row.update(
            {
                "manually moving herd": 1.0,
                "water point relocation": 0.0,
                "comments": "herd broke out",
                "neckband not activated for new parcel": float(NB_OFF),
            }
        )
    row.update(overrides)
    return row


@pytest.fixture
def heifer_csv(tmp_path):
    rows = [
        _row(NB1, 0, "luciano Heifers 2", Distance_To_Fence_Max=SENTINEL),
        _row(NB1, 1, "luciano Heifers 2"),
        _row(NB1, 2, "luciano Heifers 2", Outside=0),
        _row(NB1, 2, "heifers 3_2", Outside=3),
        _row(NB1, 3, "heifers 3_2"),
        _row(NB2, 0, "luciano Heifers 2"),
        _row(NB2, 1, "luciano Heifers 2"),
        _row(NB2, 2, "heifers 3_2", Outside=3),
        _row(NB2, 2, "luciano Heifers 2", Outside=0),
        _row(NB2, 3, "heifers 3_2"),
    ]
    frame = pd.DataFrame(rows)
    frame.insert(0, "Unnamed: 0", range(len(frame)))
    path = tmp_path / "heifers.csv"
    frame.to_csv(path)
    return path


@pytest.fixture
def ingested(heifer_csv, tmp_path):
    con = sqlite3.connect(tmp_path / "vf.db")
    counts = HeiferVFIngestor(con, heifer_csv, chunk_rows=3).run()
    yield con, counts
    con.close()


def test_counts_and_duplicates(ingested):
    _, counts = ingested
    assert counts["rows_read"] == 10
    assert counts["heifer_fixes"] == 8
    assert counts["duplicate_fixes"] == 2
    assert counts["heifers"] == 2
    assert counts["vf_weather"] == 4
    assert counts["parcel_events"] == 2


@pytest.mark.parametrize("neckband", [NB1, NB2])
def test_boundary_duplicate_keeps_later_event(ingested, neckband):
    con, _ = ingested
    event_id, outside = con.execute(
        "SELECT event_id, outside FROM heifer_fixes WHERE neckband_id = ? "
        "AND timestamp = '2024-08-02T10:02:00+00:00'",
        (neckband,),
    ).fetchone()
    assert (event_id, outside) == (2, 3)


def test_sentinel_becomes_null(ingested):
    con, _ = ingested
    value = con.execute(
        "SELECT fence_dist_max FROM heifer_fixes WHERE neckband_id = ? "
        "AND timestamp = '2024-08-02T10:00:00+00:00'",
        (NB1,),
    ).fetchone()[0]
    assert value is None


def test_events_flags_and_exclusions(ingested):
    con, _ = ingested
    events = con.execute(
        "SELECT event_id, label, manual_herding, comments, new_parcel FROM parcel_events "
        "ORDER BY event_id"
    ).fetchall()
    assert events[0][:3] == (1, "luciano Heifers 2", 0)
    assert events[1] == (2, "heifers 3_2", 1, "herd broke out", 3)
    exclusions = con.execute("SELECT * FROM parcel_event_exclusions").fetchall()
    assert exclusions == [(2, NB_OFF, "not_activated")]


def test_heifer_register_is_normalised(ingested):
    con, _ = ingested
    birth_date, gve = con.execute(
        "SELECT birth_date, gve FROM heifers WHERE lom = 'DE001'"
    ).fetchone()
    assert birth_date == "2023-04-21"
    assert gve == pytest.approx(0.6)


def test_refuses_second_ingest(ingested, heifer_csv):
    con, _ = ingested
    with pytest.raises(RuntimeError, match="not empty"):
        HeiferVFIngestor(con, heifer_csv).run()


def test_missing_column_raises(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    con = sqlite3.connect(":memory:")
    with pytest.raises(HeaderError, match="Neckband_Number"):
        HeiferVFIngestor(con, path).run()


def test_cli_end_to_end(heifer_csv, tmp_path):
    db = tmp_path / "cli.db"
    argv = ["--csv", str(heifer_csv), "--db", str(db), "--no-progress"]
    assert ingest_main(argv) == 0
    assert ingest_main(argv) == 2
    assert ingest_main([*argv, "--overwrite"]) == 0
