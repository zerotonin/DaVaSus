"""Tests for the heifer dashboard: data layer, tiles, space weather, GUI smoke."""

from __future__ import annotations

import math
import sqlite3

import numpy as np
import pandas as pd
import pytest

from davasus.vf.dashboard.data import (
    MISSING_CODE,
    compute_animal_cue_summary,
    compute_herd_snapshot,
    compute_hourly_series,
    compute_trails,
    load_dashboard_data,
)
from davasus.vf.dashboard.summaries import (
    build_herd_table,
    describe_event,
    describe_heifer,
    describe_outside_code,
    describe_space_weather,
    describe_time,
    describe_weather,
)
from davasus.vf.dashboard.tiles import (
    TileCache,
    compute_covering_tiles,
    compute_tile_bounds,
    compute_tile_xy,
)
from davasus.vf.gnss_quality import (
    build_space_weather_table,
    parse_gfz_json,
    write_space_weather,
)
from davasus.vf.ingest import HeiferVFIngestor
from tests.test_vf_ingest import heifer_csv  # noqa: F401  (fixture)


@pytest.fixture
def vf_db(heifer_csv, tmp_path):  # noqa: F811
    path = tmp_path / "vf.db"
    con = sqlite3.connect(path)
    HeiferVFIngestor(con, heifer_csv, chunk_rows=3).run()
    yield con
    con.close()


@pytest.fixture
def data(vf_db):
    return load_dashboard_data(vf_db)


# ─────────────────────────────────────────────────────────────────
#  Data layer
# ─────────────────────────────────────────────────────────────────


def test_cubes_are_minute_by_animal(data):
    assert data.n_minutes == 4
    assert data.n_animals == 2
    for cube in (data.x_m, data.outside, data.audio, data.audio_cum):
        assert cube.shape == (4, 2)
    assert (data.outside != MISSING_CODE).all()
    assert np.isfinite(data.x_m).all()


def test_event_index_follows_activations(data):
    labels = [data.events.loc[r, "label"] if r >= 0 else None for r in data.event_index]
    assert labels[0] == "luciano Heifers 2"
    assert labels[-1] == "heifers 3_2"


def test_minute_of_clips_to_trial(data):
    assert data.minute_of(pd.Timestamp("2020-01-01", tz="UTC")) == 0
    assert data.minute_of(pd.Timestamp("2030-01-01", tz="UTC")) == data.n_minutes - 1


def test_trails_are_nan_separated(data):
    x, y = compute_trails(data, 3, 2)
    assert x.shape == y.shape == (2 * 4,)
    assert np.isnan(x[3]) and np.isnan(x[7])
    assert compute_trails(data, 3, 0)[0].size == 0


def test_herd_snapshot_and_cues(data):
    herd = compute_herd_snapshot(data, 0)
    assert herd.n_plausible == 2
    assert herd.mean_distance_m == pytest.approx(0.0, abs=1e-6)
    cues = compute_animal_cue_summary(data, 3, 0)
    assert cues.audio_total == cues.audio_day == 0
    assert math.isnan(cues.sr_total)


def test_hourly_series_has_all_panels(data):
    hourly = compute_hourly_series(data, "1h")
    for column in ("audio", "pulse", "plausible_share", "thi_nrc", "hp30", "kp"):
        assert column in hourly
    assert hourly["plausible_share"].iloc[0] == pytest.approx(1.0)


# ─────────────────────────────────────────────────────────────────
#  Panel text
# ─────────────────────────────────────────────────────────────────


def test_summaries_render_every_panel(data):
    for rows in (describe_time(data, 2), describe_event(data, 2), describe_weather(data, 2),
                 describe_space_weather(data, 2), describe_heifer(data, 2, 1)):
        assert rows and all(isinstance(k, str) and isinstance(v, str) for k, v in rows)
    assert "not fetched" in describe_space_weather(data, 0)[0][1]
    assert len(build_herd_table(data, 1)) == 2


def test_outside_code_text():
    assert describe_outside_code(MISSING_CODE) == "no fix"
    assert describe_outside_code(5).startswith("5: pre-fence")


# ─────────────────────────────────────────────────────────────────
#  Tiles
# ─────────────────────────────────────────────────────────────────


def test_tile_contains_its_point():
    lat, lon = 52.39, 14.28
    x, y = compute_tile_xy(lat, lon, 17)
    bounds = compute_tile_bounds(x, y, 17)
    assert bounds.south <= lat <= bounds.north
    assert bounds.west <= lon <= bounds.east


def test_covering_tiles_span_the_box():
    tiles = compute_covering_tiles(52.38, 52.40, 14.27, 14.29, 15)
    xs, ys = {t[0] for t in tiles}, {t[1] for t in tiles}
    assert len(tiles) == len(xs) * len(ys)


def test_tile_cache_offline_returns_cached_only(tmp_path):
    cache = TileCache(tmp_path)
    assert cache.get(17, 1, 2, allow_download=False) is None
    path = cache.path_for(17, 1, 2)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"png")
    assert cache.get(17, 1, 2, allow_download=False) == path


# ─────────────────────────────────────────────────────────────────
#  Space weather
# ─────────────────────────────────────────────────────────────────

HOURS = ["2024-08-02T09:00:00Z", "2024-08-02T09:30:00Z", "2024-08-02T10:00:00Z",
         "2024-08-02T10:30:00Z"]


def _space_table() -> pd.DataFrame:
    hp30 = parse_gfz_json({"datetime": HOURS, "Hp30": [1.0, -1.0, 6.333, 2.0]}, "Hp30")
    ap30 = parse_gfz_json({"datetime": HOURS, "ap30": [4.0, 5.0, 80.0, 7.0]}, "ap30")
    kp = parse_gfz_json({"datetime": ["2024-08-02T09:00:00Z"], "Kp": [3.667]}, "Kp")
    return build_space_weather_table(hp30, ap30, kp)


def test_gfz_missing_value_becomes_nan():
    table = _space_table()
    assert math.isnan(table.loc[1, "hp30"])
    assert (table["kp"] == 3.667).all()


def test_space_weather_reaches_the_panel(vf_db):
    write_space_weather(vf_db, _space_table())
    data = load_dashboard_data(vf_db)
    assert data.has_space_weather
    rows = dict(describe_space_weather(data, 2))
    assert rows["Hp30"] == "6.33"
    assert rows["storm (Hp30 ≥ 6)"] == "yes"


# ─────────────────────────────────────────────────────────────────
#  GUI smoke test (offscreen)
# ─────────────────────────────────────────────────────────────────


def test_window_builds_and_scrubs(data, tmp_path, monkeypatch):
    pytest.importorskip("pyqtgraph")
    qt_widgets = pytest.importorskip("PySide6.QtWidgets")
    from PySide6.QtCore import QSettings

    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    QSettings.setPath(QSettings.Format.NativeFormat, QSettings.Scope.UserScope, str(tmp_path))
    app = qt_widgets.QApplication.instance() or qt_widgets.QApplication([])
    from davasus.vf.dashboard.app import build_window

    window = build_window(data, None, "test.db")
    window.set_minute(2)
    window.select_animal(1)
    window.labels_box.setChecked(True)
    window.follow_box.setChecked(True)
    window._jump_to_cue(1)
    app.processEvents()
    assert window.minute == 2
    assert window.selected == 1
    window.deleteLater()
