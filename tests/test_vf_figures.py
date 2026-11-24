"""Tests for the M8 loaders, geometry, daily cue summary, figures and movie."""

from __future__ import annotations

import shutil
import sqlite3

import matplotlib

matplotlib.use("Agg")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from davasus.vf.geometry import LocalProjection, is_plausible_fix  # noqa: E402
from davasus.vf.learning import compute_daily_stimulus_summary  # noqa: E402
from davasus.vf.load import find_most_stimulated_heifer, load_events, load_fixes  # noqa: E402
from davasus.vf.movie import (  # noqa: E402
    compute_occupancy_frames,
    render_occupancy_movie,
    summarise_frames,
)
from davasus.vf.viz import (  # noqa: E402
    compute_herd_sr_band,
    plot_daily_trajectory,
    plot_stimulus_timeline,
)


def _synthetic_fixes(n_minutes: int = 2 * 24 * 60) -> pd.DataFrame:
    """Two heifers walking circles for two days; heifer A gets the cues."""
    rng = np.random.default_rng(0)
    times = pd.date_range("2024-08-01", periods=n_minutes, freq="min", tz="UTC")
    frames = []
    for lom, radius, audio_rate in (("DE_A", 1e-4, 0.2), ("DE_B", 2e-4, 0.02)):
        phase = np.linspace(0, 8 * np.pi, n_minutes)
        audio = (rng.random(n_minutes) < audio_rate).astype(float)
        frames.append(pd.DataFrame({
            "neckband_id": lom,
            "lom": lom,
            "timestamp": times,
            "gnss_lat": 52.4 + radius * np.sin(phase),
            "gnss_lon": 14.3 + radius * np.cos(phase),
            "outside": 0,
            "audio_stim_new": audio,
            "pulse_stim_new": audio * (rng.random(n_minutes) < 0.1),
        }))
    fixes = pd.concat(frames, ignore_index=True)
    fixes.loc[0, ["gnss_lat", "gnss_lon"]] = 0.0
    fixes.loc[1, "outside"] = 2
    return fixes


def _events() -> pd.DataFrame:
    return pd.DataFrame({
        "event_id": [1, 2],
        "open_parcels": ["2", "3_2"],
        "is_transition": [0, 0],
        "manual_herding": [0, 1],
        "start_utc": pd.to_datetime(["2024-08-01", "2024-08-02"], utc=True),
        "end_utc": pd.to_datetime(["2024-08-01 23:59", "2024-08-02 23:59"], utc=True),
    })


def test_plausibility_gate():
    fixes = _synthetic_fixes(10)
    ok = is_plausible_fix(fixes)
    assert not ok.iloc[0] and not ok.iloc[1] and ok.iloc[2:].all()


def test_projection_scale():
    projection = LocalProjection(52.0, 14.0)
    x, y = projection.to_metres(pd.Series([52.001]), pd.Series([14.001]))
    assert y[0] == pytest.approx(111.2, abs=0.1)
    assert x[0] == pytest.approx(111.2 * np.cos(np.radians(52.0)), abs=0.1)


def test_daily_summary_sums_before_ratio():
    fixes = pd.DataFrame({
        "lom": "A",
        "timestamp": pd.to_datetime(["2024-08-01 01:00", "2024-08-01 02:00", "2024-08-02 01:00"],
                                    utc=True),
        "audio_stim_new": [4.0, 6.0, 5.0],
        "pulse_stim_new": [1.0, 1.0, 0.0],
    })
    daily = compute_daily_stimulus_summary(fixes)
    assert daily["sr"].tolist() == pytest.approx([0.8, 1.0])
    assert daily["ratio_file"].iloc[0] == pytest.approx(10 / 12)
    assert daily["sr_cum"].iloc[1] == pytest.approx(13 / 15)


def test_herd_band_ignores_low_cue_days():
    daily = pd.DataFrame({"date": [1, 1, 1], "audio": [10, 10, 1], "sr": [0.5, 0.7, 0.0]})
    band = compute_herd_sr_band(daily)
    assert band["median"].iloc[0] == pytest.approx(0.6)


def test_figures_return_data():
    fixes = _synthetic_fixes()
    projection = LocalProjection.from_fixes(fixes)
    day = fixes[fixes["timestamp"] < "2024-08-02"]
    fig, data = plot_daily_trajectory(day, "DE_A", projection)
    assert {"x_m", "y_m", "hour", "is_plausible"} <= set(data.columns)
    daily = compute_daily_stimulus_summary(fixes)
    fig, data = plot_stimulus_timeline(daily, _events(), "DE_A")
    assert len(data) == 2


def test_occupancy_frames_conserve_fixes():
    fixes = _synthetic_fixes()
    projection = LocalProjection.from_fixes(fixes)
    frames = compute_occupancy_frames(fixes, projection, bin_m=2.0, frame_hours=1)
    assert len(frames.starts) == 48
    summary = summarise_frames(frames)
    assert summary["n_fixes"].sum() <= is_plausible_fix(fixes).sum()
    assert summary["n_fixes"].sum() > 0.95 * is_plausible_fix(fixes).sum()


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg not installed")
def test_movie_is_written(tmp_path):
    fixes = _synthetic_fixes(6 * 60)
    frames = compute_occupancy_frames(fixes, LocalProjection.from_fixes(fixes), 2.0, 1)
    path = render_occupancy_movie(frames, _events(), tmp_path / "m.mp4", fps=6)
    assert path.stat().st_size > 0


def test_loaders_read_ingested_db(tmp_path):
    from davasus.vf.schema import VFSchema

    con = sqlite3.connect(tmp_path / "vf.db")
    VFSchema(con).create_all()
    con.execute("INSERT INTO parcel_events (event_id, label, open_parcels, start_utc) "
                "VALUES (1, 'heifers 3_2', '3_2', '2024-08-01T00:00:00+00:00')")
    con.executemany(
        "INSERT INTO heifer_fixes (neckband_id, lom, timestamp, audio_stim_new) VALUES (?,?,?,?)",
        [("1", "A", "2024-08-01T00:00:00+00:00", 1.0), ("2", "B", "2024-08-01T00:00:00+00:00", 5.0),
         ("2", "B", "2024-08-02T00:00:00+00:00", 5.0)],
    )
    assert find_most_stimulated_heifer(con) == "B"
    assert len(load_fixes(con, lom="B", end_utc="2024-08-02")) == 1
    assert load_events(con)["start_utc"].dt.tz is not None
    con.close()
