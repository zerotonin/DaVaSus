"""Formation analysis: hull arcs, lag-free offsets, heading check, role calls."""

from __future__ import annotations

import matplotlib
import numpy as np
import pandas as pd
import pytest

matplotlib.use("Agg")

from davasus.vf.formation import (  # noqa: E402
    build_formation_frame,
    compute_arc_asymmetry,
    compute_arc_membership,
    fit_formation_offsets,
)
from davasus.vf.formation_analysis import check_headings, run_formation_analysis  # noqa: E402
from davasus.vf.initiation import build_herd_tracks  # noqa: E402

N_ANIMALS = 22
N_MINUTES = 4 * 1440
SEGMENT_MIN = 30
FRONT, REAR, LATE = 0, 1, 2


def _synthetic_tracks(formation_m: np.ndarray, geo_m: np.ndarray, lags_min: np.ndarray):
    """Herd on straight segments with random headings and speeds.

    Heifer a sits at ``formation_m[a]`` (along, lateral) in the herd frame,
    plus a fixed geographic offset ``geo_m[a]`` (east, north), and her collar
    reports the position ``lags_min[a]`` later.
    """
    rng = np.random.default_rng(5)
    n_segments = N_MINUTES // SEGMENT_MIN + 2
    heading = rng.uniform(0, 2 * np.pi, n_segments)
    speed = rng.uniform(4, 25, n_segments)

    def herd_at(t: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        seg = np.clip((t // SEGMENT_MIN).astype(int), 0, n_segments - 1)
        step = np.c_[np.cos(heading) * speed, np.sin(heading) * speed] * SEGMENT_MIN
        start = np.r_[np.zeros((1, 2)), np.cumsum(step, axis=0)[:-1]]
        frac = t - seg * SEGMENT_MIN
        cx = start[seg, 0] + np.cos(heading[seg]) * speed[seg] * frac
        cy = start[seg, 1] + np.sin(heading[seg]) * speed[seg] * frac
        return cx, cy, heading[seg]

    x = np.empty((N_MINUTES, N_ANIMALS))
    y = np.empty((N_MINUTES, N_ANIMALS))
    for a in range(N_ANIMALS):
        cx, cy, h = herd_at(np.arange(N_MINUTES) + lags_min[a])
        along, lateral = formation_m[a]
        x[:, a] = cx + along * np.cos(h) - lateral * np.sin(h) + geo_m[a, 0] + rng.normal(0, 1, N_MINUTES)
        y[:, a] = cy + along * np.sin(h) + lateral * np.cos(h) + geo_m[a, 1] + rng.normal(0, 1, N_MINUTES)
    minutes = pd.date_range("2024-07-26", periods=N_MINUTES, freq="min", tz="UTC")
    events = pd.DataFrame({"start_utc": pd.to_datetime([], utc=True)})
    loms = [f"DE00000000{a:02d}" for a in range(N_ANIMALS)]
    return build_herd_tracks(minutes, loms, x, y, events, np.zeros(N_MINUTES, bool))


def _known_herd():
    rng = np.random.default_rng(7)
    formation = rng.uniform(-6, 6, (N_ANIMALS, 2))
    formation[FRONT] = (12.0, 0.0)
    formation[REAR] = (-12.0, 0.0)
    formation[LATE] = (0.0, 0.0)
    lags = np.zeros(N_ANIMALS)
    lags[LATE] = 0.5
    return _synthetic_tracks(formation, np.zeros((N_ANIMALS, 2)), lags)


def test_arc_membership_picks_extreme_points():
    px = np.array([[10.0, 0.0, -10.0, 0.0, np.nan]])
    py = np.array([[0.0, 10.0, 0.0, -1.0, 0.0]])
    member = compute_arc_membership(px, py, np.array([1.0]), np.array([0.0]))
    assert member[0, 0] == 1 and member[0, 2] == 0 and np.isnan(member[0, 4])


def test_front_and_rear_heifers_found():
    frame = build_formation_frame(_known_herd())
    table = compute_arc_asymmetry(frame)
    assert table.loc[FRONT, "asymmetry"] > 0.9
    assert table.loc[REAR, "asymmetry"] < -0.9


def test_intercept_ignores_collar_lag():
    offsets = fit_formation_offsets(build_formation_frame(_known_herd()))
    assert offsets.loc[LATE, "speed_slope_s"] == pytest.approx(30.0, abs=5.0)
    assert offsets.loc[LATE, "intercept_m"] == pytest.approx(0.0, abs=1.0)
    assert offsets.loc[FRONT, "intercept_m"] == pytest.approx(12.0, abs=1.5)


def test_heading_check_flags_fixed_gps_offsets():
    rng = np.random.default_rng(8)
    geo = rng.uniform(-10, 10, (N_ANIMALS, 2))
    tracks = _synthetic_tracks(np.zeros((N_ANIMALS, 2)), geo, np.zeros(N_ANIMALS))
    headings = check_headings(build_formation_frame(tracks)).set_index("pair")
    assert headings.loc["N-S", "rho"] < -0.5
    assert headings.loc["E-W", "rho"] < -0.5


def test_run_and_plot_formation():
    from davasus.vf.viz_formation import plot_formation

    result = run_formation_analysis(_known_herd(), np.random.default_rng(0),
                                    n_permutations=20, n_bootstrap=30)
    roles = result.per_heifer.drop_duplicates("lom").set_index("lom")["role"]
    assert roles.iloc[FRONT] == "front"
    assert roles.iloc[REAR] == "rear"
    assert roles.iloc[LATE] in {"middle", "timing-dependent", "probable front"}
    fig, table = plot_formation(result)
    assert len(fig.axes) == 4 and not table.empty
    matplotlib.pyplot.close(fig)
