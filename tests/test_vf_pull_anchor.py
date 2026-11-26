"""Pull and anchor events: zigzag pivots, event classification, noise floor."""

from __future__ import annotations

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")

from davasus.vf.constants import PULL_SWING_M  # noqa: E402
from davasus.vf.initiation import build_herd_tracks  # noqa: E402
from davasus.vf.pull_anchor import (  # noqa: E402
    detect_pull_anchor_events,
    estimate_gps_noise,
    find_zigzag_pivots,
)
from davasus.vf.pull_anchor_analysis import (  # noqa: E402
    compute_directional_consistency,
    run_pull_anchor_analysis,
)

N_ANIMALS = 6
N_DAYS = 6
DAY_MIN = 1440
EPISODE_MIN = 120
LEADER, FOLLOWER, ROAMER, HOMEBODY, STILL_A, STILL_B = range(N_ANIMALS)
GPS_SD_M = 1.5
TRIP_M = 40.0


def _episode(kind: str) -> tuple[np.ndarray, np.ndarray]:
    """Positions along x for two heifers over one episode.

    pull:   the mover walks 40 m away in 20 min, waits, the partner walks to her.
    anchor: the mover walks 40 m away and comes back; the partner stays.
    """
    t = np.arange(EPISODE_MIN)
    out = np.clip(t - 10, 0, 20) / 20 * TRIP_M
    mover = out.copy()
    partner = np.zeros(EPISODE_MIN)
    back = np.clip(t - 50, 0, 20) / 20 * TRIP_M
    if kind == "pull":
        partner = back
    else:
        mover = out - back
    return mover, partner


def _synthetic_tracks(seed: int = 3):
    rng = np.random.default_rng(seed)
    n = N_DAYS * DAY_MIN
    x = np.zeros((n, N_ANIMALS))
    y = np.zeros((n, N_ANIMALS))
    y[:, FOLLOWER] = 2.0
    y[:, [ROAMER, HOMEBODY]] = (200.0, 202.0)
    y[:, [STILL_A, STILL_B]] = (400.0, 403.0)
    for start in range(0, n - EPISODE_MIN, 4 * EPISODE_MIN):
        sl = slice(start, start + EPISODE_MIN)
        mover, partner = _episode("pull")
        x[sl, LEADER] += mover
        x[sl, FOLLOWER] += partner
        x[start + EPISODE_MIN:, [LEADER, FOLLOWER]] += TRIP_M
        mover, _ = _episode("anchor")
        x[sl, ROAMER] += mover
    x += rng.normal(0, GPS_SD_M, x.shape)
    y += rng.normal(0, GPS_SD_M, y.shape)
    minutes = pd.date_range("2024-07-26", periods=n, freq="min", tz="UTC")
    events = pd.DataFrame({"start_utc": pd.to_datetime([], utc=True)})
    loms = [f"DE00000000{a:02d}" for a in range(N_ANIMALS)]
    imu = np.full((n, N_ANIMALS), 150.0)
    imu[:, [STILL_A, STILL_B]] = 0.0
    return build_herd_tracks(minutes, loms, x, y, events, np.zeros(n, bool)), imu


def _pair_events(events: pd.DataFrame, a: int, b: int) -> pd.DataFrame:
    pair = {f"DE00000000{a:02d}", f"DE00000000{b:02d}"}
    return events[[{i, p} == pair for i, p in zip(events["initiator"], events["partner"], strict=True)]]


def test_zigzag_ignores_swings_below_threshold():
    values = np.r_[np.zeros(5), np.linspace(0, 30, 10), np.full(5, 30.0), np.linspace(30, 0, 10), np.zeros(5)]
    values = values + np.tile([0.0, 4.0], len(values) // 2 + 1)[:len(values)]
    pivots = find_zigzag_pivots(values, 20.0)
    assert len(pivots) == 3
    assert values[pivots[1]] > 25


def test_pull_and_anchor_classified():
    tracks, _ = _synthetic_tracks()
    events = detect_pull_anchor_events(tracks)
    pulls = _pair_events(events, LEADER, FOLLOWER)
    anchors = _pair_events(events, ROAMER, HOMEBODY)
    assert len(pulls) >= N_DAYS * 2 and (pulls["outcome"] == "pull").all()
    assert (pulls["initiator"] == tracks.loms[LEADER]).all()
    assert len(anchors) >= N_DAYS * 2 and (anchors["outcome"] == "anchor").all()
    assert (anchors["initiator"] == tracks.loms[ROAMER]).all()


def test_still_pair_makes_no_events():
    tracks, _ = _synthetic_tracks()
    events = detect_pull_anchor_events(tracks)
    assert _pair_events(events, STILL_A, STILL_B).empty


def test_gps_noise_floor_from_still_heifers():
    tracks, imu = _synthetic_tracks()
    noise = estimate_gps_noise(tracks.x_m, tracks.y_m, imu).set_index(["measure", "lag_min"])
    assert noise.loc[("displacement", 10), "median_m"] < 4 * GPS_SD_M
    assert noise.loc[("pair_range", 20), "p99_m"] < PULL_SWING_M


def test_dci_bounds():
    assert compute_directional_consistency(np.array([[0, 5], [0, 0]], float)) == 1.0
    assert compute_directional_consistency(np.array([[0, 5], [5, 0]], float)) == 0.0


def test_run_and_plot_pull_anchor():
    from davasus.vf.viz_pull_anchor import plot_pull_anchor

    tracks, imu = _synthetic_tracks()
    result = run_pull_anchor_analysis(tracks, imu, np.random.default_rng(0), n_permutations=50, n_bootstrap=30)
    raw = result.per_heifer[result.per_heifer["scenario"] == "raw"].set_index("lom")
    assert raw.loc[tracks.loms[LEADER], "lead_share"] == 1.0
    assert raw.loc[tracks.loms[FOLLOWER], "lead_share"] == 0.0
    assert raw.loc[tracks.loms[LEADER], "pull_role"] == "leader"
    assert raw.loc[tracks.loms[FOLLOWER], "pull_role"] == "follower"
    fig, table = plot_pull_anchor(result)
    assert len(fig.axes) >= 6 and not table.empty
    matplotlib.pyplot.close(fig)
