"""Dumont leadership: bouts, departures, lags, attempts and statistics."""

from __future__ import annotations

import matplotlib
import numpy as np
import pandas as pd
import pytest

matplotlib.use("Agg")

from davasus.vf.dumont import run_dumont_analysis  # noqa: E402
from davasus.vf.initiation import (  # noqa: E402
    build_herd_tracks,
    compute_departures,
    compute_first_mover_credits,
    detect_initiation_attempts,
    detect_travel_bouts,
    estimate_sampling_lags,
    fill_short_gaps,
    summarise_bouts,
)
from davasus.vf.leadership_stats import (  # noqa: E402
    ChoiceStage,
    CreditMatrix,
    build_credit_matrix,
    compute_leader_shares,
    compute_leader_w_benchmark,
    compute_order_w,
    fit_plackett_luce,
)
from davasus.vf.leadership_stats import test_egalitarian_null as run_egalitarian_null  # noqa: E402

N_ANIMALS = 22
BOUT_STARTS = (60, 160, 260, 360, 460, 560)
BOUT_LENGTH = 8
BOUT_SPEEDS = (12.0, 15.0, 18.0, 22.0, 26.0, 30.0)


def _synthetic_tracks(lags_min: np.ndarray | None = None, leader_lead_min: float = 2.0,
                      spread_min: float = 1.5, angle_rad: float | None = None):
    """Grazing herd with six travel bouts; heifer 0 always leaves first.

    Positions are a continuous function of time; heifer k is sampled at
    t + lags_min[k] to mimic a collar that fixes later in the minute.
    """
    rng = np.random.default_rng(3)
    n_minutes = BOUT_STARTS[-1] + 100
    lags = np.zeros(N_ANIMALS) if lags_min is None else lags_min
    offsets = rng.uniform(-10, 10, size=(N_ANIMALS, 2))
    delays = np.r_[0.0, leader_lead_min + rng.uniform(0, spread_min, N_ANIMALS - 1)]
    angles = (rng.uniform(0, 2 * np.pi, len(BOUT_STARTS)) if angle_rad is None
              else np.full(len(BOUT_STARTS), angle_rad))
    x = np.empty((n_minutes, N_ANIMALS))
    y = np.empty((n_minutes, N_ANIMALS))
    for a in range(N_ANIMALS):
        t = np.arange(n_minutes) + lags[a]
        px, py = np.full(n_minutes, offsets[a, 0]), np.full(n_minutes, offsets[a, 1])
        for start, angle, speed in zip(BOUT_STARTS, angles, BOUT_SPEEDS, strict=True):
            moving = np.clip(t - start - delays[a], 0, BOUT_LENGTH) * speed
            px += moving * np.cos(angle)
            py += moving * np.sin(angle)
        x[:, a], y[:, a] = px, py
    minutes = pd.date_range("2024-07-26", periods=n_minutes, freq="min", tz="UTC")
    events = pd.DataFrame({"start_utc": pd.to_datetime([], utc=True)})
    loms = [f"DE00000000{a:02d}" for a in range(N_ANIMALS)]
    return build_herd_tracks(minutes, loms, x, y, events, np.zeros(n_minutes, bool))


def test_fill_short_gaps_only_fills_short_runs():
    values = np.arange(12, dtype=float)[:, None]
    values[2:4] = np.nan
    values[6:11] = np.nan
    out = fill_short_gaps(values, 3)
    assert np.allclose(out[2:4, 0], [2, 3])
    assert np.isnan(out[6:11, 0]).all()


def test_bouts_and_first_mover_found():
    tracks = _synthetic_tracks()
    bouts = detect_travel_bouts(tracks)
    assert len(bouts) == len(BOUT_STARTS)
    departures = compute_departures(tracks, bouts)
    bouts = summarise_bouts(bouts, departures)
    assert bouts["is_group_movement"].all()
    credits, per_bout = compute_first_mover_credits(departures, bouts)
    shares = compute_leader_shares(build_credit_matrix(credits))
    assert shares[0] == pytest.approx(1.0)
    assert (per_bout["n_tied"] == 1).all() and per_bout["is_clear_lead"].all()


def test_departure_lag_is_recovered():
    # A herd that moves in sync: any front offset growing with speed is collar lag.
    lags = np.zeros(N_ANIMALS)
    lags[1] = 0.4
    tracks = _synthetic_tracks(lags, leader_lead_min=0.0, spread_min=0.0, angle_rad=0.3)
    estimate = estimate_sampling_lags(tracks).set_index("lom")["lag_s"]
    assert estimate.iloc[1] == pytest.approx(24.0 - 24.0 / N_ANIMALS, abs=6.0)
    assert estimate.drop(estimate.index[1]).abs().max() < 6.0


def test_order_w_matches_kendall_and_benchmark():
    rng = np.random.default_rng(0)
    n, m = 8, 30
    ranks = np.array([rng.permutation(n) for _ in range(m)], dtype=float)
    rank_sums = (ranks + 1).sum(axis=0)
    kendall = 12 * ((rank_sums - rank_sums.mean()) ** 2).sum() / (m**2 * (n**3 - n))
    assert compute_order_w(ranks / (n - 1)) == pytest.approx(kendall)
    assert compute_leader_w_benchmark(24) == pytest.approx(0.12)
    leader = np.array([np.r_[0, 1 + rng.permutation(n - 1)] for _ in range(4000)], dtype=float)
    assert compute_order_w(leader / (n - 1)) == pytest.approx(compute_leader_w_benchmark(n), abs=0.01)


def test_plackett_luce_recovers_worth_order():
    rng = np.random.default_rng(1)
    worth = np.array([0.4, 0.3, 0.2, 0.1])
    everyone = np.arange(4)
    stages = [ChoiceStage(everyone, np.array([rng.choice(4, p=worth)]), np.ones(1)) for _ in range(3000)]
    fitted = fit_plackett_luce(stages, 4)
    assert np.allclose(fitted, worth, atol=0.03)


def test_egalitarian_null_rejects_a_strong_leader():
    rng = np.random.default_rng(2)
    credit = np.zeros((60, 10))
    credit[:, 0] = 1.0
    credit[::3, 0], credit[::3, 1] = 0.0, 1.0
    matrix = CreditMatrix([f"a{k}" for k in range(10)], credit, np.repeat(np.arange(12), 5))
    summary, per_animal = run_egalitarian_null(matrix, rng, 200)
    top = summary.set_index("statistic").loc["top_share"]
    assert top["p"] < 0.01
    assert per_animal.loc[0, "q_egalitarian_bh"] < 0.05


def test_attempt_with_followers_beats_chance():
    tracks = _synthetic_tracks(leader_lead_min=4.0)
    attempts = detect_initiation_attempts(tracks, np.random.default_rng(0))
    leader = attempts[attempts["lom"] == tracks.loms[0]]
    assert len(leader) >= len(BOUT_STARTS) - 1
    assert (leader["n_followers"] >= N_ANIMALS - 2).all()
    assert leader["n_followers_chance"].max() == 0
    assert leader["is_group_success"].all()


def test_run_and_plot_dumont_analysis():
    from davasus.vf.viz_leadership import plot_dumont_leadership

    result = run_dumont_analysis(_synthetic_tracks(), np.random.default_rng(0),
                                 n_permutations=50, n_bootstrap=50)
    raw = result.per_animal[result.per_animal["scenario"] == "raw"].set_index("lom")
    assert raw["share"].idxmax() == "DE0000000000"
    assert set(result.nulls["null"]) == {"egalitarian", "graded"}
    fig, table = plot_dumont_leadership(result)
    assert len(fig.axes) == 6 and len(table) == 2 * N_ANIMALS
    matplotlib.pyplot.close(fig)
