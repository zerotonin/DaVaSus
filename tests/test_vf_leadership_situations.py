"""Shade arrival, fence retreat and turn leadership on synthetic herds."""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from davasus.vf import fence_retreat as retreat
from davasus.vf.constants import OutsideCode
from davasus.vf.fence_approach import compute_zone_entries, detect_fence_episodes
from davasus.vf.shade_arrival import (
    compute_patch_occupancy,
    compute_shade_arrivals,
    detect_shade_episodes,
)
from davasus.vf.turn_leadership import (
    compute_correlation_sums,
    compute_pair_delays,
    compute_turn_scores,
    remove_lag_trend,
)

LOMS = ["DE01", "DE02", "DE03", "DE04"]


# ─────────────────────────────────────────────────────────────────
#  Shade
# ─────────────────────────────────────────────────────────────────


SHADE_LOMS = [f"DE{k:02d}" for k in range(12)]


def _shade_herd() -> SimpleNamespace:
    """Patch at x 0-20; heifers 0-10 walk in 2 min apart from minute 100; heifer 11 lies there all along."""
    n_minutes = 300
    minutes = pd.date_range("2024-08-10 08:00", periods=n_minutes, freq="min", tz="UTC")
    x = np.full((n_minutes, len(SHADE_LOMS)), 100.0)
    y = np.full((n_minutes, len(SHADE_LOMS)), 10.0)
    for a in range(11):
        x[100 + 2 * a:250, a] = 10.0
    x[:, 11] = 10.0
    weather = pd.DataFrame({"thi_nrc": 72.0, "rad_net_avg": 300.0}, index=minutes)
    patch = [(np.array([0.0, 20.0, 20.0, 0.0]), np.array([0.0, 0.0, 20.0, 20.0]))]
    return SimpleNamespace(minutes=minutes, loms=tuple(SHADE_LOMS), x_m=x, y_m=y, weather=weather,
                           trees={"wood": patch}, is_gps_reduced=np.zeros(n_minutes, dtype=bool))


def test_shade_arrival_order():
    herd = _shade_herd()
    occupancy = compute_patch_occupancy(herd)
    episodes = detect_shade_episodes(herd, occupancy)
    assert len(episodes) == 1 and bool(episodes["is_hot"].iloc[0])
    arrivals = compute_shade_arrivals(occupancy, episodes, np.isfinite(herd.x_m), SHADE_LOMS).set_index("lom")
    i0 = episodes["i0"].iloc[0]
    assert arrivals["departure_min"].iloc[:11].tolist() == [100 + 2 * a - i0 for a in range(11)]
    assert not arrivals.loc["DE11", "is_present"]  # already in the shade: not ranked


# ─────────────────────────────────────────────────────────────────
#  Fence retreat
# ─────────────────────────────────────────────────────────────────


def _approaches(n_episodes: int = 10) -> tuple[np.ndarray, pd.DatetimeIndex]:
    """Entries at 0, 1, 2, 3 min; DE03 leaves first, the others two minutes later."""
    period = 60
    minutes = pd.date_range("2024-08-01", periods=n_episodes * period, freq="min", tz="UTC")
    outside = np.zeros((len(minutes), len(LOMS)), dtype=np.int8)
    for e in range(n_episodes):
        t0 = e * period + 10
        for a in range(len(LOMS)):
            leave = t0 + (4 if a == 2 else 6)
            outside[t0 + a:leave, a] = OutsideCode.PRE_FENCE_ZONE
    return outside, minutes


def test_first_exit_and_follow_time():
    outside, minutes = _approaches()
    audio = np.zeros(outside.shape)
    episodes = detect_fence_episodes(outside, minutes, np.zeros(len(minutes), dtype=bool))
    entries = compute_zone_entries(outside, audio, audio, episodes, LOMS)
    exits = retreat.compute_zone_exits(outside, audio, audio, episodes, entries)
    first = exits.loc[exits.groupby("bout_id")["departure_min"].idxmin(), "lom"]
    assert set(first) == {"DE03"}
    retreats = retreat.summarise_retreats(exits, episodes)
    assert (retreats["follow_min"] == 2).all()
    assert not retreats["first_out_was_first_in"].any()


# ─────────────────────────────────────────────────────────────────
#  Turns
# ─────────────────────────────────────────────────────────────────


def _following_headings(n_minutes: int = 2000, delay: int = 1) -> np.ndarray:
    """DE01 turns at random; DE02 copies her ``delay`` minutes later, DE03 and DE04 2 delays later."""
    rng = np.random.default_rng(3)
    angle = np.cumsum(rng.normal(0.0, 0.6, n_minutes))
    headings = np.full((n_minutes, len(LOMS), 2), np.nan)
    for a, shift in enumerate((0, delay, 2 * delay, 2 * delay)):
        lagged = np.roll(angle, shift)
        headings[:, a] = np.c_[np.cos(lagged), np.sin(lagged)]
    headings[:2 * delay] = np.nan
    return headings


def test_turn_delays_find_the_leader():
    headings = _following_headings()
    sums = compute_correlation_sums(headings, np.zeros(len(headings), dtype=int))
    correlation, counts = sums.pooled()
    delay = compute_pair_delays(correlation, counts, sums.lags)
    assert delay[0, 1] == pytest.approx(1.0, abs=0.05)
    assert delay[1, 0] == pytest.approx(-1.0, abs=0.05)
    scores = compute_turn_scores(delay)
    assert np.argmax(scores) == 0 and scores[0] > 0


def test_lag_correction_and_residual():
    delay = np.zeros((3, 3))
    delay[np.diag_indices(3)] = np.nan
    lag_s = np.array([-30.0, 0.0, 30.0])
    corrected = compute_turn_scores(delay, lag_s)
    assert corrected[0] > 0 > corrected[2]  # an early collar hides a true lead
    residual = remove_lag_trend(np.array([0.1, 0.2, 0.3, 0.4]), np.array([1.0, 2.0, 3.0, 4.0]))
    assert np.allclose(residual, 0.0)
