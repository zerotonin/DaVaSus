"""Paddock entry, fence approach and pre-departure activity on synthetic herds."""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from davasus.vf import fence_approach as fence
from davasus.vf import predeparture as pre
from davasus.vf.constants import MOVE_NOISE_M, OutsideCode
from davasus.vf.fence_approach import (
    add_entry_positions,
    compute_zone_entries,
    detect_fence_episodes,
)
from davasus.vf.leadership_context import compute_agreement
from davasus.vf.paddock_entry import detect_paddock_entries, find_first_run
from davasus.vf.predeparture import compute_activity_rise, compute_step_lengths

LOMS = ["DE01", "DE02", "DE03", "DE04"]
ZONE = OutsideCode.PRE_FENCE_ZONE


def test_find_first_run():
    values = np.array([0, 1, 0, 1, 1, 1, 0], dtype=bool)
    assert find_first_run(values, 2) == 3
    assert find_first_run(values, 1) == 1
    assert np.isnan(find_first_run(values, 4))


# ─────────────────────────────────────────────────────────────────
#  Paddock entry
# ─────────────────────────────────────────────────────────────────


def _square(x0: float, x1: float) -> list[tuple[np.ndarray, np.ndarray]]:
    return [(np.array([x0, x1, x1, x0]), np.array([0.0, 0.0, 100.0, 100.0]))]


def _paddock_herd() -> SimpleNamespace:
    """Herd in paddock A (x < 100); B (x 100-200) opens at minute 10, heifers walk in staggered."""
    n_minutes = 120
    start = pd.Timestamp("2024-08-01 10:00", tz="UTC")
    minutes = pd.date_range(start, periods=n_minutes, freq="min")
    x = np.full((n_minutes, len(LOMS)), 50.0)
    y = np.full((n_minutes, len(LOMS)), 50.0)
    for a, delay in enumerate((5, 20, 35, 50)):
        x[10 + delay:, a] = 150.0
    x[10 + 2:10 + 4, 3] = 100.0 + MOVE_NOISE_M / 2  # boundary fixes inside B but within GPS noise
    events = pd.DataFrame({
        "event_id": [1, 2], "label": ["only A", "A and B"],
        "start_utc": [minutes[0], minutes[10]], "new_parcel": [np.nan, 2.0], "cut_off_parcel": [np.nan, np.nan],
        "manual_herding": [0, 0], "water_point_relocated": [0, 0]})
    return SimpleNamespace(
        has_paddocks=True, paddock_numbers={"A": 1, "B": 2}, paddocks={"A": _square(0, 100), "B": _square(100, 200)},
        events=events, event_paddocks=[("A",), ("A", "B")], loms=tuple(LOMS), x_m=x, y_m=y,
        n_minutes=n_minutes, minute_of=lambda t: int(minutes.get_loc(t)))


def test_paddock_entry_order_and_boundary_fix():
    switches, entries = detect_paddock_entries(_paddock_herd())
    assert switches["is_ranked"].tolist() == [True]
    assert switches["new_paddock"].iloc[0] == "B"
    assert entries.set_index("lom")["departure_min"].tolist() == [5, 20, 35, 50]
    assert switches["spread_min"].iloc[0] == 45


def test_paddock_already_open_is_not_ranked():
    herd = _paddock_herd()
    herd.event_paddocks = [("A", "B"), ("A", "B")]
    switches, _ = detect_paddock_entries(herd)
    assert not switches["is_ranked"].any()


# ─────────────────────────────────────────────────────────────────
#  Fence approach
# ─────────────────────────────────────────────────────────────────


def _fence_herd(n_episodes: int = 12) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex]:
    """Heifer 0 always reaches the zone first and is cued; the others follow uncued."""
    period = 60
    n_minutes = n_episodes * period
    minutes = pd.date_range("2024-08-01", periods=n_minutes, freq="min", tz="UTC")
    outside = np.zeros((n_minutes, len(LOMS)), dtype=np.int8)
    audio = np.zeros(outside.shape)
    for e in range(n_episodes):
        t0 = e * period + 10
        for a in range(len(LOMS)):
            outside[t0 + 2 * a:t0 + 2 * a + 3, a] = ZONE
        outside[t0 + 4, 0] = ZONE  # within the merge gap: still one episode
        audio[t0 + 1, 0] = 1
        if e % 3 == 0:
            audio[t0 + 4, 2] = 1  # a follower cued now and then, when following
    return outside, audio, minutes


def test_fence_episodes_entry_order_and_cues():
    outside, audio, minutes = _fence_herd()
    excluded = np.zeros(len(minutes), dtype=bool)
    excluded[60 * 11 + 12] = True
    episodes = detect_fence_episodes(outside, minutes, excluded)
    assert len(episodes) == 11 and episodes["is_group_movement"].all()
    entries = compute_zone_entries(outside, audio, np.zeros_like(audio), episodes, LOMS)
    first = entries[entries["departure_min"] == 0]
    assert set(first["lom"]) == {"DE01"} and first["is_cued"].all()
    positioned = add_entry_positions(entries, episodes)
    result = fence.test_first_vs_later_cue(positioned, np.random.default_rng(0), 200)
    assert result["cue_rate_first"] == 1.0 and result["difference"] > 0.8
    assert result["p_two_sided"] < 0.05


def test_within_heifer_needs_both_roles():
    outside, audio, minutes = _fence_herd()
    episodes = detect_fence_episodes(outside, minutes, np.zeros(len(minutes), dtype=bool))
    positioned = add_entry_positions(compute_zone_entries(outside, audio, np.zeros_like(audio), episodes, LOMS),
                                     episodes)
    result = fence.test_first_vs_later_within_heifer(positioned, np.random.default_rng(0), 200, min_entries=1)
    assert result["n_heifers"] == 0  # nobody is ever both first and follower here


# ─────────────────────────────────────────────────────────────────
#  Pre-departure activity
# ─────────────────────────────────────────────────────────────────


def test_restless_heifer_ranks_first_and_leaves_first():
    rng = np.random.default_rng(1)
    n_minutes, onsets = 600, [100, 250, 400, 550]
    ticks = rng.normal(50.0, 2.0, (n_minutes, len(LOMS)))
    x = np.zeros((n_minutes, len(LOMS)))
    for i0 in onsets:
        ticks[i0 - 15:i0, 0] += 40.0
        ticks[i0 - 15:i0, 1] += 20.0
    bouts = pd.DataFrame({"bout_id": range(len(onsets)), "i0": onsets, "is_group_movement": True,
                          "day": pd.to_datetime([f"2024-08-0{k + 1}" for k in range(len(onsets))], utc=True)})
    departures = pd.DataFrame([(b, lom, True, True, float(a)) for b in range(len(onsets))
                               for a, lom in enumerate(LOMS)],
                              columns=["bout_id", "lom", "is_present", "has_joined", "departure_min"])
    table = compute_activity_rise(ticks, compute_step_lengths(x, x), bouts, departures, LOMS)
    assert (table.loc[table["lom"] == "DE01", "rise_rank"] == 0).all()
    assert table.loc[table["is_first"], "lom"].unique().tolist() == ["DE01"]
    result = pre.test_rise_predicts_departure(table, rng, 200)
    assert result["mean_rho"] < -0.7 and result["first_mover_rise_rank"] == 0


def test_walking_minutes_do_not_count_as_restless():
    n_minutes = 200
    ticks = np.full((n_minutes, 2), 50.0)
    x = np.zeros((n_minutes, 2))
    ticks[85:100, 0] = 200.0
    x[85:100, 0] = np.arange(15) * 20.0  # walking off early
    bouts = pd.DataFrame({"bout_id": [0], "i0": [100], "is_group_movement": [True],
                          "day": [pd.Timestamp("2024-08-01", tz="UTC")]})
    departures = pd.DataFrame({"bout_id": 0, "lom": ["DE01", "DE02"], "is_present": True, "has_joined": True,
                               "departure_min": [-15.0, 0.0]})
    table = compute_activity_rise(ticks, compute_step_lengths(x, x), bouts, departures, ["DE01", "DE02"])
    assert table["lom"].tolist() == ["DE02"]


def test_agreement_orients_ranks_as_leading():
    per_heifer = pd.DataFrame({"lom": [f"L{k}" for k in range(6)],
                               "paddock_entry_rank_raw": np.linspace(0, 1, 6),
                               "fence_first_share_raw": np.linspace(0.3, 0.0, 6),
                               "predeparture_rise_rank": np.linspace(0, 1, 6)})
    agreement = compute_agreement(per_heifer, None).set_index(["measure_a", "measure_b"])
    assert agreement.loc[("paddock_lead", "fence_first_share"), "rho"] == pytest.approx(1.0)
    assert agreement.loc[("paddock_lead", "restlessness"), "rho"] == pytest.approx(1.0)
