"""Fence interactions, learning measures, fence moves and social cues on synthetic herds."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from davasus.vf import learning_journey as journey
from davasus.vf.constants import LEARNING_BLOCK, OutsideCode
from davasus.vf.fence_interactions import (
    detect_interactions,
    find_criterion,
    find_untimed_fixes,
    fit_logistic_slope,
    summarise_exposure,
    summarise_learning,
)
from davasus.vf.social_cues import (
    compute_sociability,
    find_social_events,
    flag_first_cues,
    summarise_social_response,
)

LOMS = ["DE01", "DE02", "DE03"]
ZONE = OutsideCode.PRE_FENCE_ZONE


def _minutes(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-08-01", periods=n, freq="min", tz="UTC")


def test_interactions_merge_close_minutes_and_skip_untimed_fixes():
    n = 100
    audio = np.zeros((n, 3))
    pulse = np.zeros((n, 3))
    audio[[10, 12, 20], 0] = [2, 1, 3]  # 10 and 12 are one interaction, 20 another
    pulse[12, 0] = 1
    audio[50, 1] = 40  # arrives on the first fix after a gap
    has_fix = np.ones((n, 3), dtype=bool)
    has_fix[30:50, 1] = False
    is_untimed = find_untimed_fixes(has_fix)
    assert is_untimed[50, 1] and is_untimed.sum() == 1
    interactions = detect_interactions(audio, pulse, is_untimed, _minutes(n), LOMS)
    assert interactions["lom"].tolist() == ["DE01", "DE01"]
    assert interactions["audio"].tolist() == [3.0, 3.0]
    assert interactions["is_pulsed"].tolist() == [True, False]
    outside = np.where(has_fix, 0, -1).astype(np.int8)
    exposure = summarise_exposure(interactions, audio, pulse, outside, is_untimed, LOMS, 1.0).set_index("lom")
    assert exposure.loc["DE02", "audio_total"] == 40 and exposure.loc["DE02", "untimed_audio_share"] == 1.0
    assert exposure.loc["DE01", "pulsed_share"] == 0.5


def test_learning_measures_follow_a_falling_pulse_rate():
    pulsed = np.r_[np.ones(30), np.zeros(70)]
    table = pd.DataFrame({"lom": "DE01", "number": np.arange(1, 101), "is_pulsed": pulsed.astype(bool),
                          "pulse": pulsed, "audio": 1.0})
    row = summarise_learning(table).iloc[0]
    assert row["early_pulsed_share"] == 1.0 and row["late_pulsed_share"] == 0.0
    assert row["learning_slope"] < 0
    assert row["interactions_to_criterion"] == 30 - 0.2 * LEARNING_BLOCK + LEARNING_BLOCK
    assert row["cost_to_learn"] == 30
    assert np.isnan(find_criterion(np.ones(50), np.ones(50), LEARNING_BLOCK, 0.8)["cost_to_learn"])
    assert fit_logistic_slope(np.linspace(0, 1, 50), np.zeros(50)) == pytest.approx(0.0, abs=1e-6)


def test_fence_move_compares_both_sides_once_per_change():
    start = pd.Timestamp("2024-08-10 12:00", tz="UTC")
    events = pd.DataFrame({"event_id": [1, 2, 3], "label": ["a", "b step", "b"],
                           "start_utc": [start - pd.Timedelta(days=5), start, start + pd.Timedelta(hours=6)],
                           "new_parcel": [np.nan, 3.0, 3.0], "is_transition": [0, 1, 0]})
    onset = [start - pd.Timedelta(hours=h) for h in (1, 2, 3)] + [start + pd.Timedelta(hours=h) for h in (1, 2, 3, 4)]
    interactions = pd.DataFrame({"lom": "DE01", "onset_utc": onset, "audio": 1.0,
                                 "is_pulsed": [False, False, False, True, True, True, False]})
    moves = journey.compare_fence_moves(interactions, events)
    assert set(moves["event_id"]) == {2}
    row = journey.summarise_fence_moves(moves).iloc[0]
    assert row["pulsed_share_before_move"] == 0.0 and row["pulsed_share_after_move"] == 0.75
    assert row["pulsed_share_jump"] == 0.75


def _fence_line(n_visits: int = 30) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """DE01 and DE02 stand together in the zone; DE01 is cued in even visits and DE02 then leaves."""
    period = 40
    n = n_visits * period
    outside = np.zeros((n, 3), dtype=np.int8)
    is_cue = np.zeros((n, 3), dtype=bool)
    for v in range(n_visits):
        t0 = v * period + 5
        outside[t0:t0 + 25, :2] = ZONE
        if v % 2 == 0:
            is_cue[t0 + 2, 0] = True
            outside[t0 + 3:t0 + 25, 1] = 0
    x = np.tile(np.array([0.0, 5.0, 500.0]), (n, 1))
    return outside, is_cue, x, np.zeros_like(x)


def test_mate_cue_makes_the_neighbour_retreat():
    outside, is_cue, x, y = _fence_line()
    events = find_social_events(outside, is_cue, np.zeros(x.shape), x, y, np.zeros(len(x), dtype=bool), LOMS)
    assert set(events["lom"]) <= {"DE01", "DE02"}  # DE03 is never in the zone
    response = summarise_social_response(events, LOMS).set_index("lom")
    assert response.loc["DE02", "retreat_mate_cued"] == 1.0
    assert response.loc["DE02", "retreat_quiet"] < 0.5
    assert response.loc["DE02", "retreat_gain"] > 0.5
    result = journey.test_paired_shift(np.array([0.3, 0.2, 0.4, 0.25, 0.35, 0.3, 0.2, 0.3]),
                                       np.random.default_rng(0), 2000)
    assert result["n_positive"] == 8 and result["p_two_sided"] < 0.05


def test_first_cues_sociability_and_strategies():
    is_cue = np.zeros((40, 3), dtype=bool)
    is_cue[10, 0] = is_cue[12, 1] = is_cue[30, 1] = True
    interactions = pd.DataFrame({"lom": ["DE01", "DE02", "DE02"], "i0": [10, 12, 30]})
    assert flag_first_cues(interactions, is_cue, LOMS).tolist() == [True, False, True]
    x = np.tile(np.array([0.0, 2.0, 60.0]), (50, 1))
    social = compute_sociability(x, np.zeros_like(x), np.ones(50, dtype=bool), LOMS).set_index("lom")
    assert social.loc["DE03", "nearest_neighbour_m"] == 58.0
    assert social.loc["DE01", "nearest_neighbour_m"] == 2.0
    table = pd.DataFrame({"interactions_per_day": [16.0, 12.0, 2.0, 1.5], "pulsed_share": [0.1, 0.15, 0.7, 0.6]})
    strategies = journey.classify_strategies(table)
    assert strategies[0] == strategies[1] == journey.STRATEGIES[0]
    assert strategies[2] == strategies[3] == journey.STRATEGIES[3]
