# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.predeparture                                       ║
# ║  « who gets restless before the herd moves »                     ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Activity rise per heifer before each group movement: 240 mg     ║
# ║  ticks in the 15 min before onset minus 30-45 min before,        ║
# ║  using only minutes without a real step (< MOVE_MIN_M), so       ║
# ║  walking off does not count as restlessness. Ticks are summed    ║
# ║  per minute on the collar, so the fix timing lag does not        ║
# ║  shift them.                                                     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Pre-departure activity (Sueur et al. 2018; Anzai et al. 2026)."""

from __future__ import annotations

import logging
import warnings

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    MOVE_MIN_M,
    PERMUTATIONS,
    PREDEPARTURE_BASELINE_MIN,
    PREDEPARTURE_MIN_COVERAGE,
    PREDEPARTURE_WINDOW_MIN,
)
from davasus.vf.leadership_stats import test_within_group_spearman

log = logging.getLogger(__name__)

PROFILE_RANGE_MIN: tuple[int, int] = (-60, 15)
PROFILE_GROUPS: tuple[str, ...] = ("first movers", "joiners", "stayed")


# ┌────────────────────────────────────────────────────────────┐
# │ Activity rise  « window minus baseline per heifer »        │
# └────────────────────────────────────────────────────────────┘


def compute_step_lengths(x_m: np.ndarray, y_m: np.ndarray) -> np.ndarray:
    """Distance to the previous minute's fix; NaN at gaps."""
    step = np.full(x_m.shape, np.nan)
    step[1:] = np.hypot(np.diff(x_m, axis=0), np.diff(y_m, axis=0))
    return step


def compute_activity_rise(ticks: np.ndarray, step_m: np.ndarray, bouts: pd.DataFrame,
                          departures: pd.DataFrame, loms: list[str]) -> pd.DataFrame:
    """Activity rise before every group movement, one row per present heifer.

    Columns: bout_id, day, lom, rise (ticks/min), window and baseline means,
    has_joined, departure_min, is_first, and the within-bout rank
    ``rise_rank`` (0 = most restless, 1 = least).
    """
    still = np.where(np.isfinite(step_m), step_m < MOVE_MIN_M, False)
    group = bouts[bouts["is_group_movement"]]
    lo = -min(PREDEPARTURE_BASELINE_MIN[0], PREDEPARTURE_WINDOW_MIN[0])
    group = group[group["i0"] >= lo]
    frames = []
    for bout in group.itertuples():
        window = _masked_mean(ticks, still, bout.i0, PREDEPARTURE_WINDOW_MIN)
        baseline = _masked_mean(ticks, still, bout.i0, PREDEPARTURE_BASELINE_MIN)
        frames.append(pd.DataFrame({"bout_id": bout.bout_id, "day": bout.day, "lom": loms,
                                    "window": window, "baseline": baseline, "rise": window - baseline}))
    table = pd.concat(frames, ignore_index=True).merge(departures, on=["bout_id", "lom"], how="left")
    table = table[table["is_present"].fillna(False).astype(bool) & table["rise"].notna()].copy()
    first = table.groupby("bout_id")["departure_min"].transform("min")
    table["is_first"] = table["has_joined"].astype(bool) & (table["departure_min"] <= first + 0.5)
    rank = table.groupby("bout_id")["rise"].rank(ascending=False, method="average")
    n = table.groupby("bout_id")["rise"].transform("size")
    table["rise_rank"] = np.where(n > 1, (rank - 1.0) / (n - 1.0), np.nan)
    log.info("pre-departure: %d group movements, %d heifer rows", table["bout_id"].nunique(), len(table))
    return table.reset_index(drop=True)


def _masked_mean(ticks: np.ndarray, still: np.ndarray, i0: int, window: tuple[int, int]) -> np.ndarray:
    rows = slice(i0 + window[0], i0 + window[1])
    values = np.where(still[rows], ticks[rows], np.nan)
    coverage = np.isfinite(values).mean(axis=0)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        mean = np.nanmean(values, axis=0)
    return np.where(coverage >= PREDEPARTURE_MIN_COVERAGE, mean, np.nan)


# ┌────────────────────────────────────────────────────────────┐
# │ Tests  « does restlessness predict leaving first »         │
# └────────────────────────────────────────────────────────────┘


def test_rise_predicts_departure(table: pd.DataFrame, rng: np.random.Generator,
                                 n_permutations: int = PERMUTATIONS) -> dict[str, float]:
    """Mean within-bout Spearman of rise against departure time (joiners only).

    Negative means the restless heifers leave earlier. The null shuffles the
    rises within each bout. Also returns the first movers' mean rise rank
    (0.5 expected without an effect).
    """
    joined = table[table["has_joined"].astype(bool)]
    joined = joined[joined.groupby("bout_id")["rise"].transform("size") >= 3]
    result = test_within_group_spearman(joined, "rise", "departure_min", "bout_id", rng, n_permutations)
    first_rank = table.loc[table["is_first"], "rise_rank"]
    return {"n_bouts": int(joined["bout_id"].nunique()), **result,
            "first_mover_rise_rank": float(first_rank.mean()), "n_first_movers": int(first_rank.notna().sum())}


def compute_activity_profiles(ticks: np.ndarray, step_m: np.ndarray, table: pd.DataFrame,
                              bouts: pd.DataFrame, loms: list[str]) -> pd.DataFrame:
    """Mean still-minute ticks around onset for first movers, joiners and stayers."""
    still = np.where(np.isfinite(step_m), step_m < MOVE_MIN_M, False)
    lags = np.arange(*PROFILE_RANGE_MIN)
    onset = bouts.set_index("bout_id")["i0"]
    sums = {g: np.zeros(len(lags)) for g in PROFILE_GROUPS}
    counts = {g: np.zeros(len(lags)) for g in PROFILE_GROUPS}
    animal = {lom: a for a, lom in enumerate(loms)}
    n_minutes = ticks.shape[0]
    for row in table.itertuples():
        i = onset[row.bout_id] + lags
        ok = (i >= 0) & (i < n_minutes)
        a = animal[row.lom]
        values = np.where(still[i[ok], a], ticks[i[ok], a], np.nan)
        group = ("first movers" if row.is_first else "joiners" if row.has_joined else "stayed")
        finite = np.isfinite(values)
        sums[group][ok] += np.where(finite, values, 0.0)
        counts[group][ok] += finite
    frames = [pd.DataFrame({"group": g, "lag_min": lags,
                            "mean_ticks": np.where(counts[g] > 0, sums[g] / np.maximum(counts[g], 1), np.nan),
                            "n": counts[g].astype(int)}) for g in PROFILE_GROUPS]
    return pd.concat(frames, ignore_index=True)
