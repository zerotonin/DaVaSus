# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.fence_retreat                                      ║
# ║  « who turns the herd away from the fence »                      ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  For each fence approach, a heifer has retreated at the first    ║
# ║  minute of FENCE_EXIT_CLEAR_MIN fixes in a row outside the pre-  ║
# ║  fence zone. Exits go through the Dumont statistics. The first   ║
# ║  exiter is credited only if the others follow; a cue just        ║
# ║  before her exit tests whether the cue spreads as a retreat.     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Retreat order after a fence approach, and whether others follow the first."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    FENCE_EPISODE_GAP_MIN,
    FENCE_EXIT_CLEAR_MIN,
    FENCE_EXIT_CUE_MIN,
    FENCE_ZONE_CODES,
    PERMUTATIONS,
    TIE_TOLERANCE_MIN,
)
from davasus.vf.leadership_stats import test_flag_difference, test_within_group_spearman
from davasus.vf.paddock_entry import find_first_run

log = logging.getLogger(__name__)


# ┌────────────────────────────────────────────────────────────┐
# │ Exits  « first clear run outside the zone »                │
# └────────────────────────────────────────────────────────────┘


def compute_zone_exits(outside: np.ndarray, audio: np.ndarray, pulse: np.ndarray,
                       episodes: pd.DataFrame, entries: pd.DataFrame) -> pd.DataFrame:
    """Exit minute per entrant of each group approach, as Dumont departures.

    ``departure_min`` is the exit in minutes after episode onset;
    ``entry_min`` the zone entry; ``is_cued_exit`` a cue within
    ``FENCE_EXIT_CUE_MIN`` minutes before or at the exit.
    """
    cue = (np.nan_to_num(audio) > 0) | (np.nan_to_num(pulse) > 0)
    clear = (outside >= 0) & ~np.isin(outside, FENCE_ZONE_CODES)
    entered = entries[entries["has_joined"]].set_index(["bout_id", "lom"])["departure_min"]
    loms = list(entries["lom"].drop_duplicates())
    frames = []
    for episode in episodes[episodes["is_group_movement"]].itertuples():
        stop = min(episode.i1 + FENCE_EPISODE_GAP_MIN + FENCE_EXIT_CLEAR_MIN, outside.shape[0])
        window = clear[episode.i0:stop]
        rows = []
        for a, lom in enumerate(loms):
            entry = entered.get((episode.bout_id, lom), np.nan)
            if not np.isfinite(entry):
                continue
            start = int(entry) + 1
            run = find_first_run(window[start:, a], FENCE_EXIT_CLEAR_MIN)
            exit_min = start + run if np.isfinite(run) else np.nan
            is_cued = bool(np.isfinite(exit_min) and cue[episode.i0 + int(exit_min) - FENCE_EXIT_CUE_MIN:
                                                          episode.i0 + int(exit_min) + 1, a].any())
            rows.append((episode.bout_id, lom, True, np.isfinite(exit_min), exit_min, entry, is_cued))
        frames.append(pd.DataFrame(rows, columns=["bout_id", "lom", "is_present", "has_joined",
                                                  "departure_min", "entry_min", "is_cued_exit"]))
    exits = pd.concat(frames, ignore_index=True)
    log.info("fence retreat: %d exits in %d approaches", int(exits["has_joined"].sum()),
             exits["bout_id"].nunique())
    return exits


# ┌────────────────────────────────────────────────────────────┐
# │ Following  « do the others leave after the first »         │
# └────────────────────────────────────────────────────────────┘


def summarise_retreats(exits: pd.DataFrame, episodes: pd.DataFrame,
                       tie_tolerance_min: float = TIE_TOLERANCE_MIN) -> pd.DataFrame:
    """Per approach: first exiter(s), cue on her exit, how fast the rest follow.

    ``follow_min`` is the median exit of the others minus the first exit.
    """
    left = exits[exits["has_joined"]]
    rows = []
    for bout_id, g in left.groupby("bout_id"):
        first_time = g["departure_min"].min()
        is_first = g["departure_min"] <= first_time + tie_tolerance_min
        others = g.loc[~is_first, "departure_min"]
        if others.empty:
            continue
        first = g[is_first]
        first_entrant = g.loc[g["entry_min"] == g["entry_min"].min(), "lom"]
        rows.append((bout_id, ",".join(first["lom"]), bool(first["is_cued_exit"].any()),
                     float(others.median() - first_time), bool(first["lom"].isin(first_entrant).any()),
                     float(first["departure_min"].iloc[0] - first["entry_min"].min())))
    table = pd.DataFrame(rows, columns=["bout_id", "first_exiters", "first_exit_cued", "follow_min",
                                        "first_out_was_first_in", "first_exiter_dwell_min"])
    return table.merge(episodes[["bout_id", "day"]], on="bout_id")


def run_retreat_tests(exits: pd.DataFrame, retreats: pd.DataFrame, rng: np.random.Generator,
                      n_permutations: int = PERMUTATIONS) -> pd.DataFrame:
    """First in, first out; and does a cued first exit speed up the others."""
    left = exits[exits["has_joined"]]
    left = left[left.groupby("bout_id")["lom"].transform("size") >= 3]
    order = test_within_group_spearman(left, "entry_min", "departure_min", "bout_id", rng, n_permutations)
    cue = test_flag_difference(retreats["first_exit_cued"].to_numpy(), retreats["follow_min"].to_numpy(),
                               retreats["day"].to_numpy(), rng, n_permutations)
    return pd.DataFrame([
        {"test": "entry order vs exit order", **order},
        {"test": "others' follow time: cued vs uncued first exit", **cue}])


def summarise_retreat_per_heifer(exits: pd.DataFrame, retreats: pd.DataFrame) -> pd.DataFrame:
    """Per heifer: approaches left, times first out, mean dwell, how fast others follow her."""
    left = exits[exits["has_joined"]].assign(dwell_min=lambda f: f["departure_min"] - f["entry_min"])
    table = left.groupby("lom").agg(n_exits=("dwell_min", "size"), mean_dwell_min=("dwell_min", "mean"),
                                    exit_cue_rate=("is_cued_exit", "mean"))
    firsts = retreats.assign(lom=retreats["first_exiters"].str.split(",")).explode("lom")
    first = firsts.groupby("lom").agg(n_first_out=("bout_id", "size"),
                                      follow_min_when_first=("follow_min", "mean"))
    return table.join(first, how="left").fillna({"n_first_out": 0}).reset_index()
