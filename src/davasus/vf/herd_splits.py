# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.herd_splits                                        ║
# ║  « when the herd falls into two, who stands with whom »          ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Each minute, fixes within SPLIT_DISTANCE_M of each other chain  ║
# ║  into one group. A split is a run of minutes with at least two   ║
# ║  groups of SPLIT_MIN_GROUP heifers. Each heifer's side is the    ║
# ║  group she is in for most of the split; runs close in time with  ║
# ║  the same sides are one event.                                   ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Fission events of the herd and the side every heifer takes."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.sparse.csgraph import connected_components
from scipy.spatial.distance import pdist, squareform

from davasus.vf.constants import (
    SPLIT_DISTANCE_M,
    SPLIT_MERGE_AGREEMENT,
    SPLIT_MERGE_GAP_MIN,
    SPLIT_MIN_FIXES,
    SPLIT_MIN_GROUP,
    SPLIT_MIN_LENGTH_MIN,
)

log = logging.getLogger(__name__)

NO_SIDE: int = -1
EVENT_COLUMNS: list[str] = ["event_id", "i0", "i1", "onset_utc", "day", "length_min", "n_groups",
                            "group_sizes", "minority"]


@dataclass(frozen=True)
class HerdSplits:
    """Split events and, per event and heifer, the side taken (NO_SIDE: none)."""

    events: pd.DataFrame
    sides: np.ndarray

    @property
    def n_events(self) -> int:
        return len(self.events)


# ┌────────────────────────────────────────────────────────────┐
# │ Detection  « groups per minute, runs, merged events »      │
# └────────────────────────────────────────────────────────────┘

def detect_herd_splits(x_m: np.ndarray, y_m: np.ndarray, minutes: pd.DatetimeIndex, loms: list[str],
                       is_excluded: np.ndarray, distance_m: float = SPLIT_DISTANCE_M) -> HerdSplits:
    """Split events from positions of shape (minute, animal)."""
    groups = label_groups(x_m, y_m, is_excluded, distance_m)
    is_split = count_large_groups(groups) >= 2
    runs = [(i0, i1, find_sides(groups[i0:i1])) for i0, i1 in find_true_runs(is_split)
            if i1 - i0 >= SPLIT_MIN_LENGTH_MIN]
    merged = merge_runs(runs)
    rows = [describe_split(k, i0, i1, sides, minutes, loms) for k, (i0, i1, sides) in enumerate(merged)]
    events = pd.DataFrame(rows, columns=EVENT_COLUMNS)
    sides = np.array([s for _, _, s in merged]).reshape(len(merged), len(loms))
    log.info("herd splits at %g m: %d events, %d min", distance_m, len(events), int(events["length_min"].sum()))
    return HerdSplits(events, sides)


def label_groups(x_m: np.ndarray, y_m: np.ndarray, is_excluded: np.ndarray, distance_m: float) -> np.ndarray:
    """Group label per minute and animal by single linkage; NO_SIDE without a fix or in unusable minutes."""
    has_fix = np.isfinite(x_m) & np.isfinite(y_m)
    is_usable = (has_fix.sum(axis=1) >= SPLIT_MIN_FIXES) & ~np.asarray(is_excluded, dtype=bool)
    groups = np.full(x_m.shape, NO_SIDE, dtype=int)
    for t in np.flatnonzero(is_usable):
        animals = np.flatnonzero(has_fix[t])
        is_linked = squareform(pdist(np.c_[x_m[t, animals], y_m[t, animals]])) <= distance_m
        groups[t, animals] = connected_components(is_linked, directed=False)[1]
    return groups


def count_large_groups(groups: np.ndarray) -> np.ndarray:
    """Number of groups with at least SPLIT_MIN_GROUP heifers, per minute."""
    counts = np.zeros(len(groups), dtype=int)
    for t in np.flatnonzero((groups != NO_SIDE).any(axis=1)):
        counts[t] = int((np.bincount(groups[t][groups[t] != NO_SIDE]) >= SPLIT_MIN_GROUP).sum())
    return counts


def find_true_runs(flags: np.ndarray) -> list[tuple[int, int]]:
    """Half-open (start, stop) index pairs of the runs of True."""
    edges = np.diff(np.r_[0, np.asarray(flags, dtype=int), 0])
    return list(zip(np.flatnonzero(edges == 1), np.flatnonzero(edges == -1), strict=True))


def find_sides(groups: np.ndarray) -> np.ndarray:
    """Most frequent group of each heifer over a run; NO_SIDE if absent or in a group below the minimum."""
    sides = np.full(groups.shape[1], NO_SIDE, dtype=int)
    for a in range(groups.shape[1]):
        seen = groups[:, a][groups[:, a] != NO_SIDE]
        if len(seen):
            sides[a] = np.bincount(seen).argmax()
    sizes = np.bincount(sides[sides != NO_SIDE])
    is_small = (sides != NO_SIDE) & (sizes[np.maximum(sides, 0)] < SPLIT_MIN_GROUP)
    sides[is_small] = NO_SIDE
    return sides


def merge_runs(runs: list[tuple[int, int, np.ndarray]]) -> list[tuple[int, int, np.ndarray]]:
    """Join runs at most SPLIT_MERGE_GAP_MIN apart whose sides agree; the first run's sides are kept."""
    merged: list[tuple[int, int, np.ndarray]] = []
    for i0, i1, sides in runs:
        if merged and i0 - merged[-1][1] <= SPLIT_MERGE_GAP_MIN \
                and compute_side_agreement(merged[-1][2], sides) >= SPLIT_MERGE_AGREEMENT:
            merged[-1] = (merged[-1][0], i1, merged[-1][2])
        else:
            merged.append((i0, i1, sides))
    return merged


def compute_side_agreement(first: np.ndarray, second: np.ndarray) -> float:
    """Share of pairs that are together in both runs or apart in both."""
    has_both = (first != NO_SIDE) & (second != NO_SIDE)
    is_pair = has_both[:, None] & has_both[None, :] & ~np.eye(len(first), dtype=bool)
    if not is_pair.any():
        return 0.0
    same_first = first[:, None] == first[None, :]
    same_second = second[:, None] == second[None, :]
    return float((same_first == same_second)[is_pair].mean())


def describe_split(event_id: int, i0: int, i1: int, sides: np.ndarray, minutes: pd.DatetimeIndex,
                   loms: list[str]) -> tuple:
    sizes = np.sort(np.bincount(sides[sides != NO_SIDE]))[::-1]
    sizes = sizes[sizes > 0]
    largest = np.bincount(sides[sides != NO_SIDE]).argmax()
    minority = [lom for lom, side in zip(loms, sides, strict=True) if side not in (NO_SIDE, largest)]
    return (event_id, int(i0), int(i1), minutes[i0], minutes[i0].floor("D"), int(i1 - i0), len(sizes),
            "+".join(str(s) for s in sizes), ",".join(minority))


# ┌────────────────────────────────────────────────────────────┐
# │ Pairs  « how often two heifers take the same side »        │
# └────────────────────────────────────────────────────────────┘

def compute_together_rate(splits: HerdSplits) -> tuple[np.ndarray, np.ndarray]:
    """Share of splits in which a pair stood on the same side, and the number of splits both took part in."""
    has_side = splits.sides != NO_SIDE
    both = (has_side[:, :, None] & has_side[:, None, :]).sum(axis=0).astype(float)
    same = ((splits.sides[:, :, None] == splits.sides[:, None, :])
            & has_side[:, :, None] & has_side[:, None, :]).sum(axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        rate = np.where(both > 0, same / both, np.nan)
    np.fill_diagonal(rate, np.nan)
    return rate, both
