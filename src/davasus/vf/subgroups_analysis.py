# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.subgroups_analysis                                 ║
# ║  « are there subgroups, and do real splits follow them »         ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Pair bonds are built only on days without a split, communities  ║
# ║  are found in them, and both are then tested on the splits: do   ║
# ║  bonded heifers, and members of one community, end up on the     ║
# ║  same side. A second test removes the difference in hierarchy    ║
# ║  rank first.                                                     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Subgroups from pair bonds, tested on fission events."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from davasus.vf import network_stats
from davasus.vf.constants import PERMUTATIONS, SPLIT_DISTANCE_M, SPLIT_DISTANCE_WIDE_M
from davasus.vf.herd_splits import HerdSplits, compute_together_rate, detect_herd_splits
from davasus.vf.initiation import (
    HerdTracks,
    compute_departures,
    detect_travel_bouts,
    summarise_bouts,
)
from davasus.vf.pair_bonds import (
    COMPOSITE,
    combine_bonds,
    compute_association,
    compute_departure_bonds,
    compute_pull_bonds,
    compute_turn_bonds,
    shift_to_weights,
)
from davasus.vf.pull_anchor import detect_pull_anchor_events
from davasus.vf.turn_leadership import compute_headings

log = logging.getLogger(__name__)

COMMUNITY_NAMES: str = "ABCDEFGHIJKLMNOPQRSTUVWX"


@dataclass
class SubgroupsResult:
    events: pd.DataFrame
    sides: pd.DataFrame
    pairs: pd.DataFrame
    communities: pd.DataFrame
    modularity: pd.DataFrame
    bond_tests: pd.DataFrame
    community_tests: pd.DataFrame
    summary: pd.DataFrame
    layers: dict[str, pd.DataFrame]


def run_subgroups(data, tracks: HerdTracks, hierarchy: pd.DataFrame | None, rng: np.random.Generator,
                  n_permutations: int = PERMUTATIONS) -> SubgroupsResult:
    """Bonds on split-free days, communities, and their test on the splits.

    Args:
        data: Dashboard data (minute cubes, minutes, heifers).
        tracks: Herd tracks of the same minutes.
        hierarchy: Table with ``lom`` and ``rank`` (``compute_combined_hierarchy``),
            or None to skip the rank-controlled tests.
        rng: Random generator for all permutations.
        n_permutations: Relabellings per test.
    """
    loms = list(data.loms)
    splits = detect_herd_splits(data.x_m, data.y_m, data.minutes, loms, data.is_gps_reduced)
    wide = detect_herd_splits(data.x_m, data.y_m, data.minutes, loms, data.is_gps_reduced, SPLIT_DISTANCE_WIDE_M)
    is_training = find_training_minutes(data.minutes, splits.events)
    layers = build_bond_layers(data, tracks, is_training)
    rank = None if hierarchy is None else hierarchy.set_index("lom")["rank"].reindex(loms).to_numpy(dtype=float)
    communities, modularity = find_layer_communities(layers, loms, rank, rng)
    outcomes = {SPLIT_DISTANCE_M: compute_together_rate(splits), SPLIT_DISTANCE_WIDE_M: compute_together_rate(wide)}
    rank_gap = None if rank is None else np.abs(rank[:, None] - rank[None, :])
    bond_tests = run_bond_tests(layers, outcomes, rank_gap, rng, n_permutations)
    community_tests = run_community_tests(communities, loms, outcomes, rng, n_permutations)
    pairs = build_pair_table(layers, outcomes[SPLIT_DISTANCE_M], communities, rank_gap, loms)
    summary = pd.DataFrame([{
        "n_days": int(data.minutes.floor("D").nunique()),
        "n_training_days": int(data.minutes[is_training].floor("D").nunique()),
        "n_splits": splits.n_events, "split_minutes": int(splits.events["length_min"].sum()),
        "n_splits_wide": wide.n_events, "split_minutes_wide": int(wide.events["length_min"].sum())}])
    return SubgroupsResult(splits.events, pd.DataFrame(splits.sides, columns=loms), pairs, communities, modularity,
                           bond_tests, community_tests, summary,
                           {name: pd.DataFrame(layer, index=loms, columns=loms) for name, layer in layers.items()})


# ┌────────────────────────────────────────────────────────────┐
# │ Bonds  « built on the days without a split »               │
# └────────────────────────────────────────────────────────────┘

def find_training_minutes(minutes: pd.DatetimeIndex, events: pd.DataFrame) -> np.ndarray:
    """Minutes of the days on which no split starts or ends."""
    days = minutes.floor("D")
    split_days = set(days[events["i0"].to_numpy(dtype=int)]) | set(days[events["i1"].to_numpy(dtype=int) - 1])
    return ~days.isin(list(split_days))


def build_bond_layers(data, tracks: HerdTracks, is_training: np.ndarray) -> dict[str, np.ndarray]:
    """Association, pull, departure and turn bonds from the training minutes, plus their composite."""
    loms = list(data.loms)
    is_used = is_training & ~np.asarray(data.is_gps_reduced, dtype=bool)
    pulls = detect_pull_anchor_events(tracks)
    pulls = pulls[is_training[pulls["t1"].to_numpy(dtype=int)] & is_training[pulls["t3"].to_numpy(dtype=int)]]
    bouts = detect_travel_bouts(tracks)
    departures = compute_departures(tracks, bouts)
    bouts = summarise_bouts(bouts, departures)
    kept = bouts.loc[bouts["is_group_movement"] & is_training[bouts["i0"].to_numpy(dtype=int)], "bout_id"]
    headings = compute_headings(data.x_m, data.y_m, tracks.speed, tracks.is_excluded | ~is_training)
    layers = {
        "association": compute_association(data.x_m, data.y_m, is_used),
        "pulls": compute_pull_bonds(pulls, loms),
        "departures": compute_departure_bonds(departures[departures["bout_id"].isin(kept)], loms),
        "turns": compute_turn_bonds(headings, pd.factorize(data.minutes.floor("D"))[0]),
    }
    log.info("bonds from %d training minutes: %d pull events, %d group movements",
             int(is_used.sum()), len(pulls), len(kept))
    layers[COMPOSITE] = combine_bonds(layers)
    return layers


# ┌────────────────────────────────────────────────────────────┐
# │ Communities  « per layer, named by mean rank »             │
# └────────────────────────────────────────────────────────────┘

def find_layer_communities(layers: dict[str, np.ndarray], loms: list[str], rank: np.ndarray | None,
                           rng: np.random.Generator) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Community of every heifer in every layer, and Q against shuffled weights."""
    members, quality = [], []
    for name, layer in layers.items():
        weights = shift_to_weights(layer)
        labels, _ = network_stats.find_communities(weights)
        members.append(pd.DataFrame({"layer": name, "lom": loms, "community": name_communities(labels, rank)}))
        quality.append({"layer": name, **network_stats.test_modularity(weights, rng)})
    return pd.concat(members, ignore_index=True), pd.DataFrame(quality)


def name_communities(labels: np.ndarray, rank: np.ndarray | None) -> list[str]:
    """Letters A, B, ... ordered by mean hierarchy rank (by size without a hierarchy)."""
    found = np.unique(labels)
    if rank is None:
        order = sorted(found, key=lambda c: -int((labels == c).sum()))
    else:
        order = sorted(found, key=lambda c: float(np.nanmean(rank[labels == c])))
    letter = {c: COMMUNITY_NAMES[k] for k, c in enumerate(order)}
    return [letter[c] for c in labels]


# ┌────────────────────────────────────────────────────────────┐
# │ Tests  « do the splits follow the bonds »                  │
# └────────────────────────────────────────────────────────────┘

def run_bond_tests(layers: dict[str, np.ndarray], outcomes: dict[float, tuple[np.ndarray, np.ndarray]],
                   rank_gap: np.ndarray | None, rng: np.random.Generator, n_permutations: int) -> pd.DataFrame:
    """Mantel tests of every layer against the together rate, plain and with rank gap removed."""
    rows = []
    for distance, (together, _) in outcomes.items():
        for name, layer in layers.items():
            rows.append({"split_distance_m": distance, "layer": name, "control": "none",
                         **network_stats.test_matrix_correlation(layer, together, rng, n_permutations)})
            if rank_gap is not None:
                rows.append({"split_distance_m": distance, "layer": name, "control": "rank gap",
                             **network_stats.test_matrix_correlation(layer, together, rng, n_permutations,
                                                                     control=rank_gap)})
        if rank_gap is not None:
            rows.append({"split_distance_m": distance, "layer": "rank closeness", "control": "none",
                         **network_stats.test_matrix_correlation(-rank_gap, together, rng, n_permutations)})
    return pd.DataFrame(rows)


def run_community_tests(communities: pd.DataFrame, loms: list[str],
                        outcomes: dict[float, tuple[np.ndarray, np.ndarray]], rng: np.random.Generator,
                        n_permutations: int) -> pd.DataFrame:
    rows = []
    for distance, (together, _) in outcomes.items():
        for name, members in communities.groupby("layer", sort=False):
            labels = pd.factorize(members.set_index("lom")["community"].reindex(loms))[0]
            if labels.max() < 1:
                continue
            rows.append({"split_distance_m": distance, "layer": name,
                         **network_stats.test_partition_separation(labels, together, rng, n_permutations)})
    return pd.DataFrame(rows)


def build_pair_table(layers: dict[str, np.ndarray], outcome: tuple[np.ndarray, np.ndarray],
                     communities: pd.DataFrame, rank_gap: np.ndarray | None, loms: list[str]) -> pd.DataFrame:
    """One row per pair: every bond, the together rate and whether both are in one composite community."""
    a, b = np.triu_indices(len(loms), 1)
    together, n_splits = outcome
    table = pd.DataFrame({"lom_a": np.array(loms)[a], "lom_b": np.array(loms)[b]})
    for name, layer in layers.items():
        table[name] = layer[a, b]
    table["together_rate"] = together[a, b]
    table["n_splits"] = n_splits[a, b].astype(int)
    composite = communities[communities["layer"] == COMPOSITE].set_index("lom")["community"].reindex(loms).to_numpy()
    table["same_community"] = composite[a] == composite[b]
    if rank_gap is not None:
        table["rank_gap"] = rank_gap[a, b]
    return table


def summarise_split_sides(splits: HerdSplits, loms: list[str]) -> pd.DataFrame:
    """Per heifer: splits taken part in and share of them on the smaller side."""
    largest = np.array([np.bincount(s[s >= 0]).argmax() for s in splits.sides])
    has_side = splits.sides >= 0
    is_minority = has_side & (splits.sides != largest[:, None])
    return pd.DataFrame({"lom": loms, "n_splits": has_side.sum(axis=0),
                         "minority_share": is_minority.sum(axis=0) / np.maximum(has_side.sum(axis=0), 1)})
