# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.pair_bonds                                         ║
# ║  « how closely two heifers are tied, four ways »                 ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Association: how often one is among the other's nearest         ║
# ║  neighbours. Pulls: how often a pull between the two succeeds    ║
# ║  (Strandburg-Peshkin et al. 2015). Departures: how close their   ║
# ║  ranks are when the herd sets off (Dumont et al. 2005). Turns:   ║
# ║  how well their headings match at the best delay (Nagy et al.    ║
# ║  2010). All symmetric, larger = closer.                          ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Symmetric pair-bond matrices from the leadership analyses."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform

from davasus.vf.constants import ASSOCIATION_NEIGHBOURS, ASSOCIATION_STEP_MIN, PULL_BOND_MIN_EVENTS
from davasus.vf.turn_leadership import compute_correlation_sums

COMPOSITE: str = "composite"


def compute_association(x_m: np.ndarray, y_m: np.ndarray, is_used: np.ndarray,
                        n_neighbours: int = ASSOCIATION_NEIGHBOURS,
                        step_min: int = ASSOCIATION_STEP_MIN) -> np.ndarray:
    """Share of sampled minutes in which one heifer is among the other's nearest neighbours.

    Nearest-neighbour ranks do not depend on how far the herd is spread, so
    resting and grazing minutes count alike. Chance level is
    ``n_neighbours / (n - 1)``.
    """
    n = x_m.shape[1]
    has_fix = np.isfinite(x_m) & np.isfinite(y_m)
    is_near = np.zeros((n, n))
    n_seen = np.zeros((n, n))
    for t in np.flatnonzero(is_used)[::step_min]:
        animals = np.flatnonzero(has_fix[t])
        if len(animals) <= n_neighbours:
            continue
        distance = squareform(pdist(np.c_[x_m[t, animals], y_m[t, animals]]))
        np.fill_diagonal(distance, np.inf)
        nearest = np.argsort(distance, axis=1)[:, :n_neighbours]
        hits = np.zeros(distance.shape)
        np.put_along_axis(hits, nearest, 1.0, axis=1)
        is_near[np.ix_(animals, animals)] += hits
        n_seen[np.ix_(animals, animals)] += 1.0
    with np.errstate(invalid="ignore", divide="ignore"):
        rate = np.where(n_seen > 0, is_near / n_seen, np.nan)
    return _symmetrise(rate)


def compute_pull_bonds(events: pd.DataFrame, loms: list[str]) -> np.ndarray:
    """Share of the pair's pull attempts, in either direction, that the partner followed."""
    position = {lom: k for k, lom in enumerate(loms)}
    n_events = np.zeros((len(loms), len(loms)))
    n_pulls = np.zeros_like(n_events)
    a = events["initiator"].map(position).to_numpy()
    b = events["partner"].map(position).to_numpy()
    np.add.at(n_events, (a, b), 1.0)
    np.add.at(n_pulls, (a, b), (events["outcome"] == "pull").to_numpy(dtype=float))
    n_events, n_pulls = n_events + n_events.T, n_pulls + n_pulls.T
    with np.errstate(invalid="ignore", divide="ignore"):
        bond = np.where(n_events >= PULL_BOND_MIN_EVENTS, n_pulls / n_events, np.nan)
    np.fill_diagonal(bond, np.nan)
    return bond


def compute_departure_bonds(departures: pd.DataFrame, loms: list[str]) -> np.ndarray:
    """One minus the mean gap between two heifers' departure ranks (0-1) over the group movements."""
    joined = departures[departures["has_joined"]]
    times = joined.pivot(index="bout_id", columns="lom", values="departure_min").reindex(columns=loms)
    ranks = times.rank(axis=1, pct=True).to_numpy()
    gap = np.abs(ranks[:, :, None] - ranks[:, None, :])
    if not len(ranks):
        return np.full((len(loms), len(loms)), np.nan)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        bond = 1.0 - np.nanmean(gap, axis=0)
    np.fill_diagonal(bond, np.nan)
    return bond


def compute_turn_bonds(headings: np.ndarray, day_codes: np.ndarray) -> np.ndarray:
    """Peak of the heading correlation of each pair over all delays."""
    correlation, _ = compute_correlation_sums(headings, day_codes).pooled()
    bond = np.nanmax(np.where(np.isfinite(correlation), correlation, -np.inf), axis=0)
    bond[~np.isfinite(bond)] = np.nan
    np.fill_diagonal(bond, np.nan)
    return _symmetrise(bond)


def combine_bonds(layers: dict[str, np.ndarray]) -> np.ndarray:
    """Mean of the layers after z-scoring each over its pairs; a pair missing in a layer uses the rest."""
    scored = [standardise_pairs(layer) for layer in layers.values()]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        combined = np.nanmean(np.array(scored), axis=0)
    np.fill_diagonal(combined, np.nan)
    return combined


def standardise_pairs(bond: np.ndarray) -> np.ndarray:
    upper = np.triu_indices(len(bond), 1)
    values = bond[upper]
    scored = np.full(bond.shape, np.nan)
    scored[upper] = (values - np.nanmean(values)) / np.nanstd(values)
    return np.fmax(scored, scored.T)


def shift_to_weights(bond: np.ndarray) -> np.ndarray:
    """Bond minus its smallest pair value, so that it can serve as network weights (NaN -> 0)."""
    upper = np.triu_indices(len(bond), 1)
    weights = np.nan_to_num(bond - np.nanmin(bond[upper]))
    np.fill_diagonal(weights, 0.0)
    return weights


def _symmetrise(matrix: np.ndarray) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        out = np.nanmean(np.array([matrix, matrix.T]), axis=0)
    np.fill_diagonal(out, np.nan)
    return out
