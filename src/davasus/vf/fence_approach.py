# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.fence_approach                                     ║
# ║  « who reaches the virtual fence first »                         ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  A fence episode is a run of minutes with any heifer in the      ║
# ║  pre-fence zone (Outside 5 or 6); quiet gaps up to               ║
# ║  FENCE_EPISODE_GAP_MIN are bridged. Each heifer's zone entry     ║
# ║  is a departure, so the Dumont statistics apply unchanged.       ║
# ║  The collar decides the zone itself, so no GPS threshold.        ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Fence-approach episodes, entry order and who gets cued (Keshavarzi et al. 2020)."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    FENCE_CUE_WINDOW_MIN,
    FENCE_EPISODE_GAP_MIN,
    FENCE_EPISODE_MIN_ENTRANTS,
    FENCE_ZONE_CODES,
    MIN_PRESENT_COVERAGE,
    PERMUTATIONS,
    TIE_TOLERANCE_MIN,
)
from davasus.vf.leadership_stats import (
    iterate_permutation_chunks,
    permute_within_groups,
    test_flag_difference,
)

log = logging.getLogger(__name__)

# Entry positions pooled for the cue-rate panel: first, then 2-3, 4-8, 9+.
POSITION_BINS: tuple[int, ...] = (1, 2, 4, 9)
POSITION_LABELS: tuple[str, ...] = ("1st", "2nd-3rd", "4th-8th", "9th+")
# Within-heifer cue test: entries needed both as first and as follower.
WITHIN_HEIFER_MIN_ENTRIES: int = 5


# ┌────────────────────────────────────────────────────────────┐
# │ Episodes  « herd-level runs of zone presence »             │
# └────────────────────────────────────────────────────────────┘


def detect_fence_episodes(outside: np.ndarray, minutes: pd.DatetimeIndex,
                          is_excluded: np.ndarray) -> pd.DataFrame:
    """Runs of minutes with any heifer in the zone, merged over short gaps.

    Episodes touching an excluded minute (reduced GPS) are dropped.
    """
    in_zone = np.isin(outside, FENCE_ZONE_CODES)
    hits = np.flatnonzero(in_zone.any(axis=1))
    if len(hits) == 0:
        return pd.DataFrame(columns=["bout_id", "i0", "i1", "onset_utc", "day", "length_min",
                                     "n_entrants", "is_group_movement"])
    breaks = np.diff(hits) > FENCE_EPISODE_GAP_MIN
    starts, ends = hits[np.r_[True, breaks]], hits[np.r_[breaks, True]]
    rows = []
    for i0, i1 in zip(starts, ends, strict=True):
        if np.asarray(is_excluded[i0:i1 + 1]).any():
            continue
        n_entrants = int(in_zone[i0:i1 + 1].any(axis=0).sum())
        rows.append((int(i0), int(i1), minutes[i0], minutes[i0].floor("D"), int(i1 - i0 + 1), n_entrants))
    episodes = pd.DataFrame(rows, columns=["i0", "i1", "onset_utc", "day", "length_min", "n_entrants"])
    episodes.insert(0, "bout_id", np.arange(len(episodes)))
    episodes["is_group_movement"] = episodes["n_entrants"] >= FENCE_EPISODE_MIN_ENTRANTS
    log.info("fence episodes: %d, %d with >= %d entrants", len(episodes),
             int(episodes["is_group_movement"].sum()), FENCE_EPISODE_MIN_ENTRANTS)
    return episodes


def compute_zone_entries(outside: np.ndarray, audio: np.ndarray, pulse: np.ndarray,
                         episodes: pd.DataFrame, loms: list[str]) -> pd.DataFrame:
    """Zone entry minute and cue per heifer and episode, as Dumont departures.

    ``departure_min`` is minutes after episode onset. ``is_cued``: an audio cue
    or pulse within ``FENCE_CUE_WINDOW_MIN`` of the heifer's own entry.
    """
    cue = (np.nan_to_num(audio) > 0) | (np.nan_to_num(pulse) > 0)
    in_zone = np.isin(outside, FENCE_ZONE_CODES)
    frames = []
    for episode in episodes.itertuples():
        window = slice(episode.i0, episode.i1 + 1)
        has_fix = (outside[window] >= 0).mean(axis=0) >= MIN_PRESENT_COVERAGE
        zone = in_zone[window]
        entered = zone.any(axis=0)
        entry = np.where(entered, zone.argmax(axis=0), -1)
        is_present = has_fix | entered
        is_cued = np.zeros(len(loms), dtype=bool)
        for a in np.flatnonzero(entered):
            start = episode.i0 + entry[a]
            is_cued[a] = cue[start:start + FENCE_CUE_WINDOW_MIN + 1, a].any()
        frames.append(pd.DataFrame({
            "bout_id": episode.bout_id, "lom": loms, "is_present": is_present,
            "has_joined": entered & is_present,
            "departure_min": np.where(entered, entry, np.nan).astype(float),
            "is_cued": is_cued}))
    return pd.concat(frames, ignore_index=True)


# ┌────────────────────────────────────────────────────────────┐
# │ Cues  « is the first at the fence the one who gets cued »  │
# └────────────────────────────────────────────────────────────┘


def add_entry_positions(entries: pd.DataFrame, episodes: pd.DataFrame,
                        tie_tolerance_min: float = TIE_TOLERANCE_MIN) -> pd.DataFrame:
    """Entry position (1 = first, ties share the lowest) for group episodes."""
    group = set(episodes.loc[episodes["is_group_movement"], "bout_id"])
    out = entries[entries["has_joined"] & entries["bout_id"].isin(group)].copy()
    first = out.groupby("bout_id")["departure_min"].transform("min")
    out["is_first"] = out["departure_min"] <= first + tie_tolerance_min
    out["position"] = out.groupby("bout_id")["departure_min"].rank(method="min").astype(int)
    return out


def summarise_cue_by_position(positioned: pd.DataFrame) -> pd.DataFrame:
    """Share of entrants cued per entry-position bin."""
    bins = np.digitize(positioned["position"], POSITION_BINS) - 1
    labels = np.array(POSITION_LABELS)[bins]
    table = positioned.assign(position_bin=labels).groupby("position_bin", sort=False).agg(
        n_entries=("is_cued", "size"), cue_rate=("is_cued", "mean"))
    return table.reindex(list(POSITION_LABELS)).reset_index()


def test_first_vs_later_cue(positioned: pd.DataFrame, rng: np.random.Generator,
                            n_permutations: int = PERMUTATIONS) -> dict[str, float]:
    """Cue rate of first entrants minus later entrants, against shuffled entry order.

    The null shuffles the cue flags among the entrants of each episode, so
    episodes at an aversive fence section stay aversive for everyone.
    """
    by_episode = positioned.groupby("bout_id")["is_first"]
    table = positioned[by_episode.transform("any") & ~by_episode.transform("all")]
    cued = table["is_cued"].to_numpy(dtype=float)
    first = table["is_first"].to_numpy(dtype=bool)
    groups = table["bout_id"].to_numpy()
    observed = float(_first_minus_later(cued[None, :], first)[0])
    null = np.concatenate([_first_minus_later(permute_within_groups(cued, groups, rng, size), first)
                           for size in iterate_permutation_chunks(n_permutations)])
    return {"n_episodes": int(table["bout_id"].nunique()),
            "cue_rate_first": float(cued[first].mean()),
            "cue_rate_later": float(cued[~first].mean()),
            "difference": observed, "null_mean": float(null.mean()),
            "null_lo": float(np.quantile(null, 0.025)), "null_hi": float(np.quantile(null, 0.975)),
            "p_two_sided": float((np.sum(np.abs(null - null.mean()) >= abs(observed - null.mean())) + 1.0)
                                 / (n_permutations + 1.0))}


def test_first_vs_later_within_heifer(positioned: pd.DataFrame, rng: np.random.Generator,
                                      n_permutations: int = PERMUTATIONS,
                                      min_entries: int = WITHIN_HEIFER_MIN_ENTRIES) -> dict[str, float]:
    """Each heifer's cue rate when first minus when following, sign-flip null.

    Removes identity: heifers that often arrive first may simply be the ones
    that walk into the fence. Heifers need ``min_entries`` of each kind.
    """
    table = positioned.groupby(["lom", "is_first"])["is_cued"].agg(["mean", "size"]).unstack("is_first")
    keep = (table[("size", True)] >= min_entries) & (table[("size", False)] >= min_entries)
    diffs = (table.loc[keep, ("mean", True)] - table.loc[keep, ("mean", False)]).to_numpy()
    observed = float(diffs.mean()) if len(diffs) else np.nan
    signs = rng.choice((-1.0, 1.0), size=(n_permutations, len(diffs)))
    null = (signs * diffs).mean(axis=1) if len(diffs) else np.full(n_permutations, np.nan)
    return {"n_heifers": int(keep.sum()), "n_heifers_higher_when_first": int((diffs > 0).sum()),
            "difference": observed, "null_mean": float(np.mean(null)),
            "null_lo": float(np.quantile(null, 0.025)), "null_hi": float(np.quantile(null, 0.975)),
            "p_two_sided": float((np.sum(np.abs(null) >= abs(observed)) + 1.0) / (n_permutations + 1.0))}


def _first_minus_later(cued: np.ndarray, first: np.ndarray) -> np.ndarray:
    """Row-wise mean of first entrants minus mean of later ones; cued is (n_rows, n)."""
    return cued[:, first].mean(axis=1) - cued[:, ~first].mean(axis=1)


def test_leader_cue_spread(positioned: pd.DataFrame, episodes: pd.DataFrame, rng: np.random.Generator,
                           n_permutations: int = PERMUTATIONS) -> dict[str, float]:
    """Followers' cue rate after a cued versus an uncued first entrant.

    A cue that stops the first heifer may keep the followers back (fewer
    cues); one that does not may draw them on. The null permutes the leader
    flag across episodes within a day.
    """
    rows = []
    for bout_id, g in positioned.groupby("bout_id"):
        leaders, followers = g[g["is_first"]], g[~g["is_first"]]
        if len(followers):
            rows.append((bout_id, bool(leaders["is_cued"].any()), followers["is_cued"].mean()))
    table = pd.DataFrame(rows, columns=["bout_id", "leader_cued", "follower_cue_rate"]).merge(
        episodes[["bout_id", "day"]], on="bout_id")
    result = test_flag_difference(table["leader_cued"].to_numpy(), table["follower_cue_rate"].to_numpy(),
                                  table["day"].to_numpy(), rng, n_permutations)
    return {"n_episodes": result["n"], "n_leader_cued": result["n_flagged"],
            "follower_rate_leader_cued": result["mean_flagged"],
            "follower_rate_leader_uncued": result["mean_unflagged"],
            "difference": result["difference"], "null_mean": result["null_mean"],
            "p_two_sided": result["p_two_sided"]}


def summarise_cues_per_heifer(positioned: pd.DataFrame) -> pd.DataFrame:
    """Per heifer: episodes entered, times first, cue rate when first and when following."""
    grouped = positioned.groupby("lom")
    table = grouped.agg(n_fence_entries=("is_cued", "size"), n_fence_first=("is_first", "sum"),
                        fence_cue_rate=("is_cued", "mean"))
    table["fence_cue_rate_first"] = positioned[positioned["is_first"]].groupby("lom")["is_cued"].mean()
    table["fence_cue_rate_following"] = positioned[~positioned["is_first"]].groupby("lom")["is_cued"].mean()
    return table.reset_index()
