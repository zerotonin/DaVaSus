# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.leadership_context                                 ║
# ║  « leading where it matters: switches, fences, departures »      ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Runs the three context analyses on one herd: entry order        ║
# ║  into a newly opened paddock, first arrival in the pre-fence     ║
# ║  zone with who gets cued, and activity rise before group         ║
# ║  movements. Timing-sensitive orders run on raw and on            ║
# ║  collar-lag-corrected times; all rankings are then compared      ║
# ║  with the earlier measures.                                      ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Leadership in three contexts, with agreement against the earlier measures."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from davasus.vf.constants import BOOTSTRAP_DRAWS, PERMUTATIONS
from davasus.vf.dumont import analyse_scenario
from davasus.vf.fence_approach import (
    add_entry_positions,
    compute_zone_entries,
    detect_fence_episodes,
    summarise_cue_by_position,
    summarise_cues_per_heifer,
    test_first_vs_later_cue,
    test_first_vs_later_within_heifer,
    test_leader_cue_spread,
)
from davasus.vf.initiation import (
    HerdTracks,
    apply_sampling_lags,
    compute_departures,
    compute_first_mover_credits,
    detect_travel_bouts,
    estimate_sampling_lags,
    summarise_bouts,
)
from davasus.vf.leadership_stats import build_normalised_ranks, test_order_w
from davasus.vf.paddock_entry import detect_paddock_entries
from davasus.vf.predeparture import (
    compute_activity_profiles,
    compute_activity_rise,
    compute_step_lengths,
    test_rise_predicts_departure,
)

log = logging.getLogger(__name__)

SCENARIOS: tuple[str, ...] = ("raw", "lag_corrected")
# Every measure oriented so that larger = more leading.
LEADING_MEASURES: dict[str, str] = {
    "dumont_share": "first mover (Dumont)",
    "formation_asymmetry": "front of formation",
    "lead_share": "pulls led",
    "paddock_lead": "into new paddock",
    "fence_first_share": "first at the fence",
    "restlessness": "restless before departure",
    "shade_first_share": "first into the shade",
    "retreat_first_share": "first away from the fence",
    "turn_lead": "turns copied (raw)",
    "turn_lead_residual": "turns copied (lag trend removed)",
}
# Per-heifer columns behind the measures above that are not already oriented.
MEASURE_SOURCES: dict[str, tuple[str, float]] = {
    "paddock_lead": ("paddock_entry_rank_raw", -1.0),
    "fence_first_share": ("fence_first_share_raw", 1.0),
    "restlessness": ("predeparture_rise_rank", -1.0),
    "shade_first_share": ("shade_first_share_raw", 1.0),
    "retreat_first_share": ("retreat_first_share_raw", 1.0),
    "turn_lead": ("turn_lead_min_raw", 1.0),
    "turn_lead_residual": ("turn_lead_min_lag_residual", 1.0),
}


@dataclass
class LeadershipContextResult:
    switches: pd.DataFrame
    paddock_entries: pd.DataFrame
    paddock_w: pd.DataFrame
    fence_episodes: pd.DataFrame
    fence_entries: pd.DataFrame
    fence_per_heifer: pd.DataFrame
    fence_nulls: pd.DataFrame
    fence_w: pd.DataFrame
    fence_cue_by_position: pd.DataFrame
    fence_cue_tests: pd.DataFrame
    predeparture: pd.DataFrame
    predeparture_tests: pd.DataFrame
    predeparture_profiles: pd.DataFrame
    per_heifer: pd.DataFrame


# ┌────────────────────────────────────────────────────────────┐
# │ Orchestration  « three contexts, one table »               │
# └────────────────────────────────────────────────────────────┘


def run_leadership_context(
    data,
    tracks: HerdTracks,
    ticks: np.ndarray,
    rng: np.random.Generator,
    n_permutations: int = PERMUTATIONS,
    n_bootstrap: int = BOOTSTRAP_DRAWS,
) -> LeadershipContextResult:
    """Run all three analyses.

    Args:
        data:   ``DashboardData`` (raw fixes, Outside codes, cues, paddocks).
        tracks: Gap-filled herd tracks for bouts and collar lags.
        ticks:  (minute, animal) IMU ticks for the activity rise.
    """
    loms = list(data.loms)
    lags = estimate_sampling_lags(tracks)
    switches, paddock_entries, paddock_w, paddock_heifer = run_paddock_entry(data, lags, rng, n_permutations)
    fence = run_fence_approach(data, lags, rng, n_permutations, n_bootstrap)
    pre = run_predeparture(data, tracks, ticks, rng, n_permutations, n_bootstrap)
    per_heifer = (pd.DataFrame({"lom": loms})
                  .merge(paddock_heifer, on="lom", how="left")
                  .merge(fence["per_heifer_wide"], on="lom", how="left")
                  .merge(pre["per_heifer"], on="lom", how="left")
                  .merge(lags[["lom", "lag_s"]], on="lom", how="left"))
    return LeadershipContextResult(
        switches, paddock_entries, paddock_w, fence["episodes"], fence["entries"], fence["per_animal"],
        fence["nulls"], fence["w"], fence["by_position"], fence["cue_tests"],
        pre["table"], pre["tests"], pre["profiles"], per_heifer)


# ┌────────────────────────────────────────────────────────────┐
# │ Paddock entry  « few switches, so ranks and W only »       │
# └────────────────────────────────────────────────────────────┘


def run_paddock_entry(data, lags: pd.DataFrame, rng: np.random.Generator,
                      n_permutations: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    switches, entries = detect_paddock_entries(data)
    loms = list(data.loms)
    bouts = switches.rename(columns={"event_id": "bout_id"})
    ranked = bouts.loc[bouts["is_ranked"], "bout_id"]
    w_rows, heifer_rows = [], []
    for scenario, frame in (("raw", entries), ("lag_corrected", apply_sampling_lags(entries, lags))):
        frame = frame[frame["bout_id"].isin(ranked)]
        if frame.empty:
            continue
        ranks = build_normalised_ranks(frame, loms)
        each_own_block = pd.Series(ranks.index, index=ranks.index)
        if len(ranks) >= 2:
            w_rows.append({**test_order_w(ranks, each_own_block, rng, n_permutations),
                           "n_switches": len(ranks), "scenario": scenario})
        credits, _ = compute_first_mover_credits(frame, bouts)
        heifer_rows.append(pd.DataFrame({
            "lom": loms, "scenario": scenario,
            "paddock_entry_rank": ranks.mean(axis=0).reindex(loms).to_numpy(),
            "n_switches_ranked": ranks.notna().sum(axis=0).reindex(loms).to_numpy(),
            "paddock_first_credit": credits.groupby("lom")["credit"].sum().reindex(loms).to_numpy()}))
    heifer = pd.concat(heifer_rows, ignore_index=True) if heifer_rows else pd.DataFrame(columns=["lom"])
    wide = widen_by_scenario(heifer, ["paddock_entry_rank", "n_switches_ranked", "paddock_first_credit"])
    return switches, entries, pd.DataFrame(w_rows), wide


# ┌────────────────────────────────────────────────────────────┐
# │ Fence approach  « Dumont statistics on zone entries »      │
# └────────────────────────────────────────────────────────────┘


def run_fence_approach(data, lags: pd.DataFrame, rng: np.random.Generator,
                       n_permutations: int, n_bootstrap: int) -> dict[str, pd.DataFrame]:
    loms = list(data.loms)
    episodes = detect_fence_episodes(data.outside, data.minutes, data.is_gps_reduced)
    entries = compute_zone_entries(data.outside, data.audio, data.pulse, episodes, loms)
    per_animal, nulls, w = [], [], []
    for scenario, frame in (("raw", entries), ("lag_corrected", apply_sampling_lags(entries, lags))):
        animal, null, _, order_w = analyse_scenario(frame, episodes, loms, rng, n_permutations, n_bootstrap)
        per_animal.append(animal.assign(scenario=scenario))
        nulls.append(null.assign(scenario=scenario))
        w.append(order_w.assign(scenario=scenario))
    per_animal = pd.concat(per_animal, ignore_index=True)
    positioned = add_entry_positions(entries, episodes)
    cue_tests = pd.DataFrame([
        {"test": "first vs later entrants", **test_first_vs_later_cue(positioned, rng, n_permutations)},
        {"test": "first vs following, within heifer",
         **test_first_vs_later_within_heifer(positioned, rng, n_permutations)},
        {"test": "followers after cued vs uncued leader",
         **test_leader_cue_spread(positioned, episodes, rng, n_permutations)}])
    shares = per_animal[["lom", "scenario", "share", "share_lo", "share_hi", "expected_share"]].rename(
        columns={"share": "fence_first_share", "share_lo": "fence_first_share_lo",
                 "share_hi": "fence_first_share_hi", "expected_share": "fence_expected_share"})
    wide = widen_by_scenario(shares, ["fence_first_share", "fence_first_share_lo", "fence_first_share_hi",
                           "fence_expected_share"]).merge(summarise_cues_per_heifer(positioned), on="lom",
                                                          how="left")
    return {"episodes": episodes, "entries": entries, "per_animal": per_animal,
            "nulls": pd.concat(nulls, ignore_index=True), "w": pd.concat(w, ignore_index=True),
            "by_position": summarise_cue_by_position(positioned), "cue_tests": cue_tests,
            "per_heifer_wide": wide}


# ┌────────────────────────────────────────────────────────────┐
# │ Pre-departure  « restlessness per group movement »         │
# └────────────────────────────────────────────────────────────┘


def run_predeparture(data, tracks: HerdTracks, ticks: np.ndarray, rng: np.random.Generator,
                     n_permutations: int, n_bootstrap: int) -> dict[str, pd.DataFrame]:
    loms = list(data.loms)
    bouts = detect_travel_bouts(tracks)
    departures = compute_departures(tracks, bouts)
    bouts = summarise_bouts(bouts, departures)
    step = compute_step_lengths(data.x_m, data.y_m)
    table = compute_activity_rise(ticks, step, bouts, departures, loms)
    ranks = table.pivot_table(index="bout_id", columns="lom", values="rise_rank").reindex(columns=loms)
    days = table.groupby("bout_id")["day"].first()
    w = test_order_w(ranks, days, rng, n_permutations)
    tests = pd.DataFrame([{"test": "rise vs departure time", **test_rise_predicts_departure(table, rng,
                                                                                            n_permutations)},
                          {"test": "W of rise ranks", **w, "n_bouts": len(ranks)}])
    lo, hi = bootstrap_mean_by_day(table, "rise_rank", rng, n_bootstrap, loms)
    per_heifer = table.groupby("lom").agg(rise_rank=("rise_rank", "mean"), rise_ticks=("rise", "mean"),
                                          n_departures=("rise", "size")).reindex(loms)
    per_heifer["rise_rank_lo"], per_heifer["rise_rank_hi"] = lo, hi
    per_heifer = per_heifer.add_prefix("predeparture_").rename_axis("lom").reset_index()
    return {"table": table, "tests": tests, "per_heifer": per_heifer,
            "profiles": compute_activity_profiles(ticks, step, table, bouts, loms)}


def bootstrap_mean_by_day(table: pd.DataFrame, column: str, rng: np.random.Generator, n_draws: int,
                          loms: list[str]) -> tuple[np.ndarray, np.ndarray]:
    """95 % CI of each heifer's mean ``column`` with days resampled."""
    days = table["day"].unique()
    by_day = dict(list(table.groupby("day")))
    draws = np.full((n_draws, len(loms)), np.nan)
    for k in range(n_draws):
        sample = pd.concat([by_day[d] for d in rng.choice(days, len(days))], ignore_index=True)
        draws[k] = sample.groupby("lom")[column].mean().reindex(loms).to_numpy()
    return np.nanquantile(draws, 0.025, axis=0), np.nanquantile(draws, 0.975, axis=0)


# ┌────────────────────────────────────────────────────────────┐
# │ Agreement  « does one heifer lead everywhere »             │
# └────────────────────────────────────────────────────────────┘


def compute_agreement(per_heifer: pd.DataFrame, comparison: pd.DataFrame | None) -> pd.DataFrame:
    """Spearman rho and p between all leading measures (raw times), long form.

    Ranks are flipped so that a larger value always means more leading.
    """
    table = per_heifer.copy()
    for measure, (column, sign) in MEASURE_SOURCES.items():
        if column in table:
            table[measure] = sign * table[column]
    if comparison is not None:
        table = table.merge(comparison, on="lom", how="left")
    measures = [m for m in LEADING_MEASURES if m in table and table[m].notna().sum() >= 5]
    rows = []
    for a in measures:
        for b in measures:
            rho, p = spearmanr(table[a], table[b], nan_policy="omit")
            rows.append({"measure_a": a, "measure_b": b, "rho": float(rho), "p": float(p),
                         "n": int((table[a].notna() & table[b].notna()).sum())})
    return pd.DataFrame(rows)


def widen_by_scenario(table: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """One row per heifer, ``<column>_<scenario>``."""
    if table.empty or "scenario" not in table:
        return pd.DataFrame(columns=["lom"])
    wide = table.pivot_table(index="lom", columns="scenario", values=columns, dropna=False)
    wide.columns = [f"{column}_{scenario}" for column, scenario in wide.columns]
    return wide.reset_index()
