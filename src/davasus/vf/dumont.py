# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dumont                                             ║
# ║  « one call from tracks to a leadership verdict »                ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Runs the Dumont (2005) analysis end to end: bouts, departures,  ║
# ║  first-place credits, both nulls, Plackett-Luce worths, W and    ║
# ║  attempts, once on raw departures and once with collar lags      ║
# ║  removed. Returns tables only, no plotting.                      ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Orchestrate the Dumont leadership analysis (handover 4.3.1)."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    BOOTSTRAP_DRAWS,
    PERMUTATIONS,
    TIE_FRACTION_LIMIT,
    TIE_TOLERANCE_MIN,
)
from davasus.vf.initiation import (
    HerdTracks,
    apply_sampling_lags,
    compute_departures,
    compute_first_mover_credits,
    detect_initiation_attempts,
    detect_travel_bouts,
    estimate_sampling_lags,
    summarise_bouts,
)
from davasus.vf.leadership_stats import (
    CreditMatrix,
    bootstrap_plackett_luce,
    bootstrap_shares_by_day,
    build_credit_matrix,
    build_normalised_ranks,
    build_order_stages,
    build_top1_stages,
    compute_leader_shares,
    fit_plackett_luce,
    test_egalitarian_null,
    test_graded_null,
    test_order_w,
    test_rate_heterogeneity,
)

log = logging.getLogger(__name__)

SCENARIOS: tuple[str, ...] = ("raw", "lag_corrected")


@dataclass(frozen=True)
class DumontResult:
    """All tables of one run; ``per_animal`` and ``nulls`` hold both scenarios."""

    bouts: pd.DataFrame
    departures: pd.DataFrame
    lags: pd.DataFrame
    per_bout: pd.DataFrame
    per_animal: pd.DataFrame
    nulls: pd.DataFrame
    order_w: pd.DataFrame
    attempts: pd.DataFrame
    attempt_tests: pd.DataFrame


def run_dumont_analysis(
    tracks: HerdTracks,
    rng: np.random.Generator,
    n_permutations: int = PERMUTATIONS,
    n_bootstrap: int = BOOTSTRAP_DRAWS,
) -> DumontResult:
    bouts = detect_travel_bouts(tracks)
    raw = compute_departures(tracks, bouts)
    bouts = summarise_bouts(bouts, raw)
    lags = estimate_sampling_lags(tracks)
    log.info("%d travel bouts, %d group movements", len(bouts), int(bouts["is_group_movement"].sum()))
    scenario_departures = {"raw": raw, "lag_corrected": apply_sampling_lags(raw, lags)}
    per_animal, nulls, per_bout, order_w = [], [], [], []
    for name, departures in scenario_departures.items():
        result = analyse_scenario(departures, bouts, tracks.loms, rng, n_permutations, n_bootstrap)
        for table, store in zip(result, (per_animal, nulls, per_bout, order_w), strict=True):
            store.append(table.assign(scenario=name))
    attempts = detect_initiation_attempts(tracks, rng)
    attempt_tests = test_attempt_heterogeneity(attempts, rng, n_permutations)
    per_animal_table = pd.concat(per_animal, ignore_index=True).merge(
        summarise_attempts(attempts, tracks), on="lom", how="left").merge(
        lags[["lom", "lag_s"]], on="lom", how="left")
    return DumontResult(bouts, raw, lags, pd.concat(per_bout, ignore_index=True), per_animal_table,
                        pd.concat(nulls, ignore_index=True), pd.concat(order_w, ignore_index=True),
                        attempts, attempt_tests)


def analyse_scenario(
    departures: pd.DataFrame,
    bouts: pd.DataFrame,
    loms: list[str],
    rng: np.random.Generator,
    n_permutations: int,
    n_bootstrap: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Shares, nulls, worths and W for one set of departure times."""
    credits, per_bout = compute_first_mover_credits(departures, bouts)
    matrix = build_credit_matrix(credits)
    boot = bootstrap_shares_by_day(matrix, rng, n_bootstrap)
    egalitarian, per_animal = test_egalitarian_null(matrix, rng, n_permutations)
    graded, q = test_graded_null(matrix, rng)
    graded["graded_q"] = q
    per_animal["share_lo"] = np.nanquantile(boot, 0.025, axis=0)
    per_animal["share_hi"] = np.nanquantile(boot, 0.975, axis=0)
    per_animal["n_events_present"] = np.isfinite(matrix.credit).sum(axis=0)
    per_animal["first_credit"] = np.nansum(matrix.credit, axis=0)
    per_animal["expected_share"] = np.nanmean(np.where(np.isfinite(matrix.credit),
                                                       1.0 / np.isfinite(matrix.credit).sum(axis=1)[:, None],
                                                       np.nan), axis=0)
    clear = per_bout.loc[per_bout["is_clear_lead"], "first_lom"].value_counts()
    per_animal["n_clear_first"] = per_animal["lom"].map(clear).fillna(0).astype(int)
    per_animal = per_animal.join(fit_worths(matrix, departures, bouts, rng, n_bootstrap))
    ranks = build_normalised_ranks(departures[departures["bout_id"].isin(matrix_bouts(credits))], loms)
    days = bouts.set_index("bout_id")["day"]
    w = pd.DataFrame([test_order_w(ranks, days, rng, n_permutations)])
    w["tie_fraction"] = float((per_bout["n_tied"] > 1).mean())
    w["uses_partial_ranking"] = w["tie_fraction"] > TIE_FRACTION_LIMIT
    log.info("scenario: top share %.3f, ties %.0f %%, W %.3f", compute_leader_shares(matrix).max(),
             100 * w["tie_fraction"].iloc[0], w["w"].iloc[0])
    return per_animal, pd.concat([egalitarian, graded], ignore_index=True), per_bout, w


def matrix_bouts(credits: pd.DataFrame) -> np.ndarray:
    return credits["bout_id"].unique()


def fit_worths(matrix: CreditMatrix, departures: pd.DataFrame, bouts: pd.DataFrame,
               rng: np.random.Generator, n_bootstrap: int) -> pd.DataFrame:
    """Plackett-Luce worths (top-1 and full order) with day-block bootstrap CIs."""
    group_ids = set(bouts.loc[bouts["is_group_movement"], "bout_id"])
    group_departures = departures[departures["bout_id"].isin(group_ids)]
    day_of = bouts.set_index("bout_id")["day"]
    top1 = build_top1_stages(matrix)
    top1_by_day = _group_by_day(top1, pd.factorize(matrix.day_codes)[0])
    order_by_day: dict[int, list] = {}
    for code, (_, frame) in enumerate(group_departures.groupby(group_departures["bout_id"].map(day_of))):
        order_by_day[code] = build_order_stages(frame, matrix.loms, TIE_TOLERANCE_MIN)
    all_order = [s for stages in order_by_day.values() for s in stages]
    n = len(matrix.loms)
    n_draws = max(n_bootstrap // 4, 100)
    out = pd.DataFrame(index=range(n))
    for name, stages, by_day in (("pl_top1", top1, top1_by_day), ("pl_order", all_order, order_by_day)):
        draws = bootstrap_plackett_luce(by_day, n, rng, n_draws)
        out[name] = fit_plackett_luce(stages, n)
        out[f"{name}_lo"] = np.quantile(draws, 0.025, axis=0)
        out[f"{name}_hi"] = np.quantile(draws, 0.975, axis=0)
    return out


def _group_by_day(stages: list, day_codes: np.ndarray) -> dict[int, list]:
    by_day: dict[int, list] = {}
    for stage, code in zip(stages, day_codes, strict=True):
        by_day.setdefault(int(code), []).append(stage)
    return by_day


def summarise_attempts(attempts: pd.DataFrame, tracks: HerdTracks) -> pd.DataFrame:
    """Attempts per tracked day, share followed, share growing into a group movement."""
    days_tracked = pd.Series(np.isfinite(tracks.x_m).sum(axis=0) / 1440.0, index=tracks.loms)
    summary = attempts.groupby("lom").agg(n_attempts=("is_followed", "size"),
                                          n_followed=("is_followed", "sum"),
                                          n_group_success=("is_group_success", "sum"),
                                          mean_followers=("n_followers", "mean"),
                                          mean_followers_chance=("n_followers_chance", "mean"))
    summary = summary.reindex(tracks.loms, fill_value=0)
    summary["attempts_per_day"] = summary["n_attempts"] / days_tracked
    summary["follow_rate"] = summary["n_followed"] / summary["n_attempts"].replace(0, np.nan)
    summary["group_success_rate"] = summary["n_group_success"] / summary["n_attempts"].replace(0, np.nan)
    return summary.rename_axis("lom").reset_index()


def test_attempt_heterogeneity(attempts: pd.DataFrame, rng: np.random.Generator,
                               n_permutations: int) -> pd.DataFrame:
    """Do heifers differ in follow and group-success rate beyond chance?"""
    rows = []
    for outcome in ("is_followed", "is_group_success"):
        result = test_rate_heterogeneity(attempts["lom"], attempts[outcome], attempts["day"],
                                         rng, n_permutations)
        rows.append({"outcome": outcome, **result})
    rows.append({"outcome": "followers_vs_chance",
                 "sd_rate": float(attempts["n_followers"].mean()),
                 "null_mean": float(attempts["n_followers_chance"].mean()),
                 "null_hi": float(attempts["n_followers_chance"].quantile(0.975)),
                 "p": float("nan")})
    return pd.DataFrame(rows)
