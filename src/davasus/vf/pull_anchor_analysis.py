# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.pull_anchor_analysis                               ║
# ║  « a leader-follower hierarchy from pulls »                      ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Lead share per heifer from the direction of pulls within        ║
# ║  pairs, which cancels general mobility, with day-block CIs and   ║
# ║  Bradley-Terry pulling strength. Directional consistency is      ║
# ║  tested against random pull directions. Run on raw and           ║
# ║  lag-corrected positions and at several swing thresholds, next   ║
# ║  to the GPS noise floor that sets those thresholds.              ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Statistics for pull and anchor events."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from davasus.vf.constants import (
    BOOTSTRAP_DRAWS,
    PERMUTATIONS,
    PULL_SWING_M,
    PULL_SWING_SENSITIVITY_M,
)
from davasus.vf.formation import shift_by_own_velocity
from davasus.vf.initiation import HerdTracks, estimate_sampling_lags
from davasus.vf.leadership_stats import ChoiceStage, fit_plackett_luce
from davasus.vf.pull_anchor import detect_pull_anchor_events, estimate_gps_noise

log = logging.getLogger(__name__)

SCENARIOS: tuple[str, ...] = ("raw", "lag_corrected")
NOISE_CHECK_LAG_MIN: int = 20


@dataclass(frozen=True)
class PullAnchorResult:
    """Events, per-heifer table, tests, pull matrix and the GPS noise floor."""

    events: pd.DataFrame
    per_heifer: pd.DataFrame
    tests: pd.DataFrame
    pull_matrix: pd.DataFrame
    gps_noise: pd.DataFrame
    sensitivity: pd.DataFrame


def run_pull_anchor_analysis(
    tracks: HerdTracks,
    imu_40mg: np.ndarray,
    rng: np.random.Generator,
    n_permutations: int = PERMUTATIONS,
    n_bootstrap: int = BOOTSTRAP_DRAWS,
) -> PullAnchorResult:
    noise = estimate_gps_noise(tracks.x_m, tracks.y_m, imu_40mg)
    warn_if_below_noise(noise, PULL_SWING_M)
    lags = estimate_lags_or_zero(tracks)
    lag_by_heifer = lags.set_index("lom").loc[tracks.loms, "lag_s"].to_numpy()
    x_lag, y_lag = shift_by_own_velocity(tracks.x_m, tracks.y_m, lag_by_heifer / 60.0)
    events = {"raw": detect_pull_anchor_events(tracks),
              "lag_corrected": detect_pull_anchor_events(tracks, x_lag, y_lag)}
    tables, tests = [], []
    for name, frame in events.items():
        log.info("%s: %d events, %d pulls", name, len(frame), int((frame["outcome"] == "pull").sum()))
        table, summary = summarise_events(frame, tracks.loms, rng, n_permutations, n_bootstrap)
        tables.append(table.assign(scenario=name))
        tests.append(summary.assign(scenario=name))
    per_heifer = pd.concat(tables, ignore_index=True).merge(lags[["lom", "lag_s"]], on="lom")
    per_heifer = per_heifer.merge(classify_pull_roles(per_heifer), on="lom")
    sensitivity = build_sensitivity(tracks, events, lag_by_heifer)
    all_events = pd.concat([f.assign(scenario=n) for n, f in events.items()], ignore_index=True)
    return PullAnchorResult(all_events, per_heifer, pd.concat(tests, ignore_index=True),
                            build_pull_matrix(events["raw"], tracks.loms), noise, sensitivity)


def classify_pull_roles(per_heifer: pd.DataFrame) -> pd.DataFrame:
    """Leader / follower when the lead-share CI excludes 0.5 on the same side
    in both scenarios; lag-dependent when only one scenario does; else neutral."""
    above = (per_heifer["lead_share_lo"] > 0.5).groupby(per_heifer["lom"])
    below = (per_heifer["lead_share_hi"] < 0.5).groupby(per_heifer["lom"])
    n_above, n_below = above.sum(), below.sum()
    n = per_heifer.groupby("lom").size()
    role = pd.Series("neutral", index=n.index)
    role[(n_above > 0) | (n_below > 0)] = "lag-dependent"
    role[n_above == n] = "leader"
    role[n_below == n] = "follower"
    return role.rename("pull_role").reset_index()


def estimate_lags_or_zero(tracks: HerdTracks) -> pd.DataFrame:
    """Collar lags, or zero lags when the herd never travels (nothing to fit)."""
    try:
        return estimate_sampling_lags(tracks)
    except (TypeError, np.linalg.LinAlgError):
        log.warning("no travel minutes to estimate collar lags; lag-corrected run equals raw")
        return pd.DataFrame({"lom": tracks.loms, "lag_s": 0.0})


def warn_if_below_noise(noise: pd.DataFrame, swing_m: float) -> None:
    row = noise[(noise["measure"] == "pair_range") & (noise["lag_min"] == NOISE_CHECK_LAG_MIN)]
    if not row.empty and swing_m < row["p99_m"].iloc[0]:
        log.warning("swing threshold %.1f m is below the 99 %% GPS pair-distance noise "
                    "(%.1f m within %d min)", swing_m, row["p99_m"].iloc[0], NOISE_CHECK_LAG_MIN)


# ┌────────────────────────────────────────────────────────────┐
# │ Per heifer  « lead share and pulling strength »            │
# └────────────────────────────────────────────────────────────┘


def summarise_events(events: pd.DataFrame, loms: list[str], rng: np.random.Generator,
                     n_permutations: int, n_bootstrap: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    pulls = events[events["outcome"] == "pull"]
    matrix = count_pulls(pulls, loms)
    table = pd.DataFrame({"lom": loms, "n_led": matrix.sum(axis=1), "n_followed": matrix.sum(axis=0)})
    table["lead_share"] = compute_lead_share(matrix)
    boot = bootstrap_lead_share(pulls, loms, rng, n_bootstrap)
    table["lead_share_lo"] = np.nanquantile(boot, 0.025, axis=0)
    table["lead_share_hi"] = np.nanquantile(boot, 0.975, axis=0)
    table["pull_strength"] = fit_pull_strength(pulls, loms)
    initiated = events.groupby("initiator")["outcome"]
    table["n_initiated"] = table["lom"].map(initiated.size()).fillna(0).astype(int)
    table["pull_success_rate"] = table["lom"].map(initiated.apply(lambda o: (o == "pull").mean()))
    table["n_anchoring"] = table["lom"].map(
        events.loc[events["outcome"] == "anchor"].groupby("partner").size()).fillna(0).astype(int)
    summary = test_directionality(matrix, rng, n_permutations)
    summary["n_events"] = len(events)
    summary["n_pulls"] = len(pulls)
    summary["split_half_rho"] = compute_split_half(pulls, loms)
    summary["success_vs_events_rho"] = spearmanr(table["pull_success_rate"],
                                                 table["n_initiated"] + table["n_anchoring"])[0]
    summary["lead_share_vs_events_rho"] = spearmanr(table["lead_share"], table["n_led"] + table["n_followed"])[0]
    return table, summary


def count_pulls(pulls: pd.DataFrame, loms: list[str]) -> np.ndarray:
    """matrix[i, j] = pulls in which heifer i led and heifer j followed."""
    index = {lom: k for k, lom in enumerate(loms)}
    matrix = np.zeros((len(loms), len(loms)))
    np.add.at(matrix, (pulls["initiator"].map(index).to_numpy(dtype=np.intp),
                       pulls["partner"].map(index).to_numpy(dtype=np.intp)), 1)
    return matrix


def compute_lead_share(matrix: np.ndarray) -> np.ndarray:
    """Led / (led + followed). Each pull needs both heifers to move once, so
    general mobility cancels."""
    led, followed = matrix.sum(axis=1), matrix.sum(axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        return led / (led + followed)


def bootstrap_lead_share(pulls: pd.DataFrame, loms: list[str], rng: np.random.Generator,
                         n_draws: int) -> np.ndarray:
    by_day = [count_pulls(group, loms) for _, group in pulls.groupby("day")]
    draws = np.empty((n_draws, len(loms)))
    for k in range(n_draws):
        picked = rng.integers(0, len(by_day), len(by_day))
        draws[k] = compute_lead_share(sum(by_day[d] for d in picked))
    return draws


def fit_pull_strength(pulls: pd.DataFrame, loms: list[str]) -> np.ndarray:
    """Bradley-Terry worth: each pull is a contest the leader won over the follower."""
    index = {lom: k for k, lom in enumerate(loms)}
    stages = [ChoiceStage(np.array([index[a], index[b]]), np.array([index[a]]), np.ones(1))
              for a, b in zip(pulls["initiator"], pulls["partner"], strict=True)]
    return fit_plackett_luce(stages, len(loms))


def compute_split_half(pulls: pd.DataFrame, loms: list[str]) -> float:
    half = pd.factorize(pulls["day"])[0] % 2
    return float(spearmanr(compute_lead_share(count_pulls(pulls[half == 0], loms)),
                           compute_lead_share(count_pulls(pulls[half == 1], loms)), nan_policy="omit")[0])


# ┌────────────────────────────────────────────────────────────┐
# │ Hierarchy test  « pull direction against coin flips »      │
# └────────────────────────────────────────────────────────────┘


def compute_directional_consistency(matrix: np.ndarray) -> float:
    """DCI = sum |p_ij - p_ji| / sum (p_ij + p_ji) over pairs (van Hooff & Wensing 1987)."""
    upper = np.triu_indices(len(matrix), 1)
    total = (matrix + matrix.T)[upper].sum()
    return float(np.abs(matrix - matrix.T)[upper].sum() / total) if total else np.nan


def test_directionality(matrix: np.ndarray, rng: np.random.Generator, n_draws: int) -> pd.DataFrame:
    """DCI and the spread of lead shares against random pull directions.

    The null keeps every pair's number of pulls and flips each pull's
    direction with p = 0.5.
    """
    upper = np.triu_indices(len(matrix), 1)
    totals = (matrix + matrix.T)[upper].astype(int)
    observed_dci = compute_directional_consistency(matrix)
    observed_sd = float(np.nanstd(compute_lead_share(matrix)))
    null_dci, null_sd = np.empty(n_draws), np.empty(n_draws)
    for k in range(n_draws):
        null = np.zeros_like(matrix)
        forward = rng.binomial(totals, 0.5)
        null[upper] = forward
        null[(upper[1], upper[0])] = totals - forward
        null_dci[k] = compute_directional_consistency(null)
        null_sd[k] = np.nanstd(compute_lead_share(null))
    return pd.DataFrame([{
        "dci": observed_dci, "dci_null_mean": null_dci.mean(), "dci_null_hi": np.quantile(null_dci, 0.975),
        "dci_p": (np.sum(null_dci >= observed_dci) + 1.0) / (n_draws + 1.0),
        "sd_lead_share": observed_sd, "sd_null_mean": null_sd.mean(), "sd_null_hi": np.quantile(null_sd, 0.975),
        "sd_p": (np.sum(null_sd >= observed_sd) + 1.0) / (n_draws + 1.0)}])


def build_pull_matrix(events: pd.DataFrame, loms: list[str]) -> pd.DataFrame:
    matrix = count_pulls(events[events["outcome"] == "pull"], loms)
    return pd.DataFrame(matrix, index=loms, columns=loms).rename_axis("leader").rename_axis("follower", axis=1)


# ┌────────────────────────────────────────────────────────────┐
# │ Robustness  « lag and threshold »                          │
# └────────────────────────────────────────────────────────────┘


def build_sensitivity(tracks: HerdTracks, events: dict[str, pd.DataFrame],
                      lag_by_heifer: np.ndarray) -> pd.DataFrame:
    """Lead shares at other swing thresholds, their agreement and lag dependence."""
    runs = {f"{name} {PULL_SWING_M:g} m": frame for name, frame in events.items()}
    for swing in PULL_SWING_SENSITIVITY_M:
        runs[f"raw {swing:g} m"] = detect_pull_anchor_events(tracks, swing_m=swing)
    reference = compute_lead_share(count_pulls(runs[f"raw {PULL_SWING_M:g} m"].query("outcome == 'pull'"),
                                               tracks.loms))
    rows = []
    for name, frame in runs.items():
        pulls = frame[frame["outcome"] == "pull"]
        share = compute_lead_share(count_pulls(pulls, tracks.loms))
        rows.append({"run": name, "n_pulls": len(pulls),
                     "rho_vs_raw": float(spearmanr(share, reference)[0]),
                     "rho_vs_lag_s": float(spearmanr(share, lag_by_heifer)[0])})
    return pd.DataFrame(rows)
