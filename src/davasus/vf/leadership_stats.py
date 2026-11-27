# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.leadership_stats                                   ║
# ║  « is there a leader, or just a gradient »                       ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Dumont first-place shares with day-block bootstrap CIs,         ║
# ║  the egalitarian null (labels permuted within days), the         ║
# ║  graded null (geometric worths), Plackett-Luce worths and        ║
# ║  Kendall's W as whole-order consistency (handover 4.3.1).        ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Statistics for first-mover shares and departure order."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from rerandomstats import correct_pvalues
from scipy.optimize import minimize_scalar

from davasus.vf.constants import (
    BOOTSTRAP_DRAWS,
    GRADED_NULL_DRAWS,
    PERMUTATION_CHUNK,
    PERMUTATIONS,
    PL_MAX_ITERATIONS,
    PL_PSEUDO_WINS,
    PL_TOLERANCE,
)


@dataclass(frozen=True)
class CreditMatrix:
    """First-place credit per group movement (rows) and heifer (columns).

    ``credit`` is NaN where the heifer was absent; ``day_codes`` maps each
    row to its day so resampling can respect within-day bursts.
    """

    loms: list[str]
    credit: np.ndarray
    day_codes: np.ndarray

    @property
    def n_events(self) -> int:
        return self.credit.shape[0]


def build_credit_matrix(credits: pd.DataFrame) -> CreditMatrix:
    wide = credits.pivot_table(index="bout_id", columns="lom", values="credit", aggfunc="sum")
    days = credits.groupby("bout_id")["day"].first().reindex(wide.index)
    return CreditMatrix(list(wide.columns), wide.to_numpy(dtype=float),
                        pd.factorize(days)[0])


# ┌────────────────────────────────────────────────────────────┐
# │ Dumont shares  « first places per event present »          │
# └────────────────────────────────────────────────────────────┘


def compute_leader_shares(matrix: CreditMatrix) -> np.ndarray:
    """Share of first places among the events each heifer was present for."""
    present = np.isfinite(matrix.credit).sum(axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.nansum(matrix.credit, axis=0) / present


def describe_share_distribution(shares: np.ndarray) -> dict[str, float]:
    """Top share, gap from first to second, and the Gini coefficient."""
    ordered = np.sort(shares[np.isfinite(shares)])[::-1]
    return {"top_share": float(ordered[0]), "top_gap": float(ordered[0] - ordered[1]),
            "gini": compute_gini(ordered)}


def compute_gini(values: np.ndarray) -> float:
    v = np.sort(np.asarray(values, dtype=float))
    if v.sum() == 0:
        return 0.0
    n = len(v)
    return float((2.0 * np.arange(1, n + 1) - n - 1) @ v / (n * v.sum()))


def bootstrap_shares_by_day(
    matrix: CreditMatrix, rng: np.random.Generator, n_draws: int = BOOTSTRAP_DRAWS
) -> np.ndarray:
    """Shares from days resampled with replacement; shape (n_draws, n_heifers)."""
    days = np.unique(matrix.day_codes)
    rows_of = [np.flatnonzero(matrix.day_codes == d) for d in days]
    draws = np.empty((n_draws, len(matrix.loms)))
    for k in range(n_draws):
        rows = np.concatenate([rows_of[d] for d in rng.integers(0, len(days), len(days))])
        draws[k] = compute_leader_shares(CreditMatrix(matrix.loms, matrix.credit[rows],
                                                      matrix.day_codes[rows]))
    return draws


# ┌────────────────────────────────────────────────────────────┐
# │ Egalitarian null  « labels shuffled within days »          │
# └────────────────────────────────────────────────────────────┘


def test_egalitarian_null(
    matrix: CreditMatrix, rng: np.random.Generator, n_permutations: int = PERMUTATIONS
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Permute heifer labels within each day, keeping within-day bursts.

    Returns:
        Summary table (statistic, observed, null mean, null 2.5/97.5 %, p) and
        per-heifer p-values for P(share under null >= observed), BH-corrected
        with reRandomStats.
    """
    observed_shares = compute_leader_shares(matrix)
    observed = describe_share_distribution(observed_shares)
    stats = {key: np.empty(n_permutations) for key in observed}
    exceed = np.zeros(len(matrix.loms))
    for k in range(n_permutations):
        shares = compute_leader_shares(permute_within_days(matrix, rng))
        for key, value in describe_share_distribution(shares).items():
            stats[key][k] = value
        exceed += shares >= observed_shares - 1e-12
    summary = _summarise_null(observed, stats, "egalitarian")
    p_animal = (exceed + 1.0) / (n_permutations + 1.0)
    return summary, _per_animal_table(matrix.loms, observed_shares, p_animal)


def permute_within_days(matrix: CreditMatrix, rng: np.random.Generator) -> CreditMatrix:
    credit = matrix.credit.copy()
    for d in np.unique(matrix.day_codes):
        rows = matrix.day_codes == d
        credit[rows] = credit[rows][:, rng.permutation(credit.shape[1])]
    return CreditMatrix(matrix.loms, credit, matrix.day_codes)


def _per_animal_table(loms: list[str], shares: np.ndarray, p_values: np.ndarray) -> pd.DataFrame:
    corrected = correct_pvalues(dict(zip(loms, p_values.tolist(), strict=True)), method="fdr_bh")
    table = pd.DataFrame({"lom": loms, "share": shares, "p_egalitarian": p_values})
    table["q_egalitarian_bh"] = [corrected["results"][lom]["adjusted_p"] for lom in loms]
    return table


def _summarise_null(observed: dict[str, float], stats: dict[str, np.ndarray], name: str) -> pd.DataFrame:
    rows = []
    for key, value in observed.items():
        null = stats[key]
        rows.append({"null": name, "statistic": key, "observed": value, "null_mean": null.mean(),
                     "null_lo": np.quantile(null, 0.025), "null_hi": np.quantile(null, 0.975),
                     "p": (np.sum(null >= value - 1e-12) + 1.0) / (len(null) + 1.0)})
    return pd.DataFrame(rows)


# ┌────────────────────────────────────────────────────────────┐
# │ Graded null  « smooth decline, no distinct leader »        │
# └────────────────────────────────────────────────────────────┘


def test_graded_null(
    matrix: CreditMatrix, rng: np.random.Generator, n_draws: int = GRADED_NULL_DRAWS
) -> tuple[pd.DataFrame, float]:
    """Simulate first places from geometric worths q**rank (Sueur 2018).

    The ranks come from the observed shares; q is fitted by maximum
    likelihood on the first-place data. Each simulated event keeps the
    observed set of present heifers and the observed number tied first.

    Returns:
        Null summary for top share, top gap and Gini, and the fitted q.
    """
    observed_shares = compute_leader_shares(matrix)
    rank = np.argsort(np.argsort(-np.nan_to_num(observed_shares, nan=-1.0)))
    q = fit_geometric_decline(matrix, rank)
    worth = q ** rank.astype(float)
    present = np.isfinite(matrix.credit)
    n_tied = np.maximum((np.nan_to_num(matrix.credit) > 0).sum(axis=1), 1)
    stats = {key: np.empty(n_draws) for key in ("top_share", "top_gap", "gini")}
    for k in range(n_draws):
        credit = _simulate_first_places(worth, present, n_tied, rng)
        for key, value in describe_share_distribution(
                compute_leader_shares(CreditMatrix(matrix.loms, credit, matrix.day_codes))).items():
            stats[key][k] = value
    return _summarise_null(describe_share_distribution(observed_shares), stats, "graded"), q


def fit_geometric_decline(matrix: CreditMatrix, rank: np.ndarray) -> float:
    def negative_log_likelihood(q: float) -> float:
        worth = q ** rank.astype(float)
        present = np.isfinite(matrix.credit)
        denominator = (present * worth).sum(axis=1)
        wins = np.nan_to_num(matrix.credit)
        return -float((wins * np.log(worth)).sum() - np.log(denominator).sum())

    return float(minimize_scalar(negative_log_likelihood, bounds=(0.05, 1.0), method="bounded").x)


def _simulate_first_places(worth: np.ndarray, present: np.ndarray, n_tied: np.ndarray,
                           rng: np.random.Generator) -> np.ndarray:
    credit = np.where(present, 0.0, np.nan)
    for e in range(present.shape[0]):
        candidates = np.flatnonzero(present[e])
        t = min(int(n_tied[e]), len(candidates))
        chosen = rng.choice(candidates, size=t, replace=False, p=worth[candidates] / worth[candidates].sum())
        credit[e, chosen] = 1.0 / t
    return credit


# ┌────────────────────────────────────────────────────────────┐
# │ Plackett-Luce  « worths from first places and orders »     │
# └────────────────────────────────────────────────────────────┘


@dataclass(frozen=True)
class ChoiceStage:
    """One choice: ``winners`` picked from ``choice_set`` with weights."""

    choice_set: np.ndarray
    winners: np.ndarray
    weights: np.ndarray


def build_top1_stages(matrix: CreditMatrix) -> list[ChoiceStage]:
    """First stage only: Dumont's question, absent heifers excluded."""
    stages = []
    for row in matrix.credit:
        winners = np.flatnonzero(np.nan_to_num(row) > 0)
        stages.append(ChoiceStage(np.flatnonzero(np.isfinite(row)), winners, row[winners]))
    return stages


def build_order_stages(departures: pd.DataFrame, loms: list[str], tie_tolerance_min: float) -> list[ChoiceStage]:
    """Full joining orders; non-joiners rank last, ties handled as in Breslow.

    Departures within ``tie_tolerance_min`` of the stage's earliest form one
    tied group; each member counts as chosen from the remaining set.
    """
    index = {lom: a for a, lom in enumerate(loms)}
    stages = []
    for _, bout in departures[departures["is_present"]].groupby("bout_id"):
        remaining = np.array([index[lom] for lom in bout["lom"]])
        times = bout["departure_min"].to_numpy()
        while np.isfinite(times).any():
            tied = np.isfinite(times) & (times <= np.nanmin(times) + tie_tolerance_min)
            if tied.all():
                break
            stages.append(ChoiceStage(remaining.copy(), remaining[tied], np.ones(tied.sum())))
            remaining, times = remaining[~tied], times[~tied]
    return stages


def fit_plackett_luce(stages: list[ChoiceStage], n_items: int) -> np.ndarray:
    """MM algorithm (Hunter 2004) with ``PL_PSEUDO_WINS`` against a virtual opponent.

    Returns worths scaled to sum to one, i.e. the probability of being chosen
    first when every heifer is present.
    """
    members = np.concatenate([s.choice_set for s in stages]) if stages else np.empty(0, int)
    stage_of = np.repeat(np.arange(len(stages)), [len(s.choice_set) for s in stages])
    stage_weight = np.array([s.weights.sum() for s in stages])
    wins = np.full(n_items, PL_PSEUDO_WINS)
    for stage in stages:
        np.add.at(wins, stage.winners, stage.weights)
    worth = np.ones(n_items)
    for _ in range(PL_MAX_ITERATIONS):
        stage_total = np.bincount(stage_of, weights=worth[members], minlength=len(stages))
        denominator = 2.0 * PL_PSEUDO_WINS / (1.0 + worth) + np.bincount(
            members, weights=(stage_weight / stage_total)[stage_of], minlength=n_items)
        updated = wins / denominator
        updated /= np.exp(np.log(updated).mean())
        is_converged = np.max(np.abs(updated - worth)) < PL_TOLERANCE
        worth = updated
        if is_converged:
            break
    return worth / worth.sum()


def bootstrap_plackett_luce(stages_by_day: dict[int, list[ChoiceStage]], n_items: int,
                            rng: np.random.Generator, n_draws: int) -> np.ndarray:
    days = list(stages_by_day)
    draws = np.empty((n_draws, n_items))
    for k in range(n_draws):
        picked = rng.choice(len(days), size=len(days), replace=True)
        draws[k] = fit_plackett_luce([s for d in picked for s in stages_by_day[days[d]]], n_items)
    return draws


def test_rate_heterogeneity(
    labels: pd.Series, outcomes: pd.Series, days: pd.Series, rng: np.random.Generator,
    n_permutations: int = PERMUTATIONS,
) -> dict[str, float]:
    """Spread (SD) of per-heifer success rates against labels shuffled within days."""
    frame = pd.DataFrame({"label": labels.to_numpy(), "y": outcomes.to_numpy(dtype=float),
                          "day": days.to_numpy()})
    observed = float(frame.groupby("label")["y"].mean().std())
    null = np.empty(n_permutations)
    groups = [g.index.to_numpy() for _, g in frame.groupby("day")]
    shuffled = frame["label"].to_numpy().copy()
    for k in range(n_permutations):
        for rows in groups:
            shuffled[rows] = rng.permutation(frame["label"].to_numpy()[rows])
        null[k] = frame["y"].groupby(shuffled).mean().std()
    return {"sd_rate": observed, "null_mean": float(null.mean()),
            "null_hi": float(np.quantile(null, 0.975)),
            "p": float((np.sum(null >= observed) + 1.0) / (n_permutations + 1.0))}


# ┌────────────────────────────────────────────────────────────┐
# │ Kendall's W  « consistency of the whole order »            │
# └────────────────────────────────────────────────────────────┘


def compute_order_w(ranks: np.ndarray) -> float:
    """Kendall's W generalised to missing heifers.

    ``ranks`` is events x heifers of normalised ranks in [0, 1], NaN if
    absent. W = variance of the heifers' mean ranks divided by the mean
    within-event rank variance; identical to Kendall's W for complete data.
    """
    mean_rank = np.nanmean(ranks, axis=0)
    within = np.nanvar(ranks, axis=1)
    return float(np.nanvar(mean_rank) / np.nanmean(within))


def compute_leader_w_benchmark(n_animals: int) -> float:
    """W limit for a perfect leader with randomly ordered followers: 3 / (n + 1)."""
    return 3.0 / (n_animals + 1.0)


def build_normalised_ranks(departures: pd.DataFrame, loms: list[str]) -> pd.DataFrame:
    """Departure ranks per bout, non-joiners tied last; r = (rank - 1) / (n - 1)."""
    present = departures[departures["is_present"]].copy()
    present["time"] = present["departure_min"].fillna(np.inf)
    rank = present.groupby("bout_id")["time"].rank(method="average")
    n = present.groupby("bout_id")["time"].transform("size")
    present["r"] = (rank - 1.0) / (n - 1.0)
    return present.pivot_table(index="bout_id", columns="lom", values="r").reindex(columns=loms)


def test_order_w(ranks: pd.DataFrame, days: pd.Series, rng: np.random.Generator,
                 n_permutations: int = PERMUTATIONS) -> dict[str, float]:
    """W with a within-day label-permutation p-value and the leader benchmark."""
    values = ranks.to_numpy(dtype=float)
    matrix = CreditMatrix(list(ranks.columns), values, pd.factorize(days.reindex(ranks.index))[0])
    observed = compute_order_w(values)
    null = np.array([compute_order_w(permute_within_days(matrix, rng).credit)
                     for _ in range(n_permutations)])
    return {"w": observed, "null_mean": float(null.mean()),
            "null_hi": float(np.quantile(null, 0.975)),
            "p": float((np.sum(null >= observed) + 1.0) / (n_permutations + 1.0)),
            "leader_benchmark": compute_leader_w_benchmark(values.shape[1])}


# ┌────────────────────────────────────────────────────────────┐
# │ Fast permutations  « shuffles within groups at once »      │
# └────────────────────────────────────────────────────────────┘


def permute_within_groups(values: np.ndarray, groups: np.ndarray, rng: np.random.Generator,
                          n_permutations: int) -> np.ndarray:
    """``n_permutations`` shuffles of ``values`` within each group, shape (n_permutations, n).

    Sorting ``group + U(0, 1)`` keeps the groups apart and orders each group at
    random, so every row is one independent within-group permutation.
    """
    values = np.asarray(values)
    codes = pd.factorize(np.asarray(groups))[0].astype(float)
    home = np.argsort(codes, kind="stable")
    order = np.argsort(codes[None, :] + rng.random((n_permutations, len(codes))), axis=1)
    permuted = np.empty((n_permutations, len(values)), dtype=values.dtype)
    permuted[:, home] = values[order]
    return permuted


def iterate_permutation_chunks(n_permutations: int, chunk: int = PERMUTATION_CHUNK):
    """Chunk sizes that add up to ``n_permutations`` (keeps the arrays small)."""
    done = 0
    while done < n_permutations:
        size = min(chunk, n_permutations - done)
        yield size
        done += size


def test_within_group_spearman(frame: pd.DataFrame, x: str, y: str, group: str, rng: np.random.Generator,
                               n_permutations: int = PERMUTATIONS) -> dict[str, float]:
    """Mean within-group Spearman rho of ``x`` on ``y``, null from shuffling ``x`` within groups.

    Spearman rho is Pearson on average ranks; with the ``y`` ranks fixed,
    rho_g = sum_i c_i * w_i, c the centred ``x`` ranks and
    w_i = d_i / sqrt(sum c^2 * sum d^2) with d the centred ``y`` ranks.
    Shuffling ``x`` within a group shuffles c, so a block of permutations
    is one matrix product. Groups without spread in x or y are dropped
    (``spearmanr`` gives NaN there).
    """
    keys = frame[group]
    centred = {}
    for column in (x, y):
        rank = frame.groupby(group)[column].rank(method="average")
        centred[column] = rank - rank.groupby(keys).transform("mean")
    ss_x = (centred[x] ** 2).groupby(keys).transform("sum")
    ss_y = (centred[y] ** 2).groupby(keys).transform("sum")
    keep = ((ss_x > 0) & (ss_y > 0)).to_numpy()
    c = centred[x].to_numpy()[keep]
    w = (centred[y] / np.sqrt(ss_x * ss_y)).to_numpy()[keep]
    groups = keys.to_numpy()[keep]
    n_groups = len(np.unique(groups))
    if n_groups == 0:
        return {"n_groups": 0, "mean_rho": np.nan, "null_mean": np.nan, "null_lo": np.nan,
                "null_hi": np.nan, "p_two_sided": np.nan}
    observed = float(c @ w / n_groups)
    null = np.concatenate([permute_within_groups(c, groups, rng, size) @ w / n_groups
                           for size in iterate_permutation_chunks(n_permutations)])
    return {"n_groups": n_groups, "mean_rho": observed, "null_mean": float(null.mean()),
            "null_lo": float(np.quantile(null, 0.025)), "null_hi": float(np.quantile(null, 0.975)),
            "p_two_sided": _two_sided_p(null, observed)}


def test_flag_difference(flags: np.ndarray, values: np.ndarray, blocks: np.ndarray,
                         rng: np.random.Generator, n_permutations: int = PERMUTATIONS) -> dict[str, float]:
    """Mean ``values`` where flagged minus where not; flags shuffled within blocks (days)."""
    flags = np.asarray(flags, dtype=bool)
    values = np.asarray(values, dtype=float)
    observed = float(_flag_difference(flags[None, :], values)[0])
    null = np.concatenate([_flag_difference(permute_within_groups(flags, blocks, rng, size), values)
                           for size in iterate_permutation_chunks(n_permutations)])
    finite = null[np.isfinite(null)]
    return {"n": len(flags), "n_flagged": int(flags.sum()),
            "mean_flagged": float(values[flags].mean()) if flags.any() else np.nan,
            "mean_unflagged": float(values[~flags].mean()) if (~flags).any() else np.nan,
            "difference": observed, "null_mean": float(finite.mean()) if len(finite) else np.nan,
            "p_two_sided": _two_sided_p(finite, observed) if len(finite) else np.nan}


def _flag_difference(flags: np.ndarray, values: np.ndarray) -> np.ndarray:
    """Row-wise mean where flagged minus where not; NaN for rows with one class only."""
    n_flagged = flags.sum(axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        return (flags @ values) / n_flagged - ((~flags) @ values) / (flags.shape[1] - n_flagged)


def _two_sided_p(null: np.ndarray, observed: float) -> float:
    """Distance from the null mean, plus-one corrected."""
    centre = null.mean()
    return float((np.sum(np.abs(null - centre) >= abs(observed - centre)) + 1.0) / (len(null) + 1.0))
