# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.learning_journey                                   ║
# ║  « each heifer's way with the fence »                            ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Two axes per heifer. Risk taking: how often she interacts with  ║
# ║  the fence, given how much she is near it. Conditioning: how     ║
# ║  often the tone alone ends the interaction, how that changes     ║
# ║  with experience, and whether it holds when the fence moves. A   ║
# ║  social route is tested beside them: retreating on a herd        ║
# ║  mate's tone. All of it is set against rank and sociability.     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Per-heifer exposure, learning, fence moves and social cues, in one table."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import false_discovery_control, spearmanr

from davasus.vf.constants import (
    FENCE_MOVE_MIN_INTERACTIONS,
    FENCE_MOVE_WINDOW_H,
    PERMUTATIONS,
    SOCIAL_MIN_EVENTS,
)
from davasus.vf.fence_context import (
    build_distance_profile,
    convert_fence_distance,
    describe_interactions,
    run_context_tests,
    summarise_context,
)
from davasus.vf.fence_interactions import (
    build_learning_curves,
    detect_interactions,
    find_untimed_fixes,
    summarise_exposure,
    summarise_learning,
    summarise_weeks,
)
from davasus.vf.initiation import HerdTracks
from davasus.vf.social_cues import (
    OUTCOMES,
    compute_sociability,
    find_social_events,
    flag_first_cues,
    summarise_mate_cues,
    summarise_social_response,
)

log = logging.getLogger(__name__)

STRATEGIES: tuple[str, ...] = ("frequent, tone is enough", "frequent, mostly pulsed", "rare, tone is enough",
                               "rare, mostly pulsed")
HIERARCHY_COLUMNS: tuple[str, ...] = ("rank", "movement_rank", "exploration_rank", "tier")
CORRELATED: tuple[str, ...] = (
    "rank", "movement_rank", "exploration_rank", "interactions_per_day", "interactions_per_zone_hour",
    "zone_share", "pulses_per_zone_hour", "tones_per_interaction", "pulsed_share", "learning_slope",
    "first_cue_share",
    "pulsed_share_jump", "retreat_gain", "own_cue_gain", "association", "nearest_neighbour_m")


@dataclass(frozen=True)
class LearningJourneyResult:
    interactions: pd.DataFrame
    per_heifer: pd.DataFrame
    weeks: pd.DataFrame
    curves: pd.DataFrame
    fence_moves: pd.DataFrame
    social_events: pd.DataFrame
    mate_cues: pd.DataFrame
    correlations: pd.DataFrame
    tests: pd.DataFrame
    distance_profile: pd.DataFrame


def run_learning_journey(data, tracks: HerdTracks, hierarchy: pd.DataFrame | None, rng: np.random.Generator,
                         n_permutations: int = PERMUTATIONS) -> LearningJourneyResult:
    """Interactions, per-heifer profiles and the herd-level tests.

    Args:
        data: Dashboard data (minute cubes, minutes, events).
        tracks: Herd tracks of the same minutes.
        hierarchy: Table with ``lom`` and the rank columns, or None.
        rng: Random generator for the permutations.
        n_permutations: Sign flips per paired test.
    """
    loms = list(data.loms)
    has_fix = data.outside >= 0
    is_untimed = find_untimed_fixes(has_fix)
    interactions = detect_interactions(data.audio, data.pulse, is_untimed, data.minutes, loms)
    is_cue = ((np.nan_to_num(data.audio) > 0) | (np.nan_to_num(data.pulse) > 0)) & ~is_untimed
    interactions["is_first_cue"] = flag_first_cues(interactions, is_cue, loms)
    distance_m = convert_fence_distance(data.fence_dist_max)
    interactions = describe_interactions(interactions, data, distance_m, tracks.speed)
    n_days = (data.minutes[-1] - data.minutes[0]) / pd.Timedelta(days=1)
    fence_moves = compare_fence_moves(interactions, data.events)
    social_events = find_social_events(data.outside, is_cue, data.pulse, data.x_m, data.y_m, data.is_gps_reduced,
                                       loms)
    social = summarise_social_response(social_events, loms)
    thin = (social[["n_mate_cued", "n_quiet"]].min(axis=1) < SOCIAL_MIN_EVENTS) | social["n_mate_cued"].isna()
    social.loc[thin, [f"{outcome}_gain" for outcome in OUTCOMES]] = np.nan
    per_heifer = (
        summarise_exposure(interactions, data.audio, data.pulse, data.outside, is_untimed, loms, n_days)
        .merge(summarise_learning(interactions), on="lom", how="left")
        .merge(summarise_first_cues(interactions), on="lom", how="left")
        .merge(summarise_fence_moves(fence_moves), on="lom", how="left")
        .merge(summarise_context(interactions), on="lom", how="left")
        .merge(social, on="lom", how="left")
        .merge(compute_sociability(data.x_m, data.y_m, ~tracks.is_excluded, loms), on="lom", how="left"))
    if hierarchy is not None:
        per_heifer = per_heifer.merge(hierarchy[["lom", *HIERARCHY_COLUMNS]], on="lom", how="left")
    per_heifer["strategy"] = classify_strategies(per_heifer)
    tests = pd.concat([pd.DataFrame([
        {"test": "pulsed share: after minus before a fence move",
         **test_paired_shift(per_heifer["pulsed_share_jump"].to_numpy(), rng, n_permutations)},
        {"test": "pulsed share: last minus first block of interactions",
         **test_paired_shift(per_heifer["pulsed_share_change"].to_numpy(), rng, n_permutations)},
        {"test": "pulsed share: first cue minus cue after a herd mate's",
         **test_paired_shift(per_heifer["pulsed_share_first_gain"].to_numpy(), rng, n_permutations)},
        *({"test": f"{outcome}: after a mate's cue minus quiet company",
           **test_paired_shift(per_heifer[f"{outcome}_gain"].to_numpy(), rng, n_permutations)}
          for outcome in OUTCOMES)]), run_context_tests(per_heifer, test_paired_shift, rng, n_permutations)],
        ignore_index=True)
    return LearningJourneyResult(interactions, per_heifer, summarise_weeks(interactions, data.minutes[0]),
                                 build_learning_curves(interactions), fence_moves, social_events,
                                 summarise_mate_cues(social_events), correlate_measures(per_heifer), tests,
                                 build_distance_profile(distance_m, data.audio, data.pulse, is_untimed))


# ┌────────────────────────────────────────────────────────────┐
# │ Fence moves  « does the tone still work on new ground »    │
# └────────────────────────────────────────────────────────────┘


def compare_fence_moves(interactions: pd.DataFrame, events: pd.DataFrame,
                        window_h: int = FENCE_MOVE_WINDOW_H) -> pd.DataFrame:
    """Per heifer and fence move: interactions and pulsed ones before and after.

    A move is the start of a parcel with a new paddock; the second step of a
    two-step change (the parcel after a transition) is not counted again.
    """
    is_move = events["new_parcel"].notna() & ~events["is_transition"].shift(1, fill_value=0).astype(bool)
    window = pd.Timedelta(hours=window_h)
    frames = []
    for event in events[is_move].itertuples():
        since = interactions["onset_utc"] - event.start_utc
        for phase, mask in (("before", (since < pd.Timedelta(0)) & (since >= -window)),
                            ("after", (since >= pd.Timedelta(0)) & (since < window))):
            counts = interactions[mask].groupby("lom").agg(
                n_interactions=("is_pulsed", "size"), n_pulsed=("is_pulsed", "sum"), audio=("audio", "sum"))
            frames.append(counts.reset_index().assign(event_id=event.event_id, label=event.label, phase=phase))
    return pd.concat(frames, ignore_index=True)


def summarise_fence_moves(fence_moves: pd.DataFrame,
                          min_interactions: int = FENCE_MOVE_MIN_INTERACTIONS) -> pd.DataFrame:
    """Per heifer, pooled over the moves: pulsed share before and after, and the jump."""
    pooled = fence_moves.groupby(["lom", "phase"])[["n_interactions", "n_pulsed"]].sum().unstack("phase")
    pooled = pooled.reindex(columns=pd.MultiIndex.from_product([["n_interactions", "n_pulsed"], ["before", "after"]]),
                            fill_value=0).fillna(0)
    out = pd.DataFrame({"lom": pooled.index})
    for phase in ("before", "after"):
        n = pooled[("n_interactions", phase)].to_numpy(dtype=float)
        out[f"interactions_{phase}_move"] = n
        out[f"pulsed_share_{phase}_move"] = np.where(n >= min_interactions,
                                                     pooled[("n_pulsed", phase)].to_numpy() / np.maximum(n, 1), np.nan)
    out["pulsed_share_jump"] = out["pulsed_share_after_move"] - out["pulsed_share_before_move"]
    return out


# ┌────────────────────────────────────────────────────────────┐
# │ Profiles  « strategy, correlations, paired tests »         │
# └────────────────────────────────────────────────────────────┘


def summarise_first_cues(interactions: pd.DataFrame, min_interactions: int = FENCE_MOVE_MIN_INTERACTIONS) -> pd.DataFrame:
    """Per heifer: share of first cues, and the pulsed share of first and of later cues."""
    grouped = interactions.groupby(["lom", "is_first_cue"])["is_pulsed"].agg(["mean", "size"]).unstack("is_first_cue")
    share = grouped["mean"].where(grouped["size"] >= min_interactions).reindex(columns=[True, False])
    out = pd.DataFrame({"first_cue_share": interactions.groupby("lom")["is_first_cue"].mean(),
                        "pulsed_share_first_cue": share[True], "pulsed_share_later_cue": share[False]})
    out["pulsed_share_first_gain"] = out["pulsed_share_first_cue"] - out["pulsed_share_later_cue"]
    return out.rename_axis("lom").reset_index()


def classify_strategies(per_heifer: pd.DataFrame) -> list[str]:
    """Two classes on each axis, cut at the widest gap between heifers; descriptive only."""
    is_frequent = split_at_widest_gap(np.log(per_heifer["interactions_per_day"].to_numpy(dtype=float)))
    is_pulsed = split_at_widest_gap(per_heifer["pulsed_share"].to_numpy(dtype=float))
    return [STRATEGIES[2 * (not frequent) + pulsed] for frequent, pulsed in zip(is_frequent, is_pulsed, strict=True)]


def split_at_widest_gap(values: np.ndarray) -> np.ndarray:
    """True for the values above the widest gap in the sorted finite values."""
    ordered = np.sort(values[np.isfinite(values)])
    if len(ordered) < 2:
        return np.zeros(len(values), dtype=bool)
    gap = int(np.argmax(np.diff(ordered)))
    with np.errstate(invalid="ignore"):
        return values > 0.5 * (ordered[gap] + ordered[gap + 1])


def correlate_measures(per_heifer: pd.DataFrame) -> pd.DataFrame:
    """Spearman rho for every pair of per-heifer measures, with Benjamini-Hochberg q."""
    columns = [c for c in CORRELATED if c in per_heifer]
    rows = []
    for k, first in enumerate(columns):
        for second in columns[k + 1:]:
            ok = per_heifer[[first, second]].notna().all(axis=1)
            if ok.sum() < 5:
                continue
            rho, p = spearmanr(per_heifer.loc[ok, first], per_heifer.loc[ok, second])
            rows.append({"measure_a": first, "measure_b": second, "n": int(ok.sum()), "rho": float(rho),
                         "p": float(p)})
    table = pd.DataFrame(rows)
    table["q"] = false_discovery_control(table["p"].to_numpy())
    return table


def test_paired_shift(shift: np.ndarray, rng: np.random.Generator,
                      n_permutations: int = PERMUTATIONS) -> dict[str, float]:
    """Sign-flip permutation test of the mean per-heifer shift against zero."""
    shift = shift[np.isfinite(shift)]
    if len(shift) < 3:
        return {"n_heifers": len(shift), "mean_shift": np.nan, "n_positive": np.nan, "p_two_sided": np.nan}
    signs = rng.choice((-1.0, 1.0), size=(n_permutations, len(shift)))
    null = (signs * shift).mean(axis=1)
    observed = float(shift.mean())
    p = (np.sum(np.abs(null) >= abs(observed) - 1e-12) + 1.0) / (n_permutations + 1.0)
    return {"n_heifers": len(shift), "mean_shift": observed, "n_positive": int((shift > 0).sum()),
            "p_two_sided": float(p)}
