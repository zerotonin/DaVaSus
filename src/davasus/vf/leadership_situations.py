# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.leadership_situations                              ║
# ║  « shade, fence retreat and turns »                              ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Second round of leadership in context: who leads the herd into  ║
# ║  the shade, who turns it away from the virtual fence, and whose  ║
# ║  turns the others copy. Order measures run on raw and on         ║
# ║  collar-lag-corrected times; all rankings are compared with the  ║
# ║  earlier measures.                                               ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Leadership into shade, away from the fence, and through turns."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from davasus.vf.constants import BOOTSTRAP_DRAWS, PERMUTATIONS
from davasus.vf.dumont import analyse_scenario
from davasus.vf.fence_approach import compute_zone_entries, detect_fence_episodes
from davasus.vf.fence_retreat import (
    compute_zone_exits,
    run_retreat_tests,
    summarise_retreat_per_heifer,
    summarise_retreats,
)
from davasus.vf.initiation import HerdTracks, apply_sampling_lags, estimate_sampling_lags
from davasus.vf.leadership_context import widen_by_scenario
from davasus.vf.shade_arrival import (
    compute_patch_occupancy,
    compute_shade_arrivals,
    detect_shade_episodes,
)
from davasus.vf.turn_leadership import (
    analyse_turns,
    compute_correlation_sums,
    compute_headings,
    summarise_turn_scores,
)

log = logging.getLogger(__name__)

SHADE_SUBSETS: tuple[str, ...] = ("all", "hot")


@dataclass
class LeadershipSituationsResult:
    shade_episodes: pd.DataFrame
    shade_arrivals: pd.DataFrame
    shade_per_heifer: pd.DataFrame
    shade_nulls: pd.DataFrame
    shade_w: pd.DataFrame
    retreat_exits: pd.DataFrame
    retreats: pd.DataFrame
    retreat_per_heifer: pd.DataFrame
    retreat_nulls: pd.DataFrame
    retreat_w: pd.DataFrame
    retreat_tests: pd.DataFrame
    turn_scores: pd.DataFrame
    turn_delays: pd.DataFrame
    turn_correlation: pd.DataFrame
    turn_test: pd.DataFrame
    per_heifer: pd.DataFrame


# ┌────────────────────────────────────────────────────────────┐
# │ Orchestration  « three situations, one table »             │
# └────────────────────────────────────────────────────────────┘


def run_leadership_situations(
    data,
    tracks: HerdTracks,
    rng: np.random.Generator,
    n_permutations: int = PERMUTATIONS,
    n_bootstrap: int = BOOTSTRAP_DRAWS,
) -> LeadershipSituationsResult:
    """Run the shade, fence-retreat and turn analyses on one herd."""
    loms = list(data.loms)
    lags = estimate_sampling_lags(tracks)
    shade = run_shade(data, lags, rng, n_permutations, n_bootstrap)
    retreat = run_retreat(data, lags, rng, n_permutations, n_bootstrap)
    turns = run_turns(data, tracks, lags, rng, n_permutations, n_bootstrap)
    per_heifer = (pd.DataFrame({"lom": loms})
                  .merge(shade["wide"], on="lom", how="left")
                  .merge(retreat["wide"], on="lom", how="left")
                  .merge(widen_by_scenario(turns["scores"], ["turn_lead_min", "turn_lead_lo", "turn_lead_hi"]),
                         on="lom", how="left")
                  .merge(lags[["lom", "lag_s"]], on="lom", how="left"))
    return LeadershipSituationsResult(
        shade["episodes"], shade["arrivals"], shade["per_animal"], shade["nulls"], shade["w"],
        retreat["exits"], retreat["retreats"], retreat["per_animal"], retreat["nulls"], retreat["w"],
        retreat["tests"], turns["scores"], turns["delays"], turns["correlation"], turns["test"], per_heifer)


def run_dumont_scenarios(departures: pd.DataFrame, events: pd.DataFrame, lags: pd.DataFrame, loms: list[str],
                         rng: np.random.Generator, n_permutations: int, n_bootstrap: int,
                         label: str = "") -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Dumont statistics on raw and lag-corrected times; scenario names get ``label``."""
    per_animal, nulls, w = [], [], []
    for scenario, frame in (("raw", departures), ("lag_corrected", apply_sampling_lags(departures, lags))):
        name = f"{scenario}{label}"
        animal, null, _, order_w = analyse_scenario(frame, events, loms, rng, n_permutations, n_bootstrap)
        per_animal.append(animal.assign(scenario=name))
        nulls.append(null.assign(scenario=name))
        w.append(order_w.assign(scenario=name))
    return (pd.concat(per_animal, ignore_index=True), pd.concat(nulls, ignore_index=True),
            pd.concat(w, ignore_index=True))


def _shares_wide(per_animal: pd.DataFrame, prefix: str) -> pd.DataFrame:
    shares = per_animal[["lom", "scenario", "share", "share_lo", "share_hi", "expected_share"]].rename(
        columns={"share": f"{prefix}_share", "share_lo": f"{prefix}_share_lo",
                 "share_hi": f"{prefix}_share_hi", "expected_share": f"{prefix}_expected_share"})
    return widen_by_scenario(shares, [f"{prefix}_share", f"{prefix}_share_lo", f"{prefix}_share_hi",
                           f"{prefix}_expected_share"])


# ┌────────────────────────────────────────────────────────────┐
# │ Shade  « first into the tree patches »                     │
# └────────────────────────────────────────────────────────────┘


def run_shade(data, lags: pd.DataFrame, rng: np.random.Generator, n_permutations: int,
              n_bootstrap: int) -> dict[str, pd.DataFrame]:
    loms = list(data.loms)
    occupancy = compute_patch_occupancy(data)
    episodes = detect_shade_episodes(data, occupancy)
    has_fix = np.isfinite(data.x_m) & np.isfinite(data.y_m)
    arrivals = compute_shade_arrivals(occupancy, episodes, has_fix, loms)
    per_animal, nulls, w = run_dumont_scenarios(arrivals, episodes, lags, loms, rng, n_permutations,
                                                n_bootstrap)
    hot = episodes.assign(is_group_movement=episodes["is_hot"])
    if hot["is_group_movement"].sum() >= 2:
        extra = run_dumont_scenarios(arrivals, hot, lags, loms, rng, n_permutations, n_bootstrap, "_hot")
        per_animal, nulls, w = (pd.concat([a, b], ignore_index=True)
                                for a, b in zip((per_animal, nulls, w), extra, strict=True))
    return {"episodes": episodes, "arrivals": arrivals, "per_animal": per_animal, "nulls": nulls, "w": w,
            "wide": _shares_wide(per_animal, "shade_first")}


# ┌────────────────────────────────────────────────────────────┐
# │ Fence retreat  « first away, and do the others follow »    │
# └────────────────────────────────────────────────────────────┘


def run_retreat(data, lags: pd.DataFrame, rng: np.random.Generator, n_permutations: int,
                n_bootstrap: int) -> dict[str, pd.DataFrame]:
    loms = list(data.loms)
    episodes = detect_fence_episodes(data.outside, data.minutes, data.is_gps_reduced)
    entries = compute_zone_entries(data.outside, data.audio, data.pulse, episodes, loms)
    exits = compute_zone_exits(data.outside, data.audio, data.pulse, episodes, entries)
    per_animal, nulls, w = run_dumont_scenarios(exits, episodes, lags, loms, rng, n_permutations, n_bootstrap)
    retreats = summarise_retreats(exits, episodes)
    wide = _shares_wide(per_animal, "retreat_first").merge(
        summarise_retreat_per_heifer(exits, retreats), on="lom", how="left")
    return {"exits": exits, "retreats": retreats, "per_animal": per_animal, "nulls": nulls, "w": w,
            "tests": run_retreat_tests(exits, retreats, rng, n_permutations), "wide": wide}


# ┌────────────────────────────────────────────────────────────┐
# │ Turns  « directional correlation delays »                  │
# └────────────────────────────────────────────────────────────┘


def run_turns(data, tracks: HerdTracks, lags: pd.DataFrame, rng: np.random.Generator, n_permutations: int,
              n_bootstrap: int) -> dict[str, pd.DataFrame]:
    loms = list(data.loms)
    headings = compute_headings(data.x_m, data.y_m, tracks.speed, tracks.is_excluded)
    day_codes = pd.factorize(data.minutes.floor("D"))[0]
    sums = compute_correlation_sums(headings, day_codes)
    lag_s = lags.set_index("lom")["lag_s"].reindex(loms).to_numpy(dtype=float)
    result = analyse_turns(sums, lag_s, rng, n_permutations, n_bootstrap)
    delays = pd.DataFrame(result["delay"], index=loms, columns=loms)
    correlation = pd.DataFrame({"lag_min": sums.lags,
                                "mean_correlation": np.nanmean(result["correlation"], axis=(1, 2))})
    return {"scores": summarise_turn_scores(result, loms, lag_s), "delays": delays,
            "correlation": correlation, "test": pd.DataFrame([result["test"]])}
