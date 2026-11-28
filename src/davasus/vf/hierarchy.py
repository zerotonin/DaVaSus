# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.hierarchy                                          ║
# ║  « one rank per heifer from the measures that showed leaders »   ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Each heifer's rank is averaged over four movement measures      ║
# ║  (first mover, front of formation, pulls, turns) and three       ║
# ║  exploration measures (first at the fence, into a new paddock,   ║
# ║  away from the fence). Shade arrival and pre-departure activity  ║
# ║  showed no leaders and are left out.                             ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Combined leadership hierarchy and the cues each rank receives."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from davasus.vf.constants import HIERARCHY_TIER_LIMITS
from davasus.vf.leadership_context import orient_measures

MOVEMENT_MEASURES: tuple[str, ...] = ("dumont_share", "formation_asymmetry", "lead_share", "turn_lead_residual")
EXPLORATION_MEASURES: tuple[str, ...] = ("fence_first_share", "paddock_lead", "retreat_first_share")
TIERS: tuple[str, ...] = ("top", "second row", "middle", "follower")
RANK_COLUMNS: tuple[str, ...] = ("mean_rank", "movement_rank", "exploration_rank")
CUE_COLUMNS: tuple[str, ...] = ("audio_total", "pulse_total")


def compute_combined_hierarchy(per_heifer: pd.DataFrame) -> pd.DataFrame:
    """Rank 1 (leads most) to n from the mean rank over the leading measures.

    Args:
        per_heifer: One row per heifer with ``lom`` and the per-heifer
            columns of the leadership runs; measures that are missing are
            skipped.
    """
    table = orient_measures(per_heifer)
    present = [m for m in MOVEMENT_MEASURES + EXPLORATION_MEASURES if m in table]
    if not present:
        raise ValueError("no leading measure in the table; run the leadership commands first")
    ranks = table[present].rank(ascending=False)
    out = pd.DataFrame({"lom": table["lom"]})
    out["mean_rank"] = ranks.mean(axis=1)
    out["movement_rank"] = ranks[[m for m in MOVEMENT_MEASURES if m in ranks]].mean(axis=1)
    out["exploration_rank"] = ranks[[m for m in EXPLORATION_MEASURES if m in ranks]].mean(axis=1)
    out["rank"] = out["mean_rank"].rank(method="first").astype(int)
    out["tier"] = assign_tiers(out["rank"])
    out = out.join(ranks.add_suffix("_rank"))
    return out.sort_values("rank").reset_index(drop=True)


def assign_tiers(rank: pd.Series) -> list[str]:
    return [TIERS[int(np.searchsorted(HIERARCHY_TIER_LIMITS, r))] for r in rank]


def add_cue_totals(hierarchy: pd.DataFrame, loms: list[str], audio: np.ndarray, pulse: np.ndarray) -> pd.DataFrame:
    """Audio cues and pulses summed over the trial, from (minute, animal) cubes."""
    totals = pd.DataFrame({"lom": loms, "audio_total": np.nansum(audio, axis=0),
                           "pulse_total": np.nansum(pulse, axis=0)})
    totals["pulse_per_audio"] = totals["pulse_total"] / totals["audio_total"].where(totals["audio_total"] > 0)
    return hierarchy.merge(totals, on="lom", how="left")


def correlate_ranks_with_cues(hierarchy: pd.DataFrame) -> pd.DataFrame:
    """Spearman rho of each rank (overall, movement, exploration) with the cue totals."""
    rows = []
    for rank in RANK_COLUMNS:
        for cue in (*CUE_COLUMNS, "pulse_per_audio"):
            rho, p = spearmanr(hierarchy[rank], hierarchy[cue], nan_policy="omit")
            rows.append({"rank": rank, "cue": cue, "rho": float(rho), "p": float(p)})
    return pd.DataFrame(rows)
