# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.social_cues                                        ║
# ║  « does a herd mate's tone warn the others »                     ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  A heifer close to the fence can be warned by her own collar or  ║
# ║  by the tone of a herd mate next to her. For every uncued        ║
# ║  minute in the pre-fence zone with a cued mate nearby, her next  ║
# ║  minutes are scored: she leaves the zone without a cue, she is   ║
# ║  cued herself, or she stays. The same is scored for zone         ║
# ║  minutes in company while the whole herd is quiet. Sociability   ║
# ║  is how close she keeps to the others in general.                ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Responses to herd mates' cues, first cues, and sociability."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    ASSOCIATION_STEP_MIN,
    FENCE_ZONE_CODES,
    FIRST_CUE_WINDOW_MIN,
    SOCIAL_CUE_RANGE_M,
    SOCIAL_QUIET_MIN,
    SOCIAL_RESPONSE_MIN,
)
from davasus.vf.pair_bonds import compute_association

log = logging.getLogger(__name__)

CONDITIONS: tuple[str, str] = ("mate_cued", "quiet")
OUTCOMES: tuple[str, ...] = ("retreat", "own_cue", "stay")
MATE_DISTANCE_BANDS_M: tuple[float, ...] = (0.0, 5.0, 10.0, SOCIAL_CUE_RANGE_M)

# ┌────────────────────────────────────────────────────────────┐
# │ Responses  « what an uncued heifer does next »             │
# └────────────────────────────────────────────────────────────┘


def find_social_events(outside: np.ndarray, is_cue: np.ndarray, pulse: np.ndarray, x_m: np.ndarray,
                       y_m: np.ndarray, is_excluded: np.ndarray, loms: list[str]) -> pd.DataFrame:
    """One row per uncued zone minute of a heifer with company, and what follows.

    ``condition`` is ``mate_cued`` when a heifer within
    ``SOCIAL_CUE_RANGE_M`` has a cue in that minute, ``quiet`` when another
    uncued heifer in the zone is that close and nobody in the herd had a cue
    in the ``SOCIAL_QUIET_MIN`` minutes up to then. ``outcome`` over the next
    ``SOCIAL_RESPONSE_MIN`` minutes: ``own_cue``, else ``retreat`` (out of
    the zone at the end), else ``stay``. ``mate_distance_m`` and
    ``is_mate_pulsed`` describe the nearest cued mate.
    """
    n_minutes = outside.shape[0]
    in_zone = np.isin(outside, FENCE_ZONE_CODES)
    has_fix = np.isfinite(x_m) & np.isfinite(y_m) & (outside >= 0)
    recent_cue = _any_within(is_cue, SOCIAL_RESPONSE_MIN - 1, 0)
    is_candidate = in_zone & has_fix & ~recent_cue & ~np.asarray(is_excluded, dtype=bool)[:, None]
    herd_quiet = ~_any_within(is_cue, SOCIAL_QUIET_MIN, 0).any(axis=1)
    is_pulse = np.nan_to_num(pulse) > 0
    rows = []
    for t in np.flatnonzero(is_candidate.any(axis=1)):
        stop = t + SOCIAL_RESPONSE_MIN
        if stop >= n_minutes:
            continue
        distance = _compute_distances(x_m[t], y_m[t])
        to_cued = np.where(is_cue[t][None, :], distance, np.inf)
        nearest_cued = to_cued.argmin(axis=1)
        cued_distance = to_cued.min(axis=1)
        zone_near = (distance[:, in_zone[t] & ~is_cue[t]] <= SOCIAL_CUE_RANGE_M).any(axis=1)
        for a in np.flatnonzero(is_candidate[t] & has_fix[stop]):
            if cued_distance[a] <= SOCIAL_CUE_RANGE_M:
                condition = CONDITIONS[0]
            elif herd_quiet[t] and zone_near[a]:
                condition = CONDITIONS[1]
            else:
                continue
            if is_cue[t + 1:stop + 1, a].any():
                outcome = OUTCOMES[1]
            elif not in_zone[stop, a]:
                outcome = OUTCOMES[0]
            else:
                outcome = OUTCOMES[2]
            is_mate_cued = condition == CONDITIONS[0]
            rows.append((loms[a], int(t), condition, outcome, cued_distance[a] if is_mate_cued else np.nan,
                         bool(is_mate_cued and is_pulse[t, nearest_cued[a]])))
    events = pd.DataFrame(rows, columns=["lom", "i", "condition", "outcome", "mate_distance_m",
                                         "is_mate_pulsed"])
    log.info("social cue events: %d with a cued mate, %d quiet", int((events["condition"] == CONDITIONS[0]).sum()),
             int((events["condition"] == CONDITIONS[1]).sum()))
    return events


def _any_within(flags: np.ndarray, before: int, after: int) -> np.ndarray:
    """True where ``flags`` is set in the minutes ``t - before`` to ``t + after``."""
    out = flags.copy()
    for shift in range(1, before + 1):
        out[shift:] |= flags[:-shift]
    for shift in range(1, after + 1):
        out[:-shift] |= flags[shift:]
    return out


def _compute_distances(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Pairwise distances in one minute; infinite to oneself and where a fix is missing."""
    with np.errstate(invalid="ignore"):
        distance = np.hypot(x[:, None] - x[None, :], y[:, None] - y[None, :])
    distance[~np.isfinite(distance)] = np.inf
    np.fill_diagonal(distance, np.inf)
    return distance


def summarise_social_response(events: pd.DataFrame, loms: list[str]) -> pd.DataFrame:
    """Per heifer: outcome shares after a mate's cue and in quiet company, and their difference."""
    counts = events.groupby(["lom", "condition", "outcome"]).size().unstack("outcome", fill_value=0)
    counts = counts.reindex(columns=list(OUTCOMES), fill_value=0)
    shares = counts.div(counts.sum(axis=1), axis=0)
    shares["n"] = counts.sum(axis=1)
    wide = shares.unstack("condition")
    wide.columns = [f"{outcome}_{condition}" for outcome, condition in wide.columns]
    wide = wide.reindex(loms)
    for outcome in OUTCOMES:
        wide[f"{outcome}_gain"] = wide[f"{outcome}_{CONDITIONS[0]}"] - wide[f"{outcome}_{CONDITIONS[1]}"]
    return wide.rename_axis("lom").reset_index()


def summarise_mate_cues(events: pd.DataFrame) -> pd.DataFrame:
    """Outcome shares after a mate's cue by her distance and by whether she was pulsed."""
    cued = events[events["condition"] == CONDITIONS[0]]
    band = pd.cut(cued["mate_distance_m"], MATE_DISTANCE_BANDS_M, include_lowest=True).astype(str)
    kind = np.where(cued["is_mate_pulsed"], "pulse", "tone only")
    rows = []
    for name, groups in (("mate distance (m)", band), ("mate's cue", pd.Series(kind, index=cued.index))):
        counts = cued.groupby([groups, "outcome"]).size().unstack("outcome", fill_value=0)
        counts = counts.reindex(columns=list(OUTCOMES), fill_value=0)
        shares = counts.div(counts.sum(axis=1), axis=0)
        rows.append(shares.assign(n=counts.sum(axis=1), split=name).rename_axis("level").reset_index())
    quiet = events.loc[events["condition"] == CONDITIONS[1], "outcome"].value_counts(normalize=True)
    rows.append(pd.DataFrame([{"level": "quiet company", "split": "reference",
                               "n": int((events["condition"] == CONDITIONS[1]).sum()),
                               **{outcome: float(quiet.get(outcome, 0.0)) for outcome in OUTCOMES}}]))
    return pd.concat(rows, ignore_index=True)[["split", "level", "n", *OUTCOMES]]


# ┌────────────────────────────────────────────────────────────┐
# │ First cues  « who meets the fence before the others »      │
# └────────────────────────────────────────────────────────────┘


def flag_first_cues(interactions: pd.DataFrame, is_cue: np.ndarray, loms: list[str],
                    window_min: int = FIRST_CUE_WINDOW_MIN) -> pd.Series:
    """True for interactions that start with no other heifer cued in the minutes before."""
    column = pd.Index(loms).get_indexer(interactions["lom"])
    onset = interactions["i0"].to_numpy()
    is_first = np.ones(len(interactions), dtype=bool)
    for k, (i0, a) in enumerate(zip(onset, column, strict=True)):
        others = is_cue[max(0, i0 - window_min):i0 + 1].any(axis=0)
        others[a] = False
        is_first[k] = not others.any()
    return pd.Series(is_first, index=interactions.index, name="is_first_cue")


# ┌────────────────────────────────────────────────────────────┐
# │ Sociability  « how close a heifer keeps to the herd »      │
# └────────────────────────────────────────────────────────────┘


def compute_sociability(x_m: np.ndarray, y_m: np.ndarray, is_used: np.ndarray, loms: list[str],
                        step_min: int = ASSOCIATION_STEP_MIN) -> pd.DataFrame:
    """Per heifer: mean association, nearest-neighbour distance, distance to the herd centre."""
    association = np.nanmean(compute_association(x_m, y_m, is_used), axis=1)
    sample = np.flatnonzero(is_used)[::step_min]
    x, y = x_m[sample], y_m[sample]
    has_herd = np.isfinite(x).sum(axis=1) > 1
    x, y = x[has_herd], y[has_herd]
    distance = np.hypot(x[:, :, None] - x[:, None, :], y[:, :, None] - y[:, None, :])
    distance[:, np.arange(len(loms)), np.arange(len(loms))] = np.inf
    nearest = np.where(np.isfinite(distance), distance, np.inf).min(axis=2)
    nearest[~np.isfinite(nearest)] = np.nan
    centre = np.hypot(x - np.nanmedian(x, axis=1, keepdims=True), y - np.nanmedian(y, axis=1, keepdims=True))
    return pd.DataFrame({"lom": loms, "association": association,
                         "nearest_neighbour_m": np.nanmedian(nearest, axis=0),
                         "centre_distance_m": np.nanmedian(centre, axis=0)})
