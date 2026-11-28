# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.fence_context                                      ║
# ║  « what happens at the line, and around it »                     ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  The collar reports each minute's largest distance to the        ║
# ║  fence. With it an interaction gets a depth, an approach (she    ║
# ║  came from further inside) and the line it happened at: one      ║
# ║  that the previous parcel already had, or a new one. The herd    ║
# ║  around her at the onset says whether she was in the crowd, at   ║
# ║  its edge, or while the herd travelled.                          ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Depth, approach, line and herd context of each fence interaction."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import shapely
from shapely.geometry import LineString

from davasus.vf.constants import (
    APPROACH_FROM_M,
    APPROACH_LOOKBACK_MIN,
    BREAKOUT_CODES,
    BREAKOUT_SEARCH_MIN,
    CONTEXT_MIN_INTERACTIONS,
    CONTEXT_RANGE_M,
    DEEP_INTERACTION_M,
    DISTANCE_PROFILE_EDGES_M,
    FENCE_DISTANCE_LIMIT_M,
    FENCE_DISTANCE_UNIT_M,
    LINE_MAX_DISTANCE_M,
    NEW_LINE_EARLY_H,
    NEW_LINE_GAP_M,
    PERMUTATIONS,
    TRAVEL_SPEED_M_MIN,
)

log = logging.getLogger(__name__)

CONTEXT_MEASURES: dict[str, str] = {
    "n_mates_near": f"herd mates within {CONTEXT_RANGE_M:.0f} m",
    "centre_distance_m": "distance to the herd centre (m)",
    "is_herd_travelling": "share while the herd travels",
    "mates_approach_m": "mates' approach to the fence in 2 min (m)",
    "own_approach_m": "own approach to the fence in 1 min (m)",
    "is_first_cue": "share as first cue",
}

# ┌────────────────────────────────────────────────────────────┐
# │ Distance  « metres to the fence, negative inside »         │
# └────────────────────────────────────────────────────────────┘


def convert_fence_distance(raw: np.ndarray) -> np.ndarray:
    """Collar distance to the fence in metres; NaN for sentinels."""
    metres = np.asarray(raw, dtype=float) * FENCE_DISTANCE_UNIT_M
    metres[~(np.abs(metres) <= FENCE_DISTANCE_LIMIT_M)] = np.nan
    return metres


def build_distance_profile(distance_m: np.ndarray, audio: np.ndarray, pulse: np.ndarray,
                           is_untimed: np.ndarray) -> pd.DataFrame:
    """Share of fixes with a tone and with a pulse per band of distance to the fence."""
    ok = np.isfinite(distance_m) & ~is_untimed
    band = pd.cut(distance_m[ok], list(DISTANCE_PROFILE_EDGES_M))
    table = pd.DataFrame({"band": band, "tone": np.nan_to_num(audio)[ok] > 0, "pulse": np.nan_to_num(pulse)[ok] > 0})
    profile = table.groupby("band", observed=True).agg(n_fixes=("tone", "size"), tone_share=("tone", "mean"),
                                                       pulse_share=("pulse", "mean")).reset_index()
    profile["from_m"] = [interval.left for interval in profile["band"]]
    profile["to_m"] = [interval.right for interval in profile["band"]]
    return profile.drop(columns="band")[["from_m", "to_m", "n_fixes", "tone_share", "pulse_share"]]


# ┌────────────────────────────────────────────────────────────┐
# │ Interactions  « depth, approach, line, herd around her »   │
# └────────────────────────────────────────────────────────────┘


def describe_interactions(interactions: pd.DataFrame, data, distance_m: np.ndarray,
                          herd_speed: np.ndarray) -> pd.DataFrame:
    """Add depth, approach, breakout, line and herd-context columns to the interactions."""
    loms = list(data.loms)
    animal = pd.Index(loms).get_indexer(interactions["lom"])
    i0, i1 = interactions["i0"].to_numpy(), interactions["i1"].to_numpy()
    n_minutes = len(distance_m)
    rows = []
    for a, start, stop in zip(animal, i0, i1, strict=True):
        before = distance_m[max(start - APPROACH_LOOKBACK_MIN, 0):start, a]
        during = distance_m[start:stop + 1, a]
        after = data.outside[start:min(stop + BREAKOUT_SEARCH_MIN, n_minutes - 1) + 1, a]
        rows.append((_nan_reduce(np.nanmin, before), _nan_reduce(np.nanmax, during),
                     bool(np.isin(after, list(BREAKOUT_CODES)).any()),
                     *_describe_herd(data.x_m, data.y_m, distance_m, start, a)))
    out = interactions.copy()
    columns = ["furthest_before_m", "depth_m", "is_breakout", "n_mates_near", "centre_distance_m",
               "mates_approach_m", "own_approach_m"]
    out[columns] = pd.DataFrame(rows, columns=columns, index=out.index)
    out["is_approach"] = (out["furthest_before_m"] <= -APPROACH_FROM_M).where(out["furthest_before_m"].notna())
    out["is_deep"] = (out["depth_m"] > DEEP_INTERACTION_M).where(out["depth_m"].notna())
    out["is_herd_travelling"] = np.nan_to_num(herd_speed)[i0] >= TRAVEL_SPEED_M_MIN
    return out.join(assign_lines(out, data, animal))


def _nan_reduce(function, values: np.ndarray) -> float:
    return float(function(values)) if np.isfinite(values).any() else float("nan")


def _describe_herd(x_m: np.ndarray, y_m: np.ndarray, distance_m: np.ndarray, t: int,
                   a: int) -> tuple[float, float, float, float]:
    """Mates near her, her distance to the herd centre, and who was moving towards the fence."""
    away = np.hypot(x_m[t] - x_m[t, a], y_m[t] - y_m[t, a])
    away[a] = np.nan
    is_near = away <= CONTEXT_RANGE_M
    centre = np.hypot(np.nanmedian(x_m[t]) - x_m[t, a], np.nanmedian(y_m[t]) - y_m[t, a])
    mates = distance_m[t, is_near] - distance_m[max(t - 2, 0), is_near]
    return (float(is_near.sum()), float(centre), _nan_reduce(np.nanmean, mates),
            float(distance_m[t, a] - distance_m[max(t - 1, 0), a]))


def assign_lines(interactions: pd.DataFrame, data, animal: np.ndarray) -> pd.DataFrame:
    """Per interaction: hours since the parcel started and whether its fence line is new.

    ``is_new_line`` is NaN in the first parcel, without a position, and
    further than ``LINE_MAX_DISTANCE_M`` from the active virtual fence.
    """
    lines = [build_fence_line(data.parcel_outlines.get(label)) for label in data.events["label"]]
    i0 = interactions["i0"].to_numpy()
    event = np.asarray(data.event_index)[i0]
    x, y = data.x_m[i0, animal], data.y_m[i0, animal]
    starts = data.events["start_utc"]
    hours = [(onset - starts.iloc[e]) / pd.Timedelta(hours=1) for onset, e in zip(interactions["onset_utc"], event,
                                                                                 strict=True)]
    is_new = np.full(len(interactions), np.nan)
    for k, e in enumerate(event):
        if e < 1 or lines[e] is None or lines[e - 1] is None or not np.isfinite(x[k]):
            continue
        point = shapely.Point(x[k], y[k])
        if point.distance(lines[e]) > LINE_MAX_DISTANCE_M:
            continue
        on_line = lines[e].interpolate(lines[e].project(point))
        is_new[k] = float(on_line.distance(lines[e - 1]) > NEW_LINE_GAP_M)
    return pd.DataFrame({"event": event, "hours_in_parcel": hours, "is_new_line": is_new}, index=interactions.index)


def build_fence_line(outline: tuple[np.ndarray, np.ndarray, np.ndarray] | None):
    """Virtual-fence edges of a parcel outline as one line; None without an outline."""
    if outline is None:
        return None
    x, y, is_vf_edge = outline
    n = len(x)
    segments = [LineString([(x[k], y[k]), (x[(k + 1) % n], y[(k + 1) % n])]) for k in range(n) if is_vf_edge[k]]
    return shapely.line_merge(shapely.union_all(segments)) if segments else None


# ┌────────────────────────────────────────────────────────────┐
# │ Per heifer  « shares by approach, depth and line »         │
# └────────────────────────────────────────────────────────────┘


def summarise_context(described: pd.DataFrame) -> pd.DataFrame:
    """Per heifer: approach share, pulsed share by approach and by line, depth and breakouts."""
    is_early = described["hours_in_parcel"] < NEW_LINE_EARLY_H
    is_new = described["is_new_line"] == 1.0
    is_same = described["is_new_line"] == 0.0
    pulsed = described[described["is_pulsed"]]
    parts = {
        "approach_share": described.groupby("lom")["is_approach"].mean(),
        "pulsed_share_approach": _share(described[described["is_approach"] == True]),  # noqa: E712
        "pulsed_share_at_line": _share(described[described["is_approach"] == False]),  # noqa: E712
        "pulsed_share_new_line": _share(described[is_new]),
        "pulsed_share_same_line": _share(described[is_same]),
        "pulsed_share_new_line_early": _share(described[is_new & is_early]),
        "pulsed_share_new_line_later": _share(described[is_new & ~is_early]),
        "deep_share": described.groupby("lom")["is_deep"].mean(),
        "pulses_per_pulsed": pulsed.groupby("lom")["pulse"].mean(),
        "multi_pulse_share": (pulsed["pulse"] >= 2).groupby(pulsed["lom"]).mean(),
        "n_breakouts": described.groupby("lom")["is_breakout"].sum(),
    }
    table = pd.DataFrame(parts)
    table["approach_gain"] = table["pulsed_share_approach"] - table["pulsed_share_at_line"]
    table["new_line_gain"] = table["pulsed_share_new_line"] - table["pulsed_share_same_line"]
    table["new_line_early_gain"] = table["pulsed_share_new_line_early"] - table["pulsed_share_new_line_later"]
    for name in CONTEXT_MEASURES:
        means = _mean_by_outcome(described, name)
        table[f"{name}_tone_only"], table[f"{name}_pulsed"] = means[False], means[True]
        table[f"{name}_gain"] = means[True] - means[False]
    return table.rename_axis("lom").reset_index()


def _share(subset: pd.DataFrame, min_interactions: int = CONTEXT_MIN_INTERACTIONS) -> pd.Series:
    grouped = subset.groupby("lom")["is_pulsed"].agg(["mean", "size"])
    return grouped["mean"].where(grouped["size"] >= min_interactions)


def _mean_by_outcome(described: pd.DataFrame, column: str) -> pd.DataFrame:
    """Mean of ``column`` over tone-only and over pulsed interactions, per heifer."""
    values = described.assign(value=described[column].astype(float)).dropna(subset=["value"])
    grouped = values.groupby(["lom", "is_pulsed"])["value"].agg(["mean", "size"]).unstack("is_pulsed")
    means = grouped["mean"].reindex(columns=[False, True])
    sizes = grouped["size"].reindex(columns=[False, True]).fillna(0)
    return means.where(sizes.min(axis=1) >= CONTEXT_MIN_INTERACTIONS)


def run_context_tests(per_heifer: pd.DataFrame, test, rng: np.random.Generator,
                      n_permutations: int = PERMUTATIONS) -> pd.DataFrame:
    """Sign-flip tests of the per-heifer gains; ``test`` is the paired-shift test."""
    labels = {"approach_gain": "pulsed share: approach minus already at the line",
              "new_line_gain": "pulsed share: new line minus line kept from the last parcel",
              "new_line_early_gain": f"pulsed share at a new line: first {NEW_LINE_EARLY_H} h minus later",
              **{f"{name}_gain": f"{label}: pulsed minus tone only" for name, label in CONTEXT_MEASURES.items()}}
    return pd.DataFrame([{"test": label, **test(per_heifer[column].to_numpy(dtype=float), rng, n_permutations)}
                         for column, label in labels.items()])
