# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.fence_interactions                                 ║
# ║  « one visit to the fence, one row »                             ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  The collar counters give tones and pulses per fix. Cue minutes  ║
# ║  of one heifer close in time are one interaction; it is pulsed   ║
# ║  when at least one pulse fell in it. Exposure is how often a     ║
# ║  heifer interacts, efficiency how often the tone alone ends it   ║
# ║  (Lee et al. 2018; Hamidi et al. 2024). Learning is read         ║
# ║  against the interaction number, not the calendar day (Campbell  ║
# ║  et al. 2018).                                                   ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Fence interactions per heifer: exposure, efficiency and learning curves."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    CUE_MAX_FIX_GAP_MIN,
    FENCE_ZONE_CODES,
    INTERACTION_GAP_MIN,
    LEARNING_BLOCK,
    LEARNING_MIN_INTERACTIONS,
    SR_CRITERION,
)

log = logging.getLogger(__name__)

INTERACTION_COLUMNS: list[str] = ["lom", "number", "i0", "i1", "onset_utc", "day", "audio", "pulse", "is_pulsed"]

# ┌────────────────────────────────────────────────────────────┐
# │ Interactions  « runs of cue minutes per heifer »           │
# └────────────────────────────────────────────────────────────┘


def find_untimed_fixes(has_fix: np.ndarray, max_gap_min: int = CUE_MAX_FIX_GAP_MIN) -> np.ndarray:
    """Fixes that follow a gap of more than ``max_gap_min`` minutes, per animal.

    Their cue counts cover the whole gap, so the cues cannot be placed in time.
    """
    is_untimed = np.zeros(has_fix.shape, dtype=bool)
    for a in range(has_fix.shape[1]):
        fixes = np.flatnonzero(has_fix[:, a])
        is_untimed[fixes[1:][np.diff(fixes) > max_gap_min], a] = True
    return is_untimed


def detect_interactions(audio: np.ndarray, pulse: np.ndarray, is_untimed: np.ndarray,
                        minutes: pd.DatetimeIndex, loms: list[str],
                        gap_min: int = INTERACTION_GAP_MIN) -> pd.DataFrame:
    """One row per fence interaction, numbered per heifer in time order."""
    audio, pulse = np.nan_to_num(audio), np.nan_to_num(pulse)
    is_cue = ((audio > 0) | (pulse > 0)) & ~is_untimed
    rows = []
    for a, lom in enumerate(loms):
        hits = np.flatnonzero(is_cue[:, a])
        if len(hits) == 0:
            continue
        breaks = np.diff(hits) > gap_min
        starts, ends = hits[np.r_[True, breaks]], hits[np.r_[breaks, True]]
        for number, (i0, i1) in enumerate(zip(starts, ends, strict=True), start=1):
            n_audio = float(audio[i0:i1 + 1, a].sum())
            n_pulse = float(pulse[i0:i1 + 1, a].sum())
            rows.append((lom, number, int(i0), int(i1), minutes[i0], minutes[i0].floor("D"), n_audio, n_pulse,
                         n_pulse > 0))
    interactions = pd.DataFrame(rows, columns=INTERACTION_COLUMNS)
    log.info("fence interactions: %d, %.1f %% pulsed", len(interactions), 100.0 * interactions["is_pulsed"].mean())
    return interactions


# ┌────────────────────────────────────────────────────────────┐
# │ Exposure  « how often, and how it ends »                   │
# └────────────────────────────────────────────────────────────┘


def summarise_exposure(interactions: pd.DataFrame, audio: np.ndarray, pulse: np.ndarray, outside: np.ndarray,
                       is_untimed: np.ndarray, loms: list[str], n_days: float) -> pd.DataFrame:
    """Per heifer: cue totals, interactions per day, zone use and how interactions end."""
    audio, pulse = np.nan_to_num(audio), np.nan_to_num(pulse)
    has_fix = outside >= 0
    in_zone = np.isin(outside, FENCE_ZONE_CODES)
    table = pd.DataFrame({
        "lom": loms, "audio_total": audio.sum(axis=0), "pulse_total": pulse.sum(axis=0),
        "untimed_audio_share": (audio * is_untimed).sum(axis=0) / np.maximum(audio.sum(axis=0), 1.0),
        "zone_share": in_zone.sum(axis=0) / np.maximum(has_fix.sum(axis=0), 1),
        "zone_hours": in_zone.sum(axis=0) / 60.0})
    table["audio_per_day"] = table["audio_total"] / n_days
    table["pulse_per_day"] = table["pulse_total"] / n_days
    table["pulse_per_audio"] = table["pulse_total"] / table["audio_total"].where(table["audio_total"] > 0)
    per_heifer = interactions.groupby("lom").agg(
        n_interactions=("is_pulsed", "size"), pulsed_share=("is_pulsed", "mean"),
        tones_per_interaction=("audio", "mean"), median_tones=("audio", "median"))
    table = table.merge(per_heifer.reset_index(), on="lom", how="left").fillna({"n_interactions": 0})
    table["interactions_per_day"] = table["n_interactions"] / n_days
    zone_hours = table["zone_hours"].where(table["zone_hours"] > 0)
    table["interactions_per_zone_hour"] = table["n_interactions"] / zone_hours
    table["pulses_per_zone_hour"] = table["pulse_total"] / zone_hours
    return table


def summarise_weeks(interactions: pd.DataFrame, start: pd.Timestamp) -> pd.DataFrame:
    """Per heifer and trial week: interactions, tones and pulsed share."""
    week = ((interactions["onset_utc"] - start).dt.days // 7 + 1).rename("week")
    table = interactions.groupby(["lom", week]).agg(
        n_interactions=("is_pulsed", "size"), audio=("audio", "sum"), pulse=("pulse", "sum"),
        pulsed_share=("is_pulsed", "mean"), tones_per_interaction=("audio", "mean"))
    return table.reset_index()


# ┌────────────────────────────────────────────────────────────┐
# │ Learning  « pulsed share against the interaction number »  │
# └────────────────────────────────────────────────────────────┘


def summarise_learning(interactions: pd.DataFrame, block: int = LEARNING_BLOCK,
                       criterion: float = SR_CRITERION) -> pd.DataFrame:
    """Per heifer: early and late pulsed share, logistic slope, cost to learn.

    ``learning_slope`` is the change in the log odds of a pulse per tenfold
    increase in the interaction number (negative: she learns).
    ``cost_to_learn`` is the pulses up to the end of the first ``block``
    interactions in a row of which ``criterion`` ended on the tone alone;
    NaN when she never gets there.
    """
    rows = []
    for lom, g in interactions.sort_values("number").groupby("lom"):
        pulsed = g["is_pulsed"].to_numpy(dtype=float)
        row = {"lom": lom, "early_pulsed_share": np.nan, "late_pulsed_share": np.nan,
               "learning_slope": np.nan, **find_criterion(pulsed, g["pulse"].to_numpy(), block, criterion)}
        if len(pulsed) >= LEARNING_MIN_INTERACTIONS:
            row["early_pulsed_share"] = float(pulsed[:block].mean())
            row["late_pulsed_share"] = float(pulsed[-block:].mean())
            row["learning_slope"] = fit_logistic_slope(np.log10(g["number"].to_numpy(dtype=float)), pulsed)
        rows.append(row)
    table = pd.DataFrame(rows)
    table["pulsed_share_change"] = table["late_pulsed_share"] - table["early_pulsed_share"]
    return table


def find_criterion(pulsed: np.ndarray, pulses: np.ndarray, block: int, criterion: float) -> dict[str, float]:
    """Interactions and pulses up to the first block that meets the criterion."""
    if len(pulsed) < block:
        return {"interactions_to_criterion": np.nan, "cost_to_learn": np.nan}
    success = np.convolve(1.0 - pulsed, np.ones(block), mode="valid") / block
    reached = np.flatnonzero(success >= criterion - 1e-12)
    if len(reached) == 0:
        return {"interactions_to_criterion": np.nan, "cost_to_learn": np.nan}
    end = int(reached[0]) + block
    return {"interactions_to_criterion": float(end), "cost_to_learn": float(pulses[:end].sum())}


def fit_logistic_slope(x: np.ndarray, y: np.ndarray, ridge: float = 1e-3, n_iterations: int = 50) -> float:
    """Slope of a logistic regression of ``y`` (0/1) on ``x``, by Newton steps.

    The small ridge keeps the fit finite when a heifer has no pulse at all
    in part of the range.
    """
    design = np.c_[np.ones(len(x)), x - x.mean()]
    beta = np.zeros(2)
    penalty = ridge * np.eye(2)
    for _ in range(n_iterations):
        p = 1.0 / (1.0 + np.exp(-design @ beta))
        gradient = design.T @ (y - p) - penalty @ beta
        hessian = (design * (p * (1.0 - p))[:, None]).T @ design + penalty
        step = np.linalg.solve(hessian, gradient)
        beta = beta + step
        if np.abs(step).max() < 1e-8:
            break
    return float(beta[1])


def build_learning_curves(interactions: pd.DataFrame, block: int = LEARNING_BLOCK) -> pd.DataFrame:
    """Pulsed share per consecutive block of interactions, per heifer."""
    index = ((interactions["number"] - 1) // block).rename("block")
    curves = interactions.groupby(["lom", index]).agg(
        n_interactions=("is_pulsed", "size"), pulsed_share=("is_pulsed", "mean"),
        last_number=("number", "max"), tones_per_interaction=("audio", "mean")).reset_index()
    return curves[curves["n_interactions"] == block].reset_index(drop=True)
