# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.formation_analysis                                 ║
# ║  « front, back, or just a late collar »                          ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Positional leadership tested three ways: front-rear asymmetry   ║
# ║  on raw and on lag-corrected positions, and the lag-free         ║
# ║  formation offset at zero herd speed. Day-block bootstrap CIs,   ║
# ║  a within-day label-shuffle null, split-half reliability and a   ║
# ║  heading check against fixed geographic GPS offsets. A role is   ║
# ║  called only when all three measures agree.                      ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Statistics and role calls for the herd formation."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from davasus.vf.constants import (
    BOOTSTRAP_DRAWS,
    FORMATION_SPEED_BANDS,
    PERMUTATIONS,
)
from davasus.vf.formation import (
    HEADING_SECTORS,
    FormationFrame,
    build_formation_frame,
    compute_arc_asymmetry,
    fit_formation_offsets,
)
from davasus.vf.initiation import HerdTracks, estimate_sampling_lags

log = logging.getLogger(__name__)

SCENARIOS: tuple[str, ...] = ("raw", "lag_corrected")
ROLE_ORDER: tuple[str, ...] = ("front", "probable front", "middle", "timing-dependent",
                               "probable rear", "rear")


@dataclass(frozen=True)
class FormationResult:
    """Per-heifer table, test summary and heading check."""

    per_heifer: pd.DataFrame
    tests: pd.DataFrame
    headings: pd.DataFrame
    lags: pd.DataFrame


def run_formation_analysis(
    tracks: HerdTracks,
    rng: np.random.Generator,
    n_permutations: int = PERMUTATIONS,
    n_bootstrap: int = BOOTSTRAP_DRAWS,
) -> FormationResult:
    lags = estimate_sampling_lags(tracks)
    frames = {"raw": build_formation_frame(tracks),
              "lag_corrected": build_formation_frame(tracks, lags["lag_s"].to_numpy())}
    tables, tests, headings = [], [], []
    for name, frame in frames.items():
        for band, (lo, hi) in FORMATION_SPEED_BANDS.items():
            subset = frame.select((frame.herd_speed >= lo) & (frame.herd_speed < hi))
            log.info("%s, %s: %d moving minutes", name, band, len(subset.minute_index))
            table, summary = summarise_band(subset, rng, n_permutations, n_bootstrap)
            tables.append(table.assign(scenario=name, band=band))
            tests.append(summary.assign(scenario=name, band=band))
            headings.append(check_headings(subset).assign(scenario=name, band=band))
    offsets = summarise_offsets(frames["raw"], rng, n_bootstrap)
    per_heifer = pd.concat(tables, ignore_index=True).merge(offsets, on="lom").merge(
        lags[["lom", "lag_s"]], on="lom")
    per_heifer = per_heifer.merge(classify_roles(per_heifer), on="lom")
    return FormationResult(per_heifer, pd.concat(tests, ignore_index=True),
                           pd.concat(headings, ignore_index=True), lags)


def summarise_band(frame: FormationFrame, rng: np.random.Generator, n_permutations: int,
                   n_bootstrap: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Asymmetry with day-block CIs, the label-shuffle null and split-half reliability."""
    table = compute_arc_asymmetry(frame)
    draws = bootstrap_asymmetry(frame, rng, n_bootstrap)
    table["asymmetry_lo"] = np.nanquantile(draws, 0.025, axis=0)
    table["asymmetry_hi"] = np.nanquantile(draws, 0.975, axis=0)
    observed = float(np.nanstd(table["asymmetry"]))
    null = np.array([np.nanstd(compute_arc_asymmetry(shuffle_within_days(frame, rng))["asymmetry"])
                     for _ in range(n_permutations)])
    odd, even = frame.day_codes % 2 == 1, frame.day_codes % 2 == 0
    split = spearmanr(compute_arc_asymmetry(frame.select(odd))["asymmetry"],
                      compute_arc_asymmetry(frame.select(even))["asymmetry"], nan_policy="omit")[0]
    edge = spearmanr(table["front_rate"] + table["rear_rate"], table["asymmetry"], nan_policy="omit")[0]
    summary = pd.DataFrame([{
        "n_minutes": len(frame.minute_index), "sd_asymmetry": observed,
        "null_mean": float(null.mean()), "null_hi": float(np.quantile(null, 0.975)),
        "p": float((np.sum(null >= observed) + 1.0) / (n_permutations + 1.0)),
        "split_half_rho": float(split), "edge_vs_asymmetry_rho": float(edge)}])
    return table, summary


def bootstrap_asymmetry(frame: FormationFrame, rng: np.random.Generator, n_draws: int) -> np.ndarray:
    days = np.unique(frame.day_codes)
    rows_of = [np.flatnonzero(frame.day_codes == d) for d in days]
    draws = np.empty((n_draws, len(frame.loms)))
    for k in range(n_draws):
        rows = np.concatenate([rows_of[d] for d in rng.integers(0, len(days), len(days))])
        draws[k] = compute_arc_asymmetry(frame.select(rows))["asymmetry"].to_numpy()
    return draws


def shuffle_within_days(frame: FormationFrame, rng: np.random.Generator) -> FormationFrame:
    """Shuffle labels within each day, separately for the front and the rear arc.

    Under the null a heifer's front and rear rates are unrelated to her
    identity, so each arc gets its own permutation.
    """
    front, rear = frame.front.copy(), frame.rear.copy()
    n = front.shape[1]
    for d in np.unique(frame.day_codes):
        rows = frame.day_codes == d
        front[rows] = front[rows][:, rng.permutation(n)]
        rear[rows] = rear[rows][:, rng.permutation(n)]
    return FormationFrame(frame.loms, frame.minute_index, frame.day_codes, frame.herd_speed,
                          frame.heading_deg, frame.along, frame.lateral, front, rear)


def check_headings(frame: FormationFrame) -> pd.DataFrame:
    """Asymmetry per compass sector of travel and agreement of opposite sectors.

    A fixed geographic GPS offset puts a collar in front when the herd heads
    one way and behind when it heads the other way, so opposite sectors would
    disagree. A real formation place agrees across all sectors.
    """
    sector = frame.heading_sector
    columns = {name: compute_arc_asymmetry(frame.select(sector == k))["asymmetry"].to_numpy()
               for k, name in enumerate(HEADING_SECTORS)}
    rows = [{"pair": f"{a}-{b}", "rho": float(spearmanr(columns[a], columns[b], nan_policy="omit")[0]),
             "n_minutes_a": int((sector == HEADING_SECTORS.index(a)).sum()),
             "n_minutes_b": int((sector == HEADING_SECTORS.index(b)).sum())}
            for a, b in (("N", "S"), ("E", "W"), ("N", "E"), ("S", "W"))]
    return pd.DataFrame(rows)


# ┌────────────────────────────────────────────────────────────┐
# │ Formation offsets  « place at zero herd speed »            │
# └────────────────────────────────────────────────────────────┘


def summarise_offsets(frame: FormationFrame, rng: np.random.Generator, n_draws: int) -> pd.DataFrame:
    """Intercept and speed slope with day-block bootstrap CIs and split-half rho."""
    table = fit_formation_offsets(frame)
    days = np.unique(frame.day_codes)
    rows_of = [np.flatnonzero(frame.day_codes == d) for d in days]
    draws = np.empty((n_draws, len(frame.loms)))
    for k in range(n_draws):
        rows = np.concatenate([rows_of[d] for d in rng.integers(0, len(days), len(days))])
        draws[k] = fit_formation_offsets(frame.select(rows))["intercept_m"].to_numpy()
    table["intercept_lo"] = np.quantile(draws, 0.025, axis=0)
    table["intercept_hi"] = np.quantile(draws, 0.975, axis=0)
    odd, even = frame.day_codes % 2 == 1, frame.day_codes % 2 == 0
    table["intercept_split_half_rho"] = spearmanr(
        fit_formation_offsets(frame.select(odd))["intercept_m"],
        fit_formation_offsets(frame.select(even))["intercept_m"])[0]
    return table


# ┌────────────────────────────────────────────────────────────┐
# │ Role calls  « all three measures must agree »              │
# └────────────────────────────────────────────────────────────┘


def classify_roles(per_heifer: pd.DataFrame, band: str | None = None) -> pd.DataFrame:
    """Front / rear when raw asymmetry, lag-corrected asymmetry and intercept agree.

    Uses the slowest speed band (smallest collar-lag shift) unless ``band``
    is given. ``front``: all three CIs above zero. ``probable front``: both
    asymmetry CIs above zero, intercept positive but its CI includes zero.
    ``timing-dependent``: the two asymmetries have opposite signs, both CIs
    excluding zero. Rear roles mirror front roles.
    """
    band = band or next(iter(FORMATION_SPEED_BANDS))
    rows = per_heifer[per_heifer["band"] == band]
    raw = rows[rows["scenario"] == "raw"].set_index("lom")
    corrected = rows[rows["scenario"] == "lag_corrected"].set_index("lom").reindex(raw.index)
    roles = [_call_role(raw.loc[lom], corrected.loc[lom]) for lom in raw.index]
    return pd.DataFrame({"lom": raw.index, "role": roles})


def _call_role(raw: pd.Series, corrected: pd.Series) -> str:
    def sign(row: pd.Series) -> int:
        if row["asymmetry_lo"] > 0:
            return 1
        if row["asymmetry_hi"] < 0:
            return -1
        return 0

    s_raw, s_cor = sign(raw), sign(corrected)
    if s_raw * s_cor < 0:
        return "timing-dependent"
    if s_raw != s_cor or s_raw == 0:
        return "middle"
    side = "front" if s_raw > 0 else "rear"
    intercept_sign = 1 if raw["intercept_lo"] > 0 else -1 if raw["intercept_hi"] < 0 else 0
    if intercept_sign == s_raw:
        return side
    if np.sign(raw["intercept_m"]) == s_raw:
        return f"probable {side}"
    return "middle"
