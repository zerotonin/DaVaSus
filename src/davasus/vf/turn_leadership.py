# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.turn_leadership                                    ║
# ║  « whose turns the others copy »                                 ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Directional correlation delays (Nagy et al. 2010) from 1-min    ║
# ║  headings while the herd travels. C_ij(tau) = <u_i(t) .          ║
# ║  u_j(t+tau)>; the delay of its peak (parabola-refined to         ║
# ║  fractions of a minute) says how long j lags i. A collar that    ║
# ║  fixes L s late shifts every delay by L, so the lag-corrected    ║
# ║  score subtracts the own lag.                                    ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Turn leadership from time-delayed heading correlations."""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    BOOTSTRAP_DRAWS,
    MOVE_MIN_M,
    PERMUTATIONS,
    TRAVEL_SPEED_M_MIN,
    TURN_MAX_LAG_MIN,
    TURN_MIN_CORRELATION,
    TURN_MIN_PAIR_MINUTES,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CorrelationSums:
    """Per-day sums of u_i(t) . u_j(t + tau) and their counts, shape (day, lag, i, j)."""

    sums: np.ndarray
    counts: np.ndarray
    lags: np.ndarray

    def pooled(self, days: np.ndarray | None = None) -> tuple[np.ndarray, np.ndarray]:
        """Correlation (lag, i, j) and counts over ``days`` (all when None; repeats allowed)."""
        sums = self.sums if days is None else self.sums[days]
        counts = self.counts if days is None else self.counts[days]
        total = counts.sum(axis=0)
        with np.errstate(invalid="ignore", divide="ignore"):
            return sums.sum(axis=0) / total, total


# ┌────────────────────────────────────────────────────────────┐
# │ Headings  « unit step vectors while the herd travels »     │
# └────────────────────────────────────────────────────────────┘


def compute_headings(x_m: np.ndarray, y_m: np.ndarray, herd_speed: np.ndarray,
                     is_excluded: np.ndarray) -> np.ndarray:
    """Unit heading per minute and heifer, shape (minute, animal, 2); NaN when not usable.

    Usable: the herd travels (centroid speed >= TRAVEL_SPEED_M_MIN) and the
    heifer's own 1-min step is at least MOVE_MIN_M, above the GPS noise.
    """
    dx = np.full(x_m.shape, np.nan)
    dy = np.full(x_m.shape, np.nan)
    dx[1:], dy[1:] = np.diff(x_m, axis=0), np.diff(y_m, axis=0)
    step = np.hypot(dx, dy)
    travel = (np.nan_to_num(herd_speed) >= TRAVEL_SPEED_M_MIN) & ~np.asarray(is_excluded, dtype=bool)
    usable = (step >= MOVE_MIN_M) & travel[:, None]
    with np.errstate(invalid="ignore", divide="ignore"):
        headings = np.stack([dx / step, dy / step], axis=-1)
    headings[~usable] = np.nan
    return headings


def compute_correlation_sums(headings: np.ndarray, day_codes: np.ndarray,
                             max_lag: int = TURN_MAX_LAG_MIN) -> CorrelationSums:
    """Sums and counts of u_i(t) . u_j(t + tau) per day, never across midnight."""
    lags = np.arange(-max_lag, max_lag + 1)
    days = np.unique(day_codes)
    n_animals = headings.shape[1]
    sums = np.zeros((len(days), len(lags), n_animals, n_animals))
    counts = np.zeros_like(sums)
    for d, day in enumerate(days):
        block = headings[day_codes == day]
        valid = np.isfinite(block[..., 0])
        filled = np.nan_to_num(block)
        n = len(block)
        for k, lag in enumerate(lags):
            if abs(lag) >= n:
                continue
            a, b = (slice(0, n - lag), slice(lag, n)) if lag >= 0 else (slice(-lag, n), slice(0, n + lag))
            sums[d, k] = np.einsum("tic,tjc->ij", filled[a], filled[b])
            counts[d, k] = np.einsum("ti,tj->ij", valid[a].astype(float), valid[b].astype(float))
    return CorrelationSums(sums, counts, lags)


# ┌────────────────────────────────────────────────────────────┐
# │ Delays  « peak of the correlation, refined »               │
# └────────────────────────────────────────────────────────────┘


def compute_pair_delays(correlation: np.ndarray, counts: np.ndarray, lags: np.ndarray) -> np.ndarray:
    """Delay tau*_ij in minutes (positive: j follows i); NaN for weak or thin pairs."""
    n_lags, n_animals, _ = correlation.shape
    peak = np.nanargmax(np.where(np.isfinite(correlation), correlation, -np.inf), axis=0)
    i, j = np.indices((n_animals, n_animals))
    centre = correlation[peak, i, j]
    lower = correlation[np.clip(peak - 1, 0, n_lags - 1), i, j]
    upper = correlation[np.clip(peak + 1, 0, n_lags - 1), i, j]
    curvature = lower - 2.0 * centre + upper
    inner = (peak > 0) & (peak < n_lags - 1) & (curvature < 0)
    with np.errstate(invalid="ignore", divide="ignore"):
        shift = np.where(inner, 0.5 * (lower - upper) / curvature, 0.0)
    delay = lags[peak] + shift
    is_reliable = (counts.min(axis=0) >= TURN_MIN_PAIR_MINUTES) & (centre >= TURN_MIN_CORRELATION)
    delay[~is_reliable | (i == j)] = np.nan
    return delay


def compute_turn_scores(delay: np.ndarray, lag_s: np.ndarray | None = None) -> np.ndarray:
    """Mean delay by which the others follow each heifer (minutes; positive = leads).

    With collar lags, tau_true_ij = tau_obs_ij + (L_j - L_i) / 60, so the
    delays are corrected before averaging.
    """
    if lag_s is not None:
        lag_min = np.nan_to_num(np.asarray(lag_s, dtype=float)) / 60.0
        delay = delay + lag_min[None, :] - lag_min[:, None]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return np.nanmean(delay, axis=1)


# ┌────────────────────────────────────────────────────────────┐
# │ Inference  « day bootstrap, time-reversal null »           │
# └────────────────────────────────────────────────────────────┘


def analyse_turns(sums: CorrelationSums, lag_s: np.ndarray, rng: np.random.Generator,
                  n_permutations: int = PERMUTATIONS, n_bootstrap: int = BOOTSTRAP_DRAWS) -> dict:
    """Scores raw and lag-corrected with day-bootstrap CIs and a time-reversal null.

    Reversing time within a day turns C_ij(tau) into C_ij(-tau), so random
    reversals per day destroy any consistent lead while keeping the data.
    """
    correlation, counts = sums.pooled()
    delay = compute_pair_delays(correlation, counts, sums.lags)
    scores = build_score_variants(delay, lag_s)
    n_days = sums.sums.shape[0]
    boot = {name: np.full((n_bootstrap, delay.shape[0]), np.nan) for name in scores}
    for k in range(n_bootstrap):
        days = rng.integers(0, n_days, n_days)
        c, n = sums.pooled(days)
        for name, score in build_score_variants(compute_pair_delays(c, n, sums.lags), lag_s).items():
            boot[name][k] = score
    null_sd = np.full(n_permutations, np.nan)
    for k in range(n_permutations):
        flip = rng.random(n_days) < 0.5
        flipped = np.where(flip[:, None, None, None], sums.sums[:, ::-1], sums.sums)
        flipped_counts = np.where(flip[:, None, None, None], sums.counts[:, ::-1], sums.counts)
        c = flipped.sum(axis=0) / np.maximum(flipped_counts.sum(axis=0), 1)
        null_sd[k] = np.nanstd(compute_turn_scores(compute_pair_delays(c, flipped_counts.sum(axis=0),
                                                                       sums.lags)))
    observed_sd = float(np.nanstd(scores["raw"]))
    log.info("turns: %d reliable pairs, score SD %.3f min", int(np.isfinite(delay).sum() // 2), observed_sd)
    return {"delay": delay, "correlation": correlation, "scores": scores, "boot": boot,
            "test": {"n_pairs": int(np.isfinite(delay).sum() // 2), "n_days": n_days,
                     "sd_score": observed_sd, "null_mean": float(np.nanmean(null_sd)),
                     "null_hi": float(np.nanquantile(null_sd, 0.975)),
                     "p": float((np.sum(null_sd >= observed_sd) + 1.0) / (n_permutations + 1.0)),
                     "median_abs_delay_min": float(np.nanmedian(np.abs(delay)))}}


def build_score_variants(delay: np.ndarray, lag_s: np.ndarray) -> dict[str, np.ndarray]:
    """Raw, fully lag-corrected, and lag-residual turn scores.

    At 1-min sampling the peak refinement shrinks sub-minute delays, so a
    full correction over-shoots. The residual removes only the trend of the
    raw score on collar lag that is actually in the data (and with it any
    true leading that happens to line up with lag).
    """
    raw = compute_turn_scores(delay)
    return {"raw": raw, "lag_corrected": compute_turn_scores(delay, lag_s),
            "lag_residual": remove_lag_trend(raw, lag_s)}


def remove_lag_trend(score: np.ndarray, lag_s: np.ndarray) -> np.ndarray:
    """Residual of ``score`` after a least-squares line on collar lag."""
    ok = np.isfinite(score) & np.isfinite(lag_s)
    out = np.full(score.shape, np.nan)
    if ok.sum() < 3:
        return out
    slope, intercept = np.polyfit(lag_s[ok], score[ok], 1)
    out[ok] = score[ok] - (slope * lag_s[ok] + intercept)
    return out


def summarise_turn_scores(result: dict, loms: list[str], lag_s: np.ndarray) -> pd.DataFrame:
    rows = []
    for name, score in result["scores"].items():
        boot = result["boot"][name]
        rows.append(pd.DataFrame({"lom": loms, "scenario": name, "turn_lead_min": score,
                                  "turn_lead_lo": np.nanquantile(boot, 0.025, axis=0),
                                  "turn_lead_hi": np.nanquantile(boot, 0.975, axis=0),
                                  "n_partners": np.isfinite(result["delay"]).sum(axis=1), "lag_s": lag_s}))
    return pd.concat(rows, ignore_index=True)
