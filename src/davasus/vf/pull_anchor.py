# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.pull_anchor                                        ║
# ║  « who pulls whom »                                              ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Strandburg-Peshkin et al. (2015) dyadic events: the distance    ║
# ║  of a pair rises (separation) and falls again (reunion). The     ║
# ║  heifer that moved more while the gap opened initiated it. If    ║
# ║  the partner then closes the gap the initiator pulled her, if    ║
# ║  the initiator turns back she was anchored. Events last tens of  ║
# ║  minutes, far longer than any collar lag.                        ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Detect pull and anchor events between pairs of heifers."""

from __future__ import annotations

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    GPS_STILL_IMU_MAX,
    GPS_STILL_MIN_RUN_MIN,
    MOVE_MIN_M,
    MOVE_NOISE_M,
    PULL_DISTANCE_SMOOTHING_MIN,
    PULL_MAX_PHASE_MIN,
    PULL_MIN_RUN_MIN,
    PULL_MIN_STRENGTH,
    PULL_SWING_M,
)
from davasus.vf.initiation import HerdTracks

EVENT_COLUMNS: tuple[str, ...] = (
    "initiator", "partner", "t1", "t2", "t3", "day", "d1_m", "d2_m", "d3_m",
    "disparity_separation", "disparity_reunion", "strength", "outcome",
)


def detect_pull_anchor_events(
    tracks: HerdTracks,
    x_m: np.ndarray | None = None,
    y_m: np.ndarray | None = None,
    swing_m: float = PULL_SWING_M,
) -> pd.DataFrame:
    """All pull and anchor events of all pairs.

    Args:
        tracks: Herd tracks (minutes, heifers, excluded minutes).
        x_m, y_m: Positions to use instead of ``tracks``' own, e.g. with
            collar lag removed.
        swing_m: Minimum rise and fall of the pair distance; must exceed the
            GPS noise of the pair distance (``estimate_gps_noise``).

    Returns:
        One row per event with the columns of ``EVENT_COLUMNS``; times are
        minute indices, ``outcome`` is "pull" or "anchor".
    """
    x = tracks.x_m if x_m is None else x_m
    y = tracks.y_m if y_m is None else y_m
    rows = []
    for a in range(tracks.n_animals):
        for b in range(a + 1, tracks.n_animals):
            distance = smooth_distance(np.hypot(x[:, a] - x[:, b], y[:, a] - y[:, b]))
            for t1, t2, t3 in find_separation_reunions(distance, swing_m):
                if tracks.is_excluded[t1:t3 + 1].any():
                    continue
                event = classify_event(x, y, a, b, (t1, t2, t3))
                if event is not None:
                    rows.append((*event, tracks.minutes[t1].floor("D"),
                                 distance[t1], distance[t2], distance[t3]))
    frame = pd.DataFrame(rows, columns=["initiator", "partner", "t1", "t2", "t3",
                                         "disparity_separation", "disparity_reunion",
                                         "strength", "outcome", "day", "d1_m", "d2_m", "d3_m"])
    frame["initiator"] = [tracks.loms[k] for k in frame["initiator"]]
    frame["partner"] = [tracks.loms[k] for k in frame["partner"]]
    return frame[list(EVENT_COLUMNS)]


def smooth_distance(distance: np.ndarray) -> np.ndarray:
    """Centred running median, so single noisy fixes do not make extrema."""
    return (pd.Series(distance)
            .rolling(PULL_DISTANCE_SMOOTHING_MIN, center=True, min_periods=2).median().to_numpy())


def find_separation_reunions(distance: np.ndarray, swing_m: float = PULL_SWING_M) -> list[tuple[int, int, int]]:
    """(minimum, maximum, minimum) triples with both swings >= ``swing_m``.

    Works per gap-free run of at least ``PULL_MIN_RUN_MIN`` minutes; each
    phase may last at most ``PULL_MAX_PHASE_MIN``.
    """
    finite = np.isfinite(distance)
    edges = np.flatnonzero(np.diff(np.r_[0, finite.astype(int), 0]))
    triples = []
    for start, stop in zip(edges[::2], edges[1::2], strict=True):
        if stop - start < PULL_MIN_RUN_MIN:
            continue
        segment = distance[start:stop]
        pivots = find_zigzag_pivots(segment, swing_m)
        for k in range(len(pivots) - 2):
            t1, t2, t3 = pivots[k:k + 3]
            if not (segment[t2] > segment[t1] and segment[t2] > segment[t3]):
                continue
            t1, t3 = tighten_to_peak(segment, t1, t2, t3)
            if t2 - t1 <= PULL_MAX_PHASE_MIN and t3 - t2 <= PULL_MAX_PHASE_MIN:
                triples.append((start + t1, start + t2, start + t3))
    return triples


def tighten_to_peak(distance: np.ndarray, t1: int, t2: int, t3: int) -> tuple[int, int]:
    """Move both minima towards the peak while the distance stays within
    ``MOVE_NOISE_M`` of them.

    The zigzag minimum is the lowest noisy fix of a whole close-together
    stretch, which can lie hours from the separation; the event starts when
    the pair last was close and ends when it first is close again.
    """
    before = np.flatnonzero(distance[t1:t2] <= distance[t1] + MOVE_NOISE_M)
    after = np.flatnonzero(distance[t2:t3 + 1] <= distance[t3] + MOVE_NOISE_M)
    return t1 + int(before[-1]), t2 + int(after[0])


def find_zigzag_pivots(values: np.ndarray, swing: float) -> list[int]:
    """Alternating extrema where every swing between them is at least ``swing``.

      d  ^        t2
         |       /\\        a pivot is confirmed only once the series
         |  /\\  /  \\       has moved ``swing`` away from it, so noise
         | /  \\/    \\__/  smaller than ``swing`` never makes one
         |t1          t3

    A final fall of at least ``swing`` closes the series with its minimum,
    so a reunion at the end of a run is not lost.
    """
    pivots: list[int] = []
    low = high = 0
    direction = 0
    for k in range(1, len(values)):
        v = values[k]
        if v > values[high]:
            high = k
        if v < values[low]:
            low = k
        if direction == 0:
            if values[high] - values[low] >= swing:
                direction = 1 if high > low else -1
                pivots.append(low if direction == 1 else high)
                if direction == 1:
                    low = high
                else:
                    high = low
        elif direction == 1 and values[high] - v >= swing:
            pivots.append(high)
            direction, low = -1, k
        elif direction == -1 and v - values[low] >= swing:
            pivots.append(low)
            direction, high = 1, k
        if direction == 1 and v < values[low]:
            low = k
        if direction == -1 and v > values[high]:
            high = k
    if direction == -1 and pivots and values[pivots[-1]] - values[low] >= swing:
        pivots.append(low)
    return pivots


def classify_event(x: np.ndarray, y: np.ndarray, a: int, b: int,
                   times: tuple[int, int, int]) -> tuple | None:
    """Initiator, partner, times, disparities, strength and outcome, or None.

    Disparity of a phase: (m_a - m_b) / (m_a + m_b) with m the distance each
    heifer moved minus the GPS noise floor ``MOVE_NOISE_M`` (clipped at 0),
    so a heifer standing still counts as not moving. The heifer that moved
    more in each phase must have covered ``MOVE_MIN_M``. Strength is the
    product of both disparities' magnitudes; events below
    ``PULL_MIN_STRENGTH`` are dropped because both heifers moved about
    equally (e.g. the whole herd walking).
    """
    t1, t2, t3 = times
    separation = _compute_disparity(x, y, a, b, t1, t2)
    reunion = _compute_disparity(x, y, a, b, t2, t3)
    if not (np.isfinite(separation) and np.isfinite(reunion)):
        return None
    strength = abs(separation * reunion)
    if strength < PULL_MIN_STRENGTH:
        return None
    initiator, partner = (a, b) if separation > 0 else (b, a)
    initiator_moved_less = reunion < 0 if initiator == a else reunion > 0
    outcome = "pull" if initiator_moved_less else "anchor"
    sign = 1.0 if initiator == a else -1.0
    return (initiator, partner, t1, t2, t3, sign * separation, sign * reunion, strength, outcome)


def _compute_disparity(x: np.ndarray, y: np.ndarray, a: int, b: int, t0: int, t1: int) -> float:
    """Noise-corrected disparity, NaN when neither heifer made a real move."""
    moved_a = np.hypot(x[t1, a] - x[t0, a], y[t1, a] - y[t0, a])
    moved_b = np.hypot(x[t1, b] - x[t0, b], y[t1, b] - y[t0, b])
    if not max(moved_a, moved_b) >= MOVE_MIN_M:
        return np.nan
    net_a, net_b = max(moved_a - MOVE_NOISE_M, 0.0), max(moved_b - MOVE_NOISE_M, 0.0)
    return float((net_a - net_b) / (net_a + net_b))


# ┌────────────────────────────────────────────────────────────┐
# │ GPS noise floor  « what a still heifer looks like »        │
# └────────────────────────────────────────────────────────────┘


def estimate_gps_noise(x: np.ndarray, y: np.ndarray, imu_40mg: np.ndarray,
                       lags_min: tuple[int, ...] = (1, 5, 10, 20, 30)) -> pd.DataFrame:
    """Apparent displacement and pair-distance range of still heifers.

    Still: at most ``GPS_STILL_IMU_MAX`` IMU ticks per minute for at least
    ``GPS_STILL_MIN_RUN_MIN`` minutes. Rows: ``displacement`` over each lag
    for single heifers and ``pair_range`` (range of the smoothed pair
    distance within the lag) for pairs that are both still.
    """
    still = (imu_40mg <= GPS_STILL_IMU_MAX) & np.isfinite(x)
    rows = []
    for lag in lags_min:
        moved = [np.hypot(x[r0 + lag:r1, a] - x[r0:r1 - lag, a], y[r0 + lag:r1, a] - y[r0:r1 - lag, a])
                 for a, r0, r1 in _still_runs(still) if r1 - r0 > lag]
        rows.append(_describe("displacement", lag, moved))
    for lag in lags_min[2:]:
        ranges = []
        for a in range(x.shape[1]):
            for b in range(a + 1, x.shape[1]):
                for _, r0, r1 in _still_runs((still[:, a] & still[:, b])[:, None]):
                    if r1 - r0 > lag:
                        d = smooth_distance(np.hypot(x[r0:r1, a] - x[r0:r1, b], y[r0:r1, a] - y[r0:r1, b]))
                        window = np.lib.stride_tricks.sliding_window_view(d, lag + 1)
                        ranges.append(window.max(axis=1) - window.min(axis=1))
        rows.append(_describe("pair_range", lag, ranges))
    return pd.DataFrame(rows)


def _still_runs(still: np.ndarray) -> list[tuple[int, int, int]]:
    runs = []
    for a in range(still.shape[1]):
        edges = np.flatnonzero(np.diff(np.r_[0, still[:, a].astype(int), 0]))
        runs += [(a, r0, r1) for r0, r1 in zip(edges[::2], edges[1::2], strict=True)
                 if r1 - r0 >= GPS_STILL_MIN_RUN_MIN]
    return runs


def _describe(measure: str, lag: int, chunks: list[np.ndarray]) -> dict:
    values = np.concatenate(chunks) if chunks else np.array([np.nan])
    return {"measure": measure, "lag_min": lag, "n": int(np.isfinite(values).sum()),
            "median_m": float(np.nanmedian(values)), "p95_m": float(np.nanpercentile(values, 95)),
            "p99_m": float(np.nanpercentile(values, 99))}
