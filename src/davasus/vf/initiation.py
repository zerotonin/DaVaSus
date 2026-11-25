# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.initiation                                         ║
# ║  « who sets the herd in motion »                                 ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Dumont et al. (2005): the leader is the heifer that             ║
# ║  consistently initiates spontaneous group movements. Herd        ║
# ║  travel bouts from the centroid speed, departures along the      ║
# ║  travel axis, collar sampling lags, and individual attempts      ║
# ║  with their followers.                                           ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Detect spontaneous group movements and their first movers."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    ACTIVATION_EXCLUSION_MIN,
    ATTEMPT_MIN_DISPLACEMENT_M,
    ATTEMPT_MIN_STRAIGHTNESS,
    ATTEMPT_PRE_MAX_DISPLACEMENT_M,
    ATTEMPT_PRE_MIN,
    ATTEMPT_REFRACTORY_MIN,
    ATTEMPT_WINDOW_MIN,
    BOUT_MERGE_GAP_MIN,
    BOUT_MIN_DISPLACEMENT_M,
    BOUT_MIN_LENGTH_MIN,
    BOUT_POST_MIN,
    BOUT_PRE_MIN,
    CHANCE_SHIFT_MIN,
    CLEAR_LEAD_MIN,
    DEPARTURE_THRESHOLD_M,
    FOLLOW_MAX_ANGLE_DEG,
    FOLLOW_MIN_DISPLACEMENT_M,
    FOLLOW_SEARCH_MIN,
    GAP_FILL_MIN,
    GROUP_MOVEMENT_MIN_JOINED,
    JOIN_FRACTION_OF_HERD,
    MIN_ANIMALS_FOR_CENTROID,
    MIN_PRESENT_COVERAGE,
    TIE_TOLERANCE_MIN,
    TRAVEL_SPEED_M_MIN,
)


@dataclass(frozen=True)
class HerdTracks:
    """Minute-by-animal positions (short gaps filled) and the herd centroid."""

    minutes: pd.DatetimeIndex
    loms: list[str]
    x_m: np.ndarray
    y_m: np.ndarray
    centroid_x: np.ndarray
    centroid_y: np.ndarray
    speed: np.ndarray
    is_excluded: np.ndarray

    @property
    def n_minutes(self) -> int:
        return len(self.minutes)

    @property
    def n_animals(self) -> int:
        return len(self.loms)


def build_herd_tracks(
    minutes: pd.DatetimeIndex,
    loms: list[str],
    x_m: np.ndarray,
    y_m: np.ndarray,
    events: pd.DataFrame,
    is_gps_reduced: np.ndarray,
) -> HerdTracks:
    """Fill short fix gaps, compute the centroid and the excluded minutes.

    Excluded: reduced GPS (Hp30 at or above the storm threshold, D20) and the
    first ``ACTIVATION_EXCLUSION_MIN`` after every parcel activation. A cut-off
    is semi-forced and manual herding is forced, so neither is spontaneous.
    """
    x = fill_short_gaps(x_m, GAP_FILL_MIN)
    y = fill_short_gaps(y_m, GAP_FILL_MIN)
    n_ok = np.isfinite(x).sum(axis=1)
    has_centroid = n_ok >= MIN_ANIMALS_FOR_CENTROID
    with np.errstate(invalid="ignore"):
        cx = np.where(has_centroid, np.nansum(x, axis=1) / np.maximum(n_ok, 1), np.nan)
        cy = np.where(has_centroid, np.nansum(y, axis=1) / np.maximum(n_ok, 1), np.nan)
    return HerdTracks(minutes, list(loms), x, y, cx, cy, compute_central_speed(cx, cy),
                      build_exclusion_mask(minutes, events, is_gps_reduced))


def fill_short_gaps(values: np.ndarray, max_gap: int) -> np.ndarray:
    """Linear interpolation across interior NaN runs of at most ``max_gap`` rows."""
    out = values.astype(float).copy()
    rows = np.arange(len(out))
    for column in range(out.shape[1]):
        series = out[:, column]
        finite = np.isfinite(series)
        if finite.sum() < 2:
            continue
        missing = ~finite
        run_id = np.cumsum(np.r_[True, missing[1:] != missing[:-1]])
        run_length = np.bincount(run_id)[run_id]
        first, last = np.flatnonzero(finite)[[0, -1]]
        fill = missing & (run_length <= max_gap) & (rows > first) & (rows < last)
        series[fill] = np.interp(rows[fill], rows[finite], series[finite])
    return out


def compute_central_speed(cx: np.ndarray, cy: np.ndarray) -> np.ndarray:
    """Centroid speed in m/min from the central difference (t-1, t+1)."""
    vx = (np.r_[cx[1:], np.nan] - np.r_[np.nan, cx[:-1]]) / 2.0
    vy = (np.r_[cy[1:], np.nan] - np.r_[np.nan, cy[:-1]]) / 2.0
    return np.hypot(vx, vy)


def build_exclusion_mask(
    minutes: pd.DatetimeIndex, events: pd.DataFrame, is_gps_reduced: np.ndarray
) -> np.ndarray:
    mask = np.asarray(is_gps_reduced, dtype=bool).copy()
    window = pd.Timedelta(minutes=ACTIVATION_EXCLUSION_MIN)
    for start in events["start_utc"]:
        mask |= (minutes >= start) & (minutes < start + window)
    return mask


# ┌────────────────────────────────────────────────────────────┐
# │ Travel bouts  « herd centroid on the move »                │
# └────────────────────────────────────────────────────────────┘


def detect_travel_bouts(tracks: HerdTracks) -> pd.DataFrame:
    """Runs of centroid speed at or above ``TRAVEL_SPEED_M_MIN``.

    Runs separated by at most ``BOUT_MERGE_GAP_MIN`` slow minutes are merged.
    A bout needs ``BOUT_MIN_LENGTH_MIN`` minutes, a net centroid displacement of
    ``BOUT_MIN_DISPLACEMENT_M`` and no excluded minute in its analysis window.
    """
    travel = np.nan_to_num(tracks.speed) >= TRAVEL_SPEED_M_MIN
    rows = []
    for i0, i1 in _find_runs(np.flatnonzero(travel), BOUT_MERGE_GAP_MIN):
        lo, hi = i0 - BOUT_PRE_MIN, i1 + BOUT_POST_MIN
        if lo < 0 or hi >= tracks.n_minutes or tracks.is_excluded[lo:hi + 1].any():
            continue
        dx = tracks.centroid_x[i1 + 1] - tracks.centroid_x[i0 - 1]
        dy = tracks.centroid_y[i1 + 1] - tracks.centroid_y[i0 - 1]
        displacement = float(np.hypot(dx, dy))
        if (i1 - i0 + 1 < BOUT_MIN_LENGTH_MIN or not np.isfinite(displacement)
                or displacement < BOUT_MIN_DISPLACEMENT_M):
            continue
        onset = tracks.minutes[i0]
        rows.append((i0, i1, onset, onset.floor("D"), i1 - i0 + 1, displacement,
                     dx / displacement, dy / displacement))
    bouts = pd.DataFrame(rows, columns=["i0", "i1", "onset_utc", "day", "length_min",
                                        "displacement_m", "ux", "uy"])
    bouts.insert(0, "bout_id", np.arange(len(bouts)))
    return bouts


def _find_runs(index: np.ndarray, max_gap: int) -> list[tuple[int, int]]:
    if len(index) == 0:
        return []
    breaks = np.flatnonzero(np.diff(index) > max_gap + 1)
    starts = np.r_[index[0], index[breaks + 1]]
    ends = np.r_[index[breaks], index[-1]]
    return list(zip(starts.tolist(), ends.tolist(), strict=True))


# ┌────────────────────────────────────────────────────────────┐
# │ Departures  « when each heifer leaves »                    │
# └────────────────────────────────────────────────────────────┘


def compute_departures(tracks: HerdTracks, bouts: pd.DataFrame) -> pd.DataFrame:
    """Departure time of every heifer in every bout, in minutes from onset.

    Each heifer's position is projected on the bout's travel direction,
    relative to her own position ``BOUT_PRE_MIN`` before onset. Her departure
    is the last crossing of ``DEPARTURE_THRESHOLD_M``, linearly interpolated,
    so grazing drift before the bout does not count. She has joined when she
    ends the window at least ``JOIN_FRACTION_OF_HERD`` of the herd's
    displacement ahead. Present: a fix at the window start and at least
    ``MIN_PRESENT_COVERAGE`` of the window covered.
    """
    frames = [_bout_departures(tracks, bout) for bout in bouts.itertuples()]
    if not frames:
        return pd.DataFrame(columns=["bout_id", "lom", "is_present", "has_joined", "departure_min"])
    return pd.concat(frames, ignore_index=True)


def _bout_departures(tracks: HerdTracks, bout) -> pd.DataFrame:
    lo, hi = bout.i0 - BOUT_PRE_MIN, bout.i1 + BOUT_POST_MIN
    px = tracks.x_m[lo:hi + 1] - tracks.x_m[lo]
    py = tracks.y_m[lo:hi + 1] - tracks.y_m[lo]
    projection = px * bout.ux + py * bout.uy
    is_present = np.isfinite(tracks.x_m[lo]) & (np.isfinite(projection).mean(axis=0) >= MIN_PRESENT_COVERAGE)
    projection = pd.DataFrame(projection).interpolate(limit_direction="both").to_numpy()
    has_joined = is_present & (projection[-1] >= JOIN_FRACTION_OF_HERD * bout.displacement_m)
    departure = np.full(tracks.n_animals, np.nan)
    below = projection < DEPARTURE_THRESHOLD_M
    for a in np.flatnonzero(has_joined):
        j = np.flatnonzero(below[:, a])[-1]
        p0, p1 = projection[j, a], projection[j + 1, a]
        departure[a] = lo + j + (DEPARTURE_THRESHOLD_M - p0) / (p1 - p0) - bout.i0
    return pd.DataFrame({"bout_id": bout.bout_id, "lom": tracks.loms, "is_present": is_present,
                         "has_joined": has_joined, "departure_min": departure})


def summarise_bouts(bouts: pd.DataFrame, departures: pd.DataFrame) -> pd.DataFrame:
    """Add present and joined counts and the group-movement flag."""
    counts = departures.groupby("bout_id").agg(n_present=("is_present", "sum"),
                                               n_joined=("has_joined", "sum"))
    out = bouts.merge(counts, left_on="bout_id", right_index=True, how="left")
    out["is_group_movement"] = out["n_joined"] >= GROUP_MOVEMENT_MIN_JOINED * out["n_present"]
    return out


def compute_first_mover_credits(
    departures: pd.DataFrame, bouts: pd.DataFrame, tie_tolerance_min: float = TIE_TOLERANCE_MIN
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Dumont first-place credit per heifer and group movement.

    Heifers whose departure lies within ``tie_tolerance_min`` of the earliest
    share first place; each gets 1/t for t tied animals.

    Returns:
        Long credit table (bout_id, day, lom, is_present, credit) and a bout
        table with the tie size and the first-to-second departure gap.
    """
    group = bouts.loc[bouts["is_group_movement"], ["bout_id", "day"]]
    table = departures.merge(group, on="bout_id")
    table = table[table["is_present"]].copy()
    first = table.groupby("bout_id")["departure_min"].transform("min")
    is_tied_first = table["departure_min"] <= first + tie_tolerance_min
    n_tied = is_tied_first.groupby(table["bout_id"]).transform("sum")
    table["credit"] = np.where(is_tied_first, 1.0 / n_tied, 0.0)
    per_bout = table.groupby("bout_id").agg(day=("day", "first"), n_tied=("credit", lambda c: int((c > 0).sum())),
                                            gap_min=("departure_min", _first_gap)).reset_index()
    first_lom = table.loc[table["credit"] > 0].groupby("bout_id")["lom"].first()
    per_bout["first_lom"] = per_bout["bout_id"].map(first_lom)
    per_bout["is_clear_lead"] = per_bout["gap_min"] >= CLEAR_LEAD_MIN
    return table[["bout_id", "day", "lom", "is_present", "departure_min", "credit"]], per_bout


def _first_gap(departures: pd.Series) -> float:
    ordered = np.sort(departures.dropna().to_numpy())
    return float(ordered[1] - ordered[0]) if len(ordered) > 1 else np.nan


# ┌────────────────────────────────────────────────────────────┐
# │ Collar lag  « front offset grows with speed »              │
# └────────────────────────────────────────────────────────────┘


def estimate_sampling_lags(tracks: HerdTracks) -> pd.DataFrame:
    """Apparent time lag of each collar from its front offset while travelling.

    A collar that fixes L seconds later than the others sits v * L / 60 m
    ahead of the centroid at herd speed v (m/min). The slope of front offset
    on herd speed estimates L; centred on the herd mean. A heifer that truly
    walks ahead produces the same slope, so the correction is a worst case
    that treats all speed-dependent leading as artefact.
    """
    travel = (np.nan_to_num(tracks.speed) >= TRAVEL_SPEED_M_MIN) & ~tracks.is_excluded
    i = np.flatnonzero(travel)
    i = i[(i > 0) & (i < tracks.n_minutes - 1)]
    vx = (tracks.centroid_x[i + 1] - tracks.centroid_x[i - 1]) / 2.0
    vy = (tracks.centroid_y[i + 1] - tracks.centroid_y[i - 1]) / 2.0
    speed = np.hypot(vx, vy)
    offset = ((tracks.x_m[i] - tracks.centroid_x[i, None]) * (vx / speed)[:, None]
              + (tracks.y_m[i] - tracks.centroid_y[i, None]) * (vy / speed)[:, None])
    rows = []
    for a, lom in enumerate(tracks.loms):
        ok = np.isfinite(offset[:, a]) & np.isfinite(speed)
        slope, intercept = np.polyfit(speed[ok], offset[ok, a], 1)
        rows.append((lom, 60.0 * slope, intercept, int(ok.sum())))
    lags = pd.DataFrame(rows, columns=["lom", "lag_s", "intercept_m", "n_minutes"])
    lags["lag_s"] -= lags["lag_s"].mean()
    return lags


def apply_sampling_lags(departures: pd.DataFrame, lags: pd.DataFrame) -> pd.DataFrame:
    """Shift departures to true time: a late collar's fix belongs L s later."""
    out = departures.merge(lags[["lom", "lag_s"]], on="lom", how="left")
    out["departure_min"] = out["departure_min"] + out["lag_s"].fillna(0.0) / 60.0
    return out.drop(columns="lag_s")


# ┌────────────────────────────────────────────────────────────┐
# │ Attempts  « initiations with and without followers »       │
# └────────────────────────────────────────────────────────────┘


def detect_initiation_attempts(tracks: HerdTracks, rng: np.random.Generator) -> pd.DataFrame:
    """Individual departures while the herd is not travelling, with followers.

    An attempt: a heifer that was nearly stationary for ``ATTEMPT_PRE_MIN``
    covers ``ATTEMPT_MIN_DISPLACEMENT_M`` in ``ATTEMPT_WINDOW_MIN`` on a
    straight path while the centroid is slower than travel speed. A follower
    is another heifer, not already moving at the attempt, that makes a
    straight ``FOLLOW_MIN_DISPLACEMENT_M`` move within
    ``FOLLOW_MAX_ANGLE_DEG`` of the initiator's direction in the next
    ``FOLLOW_SEARCH_MIN``. ``n_followers_chance`` repeats the count at a
    minute shifted by ``CHANCE_SHIFT_MIN`` with the same direction.
    """
    moves = _compute_window_moves(tracks.x_m, tracks.y_m, ATTEMPT_WINDOW_MIN)
    pre = _compute_window_moves(tracks.x_m, tracks.y_m, ATTEMPT_PRE_MIN, backwards=True)
    with np.errstate(invalid="ignore"):
        is_candidate = ((moves.displacement >= ATTEMPT_MIN_DISPLACEMENT_M)
                        & (moves.straightness >= ATTEMPT_MIN_STRAIGHTNESS)
                        & (pre.displacement < ATTEMPT_PRE_MAX_DISPLACEMENT_M)
                        & (np.nan_to_num(tracks.speed, nan=np.inf) < TRAVEL_SPEED_M_MIN)[:, None]
                        & ~tracks.is_excluded[:, None])
    rows = []
    for i, a in _apply_refractory(is_candidate):
        direction = (moves.ex[i, a], moves.ey[i, a])
        n_present, n_followers = _count_followers(tracks, moves, pre, i, a, direction)
        shifted = i + int(rng.choice((-1, 1)) * rng.integers(*CHANCE_SHIFT_MIN))
        chance = (_count_followers(tracks, moves, pre, shifted, a, direction)[1]
                  if 0 <= shifted < tracks.n_minutes else -1)
        onset = tracks.minutes[i]
        rows.append((tracks.loms[a], i, onset, onset.floor("D"), n_present, n_followers, chance))
    columns = ["lom", "minute_index", "onset_utc", "day", "n_present", "n_followers", "n_followers_chance"]
    attempts = pd.DataFrame(rows, columns=columns)
    attempts["n_followers_chance"] = attempts["n_followers_chance"].where(attempts["n_followers_chance"] >= 0)
    attempts["is_followed"] = attempts["n_followers"] >= 1
    attempts["is_group_success"] = (attempts["n_followers"]
                                    >= GROUP_MOVEMENT_MIN_JOINED * (attempts["n_present"] - 1))
    return attempts


@dataclass(frozen=True)
class WindowMoves:
    displacement: np.ndarray
    straightness: np.ndarray
    ex: np.ndarray
    ey: np.ndarray


def _compute_window_moves(x: np.ndarray, y: np.ndarray, window: int,
                          backwards: bool = False) -> WindowMoves:
    """Displacement over ``window`` minutes starting (or ending) at each minute."""
    n = len(x)
    pad = np.full((window, x.shape[1]), np.nan)
    if backwards:
        dx, dy = x - np.r_[pad, x[:-window]], y - np.r_[pad, y[:-window]]
    else:
        dx, dy = np.r_[x[window:], pad] - x, np.r_[y[window:], pad] - y
    step = np.r_[np.hypot(np.diff(x, axis=0), np.diff(y, axis=0)), np.full((1, x.shape[1]), np.nan)]
    cumulative = np.r_[np.zeros((1, x.shape[1])), np.cumsum(np.nan_to_num(step), axis=0)]
    gaps = np.r_[np.zeros((1, x.shape[1])), np.cumsum(~np.isfinite(step), axis=0)]
    start = np.arange(n) - (window if backwards else 0)
    valid = (start >= 0) & (start + window <= n)
    path = np.full_like(dx, np.nan)
    s = start[valid]
    path[valid] = cumulative[s + window] - cumulative[s]
    path[valid] = np.where(gaps[s + window] - gaps[s] > 0, np.nan, path[valid])
    displacement = np.hypot(dx, dy)
    with np.errstate(invalid="ignore", divide="ignore"):
        return WindowMoves(displacement, displacement / path, dx / displacement, dy / displacement)


def _apply_refractory(is_candidate: np.ndarray) -> list[tuple[int, int]]:
    kept = []
    for a in range(is_candidate.shape[1]):
        last = -np.inf
        for i in np.flatnonzero(is_candidate[:, a]):
            if i - last > ATTEMPT_REFRACTORY_MIN:
                kept.append((int(i), a))
                last = i
    return sorted(kept)


def _count_followers(tracks: HerdTracks, moves: WindowMoves, pre: WindowMoves, i: int, a: int,
                     direction: tuple[float, float]) -> tuple[int, int]:
    """Heifers present at minute ``i`` and followers of heifer ``a`` in ``direction``."""
    cos_limit = np.cos(np.deg2rad(FOLLOW_MAX_ANGLE_DEG))
    window = slice(i + 1, min(i + FOLLOW_SEARCH_MIN, tracks.n_minutes - 1) + 1)
    alignment = moves.ex[window] * direction[0] + moves.ey[window] * direction[1]
    with np.errstate(invalid="ignore"):
        moved_with = ((moves.displacement[window] >= FOLLOW_MIN_DISPLACEMENT_M)
                      & (moves.straightness[window] >= ATTEMPT_MIN_STRAIGHTNESS)
                      & (alignment >= cos_limit)).any(axis=0)
        was_moving = pre.displacement[i] >= FOLLOW_MIN_DISPLACEMENT_M
    present = np.isfinite(tracks.x_m[i])
    is_follower = moved_with & ~was_moving & present
    is_follower[a] = False
    return int(present.sum()), int(is_follower.sum())
