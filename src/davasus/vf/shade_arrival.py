# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.shade_arrival                                      ║
# ║  « who leads the herd into the shade »                           ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  An arrival is the herd share inside a surveyed tree patch       ║
# ║  rising from below 10 % to 50 %. Each heifer's arrival is her    ║
# ║  first run of fixes inside the patch; those already inside when  ║
# ║  the rise starts are not ranked. Daytime arrivals are kept; THI  ║
# ║  marks the hot ones (Zimbelman et al. 2009).                     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Arrival order at the tree patches (permanent shade)."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import shapely
from shapely.geometry import Polygon
from shapely.ops import unary_union

from davasus.vf.constants import (
    MIN_PRESENT_COVERAGE,
    SHADE_ENTRY_RUN_MIN,
    SHADE_FILL_ONSET,
    SHADE_FILL_START,
    SHADE_HOT_THI,
    SHADE_LOOKBACK_MIN,
    SHADE_POST_MIN,
    SHADE_SMOOTH_MIN,
)
from davasus.vf.paddock_entry import find_first_run

log = logging.getLogger(__name__)

EPISODE_COLUMNS: list[str] = ["bout_id", "patch", "i0", "onset", "onset_utc", "day", "rise_min",
                              "thi", "net_radiation", "is_daytime", "is_hot", "is_group_movement"]


# ┌────────────────────────────────────────────────────────────┐
# │ Patches  « herd share inside each tree patch »             │
# └────────────────────────────────────────────────────────────┘


def compute_patch_occupancy(data) -> dict[str, np.ndarray]:
    """Boolean (minute, animal) array per tree patch: fix inside the patch."""
    if not data.trees:
        raise ValueError("no tree patches in heifer_vf.db; run davasus-vf-paddocks first")
    has_fix = np.isfinite(data.x_m) & np.isfinite(data.y_m)
    occupancy = {}
    for patch, rings in data.trees.items():
        polygon = unary_union([Polygon(np.c_[rx, ry]).buffer(0) for rx, ry in rings])
        inside = np.zeros(data.x_m.shape, dtype=bool)
        inside[has_fix] = shapely.contains_xy(polygon, data.x_m[has_fix], data.y_m[has_fix])
        occupancy[patch] = inside
    return occupancy


def detect_shade_episodes(data, occupancy: dict[str, np.ndarray]) -> pd.DataFrame:
    """Herd arrivals at each patch, with THI and net radiation at onset."""
    n_fixes = np.maximum((np.isfinite(data.x_m)).sum(axis=1), 1)
    rows = []
    for patch, inside in occupancy.items():
        share = pd.Series(inside.sum(axis=1) / n_fixes).rolling(SHADE_SMOOTH_MIN, center=True,
                                                                  min_periods=1).median().to_numpy()
        onsets = np.flatnonzero((share[1:] >= SHADE_FILL_ONSET) & (share[:-1] < SHADE_FILL_ONSET)) + 1
        for onset in onsets:
            lookback = share[max(0, onset - SHADE_LOOKBACK_MIN):onset]
            low = np.flatnonzero(lookback < SHADE_FILL_START)
            if len(low) == 0 or np.asarray(data.is_gps_reduced[onset - len(lookback):onset + 1]).any():
                continue
            i0 = onset - len(lookback) + int(low[-1])
            rows.append((patch, i0, int(onset), data.minutes[onset], int(onset - i0),
                         float(data.weather["thi_nrc"].iloc[onset]),
                         float(data.weather["rad_net_avg"].iloc[onset])))
    episodes = pd.DataFrame(rows, columns=["patch", "i0", "onset", "onset_utc", "rise_min", "thi",
                                           "net_radiation"])
    if episodes.empty:
        return pd.DataFrame(columns=EPISODE_COLUMNS)
    episodes = episodes.sort_values("i0").reset_index(drop=True)
    episodes.insert(0, "bout_id", np.arange(len(episodes)))
    episodes["day"] = episodes["onset_utc"].dt.floor("D")
    episodes["is_daytime"] = episodes["net_radiation"] > 0
    episodes["is_hot"] = episodes["is_daytime"] & (episodes["thi"] >= SHADE_HOT_THI)
    episodes["is_group_movement"] = episodes["is_daytime"]
    log.info("shade arrivals: %d, %d by day, %d hot", len(episodes), int(episodes["is_daytime"].sum()),
             int(episodes["is_hot"].sum()))
    return episodes[EPISODE_COLUMNS]


# ┌────────────────────────────────────────────────────────────┐
# │ Arrivals  « first run of fixes inside the patch »          │
# └────────────────────────────────────────────────────────────┘


def compute_shade_arrivals(occupancy: dict[str, np.ndarray], episodes: pd.DataFrame, has_fix: np.ndarray,
                           loms: list[str]) -> pd.DataFrame:
    """Arrival minute per heifer and episode, as Dumont departures (minutes after ``i0``)."""
    frames = []
    for episode in episodes.itertuples():
        stop = min(episode.onset + SHADE_POST_MIN, has_fix.shape[0])
        inside = occupancy[episode.patch][episode.i0:stop]
        arrival = np.array([find_first_run(inside[:, a], SHADE_ENTRY_RUN_MIN) for a in range(len(loms))])
        was_inside = inside[0]
        is_present = (has_fix[episode.i0:stop].mean(axis=0) >= MIN_PRESENT_COVERAGE) & ~was_inside
        has_joined = is_present & np.isfinite(arrival)
        frames.append(pd.DataFrame({"bout_id": episode.bout_id, "lom": loms, "is_present": is_present,
                                    "has_joined": has_joined,
                                    "departure_min": np.where(has_joined, arrival, np.nan)}))
    if not frames:
        return pd.DataFrame(columns=["bout_id", "lom", "is_present", "has_joined", "departure_min"])
    return pd.concat(frames, ignore_index=True)
