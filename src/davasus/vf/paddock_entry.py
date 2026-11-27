# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.paddock_entry                                      ║
# ║  « who steps into the new paddock first »                        ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  After a fence switch opens a paddock that was closed in the     ║
# ║  previous layout, a heifer's entry minute is the first of a run  ║
# ║  of fixes at least MOVE_NOISE_M inside its boundary. Switches    ║
# ║  with manual herding, or that open no new paddock, are not       ║
# ║  ranked.                                                         ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Entry order into a newly opened paddock after each fence switch."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import shapely
from shapely.geometry import Polygon
from shapely.ops import unary_union

from davasus.vf.constants import (
    MOVE_NOISE_M,
    PADDOCK_ENTRY_RUN_MIN,
    PADDOCK_ENTRY_WINDOW_MIN,
)

log = logging.getLogger(__name__)

# A switch is ranked when at least this share of the present heifers entered.
MIN_ENTERED_SHARE: float = 0.5
DEPARTURE_COLUMNS: list[str] = ["bout_id", "lom", "is_present", "has_joined", "departure_min"]


# ┌────────────────────────────────────────────────────────────┐
# │ Switches  « which fence switches force a move »            │
# └────────────────────────────────────────────────────────────┘


def detect_paddock_entries(data) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Entry minutes into the new paddock for every switch that names one.

    Args:
        data: ``DashboardData`` with paddocks loaded (``davasus-vf-paddocks``).

    Returns:
        A switch table (one row per event with a new paddock) and an entry
        table shaped like Dumont departures: ``bout_id`` is the event id,
        ``departure_min`` the entry minute after activation.
    """
    if not data.has_paddocks:
        raise ValueError("no paddocks in heifer_vf.db; run davasus-vf-paddocks first")
    letters = {number: letter for letter, number in data.paddock_numbers.items()}
    switches, entries = [], []
    previous_open: tuple[str, ...] = ()
    for k, event in enumerate(data.events.itertuples()):
        is_open = tuple(data.event_paddocks[k]) if k < len(data.event_paddocks) else ()
        if pd.notna(event.new_parcel) and int(event.new_parcel) in letters:
            letter = letters[int(event.new_parcel)]
            frame = compute_entry_minutes(data, data.minute_of(event.start_utc), letter)
            frame.insert(0, "bout_id", int(event.event_id))
            entries.append(frame)
            switches.append(describe_switch(event, letter, letter not in previous_open, frame,
                                            letters))
        previous_open = is_open
    switch_table = pd.DataFrame(switches)
    entry_table = (pd.concat(entries, ignore_index=True) if entries
                   else pd.DataFrame(columns=DEPARTURE_COLUMNS))
    log.info("paddock entry: %d switches, %d ranked", len(switch_table),
             int(switch_table["is_ranked"].sum()) if len(switch_table) else 0)
    return switch_table, entry_table


def describe_switch(event, letter: str, is_new_paddock: bool, entries: pd.DataFrame,
                    letters: dict[int, str]) -> dict:
    present = entries[entries["is_present"]]
    entered = present.loc[present["has_joined"], "departure_min"]
    n_present = len(present)
    is_ranked = bool(is_new_paddock and not event.manual_herding
                     and n_present > 0 and len(entered) >= MIN_ENTERED_SHARE * n_present)
    cut_off = letters.get(int(event.cut_off_parcel)) if pd.notna(event.cut_off_parcel) else None
    return {
        "event_id": int(event.event_id), "label": event.label,
        "start_utc": event.start_utc, "day": event.start_utc.floor("D"),
        "new_paddock": letter, "cut_off_paddock": cut_off,
        "is_new_paddock": is_new_paddock, "manual_herding": bool(event.manual_herding),
        "water_point_relocated": bool(event.water_point_relocated),
        "n_present": n_present, "n_entered": len(entered),
        "first_entry_min": entered.min() if len(entered) else np.nan,
        "median_entry_min": entered.median() if len(entered) else np.nan,
        "spread_min": entered.max() - entered.min() if len(entered) else np.nan,
        "n_distinct_minutes": int(entered.nunique()),
        "is_ranked": is_ranked,
        "is_group_movement": is_ranked,
    }


# ┌────────────────────────────────────────────────────────────┐
# │ Entries  « first sustained fix inside the paddock »        │
# └────────────────────────────────────────────────────────────┘


def compute_entry_minutes(data, i0: int, letter: str) -> pd.DataFrame:
    """Per heifer: present at activation, already inside, entry minute.

    A heifer already inside within the first ``PADDOCK_ENTRY_RUN_MIN`` minutes
    did not have to enter and is not ranked (``is_present`` False).
    """
    core = build_paddock_core(data.paddocks[letter])
    window = slice(i0, min(i0 + PADDOCK_ENTRY_WINDOW_MIN, data.n_minutes))
    x, y = data.x_m[window], data.y_m[window]
    inside = np.zeros(x.shape, dtype=bool)
    ok = np.isfinite(x) & np.isfinite(y)
    inside[ok] = shapely.contains_xy(core, x[ok], y[ok])
    has_fix = ok[:PADDOCK_ENTRY_RUN_MIN + 1].any(axis=0)
    entry = np.array([find_first_run(inside[:, a], PADDOCK_ENTRY_RUN_MIN) for a in range(x.shape[1])])
    already_inside = np.isfinite(entry) & (entry < PADDOCK_ENTRY_RUN_MIN)
    is_present = has_fix & ~already_inside
    has_joined = is_present & np.isfinite(entry)
    return pd.DataFrame({"lom": list(data.loms), "is_present": is_present,
                         "has_joined": has_joined, "departure_min": np.where(has_joined, entry, np.nan),
                         "was_inside": already_inside})


def build_paddock_core(rings: list[tuple[np.ndarray, np.ndarray]]):
    """Paddock polygon shrunk by the GPS noise floor, so a boundary fix is not an entry."""
    polygons = [Polygon(np.c_[rx, ry]).buffer(0) for rx, ry in rings]
    core = unary_union(polygons).buffer(-MOVE_NOISE_M)
    if core.is_empty:
        raise ValueError("paddock vanishes after the noise buffer")
    return core


def find_first_run(values: np.ndarray, run: int) -> float:
    """Index of the first run of ``run`` consecutive True values, NaN if none."""
    if run <= 1:
        hits = np.flatnonzero(values)
        return float(hits[0]) if len(hits) else np.nan
    counts = np.convolve(values.astype(int), np.ones(run, dtype=int), mode="valid")
    hits = np.flatnonzero(counts == run)
    return float(hits[0]) if len(hits) else np.nan
