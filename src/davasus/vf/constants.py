# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.constants                                          ║
# ║  « codes, sentinels and defaults for M8 »                        ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Outside codes from the heifer glossary, the no-fence sentinel,  ║
# ║  time-base offsets and the default analysis windows.             ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Shared constants for the virtual-fence (M8) analysis."""

from __future__ import annotations

from enum import IntEnum

from davasus.constants import (
    WONG_BLACK,
    WONG_BLUE,
    WONG_GREY,
    WONG_ORANGE,
    WONG_SKY,
    WONG_VERMILLION,
    WONG_YELLOW,
)

# ┌────────────────────────────────────────────────────────────┐
# │ Outside codes  « glossary: Erklaerung_Attribute.txt »      │
# └────────────────────────────────────────────────────────────┘


class OutsideCode(IntEnum):
    """Position of a fix relative to the virtual and real fence."""

    INSIDE = 0
    OUTSIDE_PARCEL = 1
    OUTSIDE_REAL_FENCE = 2
    SEARCH_AFTER_ACTIVATION = 3
    BREAKOUT_WITH_STIMULUS = 4
    PRE_FENCE_ZONE = 5
    PRE_FENCE_ZONE_WITH_STIMULUS = 6


BREAKOUT_CODES: frozenset[int] = frozenset({OutsideCode.OUTSIDE_PARCEL, OutsideCode.BREAKOUT_WITH_STIMULUS})
PRE_FENCE_CODES: frozenset[int] = frozenset(
    {OutsideCode.PRE_FENCE_ZONE, OutsideCode.PRE_FENCE_ZONE_WITH_STIMULUS}
)

# ┌────────────────────────────────────────────────────────────┐
# │ Data hygiene  « sentinels and time bases »                 │
# └────────────────────────────────────────────────────────────┘

NO_FENCE_SENTINELS: frozenset[float] = frozenset({-2147483647.0, -2147483648.0})

# Parcel-log ``Time`` is UTC+2 for every row of the 2024 heifer file
# (Jul to Sep 2024, CEST). Re-check before ingesting data across a DST switch.
PARCEL_LOG_UTC_OFFSET_H: int = 2

FIX_INTERVAL_S: dict[int, int] = {2024: 60, 2025: 600}

# ┌────────────────────────────────────────────────────────────┐
# │ Analysis defaults  « handover section 4 »                  │
# └────────────────────────────────────────────────────────────┘

FOLLOW_WINDOW_MIN: int = 5
FOLLOW_WINDOW_SENSITIVITY_MIN: tuple[int, ...] = (2, 5, 10)
INITIATION_WINDOWS_MIN: tuple[int, ...] = (2, 5, 15)
SR_BLOCK_SIZE: int = 5
SR_CRITERION: float = 0.8
PERMUTATIONS: int = 10_000

FIGURE_SUBDIR: str = "08_vf_leadership"

# ┌────────────────────────────────────────────────────────────┐
# │ Figures and movie  « semantic colours, scales »            │
# └────────────────────────────────────────────────────────────┘

VF_COLOURS: dict[str, str] = {
    "audio": WONG_ORANGE,
    "pulse": WONG_VERMILLION,
    "sr": WONG_BLUE,
    "ratio_file": WONG_SKY,
    "herd": WONG_GREY,
    "gps_error": WONG_BLACK,
    "manual_herding": WONG_YELLOW,
}
TRAJECTORY_CMAP: str = "twilight"
HEATMAP_CMAP: str = "viridis"
FOOTPRINT_GREY: str = WONG_GREY
HEATMAP_BIN_M: float = 5.0
MOVIE_FRAME_HOURS: int = 1
MOVIE_FPS: int = 24
MOVIE_DPI: int = 120
SCALE_BAR_M: float = 50.0
MIN_DAILY_AUDIO: int = 5
