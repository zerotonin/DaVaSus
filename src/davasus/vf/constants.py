# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.constants                                          ║
# ║  « codes, sentinels and defaults for M8 »                        ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Outside codes from the heifer glossary, the no-fence sentinel,  ║
# ║  time-base offsets and the default analysis windows.             ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Shared constants for the virtual-fence (M8) analysis."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from davasus.constants import (
    WONG_BLACK,
    WONG_BLUE,
    WONG_GREEN,
    WONG_GREY,
    WONG_ORANGE,
    WONG_PINK,
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
# │ Initiation (Dumont 2005)  « handover 4.3.1, 4.8 »          │
# └────────────────────────────────────────────────────────────┘

# Herd travel: centroid speed from Sarova et al. (2010), travelling 16 m/min,
# foraging 5 m/min; 10 m/min separates the two (first check, 24 Nov).
TRAVEL_SPEED_M_MIN: float = 10.0
MIN_ANIMALS_FOR_CENTROID: int = 20
GAP_FILL_MIN: int = 3
BOUT_MERGE_GAP_MIN: int = 2
BOUT_MIN_LENGTH_MIN: int = 3
BOUT_MIN_DISPLACEMENT_M: float = 50.0
BOUT_PRE_MIN: int = 10
BOUT_POST_MIN: int = 15
DEPARTURE_THRESHOLD_M: float = 15.0
JOIN_FRACTION_OF_HERD: float = 0.5
GROUP_MOVEMENT_MIN_JOINED: float = 0.5
MIN_PRESENT_COVERAGE: float = 0.8
# VF cut-offs are semi-forced, not spontaneous; manual herding is forced.
ACTIVATION_EXCLUSION_MIN: int = 120
# Half the 2024 fix interval: departures closer than this count as tied.
TIE_TOLERANCE_MIN: float = 0.5
TIE_FRACTION_LIMIT: float = 0.3
# A first mover at least one fix interval ahead survives any collar lag (< 30 s).
CLEAR_LEAD_MIN: float = 1.0

# Failed and successful initiation attempts (Sueur 2018; Anzai 2026).
ATTEMPT_WINDOW_MIN: int = 3
ATTEMPT_MIN_DISPLACEMENT_M: float = 30.0
ATTEMPT_MIN_STRAIGHTNESS: float = 0.8
ATTEMPT_PRE_MIN: int = 5
ATTEMPT_PRE_MAX_DISPLACEMENT_M: float = 15.0
ATTEMPT_REFRACTORY_MIN: int = 15
FOLLOW_SEARCH_MIN: int = 15
FOLLOW_MIN_DISPLACEMENT_M: float = 30.0
FOLLOW_MAX_ANGLE_DEG: float = 45.0
# Chance baseline: the same count 30 to 120 min before or after the attempt.
CHANCE_SHIFT_MIN: tuple[int, int] = (30, 121)

# Formation (Sarova 2010 positional leadership, herd shape).
FORMATION_MIN_SPEED_M_MIN: float = 3.0
ARC_HALF_ANGLE_DEG: float = 45.0
ARC_DIRECTIONS: int = 19
# Slow band first: it carries the smallest collar-lag shift and decides roles.
FORMATION_SPEED_BANDS: dict[str, tuple[float, float]] = {
    "slow 3-10 m/min": (3.0, 10.0),
    "travel >= 10 m/min": (10.0, float("inf")),
}

# GPS noise floor: heifers with at most 20 IMU ticks (> 40 mg) per minute for
# 15 min or more; 2024 values in the comments (63 000 still minutes).
GPS_STILL_IMU_MAX: int = 20
GPS_STILL_MIN_RUN_MIN: int = 15
# Median apparent displacement of a still heifer over 1 to 30 min (2.3-3.5 m).
MOVE_NOISE_M: float = 3.5
# 95 % of still-heifer displacements (10-12 m): above this a heifer has moved.
MOVE_MIN_M: float = 12.0

# Pull and anchor events (Strandburg-Peshkin et al. 2015).
# Each swing must clear 99 % of the pair-distance range of two still heifers
# within 20 min (20.1 m; 15.3 m at 95 %) with a margin.
PULL_SWING_M: float = 22.0
PULL_SWING_SENSITIVITY_M: tuple[float, ...] = (15.0, 30.0)
PULL_DISTANCE_SMOOTHING_MIN: int = 3
PULL_MIN_RUN_MIN: int = 10
PULL_MAX_PHASE_MIN: int = 60
PULL_MIN_STRENGTH: float = 0.1

# Statistics.
BOOTSTRAP_DRAWS: int = 2_000
GRADED_NULL_DRAWS: int = 2_000
PL_PSEUDO_WINS: float = 0.1
PL_MAX_ITERATIONS: int = 500
PL_TOLERANCE: float = 1e-8
RANDOM_SEED: int = 20_241_125

# ┌────────────────────────────────────────────────────────────┐
# │ Paddocks  « Parzelle 1-6 = A-F, eShepherd parcels »        │
# └────────────────────────────────────────────────────────────┘

# Naming: a *paddock* is one of the six fixed sub-areas of the pasture
# (Parzelle 1-6, map letters A-F); a *parcel* is one eShepherd virtual-fence
# layout, i.e. the paddocks open at one time (e.g. "heifers 3_4" = C + D).
PARCEL_SHAPE_SUBDIR: str = "Parzellen_Tränke/Parzellen"
PADDOCK_SLIVER_M2: float = 100.0
# Surveyed tree patches (permanent shade, basis of in_shadow); polygon layers only.
TREE_GEOPACKAGE: str = "Luxuswiesen_Wald.gpkg"
# A paddock is open in a parcel when this share of its area lies inside it.
PADDOCK_OPEN_SHARE: float = 0.5


@dataclass(frozen=True)
class PaddockRecipe:
    """Paddock = intersection of ``intersect`` layouts minus paddocks in ``minus``."""

    letter: str
    parzelle: int
    intersect: tuple[str, ...]
    minus: tuple[str, ...] = ()


# Ordered so every paddock in ``minus`` is built before it is used. Each
# shared area of two consecutive layouts is the paddock both kept open.
PADDOCK_RECIPES: tuple[PaddockRecipe, ...] = (
    PaddockRecipe("C", 3, ("heifers 3_2", "heifers 3_4")),
    PaddockRecipe("D", 4, ("heifers 3_4", "heifers 4_5")),
    PaddockRecipe("E", 5, ("heifers 4_5", "heifers 5_6")),
    PaddockRecipe("B", 2, ("heifers 3_2",), ("C",)),
    PaddockRecipe("F", 6, ("heifers 5_6",), ("E",)),
    PaddockRecipe("A", 1, ("luciano Heifers 2",), ("B",)),
)

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
    "leader_raw": WONG_BLUE,
    "leader_lag": WONG_GREEN,
    "null_egalitarian": WONG_GREY,
    "null_graded": WONG_PINK,
}
ROLE_COLOURS: dict[str, str] = {
    "front": WONG_GREEN,
    "probable front": WONG_SKY,
    "middle": WONG_GREY,
    "timing-dependent": WONG_PINK,
    "probable rear": WONG_ORANGE,
    "rear": WONG_VERMILLION,
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
