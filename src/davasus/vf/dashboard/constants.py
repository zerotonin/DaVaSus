# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.constants                                ║
# ║  « knobs for the heifer dashboard »                              ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Playback speeds, trail length, map tiles and the semantic       ║
# ║  colours of the Outside codes. No Qt imports, so a web front     ║
# ║  end can share them.                                             ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Constants for the heifer dashboard (desktop and future web front end)."""

from __future__ import annotations

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
from davasus.vf.constants import OutsideCode

# ┌────────────────────────────────────────────────────────────┐
# │ Playback  « minutes per timer tick »                       │
# └────────────────────────────────────────────────────────────┘

PLAY_INTERVAL_MS: int = 50
PLAY_SPEEDS_MIN_PER_TICK: tuple[int, ...] = (1, 2, 5, 15, 60)
DEFAULT_PLAY_SPEED_INDEX: int = 1
TRAIL_MINUTES_CHOICES: tuple[int, ...] = (0, 10, 30, 60, 180)
DEFAULT_TRAIL_INDEX: int = 2
TIMELINE_BIN: str = "1h"

# ┌────────────────────────────────────────────────────────────┐
# │ Map  « OSM tiles, cached on disk »                         │
# └────────────────────────────────────────────────────────────┘

TILE_URL: str = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
TILE_ZOOM: int = 17
TILE_USER_AGENT: str = "davasus-vf-dashboard/1.0"
TILE_ATTRIBUTION: str = "© OpenStreetMap contributors"
TILE_TIMEOUT_S: float = 10.0
TILE_CACHE_SUBDIR: str = "osm_tiles"
FOOTPRINT_BIN_M: float = 5.0
MAP_PAD_M: float = 40.0
TILE_PAD_M: float = 400.0

# ┌────────────────────────────────────────────────────────────┐
# │ Colours  « Wong, one meaning each »                        │
# └────────────────────────────────────────────────────────────┘

OUTSIDE_COLOURS: dict[int, str] = {
    OutsideCode.INSIDE: WONG_GREEN,
    OutsideCode.OUTSIDE_PARCEL: WONG_VERMILLION,
    OutsideCode.OUTSIDE_REAL_FENCE: WONG_BLACK,
    OutsideCode.SEARCH_AFTER_ACTIVATION: WONG_SKY,
    OutsideCode.BREAKOUT_WITH_STIMULUS: WONG_PINK,
    OutsideCode.PRE_FENCE_ZONE: WONG_ORANGE,
    OutsideCode.PRE_FENCE_ZONE_WITH_STIMULUS: WONG_YELLOW,
}
OUTSIDE_LABELS: dict[int, str] = {
    OutsideCode.INSIDE: "inside parcel",
    OutsideCode.OUTSIDE_PARCEL: "outside parcel",
    OutsideCode.OUTSIDE_REAL_FENCE: "GPS error (outside real fence)",
    OutsideCode.SEARCH_AFTER_ACTIVATION: "search after activation",
    OutsideCode.BREAKOUT_WITH_STIMULUS: "breakout with stimulus",
    OutsideCode.PRE_FENCE_ZONE: "pre-fence zone",
    OutsideCode.PRE_FENCE_ZONE_WITH_STIMULUS: "pre-fence zone, stimulus",
}
MISSING_COLOUR: str = WONG_GREY
SELECTED_COLOUR: str = WONG_BLUE
CENTROID_COLOUR: str = WONG_BLACK
TRAIL_COLOUR: str = WONG_GREY
FOOTPRINT_COLOUR: str = WONG_GREY
CURSOR_COLOUR: str = WONG_VERMILLION
MANUAL_HERDING_COLOUR: str = WONG_YELLOW
REDUCED_GPS_COLOUR: str = WONG_PINK

DASHBOARD_COLOURS: dict[str, str] = {
    "air_temp": WONG_ORANGE,
    "thi": WONG_BLUE,
    "rel_humid": WONG_GREEN,
    "rain": WONG_SKY,
    "hp30": WONG_PINK,
    "kp": WONG_BLACK,
    "audio": WONG_ORANGE,
    "pulse": WONG_VERMILLION,
    "plausible": WONG_GREEN,
    "focal": WONG_BLUE,
    "herd_km": WONG_GREY,
}

TIMELINE_ROW_SPACE_WEATHER: str = "space weather"
TIMELINE_ROW_FOCAL_DAILY: str = "selected heifer: km and cues per day"
TIMELINE_ROW_MODES: tuple[str, ...] = (TIMELINE_ROW_FOCAL_DAILY, TIMELINE_ROW_SPACE_WEATHER)
