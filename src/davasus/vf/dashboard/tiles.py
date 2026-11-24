# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.tiles                                    ║
# ║  « slippy-map tiles under the herd »                             ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Web-Mercator tile maths and a disk cache for OpenStreetMap      ║
# ║  tiles. Each tile is fetched once, with a plain User-Agent, and  ║
# ║  never again (OSM tile usage policy). Qt-free.                   ║
# ╚══════════════════════════════════════════════════════════════════╝
"""OpenStreetMap tile lookup and caching for the dashboard map."""

from __future__ import annotations

import logging
import math
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from davasus.vf.dashboard.constants import TILE_TIMEOUT_S, TILE_URL, TILE_USER_AGENT

log = logging.getLogger(__name__)

# ┌────────────────────────────────────────────────────────────┐
# │ Tile maths  « Web Mercator, OSM numbering »                │
# └────────────────────────────────────────────────────────────┘


@dataclass(frozen=True)
class TileBounds:
    """Geographic edges of one tile in degrees."""

    north: float
    south: float
    west: float
    east: float


def compute_tile_xy(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    """OSM tile column and row containing (``lat``, ``lon``)."""
    n = 2**zoom
    lat_rad = math.radians(lat)
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def compute_tile_bounds(x: int, y: int, zoom: int) -> TileBounds:
    n = 2**zoom

    def lat_of(row: int) -> float:
        return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * row / n))))

    return TileBounds(lat_of(y), lat_of(y + 1), x / n * 360.0 - 180.0, (x + 1) / n * 360.0 - 180.0)


def compute_covering_tiles(
    south: float, north: float, west: float, east: float, zoom: int
) -> list[tuple[int, int]]:
    """All (x, y) tiles that intersect the box."""
    x0, y0 = compute_tile_xy(north, west, zoom)
    x1, y1 = compute_tile_xy(south, east, zoom)
    return [(x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)]


# ┌────────────────────────────────────────────────────────────┐
# │ Cache  « fetch once, keep forever »                        │
# └────────────────────────────────────────────────────────────┘


class TileCache:
    """PNG tiles under ``cache_dir/<zoom>/<x>/<y>.png``."""

    def __init__(self, cache_dir: Path, url_template: str = TILE_URL) -> None:
        self.cache_dir = cache_dir
        self.url_template = url_template

    def path_for(self, zoom: int, x: int, y: int) -> Path:
        return self.cache_dir / str(zoom) / str(x) / f"{y}.png"

    def get(self, zoom: int, x: int, y: int, allow_download: bool = True) -> Path | None:
        """Cached tile path, downloading it first if needed; None on failure."""
        path = self.path_for(zoom, x, y)
        if path.is_file():
            return path
        if not allow_download:
            return None
        url = self.url_template.format(z=zoom, x=x, y=y)
        request = urllib.request.Request(url, headers={"User-Agent": TILE_USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=TILE_TIMEOUT_S) as response:
                payload = response.read()
        except OSError as exc:
            log.warning("tile %d/%d/%d not fetched: %s", zoom, x, y, exc)
            return None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return path
