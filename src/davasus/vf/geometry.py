# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.geometry                                           ║
# ║  « degrees in, metres out »                                      ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Local equirectangular projection around the farm (error well    ║
# ║  below GPS precision over a few hundred metres) and the          ║
# ║  plausibility gate for collar fixes.                             ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Local metric coordinates and fix plausibility."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from davasus.vf.constants import OutsideCode

EARTH_RADIUS_M: float = 6_371_008.8


def is_plausible_fix(fixes: pd.DataFrame) -> pd.Series:
    """True for fixes with real coordinates inside the real fence.

    Drops missing coordinates, the 0/0 no-fix placeholder and fixes coded
    ``Outside == 2`` (outside the real fence, defined as GPS error).
    """
    has_coords = fixes["gnss_lat"].notna() & fixes["gnss_lon"].notna()
    is_null_island = (fixes["gnss_lat"] == 0) & (fixes["gnss_lon"] == 0)
    is_gps_error = fixes["outside"] == OutsideCode.OUTSIDE_REAL_FENCE
    return has_coords & ~is_null_island & ~is_gps_error


@dataclass(frozen=True)
class LocalProjection:
    """Equirectangular projection centred on (``lat0``, ``lon0``)."""

    lat0: float
    lon0: float

    @classmethod
    def from_fixes(cls, fixes: pd.DataFrame) -> LocalProjection:
        """Centre on the median of the plausible fixes."""
        ok = fixes[is_plausible_fix(fixes)]
        if ok.empty:
            raise ValueError("no plausible fixes to centre the projection on")
        return cls(float(ok["gnss_lat"].median()), float(ok["gnss_lon"].median()))

    def to_metres(self, lat: pd.Series, lon: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        """East (x) and north (y) offsets in metres."""
        scale = np.pi / 180.0 * EARTH_RADIUS_M
        x = (np.asarray(lon, dtype=float) - self.lon0) * scale * np.cos(np.radians(self.lat0))
        y = (np.asarray(lat, dtype=float) - self.lat0) * scale
        return x, y

    def to_degrees(self, x_m: np.ndarray, y_m: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Inverse of ``to_metres``: (lat, lon) for east/north offsets."""
        scale = np.pi / 180.0 * EARTH_RADIUS_M
        lat = self.lat0 + np.asarray(y_m, dtype=float) / scale
        lon = self.lon0 + np.asarray(x_m, dtype=float) / (scale * np.cos(np.radians(self.lat0)))
        return lat, lon

    def add_xy(self, fixes: pd.DataFrame) -> pd.DataFrame:
        """Copy of ``fixes`` with ``x_m`` and ``y_m`` columns."""
        x, y = self.to_metres(fixes["gnss_lat"], fixes["gnss_lon"])
        return fixes.assign(x_m=x, y_m=y)
