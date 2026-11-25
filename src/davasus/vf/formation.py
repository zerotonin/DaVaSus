# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.formation                                          ║
# ║  « who holds the leading edge »                                  ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Positional leadership from the shape of the moving herd: for    ║
# ║  each moving minute the heifers on the front and rear arc of the ║
# ║  convex hull, and each heifer's offset along the travel axis.    ║
# ║  Collar lag can be removed by shifting fixes back along each     ║
# ║  heifer's own velocity before the geometry is computed.          ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Front and rear arcs of the moving herd, minute by minute."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from davasus.vf.constants import (
    ARC_DIRECTIONS,
    ARC_HALF_ANGLE_DEG,
    FORMATION_MIN_SPEED_M_MIN,
    MIN_ANIMALS_FOR_CENTROID,
)
from davasus.vf.initiation import HerdTracks, compute_central_speed

HEADING_SECTORS: tuple[str, ...] = ("N", "E", "S", "W")


@dataclass(frozen=True)
class FormationFrame:
    """Moving minutes (rows) by heifer (columns).

    ``front`` and ``rear`` are 1 when the heifer is an extreme point of the
    herd for some direction within ``ARC_HALF_ANGLE_DEG`` of the travel
    direction (or its opposite), 0 when not, NaN when she has no fix.
    ``along`` and ``lateral`` are offsets from the centroid in metres.
    """

    loms: list[str]
    minute_index: np.ndarray
    day_codes: np.ndarray
    herd_speed: np.ndarray
    heading_deg: np.ndarray
    along: np.ndarray
    lateral: np.ndarray
    front: np.ndarray
    rear: np.ndarray

    def select(self, rows: np.ndarray) -> FormationFrame:
        """Subset of minutes (boolean mask or integer index)."""
        return FormationFrame(self.loms, self.minute_index[rows], self.day_codes[rows],
                              self.herd_speed[rows], self.heading_deg[rows], self.along[rows],
                              self.lateral[rows], self.front[rows], self.rear[rows])

    @property
    def heading_sector(self) -> np.ndarray:
        """Compass sector of the travel direction (0 = N, 1 = E, 2 = S, 3 = W)."""
        return (((self.heading_deg + 45.0) % 360.0) // 90.0).astype(int)


def build_formation_frame(tracks: HerdTracks, lag_s: np.ndarray | None = None) -> FormationFrame:
    """Hull arcs and axis offsets for every moving, non-excluded minute.

    Args:
        tracks: Gap-filled herd tracks.
        lag_s:  Per-heifer collar lag in seconds (``estimate_sampling_lags``),
                or None for raw positions. A late collar shows where its
                heifer will be L seconds later, so each fix moves back by her
                own velocity times L.
    """
    x, y = tracks.x_m, tracks.y_m
    if lag_s is not None:
        x, y = shift_by_own_velocity(x, y, np.asarray(lag_s, dtype=float) / 60.0)
    n_ok = np.isfinite(x).sum(axis=1)
    has_centroid = n_ok >= MIN_ANIMALS_FOR_CENTROID
    with np.errstate(invalid="ignore"):
        cx = np.where(has_centroid, np.nansum(x, axis=1) / np.maximum(n_ok, 1), np.nan)
        cy = np.where(has_centroid, np.nansum(y, axis=1) / np.maximum(n_ok, 1), np.nan)
    vx = (np.r_[cx[1:], np.nan] - np.r_[np.nan, cx[:-1]]) / 2.0
    vy = (np.r_[cy[1:], np.nan] - np.r_[np.nan, cy[:-1]]) / 2.0
    speed = compute_central_speed(cx, cy)
    rows = np.flatnonzero((np.nan_to_num(speed) >= FORMATION_MIN_SPEED_M_MIN) & ~tracks.is_excluded)
    ux, uy = vx[rows] / speed[rows], vy[rows] / speed[rows]
    px, py = x[rows] - cx[rows, None], y[rows] - cy[rows, None]
    days = pd.DatetimeIndex(tracks.minutes[rows]).floor("D")
    return FormationFrame(
        loms=list(tracks.loms),
        minute_index=rows,
        day_codes=pd.factorize(days)[0],
        herd_speed=speed[rows],
        heading_deg=np.degrees(np.arctan2(ux, uy)) % 360.0,
        along=px * ux[:, None] + py * uy[:, None],
        lateral=-px * uy[:, None] + py * ux[:, None],
        front=compute_arc_membership(px, py, ux, uy),
        rear=compute_arc_membership(px, py, -ux, -uy),
    )


def shift_by_own_velocity(x: np.ndarray, y: np.ndarray, lag_min: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    nan_row = np.full((1, x.shape[1]), np.nan)
    vx = (np.r_[x[1:], nan_row] - np.r_[nan_row, x[:-1]]) / 2.0
    vy = (np.r_[y[1:], nan_row] - np.r_[nan_row, y[:-1]]) / 2.0
    return x - vx * lag_min, y - vy * lag_min


def compute_arc_membership(px: np.ndarray, py: np.ndarray, ux: np.ndarray, uy: np.ndarray) -> np.ndarray:
    """1 for heifers that are the extreme point for a direction within the arc.

    Sweeps ``ARC_DIRECTIONS`` directions across +/- ``ARC_HALF_ANGLE_DEG``
    around (ux, uy); a heifer on the arc is furthest out for at least one.
    """
    offsets = np.deg2rad(np.linspace(-ARC_HALF_ANGLE_DEG, ARC_HALF_ANGLE_DEG, ARC_DIRECTIONS))
    base = np.arctan2(uy, ux)[:, None] + offsets[None, :]
    present = np.isfinite(px)
    projection = (np.cos(base)[:, :, None] * np.where(present, px, 0.0)[:, None, :]
                  + np.sin(base)[:, :, None] * np.where(present, py, 0.0)[:, None, :])
    projection[np.broadcast_to(~present[:, None, :], projection.shape)] = -np.inf
    extreme = projection.argmax(axis=2)
    member = np.zeros(px.shape)
    np.put_along_axis(member, extreme, 1.0, axis=1)
    return np.where(present, member, np.nan)


# ┌────────────────────────────────────────────────────────────┐
# │ Per-heifer summaries  « arcs and offsets »                 │
# └────────────────────────────────────────────────────────────┘


def compute_arc_asymmetry(frame: FormationFrame) -> pd.DataFrame:
    """Front rate, rear rate and asymmetry (front - rear) / (front + rear)."""
    front = np.nanmean(frame.front, axis=0)
    rear = np.nanmean(frame.rear, axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        asymmetry = (front - rear) / (front + rear)
    return pd.DataFrame({"lom": frame.loms, "front_rate": front, "rear_rate": rear,
                         "asymmetry": asymmetry, "n_minutes": np.isfinite(frame.front).sum(axis=0)})


def fit_formation_offsets(frame: FormationFrame) -> pd.DataFrame:
    """Along-axis offset regressed on herd speed: intercept (m) and slope (s).

    The intercept is the heifer's place in the formation, free of collar lag;
    the slope mixes collar lag with walking ahead in time.
    """
    rows = []
    for a, lom in enumerate(frame.loms):
        ok = np.isfinite(frame.along[:, a])
        slope, intercept = np.polyfit(frame.herd_speed[ok], frame.along[ok, a], 1)
        rows.append((lom, intercept, 60.0 * slope))
    return pd.DataFrame(rows, columns=["lom", "intercept_m", "speed_slope_s"])
