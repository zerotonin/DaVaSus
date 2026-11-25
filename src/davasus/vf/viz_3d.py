# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_3d                                             ║
# ║  « every heifer, every day, every cue »                          ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Two 3D panels, audio cues and pulses: x = trial day,            ║
# ║  y = heifer, z = cues per day as one ridge per heifer. Back      ║
# ║  wall: herd total per day. Side wall: trial total per heifer.    ║
# ║  Heifers are ordered by total audio cues in both panels.         ║
# ╚══════════════════════════════════════════════════════════════════╝
"""3D cue landscape for the virtual-fence trial."""

from __future__ import annotations

from dataclasses import dataclass

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.collections import PolyCollection
from matplotlib.figure import Figure

from davasus.vf.constants import VF_COLOURS
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (17.0, 8.0)
VIEW_ELEVATION_DEG: float = 28.0
VIEW_AZIMUTH_DEG: float = -58.0
RIDGE_ALPHA: float = 0.55
WALL_ALPHA: float = 0.75
WALL_BAR_WIDTH: float = 0.8
WALL_GAP: float = 1.5
# Wall captions in axes coordinates, so they never collide with the data.
SIDE_LABEL_XY: tuple[float, float] = (0.05, 0.97)
BACK_LABEL_XY: tuple[float, float] = (0.05, 0.93)
ASINH_TICKS: tuple[float, ...] = (0, 1, 3, 10, 30, 100, 300, 1000)


@dataclass(frozen=True)
class CueLandscape:
    """Daily cue matrix, heifers ordered by total audio (fewest first)."""

    days: pd.DatetimeIndex
    loms: list[str]
    audio: np.ndarray
    pulse: np.ndarray

    @property
    def day_numbers(self) -> np.ndarray:
        return np.arange(len(self.days), dtype=float)


def build_cue_landscape(daily: pd.DataFrame) -> CueLandscape:
    """Pivot ``compute_daily_stimulus_summary`` output to (heifer, day) matrices."""
    audio = daily.pivot_table(index="lom", columns="date", values="audio",
                              aggfunc="sum", fill_value=0.0)
    pulse = daily.pivot_table(index="lom", columns="date", values="pulse",
                              aggfunc="sum", fill_value=0.0).reindex_like(audio).fillna(0.0)
    order = audio.sum(axis=1).sort_values().index
    audio, pulse = audio.loc[order], pulse.loc[order]
    return CueLandscape(pd.DatetimeIndex(audio.columns), list(order),
                        audio.to_numpy(dtype=float), pulse.to_numpy(dtype=float))


# ┌────────────────────────────────────────────────────────────┐
# │ Figure  « one panel per cue type, two histogram walls »    │
# └────────────────────────────────────────────────────────────┘


def plot_cue_landscape_3d(
    daily: pd.DataFrame, events: pd.DataFrame, asinh: bool = False
) -> tuple[Figure, pd.DataFrame]:
    """Audio cues (left) and pulses (right) per heifer and day, with histogram walls.

    Each panel has its own z axis in true counts. The back wall is the herd
    total per day, the side wall each heifer's trial total; the walls are
    scaled to the panel and labelled with their maximum.

    Args:
        daily:  ``compute_daily_stimulus_summary`` output.
        events: ``load_events`` output; activations are marked on the back wall.
        asinh:  Plot z as asinh(count) (ridges and walls); ticks stay in counts.
                Linear near zero, logarithmic for large counts, defined at 0.

    Returns:
        Figure and the long table behind it (heifer, day, audio, pulse).
    """
    land = build_cue_landscape(daily)
    fig = plt.figure(figsize=FIGSIZE_IN)
    for column, kind in enumerate(("audio", "pulse")):
        ax = fig.add_subplot(1, 2, column + 1, projection="3d")
        _draw_panel(ax, land, events, kind, _z_transform(asinh))
        ax.set_title(f"{'AB'[column]}  {_noun(kind)}", loc="left", fontweight="bold")
    scale_note = "; ridges and walls asinh-scaled, z ticks in counts" if asinh else ""
    fig.suptitle("Cues per heifer and day; heifers ordered by total audio cues (both panels)"
                 + scale_note, fontsize="medium")
    fig.subplots_adjust(left=0.0, right=0.98, wspace=0.02)
    return fig, _to_long_table(land)


def _draw_panel(ax, land: CueLandscape, events: pd.DataFrame, kind: str, zt: ZTransform) -> None:
    values = getattr(land, kind)
    x, n_heifers = land.day_numbers, len(land.loms)
    z_top = float(zt.forward(max(values.max(), 1.0)))
    _draw_ridges(ax, zt.forward(values), x, kind)
    _draw_time_wall(ax, x, values.sum(axis=0), kind, n_heifers, z_top, zt)
    _draw_heifer_wall(ax, values.sum(axis=1), kind, -1.0, z_top, zt)
    _mark_activations(ax, land, events, n_heifers, z_top)
    _format_axes(ax, land, z_top, kind)
    if zt.is_asinh:
        ticks = [v for v in ASINH_TICKS if v <= max(values.max(), 1.0)]
        ax.set_zticks(zt.forward(np.asarray(ticks, dtype=float)), [f"{v:g}" for v in ticks])


@dataclass(frozen=True)
class ZTransform:
    """Count-to-height mapping shared by ridges and walls."""

    is_asinh: bool

    def forward(self, values):
        return np.arcsinh(values) if self.is_asinh else np.asarray(values, dtype=float)


def _z_transform(asinh: bool) -> ZTransform:
    return ZTransform(asinh)


def _draw_ridges(ax, values: np.ndarray, x: np.ndarray, kind: str) -> None:
    verts = [list(zip(np.r_[x[0], x, x[-1]], np.r_[0.0, row, 0.0], strict=True))
             for row in values]
    ridges = PolyCollection(verts, facecolors=VF_COLOURS[kind], edgecolors="black",
                            linewidths=0.4, alpha=RIDGE_ALPHA)
    ax.add_collection3d(ridges, zs=np.arange(len(values)), zdir="y")


def _draw_time_wall(ax, x: np.ndarray, values: np.ndarray, kind: str,
                    n_heifers: int, z_top: float, zt: ZTransform) -> None:
    scale = z_top / zt.forward(max(values.max(), 1.0))
    y_wall = n_heifers + WALL_GAP
    ax.bar(x, zt.forward(values) * scale, zs=y_wall, zdir="y", width=WALL_BAR_WIDTH,
           color=VF_COLOURS[kind], alpha=WALL_ALPHA, edgecolor="none")
    ax.text2D(*BACK_LABEL_XY, f"back wall: herd total per day (max {values.max():g})",
              transform=ax.transAxes, fontsize="small", color=VF_COLOURS[kind], ha="left")


def _draw_heifer_wall(ax, values: np.ndarray, kind: str, x_wall: float, z_top: float,
                      zt: ZTransform) -> None:
    scale = z_top / zt.forward(max(values.max(), 1.0))
    y = np.arange(len(values), dtype=float)
    ax.bar(y, zt.forward(values) * scale, zs=x_wall, zdir="x", width=WALL_BAR_WIDTH,
           color=VF_COLOURS[kind], alpha=WALL_ALPHA, edgecolor="none")
    ax.text2D(*SIDE_LABEL_XY, f"side wall: trial total per heifer (max {values.max():g})",
              transform=ax.transAxes, fontsize="small", color=VF_COLOURS[kind], ha="left")


def _mark_activations(ax, land: CueLandscape, events: pd.DataFrame,
                      n_heifers: int, z_top: float) -> None:
    first = land.days[0]
    y_wall = n_heifers + WALL_GAP
    for event in events.itertuples():
        if event.is_transition:
            continue
        day = (event.start_utc - first).total_seconds() / 86_400.0
        ax.plot([day, day], [y_wall, y_wall], [0, z_top], color="black", lw=0.8, ls="--")
        ax.text(day, y_wall, z_top * 0.98, event.open_parcels, fontsize="x-small")


def _format_axes(ax, land: CueLandscape, z_top: float, kind: str) -> None:
    n_days, n_heifers = len(land.days), len(land.loms)
    ticks = np.arange(0, n_days, 7)
    ax.set_xticks(ticks, [f"{land.days[t]:%d %b}" for t in ticks])
    ax.set_yticks(np.arange(n_heifers), [lom[-4:] for lom in land.loms], fontsize="x-small")
    ax.set_xlim(-1, n_days)
    ax.set_ylim(-1, n_heifers + WALL_GAP)
    ax.set_zlim(0, z_top * 1.1)
    ax.set_xlabel("date (UTC)", labelpad=10)
    ax.set_ylabel("heifer (ear tag, last 4 digits)", labelpad=14)
    ax.set_zlabel(rf"{_noun(kind)} day$^{{-1}}$")
    ax.view_init(elev=VIEW_ELEVATION_DEG, azim=VIEW_AZIMUTH_DEG)
    ax.set_box_aspect((2.2, 1.6, 0.9))
    for axis in (ax.xaxis, ax.yaxis, ax.zaxis):
        axis.pane.set_alpha(0.0)
    ax.grid(False)


def _noun(kind: str) -> str:
    return "audio cues" if kind == "audio" else "pulses"


def _to_long_table(land: CueLandscape) -> pd.DataFrame:
    return pd.DataFrame({
        "lom": np.repeat(land.loms, len(land.days)),
        "date": np.tile(land.days, len(land.loms)),
        "audio": land.audio.ravel(),
        "pulse": land.pulse.ravel(),
    })
