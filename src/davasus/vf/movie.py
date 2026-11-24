# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.movie                                              ║
# ║  « one day per second »                                          ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Herd occupancy heatmap movie: each frame is the 2D histogram    ║
# ║  of all collars' plausible fixes in one hour, 24 frames per      ║
# ║  second, so one second of film is one day on pasture.            ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Herd occupancy heatmap movie."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.animation import FFMpegWriter
from matplotlib.colors import ListedColormap, LogNorm

from davasus.vf.constants import (
    FOOTPRINT_GREY,
    HEATMAP_CMAP,
    MOVIE_DPI,
    MOVIE_FPS,
    SCALE_BAR_M,
)
from davasus.vf.geometry import LocalProjection, is_plausible_fix
from davasus.vf.viz import add_scale_bar
from davasus.viz import apply_style

apply_style()

# ┌────────────────────────────────────────────────────────────┐
# │ Occupancy frames  « vectorised 3D histogram »              │
# └────────────────────────────────────────────────────────────┘


@dataclass(frozen=True)
class HeatmapGrid:
    """Square bins in local metres."""

    x_edges: np.ndarray
    y_edges: np.ndarray

    @classmethod
    def around(cls, x: np.ndarray, y: np.ndarray, bin_m: float, pad_m: float = 20.0) -> HeatmapGrid:
        """Grid covering the 0.5-99.5 % range of the points plus padding."""
        x_lo, x_hi = np.nanpercentile(x, [0.5, 99.5])
        y_lo, y_hi = np.nanpercentile(y, [0.5, 99.5])
        return cls(
            np.arange(x_lo - pad_m, x_hi + pad_m + bin_m, bin_m),
            np.arange(y_lo - pad_m, y_hi + pad_m + bin_m, bin_m),
        )

    @property
    def extent(self) -> tuple[float, float, float, float]:
        return (self.x_edges[0], self.x_edges[-1], self.y_edges[0], self.y_edges[-1])


@dataclass(frozen=True)
class OccupancyFrames:
    """Fix counts per frame and bin, shape (n_frames, n_y, n_x)."""

    starts: pd.DatetimeIndex
    counts: np.ndarray
    grid: HeatmapGrid


def compute_occupancy_frames(
    fixes: pd.DataFrame,
    projection: LocalProjection,
    bin_m: float,
    frame_hours: int,
) -> OccupancyFrames:
    """Bin plausible fixes of all animals into hourly occupancy maps."""
    ok = fixes[is_plausible_fix(fixes)]
    x, y = projection.to_metres(ok["gnss_lat"], ok["gnss_lon"])
    grid = HeatmapGrid.around(x, y, bin_m)
    frame_length = pd.Timedelta(hours=frame_hours)
    first = ok["timestamp"].min().floor(frame_length)
    frame = ((ok["timestamp"] - first) // frame_length).to_numpy()
    xi = np.digitize(x, grid.x_edges) - 1
    yi = np.digitize(y, grid.y_edges) - 1
    inside = (xi >= 0) & (xi < len(grid.x_edges) - 1) & (yi >= 0) & (yi < len(grid.y_edges) - 1)
    n_frames = int(frame.max()) + 1
    counts = np.zeros((n_frames, len(grid.y_edges) - 1, len(grid.x_edges) - 1), dtype=np.int32)
    np.add.at(counts, (frame[inside], yi[inside], xi[inside]), 1)
    starts = pd.date_range(first, periods=n_frames, freq=frame_length)
    return OccupancyFrames(starts, counts, grid)


def summarise_frames(frames: OccupancyFrames) -> pd.DataFrame:
    """Per-frame table for the movie's CSV companion."""
    flat = frames.counts.reshape(len(frames.starts), -1)
    return pd.DataFrame({
        "frame_start_utc": frames.starts,
        "n_fixes": flat.sum(axis=1),
        "n_occupied_bins": (flat > 0).sum(axis=1),
        "max_bin_count": flat.max(axis=1),
    })


# ┌────────────────────────────────────────────────────────────┐
# │ Rendering  « ffmpeg, one artist updated per frame »        │
# └────────────────────────────────────────────────────────────┘


def render_occupancy_movie(
    frames: OccupancyFrames,
    events: pd.DataFrame,
    path: Path,
    fps: int = MOVIE_FPS,
) -> Path:
    """Write ``frames`` as an MP4; at 24 fps and 1 h frames, 1 s is 1 day.

    Raises:
        RuntimeError: ffmpeg is not available to matplotlib.
    """
    _point_matplotlib_at_ffmpeg()
    path.parent.mkdir(parents=True, exist_ok=True)
    vmax = max(1, int(np.percentile(frames.counts[frames.counts > 0], 99)))
    fig, ax = plt.subplots(figsize=(7.0, 6.0))
    footprint = frames.counts.sum(axis=0) > 0
    ax.imshow(np.ma.masked_equal(footprint, 0), origin="lower", extent=frames.grid.extent,
              cmap=ListedColormap([FOOTPRINT_GREY]), interpolation="nearest", alpha=0.35)
    image = ax.imshow(
        np.ma.masked_equal(frames.counts[0], 0), origin="lower", extent=frames.grid.extent,
        cmap=HEATMAP_CMAP, norm=LogNorm(vmin=1, vmax=vmax), interpolation="nearest",
    )
    ax.set_aspect("equal")
    ax.grid(False)
    ax.set_xticks([])
    ax.set_yticks([])
    add_scale_bar(ax, SCALE_BAR_M)
    colourbar = fig.colorbar(image, ax=ax, fraction=0.04, pad=0.02)
    colourbar.set_label(r"fixes bin$^{-1}$ h$^{-1}$")
    stamp = ax.text(0.02, 0.98, "", transform=ax.transAxes, va="top", family="monospace")
    labels = _event_labels(frames.starts, events)

    writer = FFMpegWriter(fps=fps, metadata={"title": "DaVaSus heifer occupancy"})
    with writer.saving(fig, str(path), dpi=MOVIE_DPI):
        for i, start in enumerate(frames.starts):
            image.set_data(np.ma.masked_equal(frames.counts[i], 0))
            stamp.set_text(f"{start:%Y-%m-%d %H:%M} UTC\nparcels {labels[i]}")
            writer.grab_frame()
    plt.close(fig)
    return path


def _point_matplotlib_at_ffmpeg() -> None:
    """Use ffmpeg from PATH, else the one next to this interpreter (conda env).

    Raises:
        RuntimeError: no ffmpeg found.
    """
    if FFMpegWriter.isAvailable():
        return
    candidate = Path(sys.executable).parent / "ffmpeg"
    if candidate.is_file():
        matplotlib.rcParams["animation.ffmpeg_path"] = str(candidate)
        if FFMpegWriter.isAvailable():
            return
    raise RuntimeError("ffmpeg not found; install it (conda install -c conda-forge ffmpeg)")


def _event_labels(starts: pd.DatetimeIndex, events: pd.DataFrame) -> list[str]:
    ordered = events.sort_values("start_utc")
    position = np.searchsorted(ordered["start_utc"].to_numpy(), starts.to_numpy(), side="right") - 1
    names = ordered["open_parcels"].to_numpy()
    return [names[p] if p >= 0 else "" for p in position]
