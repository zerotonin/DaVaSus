# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.qt_map                                   ║
# ║  « the herd, minute by minute, on the map »                      ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  pyqtgraph map in local metres: OSM tiles, the trial footprint,  ║
# ║  trails, one dot per heifer coloured by its Outside code, the    ║
# ║  herd centroid and a ring around the selected animal.            ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Interactive herd map widget."""

from __future__ import annotations

import numpy as np
import pyqtgraph as pg
from PySide6.QtCore import QRectF, Signal
from PySide6.QtGui import QImage

from davasus.vf.dashboard.constants import (
    CENTROID_COLOUR,
    FOOTPRINT_BIN_M,
    FOOTPRINT_COLOUR,
    MAP_PAD_M,
    MISSING_COLOUR,
    OUTSIDE_COLOURS,
    OUTSIDE_LABELS,
    SELECTED_COLOUR,
    TILE_PAD_M,
    TILE_ZOOM,
    TRAIL_COLOUR,
)
from davasus.vf.dashboard.data import DashboardData, compute_footprint, compute_trails
from davasus.vf.dashboard.tiles import TileCache, compute_covering_tiles, compute_tile_bounds

DOT_SIZE: int = 11
RING_SIZE: int = 22
SCALE_BAR_M: float = 50.0
FOOTPRINT_ALPHA: int = 45


class HerdMapWidget(pg.PlotWidget):
    """Map of all heifers at one minute; click a dot to select it."""

    animal_clicked = Signal(int)

    def __init__(self, data: DashboardData, tiles: TileCache | None) -> None:
        super().__init__(background="w")
        self.data = data
        self.setAspectLocked(True)
        self.hideAxis("left")
        self.hideAxis("bottom")
        self.setMenuEnabled(False)
        self._brushes = {code: pg.mkBrush(colour) for code, colour in OUTSIDE_COLOURS.items()}
        self._missing_brush = pg.mkBrush(MISSING_COLOUR)

        extent = self._add_footprint()
        if tiles is not None:
            self._add_tiles(tiles, extent)
        self._trails = pg.PlotDataItem(pen=pg.mkPen(TRAIL_COLOUR, width=1.2), connect="finite")
        self._dots = pg.ScatterPlotItem(size=DOT_SIZE, pen=pg.mkPen("k", width=0.8))
        self._dots.sigClicked.connect(self._on_dot_clicked)
        self._ring = pg.ScatterPlotItem(size=RING_SIZE, brush=None,
                                        pen=pg.mkPen(SELECTED_COLOUR, width=3))
        self._centroid = pg.ScatterPlotItem(size=16, symbol="+", brush=pg.mkBrush(CENTROID_COLOUR),
                                            pen=pg.mkPen(CENTROID_COLOUR, width=2))
        for item in (self._trails, self._dots, self._ring, self._centroid):
            self.addItem(item)
        self._labels = [pg.TextItem(lom[-4:], color="k", anchor=(-0.15, 1.1))
                        for lom in data.loms]
        for label in self._labels:
            label.setZValue(20)
            self.addItem(label)
        self._add_legend()
        self._add_scale_bar()
        x0, x1, y0, y1 = extent
        self.setRange(QRectF(x0 - MAP_PAD_M, y0 - MAP_PAD_M,
                             x1 - x0 + 2 * MAP_PAD_M, y1 - y0 + 2 * MAP_PAD_M), padding=0)

    # ─────────────────────────────────────────────────────────────────
    #  Per-minute update
    # ─────────────────────────────────────────────────────────────────

    def show_minute(
        self, i: int, trail_minutes: int, selected: int, show_labels: bool, follow: bool
    ) -> None:
        """Draw minute ``i``; ``follow`` keeps the herd centroid in the view centre."""
        data = self.data
        x, y = data.x_m[i], data.y_m[i]
        ok = np.isfinite(x)
        brushes = [self._brushes.get(int(code), self._missing_brush) for code in data.outside[i]]
        index = np.flatnonzero(ok)
        self._dots.setData(x=x[ok], y=y[ok], data=index, brush=[brushes[k] for k in index])
        tx, ty = compute_trails(data, i, trail_minutes)
        self._trails.setData(tx, ty)
        if 0 <= selected < data.n_animals and ok[selected]:
            self._ring.setData([x[selected]], [y[selected]])
        else:
            self._ring.setData([], [])
        if ok.any():
            self._centroid.setData([float(x[ok].mean())], [float(y[ok].mean())])
        else:
            self._centroid.setData([], [])
        if follow and ok.any():
            self.getPlotItem().getViewBox().translateBy(
                x=float(x[ok].mean()) - self.viewRect().center().x(),
                y=float(y[ok].mean()) - self.viewRect().center().y())
        for a, label in enumerate(self._labels):
            visible = show_labels and bool(ok[a])
            label.setVisible(visible)
            if visible:
                label.setPos(float(x[a]), float(y[a]))

    def _on_dot_clicked(self, _item: pg.ScatterPlotItem, points: list, _event: object) -> None:
        if len(points):
            self.animal_clicked.emit(int(points[0].data()))

    # ─────────────────────────────────────────────────────────────────
    #  Static layers
    # ─────────────────────────────────────────────────────────────────

    def _add_footprint(self) -> tuple[float, float, float, float]:
        counts, extent = compute_footprint(self.data, FOOTPRINT_BIN_M)
        colour = pg.mkColor(FOOTPRINT_COLOUR)
        rgba = np.zeros((*counts.shape, 4), dtype=np.ubyte)
        rgba[counts > 0] = (colour.red(), colour.green(), colour.blue(), FOOTPRINT_ALPHA)
        image = pg.ImageItem(rgba, axisOrder="row-major")
        x0, x1, y0, y1 = extent
        image.setRect(QRectF(x0, y0, x1 - x0, y1 - y0))
        image.setZValue(-5)
        self.addItem(image)
        return extent

    def _add_tiles(self, tiles: TileCache, extent: tuple[float, float, float, float]) -> None:
        x0, x1, y0, y1 = extent
        projection = self.data.projection
        lat, lon = projection.to_degrees(np.array([x0 - TILE_PAD_M, x1 + TILE_PAD_M]),
                                         np.array([y0 - TILE_PAD_M, y1 + TILE_PAD_M]))
        for tx, ty in compute_covering_tiles(lat[0], lat[1], lon[0], lon[1], TILE_ZOOM):
            path = tiles.get(TILE_ZOOM, tx, ty)
            if path is None:
                continue
            bounds = compute_tile_bounds(tx, ty, TILE_ZOOM)
            west, north = projection.to_metres(np.array([bounds.north]), np.array([bounds.west]))
            east, south = projection.to_metres(np.array([bounds.south]), np.array([bounds.east]))
            image = pg.ImageItem(_load_rgba(path), axisOrder="row-major")
            image.setRect(QRectF(float(west[0]), float(south[0]),
                                 float(east[0] - west[0]), float(north[0] - south[0])))
            image.setZValue(-10)
            self.addItem(image)

    def _add_legend(self) -> None:
        legend = self.addLegend(offset=(10, 10), labelTextColor="k",
                                brush=pg.mkBrush(255, 255, 255, 200))
        for code, label in OUTSIDE_LABELS.items():
            sample = pg.ScatterPlotItem(size=DOT_SIZE, brush=self._brushes[code], pen="k")
            legend.addItem(sample, f"{code}  {label}")
        legend.addItem(pg.ScatterPlotItem(size=RING_SIZE, brush=None,
                                          pen=pg.mkPen(SELECTED_COLOUR, width=3)), "selected")
        legend.addItem(self._centroid, "herd centroid")

    def _add_scale_bar(self) -> None:
        bar = pg.ScaleBar(size=SCALE_BAR_M, suffix="m", brush=pg.mkBrush("k"), pen=pg.mkPen("k"))
        bar.setParentItem(self.getPlotItem().getViewBox())
        bar.anchor((1, 1), (1, 1), offset=(-20, -20))


def _load_rgba(path) -> np.ndarray:
    """Tile PNG as (rows, cols, 4) with row 0 at the bottom (pyqtgraph y up)."""
    image = QImage(str(path)).convertToFormat(QImage.Format.Format_RGBA8888)
    buffer = np.frombuffer(image.constBits(), dtype=np.ubyte)
    rgba = buffer.reshape(image.height(), image.bytesPerLine() // 4, 4)[:, : image.width()]
    return np.ascontiguousarray(rgba[::-1])
