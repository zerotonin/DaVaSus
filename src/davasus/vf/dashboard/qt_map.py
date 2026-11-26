# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.qt_map                                   ║
# ║  « the herd, minute by minute, on the map »                      ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  pyqtgraph map in local metres: OSM tiles, the trial footprint,  ║
# ║  trails, one dot per heifer coloured by its Outside code, the    ║
# ║  herd centroid and a ring around the selected animal. Paddocks   ║
# ║  A-F are drawn faintly; the active parcel is filled and fenced.  ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Interactive herd map widget."""

from __future__ import annotations

import numpy as np
import pyqtgraph as pg
from PySide6.QtCore import QPointF, QRectF, Signal
from PySide6.QtGui import QImage, QPolygonF
from PySide6.QtWidgets import QGraphicsPolygonItem

from davasus.vf.dashboard.constants import (
    CENTROID_COLOUR,
    FOOTPRINT_BIN_M,
    FOOTPRINT_COLOUR,
    MAP_PAD_M,
    MISSING_COLOUR,
    OPEN_PADDOCK_ALPHA,
    OPEN_PADDOCK_COLOUR,
    OUTSIDE_COLOURS,
    OUTSIDE_LABELS,
    PADDOCK_LABEL_COLOUR,
    PADDOCK_LINE_ALPHA,
    PADDOCK_LINE_COLOUR,
    PARCEL_FENCE_COLOUR,
    SELECTED_COLOUR,
    TILE_PAD_M,
    TILE_ZOOM,
    TRAIL_COLOUR,
    TREE_ALPHA,
    TREE_COLOUR,
    VIRTUAL_FENCE_COLOUR,
)
from davasus.vf.dashboard.data import DashboardData, compute_footprint, compute_trails
from davasus.vf.dashboard.tiles import TileCache, compute_covering_tiles, compute_tile_bounds

DOT_SIZE: int = 11
RING_SIZE: int = 22
SCALE_BAR_M: float = 50.0
FOOTPRINT_ALPHA: int = 45
PADDOCK_Z: float = -4.0
TREE_Z: float = -3.5
FENCE_WIDTH: float = 2.0
VIRTUAL_FENCE_WIDTH: float = 3.5
PADDOCK_LABEL_PT: int = 13


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
        self._open_fills: list[QGraphicsPolygonItem] = []
        self._active_event: int | None = None
        self._parcel_fence = pg.PlotDataItem(pen=pg.mkPen(PARCEL_FENCE_COLOUR, width=FENCE_WIDTH),
                                             connect="finite")
        self._virtual_fence = pg.PlotDataItem(
            pen=pg.mkPen(VIRTUAL_FENCE_COLOUR, width=VIRTUAL_FENCE_WIDTH), connect="finite")
        self._paddock_lines = pg.PlotDataItem(
            pen=pg.mkPen(_with_alpha(PADDOCK_LINE_COLOUR, PADDOCK_LINE_ALPHA), width=1,
                         style=pg.QtCore.Qt.PenStyle.DashLine), connect="finite")
        for item in (self._paddock_lines, self._parcel_fence, self._virtual_fence):
            item.setZValue(PADDOCK_Z + 1)
            self.addItem(item)
        self._add_paddocks()
        self._tree_sample = self._add_trees()
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
        if int(data.event_index[i]) != self._active_event:
            self.show_parcel(int(data.event_index[i]))
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

    def show_parcel(self, row: int) -> None:
        """Fill the paddocks open in event ``row`` and draw its fences (none if < 0)."""
        self._active_event = row
        for item in self._open_fills:
            self.removeItem(item)
        self._open_fills = []
        data = self.data
        label = str(data.events.loc[row, "label"]) if row >= 0 else ""
        if row < 0 or label not in data.parcel_outlines:
            self._parcel_fence.setData([], [])
            self._virtual_fence.setData([], [])
        else:
            x, y, is_vf = data.parcel_outlines[label]
            self._parcel_fence.setData(*_edges_as_segments(x, y, ~is_vf))
            self._virtual_fence.setData(*_edges_as_segments(x, y, is_vf))
        letters = data.event_paddocks[row] if 0 <= row < len(data.event_paddocks) else ()
        brush = pg.mkBrush(_with_alpha(OPEN_PADDOCK_COLOUR, OPEN_PADDOCK_ALPHA))
        for letter in letters:
            for rx, ry in data.paddocks.get(letter, []):
                item = QGraphicsPolygonItem(QPolygonF([QPointF(float(a), float(b))
                                                       for a, b in zip(rx, ry, strict=True)]))
                item.setBrush(brush)
                item.setPen(pg.mkPen(None))
                item.setZValue(PADDOCK_Z)
                self.addItem(item)
                self._open_fills.append(item)

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

    def _add_paddocks(self) -> None:
        """Faint dashed outline and 'A 1' label for every paddock."""
        xs, ys = [], []
        for letter, rings in self.data.paddocks.items():
            for rx, ry in rings:
                xs += [*rx, rx[0], np.nan]
                ys += [*ry, ry[0], np.nan]
            biggest = max(rings, key=lambda ring: len(ring[0]))
            text = pg.TextItem(f"{letter} {self.data.paddock_numbers[letter]}",
                               color=PADDOCK_LABEL_COLOUR, anchor=(0.5, 0.5))
            font = text.textItem.font()
            font.setBold(True)
            font.setPointSize(PADDOCK_LABEL_PT)
            text.setFont(font)
            text.setPos(float(np.mean(biggest[0])), float(np.mean(biggest[1])))
            text.setZValue(PADDOCK_Z + 2)
            self.addItem(text)
        self._paddock_lines.setData(np.asarray(xs, float), np.asarray(ys, float))

    def _add_trees(self) -> pg.PlotDataItem:
        """Surveyed tree patches (permanent shade); returns a legend sample."""
        brush = pg.mkBrush(_with_alpha(TREE_COLOUR, TREE_ALPHA))
        pen = pg.mkPen(TREE_COLOUR, width=1)
        for rings in self.data.trees.values():
            for rx, ry in rings:
                item = QGraphicsPolygonItem(QPolygonF([QPointF(float(a), float(b))
                                                       for a, b in zip(rx, ry, strict=True)]))
                item.setBrush(brush)
                item.setPen(pen)
                item.setZValue(TREE_Z)
                self.addItem(item)
        return pg.PlotDataItem(pen=pen, fillLevel=0, brush=brush)

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
        if self.data.has_paddocks:
            legend.addItem(self._paddock_lines, "paddock (Parzelle) outline")
            legend.addItem(self._parcel_fence, "active parcel: physical fence")
            legend.addItem(self._virtual_fence, "active parcel: virtual fence")
        if self.data.trees:
            legend.addItem(self._tree_sample, "trees (permanent shade)")

    def _add_scale_bar(self) -> None:
        bar = pg.ScaleBar(size=SCALE_BAR_M, suffix="m", brush=pg.mkBrush("k"), pen=pg.mkPen("k"))
        bar.setParentItem(self.getPlotItem().getViewBox())
        bar.anchor((1, 1), (1, 1), offset=(-20, -20))


def _edges_as_segments(x: np.ndarray, y: np.ndarray, keep: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Edges k -> k+1 (wrapping) where ``keep[k]``, NaN-separated for connect='finite'."""
    following = np.roll(np.arange(len(x)), -1)
    sx, sy = [], []
    for k in np.flatnonzero(keep):
        sx += [x[k], x[following[k]], np.nan]
        sy += [y[k], y[following[k]], np.nan]
    return np.asarray(sx, float), np.asarray(sy, float)


def _with_alpha(colour: str, alpha: int):
    qcolour = pg.mkColor(colour)
    qcolour.setAlpha(alpha)
    return qcolour


def _load_rgba(path) -> np.ndarray:
    """Tile PNG as (rows, cols, 4) with row 0 at the bottom (pyqtgraph y up)."""
    image = QImage(str(path)).convertToFormat(QImage.Format.Format_RGBA8888)
    buffer = np.frombuffer(image.constBits(), dtype=np.ubyte)
    rgba = buffer.reshape(image.height(), image.bytesPerLine() // 4, 4)[:, : image.width()]
    return np.ascontiguousarray(rgba[::-1])
