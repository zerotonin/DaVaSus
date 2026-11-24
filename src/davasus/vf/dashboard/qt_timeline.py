# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.qt_timeline                              ║
# ║  « the whole trial at a glance »                                 ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Stacked hourly plots (weather, space weather, cues, GNSS)       ║
# ║  with linked time axes, parcel activations and one draggable     ║
# ║  cursor. Click or drag anywhere to jump in time.                 ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Trial overview timeline with a time cursor."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pyqtgraph as pg
from PySide6.QtCore import Qt, Signal

from davasus.vf.dashboard.constants import (
    CURSOR_COLOUR,
    DASHBOARD_COLOURS,
    MANUAL_HERDING_COLOUR,
    TIMELINE_BIN,
)
from davasus.vf.dashboard.data import DashboardData, compute_animal_hourly_cues
from davasus.vf.gnss_quality import HP30_STORM_THRESHOLD

PLOT_HEIGHT_PX: int = 90


def _epoch_s(index: pd.DatetimeIndex) -> np.ndarray:
    """Seconds since 1970 whatever the index unit (pandas may store us or s)."""
    return index.as_unit("ns").asi8 / 1e9


class TimelineWidget(pg.GraphicsLayoutWidget):
    """Weather, space weather, herd cues and GNSS share per hour."""

    time_selected = Signal(float)

    def __init__(self, data: DashboardData, hourly: pd.DataFrame) -> None:
        super().__init__()
        self.setBackground("w")
        self.data = data
        self._t = _epoch_s(hourly.index)
        self._hourly = hourly
        self._cursors: list[pg.InfiniteLine] = []
        self._plots = [
            self._add_plot(0, "°C / THI", [("air_temp_avg", "air T"), ("thi_nrc", "THI")]),
            self._add_plot(1, "Hp30 / Kp", [("hp30", "Hp30"), ("kp", "Kp")]),
            self._add_plot(2, "cues h⁻¹", [("audio", "herd audio"),
                                                    ("pulse", "herd pulses")]),
            self._add_plot(3, "plausible", [("plausible_share", "share plausible fixes")]),
        ]
        self._plots[1].addItem(pg.InfiniteLine(HP30_STORM_THRESHOLD, angle=0,
                                               pen=pg.mkPen("k", style=Qt.PenStyle.DotLine)))
        self._plots[3].setYRange(0, 1.02)
        self._focal = self._plots[2].plot(pen=pg.mkPen(DASHBOARD_COLOURS["focal"], width=2),
                                          name="selected heifer audio")
        for plot in self._plots[1:]:
            plot.setXLink(self._plots[0])
        for plot in self._plots[:-1]:
            plot.getAxis("bottom").setStyle(showValues=False)
        self._add_events()
        self._plots[0].setXRange(self._t[0], self._t[-1], padding=0.01)
        self.scene().sigMouseClicked.connect(self._on_click)

    def _add_plot(self, row: int, ylabel: str, series: list[tuple[str, str]]) -> pg.PlotItem:
        plot = self.addPlot(row=row, col=0, axisItems={"bottom": pg.DateAxisItem(utcOffset=0)})
        plot.setLabel("left", ylabel)
        plot.setMinimumHeight(PLOT_HEIGHT_PX)
        plot.setMenuEnabled(False)
        plot.setMouseEnabled(x=True, y=False)
        plot.showGrid(x=False, y=False)
        legend = plot.addLegend(offset=(5, 2), labelTextColor="k", colCount=3)
        legend.setBrush(pg.mkBrush(255, 255, 255, 180))
        for column, name in series:
            key = column.replace("_avg", "").replace("_nrc", "").replace("_share", "")
            colour = DASHBOARD_COLOURS.get(key, "k")
            values = self._hourly[column].to_numpy(dtype=float)
            plot.plot(self._t, values, pen=pg.mkPen(colour, width=1.2), name=name,
                      connect="finite")
        cursor = pg.InfiniteLine(self._t[0], movable=True, pen=pg.mkPen(CURSOR_COLOUR, width=2))
        cursor.sigDragged.connect(self._on_cursor_dragged)
        plot.addItem(cursor)
        self._cursors.append(cursor)
        return plot

    def _add_events(self) -> None:
        for event in self.data.events.itertuples():
            start = event.start_utc.timestamp()
            for plot in self._plots:
                if event.manual_herding == 1 and pd.notna(event.end_utc):
                    region = pg.LinearRegionItem((start, event.end_utc.timestamp()), movable=False,
                                                 brush=pg.mkBrush(MANUAL_HERDING_COLOUR + "60"))
                    plot.addItem(region)
                plot.addItem(pg.InfiniteLine(start, pen=pg.mkPen("k", style=Qt.PenStyle.DashLine)))
            if not event.is_transition:
                label = pg.TextItem(event.open_parcels, color="k", anchor=(0, 0))
                self._plots[0].addItem(label)
                label.setPos(start, float(np.nanmax(self._hourly["thi_nrc"])))

    # ─────────────────────────────────────────────────────────────────
    #  Cursor and focal animal
    # ─────────────────────────────────────────────────────────────────

    def set_cursor(self, epoch_s: float) -> None:
        for cursor in self._cursors:
            cursor.blockSignals(True)
            cursor.setValue(epoch_s)
            cursor.blockSignals(False)

    def set_focal_animal(self, a: int) -> None:
        cues = compute_animal_hourly_cues(self.data, a, TIMELINE_BIN)
        self._focal.setData(_epoch_s(cues.index), cues["audio"].to_numpy(dtype=float))

    def _on_cursor_dragged(self, line: pg.InfiniteLine) -> None:
        self.time_selected.emit(float(line.value()))

    def _on_click(self, event) -> None:
        if event.button() != Qt.MouseButton.LeftButton:
            return
        for plot in self._plots:
            if plot.sceneBoundingRect().contains(event.scenePos()):
                point = plot.vb.mapSceneToView(event.scenePos())
                self.time_selected.emit(float(point.x()))
                return
