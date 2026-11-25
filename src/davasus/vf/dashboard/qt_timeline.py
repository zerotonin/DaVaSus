# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.qt_timeline                              ║
# ║  « the whole trial at a glance »                                 ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Stacked plots with linked time axes: weather, space weather     ║
# ║  or the selected heifer's km and cues per day, herd cues and     ║
# ║  GNSS share. Parcel activations and one draggable cursor         ║
# ║  click or drag anywhere to jump in time.                         ║
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
    REDUCED_GPS_COLOUR,
    TIMELINE_BIN,
    TIMELINE_ROW_FOCAL_DAILY,
    TIMELINE_ROW_SPACE_WEATHER,
)
from davasus.vf.dashboard.data import (
    DashboardData,
    compute_animal_daily_series,
    compute_animal_hourly_cues,
    compute_flag_runs,
    compute_herd_daily_km,
)
from davasus.vf.gnss_quality import HP30_STORM_THRESHOLD

PLOT_HEIGHT_PX: int = 90
SWAP_ROW: int = 1
DAY_S: float = 86_400.0
BAR_WIDTH_DAYS: float = 0.8


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
        self._daily = FocalDailyPlot(self)
        self._space = self._plots[SWAP_ROW]
        self._plots.append(self._daily.plot)
        for plot in self._plots[1:]:
            plot.setXLink(self._plots[0])
        for plot in self._plots:
            if plot is not self._plots[3]:
                plot.getAxis("bottom").setStyle(showValues=False)
        self._add_events()
        self._row_mode = TIMELINE_ROW_SPACE_WEATHER
        self._plots[0].setXRange(self._t[0], self._t[-1], padding=0.01)
        self.scene().sigMouseClicked.connect(self._on_click)

    def _add_plot(self, row: int, ylabel: str, series: list[tuple[str, str]]) -> pg.PlotItem:
        plot = self.addPlot(row=row, col=0, axisItems={"bottom": pg.DateAxisItem(utcOffset=0)})
        self.configure_plot(plot, ylabel)
        for column, name in series:
            key = column.replace("_avg", "").replace("_nrc", "").replace("_share", "")
            colour = DASHBOARD_COLOURS.get(key, "k")
            values = self._hourly[column].to_numpy(dtype=float)
            plot.plot(self._t, values, pen=pg.mkPen(colour, width=1.2), name=name,
                      connect="finite")
        self.add_cursor(plot)
        return plot

    def configure_plot(self, plot: pg.PlotItem, ylabel: str) -> None:
        plot.setLabel("left", ylabel)
        plot.setMinimumHeight(PLOT_HEIGHT_PX)
        plot.setMenuEnabled(False)
        plot.setMouseEnabled(x=True, y=False)
        plot.showGrid(x=False, y=False)
        legend = plot.addLegend(offset=(5, 2), labelTextColor="k", colCount=4)
        legend.setBrush(pg.mkBrush(255, 255, 255, 180))

    def add_cursor(self, plot: pg.PlotItem) -> None:
        cursor = pg.InfiniteLine(self._t[0], movable=True, pen=pg.mkPen(CURSOR_COLOUR, width=2))
        cursor.sigDragged.connect(self._on_cursor_dragged)
        plot.addItem(cursor)
        self._cursors.append(cursor)

    def _add_events(self) -> None:
        runs = compute_flag_runs(self.data.is_gps_reduced, self.data.minutes)
        for start, end in runs:
            for plot in self._plots:
                plot.addItem(pg.LinearRegionItem(
                    (start.timestamp(), end.timestamp()), movable=False,
                    brush=pg.mkBrush(REDUCED_GPS_COLOUR + "70"), pen=pg.mkPen(None)))
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
        self._daily.show_animal(a)

    def set_row_mode(self, mode: str) -> None:
        """Second row shows space weather or the selected heifer's daily values."""
        if mode == self._row_mode:
            return
        shown, hidden = ((self._daily.plot, self._space) if mode == TIMELINE_ROW_FOCAL_DAILY
                         else (self._space, self._daily.plot))
        self.removeItem(hidden)
        hidden.hide()
        self.addItem(shown, row=SWAP_ROW, col=0)
        shown.show()
        self._daily.set_visible(mode == TIMELINE_ROW_FOCAL_DAILY)
        self._row_mode = mode

    def _on_cursor_dragged(self, line: pg.InfiniteLine) -> None:
        self.time_selected.emit(float(line.value()))

    def _on_click(self, event) -> None:
        if event.button() != Qt.MouseButton.LeftButton:
            return
        for plot in self._plots:
            if plot.isVisible() and plot.sceneBoundingRect().contains(event.scenePos()):
                point = plot.vb.mapSceneToView(event.scenePos())
                self.time_selected.emit(float(point.x()))
                return


# ┌────────────────────────────────────────────────────────────┐
# │ Focal daily plot  « km per day left, cues per day right »  │
# └────────────────────────────────────────────────────────────┘


class FocalDailyPlot:
    """Selected heifer's daily odometer km (bars) against the herd IQR,
    with her daily audio cues and pulses on a second y axis."""

    def __init__(self, timeline: TimelineWidget) -> None:
        self.timeline = timeline
        data = timeline.data
        self.plot = pg.PlotItem(axisItems={"bottom": pg.DateAxisItem(utcOffset=0)})
        timeline.configure_plot(self.plot, "km d\u207b\u00b9")
        self.plot.hide()
        herd = compute_herd_daily_km(data)
        centre = _epoch_s(herd.index) + DAY_S / 2
        band_lo = pg.PlotDataItem(centre, herd["q25"].to_numpy(dtype=float))
        band_hi = pg.PlotDataItem(centre, herd["q75"].to_numpy(dtype=float))
        grey = pg.mkColor(DASHBOARD_COLOURS["herd_km"])
        grey.setAlpha(70)
        self.plot.addItem(pg.FillBetweenItem(band_lo, band_hi, brush=pg.mkBrush(grey)))
        self.plot.plot(centre, herd["median"].to_numpy(dtype=float),
                       pen=pg.mkPen(DASHBOARD_COLOURS["herd_km"], width=1.5),
                       name="herd km (median, IQR)")
        self._bars = pg.BarGraphItem(x=[], height=[], width=BAR_WIDTH_DAYS * DAY_S,
                                     brush=pg.mkBrush(DASHBOARD_COLOURS["focal"]), pen=None)
        self.plot.addItem(self._bars)
        self.plot.legend.addItem(self._bars, "her km")

        self._cue_box = pg.ViewBox()
        self.plot.showAxis("right")
        self.plot.getAxis("right").setLabel("cues d\u207b\u00b9")
        self.plot.getAxis("right").linkToView(self._cue_box)
        self._cue_box.setXLink(self.plot)
        self._cue_box.setMouseEnabled(x=False, y=False)
        self._audio = pg.PlotDataItem(pen=pg.mkPen(DASHBOARD_COLOURS["audio"], width=2),
                                      symbol="o", symbolSize=5,
                                      symbolBrush=DASHBOARD_COLOURS["audio"], symbolPen=None)
        self._pulse = pg.PlotDataItem(pen=pg.mkPen(DASHBOARD_COLOURS["pulse"], width=2),
                                      symbol="s", symbolSize=5,
                                      symbolBrush=DASHBOARD_COLOURS["pulse"], symbolPen=None)
        self._cue_box.addItem(self._audio)
        self._cue_box.addItem(self._pulse)
        self.plot.legend.addItem(self._audio, "her audio")
        self.plot.legend.addItem(self._pulse, "her pulses")
        timeline.scene().addItem(self._cue_box)
        self._cue_box.hide()
        self.plot.vb.sigResized.connect(self._sync_cue_box)
        timeline.add_cursor(self.plot)

    def show_animal(self, a: int) -> None:
        data = self.timeline.data
        daily = compute_animal_daily_series(data, a)
        centre = _epoch_s(daily.index) + DAY_S / 2
        self._bars.setOpts(x=centre, height=daily["km"].to_numpy(dtype=float))
        self._audio.setData(centre, daily["audio"].to_numpy(dtype=float))
        self._pulse.setData(centre, daily["pulse"].to_numpy(dtype=float))
        self._cue_box.enableAutoRange(axis=pg.ViewBox.YAxis)
        self.plot.setLabel("left", f"km d\u207b\u00b9  ({data.loms[a][-4:]})")

    def set_visible(self, visible: bool) -> None:
        self._cue_box.setVisible(visible)
        if visible:
            self._sync_cue_box()

    def _sync_cue_box(self) -> None:
        self._cue_box.setGeometry(self.plot.vb.sceneBoundingRect())
        self._cue_box.linkedViewChanged(self.plot.vb, self._cue_box.XAxis)
