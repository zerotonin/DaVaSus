# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.qt_window                                ║
# ║  « one window, many docks »                                      ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Main window: map in the centre, dockable panels for the herd,   ║
# ║  the selected heifer, parcel event, weather, space weather and   ║
# ║  the timeline                                                    ║
# ║  a transport bar to scrub and play. The dock                     ║
# ║  layout is remembered between sessions.                          ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Main dashboard window."""

from __future__ import annotations

import numpy as np
import pandas as pd
from PySide6.QtCore import QDateTime, QSettings, Qt, QTimer, QTimeZone
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QDockWidget,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QScrollArea,
    QSlider,
    QStyle,
    QToolBar,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from davasus.vf.dashboard.constants import (
    DEFAULT_PLAY_SPEED_INDEX,
    DEFAULT_TRAIL_INDEX,
    MANUAL_HERDING_COLOUR,
    PLAY_INTERVAL_MS,
    PLAY_SPEEDS_MIN_PER_TICK,
    REDUCED_GPS_COLOUR,
    TILE_ATTRIBUTION,
    TIMELINE_BIN,
    TIMELINE_ROW_MODES,
    TRAIL_MINUTES_CHOICES,
)
from davasus.vf.dashboard.data import DashboardData, compute_hourly_series
from davasus.vf.dashboard.qt_map import HerdMapWidget
from davasus.vf.dashboard.qt_panels import HerdTable, InfoPanel
from davasus.vf.dashboard.qt_timeline import TimelineWidget
from davasus.vf.dashboard.summaries import (
    build_herd_table,
    describe_event,
    describe_heifer,
    describe_space_weather,
    describe_time,
    describe_weather,
)
from davasus.vf.dashboard.tiles import TileCache
from davasus.vf.gnss_quality import HP30_STORM_THRESHOLD

SETTINGS_ORG: str = "DaVaSus"
SETTINGS_APP: str = "vf-dashboard"
LAYOUT_VERSION: int = 1
LARGE_STEP_MIN: int = 60
SIDE_DOCK_WIDTH_PX: int = 380
ROW_MODE_KEY: str = "timeline_row_mode"


class DashboardWindow(QMainWindow):
    """Scrub through the heifer trial minute by minute."""

    def __init__(self, data: DashboardData, tiles: TileCache | None, source_name: str) -> None:
        super().__init__()
        self.data = data
        self.minute = 0
        self.selected = int(np.argmax(data.audio_cum[-1]))
        self.setWindowTitle(f"DaVaSus heifer dashboard: {source_name}")

        self.map = HerdMapWidget(data, tiles)
        self.map.animal_clicked.connect(self.select_animal)
        self.setCentralWidget(self._wrap_map(tiles is not None))

        self.herd_table = HerdTable(data.n_animals)
        self.herd_table.animal_selected.connect(self.select_animal)
        self.time_panel, self.heifer_panel = InfoPanel(), InfoPanel()
        self.event_panel, self.weather_panel, self.space_panel = InfoPanel(), InfoPanel(), InfoPanel()
        self.timeline = TimelineWidget(data, compute_hourly_series(data, TIMELINE_BIN))
        self.timeline.time_selected.connect(self._on_time_selected)

        self._build_transport()
        self._build_docks()
        self._play_timer = QTimer(self)
        self._play_timer.setInterval(PLAY_INTERVAL_MS)
        self._play_timer.timeout.connect(self._on_play_tick)
        self._restore_layout()
        self.timeline.set_focal_animal(self.selected)
        self.set_minute(self._first_activation_minute())

    # ─────────────────────────────────────────────────────────────────
    #  Central update
    # ─────────────────────────────────────────────────────────────────

    def set_minute(self, i: int) -> None:
        """Move every view to grid minute ``i``."""
        data = self.data
        self.minute = int(np.clip(i, 0, data.n_minutes - 1))
        i = self.minute
        self.map.show_minute(i, TRAIL_MINUTES_CHOICES[self.trail_box.currentIndex()],
                             self.selected, self.labels_box.isChecked(),
                             self.follow_box.isChecked())
        self.time_panel.set_rows(describe_time(data, i))
        self.heifer_panel.set_rows(describe_heifer(data, i, self.selected))
        self.event_panel.set_rows(describe_event(data, i))
        self.weather_panel.set_rows(describe_weather(data, i))
        self.space_panel.set_rows(describe_space_weather(data, i))
        self.herd_table.set_rows(build_herd_table(data, i))
        stamp = data.minutes[i]
        self.timeline.set_cursor(stamp.timestamp())
        self.gps_banner.setVisible(bool(data.is_gps_reduced[i]))
        for widget, setter in ((self.slider, self.slider.setValue),
                               (self.clock, lambda _: self.clock.setDateTime(_to_qt(stamp)))):
            widget.blockSignals(True)
            setter(i)
            widget.blockSignals(False)

    def select_animal(self, a: int) -> None:
        self.selected = a
        self.herd_table.select_animal(a)
        self.timeline.set_focal_animal(a)
        self.heifer_dock.setWindowTitle(f"Selected heifer: {self.data.loms[a]}")
        self.set_minute(self.minute)

    # ─────────────────────────────────────────────────────────────────
    #  Transport bar
    # ─────────────────────────────────────────────────────────────────

    def _build_transport(self) -> None:
        bar = QToolBar("Transport")
        bar.setObjectName("transport")
        self.addToolBar(Qt.ToolBarArea.TopToolBarArea, bar)
        style = self.style()
        self.play_action = QAction(style.standardIcon(QStyle.StandardPixmap.SP_MediaPlay),
                                   "Play / pause (Space)", self)
        self.play_action.setShortcut(QKeySequence(Qt.Key.Key_Space))
        self.play_action.triggered.connect(self.toggle_play)
        steps = (
            (QStyle.StandardPixmap.SP_MediaSkipBackward, "-1 h (Shift+Left)", -LARGE_STEP_MIN,
             "Shift+Left"),
            (QStyle.StandardPixmap.SP_MediaSeekBackward, "-1 min (Left)", -1, "Left"),
            (None, None, 0, None),
            (QStyle.StandardPixmap.SP_MediaSeekForward, "+1 min (Right)", 1, "Right"),
            (QStyle.StandardPixmap.SP_MediaSkipForward, "+1 h (Shift+Right)", LARGE_STEP_MIN,
             "Shift+Right"),
        )
        for icon, tip, delta, key in steps:
            if icon is None:
                bar.addAction(self.play_action)
                continue
            action = QAction(style.standardIcon(icon), tip, self)
            action.setShortcut(QKeySequence(key))
            action.triggered.connect(lambda _=False, d=delta: self.set_minute(self.minute + d))
            bar.addAction(action)

        self.slider = QSlider(Qt.Orientation.Horizontal)
        self.slider.setRange(0, self.data.n_minutes - 1)
        self.slider.setPageStep(1440)
        self.slider.valueChanged.connect(self.set_minute)
        bar.addWidget(self.slider)

        self.clock = QDateTimeEdit()
        self.clock.setTimeZone(QTimeZone.utc())
        self.clock.setDisplayFormat("yyyy-MM-dd HH:mm 'UTC'")
        self.clock.setDateTimeRange(_to_qt(self.data.minutes[0]), _to_qt(self.data.minutes[-1]))
        self.clock.setCalendarPopup(True)
        self.clock.dateTimeChanged.connect(
            lambda value: self.set_minute(self.data.minute_of(_from_qt(value))))
        bar.addWidget(self.clock)

        bar.addSeparator()
        self.speed_box = self._add_combo(bar, "speed", [f"{s} min/step" for s in
                                                        PLAY_SPEEDS_MIN_PER_TICK],
                                         DEFAULT_PLAY_SPEED_INDEX)
        self.trail_box = self._add_combo(bar, "trail", [f"{t} min" for t in TRAIL_MINUTES_CHOICES],
                                         DEFAULT_TRAIL_INDEX)
        self.trail_box.currentIndexChanged.connect(lambda _: self.set_minute(self.minute))
        self.labels_box = QCheckBox("tags")
        self.labels_box.toggled.connect(lambda _: self.set_minute(self.minute))
        bar.addWidget(self.labels_box)
        self.follow_box = QCheckBox("follow herd")
        self.follow_box.setToolTip("Keep the herd centroid in the centre of the map")
        self.follow_box.toggled.connect(lambda _: self.set_minute(self.minute))
        bar.addWidget(self.follow_box)

        bar.addSeparator()
        self.event_box = self._add_combo(
            bar, "jump to", ["activation…"] + [
                f"{e.label}  ({e.start_utc:%d %b %H:%M})" for e in self.data.events.itertuples()],
            0)
        self.event_box.activated.connect(self._on_event_chosen)
        for text, direction in (("◀ cue", -1), ("cue ▶", 1)):
            button = QToolButton()
            button.setText(text)
            button.setToolTip("Previous / next audio cue of the selected heifer")
            button.clicked.connect(lambda _=False, d=direction: self._jump_to_cue(d))
            bar.addWidget(button)

    def _add_combo(self, bar: QToolBar, label: str, items: list[str], index: int) -> QComboBox:
        bar.addWidget(QLabel(f" {label} "))
        box = QComboBox()
        box.addItems(items)
        box.setCurrentIndex(index)
        bar.addWidget(box)
        return box

    def toggle_play(self) -> None:
        style = self.style()
        if self._play_timer.isActive():
            self._play_timer.stop()
            self.play_action.setIcon(style.standardIcon(QStyle.StandardPixmap.SP_MediaPlay))
        else:
            self._play_timer.start()
            self.play_action.setIcon(style.standardIcon(QStyle.StandardPixmap.SP_MediaPause))

    def _on_play_tick(self) -> None:
        if self.minute >= self.data.n_minutes - 1:
            self.toggle_play()
            return
        self.set_minute(self.minute + PLAY_SPEEDS_MIN_PER_TICK[self.speed_box.currentIndex()])

    def _on_time_selected(self, epoch_s: float) -> None:
        self.set_minute(self.data.minute_of(pd.Timestamp(epoch_s, unit="s", tz="UTC")))

    def _on_event_chosen(self, index: int) -> None:
        if index > 0:
            self.set_minute(self.data.minute_of(self.data.events.iloc[index - 1]["start_utc"]))
        self.event_box.setCurrentIndex(0)

    def _jump_to_cue(self, direction: int) -> None:
        cues = np.flatnonzero(self.data.audio[:, self.selected] > 0)
        if direction > 0:
            later = cues[cues > self.minute]
            if later.size:
                self.set_minute(int(later[0]))
        else:
            earlier = cues[cues < self.minute]
            if earlier.size:
                self.set_minute(int(earlier[-1]))

    def _first_activation_minute(self) -> int:
        return self.data.minute_of(self.data.events["start_utc"].min())

    # ─────────────────────────────────────────────────────────────────
    #  Docks and layout memory
    # ─────────────────────────────────────────────────────────────────

    def _wrap_map(self, has_tiles: bool) -> QWidget:
        holder = QWidget()
        layout = QVBoxLayout(holder)
        layout.setContentsMargins(0, 0, 0, 0)
        self.gps_banner = QLabel(
            f"Reduced GPS sensitivity: Hp30 \u2265 {HP30_STORM_THRESHOLD:g} "
            "(geomagnetic storm). Positions are flagged and omitted from analyses.")
        self.gps_banner.setStyleSheet(
            f"background: {REDUCED_GPS_COLOUR}; color: black; padding: 4px; font-weight: bold;")
        self.gps_banner.setVisible(False)
        layout.addWidget(self.gps_banner)
        layout.addWidget(self.map)
        note = "map " + (TILE_ATTRIBUTION if has_tiles else "tiles off") + \
            "  ·  click a heifer to select it, drag to pan, wheel to zoom"
        layout.addWidget(QLabel(note))
        return holder

    def _build_docks(self) -> None:
        menu = self.menuBar().addMenu("&View")
        left, right, bottom = (Qt.DockWidgetArea.LeftDockWidgetArea,
                               Qt.DockWidgetArea.RightDockWidgetArea,
                               Qt.DockWidgetArea.BottomDockWidgetArea)
        time_dock = self._add_dock("Time and herd", self.time_panel, left, menu)
        herd_dock = self._add_dock("Herd", self.herd_table, left, menu)
        self.heifer_dock = self._add_dock(f"Selected heifer: {self.data.loms[self.selected]}",
                                          self.heifer_panel, right, menu, "heifer")
        self._add_dock("Parcel event", self.event_panel, right, menu)
        weather = self._add_dock("Weather", self.weather_panel, right, menu)
        space = self._add_dock("Space weather", self.space_panel, right, menu)
        self.tabifyDockWidget(weather, space)
        weather.raise_()
        self._add_dock("Timeline", self._wrap_timeline(), bottom, menu, scroll=False)
        self.resizeDocks([time_dock, self.heifer_dock], [SIDE_DOCK_WIDTH_PX] * 2,
                         Qt.Orientation.Horizontal)
        self.resizeDocks([time_dock, herd_dock], [220, 600], Qt.Orientation.Vertical)
        menu.addSeparator()
        reset = menu.addAction("Reset layout")
        reset.triggered.connect(self._reset_layout)
        self._default_state = self.saveState(LAYOUT_VERSION)

    def _wrap_timeline(self) -> QWidget:
        holder = QWidget()
        layout = QVBoxLayout(holder)
        layout.setContentsMargins(0, 0, 0, 0)
        row = QHBoxLayout()
        row.addWidget(QLabel(" second row:"))
        self.row_mode_box = QComboBox()
        self.row_mode_box.addItems(list(TIMELINE_ROW_MODES))
        self.row_mode_box.currentTextChanged.connect(self.timeline.set_row_mode)
        row.addWidget(self.row_mode_box)
        row.addStretch(1)
        key = QLabel(
            f"<span style='background:{REDUCED_GPS_COLOUR}'>&nbsp;&nbsp;&nbsp;&nbsp;</span> "
            f"reduced GPS sensitivity (Hp30 \u2265 {HP30_STORM_THRESHOLD:g}) &nbsp;&nbsp; "
            f"<span style='background:{MANUAL_HERDING_COLOUR}'>&nbsp;&nbsp;&nbsp;&nbsp;</span> "
            "manual herding &nbsp;&nbsp; dashed: parcel activation ")
        row.addWidget(key)
        layout.addLayout(row)
        layout.addWidget(self.timeline)
        return holder

    def _add_dock(self, title: str, widget: QWidget, area: Qt.DockWidgetArea, menu,
                  name: str | None = None, scroll: bool = True) -> QDockWidget:
        dock = QDockWidget(title, self)
        dock.setObjectName(name or title.lower().replace(" ", "_"))
        if scroll:
            area_widget = QScrollArea()
            area_widget.setWidgetResizable(True)
            area_widget.setWidget(widget)
            dock.setWidget(area_widget)
        else:
            dock.setWidget(widget)
        self.addDockWidget(area, dock)
        menu.addAction(dock.toggleViewAction())
        return dock

    def _restore_layout(self) -> None:
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        geometry = settings.value("geometry")
        state = settings.value("state")
        if geometry is not None:
            self.restoreGeometry(geometry)
        else:
            self.resize(1600, 1000)
        if state is not None:
            self.restoreState(state, LAYOUT_VERSION)
        mode = settings.value(ROW_MODE_KEY, TIMELINE_ROW_MODES[0])
        if mode not in TIMELINE_ROW_MODES:
            mode = TIMELINE_ROW_MODES[0]
        self.row_mode_box.setCurrentText(mode)
        self.timeline.set_row_mode(mode)

    def _reset_layout(self) -> None:
        self.restoreState(self._default_state, LAYOUT_VERSION)

    def closeEvent(self, event) -> None:  # noqa: N802
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        settings.setValue("geometry", self.saveGeometry())
        settings.setValue("state", self.saveState(LAYOUT_VERSION))
        settings.setValue(ROW_MODE_KEY, self.row_mode_box.currentText())
        super().closeEvent(event)


def _to_qt(stamp: pd.Timestamp) -> QDateTime:
    return QDateTime.fromSecsSinceEpoch(int(stamp.timestamp()), QTimeZone.utc())


def _from_qt(value: QDateTime) -> pd.Timestamp:
    return pd.Timestamp(value.toSecsSinceEpoch(), unit="s", tz="UTC")
