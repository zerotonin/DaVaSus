# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.app                                      ║
# ║  « load once, then hand over to Qt »                             ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Builds the QApplication, loads heifer_vf.db into memory and     ║
# ║  opens the dashboard window.                                     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Start the desktop dashboard."""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
from pathlib import Path

os.environ.setdefault("PYQTGRAPH_QT_LIB", "PySide6")

import pyqtgraph as pg  # noqa: E402
from PySide6.QtWidgets import QApplication  # noqa: E402

from davasus.vf.dashboard.data import DashboardData, load_dashboard_data  # noqa: E402
from davasus.vf.dashboard.qt_window import DashboardWindow  # noqa: E402
from davasus.vf.dashboard.tiles import TileCache  # noqa: E402

log = logging.getLogger(__name__)


def run_dashboard(db_path: Path, tile_dir: Path | None) -> int:
    """Open the dashboard on ``db_path``; ``tile_dir=None`` disables the basemap."""
    app = QApplication.instance() or QApplication(sys.argv)
    window = build_window(load_data(db_path), tile_dir, db_path.name)
    window.show()
    return app.exec()


def load_data(db_path: Path) -> DashboardData:
    log.info("loading %s", db_path.name)
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return load_dashboard_data(connection)
    finally:
        connection.close()


def build_window(data: DashboardData, tile_dir: Path | None, source_name: str) -> DashboardWindow:
    pg.setConfigOptions(antialias=True, foreground="k")
    tiles = TileCache(tile_dir) if tile_dir is not None else None
    return DashboardWindow(data, tiles, source_name)
