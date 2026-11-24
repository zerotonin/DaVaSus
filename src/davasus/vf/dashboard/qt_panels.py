# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.qt_panels                                ║
# ║  « labels, a table and nothing clever »                          ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Key/value info panels fed by summaries.describe_* and the       ║
# ║  sortable herd table                                             ║
# ║  clicking a row selects that heifer.                             ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Info panels and the herd table."""

from __future__ import annotations

import math

from PySide6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QSortFilterProxyModel,
    Qt,
    Signal,
)
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFormLayout,
    QHeaderView,
    QLabel,
    QTableView,
    QWidget,
)

from davasus.vf.dashboard.constants import MISSING_COLOUR, OUTSIDE_COLOURS
from davasus.vf.dashboard.summaries import HERD_TABLE_COLUMNS, Rows, describe_outside_code

STATUS_COLUMN: int = 1


class InfoPanel(QWidget):
    """Form of label/value rows; rebuilt only when the labels change."""

    def __init__(self) -> None:
        super().__init__()
        self._layout = QFormLayout(self)
        self._layout.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
        self._keys: list[str] = []
        self._values: list[QLabel] = []

    def set_rows(self, rows: Rows) -> None:
        keys = [key for key, _ in rows]
        if keys != self._keys:
            self._rebuild(keys)
        for label, (_, value) in zip(self._values, rows, strict=True):
            label.setText(value)

    def _rebuild(self, keys: list[str]) -> None:
        while self._layout.rowCount():
            self._layout.removeRow(0)
        self._values = []
        for key in keys:
            value = QLabel()
            value.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            value.setWordWrap(True)
            self._layout.addRow(f"{key}:", value)
            self._values.append(value)
        self._keys = keys


class HerdTableModel(QAbstractTableModel):
    """Rows from ``summaries.build_herd_table``; row order is animal order."""

    def __init__(self, n_animals: int) -> None:
        super().__init__()
        self._rows: list[list[object]] = [[""] * len(HERD_TABLE_COLUMNS) for _ in range(n_animals)]

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: B008, N802
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: B008, N802
        return len(HERD_TABLE_COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = 0) -> object:  # noqa: N802
        if role == Qt.ItemDataRole.DisplayRole and orientation == Qt.Orientation.Horizontal:
            return HERD_TABLE_COLUMNS[section]
        return None

    def data(self, index: QModelIndex, role: int = 0) -> object:
        value = self._rows[index.row()][index.column()]
        if role == Qt.ItemDataRole.DisplayRole:
            if index.column() == STATUS_COLUMN:
                return describe_outside_code(int(value))
            if isinstance(value, float):
                return "–" if math.isnan(value) else f"{value:g}"
            return str(value)
        if role == Qt.ItemDataRole.UserRole:
            return value
        if role == Qt.ItemDataRole.DecorationRole and index.column() == STATUS_COLUMN:
            return QColor(OUTSIDE_COLOURS.get(int(value), MISSING_COLOUR))
        return None

    def set_rows(self, rows: list[list[object]]) -> None:
        self._rows = rows
        self.dataChanged.emit(self.index(0, 0), self.index(len(rows) - 1, len(HERD_TABLE_COLUMNS) - 1))


class HerdTable(QTableView):
    """Sortable view of the herd; emits the animal index of a clicked row."""

    animal_selected = Signal(int)

    def __init__(self, n_animals: int) -> None:
        super().__init__()
        self._model = HerdTableModel(n_animals)
        self._proxy = QSortFilterProxyModel()
        self._proxy.setSourceModel(self._model)
        self._proxy.setSortRole(Qt.ItemDataRole.UserRole)
        self.setModel(self._proxy)
        self.setSortingEnabled(True)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.verticalHeader().setVisible(False)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.clicked.connect(self._on_clicked)

    def set_rows(self, rows: list[list[object]]) -> None:
        self._model.set_rows(rows)

    def select_animal(self, a: int) -> None:
        row = self._proxy.mapFromSource(self._model.index(a, 0)).row()
        self.blockSignals(True)
        self.selectRow(row)
        self.blockSignals(False)

    def _on_clicked(self, index: QModelIndex) -> None:
        self.animal_selected.emit(self._proxy.mapToSource(index).row())
