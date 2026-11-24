# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.events                                             ║
# ║  « turn the parcel log into activation events »                  ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Parses labels such as 'heifers 3_4' or 'luciano Heifers 2'      ║
# ║  into open, cut-off and new parcels, and builds one row per      ║
# ║  activation for the parcel_events table.                         ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Parcel activation labels and the event table (handover 4.1)."""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

ROTATION: tuple[int, ...] = (2, 3, 4, 5, 6)
TRANSITION_MARKER = "umstellung"
_LABEL_RE = re.compile(r"(?P<a>\d+)(?:_(?P<b>\d+))?")
_NECKBAND_RE = re.compile(r"\d{6,}")

LABEL_COLUMN = "eShepherd parcel activation"
TIME_COLUMN = "Unix_Timestamp"
NECKBAND_COLUMN = "Neckband_Number"
COMMENT_COLUMN = "comments"
EVENT_COLUMN_MAP: dict[str, str] = {
    "parcel size [ha]": "parcel_size_ha",
    "total length of VF": "vf_length_m",
    "total time per parcel [min]": "total_time_min",
    "water point relocation": "water_point_relocated",
    "manually moving herd": "manual_herding",
    "plant biomass [mm]": "biomass_mm",
    "plant biomass [kg/ha]": "biomass_kg_ha",
}
FLAG_COLUMNS: tuple[str, ...] = ("water point relocation", "manually moving herd")
EXCLUSION_COLUMNS: dict[str, str] = {
    "neckband problem with GPS": "gps_problem",
    "neckband not activated for new parcel": "not_activated",
}
PARCEL_LOG_COLUMNS: tuple[str, ...] = (
    LABEL_COLUMN,
    COMMENT_COLUMN,
    *EVENT_COLUMN_MAP,
    *EXCLUSION_COLUMNS,
)

# ┌────────────────────────────────────────────────────────────┐
# │ Activation labels  « '3_4' means 3 and 4 merged »          │
# └────────────────────────────────────────────────────────────┘


@dataclass(frozen=True)
class ActivationLabel:
    """One parsed eShepherd parcel-activation label."""

    raw: str
    open_parcels: tuple[int, ...]
    is_transition: bool

    @property
    def new_parcel(self) -> int | None:
        """Highest-numbered open parcel when two are merged, else ``None``."""
        return max(self.open_parcels) if len(self.open_parcels) == 2 else None

    @property
    def cut_off_parcel(self) -> int | None:
        """Parcel removed by this activation (rotation predecessor)."""
        if len(self.open_parcels) != 2:
            return None
        low = min(self.open_parcels)
        position = ROTATION.index(low)
        previous = ROTATION[position - 1] if position > 0 else None
        return previous if previous not in self.open_parcels else None


def parse_activation_label(raw: str) -> ActivationLabel:
    """Parse a label from the ``eShepherd parcel activation`` column.

    Raises:
        ValueError: no parcel number in ``raw``.
    """
    match = _LABEL_RE.search(raw)
    if match is None:
        raise ValueError(f"No parcel number in activation label {raw!r}")
    parcels = tuple(int(g) for g in (match["a"], match["b"]) if g is not None)
    return ActivationLabel(
        raw=raw,
        open_parcels=parcels,
        is_transition=TRANSITION_MARKER in raw.lower(),
    )


# ┌────────────────────────────────────────────────────────────┐
# │ Event table  « one row per activation »                    │
# └────────────────────────────────────────────────────────────┘


class ParcelEventBuilder:
    """Build the activation-event table from per-fix parcel-log columns.

    Each distinct activation label is one event: the file stores, on every
    fix, the label of the configuration the collar ran under. Start and end
    are medians over collars. Events are numbered by start, so a higher
    ``event_id`` is a later activation.
    """

    def build(self, fixes: pd.DataFrame) -> pd.DataFrame:
        """Collapse fixes into one row per activation label.

        Args:
            fixes: Raw CSV columns :data:`TIME_COLUMN`,
                   :data:`NECKBAND_COLUMN` and :data:`PARCEL_LOG_COLUMNS`.

        Returns:
            Columns of the ``parcel_events`` table.
        """
        stamped = fixes.assign(_ts=pd.to_datetime(fixes[TIME_COLUMN], utc=True))
        rows = [
            self._build_row(label, group)
            for label, group in stamped.groupby(LABEL_COLUMN, sort=False)
        ]
        events = pd.DataFrame(rows).sort_values("start_utc", kind="stable")
        events.insert(0, "event_id", range(1, len(events) + 1))
        return events.reset_index(drop=True)

    def build_exclusions(self, fixes: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
        """Neckbands listed as GPS problem or not activated, per event."""
        label_to_id = dict(zip(events["label"], events["event_id"], strict=True))
        rows: set[tuple[int, str, str]] = set()
        for column, reason in EXCLUSION_COLUMNS.items():
            listed = fixes[[LABEL_COLUMN, column]].dropna().drop_duplicates()
            for label, cell in listed.itertuples(index=False):
                for neckband in parse_neckband_list(cell):
                    rows.add((label_to_id[label], neckband, reason))
        return pd.DataFrame(sorted(rows), columns=["event_id", "neckband_id", "reason"])

    def _build_row(self, label: str, group: pd.DataFrame) -> dict[str, object]:
        parsed = parse_activation_label(label)
        row: dict[str, object] = {
            "label": label,
            "open_parcels": "_".join(str(p) for p in parsed.open_parcels),
            "cut_off_parcel": parsed.cut_off_parcel,
            "new_parcel": parsed.new_parcel,
            "is_transition": int(parsed.is_transition),
            "start_utc": _median_collar_time(group, "min"),
            "end_utc": _median_collar_time(group, "max"),
        }
        for raw, column in EVENT_COLUMN_MAP.items():
            row[column] = _first_valid(group[raw])
        for raw in FLAG_COLUMNS:
            row[EVENT_COLUMN_MAP[raw]] = int(group[raw].fillna(0).max())
        comments = group[COMMENT_COLUMN].dropna().unique()
        row["comments"] = "; ".join(str(c) for c in comments) or None
        return row


def parse_neckband_list(cell: object) -> list[str]:
    """Neckband ids from a parcel-log cell (float, int or delimited text)."""
    if isinstance(cell, float | int):
        return [str(int(cell))]
    return _NECKBAND_RE.findall(str(cell))


def _median_collar_time(group: pd.DataFrame, which: str) -> str:
    """Median over collars of each collar's first or last fix.

    A switched-off collar can carry a stray fix under the next label hours
    before the herd; the median keeps one collar from moving the boundary.
    """
    per_collar = group.groupby(NECKBAND_COLUMN)["_ts"].agg(which).sort_values()
    return per_collar.iloc[(len(per_collar) - 1) // 2].isoformat()


def _first_valid(series: pd.Series) -> object:
    valid = series.dropna()
    return None if valid.empty else valid.iloc[0]
